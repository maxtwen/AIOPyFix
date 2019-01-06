import asyncio
import importlib
import sys
from aiopyfix.codec import Codec
from aiopyfix.journaler import DuplicateSeqNoError
from aiopyfix.message import FIXMessage, MessageDirection

from aiopyfix.session import *
from enum import Enum

class ConnectionState(Enum):
    UNKNOWN = 0
    DISCONNECTED = 1
    CONNECTED = 2
    LOGGED_IN = 3
    LOGGED_OUT = 4

class FIXException(Exception):
    class FIXExceptionReason(Enum):
        NOT_CONNECTED = 0
        DECODE_ERROR = 1
        ENCODE_ERROR = 2

    def __init__(self, reason, description = None):
        super(Exception, self).__init__(description)
        self.reason = reason

class SessionWarning(Exception):
    pass

class SessionError(Exception):
    pass

class FIXConnectionHandler(object):
    def __init__(self, engine, protocol, reader, writer, addr=None, observer=None):
        self.codec = Codec(protocol)
        self.engine = engine
        self.connectionState = ConnectionState.CONNECTED
        self.session = None
        self.addr = addr
        self.observer = observer
        self.msgBuffer = b''
        self.heartbeatPeriod = 30.0
        self.msgHandlers = []
        self.reader, self.writer = reader, writer
        self.heartbeatTimerRegistration = None
        self.expectedHeartbeatRegistration = None
        asyncio.ensure_future(self.handle_read())

    def address(self):
        return self.addr

    async def disconnect(self):
        await self.handle_close()

    async def _notifyMessageObservers(self, msg, direction, persistMessage=True):
        if persistMessage is True:
            self.engine.journaller.persistMsg(msg, self.session, direction)
        for handler in filter(lambda x: (x[1] is None or x[1] == direction) and (x[2] is None or x[2] == msg.msgType),
                              self.msgHandlers):
            await handler[0](self, msg)

    def addMessageHandler(self, handler, direction = None, msgType = None):
        self.msgHandlers.append((handler, direction, msgType))

    def removeMessageHandler(self, handler, direction = None, msgType = None):
        remove = filter(lambda x: x[0] == handler and
                                  (x[1] == direction or direction is None) and
                                  (x[2] == msgType or msgType is None), self.msgHandlers)
        for h in remove:
            self.msgHandlers.remove(h)

    def _sendHeartbeat(self):
        self.sendMsg(self.codec.protocol.messages.Messages.heartbeat())

    def _expectedHeartbeat(self, type, closure):
        logging.warning("Expected heartbeat from peer %s" % (self.expectedHeartbeatRegistration ,))
        self.sendMsg(self.codec.protocol.messages.Messages.test_request())

    def _handleResendRequest(self, msg):
        protocol = self.codec.protocol
        responses = []

        beginSeqNo = msg[protocol.fixtags.BeginSeqNo]
        endSeqNo = msg[protocol.fixtags.EndSeqNo]
        if int(endSeqNo) == 0:
            endSeqNo = sys.maxsize
        logging.info("Received resent request from %s to %s", beginSeqNo, endSeqNo)
        replayMsgs = self.engine.journaller.recoverMsgs(self.session, MessageDirection.OUTBOUND, beginSeqNo, endSeqNo)
        gapFillBegin = int(beginSeqNo)
        gapFillEnd = int(beginSeqNo)
        for replayMsg in replayMsgs:
            msgSeqNum = int(replayMsg[protocol.fixtags.MsgSeqNum])
            if replayMsg[protocol.fixtags.MsgType] in protocol.msgtype.sessionMessageTypes:
                gapFillEnd = msgSeqNum + 1
            else:
                if self.engine.shouldResendMessage(self.session, replayMsg):
                    if gapFillBegin < gapFillEnd:
                        # we need to send a gap fill message
                        gapFillMsg = FIXMessage(protocol.msgtype.SEQUENCERESET)
                        gapFillMsg.setField(protocol.fixtags.GapFillFlag, 'Y')
                        gapFillMsg.setField(protocol.fixtags.MsgSeqNum, gapFillBegin)
                        gapFillMsg.setField(protocol.fixtags.NewSeqNo, str(gapFillEnd))
                        responses.append(gapFillMsg)

                    # and then resent the replayMsg
                    replayMsg.removeField(protocol.fixtags.BeginString)
                    replayMsg.removeField(protocol.fixtags.BodyLength)
                    replayMsg.removeField(protocol.fixtags.SendingTime)
                    replayMsg.removeField(protocol.fixtags.SenderCompID)
                    replayMsg.removeField(protocol.fixtags.TargetCompID)
                    replayMsg.removeField(protocol.fixtags.CheckSum)
                    replayMsg.setField(protocol.fixtags.PossDupFlag, "Y")
                    responses.append(replayMsg)

                    gapFillBegin = msgSeqNum + 1
                else:
                    gapFillEnd = msgSeqNum + 1
                    responses.append(replayMsg)

        if gapFillBegin < gapFillEnd:
            # we need to send a gap fill message
            gapFillMsg = FIXMessage(protocol.msgtype.SEQUENCERESET)
            gapFillMsg.setField(protocol.fixtags.GapFillFlag, 'Y')
            gapFillMsg.setField(protocol.fixtags.MsgSeqNum, gapFillBegin)
            gapFillMsg.setField(protocol.fixtags.NewSeqNo, str(gapFillEnd))
            responses.append(gapFillMsg)

        return responses

    async def handle_read(self):
        while True:
            try:
                msg = await self.reader.read(8192)
                if not msg:
                    raise ConnectionError
                self.msgBuffer = self.msgBuffer + msg
                while True:
                    if self.connectionState == ConnectionState.DISCONNECTED:
                        break

                    (decodedMsg, parsedLength) = self.codec.decode(self.msgBuffer)
                    if decodedMsg is None:
                        break
                    await self.processMessage(decodedMsg)
                    self.msgBuffer = self.msgBuffer[parsedLength:]
            except ConnectionError as why:
                logging.debug("Connection has been closed %s" % (why,))
                await self.disconnect()
                return

    async def handleSessionMessage(self, msg):
        return -1

    async def processMessage(self, decodedMsg):
        protocol = self.codec.protocol

        beginString = decodedMsg[protocol.fixtags.BeginString]
        if beginString != protocol.beginstring:
            logging.warning("FIX BeginString is incorrect (expected: %s received: %s)", (protocol.beginstring, beginString))
            await self.disconnect()
            return

        msgType = decodedMsg[protocol.fixtags.MsgType]

        try:
            responses = []
            if msgType in protocol.msgtype.sessionMessageTypes:
                (recvSeqNo, responses) = await self.handleSessionMessage(decodedMsg)
            else:
                recvSeqNo = decodedMsg[protocol.fixtags.MsgSeqNum]

            # validate the seq number
            (seqNoState, lastKnownSeqNo) = self.session.validateRecvSeqNo(recvSeqNo)

            if seqNoState is False:
                # We should send a resend request
                logging.info("Requesting resend of messages: %s to %s" % (lastKnownSeqNo, 0))
                responses.append(protocol.messages.Messages.resend_request(lastKnownSeqNo, 0))
                # we still need to notify if we are processing Logon message
                if msgType == protocol.msgtype.LOGON:
                    await self._notifyMessageObservers(decodedMsg, MessageDirection.INBOUND, False)
            else:
                self.session.setRecvSeqNo(recvSeqNo)
                await self._notifyMessageObservers(decodedMsg, MessageDirection.INBOUND)

            for m in responses:
                await self.sendMsg(m)

        except SessionWarning as sw:
            logging.warning(sw)
        except SessionError as se:
            logging.error(se)
            await self.disconnect()
        except DuplicateSeqNoError:
            try:
                if decodedMsg[protocol.fixtags.PossDupFlag] == "Y":
                    logging.debug("Received duplicate message with PossDupFlag set")
            except KeyError:
                pass
            finally:
                logging.error("Failed to process message with duplicate seq no (MsgSeqNum: %s) (and no PossDupFlag='Y') - disconnecting" % (recvSeqNo, ))
                await self.disconnect()


    async def handle_close(self):
        if self.connectionState != ConnectionState.DISCONNECTED:
            logging.info("Client disconnected")
            self.writer.close()
            self.connectionState = ConnectionState.DISCONNECTED
            self.msgHandlers.clear()
            if self.observer is not None:
                await self.observer.notifyDisconnect(self)

    async def sendMsg(self, msg):
        if self.connectionState != ConnectionState.CONNECTED and self.connectionState != ConnectionState.LOGGED_IN:
            return

        encodedMsg = self.codec.encode(msg, self.session).encode('utf-8')
        self.writer.write(encodedMsg)
        await self.writer.drain()
        decodedMsg, junk = self.codec.decode(encodedMsg)

        try:
            await self._notifyMessageObservers(decodedMsg, MessageDirection.OUTBOUND)
        except DuplicateSeqNoError:
            logging.error("We have sent a message with a duplicate seq no, failed to persist it (MsgSeqNum: %s)" % (decodedMsg[self.codec.protocol.fixtags.MsgSeqNum]))


class FIXEndPoint(object):
    def __init__(self, engine, protocol):
        self.engine = engine
        self.protocol = importlib.import_module(protocol)

        self.connections = []
        self.connectionHandlers = []

    def writable(self):
        return True

    def start(self, host, port, loop):
        pass

    def stop(self):
        pass

    def addConnectionListener(self, handler, filter):
        self.connectionHandlers.append((handler, filter))

    def removeConnectionListener(self, handler, filter):
        for s in self.connectionHandlers:
            if s == (handler, filter):
                self.connectionHandlers.remove(s)

    async def notifyDisconnect(self, connection):
        self.connections.remove(connection)
        for handler in filter(lambda x: x[1] == ConnectionState.DISCONNECTED, self.connectionHandlers):
            await handler[0](connection)

