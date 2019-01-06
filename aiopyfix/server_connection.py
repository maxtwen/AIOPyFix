import asyncio
import logging
import socket
from aiopyfix.journaler import DuplicateSeqNoError
from aiopyfix.session import FIXSession
from aiopyfix.connection import FIXEndPoint, ConnectionState, MessageDirection, FIXConnectionHandler


class FIXServerConnectionHandler(FIXConnectionHandler):
    def __init__(self, engine, protocol, reader, writer, addr=None, observer=None):
        FIXConnectionHandler.__init__(self, engine, protocol, reader, writer, addr, observer)

    async def handleSessionMessage(self, msg):
        protocol = self.codec.protocol

        recvSeqNo = msg[protocol.fixtags.MsgSeqNum]

        msgType = msg[protocol.fixtags.MsgType]
        targetCompId = msg[protocol.fixtags.TargetCompID]
        senderCompId = msg[protocol.fixtags.SenderCompID]
        responses = []

        if msgType == protocol.msgtype.LOGON:
            if self.connectionState == ConnectionState.LOGGED_IN:
                logging.warning("Client session already logged in - ignoring login request")
            else:
                # compids are reversed here...
                self.session = self.engine.getOrCreateSessionFromCompIds(senderCompId, targetCompId)
                if self.session is not None:
                    try:
                        self.connectionState = ConnectionState.LOGGED_IN
                        self.heartbeatPeriod = float(msg[protocol.fixtags.HeartBtInt])
                        responses.append(protocol.messages.Messages.logon())
                    except DuplicateSeqNoError:
                        logging.error("Failed to process login request with duplicate seq no")
                        await self.disconnect()
                        return
                else:
                    logging.warning(
                        "Rejected login attempt for invalid session (SenderCompId: %s, TargetCompId: %s)" % (
                            senderCompId, targetCompId))
                    await self.disconnect()
                    return  # we have to return here since self.session won't be valid
        elif self.connectionState == ConnectionState.LOGGED_IN:
            # compids are reversed here
            if not self.session.validateCompIds(senderCompId, targetCompId):
                logging.error("Received message with unexpected comp ids")
                await self.disconnect()
                return

            if msgType == protocol.msgtype.LOGOUT:
                self.connectionState = ConnectionState.LOGGED_OUT
                self.handle_close()
            elif msgType == protocol.msgtype.TESTREQUEST:
                responses.append(protocol.messages.Messages.heartbeat())
            elif msgType == protocol.msgtype.RESENDREQUEST:
                responses.extend(self._handleResendRequest(msg))
            elif msgType == protocol.msgtype.SEQUENCERESET:
                newSeqNo = msg[protocol.fixtags.NewSeqNo]
                self.session.setRecvSeqNo(int(newSeqNo) - 1)
                recvSeqNo = newSeqNo
        else:
            logging.warning("Can't process message, counterparty is not logged in")

        return (recvSeqNo, responses)


class FIXServer(FIXEndPoint):
    def __init__(self, engine, protocol):
        FIXEndPoint.__init__(self, engine, protocol)
        self.server = None

    async def start(self, host, port, loop):
        self.connections = []
        self.server = await asyncio.start_server(self.handle_accept, host, port, loop=loop)
        logging.debug("Awaiting Connections " + host + ":" + str(port))

    async def handle_accept(self, reader, writer):
        addr = writer.get_extra_info('peername')
        logging.info("Connection from %s" % repr(addr))
        connection = FIXServerConnectionHandler(self.engine, self.protocol, reader, writer, addr, self)
        self.connections.append(connection)
        for handler in filter(lambda x: x[1] == ConnectionState.CONNECTED, self.connectionHandlers):
            await handler[0](connection)
