import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


config = {
    'description': 'aiopyfix',
    'author': 'Maksim Afanasevsky',
    'url': 'https://github.com/maxtwen/AIOPyFix/',
    'download_url': 'https://github.com/maxtwen/AIOPyFix/',
    'author_email': 'maxtwen1@gmail.com',
    'version': '0.4',
    'install_requires': [''],
    'packages': ['aiopyfix', 'aiopyfix.FIX44'],
    'scripts': [],
    'name': 'aiopyfix',
    'long_description': read('README.md'),
    'long_description_content_type': 'text/markdown'
}

setup(**config)
