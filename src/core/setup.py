import os
import sys

from setuptools import setup, find_packages

from webstompest import FULL_VERSION

def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()

if sys.version_info[:2] < (2, 6):
    print 'webstompest requires Python version 2.6 or later (%s detected).' % '.'.join(sys.version_info[:2])
    sys.exit(-1)
if sys.version_info[:2] >= (3, 0):
    print 'webstompest is not yet compatible with Python 3 (%s detected).' % '.'.join(sys.version_info[:2])
    sys.exit(-1)

setup(
    name='webstompest',
    version=FULL_VERSION,
    author='Jan Mueller, Gautam',
    author_email='nikipore@gmail.com, g@ut.am',
    description='STOMP library for Python including a synchronous client',
    license='Apache License 2.0',
    packages=find_packages(),
    namespace_packages=['webstompest'],
    long_description=read('README.txt'),
    keywords='stomp activemq rabbitmq apollo',
    url='https://github.com/hihellobolke/webstompest',
    include_package_data=True,
    zip_safe=True,
    install_requires=[],
    tests_require=['mock'],
    test_suite='webstompest.tests',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Framework :: Twisted',
        'Topic :: System :: Networking',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)
