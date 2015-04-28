HOST = 'localhost'
PORT = 61613
VERSION = '1.2'

BROKER = 'activemq'
LOGIN, PASSCODE, VIRTUALHOST = {
    'activemq': ('', '', ''),
    'apollo': ('admin', 'password', 'mybroker'),
    'rabbitmq': ('guest', 'guest', '/')
}[BROKER]
