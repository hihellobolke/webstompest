from webstompest.config import StompConfig
from webstompest.sync import Stomp

CONFIG = StompConfig('ws://localhost:4444/mq')
QUEUE = '/queue/test'

if __name__ == '__main__':
    client = Stomp(CONFIG)
    client.connect()
    client.send(QUEUE, 'test message 1')
    client.send(QUEUE, 'test message 2')
    client.disconnect()
