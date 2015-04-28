from webstompest.config import StompConfig
from webstompest.protocol import StompSpec
from webstompest.sync import Stomp

CONFIG = StompConfig('ws://localhost:4444/mq')
QUEUE = '/queue/test'

if __name__ == '__main__':
    client = Stomp(CONFIG)
    client.connect()
    client.subscribe(QUEUE, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL})
    while True:
        frame = client.receiveFrame()
        print 'Got %s' % frame.info()
        client.ack(frame)
    client.disconnect()
