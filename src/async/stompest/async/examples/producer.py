import json
import logging

from twisted.internet import defer, reactor

from stompest.config import StompConfig

from stompest.async import Stomp
from stompest.async.listener import ReceiptListener

class Producer(object):
    QUEUE = '/queue/testIn'

    def __init__(self, config=None):
        if config is None:
            config = StompConfig('tcp://localhost:61613')
        self.config = config

    @defer.inlineCallbacks
    def run(self):
        client = yield Stomp(self.config).connect()
        client.add(ReceiptListener(1.0))
        for j in range(10):
            yield client.send(self.QUEUE, json.dumps({'count': j}), receipt='message-%d' % j)
        client.disconnect(receipt='bye')
        yield client.disconnected # graceful disconnect: waits until all receipts have arrived
        reactor.stop()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    Producer().run()
    reactor.run()
