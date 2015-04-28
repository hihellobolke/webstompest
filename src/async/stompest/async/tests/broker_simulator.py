import logging

from twisted.internet import reactor
from twisted.internet.protocol import Factory, Protocol

from stompest.error import StompFrameError
from stompest.protocol import StompFrame, StompParser, StompSpec

LOG_CATEGORY = __name__

class BlackHoleStompServer(Protocol):
    delimiter = StompSpec.FRAME_DELIMITER

    def __init__(self):
        self.log = logging.getLogger(LOG_CATEGORY)
        self._parser = StompParser()
        self.commandMap = {
            StompSpec.CONNECT: self.handleConnect,
            StompSpec.DISCONNECT: self.handleDisconnect,
            StompSpec.SEND: self.handleSend,
            StompSpec.SUBSCRIBE: self.handleSubscribe,
            StompSpec.ACK: self.handleAck,
            StompSpec.NACK: self.handleNack
        }

    def connectionMade(self):
        self.log.debug('Connection made')

    def connectionLost(self, reason):
        self.log.debug('Connection lost: %s' % reason)

    def dataReceived(self, data):
        self._parser.add(data)

        for frame in iter(self._parser.get, self._parser.SENTINEL):
            try:
                self.log.debug('Received %s' % frame.info())
            except KeyError:
                raise StompFrameError('Unknown STOMP command: %s' % repr(frame))
            self.commandMap[frame.command](frame)

    def getFrame(self, command, headers, body):
        return str(StompFrame(command, headers, body, version=self._parser.version))

    def handleConnect(self, frame):
        pass

    def handleDisconnect(self, frame):
        pass

    def handleSend(self, frame):
        pass

    def handleSubscribe(self, frame):
        pass

    def handleAck(self, frame):
        pass

    def handleNack(self, frame):
        pass

class ErrorOnConnectStompServer(BlackHoleStompServer):
    def handleConnect(self, frame):
        self.transport.write(self.getFrame(StompSpec.ERROR, {}, 'Fake error message'))

class ErrorOnSendStompServer(BlackHoleStompServer):
    def handleConnect(self, frame):
        headers = {}
        if StompSpec.ACCEPT_VERSION_HEADER not in frame.headers:
            headers[StompSpec.SESSION_HEADER] = 'YMCA'
        else:
            headers = {StompSpec.VERSION_HEADER: '1.1'}
            self._parser.version = '1.1'
        self.transport.write(self.getFrame(StompSpec.CONNECTED, headers, ''))

    def handleDisconnect(self, frame):
        self.transport.loseConnection()

    def handleSend(self, frame):
        self.transport.write(self.getFrame(StompSpec.ERROR, {}, 'Fake error message'))

class RemoteControlViaFrameStompServer(BlackHoleStompServer):
    def handleConnect(self, frame):
        headers = {}
        if StompSpec.ACCEPT_VERSION_HEADER not in frame.headers:
            headers[StompSpec.SESSION_HEADER] = 'YMCA'
        else:
            headers = {StompSpec.VERSION_HEADER: '1.1'}
            self._parser.version = '1.1'
        self.transport.write(self.getFrame(StompSpec.CONNECTED, headers, ''))

    def handleDisconnect(self, frame):
        self.transport.loseConnection()

    def handleSend(self, frame):
        if frame.body == 'shutdown':
            self.transport.loseConnection()

    def handleSubscribe(self, frame):
        headers = frame.headers
        replyHeaders = {StompSpec.DESTINATION_HEADER: headers[StompSpec.DESTINATION_HEADER], StompSpec.MESSAGE_ID_HEADER: 4711}
        try:
            replyHeaders[StompSpec.SUBSCRIPTION_HEADER] = headers[StompSpec.ID_HEADER]
        except:
            pass
        self.transport.write(self.getFrame(StompSpec.MESSAGE, replyHeaders, 'hi'))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    factory = Factory()
    factory.protocol = ErrorOnConnectStompServer
    reactor.listenTCP(8007, factory)
    reactor.run()
