import logging

from twisted.internet import reactor, defer, task
from twisted.trial import unittest

from stompest import async, sync
from stompest.async.listener import SubscriptionListener, ReceiptListener
from stompest.async.util import sendToErrorDestinationAndRaise
from stompest.config import StompConfig
from stompest.error import StompConnectionError, StompProtocolError
from stompest.protocol import StompSpec, commands

logging.basicConfig(level=logging.DEBUG)
LOG_CATEGORY = __name__

from stompest.tests import HOST, PORT, LOGIN, PASSCODE, VIRTUALHOST, BROKER, VERSION

class StompestTestError(Exception):
    pass

class AsyncClientBaseTestCase(unittest.TestCase):
    queue = None
    errorQueue = None
    log = logging.getLogger(LOG_CATEGORY)
    headers = {StompSpec.ID_HEADER: u'4711'}

    TIMEOUT = 0.2

    def getConfig(self, version, port=None):
        return StompConfig('tcp://%s:%s' % (HOST, port or PORT), login=LOGIN, passcode=PASSCODE, version=version)

    def cleanQueues(self):
        self.cleanQueue(self.queue)
        self.cleanQueue(self.errorQueue)

    def cleanQueue(self, destination, headers=None):
        if not destination:
            return

        client = sync.Stomp(self.getConfig(StompSpec.VERSION_1_0))
        client.connect(host=VIRTUALHOST)
        client.subscribe(destination, headers)
        while client.canRead(0.2):
            frame = client.receiveFrame()
            self.log.debug('Dequeued old %s' % frame.info())
        client.disconnect()

    def setUp(self):
        self.cleanQueues()
        self.unhandledFrame = None
        self.errorQueueFrame = None
        self.consumedFrame = None
        self.framesHandled = 0

    def _saveFrameAndBarf(self, _, frame):
        self.log.info('Save message and barf')
        self.unhandledFrame = frame
        raise StompestTestError('this is a test')

    def _barfOneEatOneAndDisonnect(self, client, frame):
        self.framesHandled += 1
        if self.framesHandled == 1:
            self._saveFrameAndBarf(client, frame)
        self._eatOneFrameAndDisconnect(client, frame)

    def _eatOneFrameAndDisconnect(self, client, frame):
        self.log.debug('Eat message and disconnect')
        self.consumedFrame = frame
        client.disconnect()

    def _saveErrorFrameAndDisconnect(self, client, frame):
        self.log.debug('Save error message and disconnect')
        self.errorQueueFrame = frame
        client.disconnect()

    def _eatFrame(self, client, frame): # @UnusedVariable
        self.log.debug('Eat message')
        self.consumedFrame = frame
        self.framesHandled += 1

    def _nackFrame(self, client, frame):
        self.log.debug('NACK message')
        self.consumedFrame = frame
        self.framesHandled += 1
        client.nack(frame)

    def _onMessageFailedSendToErrorDestinationAndRaise(self, client, failure, frame, errorDestination):
        sendToErrorDestinationAndRaise(client, failure, frame, errorDestination)

class HandlerExceptionWithErrorQueueIntegrationTestCase(AsyncClientBaseTestCase):
    frame1 = 'choke on this'
    msg1Hdrs = {'food': 'barf', 'persistent': 'true'}
    frame2 = 'follow up message'
    queue = '/queue/asyncHandlerExceptionWithErrorQueueUnitTest'
    errorQueue = '/queue/zzz.error.asyncStompestHandlerExceptionWithErrorQueueUnitTest'

    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect_version_1_0(self):
        self.cleanQueue(self.queue)
        return self._test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect(StompSpec.VERSION_1_0)

    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect_version_1_1(self):
        self.cleanQueue(self.queue, self.headers)
        return self._test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect(StompSpec.VERSION_1_1)

    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect_version_1_2(self):
        self.cleanQueue(self.queue)
        return self._test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect(StompSpec.VERSION_1_2)

    @defer.inlineCallbacks
    def _test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect(self, version):
        if version not in commands.versions(VERSION):
            print 'Skipping test case (version %s is not configured)' % VERSION
            defer.returnValue(None)

        if BROKER == 'rabbitmq':
            print 'RabbitMQ does not support selector header'
            defer.returnValue(None)

        config = self.getConfig(version)
        client = async.Stomp(config)

        try:
            yield client.connect(host=VIRTUALHOST, versions=[version])
        except StompProtocolError as e:
            print 'Broker does not support STOMP protocol %s. Skipping this test case. [%s]' % (e, version)
            defer.returnValue(None)

        # enqueue two messages
        messageHeaders = dict(self.msg1Hdrs)
        defaultHeaders = {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL}
        specialCharactersHeader = u'fen\xeatre:\r\n'
        if version != StompSpec.VERSION_1_0:
            defaultHeaders.update(self.headers)
            messageHeaders[specialCharactersHeader] = u'\xbfqu\xe9 tal?, s\xfc\xdf'

        client.send(self.queue, self.frame1, messageHeaders)
        client.send(self.queue, self.frame2)

        client.disconnect()
        yield client.disconnected

        # barf on first message so it will get put in error queue
        # use selector to guarantee message order (order not necessarily guaranteed)
        headers = {StompSpec.SELECTOR_HEADER: u"food='barf'"}
        headers.update(defaultHeaders)

        yield client.connect(host=VIRTUALHOST, versions=[version])
        client.subscribe(self.queue, headers, listener=SubscriptionListener(self._saveFrameAndBarf, errorDestination=self.errorQueue, onMessageFailed=self._onMessageFailedSendToErrorDestinationAndRaise))

        # client disconnected and returned error
        try:
            yield client.disconnected
        except StompestTestError:
            pass
        else:
            raise

        client = async.Stomp(config) # take a fresh client to prevent replay (we were disconnected by an error)

        # reconnect and subscribe again - consuming second message then disconnecting
        client = yield client.connect(host=VIRTUALHOST)
        headers.pop('selector')

        client.subscribe(self.queue, headers, listener=SubscriptionListener(self._eatOneFrameAndDisconnect, errorDestination=self.errorQueue))

        # client disconnects without error
        yield client.disconnected

        # reconnect and subscribe to error queue
        client = yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.errorQueue, defaultHeaders, listener=SubscriptionListener(self._saveErrorFrameAndDisconnect))

        # wait for disconnect
        yield client.disconnected

        # verify that first message was in error queue
        self.assertEquals(self.frame1, self.errorQueueFrame.body)
        self.assertEquals(messageHeaders[u'food'], self.errorQueueFrame.headers['food'])
        if version != StompSpec.VERSION_1_0:
            self.assertEquals(messageHeaders[specialCharactersHeader], self.errorQueueFrame.headers[specialCharactersHeader])
        self.assertNotEquals(self.unhandledFrame.headers[StompSpec.MESSAGE_ID_HEADER], self.errorQueueFrame.headers[StompSpec.MESSAGE_ID_HEADER])

        # verify that second message was consumed
        self.assertEquals(self.frame2, self.consumedFrame.body)

    @defer.inlineCallbacks
    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_no_disconnect(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = async.Stomp(config)

        # connect
        yield client.connect(host=VIRTUALHOST)

        # enqueue two messages
        client.send(self.queue, self.frame1, self.msg1Hdrs)
        client.send(self.queue, self.frame2)

        client.disconnect()
        yield client.disconnected
        yield client.connect(host=VIRTUALHOST)

        # barf on first frame, disconnect on second frame
        client.subscribe(self.queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL}, listener=SubscriptionListener(self._barfOneEatOneAndDisonnect, errorDestination=self.errorQueue))

        # client disconnects without error
        yield client.disconnected

        # reconnect and subscribe to error queue
        client = yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.errorQueue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL}, listener=SubscriptionListener(self._saveErrorFrameAndDisconnect))

        # wait for disconnect
        yield client.disconnected

        # verify that one message was in error queue (can't guarantee order)
        self.assertNotEquals(None, self.errorQueueFrame)
        self.assertTrue(self.errorQueueFrame.body in (self.frame1, self.frame2))

    @defer.inlineCallbacks
    def test_onhandlerException_disconnect(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = async.Stomp(config)

        yield client.connect(host=VIRTUALHOST)
        client.send(self.queue, self.frame1, self.msg1Hdrs)
        disconnecting = client.disconnect()
        yield client.disconnect()
        yield disconnecting
        yield client.disconnected
        yield client.connect(host=VIRTUALHOST)
        # barf on first frame (implicit disconnect)
        client.subscribe(self.queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL}, listener=SubscriptionListener(self._saveFrameAndBarf, ack=False, onMessageFailed=self._onMessageFailedSendToErrorDestinationAndRaise))

        # client disconnected and returned error
        try:
            yield client.disconnected
        except StompestTestError:
            pass
        else:
            raise

        # reconnect and subscribe again - consuming retried message and disconnecting
        client = async.Stomp(config) # take a fresh client to prevent replay (we were disconnected by an error)
        client = yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL}, listener=SubscriptionListener(self._eatOneFrameAndDisconnect))

        # client disconnects without error
        yield client.disconnected

        # verify that message was retried
        self.assertEquals(self.frame1, self.unhandledFrame.body)
        self.assertEquals(self.frame1, self.consumedFrame.body)

class GracefulDisconnectTestCase(AsyncClientBaseTestCase):
    numMsgs = 5
    msgCount = 0
    frame = 'test'
    queue = '/queue/asyncGracefulDisconnectUnitTest'

    @defer.inlineCallbacks
    def test_onDisconnect_waitForOutstandingMessagesToFinish(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = async.Stomp(config)
        client.add(ReceiptListener(1.0))

        # connect
        client = yield client.connect(host=VIRTUALHOST)
        yield task.cooperate(iter([client.send(self.queue, self.frame, receipt='message-%d' % j) for j in xrange(self.numMsgs)])).whenDone()
        client.subscribe(self.queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL}, listener=SubscriptionListener(self._frameHandler))

        # wait for disconnect
        yield client.disconnected

        # reconnect and subscribe again to make sure that all messages in the queue were ack'ed
        client = yield client.connect(host=VIRTUALHOST)
        self.timeExpired = False
        self.timeoutDelayedCall = reactor.callLater(1, self._timesUp, client) # @UndefinedVariable
        client.subscribe(self.queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL}, listener=SubscriptionListener(self._eatOneFrameAndDisconnect))

        # wait for disconnect
        yield client.disconnected

        # time should have expired if there were no messages left in the queue
        self.assertTrue(self.timeExpired)

    def _frameHandler(self, client, _):
        self.msgCount += 1
        if self.msgCount < self.numMsgs:
            d = defer.Deferred()
            reactor.callLater(1, d.callback, None) # @UndefinedVariable
            return d
        else:
            client.disconnect(receipt='bye-bye')

    def _timesUp(self, client):
        self.log.debug("Time's up!!!")
        self.timeExpired = True
        client.disconnect()

class SubscribeTestCase(AsyncClientBaseTestCase):
    frame = 'test'
    queue = '/queue/asyncSubscribeTestCase'

    @defer.inlineCallbacks
    def test_unsubscribe(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = async.Stomp(config)

        client = yield client.connect(host=VIRTUALHOST)

        token = yield client.subscribe(self.queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL}, listener=SubscriptionListener(self._eatFrame))
        client.send(self.queue, self.frame)
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)

        client.unsubscribe(token)
        client.send(self.queue, self.frame)
        yield task.deferLater(reactor, 0.2, lambda: None)
        self.assertEquals(self.framesHandled, 1)

        client.subscribe(self.queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL}, listener=SubscriptionListener(self._eatFrame))
        while self.framesHandled != 2:
            yield task.deferLater(reactor, 0.01, lambda: None)
        client.disconnect()
        yield client.disconnected

    @defer.inlineCallbacks
    def test_replay(self):
        config = self.getConfig(StompSpec.VERSION_1_0)

        client = async.Stomp(config)
        client = yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL}, listener=SubscriptionListener(self._eatFrame))
        client.send(self.queue, self.frame)
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)
        client._protocol.loseConnection()
        try:
            yield client.disconnected
        except StompConnectionError:
            pass
        client = yield client.connect(host=VIRTUALHOST)
        client.send(self.queue, self.frame)
        while self.framesHandled != 2:
            yield task.deferLater(reactor, 0.01, lambda: None)

        yield client.disconnect(failure=RuntimeError('Hi'))
        try:
            yield client.disconnected
        except RuntimeError as e:
            self.assertEquals(str(e), 'Hi')

        client = yield client.connect(host=VIRTUALHOST)
        client.send(self.queue, self.frame)
        while self.framesHandled != 2:
            yield task.deferLater(reactor, 0.01, lambda: None)

        client.disconnect()
        yield client.disconnected

class NackTestCase(AsyncClientBaseTestCase):
    frame = 'test'
    queue = '/queue/asyncNackTestCase'

    def test_nack_stomp_1_1(self):
        return self._test_nack(StompSpec.VERSION_1_1)

    def test_nack_stomp_1_2(self):
        return self._test_nack(StompSpec.VERSION_1_2)

    @defer.inlineCallbacks
    def _test_nack(self, version):
        if version not in commands.versions(VERSION):
            print 'Skipping test case (version %s is not configured)' % VERSION
            defer.returnValue(None)

        config = self.getConfig(version)
        client = async.Stomp(config)
        try:
            client = yield client.connect(host=VIRTUALHOST, versions=[version])

        except StompProtocolError as e:
            print 'Broker does not support STOMP protocol %s. Skipping this test case. [%s]' % (version, e)
            defer.returnValue(None)

        client.subscribe(self.queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL, StompSpec.ID_HEADER: '4711'}, listener=SubscriptionListener(self._nackFrame, ack=False))
        client.send(self.queue, self.frame)
        while not self.framesHandled:
            yield task.deferLater(reactor, 0.01, lambda: None)

        client.disconnect()
        yield client.disconnected

        if BROKER == 'activemq':
            print 'Broker %s by default does not redeliver messages. Will not try and harvest the NACKed message.' % BROKER
            return

        self.framesHandled = 0
        client = yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL, StompSpec.ID_HEADER: '4711'}, listener=SubscriptionListener(self._eatFrame, ack=True))
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)

        client.disconnect()
        yield client.disconnected

class TransactionTestCase(AsyncClientBaseTestCase):
    frame = 'test'
    queue = '/queue/asyncTransactionTestCase'

    @defer.inlineCallbacks
    def test_transaction_commit(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = async.Stomp(config)
        client.add(ReceiptListener())
        yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL, StompSpec.ID_HEADER: '4711'}, listener=SubscriptionListener(self._eatFrame, ack=True))

        transaction = '4711'
        yield client.begin(transaction, receipt='%s-begin' % transaction)
        client.send(self.queue, 'test message with transaction', {StompSpec.TRANSACTION_HEADER: transaction})
        yield task.deferLater(reactor, 0.1, lambda: None)
        client.send(self.queue, 'test message without transaction')
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)
        self.assertEquals(self.consumedFrame.body, 'test message without transaction')
        yield client.commit(transaction, receipt='%s-commit' % transaction)
        while self.framesHandled != 2:
            yield task.deferLater(reactor, 0.01, lambda: None)
        self.assertEquals(self.consumedFrame.body, 'test message with transaction')
        client.disconnect()
        yield client.disconnected

    @defer.inlineCallbacks
    def test_transaction_abort(self):
        config = self.getConfig(StompSpec.VERSION_1_0)
        client = async.Stomp(config)
        client.add(ReceiptListener())
        yield client.connect(host=VIRTUALHOST)
        client.subscribe(self.queue, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL, StompSpec.ID_HEADER: '4711'}, listener=SubscriptionListener(self._eatFrame, ack=True))

        transaction = '4711'
        yield client.begin(transaction, receipt='%s-begin' % transaction)
        client.send(self.queue, 'test message with transaction', {StompSpec.TRANSACTION_HEADER: transaction})
        yield task.deferLater(reactor, 0.1, lambda: None)
        client.send(self.queue, 'test message without transaction')
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)
        self.assertEquals(self.consumedFrame.body, 'test message without transaction')
        yield client.abort(transaction, receipt='%s-commit' % transaction)
        client.disconnect()
        yield client.disconnected
        self.assertEquals(self.framesHandled, 1)

class HeartBeatTestCase(AsyncClientBaseTestCase):
    frame = 'test'
    queue = '/queue/asyncHeartBeatTestCase'

    @defer.inlineCallbacks
    def test_heart_beat(self):
        port = 61612 if (BROKER == 'activemq') else PORT
        config = self.getConfig(StompSpec.VERSION_1_1, port)
        client = async.Stomp(config)
        yield client.connect(host=VIRTUALHOST, heartBeats=(250, 250))
        disconnected = client.disconnected

        yield task.deferLater(reactor, 2.5, lambda: None)
        client.session._clientSendHeartBeat = 0
        try:
            yield disconnected
        except (StompConnectionError, StompProtocolError):
            pass
        else:
            raise

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]]) # @UndefinedVariable
    trial.run()
