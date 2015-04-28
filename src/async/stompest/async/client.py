"""The asynchronous client is based on `Twisted <http://twistedmatrix.com/>`_, a very mature and powerful asynchronous programming framework. It supports destination specific message and error handlers (with default "poison pill" error handling), concurrent message processing, graceful shutdown, and connect and disconnect timeouts.

.. seealso:: `STOMP protocol specification <http://stomp.github.com/>`_, `Twisted API documentation <http://twistedmatrix.com/documents/current/api/>`_, `Apache ActiveMQ - Stomp <http://activemq.apache.org/stomp.html>`_

Examples
--------

.. automodule:: stompest.async.examples
    :members:

Producer
^^^^^^^^

.. literalinclude:: ../../src/async/stompest/async/examples/producer.py

Transformer
^^^^^^^^^^^

.. literalinclude:: ../../src/async/stompest/async/examples/transformer.py

Consumer
^^^^^^^^

.. literalinclude:: ../../src/async/stompest/async/examples/consumer.py

API
---
"""
import logging

from twisted.internet import defer, task

from stompest.error import StompConnectionError, StompFrameError
from stompest.protocol import StompSession, StompSpec
from stompest.util import checkattr

from . import util, listener
from .protocol import StompProtocolCreator

LOG_CATEGORY = __name__

connected = checkattr('_protocol')

class Stomp(object):
    """An asynchronous STOMP client for the Twisted framework.

    :param config: A :class:`~.StompConfig` object.
    :param listenersFactory: The listeners which this (parameterless) function produces will be added to the connection each time :meth:`~.async.client.Stomp.connect` is called. The default behavior (:obj:`None`) is to use :func:`~.async.listener.defaultListeners` in the module :mod:`async.listener`. 
    :param endpointFactory: This function produces a Twisted endpoint which will be used to establish the wire-level connection. It accepts two arguments **broker** (as it is produced by iteration over an :obj:`~.protocol.failover.StompFailoverTransport`) and **timeout** (connect timeout in seconds, :obj:`None` meaning that we will wait indefinitely). The default behavior (:obj:`None`) is to use :func:`~.async.util.endpointFactory` in the module :mod:`async.util`.
    
    .. note :: All API methods which may request a **RECEIPT** frame from the broker -- which is indicated by the **receipt** parameter -- will wait for the **RECEIPT** response until this client's :obj:`~.async.listener.ReceiptListener`'s **timeout** (given that one was added to this client, which by default is not the case). Here, "wait" is to be understood in the asynchronous sense that the method's :class:`twisted.internet.defer.Deferred` result will only call back then. If **receipt** is :obj:`None`, no such header is sent, and the callback will be triggered earlier.

    .. seealso :: :class:`~.StompConfig` for how to set configuration options, :class:`~.StompSession` for session state, :mod:`.protocol.commands` for all API options which are documented here. Details on endpoints can be found in the `Twisted endpoint howto <http://twistedmatrix.com/documents/current/core/howto/endpoints.html>`_.
    """
    protocolCreatorFactory = StompProtocolCreator

    def __init__(self, config, listenersFactory=None, endpointFactory=None):
        self._config = config
        self._session = StompSession(self._config.version, self._config.check)

        self._listenersFactory = listenersFactory or listener.defaultListeners
        self._protocolCreator = self.protocolCreatorFactory(self._config.uri, endpointFactory or util.endpointFactory)

        self.log = logging.getLogger(LOG_CATEGORY)

        self._handlers = {
            'MESSAGE': self._onMessage,
            'CONNECTED': self._onConnected,
            'ERROR': self._onError,
            'RECEIPT': self._onReceipt,
        }

        self._listeners = []

    #
    # interface
    #
    def add(self, listener):
        """Add a listener to this client. For the interface definition, cf. :class:`~.async.listener.Listener`. 
        """
        if listener not in self._listeners:
            # self.log.debug('adding listener: %s' % listener)
            self._listeners.append(listener)
            listener.onAdd(self)

    def remove(self, listener):
        """Remove a listener from this client. 
        """
        # self.log.debug('removing listener: %s' % listener)
        self._listeners.remove(listener)

    @property
    def disconnected(self):
        """This :class:`twisted.internet.defer.Deferred` calls back when the connection to the broker was lost. It will err back when the connection loss was unexpected or caused by another error.
        """
        return self._disconnected

    @disconnected.setter
    def disconnected(self, value):
        self._disconnected = value

    @property
    def _protocol(self):
        try:
            protocol = self.__protocol
        except AttributeError:
            self._protocol = None
            return self._protocol
        if not protocol:
            raise StompConnectionError('Not connected')
        return protocol

    @_protocol.setter
    def _protocol(self, protocol):
        self.__protocol = protocol

    @defer.inlineCallbacks
    def sendFrame(self, frame):
        """Send a raw STOMP frame.

        .. note :: If we are not connected, this method, and all other API commands for sending STOMP frames except :meth:`~.async.client.Stomp.connect`, will raise a :class:`~.StompConnectionError`. Use this command only if you have to bypass the :class:`~.StompSession` logic and you know what you're doing!
        """
        self._protocol.send(frame)
        yield self._notify(lambda l: l.onSend(self, frame))

    @property
    def session(self):
        """The :class:`~.StompSession` associated to this client.
        """
        return self._session

    #
    # STOMP commands
    #
    @util.exclusive
    @defer.inlineCallbacks
    def connect(self, headers=None, versions=None, host=None, heartBeats=None, connectTimeout=None, connectedTimeout=None):
        """connect(headers=None, versions=None, host=None, heartBeats=None, connectTimeout=None, connectedTimeout=None)

        Establish a connection to a STOMP broker. If the wire-level connect fails, attempt a failover according to the settings in the client's :class:`~.StompConfig` object. If there are active subscriptions in the :attr:`~.async.client.Stomp.session`, replay them when the STOMP connection is established. This method returns a :class:`twisted.internet.defer.Deferred` object which calls back with :obj:`self` when the STOMP connection has been established and all subscriptions (if any) were replayed. In case of an error, it will err back with the reason of the failure.

        :param versions: The STOMP protocol versions we wish to support. The default behavior (:obj:`None`) is the same as for the :func:`~.commands.connect` function of the commands API, but the highest supported version will be the one you specified in the :class:`~.StompConfig` object. The version which is valid for the connection about to be initiated will be stored in the :attr:`~.async.client.Stomp.session`.
        :param connectTimeout: This is the time (in seconds) to wait for the wire-level connection to be established. If :obj:`None`, we will wait indefinitely.
        :param connectedTimeout: This is the time (in seconds) to wait for the STOMP connection to be established (that is, the broker's **CONNECTED** frame to arrive). If :obj:`None`, we will wait indefinitely.

        .. note :: Only one connect attempt may be pending at a time. Any other attempt will result in a :class:`~.StompAlreadyRunningError`.

        .. seealso :: The :mod:`.protocol.failover` and :mod:`~.protocol.session` modules for the details of subscription replay and failover transport.
        """
        frame = self.session.connect(self._config.login, self._config.passcode, headers, versions, host, heartBeats)

        try:
            self._protocol
        except:
            pass
        else:
            raise StompConnectionError('Already connected')

        for listener in self._listenersFactory():
            self.add(listener)

        try:
            self._protocol = yield self._protocolCreator.connect(connectTimeout, self._onFrame, self._onConnectionLost)
        except Exception as e:
            self.log.error('Endpoint connect failed')
            raise

        try:
            self.sendFrame(frame)
            yield self._notify(lambda l: l.onConnect(self, frame, connectedTimeout))

        except Exception as e:
            self.disconnect(failure=e)
            yield self.disconnected

        yield self._replay()

        defer.returnValue(self)

    @connected
    @defer.inlineCallbacks
    def disconnect(self, receipt=None, failure=None, timeout=None):
        """disconnect(self, receipt=None, failure=None, timeout=None)
        
        Send a **DISCONNECT** frame and terminate the STOMP connection.

        :param failure: A disconnect reason (a :class:`Exception`) to err back. Example: ``versions=['1.0', '1.1']``
        :param timeout: This is the time (in seconds) to wait for a graceful disconnect, that is, for pending message handlers to complete. If **timeout** is :obj:`None`, we will wait indefinitely.

        .. note :: The :attr:`~.async.client.Stomp.session`'s active subscriptions will be cleared if no failure has been passed to this method. This allows you to replay the subscriptions upon reconnect. If you do not wish to do so, you have to clear the subscriptions yourself by calling the :meth:`~.StompSession.close` method of the :attr:`~.async.client.Stomp.session`. The result of any (user-requested or not) disconnect event is available via the :attr:`disconnected` property.
        """
        try:
            yield self._notify(lambda l: l.onDisconnect(self, failure, timeout))
        except Exception as e:
            self.disconnect(failure=e)

        protocol = self._protocol
        try:
            if (self.session.state == self.session.CONNECTED):
                yield self.sendFrame(self.session.disconnect(receipt))
        except Exception as e:
            self.disconnect(failure=e)
        finally:
            protocol.loseConnection()

    @connected
    @defer.inlineCallbacks
    def send(self, destination, body='', headers=None, receipt=None):
        """send(destination, body='', headers=None, receipt=None)

        Send a **SEND** frame.
        """
        frame = self.session.send(destination, body, headers, receipt)
        yield self.sendFrame(frame)

    @connected
    @defer.inlineCallbacks
    def ack(self, frame, receipt=None):
        """ack(frame, receipt=None)

        Send an **ACK** frame for a received **MESSAGE** frame.
        """
        frame = self.session.ack(frame, receipt)
        yield self.sendFrame(frame)

    @connected
    @defer.inlineCallbacks
    def nack(self, frame, receipt=None):
        """nack(frame, receipt=None)

        Send a **NACK** frame for a received **MESSAGE** frame.
        """
        frame = self.session.nack(frame, receipt)
        yield self.sendFrame(frame)

    @connected
    @defer.inlineCallbacks
    def begin(self, transaction=None, receipt=None):
        """begin(transaction=None, receipt=None)

        Send a **BEGIN** frame to begin a STOMP transaction.
        """
        frame = self.session.begin(transaction, receipt)
        yield self.sendFrame(frame)

    @connected
    @defer.inlineCallbacks
    def abort(self, transaction=None, receipt=None):
        """abort(transaction=None, receipt=None)

        Send an **ABORT** frame to abort a STOMP transaction.
        """
        frame = self.session.abort(transaction, receipt)
        yield self.sendFrame(frame)

    @connected
    @defer.inlineCallbacks
    def commit(self, transaction=None, receipt=None):
        """commit(transaction=None, receipt=None)

        Send a **COMMIT** frame to commit a STOMP transaction.
        """
        frame = self.session.commit(transaction, receipt)
        yield self.sendFrame(frame)

    @connected
    @defer.inlineCallbacks
    def subscribe(self, destination, headers=None, receipt=None, listener=None):
        """subscribe(destination, headers=None, receipt=None, listener=None)

        :param listener: An optional :class:`~.Listener` object which will be added to this connection to handle events associated to this subscription.
        
        Send a **SUBSCRIBE** frame to subscribe to a STOMP destination. The callback value of the :class:`twisted.internet.defer.Deferred` which this method returns is a token which is used internally to match incoming **MESSAGE** frames and must be kept if you wish to :meth:`~.async.client.Stomp.unsubscribe` later.
        """
        frame, token = self.session.subscribe(destination, headers, receipt, listener)
        if listener:
            self.add(listener)
        yield self._notify(lambda l: l.onSubscribe(self, frame, l))
        yield self.sendFrame(frame)
        defer.returnValue(token)

    @connected
    @defer.inlineCallbacks
    def unsubscribe(self, token, receipt=None):
        """unsubscribe(token, receipt=None)

        Send an **UNSUBSCRIBE** frame to terminate an existing subscription.

        :param token: The result of the :meth:`~.async.client.Stomp.subscribe` command which initiated the subscription in question.
        """
        context = self.session.subscription(token)
        frame = self.session.unsubscribe(token, receipt)
        yield self.sendFrame(frame)
        yield self._notify(lambda l: l.onUnsubscribe(self, frame, context))

    #
    # callbacks for received STOMP frames
    #
    @defer.inlineCallbacks
    def _onFrame(self, frame):
        yield self._notify(lambda l: l.onFrame(self, frame))
        if not frame:
            return
        try:
            handler = self._handlers[frame.command]
        except KeyError:
            raise StompFrameError('Unknown STOMP command: %s' % repr(frame))
        yield handler(frame)

    @defer.inlineCallbacks
    def _onConnected(self, frame):
        self.session.connected(frame)
        self.log.info('Connected to stomp broker [session=%s, version=%s]' % (self.session.id, self.session.version))
        self._protocol.setVersion(self.session.version)
        yield self._notify(lambda l: l.onConnected(self, frame))

    @defer.inlineCallbacks
    def _onError(self, frame):
        yield self._notify(lambda l: l.onError(self, frame))

    @defer.inlineCallbacks
    def _onMessage(self, frame):
        headers = frame.headers
        messageId = headers[StompSpec.MESSAGE_ID_HEADER]

        try:
            token = self.session.message(frame)
        except:
            self.log.error('Ignoring message (no handler found): %s [%s]' % (messageId, frame.info()))
            defer.returnValue(None)
        context = self.session.subscription(token)

        try:
            yield self._notify(lambda l: l.onMessage(self, frame, context))
        except Exception as e:
            self.log.error('Disconnecting (error in message handler): %s [%s]' % (messageId, frame.info()))
            self.disconnect(failure=e)

    @defer.inlineCallbacks
    def _onReceipt(self, frame):
        receipt = self.session.receipt(frame)
        yield self._notify(lambda l: l.onReceipt(self, frame, receipt))

    #
    # private helpers
    #
    @defer.inlineCallbacks
    def _notify(self, notify):
        for listener in list(self._listeners):
            yield notify(listener)

    @defer.inlineCallbacks
    def _onConnectionLost(self, reason):
        self._protocol = None
        try:
            yield self._notify(lambda l: l.onConnectionLost(self, reason))
        finally:
            yield self._notify(lambda l: l.onCleanup(self))

    def _replay(self):
        def replay():
            for (destination, headers, receipt, context) in self.session.replay():
                self.log.info('Replaying subscription: %s' % headers)
                yield self.subscribe(destination, headers=headers, receipt=receipt, listener=context)
        return task.cooperate(replay()).whenDone()
