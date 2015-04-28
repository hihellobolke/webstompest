"""The synchronous client is dead simple. It does not assume anything about your concurrency model (thread vs process) or force you to use it any particular way. It gets out of your way and lets you do what you want.

Examples
--------

.. automodule:: webstompest.sync.examples
    :members:

Producer
^^^^^^^^

.. literalinclude:: ../../src/core/stompest/sync/examples/producer.py

Consumer
^^^^^^^^

.. literalinclude:: ../../src/core/stompest/sync/examples/consumer.py

API
---
"""
import collections
import contextlib
import logging
import time

from webstompest.error import StompConnectionError, StompProtocolError
from webstompest.protocol import StompFailoverTransport, StompSession
from webstompest.util import checkattr

from .transport import StompFrameTransport, StompFrameOverWebSocketTransport

LOG_CATEGORY = __name__

connected = checkattr('_transport')


class Stomp(object):

    """A synchronous STOMP client.

    This is the successor of the simple STOMP client in webstompest 1.x, but the API is not backward compatible.

    :param config: A :class:`~.StompConfig` object

    .. seealso :: :class:`~.StompConfig` for how to set session configuration options, :class:`~.StompSession`
        for session state, :mod:`.protocol.commands` for all API options which are documented here.
    """
    _failoverFactory = StompFailoverTransport

    def _transportFactorySelector(broker):
        protocol = broker['protocol']
        host = broker['host']
        port = broker['port']
        if protocol == 'wss' or protocol == 'ws':
            path = broker['path']
            return StompFrameOverWebSocketTransport(host, port, path=path, protocol=protocol)
        else:
            return StompFrameTransport(host, port)

    def __init__(self, config):
        self.log = logging.getLogger(LOG_CATEGORY)
        self._config = config
        self._session = StompSession(self._config.version, self._config.check)
        self._failover = self._failoverFactory(config.uri)
        self._transport = None

    def connect(self, headers=None, versions=None, host=None, heartBeats=None,
                connectTimeout=None, connectedTimeout=None):
        """Establish a connection to a STOMP broker.

        If the wire-level connect fails, attempt a failover according to
         the settings in the client's :class:`~.StompConfig` object. If there are active subscriptions in the
         :attr:`~.sync.client.Stomp.session`, replay them when the STOMP connection is established.

        :param versions: The STOMP protocol versions we wish to support. The default behavior (:obj:`None`) is the
            same as for the :func:`~.commands.connect` function of the commands API, but the highest supported version
            will be the one you specified in the :class:`~.StompConfig` object. The version which is valid for the
            connection about to be initiated will be stored in the :attr:`~.sync.client.Stomp.session`.
        :param connectTimeout: This is the time (in seconds) to wait for the wire-level connection to be established.
            If :obj:`None`, we will wait indefinitely.
        :param connectedTimeout: This is the time (in seconds) to wait for the STOMP connection to be established
            (that is, the broker's **CONNECTED** frame to arrive). If :obj:`None`, we will wait indefinitely.

        **Example:**

        >>> client = Stomp(StompConfig('tcp://localhost:61613', version=StompSpec.VERSION_1_1))
        >>> client.connect()
        >>> client.session.version
        '1.1'
        >>> client.disconnect()
        >>> client.connect(versions=[StompSpec.VERSION_1_0])
        >>> client.session.version
        '1.0'
        >>> client.disconnect()
        >>> client.session.version
        '1.1'

        .. seealso :: The :mod:`.protocol.failover` and :mod:`.protocol.session` modules for the details of subscription
            replay and failover transport.
        """
        try:  # preserve existing connection
            self._transport
        except StompConnectionError:
            pass
        else:
            raise StompConnectionError(
                'Already connected to %s' % self._transport)

        try:
            for (broker, connectDelay) in self._failover:
                transport = self._transportFactorySelector(broker)
                if connectDelay:
                    self.log.debug(
                        'Delaying connect attempt for %d ms' % int(connectDelay * 1000))
                    time.sleep(connectDelay)
                self.log.info('Connecting to %s ...' % transport)
                try:
                    transport.connect(connectTimeout)
                except StompConnectionError as e:
                    self.log.warning(
                        'Could not connect to %s [%s]' % (transport, e))
                else:
                    self.log.info('Connection established')
                    self._transport = transport
                    self._connect(
                        headers, versions, host, heartBeats, connectedTimeout)
                    break
        except StompConnectionError as e:
            self.log.error('Reconnect failed [%s]' % e)
            raise

    def _connect(self, headers, versions, host, heartBeats, timeout):
        frame = self.session.connect(
            self._config.login, self._config.passcode, headers, versions, host, heartBeats)
        self.sendFrame(frame)
        if not self.canRead(timeout):
            self.session.disconnect()
            raise StompProtocolError(
                'STOMP session connect failed [timeout=%s]' % timeout)
        frame = self.receiveFrame()
        self.session.connected(frame)
        self.log.info('Connected to stomp broker [session=%s, version=%s]' % (
            self.session.id, self.session.version))
        self._transport.setVersion(self.session.version)
        for (destination, headers, receipt, _) in self.session.replay():
            self.log.info('Replaying subscription %s' % headers)
            self.subscribe(destination, headers, receipt)

    @connected
    def disconnect(self, receipt=None):
        """disconnect(receipt=None)

        Send a STOMP **DISCONNECT** command and terminate the STOMP connection.

        .. note :: Calling this method will clear the session's active subscriptions unless you request a **RECEIPT** response from the broker. In the latter case, you have to disconnect the wire-level connection and flush the subscriptions yourself by calling ``self.close(flush=True)``.
        """
        self.sendFrame(self.session.disconnect(receipt))
        if not receipt:
            self.close()

    # STOMP frames

    @connected
    def send(self, destination, body='', headers=None, receipt=None):
        """send(destination, body='', headers=None, receipt=None)

        Send a **SEND** frame.
        """
        self.sendFrame(self.session.send(destination, body, headers, receipt))

    @connected
    def subscribe(self, destination, headers=None, receipt=None):
        """subscribe(destination, headers=None, receipt=None)

        Send a **SUBSCRIBE** frame to subscribe to a STOMP destination. This method returns a token which you have to keep if you wish to match incoming **MESSAGE** frames to this subscription or to :meth:`~.sync.client.Stomp.unsubscribe` later.
        """
        frame, token = self.session.subscribe(destination, headers, receipt)
        self.sendFrame(frame)
        return token

    @connected
    def unsubscribe(self, token, receipt=None):
        """unsubscribe(token, receipt=None)

        Send an **UNSUBSCRIBE** frame to terminate an existing subscription.
        """
        self.sendFrame(self.session.unsubscribe(token, receipt))

    @connected
    def ack(self, frame, receipt=None):
        """ack(frame, receipt=None)

        Send an **ACK** frame for a received **MESSAGE** frame.
        """
        self.sendFrame(self.session.ack(frame, receipt))

    @connected
    def nack(self, headers, receipt=None):
        """nack(frame, receipt=None)

        Send a **NACK** frame for a received **MESSAGE** frame.
        """
        self.sendFrame(self.session.nack(headers, receipt))

    @connected
    def begin(self, transaction, receipt=None):
        """begin(transaction=None, receipt=None)

        Send a **BEGIN** frame to begin a STOMP transaction.
        """
        self.sendFrame(self.session.begin(transaction, receipt))

    @connected
    def abort(self, transaction, receipt=None):
        """abort(transaction=None, receipt=None)

        Send an **ABORT** frame to abort a STOMP transaction.
        """
        self.sendFrame(self.session.abort(transaction, receipt))

    @connected
    def commit(self, transaction, receipt=None):
        """commit(transaction=None, receipt=None)

        Send a **COMMIT** frame to commit a STOMP transaction.
        """
        self.sendFrame(self.session.commit(transaction, receipt))

    @contextlib.contextmanager
    @connected
    def transaction(self, transaction=None, receipt=None):
        """transaction(transaction=None, receipt=None)

        A context manager for STOMP transactions. Upon entering the :obj:`with` block, a transaction will be begun and upon exiting, that transaction will be committed or (if an error occurred) aborted.

        **Example:**

        >>> client = Stomp(StompConfig('tcp://localhost:61613'))
        >>> client.connect()
        >>> client.subscribe('/queue/test', {'ack': 'client-individual'})
        (u'destination', u'/queue/test')
        >>> client.canRead(0) # Check that queue is empty.
        False
        >>> with client.transaction(receipt='important') as transaction:
        ...     client.send('/queue/test', 'message with transaction header', {StompSpec.TRANSACTION_HEADER: transaction})
        ...     client.send('/queue/test', 'message without transaction header')
        ...     raise RuntimeError('poof')
        ...
        Traceback (most recent call last):
          File "<stdin>", line 4, in <module>
        RuntimeError: poof
        >>> client.receiveFrame()
        StompFrame(command=u'RECEIPT', headers={u'receipt-id': u'important-begin'}, body='')
        >>> client.receiveFrame()
        StompFrame(command=u'RECEIPT', headers={u'receipt-id': u'important-abort'}, body='')
        >>> frame = client.receiveFrame()
        >>> frame.command, frame.body
        (u'MESSAGE', 'message without transaction header')
        >>> client.ack(frame)
        >>> client.canRead(0) # frame with transaction header was dropped by the broker
        False
        >>> client.disconnect()
        """
        transaction = self.session.transaction(transaction)
        self.begin(transaction, receipt and ('%s-begin' % receipt))
        try:
            yield transaction
            self.commit(transaction, receipt and ('%s-commit' % receipt))
        except:
            self.abort(transaction, receipt and ('%s-abort' % receipt))
            raise

    def message(self, frame):
        """If you received a **MESSAGE** frame, this method will produce a token which allows you to match it against its subscription.

        :param frame: a **MESSAGE** frame.

        .. note :: If the client is not aware of the subscription, or if we are not connected, this method will raise a :class:`~.StompProtocolError`.
        """
        return self.session.message(frame)

    def receipt(self, frame):
        """If you received a **RECEIPT** frame, this method will extract the receipt id which you employed to request that receipt.

        :param frame: A **MESSAGE** frame (a :class:`~.StompFrame` object).

        .. note :: If the client is not aware of the outstanding receipt, this method will raise a :class:`~.StompProtocolError`.
        """
        return self.session.receipt(frame)

    # frame transport

    def close(self, flush=True):
        """Close both the client's :attr:`~.sync.client.Stomp.session` and transport (that is, the wire-level connection with the broker).

        :param flush: Decides whether the :attr:`~.sync.client.Stomp.session` should forget its active subscriptions or not.

        .. note :: If you do not flush the subscriptions, they will be replayed upon this client's next :meth:`~.sync.client.Stomp.connect`!
        """
        self.session.close(flush)
        try:
            self.__transport and self.__transport.disconnect()
        finally:
            self._transport = None

    @connected
    def canRead(self, timeout=None):
        """canRead(timeout=None)

        Tell whether there is an incoming STOMP frame available for us to read.

        :param timeout: This is the time (in seconds) to wait for a frame to become available. If :obj:`None`, we will wait indefinitely.

        .. note :: If the wire-level connection is not available, this method will raise a :class:`~.StompConnectionError`!
        """
        if self._messages:
            return True
        deadline = None if (timeout is None) else (time.time() + timeout)
        while True:
            timeout = deadline and max(0, deadline - time.time())
            if not self._transport.canRead(timeout):
                return False
            frame = self._transport.receive()
            self.session.received()
            if self.log.isEnabledFor(logging.DEBUG):
                self.log.debug('Received %s' % frame.info())
            # there's a real STOMP frame on the wire, not a heart-beat
            if frame:
                self._messages.append(frame)
                return True

    def sendFrame(self, frame):
        """Send a raw STOMP frame.

        :param frame: Any STOMP frame (represented as a :class:`~.StompFrame` object).

        .. note :: If we are not connected, this method, and all other API commands for sending STOMP frames except :meth:`~.sync.client.Stomp.connect`, will raise a :class:`~.StompConnectionError`. Use this command only if you have to bypass the :class:`~.StompSession` logic and you know what you're doing!
        """
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug('Sending %s' % frame.info())
        self._transport.send(frame)
        self.session.sent()

    def receiveFrame(self):
        """Fetch the next available frame.

        .. note :: If we are not connected, this method will raise a :class:`~.StompConnectionError`. Keep in mind that this method will block forever if there are no frames incoming on the wire. Be sure to use peek with ``self.canRead(timeout)`` before!
        """
        if self.canRead():
            return self._messages.popleft()

    @property
    def session(self):
        """The :class:`~.StompSession` associated to this client.
        """
        return self._session

    @property
    def _transport(self):
        transport = self.__transport
        if not transport:
            raise StompConnectionError('Not connected')
        try:
            transport.canRead(0)
        except Exception as e:
            self.close(flush=False)
            raise e
        return transport

    @_transport.setter
    def _transport(self, transport):
        self.__transport = transport
        self._messages = collections.deque()

    # heart-beating

    @connected
    def beat(self):
        """beat()

        Create a STOMP heart-beat.

        **Example**:

        >>> # you might want to enable logging to trace the wire-level traffic
        ... import time
        >>> client = Stomp(StompConfig('tcp://localhost:61612', version=StompSpec.VERSION_1_1))
        >>> client.connect(heartBeats=(100, 100))
        >>> start = time.time()
        >>> elapsed = lambda t = None: (t or time.time()) - start
        >>> times = lambda: 'elapsed: %.2f, last received: %.2f, last sent: %.2f' % (
        ...     elapsed(), elapsed(client.lastReceived), elapsed(client.lastSent)
        ... )
        >>> while elapsed() < 2 * client.clientHeartBeat / 1000.0:
        ...     client.canRead(0.8 * client.serverHeartBeat / 1000.0) # poll server heart-beats
        ...     client.beat() # send client heart-beat
        ...     print times()
        ...
        False
        elapsed: 0.08, last received: 0.00, last sent: 0.08
        False
        elapsed: 0.17, last received: 0.00, last sent: 0.17
        False
        elapsed: 0.25, last received: 0.20, last sent: 0.25
        >>> client.canRead() # server will disconnect us because we're not heart-beating any more
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        webstompest.error.StompConnectionError: Connection closed [No more data]
        >>> print times()
        elapsed: 0.50, last received: 0.50, last sent: 0.25
        """
        self.sendFrame(self.session.beat())

    @property
    def lastSent(self):
        """The last time when data was sent.
        """
        return self.session.lastSent

    @property
    def lastReceived(self):
        """The last time when data was received.
        """
        return self.session.lastReceived

    @property
    def clientHeartBeat(self):
        """The negotiated client heart-beat period in ms.
        """
        return self.session.clientHeartBeat

    @property
    def serverHeartBeat(self):
        """The negotiated server heart-beat period in ms.
        """
        return self.session.serverHeartBeat
