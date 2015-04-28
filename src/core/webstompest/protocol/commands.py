# -*- coding: iso-8859-1 -*-
"""This module implements a low-level and stateless API for all commands of the STOMP protocol version supported by webstompest. All STOMP command frames are represented as :class:`~.frame.StompFrame` objects. It forms the basis for :class:`~.session.StompSession` which represents the full state of an abstract STOMP protocol session and (via :class:`~.session.StompSession`) of both high-level STOMP clients. You can use the commands API independently of other webstompest modules to roll your own STOMP related functionality.

.. note :: Whenever you have to pass a **version** parameter to a command, this is because the behavior of that command depends on the STOMP protocol version of your current session. The default version is the value of :attr:`StompSpec.DEFAULT_VERSION`, which is currently :obj:`'1.0'` but may change in upcoming versions of webstompest (or you might override it yourself). Any command which does not conform to the STOMP protocol version in question will result in a :class:`~.error.StompProtocolError`. The **version** parameter will always be the last argument in the signature; since command signatures may vary with a new STOMP protocol version, you are advised to always specify it as a keyword (as opposed to a positional) argument.

Examples:

>>> from webstompest.protocol import commands, StompFrame, StompSpec
>>> versions = list(commands.versions(StompSpec.VERSION_1_1))
>>> versions
['1.0', '1.1']
>>> commands.connect(versions=versions)
StompFrame(command=u'CONNECT', headers={u'host': '', u'accept-version': '1.0,1.1'})
>>> frame, token = commands.subscribe('/queue/test', {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': '100'})
>>> frame = StompFrame(StompSpec.MESSAGE, {StompSpec.DESTINATION_HEADER: '/queue/test', StompSpec.MESSAGE_ID_HEADER: '007'}, 'hello')
>>> frame
StompFrame(command=u'MESSAGE', headers={u'destination': '/queue/test', u'message-id': '007'}, body='hello')
>>> commands.message(frame) == token # This message matches your subscription.
True
>>> commands.message(frame)
(u'destination', '/queue/test')
>>> frame.version = StompSpec.VERSION_1_1
>>> commands.message(frame)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
stompest.error.StompProtocolError: Invalid MESSAGE frame (subscription header mandatory in version 1.1) [headers={u'destination': '/queue/test', u'message-id': '007'}]
>>> commands.disconnect(receipt='message-12345')
StompFrame(command=u'DISCONNECT', headers={u'receipt': 'message-12345'})

.. seealso :: Specification of STOMP protocols `1.0 <http://stomp.github.com//stomp-specification-1.0.html>`_ and `1.1 <http://stomp.github.com//stomp-specification-1.1.html>`_, your favorite broker's documentation for additional STOMP headers.
"""
from webstompest.error import StompProtocolError

from .frame import StompFrame, StompHeartBeat
from .spec import StompSpec

# outgoing frames

def stomp(login=None, passcode=None, headers=None, versions=None, host=None, heartBeats=None):
    """Create a **STOMP** frame. Not supported in STOMP protocol 1.0, synonymous to :func:`connect` for STOMP protocol 1.1 and higher.
    """
    if (versions is None) or (list(versions) == [StompSpec.VERSION_1_0]):
        raise StompProtocolError('Unsupported command (version %s): %s' % (StompSpec.VERSION_1_0, StompSpec.STOMP))
    frame = connect(login=login, passcode=passcode, headers=headers, versions=versions, host=host, heartBeats=heartBeats)
    return StompFrame(StompSpec.STOMP, frame.headers, frame.body)

def connect(login=None, passcode=None, headers=None, versions=None, host=None, heartBeats=None):
    """Create a **CONNECT** frame.
    
    :param login: The **login** header. The default is :obj:`None`, which means that no such header will be added.
    :param passcode: The **passcode** header. The default is :obj:`None`, which means that no such header will be added.
    :param headers: Additional STOMP headers.
    :param versions: A list of the STOMP versions we wish to support. The default is :obj:`None`, which means that we will offer the broker to accept any version prior or equal to the default STOMP protocol version.
    :param host: The **host** header which gives this client a human readable name on the broker side.
    :param heartBeats: A pair (client heart-beat, server heart-beat) of integer heart-beat intervals in ms. Both intervals must be non-negative. A client heart-beat of 0 means that no heart-beats will be sent by the client. Similarly, a server heart-beat of 0 means that the client does not expect heart-beats from the server.
    """
    headers = dict(headers or [])
    if login is not None:
        headers[StompSpec.LOGIN_HEADER] = login
    if passcode is not None:
        headers[StompSpec.PASSCODE_HEADER] = passcode
    versions = [StompSpec.VERSION_1_0] if (versions is None) else list(sorted(_version(v) for v in versions))
    if versions != [StompSpec.VERSION_1_0]:
        headers[StompSpec.ACCEPT_VERSION_HEADER] = ','.join(_version(version) for version in versions)
        if host is None:
            host = ''
        headers[StompSpec.HOST_HEADER] = host
    if heartBeats:
        if versions == [StompSpec.VERSION_1_0]:
            raise StompProtocolError('Heart-beating not supported (version %s)' % StompSpec.VERSION_1_0)
        try:
            heartBeats = tuple(int(t) for t in heartBeats)
            if not all(t >= 0 for t in heartBeats):
                raise
            heartBeats = '%d,%d' % heartBeats
        except:
            raise StompProtocolError('Invalid heart-beats (two non-negative integers required): %s' % str(heartBeats))
        headers[StompSpec.HEART_BEAT_HEADER] = heartBeats

    return StompFrame(StompSpec.CONNECT, headers)

def disconnect(receipt=None, version=None):
    """Create a **DISCONNECT** frame.
    
    :param receipt: Add a **receipt** header with this id to request a **RECEIPT** frame from the broker. If :obj:`None`, no such header is added.
    """
    headers = {}
    frame = StompFrame(StompSpec.DISCONNECT, headers, version=version)
    _addReceiptHeader(frame, receipt)
    return frame

def send(destination, body='', headers=None, receipt=None, version=None):
    """Create a **SEND** frame.
    
    :param destination: Destination for the frame.
    :param body: Message body. Binary content is allowed but must be accompanied by the STOMP header **content-length** which specifies the number of bytes in the message body.
    :param headers: Additional STOMP headers.
    :param receipt: See :func:`disconnect`.
    """
    frame = StompFrame(StompSpec.SEND, dict(headers or []), body, version=version)
    frame.headers[StompSpec.DESTINATION_HEADER] = destination
    _addReceiptHeader(frame, receipt)
    return frame

def subscribe(destination, headers, receipt=None, version=None):
    """Create a pair (frame, token) of a **SUBSCRIBE** frame and a token which you have to keep if you wish to match incoming **MESSAGE** frames to this subscription  with :func:`message` or to :func:`unsubscribe` later.
    
    :param destination: Destination for the subscription.
    :param headers: Additional STOMP headers.
    :param receipt: See :func:`disconnect`.
    """
    version = _version(version)
    frame = StompFrame(StompSpec.SUBSCRIBE, dict(headers or []), version=version)
    frame.headers[StompSpec.DESTINATION_HEADER] = destination
    _addReceiptHeader(frame, receipt)
    subscription = None
    try:
        subscription = _checkHeader(frame, StompSpec.ID_HEADER)
    except StompProtocolError:
        if (version != StompSpec.VERSION_1_0):
            raise
    token = (StompSpec.DESTINATION_HEADER, destination) if (subscription is None) else (StompSpec.ID_HEADER, subscription)
    return frame, tuple(map(unicode, token))

def unsubscribe(token, receipt=None, version=None):
    """Create an **UNSUBSCRIBE** frame.
    
    :param token: The result of the :func:`subscribe` command which you used to initiate the subscription in question.
    :param receipt: See :meth:`disconnect`.
    """
    version = _version(version)
    frame = StompFrame(StompSpec.UNSUBSCRIBE, dict([token]), version=version)
    _addReceiptHeader(frame, receipt)
    try:
        _checkHeader(frame, StompSpec.ID_HEADER)
    except StompProtocolError:
        if version != StompSpec.VERSION_1_0:
            raise
        _checkHeader(frame, StompSpec.DESTINATION_HEADER)
    return frame

def ack(frame, transactions=None, receipt=None):
    """Create an **ACK** frame for a received **MESSAGE** frame.
    
    :param frame: The :class:`~.frame.StompFrame` object representing the **MESSAGE** frame we wish to ack.
    :param transactions: The ids of currently active transactions --- only if the **frame** is part of one of these transactions, the **transaction** header is included in the ACK frame.
    :param receipt: See :func:`disconnect`.
    """
    frame = StompFrame(StompSpec.ACK, _ackHeaders(frame, transactions), version=frame.version)
    _addReceiptHeader(frame, receipt)
    return frame

def nack(frame, transactions=None, receipt=None):
    """Create a **NACK** frame for a received **MESSAGE** frame.
    
    :param frame: The :class:`~.frame.StompFrame` object representing the **MESSAGE** frame we wish to nack.
    :param transactions: The ids of currently active transactions --- only if the **frame** is part of one of these transactions, the **transaction** header is included in the NACK frame.
    :param receipt: See :func:`disconnect`.
    """
    version = frame.version
    if version == StompSpec.VERSION_1_0:
        raise StompProtocolError('%s not supported (version %s)' % (StompSpec.NACK, version))
    frame = StompFrame(StompSpec.NACK, _ackHeaders(frame, transactions), version=frame.version)
    _addReceiptHeader(frame, receipt)
    return frame

def begin(transaction, receipt=None, version=None):
    """Create a **BEGIN** frame.
    
    :param transaction: The id of the transaction.
    :param receipt: See :meth:`disconnect`.
    """
    frame = StompFrame(StompSpec.BEGIN, {StompSpec.TRANSACTION_HEADER: transaction}, version=version)
    _addReceiptHeader(frame, receipt)
    return frame

def abort(transaction, receipt=None, version=None):
    """Create an **ABORT** frame.
    
    :param transaction: The id of the transaction.
    :param receipt: See :meth:`disconnect`.
    """
    frame = StompFrame(StompSpec.ABORT, {StompSpec.TRANSACTION_HEADER: transaction}, version=version)
    _addReceiptHeader(frame, receipt)
    return frame

def commit(transaction, receipt=None, version=None):
    """Create a **COMMIT** frame.
    
    :param transaction: The id of the transaction.
    :param receipt: See :meth:`disconnect`.
    """
    frame = StompFrame(StompSpec.COMMIT, {StompSpec.TRANSACTION_HEADER: transaction}, version=version)
    _addReceiptHeader(frame, receipt)
    return frame

def beat(version=None):
    """Create a STOMP heart-beat.
    """
    version = _version(version)
    if version == StompSpec.VERSION_1_0:
        raise StompProtocolError('Heart-beating not supported (version %s)' % version)

    return StompHeartBeat()

def negotiateHeartBeat(client, server):
    """Determine the negotiated heart-beating period.

    :param client: The client's proposed heart-beating period.
    :param server: The server's proposed heart-beating period.
    """
    if not (client and server):
        return 0
    return max(client, server)

# incoming frames

def connected(frame, versions=None):
    """Handle a **CONNECTED** frame.
    
    :param versions: The same **versions** parameter you used to create the **CONNECT** frame.
    """
    versions = [StompSpec.VERSION_1_0] if (versions is None) else list(sorted(_version(v) for v in versions))
    version = versions[-1]
    _checkCommand(frame, [StompSpec.CONNECTED])
    headers = frame.headers
    try:
        if version != StompSpec.VERSION_1_0:
            version = _version(headers.get(StompSpec.VERSION_HEADER, StompSpec.VERSION_1_0))
            if version not in versions:
                raise StompProtocolError('')
    except StompProtocolError:
        raise StompProtocolError('Server version incompatible with accepted versions %s [headers=%s]' % (versions, headers))

    session = headers.get(StompSpec.SESSION_HEADER)
    server = None if (version == StompSpec.VERSION_1_0) else headers.get(StompSpec.SERVER_HEADER)

    heartBeats = (0, 0)
    if (version != StompSpec.VERSION_1_0) and (StompSpec.HEART_BEAT_HEADER in headers):
        try:
            heartBeats = tuple(int(t) for t in headers[StompSpec.HEART_BEAT_HEADER].split(StompSpec.HEART_BEAT_SEPARATOR))
            if (len(heartBeats) != 2) or any((t < 0) for t in heartBeats):
                raise ValueError('')
        except:
            raise StompProtocolError('Invalid %s header (two comma-separated and non-negative integers required): %s' % (StompSpec.HEART_BEAT_HEADER, heartBeats))

    return version, server, session, heartBeats

def message(frame):
    """Handle a **MESSAGE** frame. Returns a token which you can use to match this message to its subscription.
    
    .. seealso :: The :func:`subscribe` command.
    """
    _checkCommand(frame, [StompSpec.MESSAGE])
    _checkHeader(frame, StompSpec.MESSAGE_ID_HEADER)
    destination = _checkHeader(frame, StompSpec.DESTINATION_HEADER)
    subscription = None
    try:
        subscription = _checkHeader(frame, StompSpec.SUBSCRIPTION_HEADER)
    except StompProtocolError:
        if frame.version != StompSpec.VERSION_1_0:
            raise
    token = (StompSpec.DESTINATION_HEADER, destination) if (subscription is None) else (StompSpec.ID_HEADER, subscription)
    return token

def receipt(frame):
    """Handle a **RECEIPT** frame. Returns the receipt id which you can use to match this receipt to the command that requested it.
    """
    _checkCommand(frame, [StompSpec.RECEIPT])
    _checkHeader(frame, StompSpec.RECEIPT_ID_HEADER)
    return frame.headers[StompSpec.RECEIPT_ID_HEADER]

def error(frame):
    """Handle an **ERROR** frame. Does not really do anything except checking that this is an **ERROR** frame.
    """
    _checkCommand(frame, [StompSpec.ERROR])

version = _version = StompSpec.version
versions = _versions = StompSpec.versions

# private helper methods

def _ackHeaders(frame, transactions):
    version = frame.version
    _checkCommand(frame, [StompSpec.MESSAGE])
    _checkHeader(frame, StompSpec.MESSAGE_ID_HEADER)
    if version != StompSpec.VERSION_1_0:
        _checkHeader(frame, StompSpec.SUBSCRIPTION_HEADER)
    if version in (StompSpec.VERSION_1_0, StompSpec.VERSION_1_1):
        keys = {
            StompSpec.SUBSCRIPTION_HEADER: StompSpec.SUBSCRIPTION_HEADER,
            StompSpec.MESSAGE_ID_HEADER: StompSpec.MESSAGE_ID_HEADER
        }
    else:
        _checkHeader(frame, StompSpec.ACK_HEADER)
        keys = {StompSpec.ACK_HEADER: StompSpec.ID_HEADER}
    try:
        transaction = frame.headers[StompSpec.TRANSACTION_HEADER]
    except KeyError:
        pass
    else:
        if transaction in set(transactions or []):
            keys[StompSpec.TRANSACTION_HEADER] = StompSpec.TRANSACTION_HEADER
    return dict((keys[key], value) for (key, value) in frame.headers.iteritems() if key in keys)

def _addReceiptHeader(frame, receipt):
    if not receipt:
        return
    if not isinstance(receipt, basestring):
        raise StompProtocolError('Invalid receipt (not a string): %s' % repr(receipt))
    frame.headers[StompSpec.RECEIPT_HEADER] = str(receipt)

def _checkCommand(frame, commands=None):
    if frame.command not in (commands or StompSpec.COMMANDS):
        raise StompProtocolError('Cannot handle command: %s [expected=%s, headers=%s]' % (frame.command, ', '.join(commands), frame.headers))

def _checkHeader(frame, header):
    try:
        return frame.headers[header]
    except KeyError:
        raise StompProtocolError('Invalid %s frame (%s header mandatory in version %s) [headers=%s]' % (frame.command, header, frame.version, frame.headers))
