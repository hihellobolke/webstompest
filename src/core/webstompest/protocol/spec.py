import codecs
from webstompest.error import StompProtocolError

class StompSpec(object):
    """This class hosts all constants related to the STOMP protocol specification in its various versions. There really isn't much to document, but you are invited to take a look at all available constants in the source code. Wait a minute ... one attribute is particularly noteworthy, name :attr:`DEFAULT_VERSION` --- which currently is :obj:`'1.0'` (but this may change in upcoming webstompest releases, so you're advised to always explicitly define which STOMP protocol version you are going to use).

    .. seealso :: Specification of STOMP protocols `1.0 <http://stomp.github.com//stomp-specification-1.0.html>`_ and `1.1 <http://stomp.github.com//stomp-specification-1.1.html>`_, your favorite broker's documentation for additional STOMP headers.
    """
    # specification of the STOMP protocol: http://stomp.github.com//index.html
    VERSION_1_0, VERSION_1_1, VERSION_1_2 = '1.0', '1.1', '1.2'
    VERSIONS = [VERSION_1_0, VERSION_1_1, VERSION_1_2]
    DEFAULT_VERSION = VERSION_1_0

    ABORT = u'ABORT'
    ACK = u'ACK'
    BEGIN = u'BEGIN'
    COMMIT = u'COMMIT'
    CONNECT = u'CONNECT'
    DISCONNECT = u'DISCONNECT'
    NACK = u'NACK'
    SEND = u'SEND'
    STOMP = u'STOMP'
    SUBSCRIBE = u'SUBSCRIBE'
    UNSUBSCRIBE = u'UNSUBSCRIBE'

    CLIENT_COMMANDS = {
        VERSION_1_0: set([
            ABORT, ACK, BEGIN, COMMIT, CONNECT, DISCONNECT,
            SEND, SUBSCRIBE, UNSUBSCRIBE
        ]),
        VERSION_1_1: set([
            ABORT, ACK, BEGIN, COMMIT, CONNECT, DISCONNECT,
            NACK, SEND, STOMP, SUBSCRIBE, UNSUBSCRIBE
        ]),
        VERSION_1_2: set([
            ABORT, ACK, BEGIN, COMMIT, CONNECT, DISCONNECT,
            NACK, SEND, STOMP, SUBSCRIBE, UNSUBSCRIBE
        ])
    }

    CONNECTED = u'CONNECTED'
    ERROR = u'ERROR'
    MESSAGE = u'MESSAGE'
    RECEIPT = u'RECEIPT'

    SERVER_COMMANDS = {
        VERSION_1_0: set([CONNECTED, ERROR, MESSAGE, RECEIPT]),
        VERSION_1_1: set([CONNECTED, ERROR, MESSAGE, RECEIPT]),
        VERSION_1_2: set([CONNECTED, ERROR, MESSAGE, RECEIPT])
    }

    COMMANDS = dict(CLIENT_COMMANDS)
    for (version, commands) in SERVER_COMMANDS.iteritems():
        COMMANDS.setdefault(version, set()).update(commands)

    COMMANDS_BODY_ALLOWED = {
        VERSION_1_1: set([SEND, MESSAGE, ERROR]),
        VERSION_1_2: set([SEND, MESSAGE, ERROR])
    }

    CODECS = {  # for command and headers
        VERSION_1_0: 'ascii'
    }
    CODECS = dict([
        (version, codecs.lookup(CODECS.get(version, 'utf-8'))) for version in VERSIONS
    ])

    LINE_DELIMITER = '\n'
    STRIP_LINE_DELIMITER = {
        VERSION_1_2: '\r'
    }

    ESCAPE_CHARACTER = '\\'
    ESCAPED_CHARACTERS = {
        VERSION_1_0: {'\\': '\\', 'c': ':', 'n': '\n'},
        VERSION_1_1: {'\\': '\\', 'c': ':', 'n': '\n'},
        VERSION_1_2: {'\\': '\\', 'c': ':', 'n': '\n', 'r': '\r'}
    }
    COMMANDS_ESCAPE_EXCLUDED = {
        VERSION_1_0: COMMANDS[VERSION_1_0],
        VERSION_1_1: set([CONNECT, CONNECTED]),
        VERSION_1_2: set([CONNECT, CONNECTED])
    }

    FRAME_DELIMITER = '\x00'
    HEADER_SEPARATOR = ':'

    ACCEPT_VERSION_HEADER = u'accept-version'
    ACK_HEADER = u'ack'
    CONTENT_LENGTH_HEADER = u'content-length'
    CONTENT_TYPE_HEADER = u'content-type'
    DESTINATION_HEADER = u'destination'
    HEART_BEAT_HEADER = u'heart-beat'
    HOST_HEADER = u'host'
    ID_HEADER = u'id'
    LOGIN_HEADER = u'login'
    MESSAGE_ID_HEADER = u'message-id'
    PASSCODE_HEADER = u'passcode'
    RECEIPT_HEADER = u'receipt'
    RECEIPT_ID_HEADER = u'receipt-id'
    SELECTOR_HEADER = u'selector'
    SESSION_HEADER = u'session'
    SERVER_HEADER = u'server'
    SUBSCRIPTION_HEADER = u'subscription'
    TRANSACTION_HEADER = u'transaction'
    VERSION_HEADER = u'version'

    ACK_AUTO = u'auto'
    ACK_CLIENT = u'client'
    ACK_CLIENT_INDIVIDUAL = u'client-individual'
    CLIENT_ACK_MODES = set([ACK_CLIENT, ACK_CLIENT_INDIVIDUAL])

    HEART_BEAT_SEPARATOR = ','

    @classmethod
    def version(cls, version=None):
        """Check whether **version** is a valid STOMP protocol version.

        :param version: A candidate version, or :obj:`None` (which is equivalent to the value of :attr:`StompSpec.DEFAULT_VERSION`).
        """
        if version is None:
            version = cls.DEFAULT_VERSION
        if version not in cls.VERSIONS:
            raise StompProtocolError('Version is not supported [%s]' % version)
        return version

    @classmethod
    def versions(cls, version):
        """Obtain all versions prior or equal to **version**.
        """
        version = cls.version(version)
        for v in cls.VERSIONS:
            yield v
            if v == version:
                break

