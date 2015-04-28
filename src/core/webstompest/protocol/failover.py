import collections
import random
import re
import socket
from urlparse import urlparse

from webstompest.error import StompConnectTimeout


class StompFailoverTransport(object):
    """Looping over this object, you can produce a series of tuples (broker, delay in s). When the failover scheme
    does not allow further failover, a :class:`~.error.StompConnectTimeout` error is raised.

    :param uri: A failover URI.

    **Example:**

    >>> from webstompest.protocol import StompFailoverTransport
    >>> from webstompest.error import StompConnectTimeout
    >>> failover = StompFailoverTransport('failover:(tcp://remote1:61615,tcp://localhost:61616)?randomize=false,
    startupMaxReconnectAttempts=3,initialReconnectDelay=7,maxReconnectDelay=8,maxReconnectAttempts=0')
    >>> try:
    ...     for (broker, delay) in failover:
    ...         print 'broker: %s, delay: %f' % (broker, delay)
    ... except StompConnectTimeout as e:
    ...     print 'timeout: %s' % e
    ...
    broker: {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}, delay: 0.000000
    broker: {'host': 'localhost', 'protocol': 'tcp', 'port': 61616}, delay: 0.007000
    broker: {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}, delay: 0.008000
    broker: {'host': 'localhost', 'protocol': 'tcp', 'port': 61616}, delay: 0.008000
    timeout: Reconnect timeout: 3 attempts
    >>> try:
    ...     for (broker, delay) in failover:
    ...         print 'broker: %s, delay: %f' % (broker, delay)
    ... except StompConnectTimeout as e:
    ...     print 'timeout: %s' % e
    ...
    broker: {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}, delay: 0.000000
    timeout: Reconnect timeout: 0 attempts

    .. seealso :: The :class:`StompFailoverUri` which parses failover transport URIs.
    """
    _REGEX_LOCALHOST_IPV4 = re.compile('^127\.\d+\.\d+\.\d+$')

    def __init__(self, uri):
        self._failoverUri = StompFailoverUri(uri)
        self._maxReconnectAttempts = None

    def __iter__(self):
        self._reset()
        while True:
            for broker in self._brokers():
                yield broker, self._delay()

    @classmethod
    def isLocalHost(cls, host):
        if host == 'localhost' or cls._REGEX_LOCALHOST_IPV4.match(host):
            return True
        hostName = socket.gethostname()
        for alternative in (
                lambda h: h,
                socket.gethostbyname,
                socket.getfqdn
        ):
            try:
                if host == alternative(hostName):
                    return True
            except socket.gaierror:
                pass
        return False

    def _brokers(self):
        failoverUri = self._failoverUri
        options = failoverUri.options
        brokers = list(failoverUri.brokers)
        if options['randomize']:
            random.shuffle(brokers)
        if options['priorityBackup']:
            brokers.sort(key=lambda b: self.isLocalHost(b['host']), reverse=True)
        return brokers

    def _delay(self):
        options = self._failoverUri.options
        self._reconnectAttempts += 1
        if self._reconnectAttempts == 0:
            return 0
        if (self._maxReconnectAttempts != -1) and (self._reconnectAttempts > self._maxReconnectAttempts):
            raise StompConnectTimeout('Reconnect timeout: %d attempts' % self._maxReconnectAttempts)
        delay = max(0, (
            min(self._reconnectDelay + (random.random() * options['reconnectDelayJitter']),
                options['maxReconnectDelay'])))
        self._reconnectDelay *= (options['backOffMultiplier'] if options['useExponentialBackOff'] else 1)
        return delay / 1000.0

    def _reset(self):
        options = self._failoverUri.options
        self._reconnectDelay = options['initialReconnectDelay']
        if self._maxReconnectAttempts is None:
            self._maxReconnectAttempts = options['startupMaxReconnectAttempts']
        else:
            self._maxReconnectAttempts = options['maxReconnectAttempts']
        self._reconnectAttempts = -1


class StompFailoverUri(object):
    """This is a parser for the failover URI scheme used in webstompest. The parsed parameters are available in the
    attributes :attr:`brokers` and :attr:`options`. The Failover transport syntax is very close to the one used in
    ActiveMQ.

    :param uri: A failover URI. Its basic form is::

        'failover:(uri1,...,uriN)?transportOptions'

        or::

        'failover:uri1,...,uriN'

    **Example:**

    >>> from webstompest.protocol import StompFailoverUri
    >>> uri = StompFailoverUri('failover:(tcp://remote1:61615,tcp://localhost:61616)?randomize=false,
    startupMaxReconnectAttempts=3,initialReconnectDelay=7,maxReconnectDelay=8,maxReconnectAttempts=0')
    >>> print uri.brokers
    [{'host': 'remote1', 'protocol': 'tcp', 'port': 61615}, {'host': 'localhost', 'protocol': 'tcp', 'port': 61616}]
    >>> print uri.options
    {'initialReconnectDelay': 7, 'maxReconnectDelay': 8, 'backOffMultiplier': 2.0, 'startupMaxReconnectAttempts': 3,
    'priorityBackup': False, 'maxReconnectAttempts': 0, 'reconnectDelayJitter': 0, 'useExponentialBackOff': True,
    'randomize': False}

    **Supported Options:**

    =============================  ========= =============
    ================================================================
    option                         type      default       description
    =============================  ========= =============
    ================================================================
    *initialReconnectDelay*        int       :obj:`10`     how long to wait before the first reconnect attempt (in ms)
    *maxReconnectDelay*            int       :obj:`30000`  the maximum amount of time we ever wait between reconnect
    attempts (in ms)
    *useExponentialBackOff*        bool      :obj:`True`   should an exponential backoff be used between reconnect
    attempts
    *backOffMultiplier*            float     :obj:`2.0`    the exponent used in the exponential backoff attempts
    *maxReconnectAttempts*         int       :obj:`-1`     :obj:`-1` means retry forever
                                                           :obj:`0` means don't retry (only try connection once but
                                                           no retry)
                                                           :obj:`> 0` means the maximum number of reconnect attempts
                                                           before an error is sent back to the client
    *startupMaxReconnectAttempts*  int       :obj:`0`      if not :obj:`0`, then this is the maximum number of
    reconnect attempts before an error is sent back to the client on the first attempt by the client to start a
    connection, once connected the *maxReconnectAttempts* option takes precedence
    *reconnectDelayJitter*         int       :obj:`0`      jitter in ms by which reconnect delay is blurred in order
    to avoid stampeding
    *randomize*                    bool      :obj:`True`   use a random algorithm to choose the the URI to use for
    reconnect from the list provided
    *priorityBackup*               bool      :obj:`False`  if set, prefer local connections to remote connections
    =============================  ========= =============
    ================================================================

    .. seealso :: :class:`StompFailoverTransport`, `failover transport
    <http://activemq.apache.org/failover-transport-reference.html>`_ of ActiveMQ.
    """
    _configurationOption = collections.namedtuple('_configurationOption', ['parser', 'default'])
    _bool = {'true': True, 'false': False}.__getitem__

    _FAILOVER_PREFIX = 'failover:'
    _REGEX_URI = re.compile('^(?P<protocol>tcp)://(?P<host>[^:]+):(?P<port>\d+)$')
    _REGEX_BRACKETS = re.compile('^\((?P<uri>.+)\)$')
    _SUPPORTED_OPTIONS = {
        'initialReconnectDelay': _configurationOption(int, 10),
        'maxReconnectDelay': _configurationOption(int, 30000),
        'useExponentialBackOff': _configurationOption(_bool, True),
        'backOffMultiplier': _configurationOption(float, 2.0),
        'maxReconnectAttempts': _configurationOption(int, -1),
        'startupMaxReconnectAttempts': _configurationOption(int, 0),
        'reconnectDelayJitter': _configurationOption(int, 0),
        'randomize': _configurationOption(_bool, True),
        'priorityBackup': _configurationOption(_bool, False)
        # 'backup': _configurationOption(_bool, False),
        # initialize and hold a second transport connection - to
        # enable fast failover
        # 'timeout': _configurationOption(int, -1), # enables timeout on send operations (in miliseconds) without
        # interruption of reconnection process
        # 'trackMessages': _configurationOption(_bool, False), # keep a cache of in-flight messages that will
        # flushed to a broker on reconnect
        # 'maxCacheSize': _configurationOption(int, 131072), # size in bytes for the cache, if trackMessages is enabled
        # 'updateURIsSupported': _configurationOption(_bool, True), # determines whether the client should accept
        # updates to its list of known URIs from the connected broker
    }

    def __init__(self, uri):
        self._parse(uri)

    def __repr__(self):
        return "StompFailoverUri('%s')" % self.uri

    def __str__(self):
        return self.uri

    def _parse(self, uri):
        self.uri = uri
        try:
            (uri, _, options) = uri.partition('?')
            if uri.startswith(self._FAILOVER_PREFIX):
                (_, _, uri) = uri.partition(self._FAILOVER_PREFIX)
            try:
                self._setOptions(options)
            except Exception, msg:
                raise ValueError('invalid options: %s' % msg)

            try:
                self._setBrokers(uri)
            except:
                raise
                # except Exception, msg:
                #    raise ValueError('invalid broker(s): %s' % msg)

        except ValueError, msg:
            raise ValueError('invalid uri: %s [%s]' % (self.uri, msg))

    def _setBrokers(self, uri):
        brackets = self._REGEX_BRACKETS.match(uri)
        uri = brackets.groupdict()['uri'] if brackets else uri
        brokers = [urlparse(u) for u in uri.split(',')]
        mybrokers = []
        for broker in brokers:
            print broker
            host, _, port = broker.netloc.partition(':')
            mybroker = dict(host=host, protocol=broker.scheme)
            if broker.scheme == 'ws':
                mybroker['port'] = int(port) or 80
                mybroker['path'] = broker.path or '/'
            elif broker.scheme == 'wss':
                mybroker['port'] = int(port) or 443
                mybroker['path'] = broker.path or '/'
            else:
                mybroker['port'] = int(port) or 443
            mybrokers.append(mybroker)
        self.brokers = mybrokers

    def _setOptions(self, options=None):
        _options = dict((k, o.default) for (k, o) in self._SUPPORTED_OPTIONS.iteritems())
        if options:
            _options.update((k, self._SUPPORTED_OPTIONS[k].parser(v)) for (k, _, v) in
                            (o.partition('=') for o in options.split(',')))
        self.options = _options


if __name__ == '__main__':
    uri_str = 'failover:(tcp://remote1:61615,ws://localhost:61616/asd/as)?randomize=false,' \
              'startupMaxReconnectAttempts=3,initialReconnectDelay=7,maxReconnectDelay=8,maxReconnectAttempts=0'
    uri = StompFailoverUri(uri_str)
    print uri.brokers
    failover = StompFailoverTransport(uri_str)
    try:
        for (broker, delay) in failover:
            print 'broker: %s, delay: %f' % (broker, delay)
    except:
        pass
