import select
import socket
import time
import errno
import ws4py
import collections
import datetime

from webstompest.error import StompConnectionError
from webstompest.protocol import StompParser

class StompFrameTransport(object):
    factory = StompParser

    READ_SIZE = 4096

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self._socket = None
        self._parser = self.factory()

    def __str__(self):
        return '%s:%d' % (self.host, self.port)

    def canRead(self, timeout=None):
        self._check()
        if self._parser.canRead():
            return True

        startTime = time.time()
        try:
            if timeout is None:
                files, _, _ = select.select([self._socket], [], [])
            else:
                files, _, _ = select.select([self._socket], [], [], timeout)
        except select.error as (code, msg):
            if code == errno.EINTR:
                if timeout is None:
                    return self.canRead()
                else:
                    return self.canRead(max(0, timeout - (time.time() - startTime)))
            raise
        return bool(files)

    def connect(self, timeout=None):
        kwargs = {} if (timeout is None) else {'timeout': timeout}
        try:
            self._socket = socket.create_connection((self.host, self.port), **kwargs)
        except IOError as e:
            raise StompConnectionError('Could not establish connection [%s]' % e)
        self._parser.reset()

    def disconnect(self):
        try:
            self._socket and self._socket.close()
        except IOError as e:
            raise StompConnectionError('Could not close connection cleanly [%s]' % e)
        finally:
            self._socket = None

    def receive(self):
        while True:
            frame = self._parser.get()
            if frame is not None:
                return frame
            try:
                data = self._socket.recv(self.READ_SIZE)
                if not data:
                    raise StompConnectionError('No more data')
            except (IOError, StompConnectionError) as e:
                self.disconnect()
                raise StompConnectionError('Connection closed [%s]' % e)
            self._parser.add(data)

    def send(self, frame):
        self._write(str(frame))

    def setVersion(self, version):
        self._parser.version = version

    def _check(self):
        if not self._connected():
            raise StompConnectionError('Not connected')

    def _connected(self):
        return self._socket is not None

    def _write(self, data):
        self._check()
        try:
            self._socket.sendall(data)
        except IOError as e:
            raise StompConnectionError('Could not send to connection [%s]' % e)


class StompWebSocketClient(ws4py.websocket.client.WebSocketBaseClient):

    def __init__(self, url, heartbeat_freq=3, ssl_options=None, headers=None):
        self.opened = False
        self.closed = True
        self.alive = False
        self.pong_latest = None
        self.pong_interval = heartbeat_freq
        self.message_store = collections.deque()
        ws4py.websocket.client.WebSocketBaseClient.__init__(
            self, url, heartbeat_freq=self.pong_interval,
            ssl_options=ssl_options, header=headers
            )

    def opened(self):
        self.ponged()

    def ponged(self):
        self.alive = True
        self.closed = False
        self.opened = True
        self.pong_latest = datetime.datetime.now()

    def closed(self):
        self.closed = True

    def received_message(self, message):
        self.message_store.append(message)

    def message_count(self):
        return len(self.message_store)

    def message(self):
        try:
            return self.message_store.popleft()
        except:
            return None

    def am_i_alive(self):
        if self.pong_latest:
            t = datetime.datetime.now() - self.pong_latest
            if t.seconds() > self.pong_interval:
                self.alive = False
        return self.alive


class StompFrameOverWebSocketTransport(object):
    factory = StompParser
    wsc = StompWebSocketClient

    READ_SIZE = 4096

    def __init__(self, host, port, path='/', protocol='wss'):
        self.host = host
        self.port = port
        self.path = path
        self.protocol = protocol
        self.client = self.wsc("{}://{}:{}".format(self.protocol, self.host, self.port))
        self.client.resource = self.path
        self._socket = None
        self._parser = self.factory()

    def __str__(self):
        return '%s://%s:%d%s' % (self.protocol, self.host, self.port, self.path)

    def canRead(self, timeout=None):
        self._check()
        if self._parser.canRead():
            return True

        startTime = time.time()
        try:
            if timeout is None:
                files, _, _ = select.select([self._socket], [], [])
            else:
                files, _, _ = select.select([self._socket], [], [], timeout)
        except select.error as (code, msg):
            if code == errno.EINTR:
                if timeout is None:
                    return self.canRead()
                else:
                    return self.canRead(max(0, timeout - (time.time() - startTime)))
            raise
        return bool(files)

    def connect(self, timeout=None):
        # kwargs = {} if (timeout is None) else {'timeout': timeout}
        try:
            self.client.connect()
            # self._socket = socket.create_connection((self.host, self.port), **kwargs)
        except:
            raise StompConnectionError('Could not establish connection [%s]' % self.__str__())
        self._parser.reset()

    def disconnect(self, message="bye bye"):
        try:
            self.client and self.client.close(reason=message)
        except:
            raise StompConnectionError('Could not close connection cleanly [%s]' % self.__str__())
        finally:
            self.client = None

    def receive(self):
        while True:
            frame = self._parser.get()
            if frame is not None:
                return frame
            try:
                data = self.client.message()
                if not data:
                    raise StompConnectionError('No more data')
            except (IOError, StompConnectionError) as e:
                self.disconnect()
                raise StompConnectionError('Connection closed [%s]' % e)
            self._parser.add(data)

    def send(self, frame):
        self._write(str(frame))

    def send_binary(self, frame):
        self._write(frame, binary=True)

    def setVersion(self, version):
        self._parser.version = version

    def _check(self):
        if not self._connected():
            raise StompConnectionError('Not connected')

    def _connected(self):
        return self.client.am_i_alive()

    def _write(self, data, binary=False):
        self._check()
        try:
            self.client.send(data, binary=binary)
        except IOError as e:
            raise StompConnectionError('Could not send to connection [%s]' % e)
