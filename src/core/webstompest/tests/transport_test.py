import binascii
import itertools
import logging
import select
import unittest

from mock import Mock
from mock import patch

from webstompest.error import StompConnectionError
from webstompest.protocol import StompFrame, StompSpec
from webstompest.sync.transport import StompFrameTransport

logging.basicConfig(level=logging.DEBUG)

HOST = 'fakeHost'
PORT = 61613

class StompFrameTransportTest(unittest.TestCase):
    def _generate_bytes(self, stream):
        for byte in stream:
            yield byte
        while True:
            yield ''

    def _get_receive_mock(self, stream):
        transport = StompFrameTransport(HOST, PORT)
        connected = transport._connected = Mock()
        connected.return_value = True
        socket = transport._socket = Mock()
        stream = self._generate_bytes(stream)
        socket.recv = Mock(wraps=lambda size: ''.join(itertools.islice(stream, size)))
        return transport

    def _get_send_mock(self):
        transport = StompFrameTransport(HOST, PORT)
        connected = transport._connected = Mock()
        connected.return_value = True
        transport._socket = Mock()
        return transport

    def test_send(self):
        frame = StompFrame(StompSpec.MESSAGE)

        transport = self._get_send_mock()
        transport.send(frame)
        self.assertEquals(1, transport._socket.sendall.call_count)
        args, _ = transport._socket.sendall.call_args
        self.assertEquals(str(frame), args[0])

    def test_send_not_connected_raises(self):
        frame = StompFrame(StompSpec.MESSAGE)

        transport = self._get_send_mock()
        transport._connected.return_value = False
        self.assertRaises(StompConnectionError, transport.send, frame)
        self.assertEquals(0, transport._socket.sendall.call_count)

    def test_receive(self):
        headers = {'x': 'y'}
        body = 'testing 1 2 3'
        frame = StompFrame(StompSpec.MESSAGE, headers, body)

        transport = self._get_receive_mock(str(frame))
        frame_ = transport.receive()
        self.assertEquals(frame, frame_)
        self.assertEquals(1, transport._socket.recv.call_count)

        self.assertRaises(StompConnectionError, transport.receive)
        self.assertEquals(transport._socket, None)

    def test_receive_not_connected_raises_and_removes_socket(self):
        transport = self._get_receive_mock('Hi')
        transport._connected.return_value = False
        self.assertRaises(StompConnectionError, transport.receive)
        self.assertEquals(None, transport._socket)

    def test_receive_multiple_frames_extra_newlines(self):
        headers = {'x': 'y'}
        body = 'testing 1 2 3'
        frame = StompFrame(StompSpec.MESSAGE, headers, body)

        transport = self._get_receive_mock('\n\n%s\n%s\n' % (frame, frame))
        frame_ = transport.receive()
        self.assertEquals(frame, frame_)
        frame_ = transport.receive()
        self.assertEquals(frame, frame_)
        self.assertEquals(1, transport._socket.recv.call_count)

        self.assertRaises(StompConnectionError, transport.receive)
        self.assertEquals(transport._socket, None)

    def test_receive_binary(self):
        body = binascii.a2b_hex('f0000a09')
        headers = {StompSpec.CONTENT_LENGTH_HEADER: str(len(body))}
        frame = StompFrame(StompSpec.MESSAGE, headers, body)

        transport = self._get_receive_mock(str(frame))
        frame_ = transport.receive()
        self.assertEquals(frame, frame_)
        self.assertEquals(1, transport._socket.recv.call_count)

        self.assertRaises(StompConnectionError, transport.receive)
        self.assertEquals(transport._socket, None)

    def test_receive_multiple_frames_per_read(self):
        body1 = 'boo'
        body2 = 'hoo'
        headers = {'x': 'y'}
        frameBytes = str(StompFrame(StompSpec.MESSAGE, headers, body1)) + str(StompFrame(StompSpec.MESSAGE, headers, body2))

        transport = self._get_receive_mock(frameBytes)

        frame = transport.receive()
        self.assertEquals(StompSpec.MESSAGE, frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body1, frame.body)
        self.assertEquals(1, transport._socket.recv.call_count)

        frame = transport.receive()
        self.assertEquals(StompSpec.MESSAGE, frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body2, frame.body)
        self.assertEquals(1, transport._socket.recv.call_count)

        self.assertRaises(StompConnectionError, transport.receive)
        self.assertEquals(transport._socket, None)

    @patch('select.select')
    def test_can_connect_eintr_retries_connection(self, select_call):
        select_call.return_value = (Mock(), Mock(), Mock())
        transport = self._get_receive_mock('test')
        def raise_eintr_once(*args):
            select_call.side_effect = None
            raise select.error(4, 'Interrupted system call')
        select_call.side_effect = raise_eintr_once

        transport.canRead()
        self.assertEquals(2, select_call.call_count)

if __name__ == '__main__':
    unittest.main()
