import binascii
import unittest

from webstompest.protocol import StompFrame, StompSpec
import codecs

class StompFrameTest(unittest.TestCase):
    def test_frame(self):
        message = {'command': StompSpec.SEND, 'headers': {StompSpec.DESTINATION_HEADER: '/queue/world'}, 'body': 'two\nlines'}
        frame = StompFrame(**message)
        self.assertEquals(message['headers'], frame.headers)
        self.assertEquals(dict(frame), message)
        self.assertEquals(str(frame), """\
%s
%s:/queue/world

two
lines\x00""" % (StompSpec.SEND, StompSpec.DESTINATION_HEADER))
        self.assertEquals(eval(repr(frame)), frame)

    def test_frame_without_headers_and_body(self):
        message = {'command': StompSpec.DISCONNECT}
        frame = StompFrame(**message)
        self.assertEquals(frame.headers, {})
        self.assertEquals(dict(frame), message)
        self.assertEquals(str(frame), """\
%s

\x00""" % StompSpec.DISCONNECT)
        self.assertEquals(eval(repr(frame)), frame)

    def test_encoding(self):
        key = u'fen\xeatre'
        value = u'\xbfqu\xe9 tal?, s\xfc\xdf'
        command = StompSpec.DISCONNECT
        message = {'command': command, 'headers': {key: value}, 'version': StompSpec.VERSION_1_1}
        frame = StompFrame(**message)
        self.assertEquals(message['headers'], frame.headers)
        self.assertEquals(dict(frame), message)

        self.assertEquals(eval(repr(frame)), frame)
        frame.version = StompSpec.VERSION_1_1
        self.assertEquals(eval(repr(frame)), frame)
        self.assertEquals(str(frame), codecs.lookup('utf-8').encode(command + u'\n' + key + u':' + value + u'\n\n\x00')[0])

        otherFrame = StompFrame(**message)
        self.assertEquals(frame, otherFrame)

        frame.version = StompSpec.VERSION_1_0
        self.assertRaises(UnicodeEncodeError, frame.__str__)

    def test_binary_body(self):
        body = binascii.a2b_hex('f0000a09')
        headers = {'content-length': str(len(body))}
        frame = StompFrame('MESSAGE', headers, body)
        self.assertEquals(frame.body, body)
        self.assertEquals(str(frame), 'MESSAGE\ncontent-length:4\n\n\xf0\x00\n\t\x00')

    def test_duplicate_headers(self):
        rawHeaders = (('foo', 'bar1'), ('foo', 'bar2'))
        headers = dict(reversed(rawHeaders))
        message = {
            'command': 'SEND',
            'body': 'some stuff\nand more',
            'rawHeaders': rawHeaders
        }
        frame = StompFrame(**message)
        self.assertEquals(frame.headers, headers)
        self.assertEquals(frame.rawHeaders, rawHeaders)
        rawFrame = 'SEND\nfoo:bar1\nfoo:bar2\n\nsome stuff\nand more\x00'
        self.assertEquals(str(frame), rawFrame)

        frame.unraw()
        self.assertEquals(frame.headers, headers)
        self.assertEquals(frame.rawHeaders, None)
        rawFrame = 'SEND\nfoo:bar1\n\nsome stuff\nand more\x00'
        self.assertEquals(str(frame), rawFrame)

    def test_non_string_arguments(self):
        message = {'command': 0, 'headers': {123: 456}, 'body': 789}
        frame = StompFrame(**message)
        self.assertEquals(frame.command, 0)
        self.assertEquals(frame.headers, {123: 456})
        self.assertEquals(frame.body, 789)
        self.assertEquals(dict(frame), message)
        self.assertEquals(str(frame), """\
0
123:456

789\x00""")
        self.assertEquals(eval(repr(frame)), frame)

    def test_unescape(self):
        frameBytes = """%s
\\n\\\\:\\c\t\\n

\x00""" % StompSpec.DISCONNECT

        frame = StompFrame(command=StompSpec.DISCONNECT, headers={'\n\\': ':\t\n'}, version=StompSpec.VERSION_1_1)
        self.assertEquals(str(frame), frameBytes)

        frameBytes = """%s
\\n\\\\:\\c\t\\r

\x00""" % StompSpec.DISCONNECT

        frame = StompFrame(command=StompSpec.DISCONNECT, headers={'\n\\': ':\t\r'}, version=StompSpec.VERSION_1_2)
        self.assertEquals(str(frame), frameBytes)

        frameBytes = """%s
\\n\\\\:\\c\t\r

\x00""" % StompSpec.DISCONNECT

        frame = StompFrame(command=StompSpec.DISCONNECT, headers={'\n\\': ':\t\r'}, version=StompSpec.VERSION_1_1)
        self.assertEquals(str(frame), frameBytes)

        frameBytes = """%s

\\::\t\r


\x00""" % StompSpec.DISCONNECT

        frame = StompFrame(command=StompSpec.DISCONNECT, headers={'\n\\': ':\t\r\n'}, version=StompSpec.VERSION_1_0)
        self.assertEquals(str(frame), frameBytes)

        frameBytes = """%s

\\::\t\r


\x00""" % StompSpec.CONNECT

        frame = StompFrame(command=StompSpec.CONNECT, headers={'\n\\': ':\t\r\n'})
        for version in StompSpec.VERSIONS:
            frame.version = version
            self.assertEquals(str(frame), frameBytes)

if __name__ == '__main__':
    unittest.main()
