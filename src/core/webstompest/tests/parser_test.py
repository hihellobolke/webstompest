import binascii
import unittest

from webstompest.error import StompFrameError
from webstompest.protocol import commands, StompFrame, StompParser, StompSpec
from webstompest.protocol.frame import StompHeartBeat

class StompParserTest(unittest.TestCase):
    def _generate_bytes(self, stream):
        for byte in stream:
            yield byte
        while True:
            yield ''

    def test_frame_parse_succeeds(self):
        frame = StompFrame(
            StompSpec.SEND,
            {'foo': 'bar', 'hello ': 'there-world with space ', 'empty-value':'', '':'empty-header', StompSpec.DESTINATION_HEADER: '/queue/blah'},
            'some stuff\nand more'
        )

        parser = StompParser()
        parser.add(str(frame))
        self.assertEqual(parser.get(), frame)
        self.assertEqual(parser.get(), None)

    def test_duplicate_headers(self):
        command = StompSpec.SEND
        rawFrame = '%s\nfoo:bar1\nfoo:bar2\n\nsome stuff\nand more\x00' % (command,)

        parser = StompParser()
        parser.add(rawFrame)
        parsedFrame = parser.get()
        self.assertEquals(parser.get(), None)

        self.assertEquals(parsedFrame.command, command)
        self.assertEquals(parsedFrame.headers, {'foo': 'bar1'})
        self.assertEquals(parsedFrame.rawHeaders, [('foo', 'bar1'), ('foo', 'bar2')])
        self.assertEquals(parsedFrame.body, 'some stuff\nand more')

    def test_invalid_command(self):
        messages = ['RECEIPT\nreceipt-id:message-12345\n\n\x00', 'NACK\nsubscription:0\nmessage-id:007\n\n\x00']
        parser = StompParser('1.0')
        parser.add(messages[0])
        self.assertRaises(StompFrameError, parser.add, messages[1])
        self.assertEquals(parser.get(), StompFrame(StompSpec.RECEIPT, rawHeaders=((u'receipt-id', u'message-12345'),)))
        self.assertFalse(parser.canRead())
        self.assertEquals(parser.get(), None)
        parser = StompParser('1.1')
        parser.add(messages[1])
        self.assertEquals(parser.get(), StompFrame(command=u'NACK', rawHeaders=((u'subscription', u'0'), (u'message-id', u'007'))))

    def test_reset_succeeds(self):
        frame = StompFrame(
            command=StompSpec.SEND,
            headers={'foo': 'bar', 'hello ': 'there-world with space ', 'empty-value':'', '':'empty-header', StompSpec.DESTINATION_HEADER: '/queue/blah'},
            body='some stuff\nand more'
        )
        parser = StompParser()

        parser.add(str(frame))
        parser.reset()
        self.assertEqual(parser.get(), None)
        parser.add(str(frame)[:20])
        self.assertEqual(parser.get(), None)

    def test_frame_without_header_or_body_succeeds(self):
        parser = StompParser()
        parser.add(str(commands.disconnect()))
        self.assertEqual(parser.get(), commands.disconnect())

    def test_frames_with_optional_newlines_succeeds(self):
        parser = StompParser()
        disconnect = commands.disconnect()
        frame = '\n%s\n' % disconnect
        parser.add(2 * frame)
        for _ in xrange(2):
            self.assertEqual(parser.get(), disconnect)
        self.assertEqual(parser.get(), None)

    def test_frames_with_heart_beats_succeeds(self):
        parser = StompParser(version=StompSpec.VERSION_1_1)
        disconnect = commands.disconnect()
        frame = '\n%s\n' % disconnect
        parser.add(2 * frame)
        frames = []
        while parser.canRead():
            frames.append(parser.get())
        self.assertEquals(frames, [StompHeartBeat(), disconnect, StompHeartBeat(), StompHeartBeat(), disconnect, StompHeartBeat()])

        self.assertEqual(parser.get(), None)

    def test_get_returns_None_if_not_done(self):
        parser = StompParser()
        self.assertEqual(None, parser.get())
        parser.add(StompSpec.CONNECT)
        self.assertEqual(None, parser.get())

    def test_add_throws_FrameError_on_invalid_command(self):
        parser = StompParser()

        self.assertRaises(StompFrameError, parser.add, 'HELLO\n')
        self.assertFalse(parser.canRead())
        parser.add('%s\n\n\x00' % StompSpec.DISCONNECT)
        self.assertEquals(StompFrame(StompSpec.DISCONNECT), parser.get())
        self.assertFalse(parser.canRead())

    def test_add_throws_FrameError_on_header_line_missing_separator(self):
        parser = StompParser()
        parser.add('%s\n' % StompSpec.SEND)
        self.assertRaises(StompFrameError, parser.add, 'no separator\n')

    def test_colon_in_header_value(self):
        parser = StompParser()
        parser.add('%s\nheader:with:colon\n\n\x00' % StompSpec.DISCONNECT)
        self.assertEquals(parser.get().headers['header'], 'with:colon')

    def test_no_newline(self):
        headers = {'x': 'y'}
        body = 'testing 1 2 3'
        frameBytes = str(StompFrame(StompSpec.MESSAGE, headers, body))
        self.assertTrue(frameBytes.endswith('\x00'))
        parser = StompParser()
        parser.add(self._generate_bytes(frameBytes))
        frame = parser.get()
        self.assertEquals(StompSpec.MESSAGE, frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body, frame.body)
        self.assertEquals(parser.get(), None)

    def test_binary_body(self):
        body = binascii.a2b_hex('f0000a09')
        headers = {'content-length': str(len(body))}
        frameBytes = str(StompFrame(StompSpec.MESSAGE, headers, body))
        self.assertTrue(frameBytes.endswith('\x00'))
        parser = StompParser()
        parser.add(frameBytes)
        frame = parser.get()
        self.assertEquals(StompSpec.MESSAGE, frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body, frame.body)

        self.assertEquals(parser.get(), None)

    def test_body_allowed_commands(self):
        head = str(commands.disconnect()).rstrip(StompSpec.FRAME_DELIMITER)
        for (version, bodyAllowed) in [
            (StompSpec.VERSION_1_0, True),
            (StompSpec.VERSION_1_1, False),
            (StompSpec.VERSION_1_2, False)
        ]:
            parser = StompParser(version)
            parser.add(head)
            parser.add('ouch!')
            try:
                parser.add(StompSpec.FRAME_DELIMITER)
            except StompFrameError:
                if bodyAllowed:
                    raise
            except:
                raise
            else:
                if not bodyAllowed:
                    raise

    def test_strip_line_delimiter(self):
        queue = '/queue/test'
        frame = commands.send(queue)
        rawFrameReplaced = str(commands.send(queue)).replace('\n', '\r\n')
        for (version, replace) in [
            (StompSpec.VERSION_1_0, False),
            (StompSpec.VERSION_1_1, False),
            (StompSpec.VERSION_1_2, True)
        ]:
            if replace:
                parser = StompParser(version)
                parser.add(rawFrameReplaced)
                self.assertEquals(parser.get(), frame)
            else:
                self.assertRaises(StompFrameError, StompParser(version).add, rawFrameReplaced)
        textWithCarriageReturn = 'there\rfolks'
        frame = commands.send(queue, headers={'hi': textWithCarriageReturn})
        parser = StompParser(StompSpec.VERSION_1_2)
        parser.add(str(frame))
        self.assertEquals(parser.get().headers['hi'], textWithCarriageReturn)

    def test_add_multiple_frames_per_read(self):
        body1 = 'boo'
        body2 = 'hoo'
        headers = {'x': 'y'}
        frameBytes = str(StompFrame(StompSpec.MESSAGE, headers, body1)) + str(StompFrame(StompSpec.MESSAGE, headers, body2))
        parser = StompParser()
        parser.add(frameBytes)

        frame = parser.get()
        self.assertEquals(StompSpec.MESSAGE, frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body1, frame.body)

        frame = parser.get()
        self.assertEquals(StompSpec.MESSAGE, frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body2, frame.body)

        self.assertEquals(parser.get(), None)

    def test_decode(self):
        headers = {u'fen\xeatre': u'\xbfqu\xe9 tal?, s\xfc\xdf'}
        frameBytes = str(StompFrame(command=StompSpec.DISCONNECT, headers=headers, version=StompSpec.VERSION_1_1))

        parser = StompParser(version=StompSpec.VERSION_1_1)
        parser.add(frameBytes)
        frame = parser.get()
        self.assertEquals(frame.headers, headers)

        parser = StompParser(version=StompSpec.VERSION_1_0)
        self.assertRaises(UnicodeDecodeError, parser.add, frameBytes)

    def test_unescape(self):
        frameBytes = """%s
\\n\\\\:\\c\t\\n

\x00""" % StompSpec.DISCONNECT

        for version in (StompSpec.VERSION_1_1, StompSpec.VERSION_1_2):
            parser = StompParser(version=version)
            parser.add(frameBytes)
            frame = parser.get()
            self.assertEquals(frame.headers, {'\n\\': ':\t\n'})

        parser = StompParser(version=StompSpec.VERSION_1_0)
        parser.add(frameBytes)
        frame = parser.get()
        self.assertEquals(frame.headers, {'\\n\\\\': '\\c\t\\n'})

        frameBytes = """%s
\\n\\\\:\\c\\t

\x00""" % StompSpec.DISCONNECT

        for version in (StompSpec.VERSION_1_1, StompSpec.VERSION_1_2):
            self.assertRaises(StompFrameError, StompParser(version=version).add, frameBytes)

        parser = StompParser(version=StompSpec.VERSION_1_0)
        parser.add(frameBytes)
        frame = parser.get()
        self.assertEquals(frame.headers, {'\\n\\\\': '\\c\\t'})

        frameBytes = """%s
\\n\\\\:\\c\t\\r

\x00""" % StompSpec.DISCONNECT

        parser = StompParser(version=StompSpec.VERSION_1_2)
        parser.add(frameBytes)
        frame = parser.get()
        self.assertEquals(frame.headers, {'\n\\': ':\t\r'})

    def test_keep_first_of_repeated_headers(self):
        parser = StompParser()
        parser.add("""
%s
repeat:1
repeat:2

\x00""" % StompSpec.CONNECT)
        frame = parser.get()
        self.assertEquals(frame.headers['repeat'], '1')

if __name__ == '__main__':
    unittest.main()
