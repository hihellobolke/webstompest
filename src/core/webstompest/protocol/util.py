import re

from .spec import StompSpec

def escape(version):
    return _HeadersEscaper.get(version)

def unescape(version):
    return _HeadersUnescaper.get(version)

class _HeadersTransformer(object):
    _ESCAPE_CHARACTER = StompSpec.ESCAPE_CHARACTER

    @classmethod
    def get(cls, version):
        try:
            return cls._INSTANCES[version]
        except KeyError:
            return cls._INSTANCES.setdefault(version, cls(version))

    def __init__(self, version):
        self.excludedCommands = StompSpec.COMMANDS_ESCAPE_EXCLUDED[version]
        self.escapeSequences = self._escapeSequences(version)
        self.regex = re.compile(self._regex)

    def __call__(self, command, text):
        if command in self.excludedCommands:
            return text
        return self.regex.sub(self._sub, text)

    def _escapedCharacters(self, version):
        return StompSpec.ESCAPED_CHARACTERS[version]

    def _sub(self, match):
        return self.escapeSequences[match.group(1)]

class _HeadersEscaper(_HeadersTransformer):
    _INSTANCES = {} # each class needs its own instance cache

    def _escapeSequences(self, version):
        return dict((escapeSequence, '%s%s' % (self._ESCAPE_CHARACTER, character)) for (character, escapeSequence) in self._escapedCharacters(version).iteritems())

    @property
    def _regex(self):
        return '(%s)' % '|'.join(map(re.escape, self.escapeSequences))

class _HeadersUnescaper(_HeadersTransformer):
    _INSTANCES = {} # each class needs its own instance cache

    def _escapeSequences(self, version):
        return self._escapedCharacters(version)

    @property
    def _regex(self):
        return '%s(.)' % re.escape(self._ESCAPE_CHARACTER)
