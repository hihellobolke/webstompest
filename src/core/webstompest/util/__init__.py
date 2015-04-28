import copy
import functools

from webstompest.protocol import StompSpec

_RESERVED_HEADERS = set([StompSpec.MESSAGE_ID_HEADER, StompSpec.DESTINATION_HEADER, u'timestamp', u'expires', u'priority'])

def filterReservedHeaders(headers):
    return dict((header, value) for (header, value) in headers.iteritems() if header not in _RESERVED_HEADERS)

def checkattr(attribute):
    def _checkattr(f):
        @functools.wraps(f)
        def __checkattr(self, *args, **kwargs):
            getattr(self, attribute)
            return f(self, *args, **kwargs)
        return __checkattr
    return _checkattr

def cloneFrame(frame, persistent=None):
    frame = copy.deepcopy(frame)
    frame.unraw()
    headers = filterReservedHeaders(frame.headers)
    if persistent is not None:
        headers[u'persistent'] = unicode(bool(persistent)).lower()
    frame.headers = headers
    return frame
