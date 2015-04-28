class StompError(Exception):
    """Base class for STOMP errors."""

class StompFrameError(StompError):
    """Raised for error parsing STOMP frames."""

class StompProtocolError(StompError):
    """Raised for STOMP protocol errors."""

class StompConnectionError(StompError):
    """Raised for nonexistent connection."""

class StompConnectTimeout(StompConnectionError):
    """Raised for timeout waiting for connect response from broker."""

class StompExclusiveOperationError(StompError):
    """Raised for in-flight exclusive operation errors."""

class StompAlreadyRunningError(StompExclusiveOperationError, KeyError):
    """Raised when an in-flight exclusive operation is called more than once."""

class StompNotRunningError(StompExclusiveOperationError, KeyError):
    """Raised when a non-running exclusive operation is accessed."""

class StompCancelledError(StompExclusiveOperationError):
    """Raised when an in-flight exclusive operation was cancelled."""
