from __future__ import annotations

import uuid


class TransportError(Exception):
    """Base class for all transport-level errors.

    These exceptions are never sent across the wire. They signal connection
    state changes to the caller layer (TcpClient callers, handler tasks).
    """


class ResponseTimeoutError(TransportError):
    """Raised by TcpClient.request() or .stream() when RESPONSE_TIMEOUT expires.

    The underlying connection remains open. Other in-flight requests on the
    same connection are unaffected. The correlation_id is deregistered so
    that a late-arriving response is silently discarded.
    """


class ConnectionClosedError(TransportError):
    """Raised when the remote peer closes the connection before a response arrives.

    All pending request() and stream() calls on this TcpClient are failed with
    this exception as soon as the connection is detected closed.
    """

    def __init__(self, peer: str = "") -> None:
        super().__init__(
            f"Connection closed by peer: {peer}" if peer else "Connection closed"
        )
        self.peer = peer


class ProtocolError(Exception):
    """Raised by framing layer when a received envelope violates the protocol.

    Carries the error kind string (e.g. error.proto_version_unsupported) and
    the correlation_id so the server can send a matching error response before
    closing the connection.
    """

    def __init__(self, error_kind: str, correlation_id: uuid.UUID) -> None:
        super().__init__(error_kind)
        self.error_kind = error_kind
        self.correlation_id = correlation_id


class TlsValidationError(Exception):
    """Raised when TLS credentials (certificate, key, CA) fail validation."""


class ClosedError(RuntimeError):
    """Raised when a closeable is closed before a response arrives."""

