# Copyright 2026 Tourillon Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Transport-layer port — ConnectionHandler Protocol, errors, and constants.

All infrastructure constants are defined here so that the core layer can
reference them without importing ssl, asyncio, or any third-party library.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from typing import Protocol

from tourillon.core.structure.envelope import Envelope

MAX_PAYLOAD_DEFAULT: int = 4 * 1024 * 1024  # 4 MiB — global per-envelope ceiling
MAX_IN_FLIGHT_PER_CONN: int = 128  # max distinct in-flight correlation_ids
RESPONSE_TIMEOUT: float = 30.0  # seconds — per-request client-side deadline
READ_TIMEOUT: float = 30.0  # seconds — per-envelope read deadline (both sides)


type ReceiveEnvelope = Callable[[], Awaitable[Envelope]]
type SendEnvelope = Callable[[Envelope], Awaitable[None]]


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


class ConnectionHandler(Protocol):
    """Handler for one round-trip (or streaming) envelope exchange.

    The Dispatcher calls __call__ for every incoming Envelope whose kind is
    registered. receive() returns the triggering Envelope; subsequent calls
    block until the next Envelope with the same correlation_id arrives (used
    by streaming handlers). send() writes an Envelope onto the connection.

    Handlers must never raise — unhandled exceptions close the connection
    silently. Application-level errors must be signalled by sending an
    error.* Envelope before returning.
    """

    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Process one request and emit response Envelope(s)."""
