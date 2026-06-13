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

from collections.abc import Awaitable, Callable
from typing import Protocol

from tourlib.envelope import Envelope

type ReceiveEnvelope = Callable[[], Awaitable[Envelope]]
type SendEnvelope = Callable[[Envelope], Awaitable[None]]


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
