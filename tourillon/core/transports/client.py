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

import asyncio
import logging
import ssl
from collections.abc import AsyncIterator

from tourillon.core.probe.detector import PeerFailureDetector
from tourlib.constant import ENVELOPE_RESPONSE_TIMEOUT
from tourlib.envelope import Envelope
from tourlib.exceptions import ResponseTimeoutError
from tourlib.transport import EnvelopeClient

PeerFailure = (TimeoutError, ConnectionError, ResponseTimeoutError, OSError, EOFError)


class PeerClient(EnvelopeClient):
    def __init__(
        self,
        bind: str,
        tls_ctx: ssl.SSLContext,
        fd: PeerFailureDetector,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(bind, tls_ctx, logger)
        self._fd = fd
        self._phi_task: asyncio.Task[None] | None = None

    async def request(
        self, envelope: Envelope, timeout: float = ENVELOPE_RESPONSE_TIMEOUT
    ) -> Envelope:
        try:
            return await super().request(envelope, timeout)
        except PeerFailure as exc:
            await self._fd.run(self)
            raise exc

    async def stream(
        self, envelope: Envelope, timeout: float = ENVELOPE_RESPONSE_TIMEOUT
    ) -> AsyncIterator[Envelope]:
        try:
            async for response in super().stream(envelope, timeout):
                yield response
        except PeerFailure as exc:
            await self._fd.run(self)
            raise exc
