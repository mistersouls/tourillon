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
from typing import Any
from uuid import uuid4

from tourlib import Serializer
from tourlib.constant import (
    ENVELOPE_CONNECTION_TIMEOUT,
    MAX_USER_ENVELOPE_PAYLOAD,
    PROTOCOL_VERSION,
)
from tourlib.envelope import Envelope
from tourlib.exceptions import ProtocolError
from tourlib.transport import EnvelopeClient


class Transport:
    """Envelope RPC transport backed by an asyncio TLS connection."""

    def __init__(
        self,
        bind: str,
        ssl_context: ssl.SSLContext,
        serializer: Serializer,
        proto_version: int = PROTOCOL_VERSION,
        max_payload: int = MAX_USER_ENVELOPE_PAYLOAD,
        connection_timeout: float = ENVELOPE_CONNECTION_TIMEOUT,
        logger: logging.Logger | None = None,
    ) -> None:
        self._serializer = serializer
        self._proto_version = proto_version
        self._max_payload = max_payload
        self._connection_timeout = connection_timeout
        self._client = EnvelopeClient(bind=bind, tls_ctx=ssl_context, logger=logger)

    async def connect(self) -> None:
        """Open the connection, failing with TimeoutError past the deadline.

        Without this bound an unreachable peer that accepts the TCP connection
        but never completes the TLS handshake would hang the caller forever.
        """
        async with asyncio.timeout(self._connection_timeout):
            await self._client.connect()

    async def close(self) -> None:
        await self._client.close()

    async def request(self, kind: str, document: Any) -> Envelope:
        payload = self._serializer.encode(document)
        if len(payload) > self._max_payload:
            raise ProtocolError(
                "Payload size exceeds max payload "
                f"({len(payload)} > {self._max_payload})"
            )

        envelope = Envelope(
            proto_version=self._proto_version,
            schema_id=self._serializer.schema_id,
            mode=Envelope.MODE_RPC,
            correlation_id=uuid4(),
            kind=kind,
            payload=payload,
        )
        return await self._client.request(envelope)
