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
"""Envelope wire framing — async stream reader and protocol-error detection.

All multi-byte integer fields are big-endian as specified in the proposal.
The fixed header is always 24 bytes:

  offset   bytes   field
  0        1       proto_version   uint8
  1        2       schema_id       uint16
  3        16      correlation_id  bytes[16]  UUID v4 raw
  19       4       payload_len     uint32
  23       1       kind_len        uint8

Reading strategy: always read the full 24-byte header before any validation
so that correlation_id is always available for error responses.
"""

from __future__ import annotations

import asyncio
import struct
import uuid
from typing import Any

from tourlib.envelope import (
    HEADER_FMT,
    HEADER_SIZE,
    KIND_MAX_LEN,
    PROTO_VERSION,
    Envelope,
)
from tourlib.exceptions import ProtocolError
from tourlib.ports.serializer import Serializer

# Total fixed-header size on the wire including the kind_len byte.
_WIRE_HEADER_SIZE: int = HEADER_SIZE + 1  # 24 bytes

MAX_PAYLOAD_DEFAULT: int = 4 * 1024 * 1024  # 4 MiB — global per-envelope ceiling
MAX_IN_FLIGHT_PER_CONN: int = 128  # max distinct in-flight correlation_ids
RESPONSE_TIMEOUT: float = 30.0  # seconds — per-request client-side deadline
READ_TIMEOUT: float = 30.0  # seconds — per-envelope read deadline (both sides)


async def read_envelope(
    reader: asyncio.StreamReader,
    max_payload: int = MAX_PAYLOAD_DEFAULT,
) -> Envelope:
    """Read and validate one Envelope from *reader*.

    Wait indefinitely for the first byte of the header (idle connection is
    legitimate). Once bytes start arriving, enforce READ_TIMEOUT to receive
    the complete envelope (remaining header bytes + kind + payload). This
    matches the proposal semantics: READ_TIMEOUT is a per-read deadline
    *once at least one byte has arrived*, not an idle-connection deadline.

    Raise ProtocolError with the appropriate error_kind if the received
    header violates any constraint. Raise asyncio.IncompleteReadError if
    the connection closes mid-read.
    """
    # Phase 1 — wait for the envelope to start; no deadline (idle is fine).
    first_byte = await reader.readexactly(1)

    # Phase 2 — bytes are flowing; enforce READ_TIMEOUT for the rest.
    async with asyncio.timeout(READ_TIMEOUT):
        rest = await reader.readexactly(_WIRE_HEADER_SIZE - 1)

    raw_header = first_byte + rest
    proto_version, schema_id, corr_id_bytes, payload_len = struct.unpack_from(
        HEADER_FMT, raw_header
    )
    kind_len = raw_header[HEADER_SIZE]
    correlation_id = uuid.UUID(bytes=corr_id_bytes)

    if proto_version != PROTO_VERSION:
        raise ProtocolError("error.proto_version_unsupported", correlation_id)

    if not (1 <= kind_len <= KIND_MAX_LEN):
        raise ProtocolError("error.kind_len_invalid", correlation_id)

    if payload_len > max_payload:
        raise ProtocolError("error.payload_too_large", correlation_id)

    async with asyncio.timeout(READ_TIMEOUT):
        kind_bytes = await reader.readexactly(kind_len)
        payload = await reader.readexactly(payload_len)

    return Envelope(
        kind=kind_bytes.decode("utf-8"),
        payload=payload,
        correlation_id=correlation_id,
        schema_id=schema_id,
        proto_version=proto_version,
    )


def replay_envelope(
    kind: str,
    payload: dict[str, Any],
    serializer: Serializer,
    correlation_id: uuid.UUID | None = None,
) -> Envelope:
    envelope = Envelope(
        kind=kind,
        payload=serializer.encode(payload),
        correlation_id=correlation_id or uuid.uuid4(),
        schema_id=serializer.schema_id
    )
    return envelope
