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
"""Wire-level Envelope framing constants and dataclass."""

from __future__ import annotations

import struct
import uuid
from dataclasses import dataclass, field
from typing import Self

PROTO_VERSION: int = 1
KIND_MAX_LEN: int = 64  # maximum byte length of the kind field (UTF-8)

# Fixed-header struct format (excludes the kind_len byte appended after):
#   proto_version uint8 | schema_id uint16 | correlation_id bytes[16] | payload_len uint32
_HEADER_FMT: str = "!BH16sI"
_HEADER_SIZE: int = struct.calcsize(_HEADER_FMT)  # 23 bytes; kind_len is byte 23


@dataclass(frozen=True)
class Envelope:
    """Wire-level message unit exchanged on every Tourillon TCP connection.

    Wire layout (all multi-byte fields big-endian):

     offset   bytes   field
     0        1       proto_version   uint8
     1        2       schema_id       uint16
     3        16      correlation_id  bytes[16]  (UUID v4 raw)
     19       4       payload_len     uint32
     23       1       kind_len        uint8
     24       N       kind            UTF-8 string
     24+N     M       payload         bytes

    The constructor validates that *kind* encodes to 1–KIND_MAX_LEN UTF-8
    bytes and raises ValueError immediately on violation, before any I/O.
    Unknown kind strings are a connection-close event on the receive path;
    they are not rejected here so that all structural envelopes (including
    error responses) can be constructed freely.
    """

    kind: str
    payload: bytes
    correlation_id: uuid.UUID = field(default_factory=uuid.uuid4)
    schema_id: int = 1  # 1 = MessagePack (default); 0 = raw bytes
    proto_version: int = PROTO_VERSION

    def __post_init__(self) -> None:
        """Validate kind byte length at construction time."""
        kind_bytes = self.kind.encode("utf-8")
        if not (1 <= len(kind_bytes) <= KIND_MAX_LEN):
            raise ValueError(
                f"kind must be 1–{KIND_MAX_LEN} UTF-8 bytes; got {len(kind_bytes)}"
            )

    @classmethod
    def create(
        cls,
        payload: bytes,
        *,
        kind: str,
        correlation_id: uuid.UUID | None = None,
        schema_id: int = 0,
    ) -> Self:
        """Construct an Envelope with an optional auto-generated correlation_id.

        Return a new Envelope whose correlation_id defaults to a fresh UUID v4
        when None is supplied.  schema_id defaults to 0 (raw bytes) for
        responses that carry no codec-encoded body.
        """
        return cls(
            kind=kind,
            payload=payload,
            correlation_id=(
                correlation_id if correlation_id is not None else uuid.uuid4()
            ),
            schema_id=schema_id,
        )

    def encode(self) -> bytes:
        """Serialise this envelope into its canonical wire representation."""
        kind_bytes = self.kind.encode("utf-8")
        header = struct.pack(
            _HEADER_FMT,
            self.proto_version,
            self.schema_id,
            self.correlation_id.bytes,
            len(self.payload),
        )
        return header + bytes([len(kind_bytes)]) + kind_bytes + self.payload

    @classmethod
    def decode(cls, data: bytes) -> Self:
        """Deserialise a wire frame back into an Envelope.

        Raise ValueError with a descriptive message when the frame is shorter
        than the minimum header, when kind_len is zero or exceeds KIND_MAX_LEN,
        or when the declared field lengths claim more bytes than are available.
        Trailing bytes beyond the declared payload are silently ignored so that
        forward-compatible extensions can append optional data without breaking
        older receivers.
        """
        min_size = _HEADER_SIZE + 1
        if len(data) < min_size:
            raise ValueError(
                f"frame too short: got {len(data)} bytes, need at least {min_size}"
            )
        proto_version, schema_id, cid_bytes, payload_len = struct.unpack_from(
            _HEADER_FMT, data
        )
        kind_len = data[_HEADER_SIZE]
        if kind_len == 0:
            raise ValueError("kind_len is zero: kind must not be empty")
        if kind_len > KIND_MAX_LEN:
            raise ValueError(f"kind_len {kind_len} exceeds maximum of {KIND_MAX_LEN}")
        total_needed = _HEADER_SIZE + 1 + kind_len + payload_len
        if len(data) < total_needed:
            raise ValueError(
                f"truncated frame: need {total_needed} bytes, got {len(data)}"
            )
        kind_start = _HEADER_SIZE + 1
        kind = data[kind_start : kind_start + kind_len].decode("utf-8")
        payload_start = kind_start + kind_len
        payload = data[payload_start : payload_start + payload_len]
        return cls(
            proto_version=proto_version,
            correlation_id=uuid.UUID(bytes=cid_bytes),
            schema_id=schema_id,
            kind=kind,
            payload=payload,
        )
