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

import struct
import uuid
from dataclasses import dataclass, field
from typing import Self

PROTO_VERSION: int = 1
KIND_MAX_LEN: int = 64
HEADER_FMT: str = "!BH16sI"
HEADER_SIZE: int = struct.calcsize(HEADER_FMT)


@dataclass(frozen=True)
class Envelope:
    kind: str
    payload: bytes
    correlation_id: uuid.UUID = field(default_factory=uuid.uuid4)
    schema_id: int = 1
    proto_version: int = PROTO_VERSION

    def __post_init__(self) -> None:
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
        return cls(
            kind=kind,
            payload=payload,
            correlation_id=correlation_id if correlation_id is not None else uuid.uuid4(),
            schema_id=schema_id,
        )

    def encode(self) -> bytes:
        kind_bytes = self.kind.encode("utf-8")
        header = struct.pack(
            HEADER_FMT,
            self.proto_version,
            self.schema_id,
            self.correlation_id.bytes,
            len(self.payload),
        )
        return header + bytes([len(kind_bytes)]) + kind_bytes + self.payload

    @classmethod
    def decode(cls, data: bytes) -> Self:
        min_size = HEADER_SIZE + 1
        if len(data) < min_size:
            raise ValueError(
                f"frame too short: got {len(data)} bytes, need at least {min_size}"
            )
        proto_version, schema_id, cid_bytes, payload_len = struct.unpack_from(
            HEADER_FMT, data
        )
        kind_len = data[HEADER_SIZE]
        if kind_len == 0:
            raise ValueError("kind_len is zero: kind must not be empty")
        if kind_len > KIND_MAX_LEN:
            raise ValueError(f"kind_len {kind_len} exceeds maximum of {KIND_MAX_LEN}")
        total_needed = HEADER_SIZE + 1 + kind_len + payload_len
        if len(data) < total_needed:
            raise ValueError(
                f"truncated frame: need {total_needed} bytes, got {len(data)}"
            )
        kind_start = HEADER_SIZE + 1
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
