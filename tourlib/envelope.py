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
import re
import struct
import uuid
from dataclasses import dataclass
from typing import ClassVar
from uuid import UUID

from tourlib.constant import (
    ENVELOPE_HEADER_LEN as _ENVELOPE_HEADER_LEN,
)
from tourlib.constant import (
    MAX_ENVELOPE_CONTEXT as _MAX_CONTEXT,
)
from tourlib.constant import (
    MAX_ENVELOPE_PAYLOAD as _MAX_PAYLOAD,
)
from tourlib.constant import (
    MAX_KIND_LEN as _MAX_KIND_LEN,
)
from tourlib.constant import (
    PROTOCOL_VERSION,
)
from tourlib.exceptions import ProtocolError


@dataclass(frozen=True)
class Envelope:
    """Binary transport envelope used by server and client runtimes.

    Envelope wire layout:

    - 32-byte fixed header
    - UTF-8 `kind` string
    - Opaque `context` bytes (0 to MAX_CONTEXT, usually empty)
    - Opaque `payload` bytes

    The context travels next to the payload but is not part of it: the
    transport carries it unread and the receiving side's telemetry decodes
    it. An envelope without context is exactly as large as before it existed.
    """

    proto_version: int
    schema_id: int
    mode: int
    correlation_id: UUID
    kind: str
    payload: bytes
    context: bytes = b""

    HEADER_LEN: ClassVar[int] = _ENVELOPE_HEADER_LEN
    MAX_PAYLOAD: ClassVar[int] = _MAX_PAYLOAD
    MAX_KIND_LEN: ClassVar[int] = _MAX_KIND_LEN
    MAX_CONTEXT: ClassVar[int] = _MAX_CONTEXT

    MODE_RPC: ClassVar[int] = 0
    MODE_FNF: ClassVar[int] = 1

    HEADER_STRUCT: ClassVar[struct.Struct] = struct.Struct(">B H B 16s I B B 6s")
    KIND_PATTERN: ClassVar[re.Pattern[str]] = re.compile(
        r"^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)+$"
    )

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        self._validate_scalars()
        self._validate_payload()
        self._validate_kind()

    def _validate_scalars(self) -> None:
        if not 0 <= self.proto_version <= 255:
            raise ValueError("proto_version must fit in one byte")
        if not 0 <= self.schema_id <= 65535:
            raise ValueError("schema_id must fit in two bytes")
        if self.mode not in (self.MODE_RPC, self.MODE_FNF):
            raise ValueError("mode must be 0 (RPC) or 1 (FNF)")
        if self.correlation_id.int == 0:
            raise ValueError("correlation_id must not be zero")

    def _validate_payload(self) -> None:
        if not isinstance(self.payload, bytes):
            raise TypeError("payload must be bytes")
        if len(self.payload) > self.MAX_PAYLOAD:
            raise ValueError("payload exceeds MAX_PAYLOAD")
        if not isinstance(self.context, bytes):
            raise TypeError("context must be bytes")
        if len(self.context) > self.MAX_CONTEXT:
            raise ValueError("context exceeds MAX_CONTEXT")

    def _validate_kind(self) -> None:
        kind_bytes = self.kind.encode("utf-8")
        kind_len = len(kind_bytes)
        if not 1 <= kind_len <= self.MAX_KIND_LEN:
            raise ValueError("kind length must be between 1 and MAX_KIND_LEN")
        if not self.KIND_PATTERN.fullmatch(self.kind):
            raise ValueError("kind must be lowercase and follow domain.action")

    def to_bytes(self) -> bytes:
        """Serialize an envelope into its wire representation."""
        kind_bytes = self.kind.encode("utf-8")
        header = self.HEADER_STRUCT.pack(
            self.proto_version,
            self.schema_id,
            self.mode,
            self.correlation_id.bytes,
            len(self.payload),
            len(kind_bytes),
            len(self.context),
            b"\x00" * 6,
        )
        return b"".join((header, kind_bytes, self.context, self.payload))

    @classmethod
    def from_bytes(cls, frame: bytes) -> "Envelope":
        """Parse and validate an envelope from raw bytes."""
        if len(frame) < cls.HEADER_LEN:
            raise ProtocolError("frame is shorter than envelope header")

        (
            proto_version,
            schema_id,
            mode,
            correlation_bytes,
            payload_len,
            kind_len,
            context_len,
            _reserved,
        ) = cls.HEADER_STRUCT.unpack_from(frame)

        _check_header(mode, kind_len, context_len, payload_len, cls.MAX_PAYLOAD)

        expected_len = cls.HEADER_LEN + kind_len + context_len + payload_len
        if len(frame) != expected_len:
            raise ProtocolError("frame length does not match envelope header")

        correlation_id = UUID(bytes=correlation_bytes)
        if correlation_id.int == 0:
            raise ProtocolError("correlation_id must not be zero")

        kind_start = cls.HEADER_LEN
        kind_end = kind_start + kind_len
        try:
            kind = frame[kind_start:kind_end].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError("kind is not valid UTF-8") from exc

        context_end = kind_end + context_len

        return cls(
            proto_version=proto_version,
            schema_id=schema_id,
            mode=mode,
            correlation_id=correlation_id,
            kind=kind,
            payload=frame[context_end:],
            context=frame[kind_end:context_end],
        )


def _check_header(
    mode: int, kind_len: int, context_len: int, payload_len: int, max_payload: int
) -> None:
    if mode not in (Envelope.MODE_RPC, Envelope.MODE_FNF):
        raise ProtocolError("unsupported mode")
    if not 1 <= kind_len <= Envelope.MAX_KIND_LEN:
        raise ProtocolError("invalid kind length")
    if context_len > Envelope.MAX_CONTEXT:
        raise ProtocolError("context exceeds MAX_CONTEXT")
    if payload_len > max_payload:
        raise ProtocolError("payload exceeds max_payload")


def build_envelope(
    kind: str,
    payload: bytes,
    schema_id: int,
    correlation_id: UUID | None = None,
    *,
    mode: int = Envelope.MODE_RPC,
    proto_version: int = PROTOCOL_VERSION,
) -> Envelope:
    return Envelope(
        kind=kind,
        payload=payload,
        mode=mode,
        schema_id=schema_id,
        correlation_id=correlation_id or uuid.uuid4(),
        proto_version=proto_version,
    )


async def read_envelope(
    reader: asyncio.StreamReader,
    *,
    max_payload: int = _MAX_PAYLOAD,
) -> Envelope:
    """Read and parse one envelope from *reader*.

    Raises:
        EOFError: connection closed cleanly before the header arrived.
        ProtocolError: malformed frame or payload exceeds *max_payload*.
    """
    try:
        header = await reader.readexactly(Envelope.HEADER_LEN)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            raise EOFError from exc
        raise ProtocolError("truncated envelope header") from exc

    _, _, mode, _, payload_len, kind_len, context_len, _ = (
        Envelope.HEADER_STRUCT.unpack(header)
    )

    _check_header(mode, kind_len, context_len, payload_len, max_payload)

    try:
        body = await reader.readexactly(kind_len + context_len + payload_len)
    except asyncio.IncompleteReadError as exc:
        raise ProtocolError("truncated envelope body") from exc

    return Envelope.from_bytes(header + body)


def replay_envelope(kind: str, request: Envelope, payload: bytes) -> Envelope:
    # The request's context is deliberately not echoed: a response belongs to
    # the caller's trace already.
    return Envelope(
        proto_version=request.proto_version,
        schema_id=request.schema_id,
        mode=request.mode,
        correlation_id=request.correlation_id,
        kind=kind,
        payload=payload,
    )
