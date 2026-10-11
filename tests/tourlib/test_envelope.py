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
from uuid import UUID, uuid4

import pytest

from tests.helpers import make_envelope
from tourlib.constant import TRANSPORT_ERROR_KIND
from tourlib import ValidationError
from tourlib.envelope import Envelope, read_envelope
from tourlib.exceptions import ProtocolError


def test_envelope_round_trip() -> None:
    envelope = make_envelope(
        schema_id=42,
        kind="kv.put",
        payload=b"hello",
    )

    restored = Envelope.from_bytes(envelope.to_bytes())

    assert restored == envelope


def test_validation_error_is_shared_tourlib_exception() -> None:
    assert issubclass(ValidationError, ValueError)


def test_envelope_rejects_zero_correlation_id() -> None:
    with pytest.raises(ValueError, match="correlation_id"):
        Envelope(
            proto_version=1,
            schema_id=1,
            mode=Envelope.MODE_RPC,
            correlation_id=UUID(int=0),
            kind="kv.get",
            payload=b"",
        )


def test_envelope_rejects_invalid_kind_format() -> None:
    with pytest.raises(ValueError, match="domain.action"):
        Envelope(
            proto_version=1,
            schema_id=1,
            mode=Envelope.MODE_RPC,
            correlation_id=uuid4(),
            kind="KV_GET",
            payload=b"",
        )


def test_from_bytes_rejects_too_short_frame() -> None:
    with pytest.raises(ProtocolError, match="shorter than envelope header"):
        Envelope.from_bytes(b"short")


def test_from_bytes_rejects_invalid_mode() -> None:
    envelope = make_envelope(schema_id=10, kind="node.inspect", payload=b"payload")
    frame = bytearray(envelope.to_bytes())
    frame[3] = 2

    with pytest.raises(ProtocolError, match="unsupported mode"):
        Envelope.from_bytes(bytes(frame))


def test_from_bytes_rejects_bad_length_mismatch() -> None:
    envelope = make_envelope(schema_id=10, kind="gossip.ping", payload=b"payload")
    frame = envelope.to_bytes() + b"x"

    with pytest.raises(ProtocolError, match="frame length"):
        Envelope.from_bytes(frame)


@pytest.mark.parametrize(
    ("field", "value", "error_match"),
    [
        ("proto_version", -1, "proto_version"),
        ("schema_id", 70000, "schema_id"),
        ("mode", 9, "mode must be 0"),
    ],
)
def test_envelope_field_range_validation(
    field: str, value: int, error_match: str
) -> None:
    kwargs = {
        "proto_version": 1,
        "schema_id": 1,
        "mode": Envelope.MODE_RPC,
        "correlation_id": uuid4(),
        "kind": "kv.get",
        "payload": b"ok",
        field: value,
    }

    with pytest.raises(ValueError, match=error_match):
        Envelope(**kwargs)


def test_envelope_rejects_non_bytes_payload() -> None:
    with pytest.raises(TypeError, match="payload must be bytes"):
        Envelope(
            proto_version=1,
            schema_id=1,
            mode=Envelope.MODE_RPC,
            correlation_id=uuid4(),
            kind="kv.get",
            payload="not-bytes",  # type: ignore[arg-type]
        )


def test_envelope_rejects_too_large_payload() -> None:
    with pytest.raises(ValueError, match="payload exceeds MAX_PAYLOAD"):
        Envelope(
            proto_version=1,
            schema_id=1,
            mode=Envelope.MODE_RPC,
            correlation_id=uuid4(),
            kind="kv.get",
            payload=b"x" * (Envelope.MAX_PAYLOAD + 1),
        )


def test_envelope_rejects_too_long_kind() -> None:
    with pytest.raises(ValueError, match="kind length"):
        Envelope(
            proto_version=1,
            schema_id=1,
            mode=Envelope.MODE_RPC,
            correlation_id=uuid4(),
            kind="a." + ("b" * Envelope.MAX_KIND_LEN),
            payload=b"",
        )


def test_from_bytes_rejects_invalid_kind_length() -> None:
    envelope = make_envelope(schema_id=4, payload=b"x")
    frame = bytearray(envelope.to_bytes())
    frame[24] = 0

    with pytest.raises(ProtocolError, match="invalid kind length"):
        Envelope.from_bytes(bytes(frame))


def test_from_bytes_rejects_too_large_payload_header_value() -> None:
    envelope = make_envelope(schema_id=4, payload=b"x")
    frame = bytearray(envelope.to_bytes())
    frame[20:24] = (Envelope.MAX_PAYLOAD + 1).to_bytes(4, byteorder="big", signed=False)

    with pytest.raises(ProtocolError, match="payload exceeds MAX_PAYLOAD"):
        Envelope.from_bytes(bytes(frame))


def test_from_bytes_rejects_zero_correlation_id() -> None:
    envelope = make_envelope(schema_id=4, payload=b"x")
    frame = bytearray(envelope.to_bytes())
    frame[4:20] = b"\x00" * 16

    with pytest.raises(ProtocolError, match="correlation_id"):
        Envelope.from_bytes(bytes(frame))


def test_from_bytes_rejects_non_utf8_kind() -> None:
    envelope = make_envelope(schema_id=4, payload=b"x")
    frame = bytearray(envelope.to_bytes())
    frame[32] = 0xFF

    with pytest.raises(ProtocolError, match="valid UTF-8"):
        Envelope.from_bytes(bytes(frame))


@pytest.mark.asyncio
async def test_read_envelope_rejects_truncated_header() -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(b"x" * (Envelope.HEADER_LEN - 1))
    reader.feed_eof()

    with pytest.raises(ProtocolError, match="truncated envelope header"):
        await read_envelope(reader)


@pytest.mark.asyncio
async def test_read_envelope_rejects_invalid_kind_length_in_header() -> None:
    reader = asyncio.StreamReader()
    header = Envelope.HEADER_STRUCT.pack(
        1,
        1,
        Envelope.MODE_RPC,
        uuid4().bytes,
        0,
        0,
        b"\x00" * 7,
    )
    reader.feed_data(header)
    reader.feed_eof()

    with pytest.raises(ProtocolError, match="invalid kind length"):
        await read_envelope(reader)


@pytest.mark.asyncio
async def test_read_envelope_rejects_payload_exceeding_max_payload_argument() -> None:
    reader = asyncio.StreamReader()
    kind = b"kv.get"
    header = Envelope.HEADER_STRUCT.pack(
        1,
        1,
        Envelope.MODE_RPC,
        uuid4().bytes,
        2,
        len(kind),
        b"\x00" * 7,
    )
    reader.feed_data(header)
    reader.feed_eof()

    with pytest.raises(ProtocolError, match="payload exceeds max_payload"):
        await read_envelope(reader, max_payload=1)


@pytest.mark.asyncio
async def test_read_envelope_rejects_truncated_body() -> None:
    reader = asyncio.StreamReader()
    kind = b"kv.get"
    header = Envelope.HEADER_STRUCT.pack(
        1,
        1,
        Envelope.MODE_RPC,
        uuid4().bytes,
        3,
        len(kind),
        b"\x00" * 7,
    )
    reader.feed_data(header + kind)
    reader.feed_eof()

    with pytest.raises(ProtocolError, match="truncated envelope body"):
        await read_envelope(reader)


@pytest.mark.asyncio
async def test_read_envelope_returns_transport_error_envelope() -> None:
    reader = asyncio.StreamReader()
    envelope = make_envelope(
        kind=TRANSPORT_ERROR_KIND,
        payload=b"unknown kind: kv.get",
    )
    reader.feed_data(envelope.to_bytes())

    decoded = await read_envelope(reader)

    assert decoded.kind == TRANSPORT_ERROR_KIND
    assert decoded.payload == b"unknown kind: kv.get"


