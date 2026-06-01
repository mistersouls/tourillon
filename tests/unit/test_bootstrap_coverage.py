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
"""Additional coverage tests for bootstrap components."""

import uuid

import pytest

from tourillon.bootstrap.config import parse_bytes, parse_duration
from tourillon.core.structure.envelope import Envelope
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter


@pytest.mark.bootstrap
class TestEnvelopeEncoding:
    """Additional tests to improve Envelope coverage."""

    def test_envelope_encode_with_large_payload(self):
        """Test encoding with larger payload."""
        env = Envelope(
            kind="kv.put",
            payload=b"x" * 10000,
            schema_id=1,
        )
        encoded = env.encode()
        decoded = Envelope.decode(encoded)
        assert decoded.payload == b"x" * 10000

    def test_envelope_with_custom_correlation_id(self):
        """Test envelope with custom UUID."""
        cid = uuid.uuid4()
        env = Envelope(kind="test.kind", payload=b"test", correlation_id=cid)
        assert env.correlation_id == cid

    def test_envelope_create_with_schema_id_0(self):
        """Test Envelope.create() with schema_id=0 for raw bytes."""
        env = Envelope.create(b"payload", kind="error.test", schema_id=0)
        assert env.schema_id == 0
        assert env.kind == "error.test"

    def test_envelope_decode_truncated_payload(self):
        """Test decoding with truncated payload."""
        # Create a valid envelope and truncate its payload section
        env = Envelope(kind="test", payload=b"full_payload")
        encoded = env.encode()
        # Truncate the last few bytes
        truncated = encoded[:-5]
        with pytest.raises(ValueError):
            Envelope.decode(truncated)

    def test_envelope_decode_invalid_kind_len(self):
        """Test decoding with kind_len = 0 (invalid)."""
        # Manually construct a frame with kind_len=0
        import struct

        proto_version = 1
        schema_id = 1
        cid_bytes = uuid.uuid4().bytes
        payload_len = 0
        kind_len = 0

        header = struct.pack(
            "!BH16sI", proto_version, schema_id, cid_bytes, payload_len
        )
        frame = header + bytes([kind_len])

        with pytest.raises(ValueError, match="kind_len is zero"):
            Envelope.decode(frame)

    def test_envelope_decode_kind_len_exceeds_max(self):
        """Test decoding with kind_len > KIND_MAX_LEN."""
        import struct

        proto_version = 1
        schema_id = 1
        cid_bytes = uuid.uuid4().bytes
        payload_len = 0
        kind_len = 100  # > KIND_MAX_LEN (64)

        header = struct.pack(
            "!BH16sI", proto_version, schema_id, cid_bytes, payload_len
        )
        frame = header + bytes([kind_len])

        with pytest.raises(ValueError, match="exceeds maximum"):
            Envelope.decode(frame)


@pytest.mark.bootstrap
class TestDispatcherExtended:
    """Extended Dispatcher tests."""

    def test_dispatcher_register_multiple_handlers(self):
        """Test registering multiple different handlers."""
        dispatcher = Dispatcher()

        async def handler1(r, s):
            pass

        async def handler2(r, s):
            pass

        dispatcher.register("kind.one", handler1)
        dispatcher.register("kind.two", handler2)

        assert dispatcher.lookup("kind.one") is handler1
        assert dispatcher.lookup("kind.two") is handler2

    def test_dispatcher_on_decorator_returns_function(self):
        """Test that on() decorator returns the original function."""
        dispatcher = Dispatcher()

        async def my_handler(r, s):
            return "test"

        decorated = dispatcher.on("test.kind")(my_handler)
        assert decorated is my_handler


@pytest.mark.bootstrap
class TestMsgpackExtended:
    """Extended MsgpackSerializerAdapter tests."""

    def test_msgpack_with_various_types(self):
        """Test encoding/decoding various Python types."""
        adapter = MsgpackSerializerAdapter()

        test_objects = [
            {"string": "value", "int": 42, "float": 3.14, "bool": True, "null": None},
            [1, 2, 3, 4, 5],
            {"nested": {"dict": {"with": "values"}}},
            {"list": [{"of": "dicts"}, {"more": "values"}]},
        ]

        for obj in test_objects:
            encoded = adapter.encode(obj)
            decoded = adapter.decode(encoded)
            assert decoded == obj

    def test_msgpack_with_uint64_boundary(self):
        """Test integer at uint64 boundary."""
        adapter = MsgpackSerializerAdapter()
        # uint64 max
        int_val = 2**64 - 1
        encoded = adapter.encode(int_val)
        decoded = adapter.decode(encoded)
        assert decoded == int_val

    def test_msgpack_schema_id(self):
        """Test that schema_id is correct."""
        adapter = MsgpackSerializerAdapter()
        assert adapter.schema_id == 1


@pytest.mark.bootstrap
class TestParseExtended:
    """Extended parsing tests."""

    def test_parse_duration_with_whitespace(self):
        """Test parsing duration with leading/trailing whitespace."""
        assert parse_duration("  10s  ") == 10.0
        assert parse_duration("\t5m\n") == 300.0

    def test_parse_bytes_with_whitespace(self):
        """Test parsing bytes with leading/trailing whitespace."""
        assert parse_bytes("  1Mi  ") == 1048576
        assert parse_bytes("\t512Ki\n") == 524288

    def test_parse_duration_decimal(self):
        """Test parsing duration with decimal values."""
        assert parse_duration("1.5s") == 1.5
        assert parse_duration("0.5m") == 30.0

    def test_parse_duration_all_units(self):
        """Test all time units."""
        assert parse_duration("1ms") == 0.001
        assert parse_duration("1s") == 1.0
        assert parse_duration("1m") == 60.0
        assert parse_duration("1h") == 3600.0

    def test_parse_bytes_all_units(self):
        """Test all size units."""
        assert parse_bytes("1Ki") == 1024
        assert parse_bytes("1Mi") == 1024**2
        assert parse_bytes("1Gi") == 1024**3
