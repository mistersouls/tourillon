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

import pytest

from tourlib.adapter import DefaultSerializer, MsgPackSerializer


@pytest.mark.tourlib
def test_msgpack_serializer_roundtrip_nested_document() -> None:
    serializer = MsgPackSerializer()
    document = {
        "namespace": "default",
        "key": "hello",
        "value": {"count": 1, "tags": ["a", "b"]},
        "tombstone": False,
    }

    payload = serializer.encode(document)
    restored = serializer.decode(payload)

    assert isinstance(payload, bytes)
    assert restored == document


@pytest.mark.tourlib
def test_msgpack_serializer_decode_rejects_invalid_payload() -> None:
    serializer = MsgPackSerializer()

    with pytest.raises(ValueError, match="decode msgpack"):
        serializer.decode(b"not-msgpack")


@pytest.mark.tourlib
def test_default_serializer_points_to_msgpack_serializer() -> None:
    serializer = DefaultSerializer()

    assert isinstance(serializer, MsgPackSerializer)


@pytest.mark.tourlib
def test_msgpack_serializer_encode_rejects_unserializable_object() -> None:
    serializer = MsgPackSerializer()

    class Unserializable:
        pass

    with pytest.raises(ValueError, match="encode document with msgpack"):
        serializer.encode(Unserializable())
