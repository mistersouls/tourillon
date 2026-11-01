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

from typing import Any, cast

import pytest

from tourillon.core.models.storageschema import (
    MAX_VALUE_DATA_LEN,
    Key,
    Keyspace,
    KeyValue,
    MetaKind,
    ObligationKey,
    ObligationPrefix,
    Pid,
    Value,
)
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address


class _FakeTimestamp:
    def to_dict(self) -> dict[str, Any]:
        return {"wall_ms": 1, "counter": 0, "node_id": ""}

    def to_bytes(self) -> bytes:
        return b"\x00" * 12


@pytest.mark.tourillon
def test_keyvalue_to_bytes_appends_value_after_key() -> None:
    key = Key(
        pid=Pid(5),
        address=Address(key=b"k", namespace=b"ns"),
        ts=cast(Any, _FakeTimestamp()),
    )
    value = Value(data=b"payload", quorum_write=3)
    item = KeyValue(key=key, value=value)

    assert (
        item.to_bytes(Keyspace.LOGS) == key.to_bytes(Keyspace.LOGS) + value.to_bytes()
    )


@pytest.mark.tourillon
def test_keyvalue_to_dict_roundtrip_from_dict() -> None:
    payload = {
        "key": {
            "pid": 8,
            "address": {"key": b"k", "namespace": b"ns"},
            "ts": {"wall_ms": 123, "counter": 0, "node_id": ""},
        },
        "value": {"data": b"abc", "quorum_write": 2},
    }

    item = KeyValue.from_dict(payload)

    assert item.key.pid == Pid(8)
    assert item.key.address == Address(key=b"k", namespace=b"ns")
    assert item.value == Value(data=b"abc", quorum_write=2)


@pytest.mark.tourillon
def test_keyvalue_from_dict_raises_on_missing_value() -> None:
    key_payload = {
        "pid": 1,
        "address": {"key": b"k", "namespace": b"ns"},
        "ts": {"wall_ms": 0, "counter": 0, "node_id": ""},
    }

    with pytest.raises(KeyError, match="value"):
        KeyValue.from_dict({"key": key_payload})


@pytest.mark.tourillon
def test_value_allows_exact_16mb_payload() -> None:
    data = b"x" * MAX_VALUE_DATA_LEN
    value = Value(data=data, quorum_write=1)

    assert Value.from_bytes(value.to_bytes()) == value


@pytest.mark.tourillon
def test_value_rejects_payload_over_16mb() -> None:
    with pytest.raises(OverflowError, match="Value.data length"):
        Value(data=b"x" * (MAX_VALUE_DATA_LEN + 1), quorum_write=1)


def _key() -> Key:
    return Key(
        pid=Pid(7),
        address=Address(namespace=b"ns", key=b"key"),
        ts=HLCTimestamp(wall_ms=10, counter=2, node_id="node-a"),
    )


@pytest.mark.tourillon
def test_obligation_prefix_scopes_all_or_one_target() -> None:
    assert ObligationPrefix().to_bytes() == MetaKind.OBLIGATION.value
    scoped = ObligationPrefix("node-b").to_bytes()
    assert scoped == MetaKind.OBLIGATION.value + b"\x00\x06node-b"
    assert ObligationKey("node-b", _key()).to_bytes().startswith(scoped)


@pytest.mark.tourillon
@pytest.mark.parametrize("target", ["x" * 63, "é" * 31 + "a"])
def test_obligation_key_round_trips_with_the_tags_key_encoding(target: str) -> None:
    obligation = ObligationKey(target, _key())

    encoded = obligation.to_bytes()

    assert encoded.endswith(_key().to_bytes(Keyspace.TAGS))
    assert ObligationKey.from_bytes(encoded) == obligation
    assert hash(ObligationKey.from_bytes(encoded)) == hash(obligation)


@pytest.mark.tourillon
@pytest.mark.parametrize("target", ["", "x" * 64, "é" * 32])
def test_obligation_target_rejects_empty_or_oversized_names(target: str) -> None:
    with pytest.raises(ValueError):
        ObligationKey(target, _key())
    with pytest.raises(ValueError):
        ObligationPrefix(target)


@pytest.mark.tourillon
def test_obligation_key_rejects_wrong_kind_malformed_and_trailing_bytes() -> None:
    valid = ObligationKey("node-b", _key()).to_bytes()

    with pytest.raises(ValueError, match="not a replication obligation"):
        ObligationKey.from_bytes(MetaKind.DIGEST.value + valid[1:])
    with pytest.raises(ValueError, match="not valid UTF-8"):
        ObligationKey.from_bytes(b"\x01\x00\x01\xff" + valid[9:])
    with pytest.raises(ValueError):
        ObligationKey.from_bytes(valid[:-1])
    with pytest.raises(ValueError, match="trailing bytes"):
        ObligationKey.from_bytes(valid + b"\x00")
