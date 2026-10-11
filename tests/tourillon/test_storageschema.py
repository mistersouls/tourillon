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
    Pid,
    Tag,
    TagKind,
    Value,
)
from tourlib.models.version import Address


class _FakeTimestamp:
    def to_dict(self) -> dict[str, Any]:
        return {"wall_ms": 1, "counter": 0, "node_id": ""}

    def to_bytes(self) -> bytes:
        return b"\x00" * 12


@pytest.mark.tourillon
def test_keyvalue_to_bytes_appends_value_after_key() -> None:
    key = Key(pid=Pid(5), address=Address(key=b"k", namespace=b"ns"), ts=cast(Any, _FakeTimestamp()))
    value = Value(data=b"payload", quorum_write=3)
    item = KeyValue(key=key, value=value)

    assert item.to_bytes(Keyspace.LOGS) == key.to_bytes(Keyspace.LOGS) + value.to_bytes()


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


@pytest.mark.tourillon
def test_staging_round_trip_restores_every_transferable_tag() -> None:
    tags = [
        Tag(kind=TagKind.LIVE),
        Tag(kind=TagKind.TOMBSTONE),
        Tag.hint(Tag(kind=TagKind.LIVE), "node-b"),
        Tag.hint(Tag(kind=TagKind.TOMBSTONE), "node-c"),
    ]

    for tag in tags:
        staged = Tag.from_bytes(Tag.staging(tag, "source").to_bytes())
        restored = Tag.unstaging(staged, "source")

        assert staged.kind is TagKind.STAGING
        assert restored == tag
        assert Tag.from_bytes(restored.to_bytes()) == tag


@pytest.mark.tourillon
def test_unstaging_rejects_foreign_and_non_staging_tags() -> None:
    staged = Tag.staging(Tag(kind=TagKind.LIVE), "source")

    with pytest.raises(ValueError, match="source mismatch"):
        Tag.unstaging(staged, "other")
    with pytest.raises(ValueError, match="non-STAGING"):
        Tag.unstaging(Tag(kind=TagKind.LIVE))


@pytest.mark.tourillon
def test_hint_round_trip_restores_original_tag() -> None:
    original = Tag.hint(Tag(kind=TagKind.TOMBSTONE), "handoff")
    hinted = Tag.from_bytes(Tag.hint(original, "node-b").to_bytes())

    restored = Tag.unhint(hinted, "node-b")

    assert hinted.kind is TagKind.HINT
    assert restored == original
    assert Tag.from_bytes(restored.to_bytes()) == original


@pytest.mark.tourillon
def test_unhint_rejects_foreign_and_non_hint_tags() -> None:
    hinted = Tag.hint(Tag(kind=TagKind.LIVE), "node-b")

    with pytest.raises(ValueError, match="node id mismatch"):
        Tag.unhint(hinted, "node-c")
    with pytest.raises(ValueError, match="non-HINT"):
        Tag.unhint(Tag(kind=TagKind.LIVE))


