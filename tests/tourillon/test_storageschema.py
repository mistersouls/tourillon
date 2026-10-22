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
    Hint,
    HintKey,
    HintPrefix,
    HintReference,
    Key,
    Keyspace,
    KeyValue,
    Pid,
    Record,
    Tag,
    TagKind,
    TagPayload,
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


@pytest.mark.tourillon
def test_staging_round_trip_preserves_tag_payloads() -> None:
    tags = [
        Tag(kind=TagKind.LIVE),
        Tag(kind=TagKind.TOMBSTONE),
        Tag.hint(Tag(kind=TagKind.LIVE)),
        Tag.hint(Tag(kind=TagKind.TOMBSTONE)),
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
    original = Tag(
        kind=TagKind.TOMBSTONE,
        payload=TagPayload(sub=TagKind.META, payload=b"reason"),
    )
    hinted = Tag.from_bytes(Tag.hint(original).to_bytes())

    restored = Tag.unhint(hinted)

    assert hinted.kind is TagKind.HINT
    assert restored == original
    assert Tag.from_bytes(restored.to_bytes()) == original


@pytest.mark.tourillon
def test_unhint_rejects_non_hint_tags() -> None:
    with pytest.raises(ValueError, match="non-HINT"):
        Tag.unhint(Tag(kind=TagKind.LIVE))


def _hint_key() -> HintKey:
    return HintKey(
        intent="node-a",
        hlc=HLCTimestamp(wall_ms=123, counter=4, node_id="node-b"),
    )


def _record(tag: Tag) -> Record:
    return Record(
        key=Key(
            pid=Pid(8),
            address=Address(key=b"k", namespace=b"ns"),
            ts=HLCTimestamp(wall_ms=456, counter=2, node_id="node-a"),
        ),
        value=Value(data=b"value", quorum_write=1),
        tag=tag,
    )


@pytest.mark.tourillon
def test_hint_prefix_and_key_round_trip_with_and_without_intent_filter() -> None:
    prefixes = [HintPrefix(), HintPrefix(intent="node-a")]

    for prefix in prefixes:
        assert HintPrefix.from_bytes(prefix.to_bytes()) == prefix

    key = _hint_key()
    assert HintKey.from_bytes(key.to_bytes()) == key
    assert key.to_bytes().startswith(key.prefix())


@pytest.mark.tourillon
def test_hint_reference_round_trip_uses_the_tags_key_encoding() -> None:
    record = _record(Tag(kind=TagKind.LIVE))
    key = record.key
    reference = HintReference(key=key)

    assert HintReference.from_bytes(reference.to_bytes()) == reference
    assert reference.to_bytes() == reference.key.to_bytes(Keyspace.TAGS)


@pytest.mark.tourillon
@pytest.mark.parametrize("intent", ["x" * 63, "é" * 31 + "a"])
def test_hint_intent_accepts_at_most_63_utf8_bytes(intent: str) -> None:
    prefix = HintPrefix(intent=intent)
    template = _hint_key()
    hlc = template.hlc
    key = HintKey(intent=intent, hlc=hlc)

    assert HintPrefix.from_bytes(prefix.to_bytes()) == prefix
    assert HintKey.from_bytes(key.to_bytes()) == key


@pytest.mark.tourillon
@pytest.mark.parametrize("intent", ["", "x" * 64, "é" * 32])
def test_hint_intent_rejects_empty_or_more_than_63_utf8_bytes(intent: str) -> None:
    template = _hint_key()
    hlc = template.hlc
    with pytest.raises(ValueError):
        HintPrefix(intent=intent)
    with pytest.raises(ValueError):
        HintKey(intent=intent, hlc=hlc)


@pytest.mark.tourillon
def test_hint_prefix_rejects_wrong_kind_malformed_and_trailing_bytes() -> None:
    valid = HintPrefix(intent="node-a").to_bytes()

    with pytest.raises(ValueError, match="not a hint prefix"):
        HintPrefix.from_bytes(b"\x02")
    with pytest.raises(ValueError):
        HintPrefix.from_bytes(b"\x01\x00")
    with pytest.raises(ValueError):
        HintPrefix.from_bytes(b"\x01\x00\x01\xff")
    with pytest.raises(ValueError, match="trailing"):
        HintPrefix.from_bytes(valid + b"\x00")


@pytest.mark.tourillon
def test_hint_key_rejects_wrong_kind_malformed_and_trailing_bytes() -> None:
    valid = _hint_key().to_bytes()

    with pytest.raises(ValueError, match="not a hint obligation"):
        HintKey.from_bytes(b"\x02")
    with pytest.raises(ValueError):
        HintKey.from_bytes(b"\x01\x00\x02x")
    with pytest.raises(ValueError, match="UTF-8"):
        HintKey.from_bytes(b"\x01\x00\x01\xff" + b"\x00" * 12)
    with pytest.raises(ValueError):
        HintKey.from_bytes(valid[:-1])
    with pytest.raises(ValueError, match="trailing"):
        HintKey.from_bytes(valid + b"\x00")


@pytest.mark.tourillon
def test_hint_reference_rejects_malformed_and_trailing_bytes() -> None:
    record = _record(Tag(kind=TagKind.TOMBSTONE))
    key = record.key
    reference = HintReference(key=key)

    with pytest.raises(ValueError):
        HintReference.from_bytes(reference.to_bytes()[:-1])
    with pytest.raises(ValueError, match="canonical"):
        HintReference.from_bytes(reference.to_bytes() + b"\x00")


@pytest.mark.tourillon
@pytest.mark.parametrize("kind", [TagKind.LIVE, TagKind.TOMBSTONE])
def test_hint_accepts_only_wrapped_live_and_tombstone_records(kind: TagKind) -> None:
    record = _record(Tag.hint(Tag(kind=kind)))

    template = _hint_key()
    hlc = template.hlc
    hint = Hint(record=record, intent="node-b", hlc=hlc)
    hinted_record = hint.record
    assert hinted_record == record


@pytest.mark.tourillon
def test_hint_rejects_unwrapped_and_non_transferable_record_tags() -> None:
    template = _hint_key()
    hlc = template.hlc

    with pytest.raises(ValueError, match="non-HINT"):
        Hint(record=_record(Tag(kind=TagKind.LIVE)), intent="node-b", hlc=hlc)
    for original_kind in (TagKind.STAGING, TagKind.STALE, TagKind.PHANTOM):
        with pytest.raises(ValueError, match="LIVE or TOMBSTONE"):
            Hint(
                record=_record(Tag.hint(Tag(kind=original_kind))),
                intent="node-b",
                hlc=hlc,
            )
