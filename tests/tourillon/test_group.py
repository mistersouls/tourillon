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

from contextlib import asynccontextmanager

import pytest

from tourillon.core.models.storageschema import (
    Key,
    Keyspace,
    KVPair,
    KVRef,
    Pid,
    Record,
    Tag,
    TagKind,
    TagPayload,
    Value,
)
from tourillon.core.storage.group import (
    PartitionGroupHint,
    PartitionGroupStaging,
    PartitionGroupStore,
)
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address


class _FakeBatch:
    def __init__(self, store: "_FakeStore") -> None:
        self._store = store
        self._ops: list[tuple] = []

    async def __aenter__(self) -> "_FakeBatch":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        return False

    def get(self, ref: KVRef) -> None:
        self._ops.append(("get", ref))

    def put(self, kv: KVPair) -> None:
        self._ops.append(("put", kv))

    def delete(self, ref: KVRef) -> None:
        self._ops.append(("delete", ref))

    def last(
        self,
        keyspace: Keyspace,
        prefix=None,
        predicate=None,
    ) -> None:
        self._ops.append(("last", keyspace, prefix, predicate))

    def list(
        self,
        keyspace: Keyspace,
        prefix=None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> None:
        self._ops.append(("list", keyspace, prefix, start, limit, reverse))

    async def result(self):
        results = []
        for op in self._ops:
            kind = op[0]
            if kind == "get":
                results.append(self._store.get(op[1]))
            elif kind == "put":
                self._store.put(op[1])
                results.append(None)
            elif kind == "delete":
                self._store.delete(op[1])
                results.append(None)
            elif kind == "last":
                _, keyspace, prefix, predicate = op
                results.append(
                    self._store.last(keyspace, prefix=prefix, predicate=predicate)
                )
            elif kind == "list":
                _, keyspace, prefix, start, limit, reverse = op
                results.append(
                    self._store.list(
                        keyspace,
                        prefix=prefix,
                        start=start,
                        limit=limit,
                        reverse=reverse,
                    )
                )
        return results


class _FakeStore:
    def __init__(self) -> None:
        self._items: dict[tuple[Keyspace, bytes], KVPair] = {}

    def put(self, kv: KVPair) -> None:
        self._items[(kv.keyspace, kv.key)] = kv

    def get(self, ref: KVRef) -> KVPair | None:
        return self._items.get((ref.keyspace, ref.key))

    def delete(self, ref: KVRef) -> None:
        self._items.pop((ref.keyspace, ref.key), None)

    def _sorted_items(self, keyspace: Keyspace, reverse: bool = False) -> list[KVPair]:
        items = [kv for (ks, _), kv in self._items.items() if ks == keyspace]
        return sorted(items, key=lambda kv: kv.key, reverse=reverse)

    def list(
        self,
        keyspace: Keyspace,
        prefix=None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> list[KVPair]:
        items = self._sorted_items(keyspace, reverse=reverse)
        prefix_bytes = prefix.to_bytes() if prefix is not None else None

        filtered: list[KVPair] = []
        for kv in items:
            if prefix_bytes is not None and not kv.key.startswith(prefix_bytes):
                continue
            if start is not None and kv.key <= start:
                continue
            filtered.append(kv)

        if limit is not None:
            return filtered[:limit]
        return filtered

    def last(self, keyspace: Keyspace, prefix=None, predicate=None) -> KVPair | None:
        items = self._sorted_items(keyspace)
        prefix_bytes = prefix.to_bytes() if prefix is not None else None

        found: KVPair | None = None
        for kv in items:
            if prefix_bytes is not None and not kv.key.startswith(prefix_bytes):
                continue
            if predicate is not None and not predicate(kv):
                continue
            found = kv
        return found


class _FakeSession:
    def __init__(self, store: _FakeStore) -> None:
        self._store = store

    def batch(self) -> _FakeBatch:
        return _FakeBatch(self._store)

    async def get(self, ref: KVRef) -> KVPair | None:
        return self._store.get(ref)

    async def put(self, kv: KVPair) -> None:
        self._store.put(kv)

    async def delete(self, ref: KVRef) -> None:
        self._store.delete(ref)

    async def last(
        self, keyspace: Keyspace, prefix=None, predicate=None
    ) -> KVPair | None:
        return self._store.last(keyspace, prefix=prefix, predicate=predicate)

    async def iter(
        self,
        keyspace: Keyspace,
        prefix=None,
        start: bytes | None = None,
        reverse: bool = False,
        batch_size: int = 32,
        limit: int | None = None,
    ):
        del batch_size
        for kv in self._store.list(
            keyspace,
            prefix=prefix,
            start=start,
            limit=limit,
            reverse=reverse,
        ):
            yield kv


class _FakeSessionProvider:
    """Session provider over a _FakeStore that counts the sessions it opens."""

    def __init__(self, store: _FakeStore, group_id: int = 0) -> None:
        self._store = store
        self._group_id = group_id
        self.opened = 0
        self.active = 0

    @property
    def group_id(self) -> int:
        return self._group_id

    @asynccontextmanager
    async def session(self, write: bool = False):
        del write
        self.opened += 1
        self.active += 1
        try:
            yield _FakeSession(self._store)
        finally:
            self.active -= 1

    async def close(self) -> None:
        return None


class _FakePartitioner:
    def __init__(self, start: int, end: int, group_id: int = 0) -> None:
        self._start = start
        self._end = end
        self._group_id = group_id

    def group_bounds(self, gid: int) -> tuple[int, int]:
        assert gid == self._group_id
        return self._start, self._end


def _record(pid: int, idx: int) -> Record:
    return Record(
        key=Key(
            pid=Pid(pid),
            address=Address(namespace=b"ns", key=f"k{pid}-{idx}".encode()),
            ts=HLCTimestamp(wall_ms=100 + idx + pid, counter=idx, node_id="n"),
        ),
        value=Value(data=f"v{pid}-{idx}".encode(), quorum_write=1),
        tag=Tag(kind=TagKind.LIVE),
    )


def _with_tag(record: Record, tag: Tag) -> Record:
    return Record(key=record.key, value=record.value, tag=tag)


def _seed_record(store: _FakeStore, record: Record) -> None:
    store.put(
        KVPair(
            key=record.key.to_bytes(Keyspace.TAGS),
            value=record.tag.to_bytes(),
            keyspace=Keyspace.TAGS,
        )
    )
    store.put(
        KVPair(
            key=record.key.to_bytes(Keyspace.LOGS),
            value=record.value.to_bytes(),
            keyspace=Keyspace.LOGS,
        )
    )


def _stored_tag(store: _FakeStore, record: Record) -> Tag:
    kv = store.get(
        KVRef(key=record.key.to_bytes(Keyspace.TAGS), keyspace=Keyspace.TAGS)
    )
    assert kv is not None
    return Tag.from_bytes(kv.value)


def _staging(
    store: _FakeStore,
    start: int = 7,
    end: int = 8,
    segment: tuple[int, int] | None = None,
) -> PartitionGroupStaging:
    return PartitionGroupStaging(
        "source",
        _FakeSessionProvider(store),
        _FakePartitioner(start, end),  # type: ignore[arg-type]
        segment=segment,
    )


def _hints(store: _FakeStore) -> PartitionGroupHint:
    return PartitionGroupHint(_FakeSessionProvider(store))


async def _scan(staging: PartitionGroupStaging, **kwargs) -> list[Record]:
    return [record async for record in staging.scan(**kwargs)]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_scan_yields_records_in_key_order_across_pids_and_pages() -> None:
    store = _FakeStore()
    records = [_record(8, 1), _record(7, 2), _record(7, 1), _record(8, 2)]
    for record in records:
        _seed_record(store, record)
    staging = _staging(store)

    expected = sorted(records, key=lambda record: record.key.to_bytes(Keyspace.TAGS))
    for batch_size in (1, 2, 128):
        assert await _scan(staging, batch_size=batch_size) == expected


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_scan_skips_resumed_keys_keys_up_to_after_and_other_kinds() -> None:
    store = _FakeStore()
    pid7 = [_record(7, idx) for idx in range(1, 4)]
    pid8 = [_record(8, idx) for idx in range(1, 4)]
    stale = _with_tag(_record(8, 9), Tag(kind=TagKind.STALE))
    outside = _record(9, 1)  # same group, outside the segment
    for record in [*pid7, *pid8, stale, outside]:
        _seed_record(store, record)
    staging = _staging(store, start=7, end=9, segment=(7, 8))
    live = frozenset({TagKind.LIVE})

    assert await _scan(staging, resumes={8: pid8[0].key}, kinds=live, batch_size=1) == [
        *pid7,
        *pid8[1:],
    ]
    assert await _scan(staging, after=pid7[1].key, kinds=live) == [pid7[2], *pid8]
    # A resume key behind `after` is moot, one ahead of it still applies.
    assert await _scan(
        staging,
        after=pid7[1].key,
        resumes={7: pid7[0].key, 8: pid8[1].key},
    ) == [pid7[2], pid8[2], stale]
    # Resume keys of pids handled elsewhere are ignored.
    assert await _scan(staging, resumes={42: pid7[0].key}, kinds=live) == [*pid7, *pid8]
    # `after` before the segment means everything, after the segment nothing.
    assert await _scan(staging, after=_record(1, 1).key, kinds=live) == [*pid7, *pid8]
    assert await _scan(staging, after=outside.key) == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_scan_skips_unreadable_entries() -> None:
    store = _FakeStore()
    good, no_value, bad_value, bad_tag = (_record(7, idx) for idx in range(1, 5))
    _seed_record(store, good)
    store.put(
        KVPair(
            key=no_value.key.to_bytes(Keyspace.TAGS),
            value=no_value.tag.to_bytes(),
            keyspace=Keyspace.TAGS,
        )
    )
    _seed_record(store, bad_value)
    store.put(
        KVPair(
            key=bad_value.key.to_bytes(Keyspace.LOGS), value=b"", keyspace=Keyspace.LOGS
        )
    )
    _seed_record(store, bad_tag)
    store.put(
        KVPair(
            key=bad_tag.key.to_bytes(Keyspace.TAGS),
            value=b"\xff",
            keyspace=Keyspace.TAGS,
        )
    )

    assert await _scan(_staging(store)) == [good]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_scan_holds_no_session_while_records_are_consumed() -> None:
    store = _FakeStore()
    for idx in range(1, 6):
        _seed_record(store, _record(7, idx))
    provider = _FakeSessionProvider(store)
    staging = PartitionGroupStaging("source", provider, _FakePartitioner(7, 8))  # type: ignore[arg-type]

    with pytest.raises(ValueError, match=">= 1"):
        await _scan(staging, batch_size=0)

    seen = 0
    async for _ in staging.scan(batch_size=2):
        assert provider.active == 0
        seen += 1

    assert seen == 5
    assert provider.opened == 3  # one short read transaction per page


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_stage_writes_staging_entries_and_keeps_active_records_visible() -> None:
    store = _FakeStore()
    fresh, active, corrupted = _record(7, 1), _record(7, 2), _record(7, 3)
    foreign = _record(8, 1)
    _seed_record(store, active)
    _seed_record(store, corrupted)
    store.put(
        KVPair(
            key=corrupted.key.to_bytes(Keyspace.TAGS),
            value=b"\xff",
            keyspace=Keyspace.TAGS,
        )
    )
    _seed_record(store, _with_tag(foreign, Tag.staging(foreign.tag, "other")))
    staging = _staging(store)

    assert await staging.stage([]) == 0
    assert await staging.stage([fresh, active, corrupted, foreign]) == 3
    assert _stored_tag(store, fresh) == Tag.staging(fresh.tag, "source")
    assert _stored_tag(store, active) == active.tag
    assert _stored_tag(store, corrupted) == Tag.staging(corrupted.tag, "source")
    assert _stored_tag(store, foreign) == Tag.staging(foreign.tag, "source")
    assert await staging.last_keys() == {7: corrupted.key, 8: foreign.key}

    # A retry stages the same entries again, with the same outcome.
    assert await staging.stage([fresh, foreign]) == 2
    assert await staging.last_keys() == {7: corrupted.key, 8: foreign.key}


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_commit_activates_owned_records_only_and_is_idempotent() -> None:
    store = _FakeStore()
    owned = [_record(7, 1), _record(7, 2), _record(8, 1)]
    foreign, live = _record(7, 3), _record(8, 2)
    staging = _staging(store)
    await staging.stage(owned)
    _seed_record(store, _with_tag(foreign, Tag.staging(foreign.tag, "other")))
    _seed_record(store, live)

    assert await staging.commit(batch_size=1) == 3
    assert [_stored_tag(store, record) for record in owned] == [
        record.tag for record in owned
    ]
    assert _stored_tag(store, foreign).kind == TagKind.STAGING
    assert _stored_tag(store, live) == live.tag
    assert await staging.last_keys() == {}
    assert await staging.commit() == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_commit_restores_hint_and_tombstone_tags() -> None:
    store = _FakeStore()
    hint = _with_tag(_record(7, 1), Tag.hint(Tag(kind=TagKind.LIVE), "node-b"))
    tombstone = _with_tag(_record(7, 2), Tag(kind=TagKind.TOMBSTONE))
    staging = _staging(store)
    await staging.stage([hint, tombstone])

    assert await staging.commit() == 2
    assert _stored_tag(store, hint) == Tag.hint(Tag(kind=TagKind.LIVE), "node-b")
    assert _stored_tag(store, tombstone) == Tag(kind=TagKind.TOMBSTONE)


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_commit_keeps_unreadable_entries_and_stops_at_the_segment_end() -> None:
    store = _FakeStore()
    source = b"source"
    legacy = _record(7, 1)  # staged by an older version: HINT without its sub-kind
    legacy_tag = Tag(
        kind=TagKind.STAGING,
        payload=TagPayload(
            sub=TagKind.HINT,
            payload=len(source).to_bytes(2, "big") + source + b"node-b",
        ),
    )
    _seed_record(store, _with_tag(legacy, legacy_tag))
    corrupted = _record(7, 3)
    _seed_record(store, corrupted)
    store.put(
        KVPair(
            key=corrupted.key.to_bytes(Keyspace.TAGS),
            value=b"\xff",
            keyspace=Keyspace.TAGS,
        )
    )
    inside, beyond = _record(7, 2), _record(8, 1)
    whole_group = _staging(store)
    await whole_group.stage([inside, beyond])

    assert await _staging(store, segment=(7, 7)).commit() == 1
    assert _stored_tag(store, inside) == inside.tag
    assert _stored_tag(store, beyond).kind == TagKind.STAGING
    assert _stored_tag(store, legacy) == legacy_tag
    assert store.get(KVRef(key=corrupted.key.to_bytes(Keyspace.TAGS), keyspace=Keyspace.TAGS)).value == b"\xff"  # type: ignore[union-attr]

    with pytest.raises(ValueError, match=">= 1"):
        await whole_group.commit(batch_size=0)


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_hint_satisfy_restores_original_tags_and_is_idempotent() -> None:
    store = _FakeStore()
    live = _with_tag(_record(7, 1), Tag.hint(Tag(kind=TagKind.LIVE), "node-b"))
    tombstone = _with_tag(
        _record(7, 2), Tag.hint(Tag(kind=TagKind.TOMBSTONE), "node-b")
    )
    plain = _record(7, 3)
    for record in (live, tombstone, plain):
        _seed_record(store, record)

    hints = _hints(store)

    assert await hints.satisfy([live, tombstone, plain]) is True
    assert _stored_tag(store, live) == Tag(kind=TagKind.LIVE)
    assert _stored_tag(store, tombstone) == Tag(kind=TagKind.TOMBSTONE)
    assert _stored_tag(store, plain) == plain.tag
    assert await hints.satisfy([live, tombstone]) is False


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_hint_satisfy_skips_missing_and_unreadable_entries() -> None:
    store = _FakeStore()
    missing = _with_tag(_record(7, 1), Tag.hint(Tag(kind=TagKind.LIVE), "node-b"))
    broken = _with_tag(_record(7, 2), Tag.hint(Tag(kind=TagKind.LIVE), "node-b"))
    _seed_record(store, broken)
    store.put(
        KVPair(
            key=broken.key.to_bytes(Keyspace.TAGS),
            value=b"\xff",
            keyspace=Keyspace.TAGS,
        )
    )

    assert await _hints(store).satisfy([missing, broken]) is False
    assert store.get(KVRef(key=broken.key.to_bytes(Keyspace.TAGS), keyspace=Keyspace.TAGS)).value == b"\xff"  # type: ignore[union-attr]


@pytest.mark.tourillon
def test_partition_group_staging_rejects_segment_outside_its_group() -> None:
    with pytest.raises(
        ValueError, match=r"segment \[0, 1\] is not handled by group 0 \[7, 8\]"
    ):
        _staging(_FakeStore(), segment=(0, 1))
    with pytest.raises(ValueError, match="is not handled"):
        _staging(_FakeStore(), segment=(8, 7))


def _store_over(store: _FakeStore) -> PartitionGroupStore:
    return PartitionGroupStore(_FakeSessionProvider(store))  # type: ignore[arg-type]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_at_returns_the_version_stored_at_that_exact_timestamp() -> None:
    store = _FakeStore()
    older = _record(7, 1)
    newer = _record(7, 1)
    newer = Record(
        key=Key(
            pid=newer.key.pid, address=newer.key.address, ts=HLCTimestamp(900, 0, "n")
        ),
        value=Value(data=b"newer", quorum_write=1),
        tag=newer.tag,
    )
    _seed_record(store, older)
    _seed_record(store, newer)

    found = await _store_over(store).get_at(older.key)

    # `get` would answer the newest version; `get_at` answers the one asked for.
    assert found == older


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_at_returns_none_for_a_timestamp_never_written() -> None:
    store = _FakeStore()
    record = _record(7, 1)
    _seed_record(store, record)
    absent = Key(
        pid=record.key.pid,
        address=record.key.address,
        ts=HLCTimestamp(wall_ms=1, counter=0, node_id="n"),
    )

    assert await _store_over(store).get_at(absent) is None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_at_returns_none_when_the_tag_is_missing() -> None:
    store = _FakeStore()
    record = _record(7, 1)
    store.put(
        KVPair(
            key=record.key.to_bytes(Keyspace.LOGS),
            value=record.value.to_bytes(),
            keyspace=Keyspace.LOGS,
        )
    )

    assert await _store_over(store).get_at(record.key) is None
