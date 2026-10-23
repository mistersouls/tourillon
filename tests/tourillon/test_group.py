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
    MAX_VALUE_DATA_LEN,
    Hint,
    HintKey,
    HintPrefix,
    HintReference,
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
)
from tourillon.core.storage.session import GroupSessionManager
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

    def exists(self, ref: KVRef) -> None:
        self._ops.append(("exists", ref))

    def put(self, kv: KVPair, overwrite: bool = True) -> None:
        self._ops.append(("put", kv, overwrite))

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
            elif kind == "exists":
                results.append((op[1].keyspace, op[1].key) in self._store._items)
            elif kind == "put":
                results.append(self._put(op[1], op[2]))
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

    def _put(self, kv: KVPair, overwrite: bool) -> bool:
        if not overwrite and (kv.keyspace, kv.key) in self._store._items:
            return False
        self._store.put(kv)
        return True


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

    async def exists(self, ref: KVRef) -> bool:
        return (ref.keyspace, ref.key) in self._store._items

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


class _MetadataOnlyStore(_FakeStore):
    def get(self, ref: KVRef) -> KVPair | None:
        assert ref.keyspace is not Keyspace.LOGS, "existence must not load the value"
        return super().get(ref)


@pytest.mark.tourillon
@pytest.mark.parametrize("kind", list(TagKind))
@pytest.mark.parametrize("has_value", [False, True])
async def test_exists_checks_exact_owned_version_without_loading_value(
    kind: TagKind, has_value: bool
) -> None:
    store = _MetadataOnlyStore()
    record = _record(1, 1)
    store.put(
        KVPair(record.key.to_bytes(Keyspace.TAGS), Tag(kind).to_bytes(), Keyspace.TAGS)
    )
    if has_value:
        store.put(
            KVPair(
                record.key.to_bytes(Keyspace.LOGS),
                record.value.to_bytes(),
                Keyspace.LOGS,
            )
        )
    async with GroupSessionManager(_FakeSessionProvider(store)).session() as session:
        assert await session.exists(record.key) is (
            has_value and kind in {TagKind.LIVE, TagKind.TOMBSTONE}
        )
        assert not await session.exists(_record(1, 2).key)


class _FakePartitioner:
    def __init__(self, start: int, end: int, group_id: int = 0) -> None:
        self._start = start
        self._end = end
        self._group_id = group_id

    def group_bounds(self, gid: int) -> tuple[int, int]:
        assert gid == self._group_id
        return self._start, self._end


@pytest.mark.tourillon
@pytest.mark.parametrize("kind", list(TagKind))
@pytest.mark.parametrize("has_value", [False, True])
async def test_hint_promotion_checks_metadata_without_writing_when_readonly(
    kind: TagKind, has_value: bool
) -> None:
    storage = _MetadataOnlyStore()
    record = _record(1, 1)
    tag = Tag(kind)
    storage.put(
        KVPair(record.key.to_bytes(Keyspace.TAGS), tag.to_bytes(), Keyspace.TAGS)
    )
    if has_value:
        storage.put(
            KVPair(
                record.key.to_bytes(Keyspace.LOGS),
                record.value.to_bytes(),
                Keyspace.LOGS,
            )
        )
    provider = _FakeSessionProvider(storage)
    group = PartitionGroupHint(GroupSessionManager(provider))
    absent = _record(1, 2)

    confirmed, applied = await group.promote([record, absent], writable=False)
    assert confirmed == [
        has_value and kind in {TagKind.LIVE, TagKind.TOMBSTONE},
        False,
    ]
    assert applied == 0
    assert provider.opened == 1


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
        GroupSessionManager(_FakeSessionProvider(store)),
        _FakePartitioner(start, end),  # type: ignore[arg-type]
        segment=segment,
    )


def _hints(store: _FakeStore) -> PartitionGroupHint:
    return PartitionGroupHint(GroupSessionManager(_FakeSessionProvider(store)))


def _hint(record: Record, intent: str, wall_ms: int) -> Hint:
    return Hint(
        record=_with_tag(record, Tag.hint(record.tag)),
        intent=intent,
        hlc=HLCTimestamp(wall_ms=wall_ms, counter=0, node_id="hint"),
    )


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
    staging = PartitionGroupStaging("source", GroupSessionManager(provider), _FakePartitioner(7, 8))  # type: ignore[arg-type]

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
    hint = _with_tag(_record(7, 1), Tag.hint(Tag(kind=TagKind.LIVE)))
    tombstone = _with_tag(_record(7, 2), Tag(kind=TagKind.TOMBSTONE))
    staging = _staging(store)
    await staging.stage([hint, tombstone])

    assert await staging.commit() == 2
    assert _stored_tag(store, hint) == Tag.hint(Tag(kind=TagKind.LIVE))
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
async def test_hint_put_persists_version_and_independent_obligations() -> None:
    store = _FakeStore()
    record = _record(7, 1)
    first = _hint(record, "node-a", 500)
    second = _hint(record, "node-b", 500)
    hints = _hints(store)

    first_key = await hints.put(first)
    second_key = await hints.put(second)
    await hints.put(first)

    assert first_key != second_key
    assert _stored_tag(store, record) == first.record.tag
    assert store.get(
        KVRef(record.key.to_bytes(Keyspace.LOGS), Keyspace.LOGS)
    ) == KVPair(
        record.key.to_bytes(Keyspace.LOGS), record.value.to_bytes(), Keyspace.LOGS
    )
    obligations = store.list(Keyspace.META, prefix=HintPrefix("node-a"))
    assert len(obligations) == 1
    reference = HintReference.from_bytes(obligations[0].value)
    key = reference.key
    assert key == record.key
    assert len(store.list(Keyspace.META)) == 2


@pytest.mark.tourillon
@pytest.mark.parametrize("kind", [TagKind.LIVE, TagKind.TOMBSTONE])
@pytest.mark.parametrize("has_value", [False, True])
async def test_hint_put_preserves_owned_version_and_records_obligation(
    kind: TagKind, has_value: bool
) -> None:
    class TrackingStore(_FakeStore):
        def __init__(self) -> None:
            super().__init__()
            self.writes: list[Keyspace] = []

        def put(self, kv: KVPair) -> None:
            self.writes.append(kv.keyspace)
            super().put(kv)

    store = TrackingStore()
    record = _with_tag(_record(7, 1), Tag(kind))
    _seed_record(store, record)
    if not has_value:
        store.delete(KVRef(record.key.to_bytes(Keyspace.LOGS), Keyspace.LOGS))
    store.writes.clear()
    provider = _FakeSessionProvider(store)
    hints = PartitionGroupHint(GroupSessionManager(provider))
    hint = _hint(record, "node-b", 500)

    key = await hints.put(hint)

    assert _stored_tag(store, record) == record.tag
    assert store.writes == [Keyspace.META]
    assert provider.opened == 1
    assert store.get(KVRef(key.to_bytes(), Keyspace.META)) is not None
    assert (
        store.get(KVRef(record.key.to_bytes(Keyspace.LOGS), Keyspace.LOGS)) is not None
    ) is has_value


@pytest.mark.tourillon
@pytest.mark.parametrize("kind", [TagKind.LIVE, TagKind.TOMBSTONE])
async def test_hint_promotion_activates_in_one_session_and_preserves_obligations(
    kind: TagKind,
) -> None:
    storage = _FakeStore()
    provider = _FakeSessionProvider(storage)
    group = PartitionGroupHint(GroupSessionManager(provider))
    record = _with_tag(_record(7, 1), Tag(kind))
    first = _hint(record, "node-b", 800)
    second = _hint(record, "node-c", 801)
    await group.put(first)
    await group.put(second)
    obligations = storage.list(Keyspace.META)
    before = provider.opened

    assert await group.promote([record]) == ([True], 1)
    assert provider.opened == before + 1
    assert _stored_tag(storage, record) == record.tag
    assert storage.list(Keyspace.META, prefix=HintPrefix()) == obligations
    assert await group.promote([record]) == ([True], 0)
    assert [hint async for hint in group.iter()] == [first, second]


@pytest.mark.tourillon
async def test_hint_promotion_rejects_unowned_records_before_opening_session() -> None:
    storage = _FakeStore()
    provider = _FakeSessionProvider(storage)
    group = PartitionGroupHint(GroupSessionManager(provider))
    record = _record(7, 1)
    hinted = _with_tag(record, Tag.hint(record.tag))

    with pytest.raises(ValueError, match="promotion requires"):
        await group.promote([record, hinted])

    assert provider.opened == 0
    assert storage.list(Keyspace.TAGS) == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_hint_satisfy_deletes_only_exact_obligation_and_keeps_version() -> None:
    store = _FakeStore()
    record = _record(7, 1)
    delivered = _hint(record, "node-b", 600)
    newer = _hint(record, "node-b", 601)
    hints = _hints(store)
    await hints.put(delivered)
    await hints.put(newer)

    await hints.satisfy([delivered])
    await hints.satisfy([delivered])
    assert _stored_tag(store, record) == delivered.record.tag
    assert (
        store.get(KVRef(record.key.to_bytes(Keyspace.LOGS), Keyspace.LOGS)) is not None
    )
    remaining = store.list(Keyspace.META)
    assert len(remaining) == 1
    assert HintKey.from_bytes(remaining[0].key) == HintKey(newer.intent, newer.hlc)


@pytest.mark.tourillon
async def test_hint_satisfy_deletes_in_one_session_without_reading() -> None:
    class NoReadStore(_FakeStore):
        forbid_reads = False

        def get(self, ref: KVRef) -> KVPair | None:
            assert not self.forbid_reads, "satisfaction must not read before deleting"
            return super().get(ref)

    store = NoReadStore()
    provider = _FakeSessionProvider(store)
    hints = PartitionGroupHint(GroupSessionManager(provider))
    delivered = [_hint(_record(7, idx), "node-b", 800 + idx) for idx in (1, 2)]
    for hint in delivered:
        await hints.put(hint)
    store.forbid_reads = True
    before = provider.opened

    await hints.satisfy([])
    assert provider.opened == before
    await hints.satisfy(delivered)

    assert provider.opened == before + 1
    assert store.list(Keyspace.META) == []
    assert len(store.list(Keyspace.TAGS)) == 2
    assert len(store.list(Keyspace.LOGS)) == 2


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_hint_iter_pages_with_short_sessions_and_deleted_boundary() -> None:
    store = _FakeStore()
    provider = _FakeSessionProvider(store)
    hints = PartitionGroupHint(GroupSessionManager(provider))
    expected = [_hint(_record(7, idx), "node-a", 700 + idx) for idx in range(1, 6)]
    for hint in expected:
        await hints.put(hint)

    before_scan = provider.opened
    iterator = hints.iter(batch_size=2)
    found = [await anext(iterator), await anext(iterator)]
    assert provider.active == 0
    boundary = found[-1]
    store.delete(
        KVRef(HintKey(boundary.intent, boundary.hlc).to_bytes(), Keyspace.META)
    )

    found.extend([hint async for hint in iterator])

    assert found == expected
    assert provider.opened - before_scan == 4
    assert provider.active == 0


@pytest.mark.tourillon
@pytest.mark.parametrize(
    "corruption",
    [
        "meta_key",
        "meta_reference",
        "empty_tag",
        "unknown_tag",
        "hint_tag",
        "value",
        "oversized_value",
    ],
)
@pytest.mark.parametrize("batch_size", [1, 2])
async def test_hint_iter_logs_invalid_entry_and_continues(
    corruption: str, batch_size: int, caplog: pytest.LogCaptureFixture
) -> None:
    store = _FakeStore()
    hints = _hints(store)
    invalid = _hint(_record(7, 1), "node-b", 800)
    following = _hint(_record(7, 2), "node-b", 801)
    invalid_key = await hints.put(invalid)
    await hints.put(following)
    key = invalid_key.to_bytes()
    if corruption == "meta_key":
        store.delete(KVRef(key, Keyspace.META))
        key = HintPrefix().to_bytes()
        reference = HintReference(invalid.record.key)
        store.put(KVPair(key, reference.to_bytes(), Keyspace.META))
    elif corruption == "meta_reference":
        store.put(KVPair(key, b"", Keyspace.META))
    else:
        encoded = {
            "empty_tag": b"",
            "unknown_tag": b"\xff",
            "hint_tag": TagKind.HINT.value,
            "value": b"",
            "oversized_value": (
                b"\x01" + b"x" * (MAX_VALUE_DATA_LEN + 1)
                if corruption == "oversized_value"
                else b""
            ),
        }[corruption]
        keyspace = (
            Keyspace.LOGS
            if corruption in {"value", "oversized_value"}
            else Keyspace.TAGS
        )
        store.put(KVPair(invalid.record.key.to_bytes(keyspace), encoded, keyspace))
    obligations = store.list(Keyspace.META)

    found = [hint async for hint in hints.iter(batch_size=batch_size)]

    assert found == [following]
    assert "Cannot decode hint obligation" in caplog.text
    assert key.hex() in caplog.text
    assert store.list(Keyspace.META) == obligations
    assert await hints.pending()


@pytest.mark.tourillon
async def test_hint_iter_propagates_storage_access_failure() -> None:
    class FailingReadStore(_FakeStore):
        def get(self, ref: KVRef) -> KVPair | None:
            raise OSError("storage unavailable")

    store = FailingReadStore()
    hint = _hint(_record(7, 1), "node-b", 800)
    hint_key = HintKey(hint.intent, hint.hlc)
    reference = HintReference(hint.record.key)
    store.put(KVPair(hint_key.to_bytes(), reference.to_bytes(), Keyspace.META))
    hints = _hints(store)

    with pytest.raises(OSError, match="storage unavailable"):
        _ = [item async for item in hints.iter()]

    assert await hints.pending()


@pytest.mark.tourillon
@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [Keyspace.TAGS, Keyspace.LOGS])
@pytest.mark.parametrize("batch_size", [1, 2])
async def test_hint_iter_logs_missing_version_and_continues(
    missing: Keyspace, batch_size: int, caplog: pytest.LogCaptureFixture
) -> None:
    store = _FakeStore()
    hint = _hint(_record(7, 1), "node-b", 800)
    following = _hint(_record(7, 2), "node-b", 801)
    hints = _hints(store)
    await hints.put(hint)
    await hints.put(following)
    store.delete(KVRef(hint.record.key.to_bytes(missing), missing))
    obligations = store.list(Keyspace.META)

    found = [item async for item in hints.iter(batch_size=batch_size)]

    assert found == [following]
    assert "Hint obligation references a missing version" in caplog.text
    assert store.list(Keyspace.META) == obligations
    assert await hints.pending()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_hint_put_rolls_back_all_keyspaces_on_storage_failure(tmp_path) -> None:
    import lmdb

    from tourillon.core.storage.group import PartitionGroupHint
    from tourillon.infra.storage.lmdb_session import (
        LmdbConfig,
        LmdbSessionProvider,
    )

    provider = LmdbSessionProvider(
        LmdbConfig(path=tmp_path / "hints", map_size=1024 * 1024), group=0
    )
    record = _record(7, 1)
    oversized = Record(
        key=record.key,
        value=Value(data=b"x" * (2 * 1024 * 1024), quorum_write=1),
        tag=Tag.hint(record.tag),
    )
    hint = Hint(oversized, "node-b", HLCTimestamp(900, 0, "hint"))
    try:
        with pytest.raises(lmdb.MapFullError):
            await PartitionGroupHint(GroupSessionManager(provider)).put(hint)

        async with provider.session() as session:
            for keyspace in (Keyspace.TAGS, Keyspace.LOGS):
                assert (
                    await session.get(KVRef(oversized.key.to_bytes(keyspace), keyspace))
                    is None
                )
            assert (
                await session.get(
                    KVRef(HintKey(hint.intent, hint.hlc).to_bytes(), Keyspace.META)
                )
                is None
            )
    finally:
        await provider.close()


@pytest.mark.tourillon
def test_partition_group_staging_rejects_segment_outside_its_group() -> None:
    with pytest.raises(
        ValueError, match=r"segment \[0, 1\] is not handled by group 0 \[7, 8\]"
    ):
        _staging(_FakeStore(), segment=(0, 1))
    with pytest.raises(ValueError, match="is not handled"):
        _staging(_FakeStore(), segment=(8, 7))


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_returns_the_version_stored_at_that_exact_timestamp() -> None:
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

    async with GroupSessionManager(_FakeSessionProvider(store)).session() as session:
        found = await session.read(older.key)

    # `get` would answer the newest version; `read` answers the one asked for.
    assert found == older


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_returns_none_for_a_timestamp_never_written() -> None:
    store = _FakeStore()
    record = _record(7, 1)
    _seed_record(store, record)
    absent = Key(
        pid=record.key.pid,
        address=record.key.address,
        ts=HLCTimestamp(wall_ms=1, counter=0, node_id="n"),
    )

    async with GroupSessionManager(_FakeSessionProvider(store)).session() as session:
        assert await session.read(absent) is None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_returns_none_when_the_tag_is_missing() -> None:
    store = _FakeStore()
    record = _record(7, 1)
    store.put(
        KVPair(
            key=record.key.to_bytes(Keyspace.LOGS),
            value=record.value.to_bytes(),
            keyspace=Keyspace.LOGS,
        )
    )

    async with GroupSessionManager(_FakeSessionProvider(store)).session() as session:
        assert await session.read(record.key) is None
