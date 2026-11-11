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

from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path

import pytest

from tourillon.core.config import StorageConfig
from tourillon.core.models.config import RetentionConfig
from tourillon.core.models.storageschema import (
    Key,
    Keyspace,
    KeyTag,
    KVPair,
    KVRef,
    ObligationKey,
    PartitionAddress,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.storage.retention import RetentionPolicy
from tourillon.core.storage.session import (
    DEFAULT_BUCKET_SHIFT,
    DigestCell,
    SessionProvider,
    root_key,
)
from tourillon.infra.storage.lmdb_storage import LmdbStorage
from tourlib.exceptions import ValidationError
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address

pytestmark = pytest.mark.tourillon


@pytest.fixture
async def storage(tmp_path: Path) -> AsyncIterator[LmdbStorage]:
    storage = LmdbStorage(StorageConfig(data_dir=str(tmp_path), group_size="16Mi"))
    try:
        yield storage
    finally:
        await storage.close()


def _record(index: int, *, pid: int = 1) -> Record:
    return Record(
        key=Key(
            pid=Pid(pid),
            address=Address(namespace=b"ns", key=f"key-{index}".encode()),
            ts=HLCTimestamp(wall_ms=100 + index, counter=0, node_id="node"),
        ),
        tag=Tag(kind=TagKind.LIVE),
        value=Value(data=b"value", quorum_write=1),
    )


def _history(count: int, *, tombstone: bool = False) -> list[Record]:
    base = _record(0)
    return [
        replace(
            base,
            key=replace(
                base.key,
                ts=HLCTimestamp(
                    wall_ms=i << DEFAULT_BUCKET_SHIFT, counter=0, node_id="node"
                ),
            ),
            tag=Tag(
                kind=TagKind.TOMBSTONE if tombstone and i == count else TagKind.LIVE
            ),
        )
        for i in range(1, count + 1)
    ]


async def test_session_flushes_digest_with_records(storage: LmdbStorage) -> None:
    sessions = SessionProvider(storage)
    first, second = _record(1), _record(2)

    async with sessions.session(group=0, write=True) as session:
        assert await session.put_record(first)
    async with sessions.session(group=0, write=True) as session:
        assert await session.put_record(second)

    async with sessions.session(group=0) as session:
        assert await session.roots([1]) == [
            (1, DigestCell.of(first.key) + DigestCell.of(second.key))
        ]
        assert await session.read_record(first.key) == first
        assert await session.read_record(second.key) == second


@pytest.mark.parametrize("versions", [1, 2, 4])
async def test_collection_keeps_top_versions_and_current_tombstone(
    storage: LmdbStorage,
    versions: int,
) -> None:
    records = _history(5, tombstone=True)
    sessions = SessionProvider(storage)

    async with sessions.session(group=0, write=True) as session:
        await session.put_record_many(records)
        await session.put_record(_record(42))
        await session.put_obligation_many([ObligationKey("target", records[0].key)])

    async with sessions.session(group=0, write=True) as session:
        count, size = await RetentionPolicy(RetentionConfig(versions=versions)).collect(
            session, [record.key for record in records]
        )
        assert count == 5 - versions
        assert size > 0

    async with sessions.session(group=0) as session:
        assert [await session.read_record(record.key) for record in records] == [
            None
        ] * (5 - versions) + records[-versions:]
        assert await session.read_record(_record(42).key) == _record(42)
        assert await session.obligation_keys(limit=10) == [
            ObligationKey("target", records[0].key)
        ]
        root = (await session.roots([1]))[0][1]
        buckets = await session.list_buckets(1, limit=10)
        assert sum((cell for _, cell in buckets), DigestCell()) == root


async def test_dry_run_and_rollback_preserve_records_and_digests(
    storage: LmdbStorage,
) -> None:
    records = _history(3)
    sessions = SessionProvider(storage)

    async with sessions.session(group=0, write=True) as session:
        await session.put_record_many(records)

    async with sessions.session(group=0) as session:
        result = await RetentionPolicy(RetentionConfig(versions=1)).collect(
            session, [record.key for record in records], dry_run=True
        )
        assert result[0] == 2

    with pytest.raises(RuntimeError, match="rollback"):
        async with sessions.session(group=0, write=True) as session:
            await RetentionPolicy(RetentionConfig(versions=1)).collect(
                session, [record.key for record in records]
            )
            await session.flush()
            raise RuntimeError("rollback")

    async with sessions.session(group=0) as session:
        assert [await session.read_record(record.key) for record in records] == records
        assert await session.roots([1]) == [
            (1, sum((DigestCell.of(record.key) for record in records), DigestCell()))
        ]


async def test_retention_rejects_out_of_order_historical_arrivals(
    storage: LmdbStorage,
) -> None:
    records = _history(4)
    sessions = SessionProvider(storage)
    address = PartitionAddress(records[0].key.pid, records[0].key.address)
    versions = RetentionPolicy(RetentionConfig(versions=2))

    async with sessions.session(group=0, write=True) as session:
        assert await versions.apply(session, records[-2:]) == ["applied", "applied"]
        lookup = await versions.lookup(session, records[0].key)
        assert lookup.outside_retention
        assert await versions.apply(session, records[:2]) == [
            "ignored_retention",
            "ignored_retention",
        ]
        lookup = await versions.lookup(session, records[-1].key)
        assert not lookup.outside_retention
        assert await session.latest_identities(address, 2) == [
            KeyTag(record.key, record.tag) for record in reversed(records[-2:])
        ]


@pytest.mark.parametrize("versions", [0, -1, True, 65])
async def test_collection_rejects_invalid_retention_without_removing_records(
    storage: LmdbStorage,
    versions: int,
) -> None:
    record = _record(1)
    sessions = SessionProvider(storage)

    async with sessions.session(group=0, write=True) as session:
        await session.put_record(record)

    with pytest.raises(ValidationError, match="retention.versions"):
        async with sessions.session(group=0, write=True) as session:
            await RetentionPolicy(RetentionConfig(versions=versions)).collect(
                session, [record.key]
            )

    async with sessions.session(group=0) as session:
        assert await session.read_record(record.key) == record


@pytest.mark.parametrize("versions", [None, 0, True, 65])
def test_retention_policy_rejects_disabled_or_invalid_versions(
    versions: int | None,
) -> None:
    with pytest.raises(ValidationError, match="retention.versions"):
        RetentionConfig(versions=versions)


async def test_repeated_flush_preserves_cumulative_deltas(storage: LmdbStorage) -> None:
    sessions = SessionProvider(storage)
    first, second = _record(1), _record(2)

    async with sessions.session(group=0, write=True) as session:
        await session.flush()
        assert await session.put_record(first)
        await session.flush()
        await session.flush()
        assert await session.put_record(second)
        assert not await session.put_record(first)
        await session.flush()

    async with sessions.session(group=0) as session:
        assert await session.roots([1]) == [
            (1, DigestCell.of(first.key) + DigestCell.of(second.key))
        ]


async def test_body_failure_rolls_back_records_and_flushed_digest(
    storage: LmdbStorage,
) -> None:
    record = _record(1)
    sessions = SessionProvider(storage)

    with pytest.raises(RuntimeError, match="abort"):
        async with sessions.session(group=0, write=True) as session:
            await session.put_record(record)
            await session.flush()
            raise RuntimeError("abort")

    async with sessions.session(group=0) as session:
        assert await session.roots([1]) == [(1, DigestCell())]
        assert await session.read_record(record.key) is None


async def test_flush_failure_rolls_back_records(storage: LmdbStorage) -> None:
    maximum = DigestCell(count=2**64 - 1)
    record = _record(1)

    transactional = await storage.transactional_for(0)
    async with transactional.transaction(write=True) as txn:
        await txn.put(KVPair(root_key(1), maximum.to_bytes(), Keyspace.META)).get()

    with pytest.raises(OverflowError):
        async with SessionProvider(storage).session(group=0, write=True) as session:
            await session.put_record(record)

    async with transactional.transaction() as txn:
        assert await txn.get(KVRef(root_key(1), Keyspace.META)).get() == KVPair(
            root_key(1), maximum.to_bytes(), Keyspace.META
        )
        assert (
            await txn.get(
                KVRef(record.key.to_bytes(Keyspace.TAGS), Keyspace.TAGS)
            ).get()
            is None
        )


async def test_record_reads_preserve_missing_entries_and_duplicates(
    storage: LmdbStorage,
) -> None:
    sessions = SessionProvider(storage)
    record, missing = _record(1), _record(2)

    async with sessions.session(group=0, write=True) as session:
        await session.put_record(record)

    async with sessions.session(group=0) as session:
        assert await session.read_record_many(
            [missing.key, record.key, record.key]
        ) == [
            None,
            record,
            record,
        ]
        assert await session.read_record(missing.key) is None
        assert (
            await session.get_last_record(
                PartitionAddress(record.key.pid, record.key.address)
            )
            == record
        )
        assert await session.read_record_many([]) == []


@pytest.mark.parametrize("missing_space", [Keyspace.TAGS, Keyspace.LOGS])
async def test_record_read_requires_both_entries(
    storage: LmdbStorage, missing_space: Keyspace
) -> None:
    record = _record(1)
    other_space = Keyspace.LOGS if missing_space == Keyspace.TAGS else Keyspace.TAGS
    raw = KVPair(
        record.key.to_bytes(other_space),
        (
            record.value.to_bytes()
            if other_space == Keyspace.LOGS
            else record.tag.to_bytes()
        ),
        other_space,
    )

    transactional = await storage.transactional_for(0)
    async with transactional.transaction(write=True) as txn:
        await txn.put(raw).get()

    async with SessionProvider(storage).session(group=0) as session:
        assert await session.read_record(record.key) is None


async def test_root_and_bucket_pages_are_bounded_and_contiguous(
    storage: LmdbStorage,
) -> None:
    sessions = SessionProvider(storage)
    bucket = 1 << DEFAULT_BUCKET_SHIFT
    records = [
        _record(10, pid=2),
        _record(11, pid=1),
        _record(12, pid=1),
        _record(13, pid=1),
    ]
    records = [
        replace(
            record,
            key=replace(
                record.key,
                ts=HLCTimestamp(
                    wall_ms=(
                        10 if index < 2 else bucket + 10 if index == 2 else 3 * bucket
                    ),
                    counter=0,
                    node_id="node",
                ),
            ),
        )
        for index, record in enumerate(records)
    ]

    async with sessions.session(group=0, write=True) as session:
        await session.put_record_many(records)

    async with sessions.session(group=0) as session:
        roots = await session.list_roots(limit=1)
        roots += await session.list_roots(after=roots[-1][0], limit=2)
        buckets = await session.list_buckets(1, limit=1)
        buckets += await session.list_buckets(1, after=buckets[-1][0], limit=3)
        assert [pid for pid, _ in roots] == [1, 2]
        assert [bucket_id for bucket_id, _ in buckets] == [0, 1, 3]
        assert await session.roots([1, 2]) == roots
        with pytest.raises(ValueError, match="limit must be >= 1"):
            await session.list_roots(limit=0)
        assert await session.list_buckets(1, limit=0) == []


async def test_put_record_is_idempotent_and_does_not_refold_existing_version(
    storage: LmdbStorage,
) -> None:
    sessions = SessionProvider(storage)
    record = _record(1)

    async with sessions.session(group=0, write=True) as session:
        assert await session.put_record_many([record, record]) == [True, False]
        assert await session.put_record_many([]) == []

    async with sessions.session(group=0, write=True) as session:
        assert not await session.put_record(record)

    async with sessions.session(group=0) as session:
        assert await session.roots([1]) == [(1, DigestCell.of(record.key))]


async def test_obligations_are_paged_removed_and_excluded_from_digests(
    storage: LmdbStorage,
) -> None:
    sessions = SessionProvider(storage)
    record = _record(1)
    first, second = (
        ObligationKey("node-a", record.key),
        ObligationKey("node-b", record.key),
    )

    async with sessions.session(group=0, write=True) as session:
        await session.put_record(record)
        await session.put_obligation_many([second, first, first])
        await session.put_obligation_many([])

    async with sessions.session(group=0) as session:
        assert await session.obligation_keys(limit=1) == [first]
        assert await session.obligation_keys(after=first.to_bytes(), limit=1) == [
            second
        ]
        assert await session.obligation_keys(after=second.to_bytes(), limit=1) == []

    async with sessions.session(group=0, write=True) as session:
        assert await session.remove_obligation_many([first, second]) == 2
        assert await session.remove_obligation_many([]) == 0

    async with sessions.session(group=0) as session:
        assert await session.obligation_keys(limit=10) == []
        assert await session.read_record(record.key) == record
        assert await session.roots([1]) == [(1, DigestCell.of(record.key))]


async def test_obligation_changes_roll_back_with_the_session(
    storage: LmdbStorage,
) -> None:
    sessions = SessionProvider(storage)
    record = _record(1)
    obligation = ObligationKey("node-a", record.key)

    with pytest.raises(RuntimeError, match="abort"):
        async with sessions.session(group=0, write=True) as session:
            await session.put_record(record)
            await session.put_obligation_many([obligation])
            raise RuntimeError("abort")

    async with sessions.session(group=0) as session:
        assert await session.obligation_keys(limit=10) == []
        assert await session.read_record(record.key) is None

    async with sessions.session(group=0, write=True) as session:
        await session.put_obligation_many([obligation])

    with pytest.raises(RuntimeError, match="abort"):
        async with sessions.session(group=0, write=True) as session:
            await session.remove_obligation_many([obligation])
            raise RuntimeError("abort")

    async with sessions.session(group=0) as session:
        assert await session.obligation_keys(limit=10) == [obligation]
