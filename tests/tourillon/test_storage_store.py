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
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path

import pytest

from tourillon.core.models.config import RetentionConfig, StorageConfig
from tourillon.core.models.storageschema import (
    Key,
    Keyspace,
    KVPair,
    MetaKind,
    ObligationKey,
    PartitionAddress,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports.storage import Transaction, Transactional
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.storage.retention import RetentionPolicy
from tourillon.core.storage.session import DigestCell
from tourillon.core.storage.store import LocalStore
from tourillon.infra.storage.lmdb_storage import LmdbStorage, StorageError
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address

pytestmark = pytest.mark.tourillon
PARTITIONER = Partitioner(pshift=2, gshift=1, hashspace=HashSpace(bits=8))


class _Storage:
    def __init__(self, path: Path) -> None:
        self.backend = LmdbStorage(StorageConfig(str(path), "32Mi"))
        self.fail_commit = False
        self.committed_writes = 0

    async def transactional_for(self, group: int) -> Transactional:
        backend = await self.backend.transactional_for(group)
        owner = self

        class Group:
            @asynccontextmanager
            async def transaction(
                self, write: bool = False
            ) -> AsyncIterator[Transaction]:
                async with backend.transaction(write=write) as txn:
                    yield txn
                    if write and owner.fail_commit:
                        raise OSError("commit failed")
                if write:
                    owner.committed_writes += 1

            async def close(self) -> None:
                await backend.close()

        return Group()

    async def close(self) -> None:
        await self.backend.close()


@pytest.fixture
async def storage(tmp_path: Path) -> AsyncIterator[_Storage]:
    backend = _Storage(tmp_path)
    try:
        yield backend
    finally:
        await backend.close()


def _store(storage: _Storage, *, shift: int = 22) -> LocalStore:
    return LocalStore(
        storage, PARTITIONER, RetentionPolicy(RetentionConfig(versions=2)), shift
    )


def _record(pid: int, timestamp: int) -> Record:
    return Record(
        key=Key(Pid(pid), Address(b"ns", b"key"), HLCTimestamp(timestamp, 0, "node")),
        value=Value(b"value", 1),
        tag=Tag(TagKind.LIVE),
    )


async def test_store_routes_all_surfaces_and_pages_obligations_globally(
    storage: _Storage,
) -> None:
    store = _store(storage)
    first, second = _record(0, 1), _record(3, 2)
    debts = [
        ObligationKey("z-target", first.key),
        ObligationKey("a-target", second.key),
    ]
    assert await store.put(first, debts[:1])
    assert await store.put(second, debts[1:])
    assert not await store.put(replace(first, value=Value(b"not rewritten", 1)))
    assert await store.read_at(first.key) == first
    assert (
        await store.get(PartitionAddress(second.key.pid, second.key.address)) == second
    )
    assert (await store.lookup(first.key)).record == first
    assert await store.latest_hlc() == second.key.ts
    page = await store.list_obligations(limit=1)
    assert page == [debts[1]]
    assert await store.list_obligations(page[-1].to_bytes(), 1) == [debts[0]]
    assert await store.list_obligations(debts[0].to_bytes(), 1) == []
    assert await store.record_from_obligations(debts) == {
        first.key: first,
        second.key: second,
    }
    await store.remove_obligations(debts)
    assert not await store.has_obligations()
    assert await store.roots([0, 1, 3]) == {
        0: DigestCell.of(first.key),
        1: DigestCell(),
        3: DigestCell.of(second.key),
    }


async def test_failed_commit_rolls_back_record_obligation_and_digest(
    storage: _Storage,
) -> None:
    store = _store(storage)
    record = _record(3, 10)
    storage.fail_commit = True
    with pytest.raises(OSError, match="commit failed"):
        await store.put(record, [ObligationKey("peer", record.key)])
    storage.fail_commit = False
    assert await store.read_at(record.key) is None
    assert not await store.has_obligations()
    assert await store.roots([3]) == {3: DigestCell()}


async def test_collection_commit_failure_preserves_retention_and_digests(
    storage: _Storage,
) -> None:
    store = _store(storage, shift=2)
    records = [_record(0, i) for i in (4, 8, 12)]
    for record in records:
        await store.put(record)
    assert [bucket for bucket, _ in await store.list_buckets(0)] == [1, 2, 3]
    storage.fail_commit = True
    with pytest.raises(OSError, match="commit failed"):
        await store.collect_address(PartitionAddress(Pid(0), records[0].key.address), 3)
    storage.fail_commit = False
    assert await store.read_at(records[0].key) == records[0]
    keys, count, size = await store.collect_page(0, None, 3, dry_run=True)
    assert len(keys) == 3 and count == 1 and size > 0
    assert await store.read_at(records[0].key) == records[0]
    _, count, _ = await store.collect_page(0, None, 3)
    assert count == 1
    assert await store.read_at(records[0].key) is None
    assert [bucket for bucket, _ in await store.list_buckets(0)] == [2, 3]
    assert await store.range_digest(b"\0" * 4, Pid(1).to_bytes()) == (
        DigestCell.of(records[1].key) + DigestCell.of(records[2].key)
    )


async def test_store_rejects_cross_partition_mutations_before_any_write(
    storage: _Storage,
) -> None:
    store = _store(storage)
    record, foreign = _record(0, 1), _record(3, 2)
    with pytest.raises(ValueError, match="requested partition"):
        await store.apply_result(0, [record, foreign])
    with pytest.raises(ValueError, match="stored version"):
        await store.put(record, [ObligationKey("peer", foreign.key)])
    with pytest.raises(ValueError, match="requested partition"):
        await store.read(0, [foreign.key], 1)
    assert await store.list_root() == []
    assert not await store.has_obligations()


@pytest.mark.parametrize("limit", [0, -1])
async def test_store_rejects_invalid_page_limits(storage: _Storage, limit: int) -> None:
    store = _store(storage)
    with pytest.raises(ValueError):
        await store.list_root(limit=limit)
    with pytest.raises(ValueError):
        await store.list_buckets(0, limit=limit)
    with pytest.raises(ValueError):
        await store.buckets_until(0, limit=limit)
    with pytest.raises(ValueError):
        await store.list_obligations(limit=limit)
    with pytest.raises(ValueError):
        await store.keys(Pid(0).to_bytes(), Pid(1).to_bytes(), limit)
    with pytest.raises(ValueError):
        await store.collect_page(0, None, limit)


async def test_store_empty_pages_and_missing_versions(storage: _Storage) -> None:
    store = _store(storage)
    record = _record(0, 1)
    assert await store.latest_hlc() == HLCTimestamp(0, 0, "")
    assert await store.read_at(record.key) is None
    assert await store.read(0, [record.key], 1) == ([], 1)
    assert await store.read(0, [], 1) == ([], 0)
    assert (
        await store.record_from_obligations([ObligationKey("peer", record.key)]) == {}
    )
    assert await store.apply_result(0, []) == {
        "applied": 0,
        "already_present": 0,
        "ignored_retention": 0,
    }
    await store.remove_obligations([])
    assert await store.list_root(after=PARTITIONER.total_partitions - 1) == []
    assert await store.keys(Pid(0).to_bytes(), Pid(1).to_bytes(), 1) == ([], None)
    with pytest.raises(ValueError, match="max_bytes"):
        await store.read(0, [], 0)


async def test_obligation_pages_log_and_skip_unreadable_entries(
    storage: _Storage, caplog: pytest.LogCaptureFixture
) -> None:
    store = _store(storage)
    record = _record(0, 1)
    debt = ObligationKey("peer", record.key)
    await store.put(record, [debt])
    backend = await storage.transactional_for(0)
    async with backend.transaction(write=True) as txn:
        await txn.put(
            KVPair(MetaKind.OBLIGATION.value + b"\x00", b"", Keyspace.META)
        ).get()

    assert await store.list_obligations(limit=1) == [debt]
    assert await store.list_obligations(debt.to_bytes(), 1) == []
    assert "storage.obligation unreadable" in caplog.text


async def test_failed_deferred_operations_abort_the_transaction(
    storage: _Storage,
) -> None:
    store = _store(storage)
    record = _record(0, 1)
    backend = await storage.transactional_for(0)
    with pytest.raises(StorageError, match="batch execution"):
        async with backend.transaction(write=True) as txn:
            txn.put(
                KVPair(
                    record.key.to_bytes(Keyspace.LOGS),
                    record.value.to_bytes(),
                    Keyspace.LOGS,
                )
            )
            txn.put(KVPair(b"", b"value", Keyspace.LOGS))
            await txn.results()

    assert await store.get(PartitionAddress(record.key.pid, record.key.address)) is None


@pytest.mark.parametrize("count, transactions", [(64, 1), (65, 2)])
async def test_ae_application_bounds_transactions_by_record_count(
    storage: _Storage, count: int, transactions: int
) -> None:
    store = _store(storage)
    records = [_record(0, i) for i in range(count)]
    result = await store.apply_result(0, records)
    assert result == {
        "applied": count,
        "already_present": 0,
        "ignored_retention": 0,
    }
    assert storage.committed_writes == transactions
    assert await store.read_at(records[-1].key) == records[-1]


async def test_ae_application_bounds_transactions_by_encoded_bytes(
    storage: _Storage,
) -> None:
    store = _store(storage)
    records = [
        replace(_record(0, i), value=Value(b"x" * (4 * 1024 * 1024), 1)) for i in (1, 2)
    ]
    assert sum(record.size() for record in records) > 8 * 1024 * 1024
    assert (await store.apply_result(0, records))["applied"] == 2
    assert storage.committed_writes == 2
    assert await store.read_at(records[-1].key) == records[-1]
