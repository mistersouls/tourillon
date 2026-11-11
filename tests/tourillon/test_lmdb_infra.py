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

import pytest

from tourillon.core.config import StorageConfig
from tourillon.core.models.config import RetentionConfig
from tourillon.core.models.storageschema import (
    Key,
    Keyspace,
    KVPair,
    KVRef,
    ObligationKey,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.storage.retention import RetentionPolicy
from tourillon.core.storage.session import SessionProvider
from tourillon.core.storage.store import LocalStore
from tourillon.infra.storage.lmdb_storage import (
    LmdbConfig,
    LmdbStorage,
    LmdbStorageProvider,
    LmdbTransactional,
)
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address


def _record(pid: int, idx: int, kind: TagKind = TagKind.LIVE) -> Record:
    key = Key(
        pid=Pid(pid),
        address=Address(namespace=b"ns", key=f"k-{pid}-{idx}".encode()),
        ts=HLCTimestamp(wall_ms=100 + idx, counter=idx, node_id="n"),
    )
    return Record(
        key=key,
        value=Value(data=f"value-{pid}-{idx}".encode(), quorum_write=idx + 1),
        tag=Tag(kind=kind),
    )


def _tag_kv(record: Record) -> KVPair:
    return KVPair(
        record.key.to_bytes(Keyspace.TAGS),
        record.tag.to_bytes(),
        Keyspace.TAGS,
    )


def _log_kv(record: Record) -> KVPair:
    return KVPair(
        record.key.to_bytes(Keyspace.LOGS),
        record.value.to_bytes(),
        Keyspace.LOGS,
    )


def _config(tmp_path) -> StorageConfig:
    return StorageConfig(data_dir=str(tmp_path), group_size="8Mi")


@pytest.mark.tourillon
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("limit", [0, -1, 1, 2, 10])
@pytest.mark.parametrize("prefix", [None, Pid(1), Pid(3)])
async def test_key_only_pages_match_pair_pages_without_changing_values(
    tmp_path,
    reverse: bool,
    limit: int,
    prefix: Pid | None,
) -> None:
    transactional = LmdbTransactional(
        LmdbConfig(path=tmp_path / "group", map_size=8 * 1024 * 1024)
    )
    pairs = [
        KVPair(Pid(pid).to_bytes() + suffix, b"payload" * 1024, Keyspace.LOGS)
        for pid, suffix in [(0, b"a"), (1, b"a"), (1, b"b"), (2, b"a")]
    ]

    try:
        async with transactional.transaction(write=True) as txn:
            for pair in pairs:
                await txn.put(pair).get()

        async with transactional.transaction() as txn:
            listed = await txn.list(
                Keyspace.LOGS, prefix=prefix, limit=limit, reverse=reverse
            ).get()
            keys = await txn.keys(
                Keyspace.LOGS, prefix=prefix, limit=limit, reverse=reverse
            ).get()
            assert keys == [KVRef(pair.key, pair.keyspace) for pair in listed]
            assert all(pair.value == b"payload" * 1024 for pair in listed)
    finally:
        await transactional.close()


@pytest.mark.tourillon
@pytest.mark.parametrize("reverse", [False, True])
async def test_bounded_pages_preserve_start_bounds_and_deleted_cursor(
    tmp_path,
    reverse: bool,
) -> None:
    transactional = LmdbTransactional(
        LmdbConfig(path=tmp_path / "group", map_size=8 * 1024 * 1024)
    )
    prefix = Pid(1)
    keys = [prefix.to_bytes() + suffix for suffix in (b"a", b"b", b"c")]

    try:
        async with transactional.transaction(write=True) as txn:
            for key in keys:
                await txn.put(KVPair(key, b"value", Keyspace.LOGS)).get()
            await txn.put(
                KVPair(Pid(2).to_bytes() + b"a", b"other", Keyspace.LOGS)
            ).get()

            for start in (keys[1], prefix.to_bytes() + b"bb", b"\xff" * 8):
                listed = await txn.list(
                    Keyspace.LOGS, prefix=prefix, start=start, reverse=reverse
                ).get()
                listed_keys = await txn.keys(
                    Keyspace.LOGS, prefix=prefix, start=start, reverse=reverse
                ).get()
                assert listed_keys == [
                    KVRef(pair.key, pair.keyspace) for pair in listed
                ]

            await txn.delete(KVRef(keys[1], Keyspace.LOGS)).get()
            resumed = await txn.keys(
                Keyspace.LOGS, prefix=prefix, start=keys[1], reverse=reverse
            ).get()
            assert resumed == [KVRef(keys[0] if reverse else keys[2], Keyspace.LOGS)]
    finally:
        await transactional.close()


@pytest.mark.tourillon
async def test_lmdb_transaction_round_trips_and_filters_records(tmp_path) -> None:
    transactional = LmdbTransactional(
        LmdbConfig(path=tmp_path / "group", map_size=8 * 1024 * 1024)
    )

    try:
        live, tombstone, other = (
            _record(1, 1),
            _record(1, 2, TagKind.TOMBSTONE),
            _record(2, 1),
        )

        async with transactional.transaction(write=True) as txn:
            for record in (live, tombstone, other):
                await txn.put(_tag_kv(record)).get()
            await txn.put(_log_kv(live)).get()

            live_ref = KVRef(live.key.to_bytes(Keyspace.LOGS), Keyspace.LOGS)
            missing_ref = KVRef(tombstone.key.to_bytes(Keyspace.LOGS), Keyspace.LOGS)
            assert await txn.exists(live_ref).get()
            assert not await txn.exists(missing_ref).get()
            assert await txn.get(live_ref).get() == _log_kv(live)
            assert await txn.list(Keyspace.TAGS, prefix=Pid(1), limit=0).get() == []

            listed = await txn.list(Keyspace.TAGS, prefix=Pid(1), limit=10).get()
            assert [item.key for item in listed] == [
                live.key.to_bytes(Keyspace.TAGS),
                tombstone.key.to_bytes(Keyspace.TAGS),
            ]

            after_first = await txn.list(
                Keyspace.TAGS,
                prefix=Pid(1),
                start=live.key.to_bytes(Keyspace.TAGS),
                limit=10,
            ).get()
            assert [item.key for item in after_first] == [
                tombstone.key.to_bytes(Keyspace.TAGS)
            ]

            reverse = await txn.list(
                Keyspace.TAGS, prefix=Pid(1), reverse=True, limit=1
            ).get()
            assert reverse == [_tag_kv(tombstone)]

            last_live = await txn.last(
                Keyspace.TAGS,
                prefix=Pid(1),
                predicate=lambda kv: Tag.from_bytes(kv.value).kind == TagKind.LIVE,
            ).get()
            assert last_live == _tag_kv(live)

            assert await txn.delete(live_ref).get() == _log_kv(live)
            assert not await txn.exists(live_ref).get()
            assert await txn.get(live_ref).get() is None
    finally:
        await transactional.close()


@pytest.mark.tourillon
async def test_session_reads_bounded_pages_and_aborts_failed_writes(tmp_path) -> None:
    storage = LmdbStorage(_config(tmp_path))
    sessions = SessionProvider(storage)
    records = [_record(1, index) for index in range(1, 4)]

    try:
        async with sessions.session(group=0, write=True) as session:
            assert await session.put_record_many(records) == [True, True, True]

        async with sessions.session(group=0) as session:
            first = await session.keys(prefix=Pid(1), limit=2)
            second = await session.keys(
                prefix=Pid(1),
                after=first[-1].to_bytes(Keyspace.TAGS),
                limit=2,
            )
            assert first == [record.key for record in records[:2]]
            assert second == [records[2].key]
            assert await session.read_record(records[0].key) == records[0]

        aborted = _record(2, 4)
        with pytest.raises(RuntimeError, match="boom"):
            async with sessions.session(group=0, write=True) as session:
                await session.put_record(aborted)
                raise RuntimeError("boom")

        async with sessions.session(group=0) as session:
            assert await session.read_record(aborted.key) is None
    finally:
        await storage.close()


@pytest.mark.tourillon
async def test_lmdb_storage_caches_transactionals_restores_hlc_and_closes(
    tmp_path,
) -> None:
    storage = LmdbStorage(_config(tmp_path))
    partitioner = Partitioner(pshift=16, gshift=1, hashspace=HashSpace())
    store = LocalStore(storage, partitioner, RetentionPolicy(RetentionConfig(versions=2)))
    key_b = _record(50000, 3).key

    try:
        transactional0 = await storage.transactional_for(0)
        transactional1 = await storage.transactional_for(1)
        assert transactional0 is await storage.transactional_for(0)
        assert transactional0 is not transactional1

        async with SessionProvider(storage).session(group=0, write=True) as session:
            await session.put_record(_record(1, 1))
        async with SessionProvider(storage).session(group=1, write=True) as session:
            await session.put_record(_record(50000, 3))

        assert await store.latest_hlc() == key_b.ts
    finally:
        await storage.close()
        await storage.close()

    with pytest.raises(RuntimeError, match="closed"):
        await storage.transactional_for(0)


@pytest.mark.tourillon
async def test_latest_hlc_restores_versions_held_for_obligations(tmp_path) -> None:
    config = _config(tmp_path)
    storage = LmdbStorage(config)
    partitioner = Partitioner(pshift=16, gshift=0, hashspace=HashSpace())
    records = [_record(1, 1), _record(1, 4)]

    try:
        async with SessionProvider(storage).session(group=0, write=True) as session:
            await session.put_record_many(records)
            await session.put_obligation_many(
                [ObligationKey("peer", record.key) for record in records]
            )
    finally:
        await storage.close()

    reopened = LmdbStorage(config)
    try:
        assert (
            await LocalStore(
                reopened,
                partitioner,
                RetentionPolicy(RetentionConfig(versions=2)),
            ).latest_hlc()
            == records[-1].key.ts
        )
    finally:
        await reopened.close()


@pytest.mark.tourillon
async def test_lmdb_storage_wraps_transactional_initialization_failures(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = LmdbStorage(_config(tmp_path))

    def broken(*args, **kwargs):
        raise OSError("broken")

    monkeypatch.setattr(
        "tourillon.infra.storage.lmdb_storage.LmdbTransactional", broken
    )

    with pytest.raises(RuntimeError, match="Failed to initialize LMDB transactional"):
        await storage.transactional_for(0)


@pytest.mark.tourillon
def test_lmdb_storage_provider_creates_directory_and_storage(tmp_path) -> None:
    config = StorageConfig(data_dir=str(tmp_path / "data"), group_size="8Mi")

    storage = LmdbStorageProvider().create(config)

    assert (tmp_path / "data").exists()
    assert isinstance(storage, LmdbStorage)


@pytest.mark.tourillon
async def test_cancelled_transaction_aborts_pending_writes(tmp_path) -> None:
    transactional = LmdbTransactional(
        LmdbConfig(path=tmp_path / "group", map_size=8 * 1024 * 1024)
    )
    record = _record(1, 1)
    ref = KVRef(record.key.to_bytes(Keyspace.TAGS), Keyspace.TAGS)

    try:
        with pytest.raises(asyncio.CancelledError):
            async with transactional.transaction(write=True) as txn:
                await txn.put(_tag_kv(record)).get()
                raise asyncio.CancelledError()

        async with transactional.transaction() as txn:
            assert await txn.get(ref).get() is None
    finally:
        await transactional.close()
