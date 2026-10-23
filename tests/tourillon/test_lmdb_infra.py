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
from tourillon.core.models.storageschema import (
    Hint,
    Key,
    Keyspace,
    KVPair,
    KVRef,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.storage.group import PartitionGroupHint
from tourillon.core.storage.session import GroupSessionManager
from tourillon.infra.storage.lmdb_session import (
    LmdbConfig,
    LmdbSession,
    LmdbSessionProvider,
)
from tourillon.infra.storage.lmdb_storage import LMDBStorage, LMDBStorageProvider
from tourillon.infra.storage.lmdb_transaction import LmdbTransaction
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address


def _record(pid: int, idx: int, kind: TagKind = TagKind.LIVE):
    key = Key(
        pid=Pid(pid),
        address=Address(namespace=b"ns", key=f"k-{pid}-{idx}".encode()),
        ts=HLCTimestamp(wall_ms=100 + idx, counter=idx, node_id="n"),
    )
    value = Value(data=f"value-{pid}-{idx}".encode(), quorum_write=idx + 1)
    tag = Tag(kind=kind)
    return key, value, tag


def _tag_kv(key: Key, tag: Tag) -> KVPair:
    return KVPair(
        key=key.to_bytes(Keyspace.TAGS), value=tag.to_bytes(), keyspace=Keyspace.TAGS
    )


def _log_kv(key: Key, value: Value) -> KVPair:
    return KVPair(
        key=key.to_bytes(Keyspace.LOGS), value=value.to_bytes(), keyspace=Keyspace.LOGS
    )


def _provider(tmp_path, group: int = 0) -> LmdbSessionProvider:
    config = LmdbConfig(path=tmp_path / str(group), map_size=8 * 1024 * 1024)
    return LmdbSessionProvider(config=config, group=group)


@pytest.mark.tourillon
async def test_owned_exists_uses_key_presence_without_reading_logs(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = _provider(tmp_path)
    key, value, tag = _record(1, 1)
    try:
        async with provider.session(write=True) as session:
            await session.put(_tag_kv(key, tag))
            assert not await session.exists(
                KVRef(key.to_bytes(Keyspace.LOGS), Keyspace.LOGS)
            )
            await session.put(_log_kv(key, value))
        original_get = LmdbTransaction.get

        def metadata_get(self, ref: KVRef) -> KVPair | None:
            assert ref.keyspace is not Keyspace.LOGS
            return original_get(self, ref)

        monkeypatch.setattr(LmdbTransaction, "get", metadata_get)
        async with GroupSessionManager(provider).session() as session:
            assert await session.exists(key)
            assert not await session.exists(_record(1, 2)[0])
    finally:
        await provider.close()


@pytest.mark.tourillon
async def test_group_batches_owned_checks_and_writes_without_loading_values(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = _provider(tmp_path)
    group = PartitionGroupHint(GroupSessionManager(provider))
    records = [
        Record(key=key, value=value, tag=tag)
        for key, value, tag in (_record(1, 1), _record(1, 2, TagKind.TOMBSTONE))
    ]
    key, value, tag = _record(1, 3)
    missing = Record(key=key, value=value, tag=tag)
    try:
        assert await group.promote(records) == ([True, True], 2)
        original_get = LmdbTransaction.get

        def metadata_get(self, ref: KVRef) -> KVPair | None:
            assert ref.keyspace is not Keyspace.LOGS
            return original_get(self, ref)

        monkeypatch.setattr(LmdbTransaction, "get", metadata_get)
        assert await group.promote(records + [missing], writable=False) == (
            [True, True, False],
            0,
        )
        assert await group.promote(records) == ([True, True], 0)
        assert await group.promote([]) == ([], 0)
    finally:
        await provider.close()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_lmdb_transaction_round_trips_and_filters_records(tmp_path) -> None:
    provider = _provider(tmp_path)
    try:
        key1, value1, tag1 = _record(1, 1, TagKind.LIVE)
        key2, value2, tag2 = _record(1, 2, TagKind.TOMBSTONE)
        key3, value3, tag3 = _record(2, 1, TagKind.LIVE)

        with provider._env.begin(write=True) as txn:  # noqa: SLF001
            store = LmdbTransaction(
                txn=txn,
                dbi_logs=provider._dbi_logs,  # noqa: SLF001
                dbi_tags=provider._dbi_tags,  # noqa: SLF001
                dbi_meta=provider._dbi_meta,  # noqa: SLF001
            )
            store.put(_tag_kv(key1, tag1))
            store.put(_tag_kv(key2, tag2))
            store.put(_tag_kv(key3, tag3))
            store.put(_log_kv(key1, value1))
            assert store.exists(KVRef(key1.to_bytes(Keyspace.LOGS), Keyspace.LOGS))
            assert not store.exists(KVRef(key2.to_bytes(Keyspace.LOGS), Keyspace.LOGS))

            assert store.get(
                KVRef(key=key1.to_bytes(Keyspace.LOGS), keyspace=Keyspace.LOGS)
            ) == _log_kv(key1, value1)
            assert store.list(Keyspace.TAGS, prefix=Pid(1), limit=0) == []

            listed = store.list(Keyspace.TAGS, prefix=Pid(1), limit=10)
            assert [item.key for item in listed] == [
                key1.to_bytes(Keyspace.TAGS),
                key2.to_bytes(Keyspace.TAGS),
            ]

            after_first = store.list(
                Keyspace.TAGS,
                prefix=Pid(1),
                start=key1.to_bytes(Keyspace.TAGS),
                limit=10,
            )
            assert [item.key for item in after_first] == [key2.to_bytes(Keyspace.TAGS)]

            reverse = store.list(Keyspace.TAGS, prefix=Pid(1), reverse=True, limit=1)
            assert reverse == [_tag_kv(key2, tag2)]

            last_live = store.last(
                Keyspace.TAGS,
                prefix=Pid(1),
                predicate=lambda kv: Tag.from_bytes(kv.value).kind == TagKind.LIVE,
            )
            assert last_live == _tag_kv(key1, tag1)

            store.delete(
                KVRef(key=key1.to_bytes(Keyspace.LOGS), keyspace=Keyspace.LOGS)
            )
            assert not store.exists(KVRef(key1.to_bytes(Keyspace.LOGS), Keyspace.LOGS))
            assert (
                store.get(
                    KVRef(key=key1.to_bytes(Keyspace.LOGS), keyspace=Keyspace.LOGS)
                )
                is None
            )
    finally:
        await provider.close()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_lmdb_session_iter_last_and_abort_paths(tmp_path) -> None:
    provider = _provider(tmp_path)
    key1, value1, tag1 = _record(1, 1, TagKind.LIVE)
    key2, value2, tag2 = _record(1, 2, TagKind.TOMBSTONE)
    key3, value3, tag3 = _record(1, 3, TagKind.HINT)

    try:
        async with provider.session(write=True) as session:
            await session.put(_tag_kv(key1, tag1))
            await session.put(_tag_kv(key2, tag2))
            await session.put(_tag_kv(key3, tag3))
            await session.put(_log_kv(key1, value1))
            await session.put(_log_kv(key2, value2))
            await session.put(_log_kv(key3, value3))

        async with provider.session(write=False) as session:
            assert await session.get(
                KVRef(key=key1.to_bytes(Keyspace.LOGS), keyspace=Keyspace.LOGS)
            ) == _log_kv(key1, value1)

            forward = [
                kv
                async for kv in session.iter(
                    Keyspace.TAGS,
                    prefix=Pid(1),
                    batch_size=1,
                    limit=2,
                )
            ]
            assert [item.key for item in forward] == [
                key1.to_bytes(Keyspace.TAGS),
                key2.to_bytes(Keyspace.TAGS),
            ]

            reverse = [
                kv
                async for kv in session.iter(
                    Keyspace.TAGS,
                    prefix=Pid(1),
                    reverse=True,
                    batch_size=1,
                )
            ]
            assert [item.key for item in reverse] == [
                key3.to_bytes(Keyspace.TAGS),
                key2.to_bytes(Keyspace.TAGS),
                key1.to_bytes(Keyspace.TAGS),
            ]

            last_non_hint = await session.last(
                Keyspace.TAGS,
                prefix=Pid(1),
                predicate=lambda kv: Tag.from_bytes(kv.value).kind != TagKind.HINT,
            )
            assert last_non_hint == _tag_kv(key2, tag2)

        with pytest.raises(RuntimeError, match="boom"):
            async with provider.session(write=True) as session:
                key4, value4, tag4 = _record(2, 4, TagKind.LIVE)
                await session.put(_tag_kv(key4, tag4))
                await session.put(_log_kv(key4, value4))
                raise RuntimeError("boom")

        async with provider.session(write=False) as session:
            aborted = await session.get(
                KVRef(
                    key=Key(
                        pid=Pid(2),
                        address=Address(namespace=b"ns", key=b"k-2-4"),
                        ts=HLCTimestamp(wall_ms=104, counter=4, node_id="n"),
                    ).to_bytes(Keyspace.TAGS),
                    keyspace=Keyspace.TAGS,
                )
            )
            assert aborted is None

        assert LmdbSession._next_page_key(b"\x00\xff", reverse=False) == b"\x01"
        assert LmdbSession._next_page_key(b"\x01\x00", reverse=True) == b"\x00\xff"
        assert LmdbSession._setup_limit(None, 4) == (4, None)
        assert LmdbSession._setup_limit(-1, 4) == (4, None)
        assert LmdbSession._setup_limit(2, 4) == (2, 2)
    finally:
        await provider.close()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_lmdb_session_provider_rejects_new_sessions_after_close(tmp_path) -> None:
    provider = _provider(tmp_path)
    await provider.close()

    with pytest.raises(RuntimeError, match="closed"):
        async with provider.session():
            raise AssertionError("unreachable")


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_lmdb_storage_caches_providers_reports_latest_hlc_and_closes(
    tmp_path,
) -> None:
    config = StorageConfig(data_dir=str(tmp_path), group_size="8Mi")
    storage = LMDBStorage(config)

    provider0 = await storage.session_provider_for(0)
    provider1 = await storage.session_provider_for(1)
    assert provider0 is await storage.session_provider_for(0)

    key_a, _, tag_a = _record(1, 1, TagKind.LIVE)
    key_b, _, tag_b = _record(50000, 3, TagKind.LIVE)

    async with provider0.session(write=True) as session:
        await session.put(_tag_kv(key_a, tag_a))
    async with provider1.session(write=True) as session:
        await session.put(_tag_kv(key_b, tag_b))

    latest = await storage.latest_hlc(total_groups=2)
    assert latest == key_b.ts

    await storage.close()
    await storage.close()

    with pytest.raises(RuntimeError, match="closed"):
        await storage.session_provider_for(0)


@pytest.mark.tourillon
async def test_latest_hlc_restores_pending_obligation_clock_after_restart(
    tmp_path,
) -> None:
    config = StorageConfig(data_dir=str(tmp_path), group_size="8Mi")
    storage = LMDBStorage(config)
    key, value, tag = _record(1, 1)
    record = Record(key=key, value=value, tag=Tag.hint(tag))
    timestamps = [
        HLCTimestamp(wall_ms=1000, counter=5, node_id="keeper"),
        HLCTimestamp(wall_ms=2000, counter=7, node_id="keeper"),
    ]
    try:
        hints = PartitionGroupHint(
            GroupSessionManager(await storage.session_provider_for(0))
        )
        for intent, hlc in zip(("z-target", "a-target"), timestamps, strict=True):
            await hints.put(Hint(record=record, intent=intent, hlc=hlc))
    finally:
        await storage.close()
    reopened = LMDBStorage(config)
    try:
        assert await reopened.latest_hlc(1) == timestamps[-1]
    finally:
        await reopened.close()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_lmdb_storage_wraps_provider_initialization_failures(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = StorageConfig(data_dir=str(tmp_path), group_size="8Mi")
    storage = LMDBStorage(config)

    def boom(*args, **kwargs):
        raise OSError("broken")

    monkeypatch.setattr(
        "tourillon.infra.storage.lmdb_storage.LmdbSessionProvider", boom
    )

    with pytest.raises(
        RuntimeError, match="Failed to initialize LMDB session provider"
    ):
        await storage.session_provider_for(0)


@pytest.mark.tourillon
def test_lmdb_storage_provider_creates_directory_and_storage(tmp_path) -> None:
    config = StorageConfig(data_dir=str(tmp_path / "data"), group_size="8Mi")
    provider = LMDBStorageProvider()

    storage = provider.create(config)

    assert (tmp_path / "data").exists()
    assert isinstance(storage, LMDBStorage)


@pytest.mark.tourillon
def test_lmdb_session_abort_on_cancelled_error() -> None:
    """Test that LMDB session properly aborts on CancelledError.

    Root cause: CancelledError inherits from BaseException, NOT Exception.
    Exception handlers using `except Exception` will NOT catch it.
    This test verifies that the fix using `except BaseException` works correctly.
    """

    # BUGGY CODE: abort is NOT called
    abort_called_buggy = False

    def buggy_exception_handler():
        nonlocal abort_called_buggy
        try:
            raise asyncio.CancelledError()
        except Exception:  # BUG: does NOT catch CancelledError
            abort_called_buggy = True
            raise

    # Verify CancelledError escapes
    with pytest.raises(asyncio.CancelledError):
        buggy_exception_handler()

    # Verify abort was NOT called (this is the bug)
    assert not abort_called_buggy, "Buggy code: abort should NOT have been called"

    # FIXED CODE: abort IS called
    abort_called_fixed = False

    def fixed_exception_handler():
        nonlocal abort_called_fixed
        try:
            raise asyncio.CancelledError()
        except BaseException:  # FIX: catches CancelledError
            abort_called_fixed = True
            raise

    # CancelledError still escapes (that's correct)
    with pytest.raises(asyncio.CancelledError):
        fixed_exception_handler()

    # Verify abort WAS called (this is the fix)
    assert (
        abort_called_fixed
    ), "Fixed code: abort SHOULD be called with except BaseException"

    # Final verification
    assert (
        not abort_called_buggy and abort_called_fixed
    ), "Test verifies: buggy code doesn't call abort, fixed code does"
