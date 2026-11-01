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
from dataclasses import dataclass
from pathlib import Path

import pytest

from tourillon.core.models.storageschema import (
    Key,
    Keyspace,
    KVPair,
    KVRef,
    MetaKind,
    ObligationKey,
    PartitionAddress,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.ports.storage import Session
from tourillon.core.storage.session import (
    BUCKET_PREFIX,
    DEFAULT_BUCKET_SHIFT,
    ROOT_PREFIX,
    DigestCell,
    GroupSessionManager,
    bucket_key,
    root_key,
)
from tourillon.infra.storage.lmdb_session import LmdbConfig, LmdbSessionProvider
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address

pytestmark = pytest.mark.tourillon


@pytest.fixture
async def provider(tmp_path: Path) -> AsyncIterator[LmdbSessionProvider]:
    provider = LmdbSessionProvider(
        config=LmdbConfig(path=tmp_path / "group", map_size=8 * 1024 * 1024),
        group=0,
    )
    try:
        yield provider
    finally:
        await provider.close()


def _record(index: int) -> Record:
    return Record(
        key=Key(
            pid=Pid(1),
            address=Address(namespace=b"ns", key=f"key-{index}".encode()),
            ts=HLCTimestamp(wall_ms=100 + index, counter=0, node_id="node"),
        ),
        tag=Tag(kind=TagKind.LIVE),
        value=Value(data=b"value", quorum_write=1),
    )


async def test_manager_flushes_digest_with_records(
    provider: LmdbSessionProvider,
) -> None:
    manager = GroupSessionManager(provider)
    first, second = _record(1), _record(2)
    async with manager.session(write=True) as session:
        assert await session.put(first)
    async with manager.session(write=True) as session:
        assert await session.put(second)
    async with provider.session() as raw:
        kv = await raw.get(KVRef(bucket_key(1, 0), Keyspace.META))
        assert kv is not None
        assert DigestCell.from_bytes(kv.value) == (
            DigestCell.of(first.key) + DigestCell.of(second.key)
        )
    async with manager.session() as session:
        assert await session.read(first.key) == first
        assert await session.read(second.key) == second


async def test_repeated_flush_preserves_cumulative_deltas(
    provider: LmdbSessionProvider,
) -> None:
    first, second = _record(1), _record(2)
    async with GroupSessionManager(provider).session(write=True) as session:
        await session.flush()
        assert await session.put(first)
        await session.flush()
        await session.flush()
        assert await session.put(second)
        assert not await session.put(first)
        await session.flush()
    async with provider.session() as raw:
        kv = await raw.get(KVRef(bucket_key(1, 0), Keyspace.META))
        assert kv is not None
        assert DigestCell.from_bytes(kv.value) == (
            DigestCell.of(first.key) + DigestCell.of(second.key)
        )


async def test_body_failure_rolls_back_records_and_flushed_digest(
    provider: LmdbSessionProvider,
) -> None:
    record = _record(1)
    with pytest.raises(RuntimeError, match="abort"):
        async with GroupSessionManager(provider).session(write=True) as session:
            await session.put(record)
            await session.flush()
            raise RuntimeError("abort")
    async with provider.session() as raw:
        assert await raw.get(KVRef(bucket_key(1, 0), Keyspace.META)) is None
        assert (
            await raw.get(KVRef(record.key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))
            is None
        )
        assert (
            await raw.get(KVRef(record.key.to_bytes(Keyspace.LOGS), Keyspace.LOGS))
            is None
        )


async def test_flush_failure_rolls_back_records(
    provider: LmdbSessionProvider,
) -> None:
    ref = KVRef(bucket_key(1, 0), Keyspace.META)
    # A malformed persisted cell makes the automatic flush fail on encoding.
    maximum = DigestCell(count=2**64 - 1)
    async with provider.session(write=True) as raw:
        await raw.put(KVPair(ref.key, maximum.to_bytes(), Keyspace.META))
    record = _record(1)
    with pytest.raises(OverflowError):
        async with GroupSessionManager(provider).session(write=True) as session:
            await session.put(record)
    async with provider.session() as raw:
        assert await raw.get(ref) == KVPair(ref.key, maximum.to_bytes(), Keyspace.META)
        assert (
            await raw.get(KVRef(record.key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))
            is None
        )
        assert (
            await raw.get(KVRef(record.key.to_bytes(Keyspace.LOGS), Keyspace.LOGS))
            is None
        )


async def test_batch_reads_preserve_missing_entries_and_duplicates(
    provider: LmdbSessionProvider,
) -> None:
    record, missing = _record(1), _record(2)
    manager = GroupSessionManager(provider)
    async with manager.session(write=True) as session:
        await session.put(record)
    async with manager.session() as session:
        keys = [missing.key, record.key, record.key]
        assert await session.get_many(keys) == [None, record, record]
        assert await session.read(missing.key) is None
        assert (
            await session.get(PartitionAddress(record.key.pid, record.key.address))
            == record
        )
        assert await session.get_many([]) == []


@pytest.mark.parametrize("missing_space", [Keyspace.TAGS, Keyspace.LOGS])
async def test_batch_read_requires_both_entries(
    provider: LmdbSessionProvider, missing_space: Keyspace
) -> None:
    record = _record(1)
    async with provider.session(write=True) as raw:
        for space, value in (
            (Keyspace.TAGS, record.tag.to_bytes()),
            (Keyspace.LOGS, record.value.to_bytes()),
        ):
            if space != missing_space:
                await raw.put(KVPair(record.key.to_bytes(space), value, space))
    async with GroupSessionManager(provider).session() as session:
        assert await session.get_many([record.key]) == [None]


@pytest.mark.parametrize("limit", [0, -1])
async def test_paged_session_reads_reject_invalid_limits(
    provider: LmdbSessionProvider, limit: int
) -> None:
    async with GroupSessionManager(provider).session() as session:
        with pytest.raises(ValueError, match="limit must be >= 1"):
            await session.obligation_page(None, limit)
        with pytest.raises(ValueError, match="limit must be >= 1"):
            await session.root_page(None, limit)


def _versioned(pid: int, wall_ms: int) -> Record:
    return Record(
        key=Key(
            pid=Pid(pid),
            address=Address(namespace=b"ns", key=f"key-{pid}-{wall_ms}".encode()),
            ts=HLCTimestamp(wall_ms=wall_ms, counter=0, node_id="node"),
        ),
        tag=Tag(kind=TagKind.LIVE),
        value=Value(data=b"value", quorum_write=1),
    )


@dataclass(frozen=True)
class _Prefix:
    raw: bytes

    def to_bytes(self) -> bytes:
        return self.raw


async def _meta_cells(raw: Session, prefix: bytes) -> dict[bytes, DigestCell]:
    async with raw.batch() as batch:
        batch.list(Keyspace.META, prefix=_Prefix(prefix))

    (page,) = await batch.result()

    return {kv.key: DigestCell.from_bytes(kv.value) for kv in page}


async def test_pid_root_combines_its_buckets_and_roots_are_contiguous(
    provider: LmdbSessionProvider,
) -> None:
    bucket = 1 << DEFAULT_BUCKET_SHIFT

    records = [
        _versioned(2, 10),
        _versioned(1, 10),
        _versioned(1, bucket + 10),
        _versioned(1, 3 * bucket),
    ]

    manager = GroupSessionManager(provider)

    async with manager.session(write=True) as session:
        assert await session.put_many(records[:2]) == [True, True]

    async with manager.session(write=True) as session:
        assert await session.put_many(records[2:]) == [True, True]

    async with provider.session() as raw:
        roots = await _meta_cells(raw, ROOT_PREFIX)
        buckets = await _meta_cells(raw, BUCKET_PREFIX)

    assert list(roots) == [root_key(1), root_key(2)]
    assert roots[root_key(2)] == DigestCell.of(records[0].key)

    assert roots[root_key(1)] == (
        DigestCell.of(records[1].key)
        + DigestCell.of(records[2].key)
        + DigestCell.of(records[3].key)
    )

    assert list(buckets) == [
        bucket_key(1, 0),
        bucket_key(1, 1),
        bucket_key(1, 3),
        bucket_key(2, 0),
    ]

    pid_one = [cell for key, cell in buckets.items() if key != bucket_key(2, 0)]
    assert sum(pid_one, DigestCell()) == roots[root_key(1)]


async def test_pid_root_flush_failure_rolls_back_records_and_cells(
    provider: LmdbSessionProvider,
) -> None:
    ref = KVRef(root_key(1), Keyspace.META)
    maximum = DigestCell(count=2**64 - 1)

    async with provider.session(write=True) as raw:
        await raw.put(KVPair(ref.key, maximum.to_bytes(), Keyspace.META))

    record = _record(1)

    with pytest.raises(OverflowError):
        async with GroupSessionManager(provider).session(write=True) as session:
            await session.put(record)

    async with provider.session() as raw:
        assert await raw.get(ref) == KVPair(ref.key, maximum.to_bytes(), Keyspace.META)
        assert await raw.get(KVRef(bucket_key(1, 0), Keyspace.META)) is None

        assert (
            await raw.get(KVRef(record.key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))
            is None
        )


async def test_put_many_never_rewrites_or_refolds_an_existing_version(
    provider: LmdbSessionProvider,
) -> None:
    record = _record(1)
    manager = GroupSessionManager(provider)

    async with manager.session(write=True) as session:
        assert await session.put_many([record, record]) == [True, False]
        assert await session.put_many([]) == []

    async with manager.session(write=True) as session:
        assert not await session.put(record)

    async with provider.session() as raw:
        kv = await raw.get(KVRef(bucket_key(1, 0), Keyspace.META))
        assert kv is not None
        assert DigestCell.from_bytes(kv.value) == DigestCell.of(record.key)


async def test_obligations_are_paged_removed_and_kept_out_of_digests(
    provider: LmdbSessionProvider,
) -> None:
    record = _record(1)
    first = ObligationKey("node-a", record.key)
    second = ObligationKey("node-b", record.key)
    manager = GroupSessionManager(provider)

    async with manager.session(write=True) as session:
        await session.put(record)
        assert not await session.has_obligations()
        await session.put_obligations([second, first, first])
        await session.put_obligations([])

    async with manager.session() as session:
        assert await session.has_obligations()
        page, cursor = await session.obligation_page(None, 1)
        assert page == [first]
        assert cursor == first.to_bytes()
        assert await session.obligation_page(cursor, 1) == (
            [second],
            second.to_bytes(),
        )
        assert await session.obligation_page(second.to_bytes(), 1) == ([], None)

    async with manager.session(write=True) as session:
        await session.remove_obligations([first, second])
        await session.remove_obligations([])

    async with manager.session() as session:
        assert not await session.has_obligations()
        assert await session.read(record.key) == record
    async with provider.session() as raw:
        kv = await raw.get(KVRef(bucket_key(1, 0), Keyspace.META))
        assert kv is not None
        assert DigestCell.from_bytes(kv.value) == DigestCell.of(record.key)


async def test_obligation_changes_roll_back_with_the_session(
    provider: LmdbSessionProvider,
) -> None:
    record = _record(1)
    obligation = ObligationKey("node-a", record.key)
    manager = GroupSessionManager(provider)

    with pytest.raises(RuntimeError, match="abort"):
        async with manager.session(write=True) as session:
            await session.put(record)
            await session.put_obligations([obligation])
            raise RuntimeError("abort")

    async with manager.session() as session:
        assert not await session.has_obligations()
        assert await session.read(record.key) is None

    async with manager.session(write=True) as session:
        await session.put_obligations([obligation])

    with pytest.raises(RuntimeError, match="abort"):
        async with manager.session(write=True) as session:
            await session.remove_obligations([obligation])
            raise RuntimeError("abort")

    async with manager.session() as session:
        assert await session.has_obligations()


async def test_obligation_pages_skip_unreadable_entries(
    provider: LmdbSessionProvider,
) -> None:
    record = _record(1)
    obligation = ObligationKey("node-a", record.key)
    corrupt = MetaKind.OBLIGATION.value + b"\x00"

    async with provider.session(write=True) as raw:
        await raw.put(KVPair(corrupt, b"", Keyspace.META))

    async with GroupSessionManager(provider).session(write=True) as session:
        await session.put_obligations([obligation])

    async with GroupSessionManager(provider).session() as session:
        page, cursor = await session.obligation_page(None, 1)
        assert page == []
        assert cursor == corrupt
        assert await session.obligation_page(cursor, 2) == ([obligation], None)
