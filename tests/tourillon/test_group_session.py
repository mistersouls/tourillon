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

from tourillon.core.models.storageschema import (
    Hint,
    HintKey,
    HintReference,
    Key,
    Keyspace,
    KeyTag,
    KVPair,
    KVRef,
    PartitionAddress,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports.storage import Session
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.storage.group import (
    PartitionGroupHint,
    PartitionGroupStaging,
)
from tourillon.core.storage.session import (
    DigestCell,
    GroupSessionManager,
    bucket_key,
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
        assert await session.tags(keys) == [None, record.tag, record.tag]
        assert await session.read(missing.key) is None
        assert await session.exists(record.key)
        assert not await session.exists(missing.key)
        assert (
            await session.get(PartitionAddress(record.key.pid, record.key.address))
            == record
        )
        assert await session.get_many([]) == []
        assert await session.tags([]) == []


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
        assert not await session.exists(record.key)


async def test_exists_many_preserves_order_without_decoding_values(
    provider: LmdbSessionProvider,
) -> None:
    owned = _record(1)
    hint = replace(_record(2), tag=Tag.hint(Tag(TagKind.LIVE)))
    manager = GroupSessionManager(provider)

    async with manager.session(write=True) as session:
        await session.put_many([owned, hint])

    async with provider.session(write=True) as raw:
        await raw.put(KVPair(owned.key.to_bytes(Keyspace.LOGS), b"", Keyspace.LOGS))

    async with manager.session() as session:
        assert await session.exists_many(
            [hint.key, owned.key, _record(3).key, owned.key]
        ) == [False, True, False, True]
        assert await session.exists_many([]) == []


async def test_exists_many_rejects_unreadable_tags(
    provider: LmdbSessionProvider,
) -> None:
    record = _record(1)
    async with provider.session(write=True) as raw:
        await raw.put(KVPair(record.key.to_bytes(Keyspace.TAGS), b"", Keyspace.TAGS))

    async with GroupSessionManager(provider).session() as session:
        with pytest.raises(ValueError):
            await session.exists_many([record.key])


async def test_stage_many_retries_without_hiding_owned_versions(
    provider: LmdbSessionProvider,
) -> None:
    owned, incoming = _record(1), _record(2)
    manager = GroupSessionManager(provider)

    async with manager.session(write=True) as session:
        await session.put(owned)
        assert await session.stage_many([owned, incoming], "first") == 1
        assert await session.stage_many([owned, incoming], "second") == 1
        assert await session.stage_many([], "second") == 0
        assert await session.stage_many([owned], "second") == 0

    async with manager.session() as session:
        assert await session.read(owned.key) == owned
        assert await session.read(incoming.key) == replace(
            incoming, tag=Tag.staging(incoming.tag, "second")
        )

    async with provider.session() as raw:
        kv = await raw.get(KVRef(bucket_key(1, 0), Keyspace.META))
        assert kv is not None
        assert DigestCell.from_bytes(kv.value) == DigestCell.of(owned.key)


async def test_stage_many_rolls_back_source_changes_and_insertions(
    provider: LmdbSessionProvider,
) -> None:
    first, second = _record(1), _record(2)
    manager = GroupSessionManager(provider)
    async with manager.session(write=True) as session:
        await session.stage_many([first], "first")

    with pytest.raises(RuntimeError, match="abort"):
        async with manager.session(write=True) as session:
            await session.stage_many([first, second], "second")
            raise RuntimeError("abort")

    async with manager.session() as session:
        assert await session.read(first.key) == replace(
            first, tag=Tag.staging(first.tag, "first")
        )
        assert await session.read(second.key) is None


async def test_stage_many_rejects_unencodable_source_without_writing(
    provider: LmdbSessionProvider,
) -> None:
    record = _record(1)
    manager = GroupSessionManager(provider)

    with pytest.raises(OverflowError):
        async with manager.session(write=True) as session:
            await session.stage_many([record], "s" * 65536)

    async with manager.session() as session:
        assert await session.read(record.key) is None


@pytest.mark.parametrize("kind", [TagKind.LIVE, TagKind.TOMBSTONE])
@pytest.mark.parametrize("wrapper", ["hint", "staging", "nested"])
async def test_put_many_promotes_wrapped_versions_without_changing_values(
    provider: LmdbSessionProvider, kind: TagKind, wrapper: str
) -> None:
    owned = Tag(kind)
    wrappers = {
        "hint": Tag.hint(owned),
        "staging": Tag.staging(owned, "source"),
        "nested": Tag.staging(Tag.hint(owned), "source"),
    }
    record = replace(_record(1), tag=wrappers[wrapper])
    manager = GroupSessionManager(provider)
    async with manager.session(write=True) as session:
        await session.put(record)
    async with manager.session(write=True) as session:
        promoted = replace(record, tag=owned)
        assert await session.put_many([promoted, promoted]) == [
            True,
            False,
        ]
        assert await session.read(record.key) == promoted
        assert await session.put_many([promoted]) == [False]
        assert await session.put_many([]) == []
    async with provider.session() as raw:
        kv = await raw.get(KVRef(bucket_key(1, 0), Keyspace.META))
        assert kv is not None
        assert DigestCell.from_bytes(kv.value) == DigestCell.of(record.key)


async def test_store_and_hint_share_manager_and_update_digest(
    provider: LmdbSessionProvider,
) -> None:
    manager = GroupSessionManager(provider)
    hints = PartitionGroupHint(session_manager=manager)
    owned, pending = _record(1), _record(2)
    hint = Hint(
        record=replace(pending, tag=Tag.hint(pending.tag)),
        intent="peer",
        hlc=pending.key.ts,
    )
    async with manager.session(write=True) as session:
        assert await session.put(owned)
    await hints.put(hint)
    assert await hints.promote([pending], writable=False) == ([False], 0)
    assert await hints.promote([pending]) == ([True], 1)
    async with manager.session() as session:
        assert await session.read(pending.key) == pending
        assert await session.exists(pending.key)
    assert await hints.pending()
    async with provider.session() as raw:
        kv = await raw.get(KVRef(bucket_key(1, 0), Keyspace.META))
        assert kv is not None
        assert DigestCell.from_bytes(kv.value) == (
            DigestCell.of(owned.key) + DigestCell.of(pending.key)
        )


@pytest.mark.parametrize("kind", [TagKind.LIVE, TagKind.TOMBSTONE, TagKind.HINT])
async def test_staging_restores_exact_tag_and_updates_owned_digest(
    provider: LmdbSessionProvider, kind: TagKind
) -> None:
    manager = GroupSessionManager(provider)
    partitioner = Partitioner(pshift=4, gshift=1, hashspace=HashSpace(bits=8))
    staging = PartitionGroupStaging("source", manager, partitioner)
    tag = Tag.hint(Tag(TagKind.LIVE)) if kind is TagKind.HINT else Tag(kind)
    record = replace(_record(1), tag=tag)
    assert await staging.stage([record]) == 1
    async with provider.session() as raw:
        assert await raw.get(KVRef(bucket_key(1, 0), Keyspace.META)) is None
    assert await staging.commit() == 1
    assert await staging.commit() == 0
    async with manager.session() as session:
        assert await session.read(record.key) == record
    async with provider.session() as raw:
        kv = await raw.get(KVRef(bucket_key(1, 0), Keyspace.META))
        if kind is TagKind.HINT:
            assert kv is None
        else:
            assert kv is not None
            assert DigestCell.from_bytes(kv.value) == DigestCell.of(record.key)


async def test_staging_selects_and_activates_each_page_in_one_write_transaction(
    provider: LmdbSessionProvider, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = GroupSessionManager(provider)
    partitioner = Partitioner(pshift=4, gshift=1, hashspace=HashSpace(bits=8))
    staging = PartitionGroupStaging("source", manager, partitioner)
    assert await staging.stage([_record(1), _record(2)]) == 2
    modes: list[bool] = []
    open_session = provider.session

    @asynccontextmanager
    async def tracked_session(write: bool = False) -> AsyncIterator[Session]:
        modes.append(write)
        async with open_session(write=write) as raw:
            yield raw

    monkeypatch.setattr(provider, "session", tracked_session)

    assert await staging.commit(batch_size=1) == 2
    assert modes == [True, True, True]


async def test_staging_activation_flush_failure_rolls_back_the_page(
    provider: LmdbSessionProvider,
) -> None:
    manager = GroupSessionManager(provider)
    partitioner = Partitioner(pshift=4, gshift=1, hashspace=HashSpace(bits=8))
    staging = PartitionGroupStaging("source", manager, partitioner)
    record = _record(1)
    assert await staging.stage([record]) == 1
    ref = KVRef(bucket_key(1, 0), Keyspace.META)
    maximum = DigestCell(count=2**64 - 1)

    async with provider.session(write=True) as raw:
        await raw.put(KVPair(ref.key, maximum.to_bytes(), Keyspace.META))

    with pytest.raises(OverflowError):
        await staging.commit()

    async with manager.session() as session:
        assert await session.read(record.key) == replace(
            record, tag=Tag.staging(record.tag, "source")
        )
    async with provider.session() as raw:
        assert await raw.get(ref) == KVPair(ref.key, maximum.to_bytes(), Keyspace.META)


async def test_tag_updates_track_owned_transitions_and_duplicate_keys(
    provider: LmdbSessionProvider,
) -> None:
    record = _record(1)
    hint = Tag.hint(record.tag)
    manager = GroupSessionManager(provider)
    async with manager.session(write=True) as session:
        await session.put(record)
        await session.update_tags(
            [KeyTag(record.key, hint), KeyTag(record.key, record.tag)]
        )
        await session.flush()
        await session.update_tags([KeyTag(record.key, hint)])
    async with provider.session() as raw:
        assert await raw.get(KVRef(bucket_key(1, 0), Keyspace.META)) is None
    async with manager.session() as session:
        assert await session.read(record.key) == replace(record, tag=hint)


async def test_tag_update_failure_rolls_back_activation(
    provider: LmdbSessionProvider,
) -> None:
    record = _record(1)
    staged = replace(record, tag=Tag.staging(record.tag, "source"))
    manager = GroupSessionManager(provider)
    async with manager.session(write=True) as session:
        await session.put(staged)
    with pytest.raises(RuntimeError, match="abort"):
        async with manager.session(write=True) as session:
            await session.update_tags([KeyTag(record.key, record.tag)])
            await session.flush()
            raise RuntimeError("abort")
    async with manager.session() as session:
        assert await session.read(record.key) == staged
    async with provider.session() as raw:
        assert await raw.get(KVRef(bucket_key(1, 0), Keyspace.META)) is None


@pytest.mark.parametrize("missing_space", [Keyspace.TAGS, Keyspace.LOGS])
async def test_tag_updates_reject_incomplete_versions_before_any_change(
    provider: LmdbSessionProvider, missing_space: Keyspace
) -> None:
    first, second = _record(1), _record(2)
    manager = GroupSessionManager(provider)
    async with manager.session(write=True) as session:
        await session.put_many([first, second])

    async with provider.session(write=True) as raw:
        await raw.delete(KVRef(second.key.to_bytes(missing_space), missing_space))

    async with manager.session(write=True) as session:
        with pytest.raises(ValueError, match=f"without its {missing_space.name}"):
            await session.update_tags(
                [
                    KeyTag(first.key, Tag.hint(first.tag)),
                    KeyTag(second.key, Tag.hint(second.tag)),
                ]
            )
        assert await session.read(first.key) == first

    async with provider.session() as raw:
        kv = await raw.get(KVRef(bucket_key(1, 0), Keyspace.META))
        assert kv is not None
        assert DigestCell.from_bytes(kv.value) == (
            DigestCell.of(first.key) + DigestCell.of(second.key)
        )


async def test_tag_pages_advance_past_unreadable_entries(
    provider: LmdbSessionProvider,
) -> None:
    first, second = _record(1), _record(2)
    manager = GroupSessionManager(provider)
    async with manager.session(write=True) as session:
        await session.put(second)

    async with provider.session(write=True) as raw:
        await raw.put(KVPair(first.key.to_bytes(Keyspace.TAGS), b"", Keyspace.TAGS))

    async with manager.session() as session:
        entries, cursor = await session.tag_page(Pid(1).to_bytes(), 1)
        assert entries == []
        assert cursor == first.key.to_bytes(Keyspace.TAGS)
        entries, cursor = await session.tag_page(cursor, 1)
        assert entries == [KeyTag(second.key, second.tag)]
        assert await session.load_records(entries) == [second]
        assert cursor == second.key.to_bytes(Keyspace.TAGS)
        assert await session.tag_page(cursor, 1) == ([], None)
        assert await session.load_records([]) == []
        assert await session.last_staged_keys([], "source") == {}


@pytest.mark.parametrize("limit", [0, -1])
async def test_paged_session_reads_reject_invalid_limits(
    provider: LmdbSessionProvider, limit: int
) -> None:
    async with GroupSessionManager(provider).session() as session:
        with pytest.raises(ValueError, match="limit must be >= 1"):
            await session.tag_page(Pid(1).to_bytes(), limit)
        with pytest.raises(ValueError, match="limit must be >= 1"):
            await session.hint_page(None, limit)


async def test_hint_pages_advance_past_missing_versions_and_preserve_digests(
    provider: LmdbSessionProvider,
) -> None:
    owned, absent = _record(1), _record(2)
    first = HintKey("peer", owned.key.ts)
    second = HintKey("peer", absent.key.ts)
    manager = GroupSessionManager(provider)

    async with manager.session(write=True) as session:
        await session.put(owned)
        assert not await session.has_hints()
        await session.put_hint_obligation(first, HintReference(owned.key))
        await session.put_hint_obligation(second, HintReference(absent.key))

    async with manager.session() as session:
        assert await session.has_hints()
        hints, cursor = await session.hint_page(None, 1)
        assert hints == [
            Hint(
                record=replace(owned, tag=Tag.hint(owned.tag)),
                intent=first.intent,
                hlc=first.hlc,
            )
        ]
        assert cursor == first.to_bytes()
        assert await session.hint_page(cursor, 1) == ([], second.to_bytes())
        assert await session.hint_page(second.to_bytes(), 1) == ([], None)

    async with manager.session(write=True) as session:
        await session.remove_hint_obligations([first, second, first])
        await session.remove_hint_obligations([])

    async with manager.session() as session:
        assert not await session.has_hints()
        assert await session.read(owned.key) == owned
    async with provider.session() as raw:
        kv = await raw.get(KVRef(bucket_key(1, 0), Keyspace.META))
        assert kv is not None
        assert DigestCell.from_bytes(kv.value) == DigestCell.of(owned.key)


async def test_hint_obligation_changes_roll_back_with_the_session(
    provider: LmdbSessionProvider,
) -> None:
    record = _record(1)
    key = HintKey("peer", record.key.ts)
    manager = GroupSessionManager(provider)

    with pytest.raises(RuntimeError, match="abort"):
        async with manager.session(write=True) as session:
            await session.put_hint_obligation(key, HintReference(record.key))
            raise RuntimeError("abort")

    async with manager.session() as session:
        assert not await session.has_hints()

    async with manager.session(write=True) as session:
        await session.put_hint_obligation(key, HintReference(record.key))

    with pytest.raises(RuntimeError, match="abort"):
        async with manager.session(write=True) as session:
            await session.remove_hint_obligations([key])
            raise RuntimeError("abort")

    async with manager.session() as session:
        assert await session.has_hints()
