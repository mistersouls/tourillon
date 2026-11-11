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
    KeyTag,
    ObligationKey,
    PartitionAddress,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.storage.retention import RetentionPolicy
from tourillon.core.storage.session import DigestCell, SessionProvider
from tourillon.infra.storage.lmdb_storage import LmdbStorage
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address

pytestmark = pytest.mark.tourillon
policy = RetentionPolicy(RetentionConfig(versions=2))


@pytest.fixture
async def storage(tmp_path: Path) -> AsyncIterator[LmdbStorage]:
    storage = LmdbStorage(StorageConfig(data_dir=str(tmp_path), group_size="16Mi"))
    try:
        yield storage
    finally:
        await storage.close()


def _record(index: int) -> Record:
    return Record(
        key=Key(
            Pid(1), Address(namespace=b"ns", key=b"key"), HLCTimestamp(index, 0, "node")
        ),
        tag=Tag(TagKind.TOMBSTONE if index == 3 else TagKind.LIVE),
        value=Value(b"value", 1),
    )


async def test_shared_policy_reports_dispositions_and_lookup(
    storage: LmdbStorage,
) -> None:
    sessions = SessionProvider(storage)
    old, middle, latest = [_record(index) for index in range(1, 4)]

    async with sessions.session(group=0, write=True) as session:
        assert await policy.apply(session, [middle, latest]) == ["applied", "applied"]
        assert await policy.apply(session, [old, latest]) == [
            "ignored_retention",
            "already_present",
        ]
        assert await policy.apply(session, []) == []
        lookup = await policy.lookup(session, old.key)
        assert lookup.record is None
        assert lookup.outside_retention
        assert lookup.retained == (
            KeyTag(latest.key, latest.tag),
            KeyTag(middle.key, middle.tag),
        )
        assert await policy.collect(session, [old.key]) == (0, 0)

    async with sessions.session(group=0) as session:
        assert await session.read_record(latest.key) == latest


async def test_admission_is_recomputed_after_another_write_in_same_transaction(
    storage: LmdbStorage,
) -> None:
    sessions = SessionProvider(storage)

    async with sessions.session(group=0, write=True) as session:
        assert await policy.apply(session, [_record(1)]) == ["applied"]
        assert await policy.apply(session, [_record(2), _record(3)]) == [
            "applied",
            "applied",
        ]
        assert await policy.apply(session, [_record(1)]) == ["ignored_retention"]
        count, _ = await policy.collect(session, [_record(1).key] * 2)
        assert count == 1

    async with sessions.session(group=0) as session:
        assert await session.read_record(_record(1).key) is None
        assert await session.read_record(_record(3).key) == _record(3)
        assert await session.roots([1]) == [
            (1, DigestCell.of(_record(2).key) + DigestCell.of(_record(3).key))
        ]


async def test_policy_and_raw_mutations_rollback_together(
    storage: LmdbStorage,
) -> None:
    sessions = SessionProvider(storage)

    async with sessions.session(group=0, write=True) as session:
        await policy.apply(session, [_record(1), _record(2)])

    debt = ObligationKey("target", _record(1).key)
    with pytest.raises(RuntimeError, match="rollback"):
        async with sessions.session(group=0, write=True) as session:
            await session.put_record(_record(3))
            await session.put_obligation_many([debt])
            count, _ = await policy.collect(session, [_record(1).key])
            assert count == 1
            await session.flush()
            raise RuntimeError("rollback")

    async with sessions.session(group=0) as session:
        lookup = await policy.lookup(session, _record(1).key)
        assert lookup.record == _record(1)
        assert not lookup.outside_retention
        assert await session.read_record(_record(3).key) is None
        assert await session.obligation_keys(limit=10) == []
        assert await session.roots([1]) == [
            (1, DigestCell.of(_record(1).key) + DigestCell.of(_record(2).key))
        ]


async def test_raw_records_have_no_retention_and_identity_pages_separate_addresses(
    storage: LmdbStorage,
) -> None:
    sessions = SessionProvider(storage)
    other = replace(
        _record(3),
        key=replace(_record(3).key, address=Address(namespace=b"other", key=b"key")),
    )
    address = PartitionAddress(_record(1).key.pid, _record(1).key.address)
    other_address = PartitionAddress(other.key.pid, other.key.address)

    async with sessions.session(group=0, write=True) as records:
        assert await records.put_record_many(
            [_record(3), _record(2), _record(1), other]
        ) == [
            True,
            True,
            True,
            True,
        ]
        identities = await records.latest_identities_many(
            [address, other_address, address, other_address], 2
        )
        assert identities[address] == [
            KeyTag(_record(3).key, _record(3).tag),
            KeyTag(_record(2).key, _record(2).tag),
        ]
        assert identities[other_address] == [KeyTag(other.key, other.tag)]

    async with sessions.session(group=0, write=True) as session:
        count, _ = await policy.collect(session, [_record(1).key])
        assert count == 1
        assert await session.read_record(other.key) == other
