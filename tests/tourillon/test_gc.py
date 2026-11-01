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
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from tourillon.core.models.config import RetentionConfig
from tourillon.core.models.storageschema import (
    Key,
    PartitionAddress,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.services.gc import GarbageCollector
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.storage.session import GroupSessionManager
from tourillon.infra.storage.lmdb_session import LmdbConfig, LmdbSessionProvider
from tourlib.exceptions import ValidationError
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address

pytestmark = pytest.mark.tourillon


class _Storage:
    def __init__(self, provider: LmdbSessionProvider) -> None:
        self.provider = provider

    async def session_provider_for(self, group: int) -> LmdbSessionProvider:
        assert group == 0
        return self.provider


@pytest.fixture
async def gc(tmp_path: Path) -> AsyncIterator[GarbageCollector]:
    provider = LmdbSessionProvider(LmdbConfig(tmp_path / "group", map_size=1 << 24), 0)
    worker = GarbageCollector(
        _Storage(provider),
        Partitioner(pshift=1, gshift=0, hashspace=HashSpace(bits=8)),
        RetentionConfig(versions=1, interval=600, batch_size=1, hotqueue_size=1),
    )
    try:
        yield worker
    finally:
        await worker.stop()
        await provider.close()


def _record(index: int) -> Record:
    return Record(
        key=Key(
            Pid(0), Address(namespace=b"ns", key=b"key"), HLCTimestamp(index, 0, "node")
        ),
        value=Value(b"value", 1),
        tag=Tag(TagKind.TOMBSTONE if index == 4 else TagKind.LIVE),
    )


async def _seed(gc: GarbageCollector) -> GroupSessionManager:
    manager = GroupSessionManager(await gc._storage.session_provider_for(0))
    async with manager.session(write=True) as session:
        await session.put_many([_record(i) for i in range(1, 5)])
    return manager


async def _completed(gc: GarbageCollector) -> None:
    async with asyncio.timeout(3):
        while gc.status.state in ("queued", "running"):
            await asyncio.sleep(0.001)


async def test_gc_request_dry_run_then_collection_and_restart(
    gc: GarbageCollector,
) -> None:
    manager = await _seed(gc)
    with pytest.raises(ValueError, match="not running"):
        gc.request()
    await gc.start()
    await gc.start()
    estimate = gc.request(dry_run=True)
    with pytest.raises(ValueError, match="already"):
        gc.request()
    await _completed(gc)
    assert gc.status.job_id == estimate.job_id
    assert gc.status.state == "completed"
    assert gc.status.collected == 3
    async with manager.session() as session:
        assert await session.read(_record(1).key) == _record(1)
    gc.request()
    await _completed(gc)
    assert gc.status.scanned == 4
    assert gc.status.collected == 3
    assert gc.status.to_dict()["bytes_removed"] > 0
    async with manager.session() as session:
        assert await session.read(_record(1).key) is None
        assert await session.read(_record(4).key) == _record(4)
    await gc.stop()
    await gc.start()
    gc.request()
    await _completed(gc)
    assert gc.status.collected == 0


async def test_generic_hot_queue_deduplicates_and_bounds_memory(
    gc: GarbageCollector,
) -> None:
    manager = await _seed(gc)
    address = PartitionAddress(Pid(0), _record(1).key.address)
    gc.submit(address)
    gc.submit(address)
    gc.submit(PartitionAddress(Pid(0), Address(b"ns", b"another")))
    assert len(gc._hot) == 1
    assert gc.overflowed == 1
    await gc.start()
    async with asyncio.timeout(3):
        while True:
            async with manager.session() as session:
                if await session.read(_record(3).key) is None:
                    break
            await asyncio.sleep(0.001)
    async with manager.session() as session:
        assert await session.read(_record(4).key) == _record(4)


async def test_periodic_gc_and_cancelled_request(gc: GarbageCollector) -> None:
    await _seed(gc)
    gc.config = RetentionConfig(versions=1, interval=0.01, batch_size=1)
    await gc.start()
    async with asyncio.timeout(3):
        while gc.status.state != "completed":
            await asyncio.sleep(0.001)
    assert gc.status.collected == 3
    gc.request()
    await gc.stop()
    assert gc.status.state == "cancelled"


async def test_gc_failure_is_visible_and_can_be_retried(
    gc: GarbageCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = gc._storage.session_provider_for

    async def fail(group: int):
        raise OSError("unavailable")

    monkeypatch.setattr(gc._storage, "session_provider_for", fail)
    await gc.start()
    gc.request()
    await _completed(gc)
    assert gc.status.state == "failed"
    assert gc.status.error == "OSError: unavailable"
    monkeypatch.setattr(gc._storage, "session_provider_for", original)
    gc.request()
    await _completed(gc)
    assert gc.status.state == "completed"


@pytest.mark.parametrize(
    "document",
    [
        {"versions": 0},
        {"versions": True},
        {"versions": 65},
        {"batch_size": 0},
        {"interval": float("nan")},
        {"interval": -1},
        {"hotqueue_size": 0},
    ],
)
def test_retention_config_rejects_invalid_limits(document) -> None:
    with pytest.raises(ValidationError):
        RetentionConfig.from_document(document)


def test_retention_config_round_trip() -> None:
    config = RetentionConfig(versions=3, interval=10, batch_size=2, hotqueue_size=8)
    assert RetentionConfig.from_document(config.to_document()) == config
