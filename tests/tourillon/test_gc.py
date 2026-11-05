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
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from tests.helpers.telemetry import RecordingTelemetry
from tourillon.core.models.config import RetentionConfig
from tourillon.core.models.storageschema import (
    Key,
    Keyspace,
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
def telemetry() -> RecordingTelemetry:
    return RecordingTelemetry()


@pytest.fixture
async def gc(
    tmp_path: Path, telemetry: RecordingTelemetry
) -> AsyncIterator[GarbageCollector]:
    provider = LmdbSessionProvider(LmdbConfig(tmp_path / "group", map_size=1 << 24), 0)
    worker = GarbageCollector(
        _Storage(provider),
        Partitioner(pshift=1, gshift=0, hashspace=HashSpace(bits=8)),
        RetentionConfig(versions=1, interval=600, batch_size=1, hotqueue_size=1),
        node_id="node",
        telemetry=telemetry.as_port(),
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


def _record_bytes(record: Record) -> int:
    return len(record.to_bytes(Keyspace.TAGS)) + len(record.key.to_bytes(Keyspace.LOGS))


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
    telemetry: RecordingTelemetry,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="tourillon.global")
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
    assert (
        telemetry.total("gc_versions_total", event="collectible", mode="estimate") == 3
    )
    assert telemetry.total("gc_versions_total", event="collected") == 0
    assert telemetry.total("gc_record_bytes_removed_total") == 0
    async with manager.session() as session:
        assert await session.read(_record(1).key) == _record(1)
    gc.request()
    await _completed(gc)
    assert gc.status.scanned == 4
    assert gc.status.collected == 3
    assert gc.status.to_dict()["bytes_removed"] > 0
    assert gc.status.bytes_removed == 3 * _record_bytes(_record(1))
    assert telemetry.total("gc_versions_total", event="collected", source="scan") == 3
    assert telemetry.total("gc_versions_total", event="scanned", mode="collect") == 4
    assert telemetry.total("gc_record_bytes_removed_total") == 3 * _record_bytes(
        _record(1)
    )
    assert (
        telemetry.total("gc_passes_total", source="requested", outcome="completed") == 2
    )
    assert len(telemetry.calls["gc_pass_duration_seconds"]) == 2
    assert all(
        duration >= 0
        for duration, _ in telemetry.calls["gc_transaction_duration_seconds"]
    )
    assert telemetry.spans == []
    async with manager.session() as session:
        assert await session.read(_record(1).key) is None
        assert await session.read(_record(4).key) == _record(4)
    await gc.stop()
    await gc.start()
    gc.request()
    await _completed(gc)
    assert gc.status.collected == 0
    await gc.stop()
    assert caplog.text.count("gc.started node=node") == 2
    assert caplog.text.count("gc.stopped node=node") == 2
    assert "mode=requested" in caplog.text
    assert "mode=estimate scanned=4 collectible=3" in caplog.text
    assert "mode=requested scanned=4 collected=3" in caplog.text
    assert "job=" not in caplog.text
    assert "dry_run=" not in caplog.text
    assert "duration=" in caplog.text


async def test_generic_hot_queue_deduplicates_and_bounds_memory(
    gc: GarbageCollector,
    telemetry: RecordingTelemetry,
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
    assert telemetry.total("gc_versions_total", source="hot", event="collected") == 3
    assert telemetry.total(
        "gc_record_bytes_removed_total", source="hot"
    ) == 3 * _record_bytes(_record(1))
    assert telemetry.calls["gc_hot_queue_size"][-1][0] == 0


async def test_periodic_gc_and_cancelled_request(
    gc: GarbageCollector, telemetry: RecordingTelemetry
) -> None:
    await _seed(gc)
    gc.config = RetentionConfig(versions=1, interval=0.01, batch_size=1)
    await gc.start()
    async with asyncio.timeout(3):
        while gc.status.state != "completed":
            await asyncio.sleep(0.001)
    assert gc.status.collected == 3
    assert (
        telemetry.total("gc_passes_total", source="periodic", outcome="completed") >= 1
    )
    gc.request()
    await gc.stop()
    assert gc.status.state == "cancelled"


async def test_partition_gc_is_bounded_and_keeps_previous_job_results(
    gc: GarbageCollector,
) -> None:
    manager = await _seed(gc)
    other = Record(
        key=Key(Pid(1), _record(1).key.address, _record(1).key.ts),
        value=_record(1).value,
        tag=_record(1).tag,
    )
    async with manager.session(write=True) as session:
        await session.put(other)
    await gc.start()
    first = gc.request(dry_run=True, partition=0)
    await _completed(gc)
    (estimate,) = gc.jobs()
    assert (estimate.job_id, estimate.scanned, estimate.collected) == (
        first.job_id,
        4,
        3,
    )
    second = gc.request(partition=0)
    await _completed(gc)
    newest, oldest = gc.jobs()
    assert newest.job_id == second.job_id and newest.collected == 3
    assert oldest.job_id == first.job_id and oldest.dry_run
    assert gc.inspect().status.partition == 0
    assert gc.inspect().status.finished_at is not None
    async with manager.session() as session:
        assert await session.read(other.key) == other
        assert await session.read(_record(4).key) == _record(4)


@pytest.mark.parametrize("partition", [-1, 2, True])
async def test_invalid_gc_partition_is_rejected_without_queuing(gc, partition):
    await gc.start()
    with pytest.raises(ValueError, match="Partition"):
        gc.request(partition=partition)
    assert gc.status.state == "idle"


async def test_gc_results_expire_explicitly_after_bounded_history(
    gc: GarbageCollector,
) -> None:
    await gc.start()
    first = gc.request()
    await _completed(gc)
    for _ in range(64):
        gc.request()
        await _completed(gc)
    jobs = gc.jobs()
    assert len(jobs) == 64
    assert first.job_id not in {job.job_id for job in jobs}


async def test_gc_jobs_include_the_pass_in_progress_and_next_pass_time(
    gc: GarbageCollector,
) -> None:
    assert gc.jobs() == ()
    assert gc.next_pass_at is None
    await gc.start()
    assert gc.next_pass_at is not None
    queued = gc.request(dry_run=True)
    (current,) = gc.jobs()
    assert current.job_id == queued.job_id
    assert current.state in {"queued", "running", "completed"}
    await gc.stop()
    assert gc.next_pass_at is None


async def test_gc_failure_is_visible_and_can_be_retried(
    gc: GarbageCollector,
    monkeypatch: pytest.MonkeyPatch,
    telemetry: RecordingTelemetry,
    caplog: pytest.LogCaptureFixture,
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
    assert telemetry.total("gc_errors_total", source="scan") == 1
    assert telemetry.total("gc_passes_total", outcome="failed") == 1
    assert telemetry.total("gc_versions_total", event="collected") == 0
    assert "gc.transaction failed node=node group=0 source=scan" in caplog.text
    monkeypatch.setattr(gc._storage, "session_provider_for", original)
    gc.request()
    await _completed(gc)
    assert gc.status.state == "completed"


async def test_failed_gc_commit_does_not_report_removals(
    gc: GarbageCollector, telemetry: RecordingTelemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = await _seed(gc)
    provider = await gc._storage.session_provider_for(0)
    original = provider.session

    @asynccontextmanager
    async def fail_commit(write: bool = False):
        async with original(write=write) as session:
            yield session
            raise OSError("commit failed")

    monkeypatch.setattr(provider, "session", fail_commit)
    await gc.start()
    gc.request()
    await _completed(gc)
    assert gc.status.state == "failed"
    assert telemetry.total("gc_versions_total", event="collected") == 0
    assert telemetry.total("gc_record_bytes_removed_total") == 0
    monkeypatch.setattr(provider, "session", original)
    async with manager.session() as session:
        assert await session.read(_record(1).key) == _record(1)


async def test_hot_gc_failure_is_measured_and_periodic_pass_can_retry(
    gc: GarbageCollector, telemetry: RecordingTelemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _seed(gc)
    original = gc._storage.session_provider_for

    async def fail(group: int):
        raise OSError("unavailable")

    monkeypatch.setattr(gc._storage, "session_provider_for", fail)
    gc.submit(PartitionAddress(Pid(0), _record(1).key.address))
    await gc._hot_step()
    assert telemetry.total("gc_errors_total", source="hot") == 1
    assert telemetry.total("gc_record_bytes_removed_total") == 0
    assert telemetry.calls["gc_hot_queue_size"][-1][0] == 0
    monkeypatch.setattr(gc._storage, "session_provider_for", original)
    await gc.start()
    gc.request()
    await _completed(gc)
    assert telemetry.total("gc_versions_total", event="collected") == 3


async def test_cancelled_gc_pass_records_duration_without_errors_or_removals(
    gc: GarbageCollector, telemetry: RecordingTelemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = asyncio.Event()

    async def blocked(group: int):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(gc._storage, "session_provider_for", blocked)
    await gc.start()
    gc.request()
    async with asyncio.timeout(3):
        await entered.wait()
    await gc.stop()
    assert gc.status.state == "cancelled"
    assert telemetry.total("gc_passes_total", outcome="cancelled") == 1
    assert (
        telemetry.calls["gc_transaction_duration_seconds"][-1][1]["outcome"]
        == "cancelled"
    )
    assert telemetry.total("gc_errors_total") == 0
    assert telemetry.total("gc_record_bytes_removed_total") == 0


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
