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
import ssl
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.helpers import start_envelope_server
from tests.tourillon.test_transfer_stack import (
    SERIALIZER,
    _FakePeerPool,
    _LoopbackClient,
    _TopologyManager,
)
from tourillon.bootstrap.handlers import transfer as transfer_handlers
from tourillon.core.config import StorageConfig
from tourillon.core.models.retry import BackoffRetry
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
from tourillon.core.models.transfer import (
    RangeTransfer,
    TransferChunk,
    TransferHandle,
    TransferPlan,
    TransferState,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports.storage import Storage
from tourillon.core.services.transfer import TransferService
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.storage.group import PartitionGroupStaging
from tourillon.core.storage.session import GroupSession, GroupSessionManager
from tourillon.core.transfer.applicator import TransferApplicator
from tourillon.core.transfer.stream import TransferStreamer, _read_frames
from tourillon.infra.storage.lmdb_storage import LMDBStorageProvider
from tourlib.envelope import Envelope
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address
from tourlib.transport import EnvelopeClient

# 16 pids in two LMDB groups: the range below spans both.
PARTITIONER = Partitioner(pshift=4, gshift=1, hashspace=HashSpace(bits=8))
RANGE = RangeTransfer(segment=(0, 15), src="node-a", dst="node-b")
PLAN = TransferPlan(ranges=[RANGE], serial="serial")


def _record(pid: int, idx: int, tag: Tag) -> Record:
    return Record(
        key=Key(
            pid=Pid(pid),
            address=Address(namespace=b"ns", key=f"key-{pid}-{idx}".encode()),
            ts=HLCTimestamp(wall_ms=1_000 + idx, counter=0, node_id="node-a"),
        ),
        value=Value(data=f"value-{pid}-{idx}".encode() * 4, quorum_write=1),
        tag=tag,
    )


def _dataset() -> tuple[list[Record], list[Record]]:
    """Return the records a transfer must carry and those it must leave behind."""
    tags = [
        Tag(kind=TagKind.LIVE),
        Tag(kind=TagKind.TOMBSTONE),
    ]
    transferable = [
        _record(pid, idx, tags[(pid + idx) % 2])
        for pid in range(16)
        for idx in range(6)
    ]
    left_behind = [
        _record(3, 98, Tag.hint(Tag(kind=TagKind.LIVE))),
        _record(3, 99, Tag.staging(Tag(kind=TagKind.LIVE), "node-z")),
        _record(9, 99, Tag(kind=TagKind.STALE)),
    ]
    return transferable, left_behind


def _storage(tmp_path: Path, name: str) -> Storage:
    config = StorageConfig(data_dir=str(tmp_path / name), group_size="8Mi")
    return LMDBStorageProvider().create(config)


async def _seed(storage: Storage, records: list[Record]) -> None:
    for record in records:
        provider = await storage.session_provider_for(
            PARTITIONER.group_for(record.key.pid.value)
        )
        async with GroupSessionManager(provider).session(write=True) as session:
            await session.put(record)


async def _tags(storage: Storage) -> dict[bytes, Tag]:
    tags: dict[bytes, Tag] = {}
    for group in range(PARTITIONER.total_groups):
        provider = await storage.session_provider_for(group)
        async with provider.session(write=False) as session:
            async with session.batch() as batch:
                batch.list(keyspace=Keyspace.TAGS)
            (kvs,) = await batch.result()
        tags.update({kv.key: Tag.from_bytes(kv.value) for kv in kvs})
    return tags


async def _staged_keys(storage: Storage) -> dict[int, Key]:
    staged: dict[int, Key] = {}
    for group in range(PARTITIONER.total_groups):
        provider = await storage.session_provider_for(group)
        staging = PartitionGroupStaging(
            RANGE.src, GroupSessionManager(provider), PARTITIONER, segment=RANGE.segment
        )
        staged.update(await staging.last_keys())
    return staged


async def _assert_transferred(storage: Storage, records: list[Record]) -> None:
    assert await _tags(storage) == {
        record.key.to_bytes(Keyspace.TAGS): record.tag for record in records
    }
    assert await _staged_keys(storage) == {}
    for record in records:
        provider = await storage.session_provider_for(
            PARTITIONER.group_for(record.key.pid.value)
        )
        address = PartitionAddress(pid=record.key.pid, address=record.key.address)
        async with GroupSessionManager(provider).session() as session:
            assert await session.get(address) == record


def _decode(storage: Storage, chunks: list[TransferChunk]) -> list[Record]:
    streamer = TransferStreamer(RANGE, storage, SERIALIZER, PARTITIONER)
    frames, _ = _read_frames(b"".join(chunk.raw for chunk in chunks))
    return [streamer.decode_record(frame) for frame in frames]


def _service(storage: Storage, **kwargs) -> TransferService:
    return TransferService(
        topologymgr=_TopologyManager("serial"),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        storage=storage,
        partitioner=PARTITIONER,
        **kwargs,
    )


def _applicator(
    pool: _FakePeerPool, storage: Storage, max_retries: int = 0, **kwargs
) -> TransferApplicator:
    return TransferApplicator(
        node_id=RANGE.dst,
        peer_pool=pool,  # type: ignore[arg-type]
        storage=storage,
        serializer=SERIALIZER,
        partitioner=PARTITIONER,
        backoff=BackoffRetry(
            initial_delay=0.0, max_delay=0.0, max_retries=max_retries, jitter=0.0
        ),
        **kwargs,
    )


async def _run(app: TransferApplicator) -> TransferHandle:
    await app.apply(PLAN)
    await app.wait()
    return app.handles[RANGE.id]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_join_over_lmdb_survives_resets_a_crash_and_a_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transferable, left_behind = _dataset()
    source, dest = _storage(tmp_path, "source"), _storage(tmp_path, "dest")
    try:
        await _seed(source, transferable + left_behind)
        client = _LoopbackClient(_service(source, max_chunk_size=512), monkeypatch)
        pool = _FakePeerPool({RANGE.src: client})

        # The stream resets mid-range, then the source stays unreachable: the
        # node gives up (as if it crashed) with part of the range staged.
        client.stream_faults = [5, 0]
        handle = await _run(_applicator(pool, dest, max_retries=1))
        assert handle.state is TransferState.FAILED
        assert handle.attempts == 2
        staged = await _staged_keys(dest)
        assert staged
        assert all(tag.kind is TagKind.STAGING for tag in (await _tags(dest)).values())

        # A restarted node resumes from what is durably staged, window by window.
        handle = await _run(_applicator(pool, dest, window_size=2048))

        assert handle.state is TransferState.COMPLETED
        assert client.claims[2].resumes == staged
        assert len(client.claims) > 3
        before_crash = _decode(dest, client.served[0])
        after_restart = _decode(
            dest, [chunk for served in client.served[2:] for chunk in served]
        )
        assert (
            before_crash + after_restart == transferable
        )  # nothing lost, nothing sent twice
        await _assert_transferred(dest, transferable)
    finally:
        await source.close()
        await dest.close()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_commit_interrupted_by_a_crash_is_completed_without_hiding_active_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transferable, _ = _dataset()
    source, dest = _storage(tmp_path, "source"), _storage(tmp_path, "dest")
    flushes: list[int] = []
    writes_after_crash: list[int] = []
    update_tags = GroupSession.update_tags
    stage = PartitionGroupStaging.stage

    async def crashing_flush(self, entries) -> None:
        flushes.append(len(entries))
        if len(flushes) == 2:
            raise RuntimeError("disk unavailable")
        await update_tags(self, entries)

    async def spying_stage(self, records) -> int:
        written = await stage(self, records)
        if len(flushes) >= 2:
            writes_after_crash.append(written)
        return written

    monkeypatch.setattr(GroupSession, "update_tags", crashing_flush)
    monkeypatch.setattr(PartitionGroupStaging, "stage", spying_stage)
    try:
        await _seed(source, transferable)
        client = _LoopbackClient(_service(source), monkeypatch)

        handle = await _run(
            _applicator(_FakePeerPool({RANGE.src: client}), dest, max_retries=1)
        )

        assert handle.state is TransferState.COMPLETED
        assert handle.attempts == 2
        assert handle.last_error == "disk unavailable"
        assert client.claims[1].resumes  # the staged remainder is resumed...
        assert (
            writes_after_crash and sum(writes_after_crash) == 0
        )  # ...activated data is not re-staged
        await _assert_transferred(dest, transferable)
    finally:
        await source.close()
        await dest.close()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pull_over_a_real_connection_resumes_after_the_link_drops(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    transferable, left_behind = _dataset()
    source, dest = _storage(tmp_path, "source"), _storage(tmp_path, "dest")
    manager = SimpleNamespace(
        serializer=SERIALIZER, transfer=_service(source, max_chunk_size=512)
    )
    monkeypatch.setattr(transfer_handlers, "get_manager", lambda: manager)
    claims: list[Envelope] = []
    peers: dict[str, EnvelopeClient] = {}

    async def flaky_claim(request: Envelope) -> AsyncIterator[Envelope]:
        claims.append(request)
        sent = 0
        async for response in transfer_handlers.claim(request):
            if len(claims) == 1 and sent == 3:
                await peers[
                    RANGE.src
                ].close()  # the link drops in the middle of the stream
                return
            sent += 1
            yield response

    server = await start_envelope_server({"transfer.claim": flaky_claim}, server_ssl)
    host, port = server.endpoint
    peers[RANGE.src] = EnvelopeClient(f"{host}:{port}", client_ssl)
    try:
        await _seed(source, transferable + left_behind)

        app = _applicator(_FakePeerPool(peers), dest, window_size=2048)
        handle = await asyncio.wait_for(_run(app), timeout=30)

        assert handle.state is TransferState.COMPLETED
        assert handle.attempts == 2
        assert len(claims) > 2
        await _assert_transferred(dest, transferable)
    finally:
        await peers[RANGE.src].close()
        await server.stop()
        await source.close()
        await dest.close()
