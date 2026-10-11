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
from collections.abc import AsyncIterator, Callable
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from tests.tourillon.test_group import (
    _FakeSessionProvider,
    _FakeStore,
    _record,
    _seed_record,
    _with_tag,
)
from tourillon.bootstrap.handlers import transfer as transfer_handlers
from tourillon.core.exceptions import ProcessError, TransferError
from tourillon.core.models.retry import BackoffRetry
from tourillon.core.models.storageschema import (
    MAX_QUORUM_WRITE,
    MAX_VALUE_DATA_LEN,
    Key,
    Keyspace,
    KVRef,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.models.transfer import (
    PartitionTransfer,
    RangeTransfer,
    TransferChunk,
    TransferClaim,
    TransferCommit,
    TransferHandle,
    TransferInit,
    TransferInitAccept,
    TransferPlan,
    TransferReject,
    TransferState,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.services.transfer import TransferService
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.spaces.ring import Ring
from tourillon.core.spaces.vnode import VNode
from tourillon.core.transfer.applicator import (
    DEFAULT_WINDOW_SIZE,
    TransferApplicator,
    _default_backoff,
    _resumes_after,
)
from tourillon.core.transfer.planner import TransferPlanner
from tourillon.core.transfer.runner import TransferRunner
from tourillon.core.transfer.stream import (
    MAX_CHUNK_SIZE,
    MAX_FRAME_SIZE,
    TransferReceiver,
    TransferStreamer,
    _read_frames,
)
from tourlib.adapter import MsgPackSerializer
from tourlib.constant import MAX_KEY_LEN, MAX_NAMESPACE_LEN, MAX_NODE_ID_LEN_BYTES
from tourlib.envelope import Envelope, build_envelope
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address

SERIALIZER = MsgPackSerializer()
RANGE = RangeTransfer(segment=(0, 3), src="src", dst="dst")
PLAN = TransferPlan(ranges=[RANGE], serial="serial")

_HANDLERS = {
    "transfer.init": transfer_handlers.init,
    "transfer.claim": transfer_handlers.claim,
    "transfer.chunk": transfer_handlers.chunk,
    "transfer.commit": transfer_handlers.commit,
}


class _MiniPartitioner:
    """Four pids in two groups: {0, 1} and {2, 3}."""

    _BOUNDS = {0: (0, 1), 1: (2, 3)}

    def group_bounds(self, gid: int) -> tuple[int, int]:
        return self._BOUNDS[gid]

    def group_for(self, pid: int) -> int:
        return 0 if pid < 2 else 1

    def groups_for_arc(self, arc: tuple[int, int]) -> set[int]:
        lo, hi = arc
        return {self.group_for(pid) for pid in range(lo, hi + 1)}


class _Storage:
    """In-memory storage of the two groups of _MiniPartitioner."""

    def __init__(self) -> None:
        self.stores = {0: _FakeStore(), 1: _FakeStore()}
        self._providers = {gid: _FakeSessionProvider(store, gid) for gid, store in self.stores.items()}

    async def session_provider_for(self, group: int) -> _FakeSessionProvider:
        return self._providers[group]

    def seed(self, *records: Record) -> None:
        for record in records:
            _seed_record(self._store_of(record), record)

    def tag(self, record: Record) -> Tag | None:
        ref = KVRef(key=record.key.to_bytes(Keyspace.TAGS), keyspace=Keyspace.TAGS)
        kv = self._store_of(record).get(ref)
        return None if kv is None else Tag.from_bytes(kv.value)

    def _store_of(self, record: Record) -> _FakeStore:
        return self.stores[_MiniPartitioner().group_for(record.key.pid.value)]


class _TopologyManager:
    """Ring serials seen by successive snapshots; the last one sticks."""

    def __init__(self, *serials: str) -> None:
        self._serials = list(serials)

    async def snapshot(self) -> SimpleNamespace:
        serial = self._serials.pop(0) if len(self._serials) > 1 else self._serials[0]
        return SimpleNamespace(ring=SimpleNamespace(serial=lambda: serial))


class _FakePeerPool:
    def __init__(self, clients: dict[str, Any]) -> None:
        self._clients = clients
        self.acquired: list[str] = []
        self.released: list[str] = []

    async def acquire(self, peer: str) -> Any:
        self.acquired.append(peer)
        return self._clients[peer]

    async def release(self, peer: str) -> None:
        self.released.append(peer)


class _ScriptedClient:
    """Peer answering from scripts: one list of items per stream, one item per request."""

    def __init__(self, streams: list[list[Any]] | None = None, requests: list[Any] | None = None) -> None:
        self._streams = list(streams or [])
        self._requests = list(requests or [])
        self.streamed: list[Envelope] = []
        self.requested: list[Envelope] = []

    async def stream(self, envelope: Envelope, timeout: float = 30.0) -> AsyncIterator[Envelope]:
        self.streamed.append(envelope)
        for item in self._streams.pop(0):
            if isinstance(item, BaseException):
                raise item
            yield item

    async def request(self, envelope: Envelope, timeout: float = 30.0) -> Envelope:
        self.requested.append(envelope)
        item = self._requests.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class _LoopbackClient:
    """In-process peer: envelopes are served by the real handlers of `service`.

    `stream_faults` holds, per stream, the number of chunks served before the
    connection resets (None: no fault). `fail_requests` holds the (kind, n)
    pairs whose n-th request of that kind fails with a connection reset.
    """

    def __init__(self, service: TransferService, monkeypatch: pytest.MonkeyPatch) -> None:
        manager = SimpleNamespace(serializer=SERIALIZER, transfer=service)
        monkeypatch.setattr(transfer_handlers, "get_manager", lambda: manager)
        self.stream_faults: list[int | None] = []
        self.fail_requests: set[tuple[str, int]] = set()
        self.claims: list[TransferClaim] = []
        self.served: list[list[TransferChunk]] = []
        self.requests: list[tuple[str, float]] = []

    async def stream(self, envelope: Envelope, timeout: float = 30.0) -> AsyncIterator[Envelope]:
        self.claims.append(TransferClaim.from_dict(SERIALIZER.decode(envelope.payload)))
        served: list[TransferChunk] = []
        self.served.append(served)
        limit = self.stream_faults.pop(0) if self.stream_faults else None
        async for response in _HANDLERS[envelope.kind](envelope):
            if limit is not None and len(served) >= limit:
                raise ConnectionResetError("connection reset by peer")
            if response.kind == "transfer.chunk":
                served.append(TransferChunk.from_dict(SERIALIZER.decode(response.payload)))
            yield response

    async def request(self, envelope: Envelope, timeout: float = 30.0) -> Envelope:
        self.requests.append((envelope.kind, timeout))
        ordinal = [kind for kind, _ in self.requests].count(envelope.kind)
        if (envelope.kind, ordinal) in self.fail_requests:
            raise ConnectionResetError("connection reset by peer")
        return [response async for response in _HANDLERS[envelope.kind](envelope)][0]


def _kind(record: Record, kind: TagKind) -> Record:
    return _with_tag(record, Tag(kind=kind))


def _dataset() -> list[Record]:
    """Transferable records of every pid of RANGE, in key order."""
    return [
        _record(0, 1),
        _record(0, 2),
        _kind(_record(1, 1), TagKind.TOMBSTONE),
        _with_tag(_record(2, 1), Tag.hint(Tag(kind=TagKind.LIVE), "node-x")),
        _record(3, 1),
        _record(3, 2),
    ]


def _streamer(storage: _Storage, max_chunk_size: int = 4096) -> TransferStreamer:
    return TransferStreamer(
        transfer=RANGE,
        storage=storage,  # type: ignore[arg-type]
        serializer=SERIALIZER,
        partitioner=_MiniPartitioner(),  # type: ignore[arg-type]
        max_chunk_size=max_chunk_size,
    )


def _service(storage: _Storage, *serials: str, **kwargs: Any) -> TransferService:
    return TransferService(
        topologymgr=_TopologyManager(*(serials or ("serial",))),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        storage=storage,  # type: ignore[arg-type]
        partitioner=_MiniPartitioner(),  # type: ignore[arg-type]
        **kwargs,
    )


def _applicator(
    node_id: str,
    pool: _FakePeerPool,
    storage: _Storage,
    max_retries: int = 0,
    **kwargs: Any,
) -> TransferApplicator:
    return TransferApplicator(
        node_id=node_id,
        peer_pool=pool,  # type: ignore[arg-type]
        storage=storage,  # type: ignore[arg-type]
        serializer=SERIALIZER,
        partitioner=_MiniPartitioner(),  # type: ignore[arg-type]
        backoff=BackoffRetry(initial_delay=0.0, max_delay=0.0, max_retries=max_retries, jitter=0.0),
        **kwargs,
    )


def _decode(streamer: TransferStreamer, chunks: list[TransferChunk]) -> list[Record]:
    """Return the records of every complete frame carried by `chunks`."""
    frames, _ = _read_frames(b"".join(chunk.raw for chunk in chunks))
    return [streamer.decode_record(frame) for frame in frames]


def _env(kind: str, payload: dict[str, Any]) -> Envelope:
    return build_envelope(kind=kind, payload=SERIALIZER.encode(payload), schema_id=SERIALIZER.schema_id)


def _frame(payload: bytes) -> bytes:
    return len(payload).to_bytes(4, "big") + payload


async def _run(app: TransferApplicator, plan: TransferPlan = PLAN) -> TransferHandle:
    await app.apply(plan)
    await app.wait()
    return app.handles[plan.ranges[0].id]


async def _until(predicate: Callable[[], bool]) -> None:
    for _ in range(1000):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition not reached")


@pytest.mark.tourillon
def test_transfer_models_round_trip_through_the_wire_format() -> None:
    record = _record(1, 1)
    handle = TransferHandle(
        range=RANGE,
        state=TransferState.FAILED,
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        last_error="boom",
        attempts=3,
        transferred=42,
    )
    models = [
        TransferPlan(ranges=[RANGE], serial="serial"),
        TransferClaim(serial="serial", resumes={1: record.key}, range=RANGE, after=record.key, window=7),
        TransferClaim(serial="serial", resumes={}, range=RANGE),
        TransferInit(serial="serial", range=RANGE),
        TransferInitAccept(resumes={1: record.key}, session="s-1"),
        TransferChunk(range=RANGE, raw=b"raw", last=True, seq=4, more=True, session="s-1"),
        TransferCommit(range=RANGE, session="s-1"),
        TransferReject(reason="nope", fatal=True),
    ]

    for model in models:
        assert type(model).from_dict(SERIALIZER.decode(SERIALIZER.encode(model.to_dict()))) == model

    wired = RangeTransfer.from_dict(SERIALIZER.decode(SERIALIZER.encode(RANGE.to_dict())))
    assert wired == RANGE and hash(wired) == hash(RANGE)
    assert TransferHandle.from_dict(handle.to_dict()) == handle
    transfer = PartitionTransfer(pid=1, src="a", dst="b")
    assert PartitionTransfer.from_dict(transfer.to_dict()) == transfer
    assert transfer.id == "1:a->b"
    assert TransferReject.from_dict(None) == TransferReject(reason="unknown", fatal=False)
    assert TransferChunk.from_dict({"range": RANGE.to_dict(), "raw": b"x"}) == TransferChunk(range=RANGE, raw=b"x")


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_stream_sends_transferable_records_in_key_order_in_numbered_chunks() -> None:
    source = _Storage()
    records = _dataset()
    staged_elsewhere = _with_tag(_record(1, 5), Tag.staging(Tag(kind=TagKind.LIVE), "other"))
    stale = _kind(_record(2, 7), TagKind.STALE)
    source.seed(*records, staged_elsewhere, stale)
    streamer = _streamer(source, max_chunk_size=64)

    chunks = [chunk async for chunk in streamer.stream()]

    assert [chunk.seq for chunk in chunks] == list(range(len(chunks)))
    assert [chunk.last for chunk in chunks] == [False] * (len(chunks) - 1) + [True]
    assert chunks[-1].more is False
    assert all(len(chunk.raw) == 64 for chunk in chunks[:-1])
    assert _decode(streamer, chunks) == records
    assert [provider.group_id for provider in await streamer.session_providers()] == [0, 1]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_stream_resumes_per_pid_and_cuts_windows_at_record_boundaries() -> None:
    source = _Storage()
    records = _dataset()
    source.seed(*records)
    streamer = _streamer(source, max_chunk_size=64)

    resumed = [chunk async for chunk in streamer.stream(resumes={0: records[1].key, 3: records[4].key})]
    assert _decode(streamer, resumed) == [records[2], records[3], records[5]]

    received: list[Record] = []
    after = None
    windows = 0
    while True:
        chunks = [chunk async for chunk in streamer.stream(after=after, window=1)]
        windows += 1
        window_records = _decode(streamer, chunks)
        received.extend(window_records)
        if not chunks[-1].more:
            break
        assert len(window_records) == 1
        after = window_records[-1].key

    assert received == records
    assert windows == len(records) + 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_receiver_stages_a_stream_and_commit_activates_it() -> None:
    source, dest = _Storage(), _Storage()
    records = _dataset()
    source.seed(*records)
    receiver = TransferReceiver(_streamer(dest))

    received = 0
    async for chunk in _streamer(source, max_chunk_size=64).stream():
        received += await receiver.receive(chunk)

    assert received == len(records)
    assert receiver.complete and not receiver.more
    assert receiver.cursor == records[-1].key
    assert [dest.tag(record) for record in records] == [Tag.staging(r.tag, "src") for r in records]
    assert await receiver.streamer.last_keys() == {
        0: records[1].key,
        1: records[2].key,
        2: records[3].key,
        3: records[5].key,
    }

    assert await receiver.streamer.commit() == len(records)
    assert [dest.tag(record) for record in records] == [record.tag for record in records]
    assert await receiver.streamer.last_keys() == {}


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_receiver_rejects_gaps_foreign_ranges_and_chunks_after_the_end() -> None:
    receiver = TransferReceiver(_streamer(_Storage()))

    with pytest.raises(TransferError, match="expected chunk 0, received 1"):
        await receiver.receive(TransferChunk(range=RANGE, raw=b"", seq=1))
    with pytest.raises(ProcessError, match="received a chunk of range"):
        await receiver.receive(
            TransferChunk(range=RangeTransfer(segment=(9, 9), src="src", dst="dst"), raw=b"")
        )

    assert await receiver.receive(TransferChunk(range=RANGE, raw=b"", last=True, more=True)) == 0
    assert receiver.complete and receiver.more and receiver.cursor is None

    with pytest.raises(TransferError, match="after the last one"):
        await receiver.receive(TransferChunk(range=RANGE, raw=b"", seq=1))


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_receiver_rejects_corrupted_streams() -> None:
    streamer = _streamer(_Storage())
    valid = streamer.encode_record(_record(0, 1))
    cases = [
        (valid[:-1], "stream ended inside a record frame"),
        ((MAX_FRAME_SIZE + 1).to_bytes(4, "big"), "exceeds"),
        (_frame(b"\xc1"), "invalid record frame"),
        (_frame(SERIALIZER.encode([1, 2])), "invalid record frame"),
        (_frame(SERIALIZER.encode({"key": {}})), "invalid record frame"),
        (streamer.encode_record(_record(9, 1)), "outside its segment"),
        (
            streamer.encode_record(_with_tag(_record(0, 1), Tag.staging(Tag(kind=TagKind.LIVE), "x"))),
            "received a STAGING record",
        ),
    ]

    for raw, message in cases:
        with pytest.raises(ProcessError, match=message):
            await TransferReceiver(streamer).receive(TransferChunk(range=RANGE, raw=raw, last=True))


@pytest.mark.tourillon
def test_chunks_must_carry_frames_and_fit_in_one_envelope() -> None:
    with pytest.raises(ValueError, match="too small"):
        _streamer(_Storage(), max_chunk_size=4)
    with pytest.raises(ValueError, match="must not exceed"):
        _streamer(_Storage(), max_chunk_size=MAX_CHUNK_SIZE + 1)
    with pytest.raises(ValueError, match="must not exceed"):
        _service(_Storage(), max_chunk_size=MAX_CHUNK_SIZE + 1)


def _largest_record() -> Record:
    return Record(
        key=Key(
            pid=Pid(3),
            address=Address(namespace=b"n" * MAX_NAMESPACE_LEN, key=b"k" * MAX_KEY_LEN),
            ts=HLCTimestamp(wall_ms=2**63 - 1, counter=2**16 - 1, node_id="x" * MAX_NODE_ID_LEN_BYTES),
        ),
        value=Value(data=b"v" * MAX_VALUE_DATA_LEN, quorum_write=MAX_QUORUM_WRITE),
        tag=Tag.hint(Tag(kind=TagKind.TOMBSTONE), "h" * MAX_NODE_ID_LEN_BYTES),
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_the_largest_record_fits_in_a_frame_and_the_largest_chunks_in_an_envelope() -> None:
    source, dest = _Storage(), _Storage()
    record = _largest_record()
    source.seed(record)
    streamer = _streamer(source, max_chunk_size=MAX_CHUNK_SIZE)
    assert len(streamer.encode_record(record)) - 4 <= MAX_FRAME_SIZE

    receiver = TransferReceiver(_streamer(dest, max_chunk_size=MAX_CHUNK_SIZE))
    async for chunk in streamer.stream():
        envelope = _env("transfer.chunk", chunk.to_dict())  # refused above the envelope limit
        await receiver.receive(TransferChunk.from_dict(SERIALIZER.decode(envelope.payload)))

    assert receiver.complete
    assert dest.tag(record) == Tag.staging(record.tag, "src")
    assert await receiver.streamer.commit() == 1
    assert dest.tag(record) == record.tag


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_service_claim_checks_the_ring_serial_and_caps_the_window() -> None:
    source = _Storage()
    source.seed(*_dataset())
    service = _service(source, max_window=1)

    with pytest.raises(TransferError, match="Serial mismatch"):
        [chunk async for chunk in service.claim(TransferClaim(serial="other", resumes={}, range=RANGE))]

    for window in (None, 0, 10**9):
        claim = TransferClaim(serial="serial", resumes={}, range=RANGE, window=window)
        chunks = [chunk async for chunk in service.claim(claim)]
        assert chunks[-1].more is True
        assert len(_decode(_streamer(source), chunks)) == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_service_receives_a_pushed_range_in_a_session() -> None:
    source, dest = _Storage(), _Storage()
    records = _dataset()
    source.seed(*records)
    service = _service(dest)

    with pytest.raises(TransferError, match="Serial mismatch"):
        await service.init(TransferInit(serial="other", range=RANGE))
    accept = await service.init(TransferInit(serial="serial", range=RANGE))
    assert accept.resumes == {} and accept.session

    chunks = [
        replace(chunk, session=accept.session)
        async for chunk in _streamer(source, max_chunk_size=64).stream()
    ]
    with pytest.raises(TransferError, match="unknown transfer session"):
        await service.chunk(replace(chunks[0], session="stale"))
    with pytest.raises(TransferError, match="before the last chunk"):
        await service.commit(TransferCommit(range=RANGE, session=accept.session))

    for chunk in chunks:
        await service.chunk(chunk)

    assert await service.commit(TransferCommit(range=RANGE, session=accept.session)) == len(records)
    assert [dest.tag(record) for record in records] == [record.tag for record in records]
    with pytest.raises(TransferError, match="unknown transfer session"):
        await service.commit(TransferCommit(range=RANGE, session=accept.session))


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_service_replaces_and_evicts_push_sessions() -> None:
    now = [0.0]
    service = _service(_Storage(), session_ttl=10.0, clock=lambda: now[0])
    other = RangeTransfer(segment=(2, 3), src="src", dst="dst")

    first = await service.init(TransferInit(serial="serial", range=RANGE))
    second = await service.init(TransferInit(serial="serial", range=RANGE))
    assert first.session != second.session
    with pytest.raises(TransferError, match="unknown transfer session"):
        await service.chunk(TransferChunk(range=RANGE, raw=b"", session=first.session))
    await service.chunk(TransferChunk(range=RANGE, raw=b"", session=second.session))

    now[0] = 11.0
    await service.init(TransferInit(serial="serial", range=other))

    with pytest.raises(TransferError, match="unknown transfer session"):
        await service.chunk(TransferChunk(range=RANGE, raw=b"", seq=1, session=second.session))


@pytest.mark.tourillon
def test_service_rejects_invalid_configuration() -> None:
    with pytest.raises(ValueError, match="max_window"):
        _service(_Storage(), max_window=0)
    with pytest.raises(ValueError, match="session_ttl"):
        _service(_Storage(), session_ttl=0)


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pull_claims_windows_until_the_range_is_exhausted_then_activates_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, dest = _Storage(), _Storage()
    records = _dataset()
    source.seed(*records)
    client = _LoopbackClient(_service(source, max_chunk_size=64), monkeypatch)
    pool = _FakePeerPool({"src": client})
    app = _applicator("dst", pool, dest, window_size=150)

    handle = await _run(app)

    assert handle.state is TransferState.COMPLETED
    assert handle.attempts == 1
    assert handle.transferred == sum(len(chunk.raw) for served in client.served for chunk in served)
    assert [dest.tag(record) for record in records] == [record.tag for record in records]
    assert len(client.claims) > 2
    assert client.claims[0].after is None
    assert all(claim.after is not None for claim in client.claims[1:])
    assert all(claim.serial == "serial" and claim.window == 150 for claim in client.claims)
    assert pool.acquired == ["src"] and pool.released == ["src"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pull_resumes_from_staged_keys_after_a_reset_without_resending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, dest = _Storage(), _Storage()
    records = _dataset()
    source.seed(*records)
    client = _LoopbackClient(_service(source, max_chunk_size=64), monkeypatch)
    client.stream_faults = [4]
    app = _applicator("dst", _FakePeerPool({"src": client}), dest)

    handle = await _run(app)

    assert handle.state is TransferState.COMPLETED
    assert handle.attempts == 2
    assert handle.last_error == "connection reset by peer"
    assert client.claims[1].resumes  # the second attempt resumed from staged keys
    streamer = _streamer(source)
    first_attempt = _decode(streamer, client.served[0])
    assert first_attempt and first_attempt + _decode(streamer, client.served[1]) == records
    assert [dest.tag(record) for record in records] == [record.tag for record in records]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pull_retries_rejected_claims_until_the_serial_converges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, dest = _Storage(), _Storage()
    records = _dataset()
    source.seed(*records)
    client = _LoopbackClient(_service(source, "old", "old", "serial"), monkeypatch)
    app = _applicator("dst", _FakePeerPool({"src": client}), dest)

    handle = await _run(app)

    assert handle.state is TransferState.COMPLETED
    assert handle.attempts == 3
    assert "Serial mismatch" in (handle.last_error or "")
    assert [dest.tag(record) for record in records] == [record.tag for record in records]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pull_fails_at_once_on_fatal_rejections_and_protocol_violations() -> None:
    fatal = _env("transfer.claim.reject", TransferReject(reason="broken", fatal=True).to_dict())
    unexpected = _env("transfer.other", {})
    empty_more = _env("transfer.chunk", TransferChunk(range=RANGE, raw=b"", last=True, more=True).to_dict())
    cases = [([fatal], "broken"), ([unexpected], "unexpected response kind"), ([empty_more], "without sending")]

    for stream, message in cases:
        client = _ScriptedClient(streams=[stream])
        handle = await _run(_applicator("dst", _FakePeerPool({"src": client}), _Storage()))
        assert handle.state is TransferState.FAILED
        assert handle.attempts == 1
        assert message in (handle.last_error or "")


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pull_retries_streams_closed_before_their_last_chunk() -> None:
    first_chunk = _env("transfer.chunk", TransferChunk(range=RANGE, raw=b"").to_dict())
    client = _ScriptedClient(streams=[[first_chunk], []])

    handle = await _run(_applicator("dst", _FakePeerPool({"src": client}), _Storage(), max_retries=1))

    assert handle.state is TransferState.FAILED
    assert handle.attempts == 2
    assert "closed before its last chunk" in (handle.last_error or "")


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_push_sends_acknowledged_chunks_then_commits(monkeypatch: pytest.MonkeyPatch) -> None:
    source, dest = _Storage(), _Storage()
    records = _dataset()
    source.seed(*records)
    client = _LoopbackClient(_service(dest), monkeypatch)
    app = _applicator("src", _FakePeerPool({"dst": client}), source, max_chunk_size=1024, commit_timeout=99.0)

    handle = await _run(app)

    assert handle.state is TransferState.COMPLETED
    kinds = [kind for kind, _ in client.requests]
    assert kinds[0] == "transfer.init" and kinds[-1] == "transfer.commit"
    assert set(kinds[1:-1]) == {"transfer.chunk"}
    assert client.requests[-1] == ("transfer.commit", 99.0)
    assert handle.transferred > 0
    assert [dest.tag(record) for record in records] == [record.tag for record in records]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_push_reopens_a_lost_session_and_resumes(monkeypatch: pytest.MonkeyPatch) -> None:
    source, dest = _Storage(), _Storage()
    records = [_record(pid, idx) for pid in range(4) for idx in range(1, 30)]
    source.seed(*records)
    client = _LoopbackClient(_service(dest), monkeypatch)
    client.fail_requests = {("transfer.chunk", 3)}
    app = _applicator("src", _FakePeerPool({"dst": client}), source, max_chunk_size=1024)
    full_stream = len([chunk async for chunk in _streamer(source, max_chunk_size=1024).stream()])
    assert full_stream > 3

    handle = await _run(app)

    kinds = [kind for kind, _ in client.requests]
    second_attempt = kinds[kinds.index("transfer.init", 1):]
    assert handle.state is TransferState.COMPLETED
    assert handle.attempts == 2
    assert handle.last_error == "connection reset by peer"
    assert kinds[:4] == ["transfer.init", "transfer.chunk", "transfer.chunk", "transfer.chunk"]
    assert second_attempt.count("transfer.chunk") < full_stream
    assert [dest.tag(record) for record in records] == [record.tag for record in records]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_push_maps_rejections_to_retries_or_failures() -> None:
    retryable = _env("transfer.init.reject", TransferReject(reason="serial").to_dict())
    accept = _env("transfer.init.accept", TransferInitAccept(resumes={}, session="s").to_dict())
    chunk_accept = _env("transfer.chunk.accept", {"seq": 0})
    fatal_commit = _env("transfer.commit.reject", TransferReject(reason="corrupted", fatal=True).to_dict())
    client = _ScriptedClient(requests=[retryable, accept, chunk_accept, fatal_commit])

    handle = await _run(_applicator("src", _FakePeerPool({"dst": client}), _Storage(), max_retries=3))

    assert handle.state is TransferState.FAILED
    assert handle.attempts == 2
    assert handle.last_error == "transfer.commit.reject: corrupted"
    assert [envelope.kind for envelope in client.requested] == [
        "transfer.init",
        "transfer.init",
        "transfer.chunk",
        "transfer.commit",
    ]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_retry_budget_only_counts_attempts_without_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _applicator("dst", _FakePeerPool({}), _Storage(), max_retries=2)
    progress = [0, 0, 100, 0, 0, 0]

    async def do_transfer(handle: TransferHandle) -> None:
        handle.transferred += progress.pop(0)
        raise ConnectionResetError("reset")

    monkeypatch.setattr(app, "_do_transfer", do_transfer)
    handle = await _run(app)

    assert handle.state is TransferState.FAILED
    assert handle.attempts == 5  # would be 3 without the reset made by the progress
    assert handle.last_error == "reset"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_unbounded_retries_outlast_long_outages(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _default_backoff().max_retries == 0
    app = _applicator("dst", _FakePeerPool({}), _Storage(), max_retries=0)
    outcomes: list[BaseException | None] = [ConnectionRefusedError()] * 25 + [None]

    async def do_transfer(handle: TransferHandle) -> None:
        error = outcomes.pop(0)
        if error is not None:
            raise error

    monkeypatch.setattr(app, "_do_transfer", do_transfer)
    handle = await _run(app)

    assert handle.state is TransferState.COMPLETED
    assert handle.attempts == 26
    assert handle.last_error == "ConnectionRefusedError"

    app._finish(handle, TransferState.CANCELLED)  # noqa: SLF001
    assert handle.state is TransferState.COMPLETED  # a settled handle never changes


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_process_and_unexpected_errors_fail_the_range_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    for error, message in [(ProcessError("inconsistent"), "inconsistent"), (KeyError("bug"), "'bug'")]:
        app = _applicator("dst", _FakePeerPool({}), _Storage())

        async def do_transfer(handle: TransferHandle, error: Exception = error) -> None:
            raise error

        monkeypatch.setattr(app, "_do_transfer", do_transfer)
        handle = await _run(app)

        assert handle.state is TransferState.FAILED
        assert handle.attempts == 1
        assert handle.last_error == message


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_ranges_not_involving_this_node_fail() -> None:
    foreign = RangeTransfer(segment=(0, 0), src="a", dst="b")
    loop = RangeTransfer(segment=(1, 1), src="dst", dst="dst")
    app = _applicator("dst", _FakePeerPool({}), _Storage())

    await app.apply(TransferPlan(ranges=[foreign, loop], serial="serial"))
    assert await app.wait() == ([], sorted([foreign.id, loop.id]))
    assert "neither source nor destination" in (app.handles[foreign.id].last_error or "")
    assert "same source and destination" in (app.handles[loop.id].last_error or "")


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_apply_restarts_failed_ranges_cancels_dropped_ones_and_keeps_the_others(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _applicator("dst", _FakePeerPool({}), _Storage())
    r1, r2, r3 = (RangeTransfer(segment=(pid, pid), src="src", dst="dst") for pid in range(3))
    release = asyncio.Event()
    r2_outcomes: list[Exception | None] = [ProcessError("bad"), None]

    async def do_transfer(handle: TransferHandle) -> None:
        if handle.range == r1:
            await release.wait()
        elif handle.range == r2 and (error := r2_outcomes.pop(0)) is not None:
            raise error

    monkeypatch.setattr(app, "_do_transfer", do_transfer)

    await app.apply(TransferPlan(ranges=[r1, r2], serial="s1"))
    await _until(lambda: app.handles[r2.id].state is TransferState.FAILED)
    running, failed = app.handles[r1.id], app.handles[r2.id]

    await app.apply(TransferPlan(ranges=[r1, r2, r3], serial="s2"))
    assert app.handles[r1.id] is running
    assert app.handles[r2.id] is not failed
    await _until(lambda: all(
        app.handles[r.id].state is TransferState.COMPLETED for r in (r2, r3)
    ))

    await app.apply(TransferPlan(ranges=[r2, r3], serial="s3"))
    assert r1.id not in app.handles
    await _until(lambda: running.state is TransferState.CANCELLED)
    assert await app.wait() == (sorted([r2.id, r3.id]), [])


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_wait_also_awaits_ranges_of_plans_applied_while_waiting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _applicator("dst", _FakePeerPool({}), _Storage())
    r1, r2 = (RangeTransfer(segment=(pid, pid), src="src", dst="dst") for pid in range(2))
    gates = {r1.id: asyncio.Event(), r2.id: asyncio.Event()}

    async def do_transfer(handle: TransferHandle) -> None:
        await gates[handle.range.id].wait()

    monkeypatch.setattr(app, "_do_transfer", do_transfer)

    await app.apply(TransferPlan(ranges=[r1], serial="s1"))
    r1_done = asyncio.Event()
    # Registered before the waiter's own callback: the test resumes right after r1
    # settles, before the waiter does (the window where m13 lost the new plan).
    app._tasks[r1.id].add_done_callback(lambda _: r1_done.set())  # noqa: SLF001
    waiter = asyncio.create_task(app.wait())
    await asyncio.sleep(0)
    gates[r1.id].set()
    await r1_done.wait()
    assert not waiter.done()

    await app.apply(TransferPlan(ranges=[r1, r2], serial="s2"))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert not waiter.done()

    gates[r2.id].set()
    assert await waiter == (sorted([r1.id, r2.id]), [])


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_cancel_stops_running_transfers_which_count_as_not_completed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _applicator("dst", _FakePeerPool({}), _Storage(), max_concurrency=1)
    r1, r2 = (RangeTransfer(segment=(pid, pid), src="src", dst="dst") for pid in range(2))

    async def do_transfer(handle: TransferHandle) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(app, "_do_transfer", do_transfer)
    await app.apply(TransferPlan(ranges=[r1, r2], serial="s1"))
    await _until(lambda: app.handles[r1.id].state is TransferState.STARTED)

    await app.cancel()

    assert app.handles[r1.id].state is TransferState.CANCELLED
    assert app.handles[r2.id].state is TransferState.CANCELLED  # was waiting for a slot
    assert await app.wait() == ([], sorted([r1.id, r2.id]))


@pytest.mark.tourillon
def test_applicator_rejects_invalid_constructor_arguments() -> None:
    for kwargs, message in [
        ({"max_concurrency": 0}, "max_concurrency"),
        ({"max_chunk_size": 1}, ">= 1024"),
        ({"max_chunk_size": MAX_CHUNK_SIZE + 1}, "must not exceed"),
        ({"window_size": 0}, "window_size"),
        ({"commit_timeout": 0}, "commit_timeout"),
    ]:
        with pytest.raises(ValueError, match=message):
            _applicator("dst", _FakePeerPool({}), _Storage(), **kwargs)


@pytest.mark.tourillon
def test_resumes_after_keeps_only_the_pids_left_to_receive() -> None:
    records = _dataset()
    resumes = {0: records[0].key, 2: records[3].key, 3: records[4].key}

    assert _resumes_after(resumes, None) == resumes
    assert _resumes_after(resumes, records[3].key) == {3: records[4].key}
    assert DEFAULT_WINDOW_SIZE >= 1024 * 1024


@pytest.mark.tourillon
def test_transfer_planner_plan_and_helpers() -> None:
    partitioner = Partitioner(pshift=4, gshift=1, hashspace=HashSpace(bits=8))
    planner = TransferPlanner(node_id="node-b", partitioner=partitioner, replication_factor=1)
    current = Ring([VNode(token=0, node_id="node-a")])
    target = Ring([VNode(token=0, node_id="node-a"), VNode(token=128, node_id="node-b")])

    plan = planner.plan(current, target)

    assert plan.serial == current.serial()
    assert plan.ranges
    assert all(range_.dst == "node-b" for range_ in plan.ranges)

    assert planner._pair_segments((1, 2), {"node-a"}, {"node-a"}) == []  # noqa: SLF001
    assert planner._pair_segments((1, 2), {"node-a"}, {"node-c"}) == []  # noqa: SLF001
    assert planner._pair_segments((1, 2), {"node-a", "node-b"}, {"node-b", "node-c", "node-d"}) == [  # noqa: SLF001
        RangeTransfer(segment=(1, 2), src="node-a", dst="node-c"),
        RangeTransfer(segment=(1, 2), src="node-a", dst="node-d"),
    ]
    planner_rf2 = TransferPlanner(node_id="node-b", partitioner=partitioner, replication_factor=2)
    assert planner_rf2._replicaset(VNode(token=0, node_id="node-a"), Ring([  # noqa: SLF001
        VNode(token=0, node_id="node-a"),
        VNode(token=1, node_id="node-a"),
        VNode(token=2, node_id="node-b"),
    ])) == frozenset({"node-a", "node-b"})


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_runner_plans_runs_replans_and_cancels(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = TransferRunner(
        node_id="node-b",
        replication_factor=1,
        partitioner=Partitioner(pshift=4, gshift=1, hashspace=HashSpace(bits=8)),
        peer_pool=_FakePeerPool({}),  # type: ignore[arg-type]
        storage=_Storage(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
    )
    release = asyncio.Event()

    async def do_transfer(handle: TransferHandle) -> None:
        await release.wait()

    monkeypatch.setattr(runner._applicator, "_do_transfer", do_transfer)  # noqa: SLF001
    current = Ring([VNode(token=0, node_id="node-a")])
    target = current.add_vnodes([VNode(token=128, node_id="node-b")])

    release.set()
    assert await runner.run(current, target) == (["0-7:node-a->node-b"], [])

    release.clear()
    await runner.apply(current, target.add_vnodes([VNode(token=64, node_id="node-b")]))
    await runner.cancel()

    assert await runner.wait() == ([], ["0-3:node-a->node-b", "4-7:node-a->node-b"])











