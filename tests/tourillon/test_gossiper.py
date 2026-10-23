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
import contextlib
import uuid
from collections.abc import AsyncIterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import cast

import pytest

from tourillon.core.config import GossipConfig
from tourillon.core.gossip.gossiper import Gossiper, GossipStats
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.peer.pool import PeerPool
from tourillon.core.ports.telemetry import AttributeValue, Span, Telemetry
from tourillon.core.telemetry import build_noop_telemetry
from tourlib import ValidationError
from tourlib.constant import PROTOCOL_VERSION
from tourlib.envelope import Envelope
from tourlib.exceptions import EnvelopeError
from tourlib.transport import EnvelopeClient


class _FakeSerializer:
    schema_id = 1

    def encode(self, document: object) -> bytes:
        return b""

    def decode(self, payload: bytes) -> object:
        return {}


class _LoopingGossiper(Gossiper):
    def __init__(self) -> None:
        super().__init__(
            node_id="node-a",
            config=GossipConfig(),
            stats=GossipStats(),
            topologymgr=TopologyManager(),
            peer_pool=cast(PeerPool, object()),
            serializer=_FakeSerializer(),
            telemetry=build_noop_telemetry(),
        )
        self.hot_started = asyncio.Event()
        self.ae_started = asyncio.Event()
        self.hot_cancelled = asyncio.Event()
        self.ae_cancelled = asyncio.Event()
        self.processed_members: list[Member] = []

    async def _hot_loop(self) -> None:
        self.hot_started.set()
        try:
            while True:
                member = await self._hot_queue.get()
                try:
                    self.processed_members.append(member)
                finally:
                    self._hot_queue.task_done()
        except asyncio.CancelledError:
            self.hot_cancelled.set()
            raise

    async def _ae_loop(self) -> None:
        self.ae_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.ae_cancelled.set()
            raise


class _NoTaskDoneGossiper(_LoopingGossiper):
    def __init__(self) -> None:
        super().__init__()
        self._config.stop_drain_timeout = 0.01

    async def _hot_loop(self) -> None:
        self.hot_started.set()
        try:
            while True:
                member = await self._hot_queue.get()
                self.processed_members.append(member)
        except asyncio.CancelledError:
            self.hot_cancelled.set()
            raise


class _CrashAeGossiper(_LoopingGossiper):
    async def _ae_loop(self) -> None:
        self.ae_started.set()
        raise RuntimeError("ae loop crashed")


class _MapSerializer:
    schema_id = 1

    def __init__(self) -> None:
        self._seq = 0
        self._docs: dict[bytes, object] = {}

    def encode(self, document: object) -> bytes:
        payload = f"p-{self._seq}".encode()
        self._seq += 1
        self._docs[payload] = document
        return payload

    def decode(self, payload: bytes) -> object:
        if payload not in self._docs:
            raise ValidationError("unknown payload")
        return self._docs[payload]


class _Counter:
    def __init__(self) -> None:
        self.total = 0.0

    def add(self, amount: int | float = 1, *, attributes: object | None = None) -> None:
        _ = attributes
        self.total += float(amount)


class _Gauge:
    def __init__(self) -> None:
        self.value = 0.0

    def set(self, value: int | float, *, attributes: object | None = None) -> None:
        _ = attributes
        self.value = float(value)


@dataclass
class _SpanRecorder(Span):
    attributes: dict[str, AttributeValue] = field(default_factory=dict)
    exceptions: list[BaseException] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def set_attribute(self, key: str, value: AttributeValue) -> None:
        self.attributes[key] = value

    def record_exception(self, exc: BaseException) -> None:
        self.exceptions.append(exc)

    def set_status_error(self, description: str = "") -> None:
        self.errors.append(description)


class _SpanCtx(AbstractContextManager[Span]):
    def __init__(self, span: _SpanRecorder) -> None:
        self._span = span

    def __enter__(self) -> Span:
        return self._span

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        _ = exc_type
        _ = exc
        _ = tb
        return None


class _TelemetryRecorder:
    def __init__(self) -> None:
        self._gossip_push_attempt_total = _Counter()
        self._gossip_push_error_total = _Counter()
        self._gossip_ae_cycle_total = _Counter()
        self._gossip_hot_queue_size = _Gauge()
        self._gossip_ae_diverged = _Counter()
        self._gossip_last_ae_peer = _Gauge()
        self._gossip_last_ae_at = _Gauge()
        self._gossip_failed_announcements_total = _Counter()
        self.spans: list[tuple[str, dict[str, AttributeValue], _SpanRecorder]] = []

    @property
    def gossip_push_attempt_total(self) -> _Counter:
        return self._gossip_push_attempt_total

    @property
    def gossip_push_error_total(self) -> _Counter:
        return self._gossip_push_error_total

    @property
    def gossip_ae_cycle_total(self) -> _Counter:
        return self._gossip_ae_cycle_total

    @property
    def gossip_hot_queue_size(self) -> _Gauge:
        return self._gossip_hot_queue_size

    @property
    def gossip_ae_diverged(self) -> _Counter:
        return self._gossip_ae_diverged

    @property
    def gossip_last_ae_peer(self) -> _Gauge:
        return self._gossip_last_ae_peer

    @property
    def gossip_last_ae_at(self) -> _Gauge:
        return self._gossip_last_ae_at

    @property
    def gossip_failed_announcements_total(self) -> _Counter:
        return self._gossip_failed_announcements_total

    def start_span(
        self, name: str, *, attributes: dict[str, AttributeValue] | None = None
    ) -> AbstractContextManager[Span]:
        span = _SpanRecorder()
        self.spans.append((name, dict(attributes or {}), span))
        return _SpanCtx(span)


class _FakeClient:
    def __init__(
        self,
        *,
        request_responses: list[Envelope] | None = None,
        stream_responses: list[Envelope] | None = None,
        request_error: BaseException | None = None,
    ) -> None:
        self.request_responses = request_responses or []
        self.stream_responses = stream_responses or []
        self.request_error = request_error
        self.requests: list[Envelope] = []

    async def request(self, envelope: Envelope) -> Envelope:
        self.requests.append(envelope)
        if self.request_error is not None:
            raise self.request_error
        assert self.request_responses
        return self.request_responses.pop(0)

    async def stream(self, envelope: Envelope) -> AsyncIterator[Envelope]:
        self.requests.append(envelope)
        for response in self.stream_responses:
            yield response


class _FakePool:
    def __init__(
        self, client: _FakeClient | None = None, error: BaseException | None = None
    ) -> None:
        self._client = client
        self._error = error
        self.calls: list[str] = []
        self.released: list[str] = []

    async def acquire(self, node_id: str) -> _FakeClient:
        self.calls.append(node_id)
        if self._error is not None:
            raise self._error
        assert self._client is not None
        return self._client

    @contextlib.asynccontextmanager
    async def lease(self, node_id: str) -> AsyncIterator[_FakeClient]:
        client = await self.acquire(node_id)
        try:
            yield client
        finally:
            self.released.append(node_id)


def _member(
    node_id: str, *, phase: MemberPhase = MemberPhase.READY, epoch: int = 1
) -> Member:
    return Member(
        node_id=node_id,
        peer_address=f"127.0.0.1:{4000 + epoch}",
        epoch=epoch,
        phase=phase,
        tokens=(f"{epoch:02x}", f"{epoch + 100:02x}"),
    )


def _envelope(kind: str, serializer: _MapSerializer, payload: object) -> Envelope:
    return Envelope(
        kind=kind,
        correlation_id=uuid.uuid4(),
        mode=Envelope.MODE_RPC,
        proto_version=PROTOCOL_VERSION,
        schema_id=serializer.schema_id,
        payload=serializer.encode(payload),
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_engine_start_schedules_loops_and_stop_cancels_them() -> None:
    engine = _LoopingGossiper()

    await engine.start()
    await asyncio.wait_for(engine.hot_started.wait(), timeout=0.1)
    await asyncio.wait_for(engine.ae_started.wait(), timeout=0.1)

    assert engine._hot_task is not None
    assert engine._ae_task is not None
    assert not engine._hot_task.done()
    assert not engine._ae_task.done()

    await engine.stop()

    assert engine.hot_cancelled.is_set()
    assert engine.ae_cancelled.is_set()
    assert engine._hot_task is None
    assert engine._ae_task is None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_engine_stop_is_noop_when_not_started() -> None:
    engine = _LoopingGossiper()

    await engine.stop()

    assert engine._hot_task is None
    assert engine._ae_task is None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_engine_start_is_idempotent_while_running() -> None:
    engine = _LoopingGossiper()

    await engine.start()
    await asyncio.wait_for(engine.hot_started.wait(), timeout=0.1)
    await asyncio.wait_for(engine.ae_started.wait(), timeout=0.1)
    first_hot_task = engine._hot_task
    first_ae_task = engine._ae_task

    await engine.start()

    assert engine._hot_task is first_hot_task
    assert engine._ae_task is first_ae_task

    await engine.stop()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_engine_stop_waits_for_hot_queue_to_drain() -> None:
    engine = _LoopingGossiper()
    member = Member(
        node_id="node-b",
        peer_address="127.0.0.1:4000",
        epoch=1,
        phase=MemberPhase.READY,
        tokens=("01", "02"),
    )

    await engine.start()
    await asyncio.wait_for(engine.hot_started.wait(), timeout=0.1)
    await asyncio.wait_for(engine.ae_started.wait(), timeout=0.1)
    await engine.announce(member)

    await engine.stop()

    assert engine.processed_members == [member]
    assert engine._hot_queue.empty()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_engine_stop_logs_and_records_timeout_when_queue_does_not_drain(
    caplog: pytest.LogCaptureFixture,
) -> None:
    engine = _NoTaskDoneGossiper()
    member = Member(
        node_id="node-c",
        peer_address="127.0.0.1:4001",
        epoch=1,
        phase=MemberPhase.READY,
        tokens=("03", "04"),
    )

    await engine.start()
    await asyncio.wait_for(engine.hot_started.wait(), timeout=0.1)
    await asyncio.wait_for(engine.ae_started.wait(), timeout=0.1)
    await engine.announce(member)

    with caplog.at_level("ERROR"):
        await engine.stop()

    status = engine.status()
    assert "gossip.stop hot queue drain timed out" in caplog.text
    assert status["last_failures"]["stop"]["error"] is not None
    assert status["last_failures"]["stop"]["at"] is not None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_engine_status_exposes_running_state_and_loop_failures() -> None:
    engine = _CrashAeGossiper()

    await engine.start()
    await asyncio.wait_for(engine.hot_started.wait(), timeout=0.1)
    await asyncio.wait_for(engine.ae_started.wait(), timeout=0.1)
    await asyncio.sleep(0)

    status = engine.status()
    assert status["hot_running"] is True
    assert status["ae_running"] is False
    assert status["hot_queue_size"] == 0
    assert status["last_failures"]["ae"]["error"] == "RuntimeError: ae loop crashed"
    assert status["last_failures"]["ae"]["at"] is not None

    with pytest.raises(RuntimeError, match="ae loop crashed"):
        await engine.stop()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossiper_announce_updates_queue_metrics_and_failed_counter() -> None:
    telemetry = _TelemetryRecorder()
    engine = Gossiper(
        node_id="node-a",
        config=GossipConfig(),
        stats=GossipStats(),
        topologymgr=TopologyManager(),
        peer_pool=cast(PeerPool, cast(object, _FakePool(_FakeClient()))),
        serializer=_MapSerializer(),
        telemetry=cast(Telemetry, cast(object, telemetry)),
    )

    await engine.announce(_member("node-b"))
    await engine.announce(_member("node-c", phase=MemberPhase.FAILED))

    assert telemetry.gossip_hot_queue_size.value == 2
    assert telemetry.gossip_failed_announcements_total.total == 1
    assert engine.status()["hot_queue_size"] == 2


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossiper_stop_warns_when_hot_loop_unavailable_with_pending_queue(
    caplog: pytest.LogCaptureFixture,
) -> None:
    telemetry = _TelemetryRecorder()
    engine = Gossiper(
        node_id="node-a",
        config=GossipConfig(),
        stats=GossipStats(),
        topologymgr=TopologyManager(),
        peer_pool=cast(PeerPool, cast(object, _FakePool(_FakeClient()))),
        serializer=_MapSerializer(),
        telemetry=cast(Telemetry, cast(object, telemetry)),
    )

    done_hot = asyncio.create_task(asyncio.sleep(0))
    done_ae = asyncio.create_task(asyncio.sleep(0))
    await done_hot
    await done_ae
    engine._hot_task = done_hot
    engine._ae_task = done_ae
    await engine.announce(_member("node-b"))

    with caplog.at_level("WARNING"):
        await engine.stop()

    assert "hot queue not drained" in caplog.text


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossiper_push_member_and_send_push_paths_cover_success_and_errors(
    caplog: pytest.LogCaptureFixture,
) -> None:
    serializer = _MapSerializer()
    telemetry = _TelemetryRecorder()
    topology = TopologyManager()
    await topology.merge_registry(
        (
            _member("node-a"),
            _member("node-b"),
            _member("node-c", phase=MemberPhase.PAUSED),
        )
    )

    push_accept = _envelope("gossip.push.accept", serializer, {"accepted": 0})
    push_error = _envelope("gossip.error", serializer, {"error": "reject", "code": ""})
    decode_fail = Envelope(
        kind="gossip.error",
        correlation_id=uuid.uuid4(),
        mode=Envelope.MODE_RPC,
        proto_version=PROTOCOL_VERSION,
        schema_id=serializer.schema_id,
        payload=b"invalid",
    )
    client = _FakeClient(request_responses=[push_accept, push_error, decode_fail])
    pool = _FakePool(client)
    engine = Gossiper(
        node_id="node-a",
        config=GossipConfig(),
        stats=GossipStats(),
        topologymgr=topology,
        peer_pool=cast(PeerPool, cast(object, pool)),
        serializer=serializer,
        telemetry=cast(Telemetry, cast(object, telemetry)),
    )

    await engine._push_member(_member("node-x", epoch=2))
    with caplog.at_level("WARNING"):
        await engine._send_push(_member("node-x", epoch=3), _member("node-b"))
        await engine._send_push(_member("node-x", epoch=4), _member("node-b"))

    error_pool = _FakePool(error=EnvelopeError("boom"))
    error_engine = Gossiper(
        node_id="node-a",
        config=GossipConfig(),
        stats=GossipStats(),
        topologymgr=topology,
        peer_pool=cast(PeerPool, cast(object, error_pool)),
        serializer=serializer,
        telemetry=cast(Telemetry, cast(object, telemetry)),
    )
    await error_engine._send_push(_member("node-x", epoch=5), _member("node-b"))

    assert telemetry.gossip_push_attempt_total.total >= 4
    assert telemetry.gossip_push_error_total.total >= 3
    assert "cid=" in caplog.text


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossiper_ae_cycle_ping_pong_and_digest_delta_paths(
    caplog: pytest.LogCaptureFixture,
) -> None:
    serializer = _MapSerializer()
    telemetry = _TelemetryRecorder()
    topology = TopologyManager()
    local = _member("node-a", epoch=1)
    peer = _member("node-b", epoch=2)
    await topology.merge_registry((local, peer))

    delta_member = _member("node-c", epoch=3)
    pong = _envelope("gossip.pong", serializer, {"same": False})
    push_accept = _envelope("gossip.push.accept", serializer, {"accepted": 1})
    delta = _envelope(
        "gossip.delta",
        serializer,
        {
            "members": [delta_member.to_dict()],
            "wanted": ["node-a"],
            "has_more": False,
        },
    )
    client = _FakeClient(
        request_responses=[pong, push_accept], stream_responses=[delta]
    )
    engine = Gossiper(
        node_id="node-a",
        config=GossipConfig(),
        stats=GossipStats(),
        topologymgr=topology,
        peer_pool=cast(PeerPool, cast(object, _FakePool(client))),
        serializer=serializer,
        telemetry=cast(Telemetry, cast(object, telemetry)),
    )

    await engine._run_ae_cycle()

    snapshot = await topology.snapshot()
    assert snapshot.registry.get("node-c") is not None
    assert telemetry.gossip_ae_cycle_total.total == 1
    assert telemetry.gossip_ae_diverged.total == 1
    assert telemetry.gossip_last_ae_peer.value > 0
    assert telemetry.gossip_last_ae_at.value > 0

    ae_error = Envelope(
        kind="gossip.error",
        correlation_id=uuid.uuid4(),
        mode=Envelope.MODE_RPC,
        proto_version=PROTOCOL_VERSION,
        schema_id=serializer.schema_id,
        payload=b"bad-error",
    )
    error_client = _FakeClient(stream_responses=[ae_error])
    with caplog.at_level("WARNING"):
        await engine._ae_digest_delta(
            peer, cast(EnvelopeClient, cast(object, error_client))
        )

    assert "Received gossip.error during AE cycle" in caplog.text
    assert "cid=" in caplog.text


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossiper_build_digest_and_merge_delta_error_paths(
    caplog: pytest.LogCaptureFixture,
) -> None:
    serializer = _MapSerializer()
    telemetry = _TelemetryRecorder()
    topology = TopologyManager()
    await topology.merge_registry(
        (_member("node-b", epoch=2), _member("node-a", epoch=1))
    )
    engine = Gossiper(
        node_id="node-a",
        config=GossipConfig(),
        stats=GossipStats(),
        topologymgr=topology,
        peer_pool=cast(PeerPool, cast(object, _FakePool(_FakeClient()))),
        serializer=serializer,
        telemetry=cast(Telemetry, cast(object, telemetry)),
    )

    digest_env = await engine._build_digest()
    payload = cast(dict[str, object], serializer.decode(digest_env.payload))
    digest = cast(list[dict[str, object]], payload["digest"])
    assert [cast(str, item["node_id"]) for item in digest] == ["node-a", "node-b"]

    valid_delta = _envelope(
        "gossip.delta",
        serializer,
        {"members": [_member("node-c", epoch=3).to_dict()]},
    )
    await engine._merge_delta(valid_delta, "node-b")

    invalid_delta = Envelope(
        kind="gossip.delta",
        correlation_id=uuid.uuid4(),
        mode=Envelope.MODE_RPC,
        proto_version=PROTOCOL_VERSION,
        schema_id=serializer.schema_id,
        payload=b"invalid-delta",
    )
    with caplog.at_level("WARNING"):
        await engine._merge_delta(invalid_delta, "node-b")

    assert "Failed to merge delta" in caplog.text


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossiper_satisfy_wanted_handles_empty_and_request_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    serializer = _MapSerializer()
    telemetry = _TelemetryRecorder()
    topology = TopologyManager()
    await topology.merge_registry((_member("node-a"), _member("node-b")))
    peer = _member("node-b")

    ok_client = _FakeClient(
        request_responses=[_envelope("gossip.push.accept", serializer, {"accepted": 1})]
    )
    engine = Gossiper(
        node_id="node-a",
        config=GossipConfig(),
        stats=GossipStats(),
        topologymgr=topology,
        peer_pool=cast(PeerPool, cast(object, _FakePool(ok_client))),
        serializer=serializer,
        telemetry=cast(Telemetry, cast(object, telemetry)),
    )

    await engine._satisfy_wanted(
        peer, cast(EnvelopeClient, cast(object, ok_client)), {"node-z"}
    )
    await engine._satisfy_wanted(
        peer, cast(EnvelopeClient, cast(object, ok_client)), {"node-a"}
    )

    failing_client = _FakeClient(request_error=EnvelopeError("failed"))
    with caplog.at_level("WARNING"):
        await engine._satisfy_wanted(
            peer, cast(EnvelopeClient, cast(object, failing_client)), {"node-a"}
        )

    assert "gossip.satisfy_wanted failed" in caplog.text


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_ae_loop_continues_after_network_errors() -> None:
    """Test that anti-entropy loop survives network errors and continues running.

    The AE loop must be resilient: when peer_pool.acquire() raises
    ConnectionRefusedError or TimeoutError, the loop should catch it,
    log it, sleep, and retry rather than dying silently.
    """

    class FailingPool:
        """Pool that always raises ConnectionRefusedError."""

        async def acquire(self, peer_id: str):
            raise ConnectionRefusedError(f"peer {peer_id} unreachable")

    topology = TopologyManager()
    await topology.apply_member(
        Member(
            node_id="peer-a",
            peer_address="127.0.0.1:1",
            epoch=1,
            phase=MemberPhase.READY,
            tokens=("0x10",),
        )
    )

    stats = GossipStats()
    config = GossipConfig(ae_interval=0.05)  # Short interval for testing

    # Create gossiper with failing pool
    gossiper = Gossiper(
        node_id="node-b",
        config=config,
        stats=stats,
        topologymgr=topology,
        peer_pool=cast(PeerPool, FailingPool()),  # type: ignore
        serializer=_FakeSerializer(),
        telemetry=build_noop_telemetry(),
    )

    # Start the gossiper
    await gossiper.start()

    # Wait for multiple AE cycles (with 50ms interval, we get ~6 cycles in 300ms)
    # If AE loop dies on first error, ae_running will be False
    await asyncio.sleep(0.3)

    status = gossiper.status()

    # With the fix, loop should still be running despite network errors
    assert status["ae_running"], (
        "AE loop should survive network errors and continue running. "
        "Wrap _ae_loop body with try/except Exception to catch all errors and retry."
    )

    await asyncio.gather(gossiper.stop(), return_exceptions=True)


def _relay_engine(
    pool: _FakePool, serializer: _MapSerializer, topology: TopologyManager
) -> Gossiper:
    engine = Gossiper(
        node_id="node-a",
        config=GossipConfig(),
        stats=GossipStats(),
        topologymgr=topology,
        peer_pool=cast(PeerPool, cast(object, pool)),
        serializer=serializer,
        telemetry=build_noop_telemetry(),
    )

    engine._is_running = lambda: True  # type: ignore[method-assign]

    return engine


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossiper_relay_forwards_with_remaining_budget_excluding_sender() -> None:
    serializer = _MapSerializer()
    topology = TopologyManager()

    await topology.merge_registry(
        (_member("node-a"), _member("node-b"), _member("node-c"), _member("node-x"))
    )

    accept = _envelope("gossip.push.accept", serializer, {"accepted": 1})
    client = _FakeClient(request_responses=[accept, accept])
    pool = _FakePool(client)
    engine = _relay_engine(pool, serializer, topology)

    engine.relay([_member("node-x", epoch=2)], 2, "node-b")
    await asyncio.gather(*engine._relay_tasks)

    # node-b sent it and node-x is the member itself: only node-c is left.
    assert pool.calls == ["node-c"]
    sent = cast(dict[str, object], serializer.decode(client.requests[0].payload))
    assert sent["hop_limit"] == 2 and sent["sender"] == "node-a"
    assert not engine._relay_tasks


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossiper_relay_drops_spent_budget_idle_engine_and_overflow() -> None:
    serializer = _MapSerializer()
    topology = TopologyManager()
    await topology.merge_registry((_member("node-a"), _member("node-c")))
    pool = _FakePool(_FakeClient())
    engine = _relay_engine(pool, serializer, topology)
    member = _member("node-x", epoch=2)

    engine.relay([member], 0, "node-b")
    engine.relay([], 3, "node-b")
    assert not engine._relay_tasks

    blocker = asyncio.Event()

    engine._relay_tasks.update(
        asyncio.create_task(blocker.wait()) for _ in range(Gossiper._MAX_PENDING_RELAYS)
    )

    engine.relay([member], 3, "node-b")
    assert len(engine._relay_tasks) == Gossiper._MAX_PENDING_RELAYS

    await engine._cancel_relays()
    assert not engine._relay_tasks

    engine._is_running = lambda: False  # type: ignore[method-assign]
    engine.relay([member], 3, "node-b")
    assert not engine._relay_tasks and pool.calls == []
