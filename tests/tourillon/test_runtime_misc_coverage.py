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
import signal
import ssl
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.tourillon.test_lifecycle import _build_config, _DocumentStatePersistence
from tourillon.core.gossip.bootstrapper import GossipBootstrapper
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.manager import Manager
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.partition.placement import SimplePlacementStrategy
from tourillon.core.partition.types import PartitionPlacement
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.telemetry import build_noop_telemetry
from tourillon.core.transports.client import PeerClient
from tourillon.core.transports.dispatcher import Dispatcher
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import build_envelope
from tourlib.models.inspect import NodeInspectRequest
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address


class _FD:
    def __init__(self, alive: bool = True) -> None:
        self._alive = alive
        self.runs = []

    async def run(self, client) -> None:
        self.runs.append(client)

    def is_alive(self) -> bool:
        return self._alive


class _PeerPoolStub:
    def __init__(self, detectors: dict[str, _FD | None]) -> None:
        self._detectors = detectors

    def failure_detector(self, node_id: str):
        return self._detectors.get(node_id)


class _Storage:
    def __init__(self) -> None:
        self.closed = False
        self.latest = HLCTimestamp(wall_ms=12, counter=3, node_id="seed")

    async def session_provider_for(self, group: int):
        raise AssertionError(f"unexpected group lookup {group}")

    async def latest_hlc(self, total_groups: int) -> HLCTimestamp:
        assert total_groups > 0
        return self.latest

    async def close(self) -> None:
        self.closed = True


class _StorageProvider:
    def __init__(self, storage: _Storage) -> None:
        self._storage = storage

    def create(self, config):
        assert config.data_dir
        return self._storage


class _TelemetryProvider:
    def create(self, config):
        del config
        return build_noop_telemetry()


class _BootstrapClient:
    def __init__(
        self, *, stream_items=None, connect_error: Exception | None = None
    ) -> None:
        self._stream_items = list(stream_items or [])
        self._connect_error = connect_error
        self.closed = False
        self.connected = False

    async def connect(self) -> None:
        if self._connect_error is not None:
            raise self._connect_error
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def stream(self, env):
        assert env.kind == "gossip.digest"
        for item in self._stream_items:
            yield item


def _member(node_id: str, epoch: int, phase: MemberPhase, token: int) -> Member:
    return Member(
        node_id=node_id,
        peer_address=f"{node_id}:9000",
        epoch=epoch,
        phase=phase,
        tokens=(hex(token),),
    )


def _build_manager(tmp_path: Path, storage: _Storage) -> Manager:
    config = _build_config(tmp_path, node_id="manager-node")
    return Manager(
        config=config,
        peer_dispatcher=Dispatcher(),
        client_dispatcher=Dispatcher(),
        tls_server=ssl.create_default_context(),
        tls_client=ssl.create_default_context(),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "manager-node", "phase": "idle", "epoch": 0}}
        ),
        serializer=MsgPackSerializer(),
        storage_provider=_StorageProvider(storage),
        telemetry_provider=_TelemetryProvider(),
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_bootstrapper_run_retry_and_attempt_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serializer = MsgPackSerializer()
    topologymgr = TopologyManager()
    bootstrapper = GossipBootstrapper(
        node_id="node-a",
        serializer=serializer,
        topologymgr=topologymgr,
        backoff=SimpleNamespace(
            initial_delay=0.01, max_retries=2, jitter=0.0, factor=2.0, max_delay=1.0
        ),
        tls_client=ssl.create_default_context(),
    )
    attempts = {"count": 0}

    async def fake_attempt(seeds):
        attempts["count"] += 1
        if attempts["count"] == 1:
            from tourillon.core.exceptions import BootstrapAttemptError

            raise BootstrapAttemptError("no seed")
        assert seeds == ("seed-1:9000",)
        return 1

    async def no_sleep(delay: float) -> None:
        assert delay >= 0

    monkeypatch.setattr(bootstrapper, "_attempt", fake_attempt)
    monkeypatch.setattr("tourillon.core.gossip.bootstrapper.asyncio.sleep", no_sleep)
    monkeypatch.setattr(
        "tourillon.core.gossip.bootstrapper.random.uniform", lambda a, b: 0
    )

    assert await bootstrapper.run(("seed-1:9000",)) == 1
    assert attempts["count"] == 2

    with pytest.raises(RuntimeError, match="without seeds"):
        await bootstrapper.run(())


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_bootstrapper_exchange_and_attempt_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serializer = MsgPackSerializer()
    topologymgr = TopologyManager()
    bootstrapper = GossipBootstrapper(
        node_id="node-a",
        serializer=serializer,
        topologymgr=topologymgr,
        backoff=SimpleNamespace(
            initial_delay=0.01, max_retries=1, jitter=0.0, factor=2.0, max_delay=1.0
        ),
        tls_client=ssl.create_default_context(),
    )
    good_member = _member("node-b", 1, MemberPhase.READY, 10)
    delta = build_envelope(
        kind="gossip.delta",
        payload=serializer.encode(
            {"members": [good_member.to_dict(), {"broken": True}], "has_more": False}
        ),
        schema_id=serializer.schema_id,
    )
    error = build_envelope(
        kind="gossip.error",
        payload=serializer.encode({}),
        schema_id=serializer.schema_id,
    )
    weird = build_envelope(
        kind="gossip.weird",
        payload=serializer.encode({}),
        schema_id=serializer.schema_id,
    )

    assert await bootstrapper._exchange_digest(
        _BootstrapClient(stream_items=[delta]), "seed"
    ) == [
        good_member
    ]  # noqa: SLF001
    assert (
        await bootstrapper._exchange_digest(
            _BootstrapClient(stream_items=[error]), "seed"
        )
        == []
    )  # noqa: SLF001
    assert (
        await bootstrapper._exchange_digest(
            _BootstrapClient(stream_items=[weird]), "seed"
        )
        == []
    )  # noqa: SLF001

    clients = {
        "ok:9000": _BootstrapClient(stream_items=[delta]),
        "bad:9000": _BootstrapClient(connect_error=OSError("down")),
    }
    monkeypatch.setattr(
        "tourillon.core.gossip.bootstrapper.EnvelopeClient",
        lambda address, tls_ctx, logger: clients[address],
    )

    results = [False, False]
    await bootstrapper._attempt_seed("ok:9000", results, 0)  # noqa: SLF001
    await bootstrapper._attempt_seed("bad:9000", results, 1)  # noqa: SLF001
    assert results == [True, False]
    assert clients["ok:9000"].closed is True
    assert clients["bad:9000"].closed is True


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_simple_placement_strategy_builds_preference_list_and_handoffs() -> None:
    topologymgr = TopologyManager()
    members = [
        _member("node-a", 1, MemberPhase.READY, 10),
        _member("node-b", 1, MemberPhase.DRAINING, 40),
        _member("node-c", 1, MemberPhase.READY, 70),
        _member("node-d", 1, MemberPhase.PAUSED, 90),
        _member("node-e", 1, MemberPhase.JOINING, 120),
    ]
    for member in members:
        await topologymgr.apply_member(member)

    detectors = {
        "node-a": _FD(alive=False),
        "node-b": _FD(alive=True),
        "node-c": _FD(alive=True),
        "node-d": _FD(alive=True),
    }
    strategy = SimplePlacementStrategy(
        replication_factor=2,
        topologymgr=topologymgr,
        partitioner=Partitioner(pshift=4, gshift=1, hashspace=HashSpace(bits=8)),
        peer_pool=_PeerPoolStub(detectors),
    )
    first_vnode = members[0].vnodes[0]
    topology = await topologymgr.snapshot()
    assert [
        node_id
        async for node_id in strategy._handoff_candidates(  # noqa: SLF001
            first_vnode,
            topology,
            {"node-a", "node-b"},
        )
    ] == ["node-c"]

    async def fake_candidates(*args, **kwargs):
        del args, kwargs
        yield "node-c"
        yield "node-d"

    strategy._handoff_candidates = fake_candidates  # type: ignore[method-assign]  # noqa: SLF001
    strategy._placement_addr = lambda addr, ring: PartitionPlacement(  # type: ignore[method-assign]  # noqa: SLF001
        group=0,
        vnode=first_vnode,
        partition=strategy._partitioner.partition_for(0),  # noqa: SLF001
    )

    placement = await strategy.placement_for(Address(namespace=b"ns", key=b"key"))
    prefs = await strategy.preference_list(Address(namespace=b"ns", key=b"key"))

    assert placement.group in {0, 1}
    assert prefs.placement == placement
    assert len(prefs.preferences) == 2
    assert any(pref.handoff is not None for pref in prefs.preferences)
    assert strategy._is_alive("node-c") is True  # noqa: SLF001


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_manager_start_stop_inspect_join_and_serve_forever(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    storage = _Storage()
    manager = _build_manager(tmp_path, storage)
    started = []
    stopped = []
    joins = []
    restored = []

    async def fake_start(*, seeds=None) -> None:
        started.append(seeds)

    async def fake_stop() -> None:
        stopped.append(True)

    async def fake_join(*, seeds=None) -> None:
        joins.append(seeds)

    async def fake_shutdown() -> None:
        stopped.append("pool")

    async def fake_inspect(request):
        yield {"request": request.limit}

    manager._lifecycle.start = fake_start  # type: ignore[method-assign]  # noqa: SLF001
    manager._lifecycle.stop = fake_stop  # type: ignore[method-assign]  # noqa: SLF001
    manager._lifecycle.join = fake_join  # type: ignore[method-assign]  # noqa: SLF001
    manager._peer_pool.shutdown = fake_shutdown  # type: ignore[method-assign]  # noqa: SLF001
    manager._inspector.inspect = fake_inspect  # type: ignore[method-assign]  # noqa: SLF001
    manager._clock.restore = lambda wall_ms, counter: restored.append((wall_ms, counter))  # type: ignore[method-assign]  # noqa: SLF001

    assert manager.node_id == "manager-node"
    assert manager.seeds is None
    assert manager.serializer is not None
    assert manager.gossip is manager._gossip  # noqa: SLF001
    assert manager.transfer is manager._transfer  # noqa: SLF001
    assert manager.telemetry is manager._telemetry  # noqa: SLF001
    assert manager.store is manager._store  # noqa: SLF001

    await manager.start(seeds=["seed:9000"])
    assert restored == [(12, 3)]
    assert started == [["seed:9000"]]

    infos = [info async for info in manager.inspect(NodeInspectRequest())]
    assert infos == [{"request": 1024}]

    await manager.join(seeds=["seed:9000"])
    assert joins == [["seed:9000"]]

    await manager.stop()
    assert stopped == [True, "pool"]

    def fake_install(event: asyncio.Event) -> None:
        event.set()

    monkeypatch.setattr(
        manager, "_install_signal_handlers", fake_install, raising=False
    )
    started.clear()
    stopped.clear()
    await manager.serve_forever()
    assert started == [None]
    assert stopped == [True, "pool"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_manager_serve_forever_stops_after_start_failure(tmp_path: Path) -> None:
    manager = _build_manager(tmp_path, _Storage())
    stopped: list[bool] = []

    async def failing_start(*, seeds=None) -> None:
        assert seeds == ["seed:9000"]
        raise RuntimeError("boom")

    async def fake_stop() -> None:
        stopped.append(True)

    manager.start = failing_start  # type: ignore[method-assign]
    manager.stop = fake_stop  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="boom"):
        await manager.serve_forever(seeds=["seed:9000"])

    assert stopped == [True]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_manager_serve_forever_installs_sigint_and_sigterm_handlers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    manager = _build_manager(tmp_path, _Storage())
    installed: list[asyncio.Event] = []
    stopped: list[bool] = []

    async def fake_start(*, seeds=None) -> None:
        assert seeds is None

    async def fake_stop() -> None:
        stopped.append(True)

    async def immediate_wait(self) -> None:
        return None

    def fake_install(event: asyncio.Event) -> None:
        installed.append(event)
        event.set()

    manager.start = fake_start  # type: ignore[method-assign]
    manager.stop = fake_stop  # type: ignore[method-assign]
    monkeypatch.setattr(asyncio.Event, "wait", immediate_wait)
    monkeypatch.setattr(
        manager, "_install_signal_handlers", fake_install, raising=False
    )

    await manager.serve_forever()

    assert len(installed) == 1
    assert stopped == [True]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_manager_serve_forever_handles_keyboard_interrupt_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    manager = _build_manager(tmp_path, _Storage())
    stopped: list[bool] = []

    async def fake_start(*, seeds=None) -> None:
        assert seeds is None

    async def fake_stop() -> None:
        stopped.append(True)

    async def interrupted_wait(self) -> None:
        raise KeyboardInterrupt

    manager.start = fake_start  # type: ignore[method-assign]
    manager.stop = fake_stop  # type: ignore[method-assign]
    monkeypatch.setattr(asyncio.Event, "wait", interrupted_wait)
    monkeypatch.setattr(
        manager, "_install_signal_handlers", lambda event: None, raising=False
    )

    await manager.serve_forever()

    assert stopped == [True]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_manager_install_signal_handlers_registers_sigint_and_sigterm(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    manager = _build_manager(tmp_path, _Storage())
    registered: list[signal.Signals] = []

    class _LoopStub:
        def add_signal_handler(self, sig: signal.Signals, callback) -> None:
            registered.append(sig)
            assert callback is not None

    monkeypatch.setattr(
        "tourillon.core.manager.asyncio.get_running_loop", lambda: _LoopStub()
    )

    manager._install_signal_handlers(asyncio.Event())  # type: ignore[attr-defined]  # noqa: SLF001

    assert registered == [signal.SIGINT, signal.SIGTERM]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_manager_stop_continues_shutdown_after_lifecycle_error(
    tmp_path: Path,
) -> None:
    manager = _build_manager(tmp_path, _Storage())
    steps: list[str] = []

    async def failing_lifecycle_stop() -> None:
        steps.append("lifecycle")
        raise ConnectionError("stop failed")

    async def fake_pool_shutdown() -> None:
        steps.append("pool")

    async def fake_storage_close() -> None:
        steps.append("storage")

    async def fake_telemetry_close() -> None:
        steps.append("telemetry")

    manager._lifecycle.stop = failing_lifecycle_stop  # type: ignore[method-assign]  # noqa: SLF001
    manager._peer_pool.shutdown = fake_pool_shutdown  # type: ignore[method-assign]  # noqa: SLF001
    manager._storage.close = fake_storage_close  # type: ignore[method-assign]  # noqa: SLF001
    manager._telemetry.close = fake_telemetry_close  # type: ignore[method-assign]  # noqa: SLF001

    await manager.stop()

    assert steps == ["lifecycle", "pool", "storage", "telemetry"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_peer_client_reports_failures_to_detector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fd = _FD()
    client = PeerClient("127.0.0.1:9000", ssl.create_default_context(), fd)
    request_env = build_envelope(kind="probe.ping", payload=b"{}", schema_id=1)
    reply_env = build_envelope(kind="probe.pong", payload=b"{}", schema_id=1)

    async def ok_request(self, envelope, timeout):
        assert envelope is request_env
        assert timeout > 0
        return reply_env

    async def failing_request(self, envelope, timeout):
        del envelope, timeout
        raise TimeoutError("boom")

    async def ok_stream(self, envelope, timeout):
        assert envelope is request_env
        assert timeout > 0
        yield reply_env

    async def failing_stream(self, envelope, timeout):
        del envelope, timeout
        raise ConnectionError("boom")
        yield reply_env  # pragma: no cover

    monkeypatch.setattr(
        "tourillon.core.transports.client.EnvelopeClient.request", ok_request
    )
    assert await client.request(request_env) == reply_env

    monkeypatch.setattr(
        "tourillon.core.transports.client.EnvelopeClient.request", failing_request
    )
    with pytest.raises(TimeoutError, match="boom"):
        await client.request(request_env)
    assert fd.runs[-1] is client

    monkeypatch.setattr(
        "tourillon.core.transports.client.EnvelopeClient.stream", ok_stream
    )
    assert [item async for item in client.stream(request_env)] == [reply_env]

    monkeypatch.setattr(
        "tourillon.core.transports.client.EnvelopeClient.stream", failing_stream
    )
    with pytest.raises(ConnectionError, match="boom"):
        [item async for item in client.stream(request_env)]
    assert fd.runs[-1] is client
