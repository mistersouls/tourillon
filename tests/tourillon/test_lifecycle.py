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
import contextvars
import logging
import ssl
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Any

import pytest

from tourillon.core.ae.obligations import ObligationQueue
from tourillon.core.config import (
    ClientServerConfig,
    NodeConfig,
    PeerServerConfig,
    StorageConfig,
    TlsConfig,
    TourillonConfig,
    TransportConfig,
    TransportPoolConfig,
)
from tourillon.core.exceptions import (
    BootstrapError,
    DrainError,
    JoinError,
    PauseError,
    ResumeError,
)
from tourillon.core.gossip.gossiper import GossipStats
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.clock import HLCClock
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.models.state import NodeState
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.partition.placement import SimplePlacementStrategy
from tourillon.core.peer.pool import PeerPool
from tourillon.core.probe.detector import FailureDetectorProvider
from tourillon.core.services.gc import GarbageCollector
from tourillon.core.services.lifecycle import NodeLifecycle, _background_task
from tourillon.core.services.store import StoreService
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.storage.retention import RetentionPolicy
from tourillon.core.telemetry import build_noop_telemetry
from tourillon.core.transports.dispatcher import Dispatcher
from tourlib.adapter import MsgPackSerializer
from tourlib.constant import NODE_STATE_FILENAME
from tourlib.exceptions import ValidationError
from tourlib.models.ts import HLCTimestamp
from tourlib.port import Persistence


class _MissingStatePersistence(Persistence):
    def __init__(self) -> None:
        self.writes: list[tuple[Path, Mapping[str, object]]] = []

    async def load(self, path: Path) -> Mapping[str, object]:
        raise FileNotFoundError(path)

    async def write(self, path: Path, document: Mapping[str, object]) -> None:
        self.writes.append((path, document))

    def edit(self, path: Path) -> AbstractAsyncContextManager[dict[str, Any]]:
        _ = path

        class _MissingEditContext(AbstractAsyncContextManager[dict[str, Any]]):
            async def __aenter__(self) -> dict[str, Any]:
                raise FileNotFoundError(path)

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: object,
            ) -> bool | None:
                _ = exc_type
                _ = exc
                _ = tb
                return None

        return _MissingEditContext()


class _DocumentStatePersistence(Persistence):
    def __init__(self, document: dict[str, object]) -> None:
        self._document = document

    async def load(self, path: Path) -> Mapping[str, object]:
        return self._document

    async def write(self, path: Path, document: Mapping[str, object]) -> None:
        _ = path
        self._document = dict(document)

    def edit(self, path: Path) -> AbstractAsyncContextManager[dict[str, object]]:
        _ = path

        owner = self
        document = dict(owner._document)

        class _DocumentEditContext(AbstractAsyncContextManager[dict[str, object]]):
            async def __aenter__(self) -> dict[str, object]:
                return document

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: object,
            ) -> bool | None:
                _ = exc_type
                _ = exc
                _ = tb
                owner._document = document
                return None

        return _DocumentEditContext()


class _StorageStub:
    """Storage stub for testing that blocks access."""

    async def open(self, pid: int):
        raise AssertionError(f"unexpected storage access for pid={pid}")

    async def latest_hlc(self) -> HLCTimestamp:
        return HLCTimestamp(wall_ms=0, counter=0, node_id="test-node")

    async def close(self) -> None:
        return None


def _build_config(data_dir: Path, node_id: str = "test-node") -> TourillonConfig:
    return TourillonConfig(
        node=NodeConfig(id=node_id),
        storage=StorageConfig(data_dir=str(data_dir), group_size="8Mi"),
        tls=TlsConfig(ca_data="", cert_data="", key_data=""),
        server_client=ClientServerConfig(bind="127.0.0.1:7000"),
        server_peer=PeerServerConfig(bind="127.0.0.1:7001", advertise="127.0.0.1:7001"),
    )


def _build_lifecycle(
    config: TourillonConfig, persistence: Persistence
) -> NodeLifecycle:
    serializer = MsgPackSerializer()
    pool_opts = config.transport.pool
    topologymgr = TopologyManager()
    peer_pool = PeerPool(
        tls_ctx=ssl.create_default_context(),
        topologymgr=topologymgr,
        fd_provider=FailureDetectorProvider(serializer=serializer),
        telemetry=build_noop_telemetry(),
        min_size=pool_opts.min_size,
        max_size=pool_opts.max_size,
        max_overflow=pool_opts.max_overflow,
        priority_size=pool_opts.priority_size,
        idle_ttl=pool_opts.idle_ttl,
    )
    partitioner = Partitioner(hashspace=HashSpace(bits=128), pshift=17, gshift=5)
    storage = _StorageStub()
    placement = SimplePlacementStrategy(
        replication_factor=config.placement.replication_factor,
        topologymgr=topologymgr,
        partitioner=partitioner,
        peer_pool=peer_pool,
    )
    retention = RetentionPolicy(config.placement.retention)
    obligations = ObligationQueue(config.placement.anti_entropy.hotqueue_size)
    store = StoreService(
        retention=retention,
        node_id=config.node.id,
        storage=storage,
        partitioner=partitioner,
        placement_strategy=placement,
        clock=HLCClock(node_id=config.node.id),
        serializer=serializer,
        peer_pool=peer_pool,
        obligations=obligations,
    )
    return NodeLifecycle(
        placement_strategy=placement,
        config=config,
        peer_dispatcher=Dispatcher(),
        client_dispatcher=Dispatcher(),
        tls_server=ssl.create_default_context(),
        tls_client=ssl.create_default_context(),
        persistence=persistence,
        serializer=serializer,
        peer_pool=peer_pool,
        topologymgr=topologymgr,
        partitioner=Partitioner(hashspace=HashSpace(bits=128), pshift=17, gshift=5),
        storage=storage,
        gossip_stats=GossipStats(),
        telemetry=build_noop_telemetry(),
        store=store,
        obligations=obligations,
        gc=GarbageCollector(
            storage,
            partitioner,
            config.placement.retention,
            node_id=config.node.id,
            telemetry=build_noop_telemetry(),
        ),
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_load_state_defaults_to_config_node_id_when_missing(
    tmp_path: Path,
) -> None:
    persistence = _MissingStatePersistence()
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=persistence,
    )

    state = await lifecycle._load_or_create_state()

    assert state.node_id == "node-a"
    assert state.phase == MemberPhase.IDLE
    assert state.epoch == 0
    assert persistence.writes == [
        (
            tmp_path / NODE_STATE_FILENAME,
            {
                "node": {
                    "node_id": "node-a",
                    "phase": "idle",
                    "epoch": 0,
                }
            },
        )
    ]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_load_state_uses_persisted_document(tmp_path: Path) -> None:
    persistence = _DocumentStatePersistence(
        {
            "node": {
                "node_id": "node-b",
                "phase": "ready",
                "prev_phase": "joining",
                "epoch": 4,
            }
        }
    )
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=persistence,
    )

    state = await lifecycle._load_or_create_state()

    assert state.node_id == "node-b"
    assert state.phase == MemberPhase.READY
    assert "prev_phase" not in state.to_document()["node"]
    assert state.epoch == 4


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_lifecycle_configures_peer_pool_from_transport_pool_options(
    tmp_path: Path,
) -> None:
    config = TourillonConfig(
        node=NodeConfig(id="node-pool"),
        storage=StorageConfig(data_dir=str(tmp_path), group_size="8Mi"),
        tls=TlsConfig(ca_data="", cert_data="", key_data=""),
        server_client=ClientServerConfig(bind="127.0.0.1:7000"),
        server_peer=PeerServerConfig(bind="127.0.0.1:7001", advertise="127.0.0.1:7001"),
        transport=TransportConfig(
            pool=TransportPoolConfig(
                min_size=2,
                max_size=7,
                max_overflow=3,
                priority_size=1,
                idle_ttl=9.5,
            )
        ),
    )
    lifecycle = _build_lifecycle(config=config, persistence=_MissingStatePersistence())

    assert lifecycle._peer_pool._min_size == 2
    assert lifecycle._peer_pool._max_size == 7
    assert lifecycle._peer_pool._max_overflow == 3
    assert lifecycle._peer_pool._priority_size == 1
    assert lifecycle._peer_pool._idle_ttl == 9.5


@pytest.mark.tourillon
def test_log_state_tokens_renders_one_line_per_token(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence({"node": {"node_id": "node-a"}}),
    )
    state = NodeState(
        node_id="node-a",
        phase=MemberPhase.READY,
        epoch=7,
        tokens=(
            "0x0000000000000001",
            "0x000000001234abcd",
            "0x0123456789abcdef0123456789abcdef",
        ),
    )

    caplog.set_level(logging.DEBUG, logger="tourillon.global")
    lifecycle._log_tokens(state)

    messages = [record.getMessage() for record in caplog.records]
    assert messages == [
        "Node 'node-a' epoch=7 owns 3 token(s):",
        "  [1] 0x0000000000000001",
        "  [2] 0x000000001234abcd",
        "  [3] 0x0123456789abcdef",
    ]


@pytest.mark.tourillon
def test_log_state_tokens_renders_none_when_empty(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence({"node": {"node_id": "node-a"}}),
    )
    state = NodeState(
        node_id="node-a",
        phase=MemberPhase.IDLE,
        epoch=0,
        tokens=(),
    )

    caplog.set_level(logging.DEBUG, logger="tourillon.global")
    lifecycle._log_tokens(state)

    messages = [record.getMessage() for record in caplog.records]
    assert messages == ["Node 'node-a' epoch=0 owns 0 token(s)"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_join_raises_when_no_seeds_configured(tmp_path: Path) -> None:
    from tourillon.core.exceptions import JoinError

    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "idle", "epoch": 0}}
        ),
    )

    with pytest.raises(JoinError, match="No seed peers provided"):
        await lifecycle.join()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_join_on_ready_node_is_a_no_op(tmp_path: Path) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "ready", "epoch": 3}}
        ),
    )

    assert await lifecycle.join() is False
    assert lifecycle.is_joining() is False


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_cancel_join_returns_to_idle_and_stops_join_services(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {
                "node": {
                    "node_id": "node-a",
                    "phase": "joining",
                    "epoch": 4,
                    "tokens": ["0x80"],
                    "seeds": ["seed-a"],
                }
            }
        ),
    )

    async def blocked() -> None:
        await asyncio.Event().wait()

    task = asyncio.create_task(blocked())
    lifecycle._join_task = task  # noqa: SLF001
    await asyncio.sleep(0)
    events: list[str] = []

    async def noop() -> None:
        events.append("started")

    async def publish(state: NodeState) -> Member:
        events.append(f"published:{state.phase.value}")
        return lifecycle._member(state)  # noqa: SLF001

    monkeypatch.setattr(lifecycle._peer_pool, "start", noop)  # noqa: SLF001
    monkeypatch.setattr(lifecycle._gossiper, "start", noop)  # noqa: SLF001
    monkeypatch.setattr(lifecycle._antientropy, "stop", noop)  # noqa: SLF001
    monkeypatch.setattr(lifecycle, "_publish_state", publish)

    state = await lifecycle.cancel_join()

    assert state.phase == MemberPhase.IDLE
    assert state.epoch == 5
    assert events == ["started", "started", "published:idle", "started"]
    assert task.cancelled()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_cancel_join_rejects_node_not_joining(tmp_path: Path) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "ready", "epoch": 1}}
        ),
    )

    with pytest.raises(JoinError, match="Node is not joining"):
        await lifecycle.cancel_join()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_cancel_drain_returns_to_ready_and_reopens_services(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "draining", "epoch": 4}}
        ),
    )

    async def blocked() -> None:
        await asyncio.Event().wait()

    task = asyncio.create_task(blocked())
    lifecycle._drain_task = task  # noqa: SLF001
    await asyncio.sleep(0)
    events: list[str] = []

    async def start() -> None:
        events.append("started")

    async def publish(state: NodeState) -> Member:
        events.append(f"published:{state.phase.value}")
        return lifecycle._member(state)  # noqa: SLF001

    async def start_client() -> None:
        events.append("client")

    monkeypatch.setattr(lifecycle._gossiper, "start", start)  # noqa: SLF001
    monkeypatch.setattr(lifecycle._antientropy, "start", start)  # noqa: SLF001
    monkeypatch.setattr(lifecycle, "_publish_state", publish)
    monkeypatch.setattr(lifecycle, "_start_client", start_client)

    state = await lifecycle.cancel_drain()

    assert state.phase == MemberPhase.READY
    assert state.epoch == 5
    assert events == ["started", "published:ready", "client", "started"]
    assert task.cancelled()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_cancel_drain_rejects_node_not_draining(tmp_path: Path) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "ready", "epoch": 1}}
        ),
    )

    with pytest.raises(DrainError, match="Node is not draining"):
        await lifecycle.cancel_drain()


@pytest.mark.tourillon
def test_is_joining_returns_false_when_no_task(tmp_path: Path) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "idle", "epoch": 0}}
        ),
    )

    assert lifecycle.is_joining() is False


@pytest.mark.tourillon
def test_generate_tokens_returns_hex_strings(tmp_path: Path) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "idle", "epoch": 0}}
        ),
    )

    tokens = lifecycle._generate_tokens()

    assert isinstance(tokens, tuple)
    assert len(tokens) > 0
    for token in tokens:
        assert isinstance(token, str)
        int(token, 16)  # must be valid hex


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_start_peer_rejects_invalid_bind(tmp_path: Path) -> None:
    config = TourillonConfig(
        node=NodeConfig(id="test-node"),
        storage=StorageConfig(data_dir=str(tmp_path), group_size="8Mi"),
        tls=TlsConfig(ca_data="", cert_data="", key_data=""),
        server_client=ClientServerConfig(bind="127.0.0.1:7000"),
        server_peer=PeerServerConfig(bind="invalid-bind", advertise="127.0.0.1:7001"),
    )
    lifecycle = _build_lifecycle(config=config, persistence=_MissingStatePersistence())

    with pytest.raises(ValidationError, match="Invalid server_peer.bind format"):
        await lifecycle._start_peer()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_start_client_rejects_invalid_bind(tmp_path: Path) -> None:
    config = TourillonConfig(
        node=NodeConfig(id="test-node"),
        storage=StorageConfig(data_dir=str(tmp_path), group_size="8Mi"),
        tls=TlsConfig(ca_data="", cert_data="", key_data=""),
        server_client=ClientServerConfig(bind="invalid-bind"),
        server_peer=PeerServerConfig(bind="127.0.0.1:7001", advertise="127.0.0.1:7001"),
    )
    lifecycle = _build_lifecycle(config=config, persistence=_MissingStatePersistence())

    with pytest.raises(ValidationError, match="Invalid server_client.bind format"):
        await lifecycle._start_client()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_sync_full_returns_false_when_bootstrapper_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path),
        persistence=_MissingStatePersistence(),
    )
    state = NodeState(
        node_id="node-a", phase=MemberPhase.JOINING, epoch=1, seeds=("seed1:9001",)
    )

    class _Bootstrapper:
        async def run(self, seeds):
            _ = seeds
            raise BootstrapError("boom")

    monkeypatch.setattr(
        "tourillon.core.services.lifecycle.GossipBootstrapper",
        lambda **kwargs: _Bootstrapper(),
    )

    assert await lifecycle._sync_full(state) is False
    assert lifecycle._gossip_stats.bootstrap_err_total == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_transition_to_persists_phase_tokens_and_seeds(tmp_path: Path) -> None:
    persistence = _DocumentStatePersistence(
        {
            "node": {
                "node_id": "node-a",
                "phase": "idle",
                "epoch": 0,
            }
        }
    )
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=persistence,
    )
    state = NodeState(node_id="node-a", phase=MemberPhase.IDLE, epoch=0)

    transitioned = await lifecycle._transition_to(
        state,
        MemberPhase.JOINING,
        with_tokens=True,
        with_seeds=["seed:9000"],
    )

    assert transitioned.phase == MemberPhase.JOINING
    assert "prev_phase" not in transitioned.to_document()["node"]
    assert transitioned.epoch == 1
    assert transitioned.tokens
    assert transitioned.seeds == ("seed:9000",)


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_start_dispatches_to_phase_specific_startup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "ready", "epoch": 4}}
        ),
    )
    calls: list[tuple[str, list[str] | None]] = []

    async def idle(state, seeds=None) -> None:
        calls.append(("idle", seeds))

    async def joining(state, seeds=None) -> None:
        calls.append(("joining", seeds))

    async def ready(state, seeds=None) -> None:
        calls.append(("ready", seeds))

    async def draining() -> None:
        calls.append(("draining", None))

    async def failed() -> None:
        calls.append(("failed", None))

    async def paused() -> None:
        calls.append(("paused", None))

    monkeypatch.setattr(lifecycle, "_startup_idle", idle)
    monkeypatch.setattr(lifecycle, "_startup_joining", joining)
    monkeypatch.setattr(lifecycle, "_startup_ready", ready)
    monkeypatch.setattr(lifecycle, "_startup_draining", draining)
    monkeypatch.setattr(lifecycle, "_startup_failed", failed)
    monkeypatch.setattr(lifecycle, "_startup_paused", paused)

    for phase, expected in [
        (MemberPhase.IDLE, "idle"),
        (MemberPhase.JOINING, "joining"),
        (MemberPhase.READY, "ready"),
        (MemberPhase.DRAINING, "draining"),
        (MemberPhase.FAILED, "failed"),
        (MemberPhase.PAUSED, "paused"),
    ]:
        lifecycle._persistence = _DocumentStatePersistence(  # noqa: SLF001
            {"node": {"node_id": "node-a", "phase": phase.value, "epoch": 4}}
        )
        await lifecycle.start(seeds=["seed:9000"])
        assert calls[-1][0] == expected


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_paused_startup_keeps_client_closed_and_starts_control_services(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lifecycle = _build_lifecycle(
        _build_config(tmp_path),
        _DocumentStatePersistence(
            {"node": {"node_id": "test-node", "phase": "paused"}}
        ),
    )
    calls = []

    async def start_peer() -> None:
        calls.append("peer")

    async def start_paused_services(state: NodeState) -> None:
        calls.append(state.phase)

    monkeypatch.setattr(lifecycle, "_start_peer", start_peer)
    monkeypatch.setattr(lifecycle, "_start_paused_services", start_paused_services)

    await lifecycle._startup_paused()

    assert calls == ["peer", MemberPhase.PAUSED]
    assert not lifecycle._client_started


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_stop_cancels_active_join_and_stops_components(tmp_path: Path) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "ready", "epoch": 1}}
        ),
    )
    seen: list[str] = []

    async def join_task() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            seen.append("cancelled")
            raise

    lifecycle._join_task = asyncio.create_task(join_task())  # noqa: SLF001
    await asyncio.sleep(0)

    async def stop_client() -> None:
        seen.append("client")

    async def stop_antientropy() -> None:
        seen.append("antientropy")

    async def stop_gossiper() -> None:
        seen.append("gossiper")

    async def stop_peer() -> None:
        seen.append("peer")

    lifecycle._stop_client = stop_client  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._antientropy.stop = stop_antientropy  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._gossiper.stop = stop_gossiper  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._stop_peer = stop_peer  # type: ignore[method-assign]  # noqa: SLF001

    await lifecycle.stop()

    assert seen == ["client", "cancelled", "antientropy", "gossiper", "peer"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_startup_idle_branches_on_seed_configuration(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "idle", "epoch": 0}}
        ),
    )
    state = NodeState(node_id="node-a", phase=MemberPhase.IDLE, epoch=0)
    events: list[str] = []

    async def start_peer() -> None:
        events.append("peer")

    async def start_ready(_state: NodeState) -> None:
        events.append("ready")

    async def transition_to(*args, **kwargs) -> NodeState:
        events.append("transition")
        return NodeState(
            node_id="node-a",
            phase=MemberPhase.READY,
            epoch=1,
            tokens=("0x1",),
        )

    lifecycle._start_peer = start_peer  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._start_ready = start_ready  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._transition_to = transition_to  # type: ignore[method-assign]  # noqa: SLF001

    await lifecycle._startup_idle(state, seeds=["seed:9000"])
    assert events == ["peer"]

    events.clear()
    await lifecycle._startup_idle(state.with_seeds(["persisted:9000"]))
    assert events == ["peer"]

    events.clear()
    await lifecycle._startup_idle(state)
    assert events == ["peer", "transition", "ready"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_publish_state_uses_advertise_address(tmp_path: Path) -> None:
    config = TourillonConfig(
        node=NodeConfig(id="node-a"),
        storage=StorageConfig(data_dir=str(tmp_path), group_size="8Mi"),
        tls=TlsConfig(ca_data="", cert_data="", key_data=""),
        server_client=ClientServerConfig(bind="127.0.0.1:7000"),
        server_peer=PeerServerConfig(bind="0.0.0.0:7001", advertise="10.0.0.5:7001"),
    )
    lifecycle = _build_lifecycle(
        config=config,
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "ready", "epoch": 2}}
        ),
    )
    state = NodeState(
        node_id="node-a",
        phase=MemberPhase.READY,
        epoch=2,
        tokens=("0x1",),
    )
    applied: list[Member] = []
    announced: list[Member] = []

    async def apply_member(member) -> None:
        applied.append(member)

    async def announce(member) -> None:
        announced.append(member)

    lifecycle._topologymgr.apply_member = apply_member  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._gossiper.announce = announce  # type: ignore[method-assign]  # noqa: SLF001

    member = await lifecycle._publish_state(state)

    assert member.peer_address == "10.0.0.5:7001"
    assert applied[0].peer_address == "10.0.0.5:7001"
    assert announced[0].peer_address == "10.0.0.5:7001"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_local_member_reads_persisted_state(tmp_path: Path) -> None:
    config = TourillonConfig(
        node=NodeConfig(id="node-a"),
        storage=StorageConfig(data_dir=str(tmp_path), group_size="8Mi"),
        tls=TlsConfig(ca_data="", cert_data="", key_data=""),
        server_client=ClientServerConfig(bind="127.0.0.1:7000"),
        server_peer=PeerServerConfig(bind="0.0.0.0:7001", advertise="10.0.0.5:7001"),
    )
    lifecycle = _build_lifecycle(
        config=config,
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "idle", "epoch": 4}}
        ),
    )

    member = await lifecycle.local_member()

    assert member == Member("node-a", "10.0.0.5:7001", 4, MemberPhase.IDLE, ())


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_startup_joining_and_ready_override_seeds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    persistence = _DocumentStatePersistence(
        {
            "node": {
                "node_id": "node-a",
                "phase": "joining",
                "epoch": 1,
                "tokens": ["0x1"],
                "seeds": ["old:9000"],
            }
        }
    )
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=persistence,
    )
    state = NodeState(
        node_id="node-a",
        phase=MemberPhase.JOINING,
        epoch=1,
        tokens=("0x1",),
        seeds=("old:9000",),
    )
    calls: list[tuple[str, tuple[str, ...]]] = []

    async def start_peer() -> None:
        calls.append(("peer", ()))

    async def perform_join(join_state: NodeState) -> None:
        calls.append(("join", join_state.seeds))

    async def start_ready(ready_state: NodeState) -> None:
        calls.append(("ready", ready_state.seeds))

    lifecycle._start_peer = start_peer  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._perform_join = perform_join  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._start_ready = start_ready  # type: ignore[method-assign]  # noqa: SLF001

    await lifecycle._startup_joining(state, seeds=["new:9000"])
    assert calls == [("peer", ()), ("join", ("new:9000",))]

    calls.clear()
    ready_state = NodeState(
        node_id="node-a",
        phase=MemberPhase.READY,
        epoch=1,
        tokens=("0x1",),
        seeds=("old:9000",),
    )
    await lifecycle._startup_ready(ready_state, seeds=["other:9000"])
    assert calls == [("peer", ()), ("ready", ("other:9000",))]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_start_ready_publish_state_and_sync_success_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "ready", "epoch": 2}}
        ),
    )
    ready_state = NodeState(
        node_id="node-a",
        phase=MemberPhase.READY,
        epoch=2,
        tokens=("0x1",),
        seeds=("seed:9000",),
    )
    calls: list[str] = []

    async def peer_pool_start() -> None:
        calls.append("pool")

    async def sync_full(state: NodeState) -> bool:
        calls.append(f"sync:{','.join(state.seeds)}")
        return True

    async def gossiper_start() -> None:
        calls.append("gossiper")

    async def publish_state(state: NodeState):
        calls.append(f"publish:{state.phase.value}")
        return None

    async def start_client() -> None:
        calls.append("client")

    async def start_antientropy() -> None:
        calls.append("antientropy")

    lifecycle._peer_pool.start = peer_pool_start  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._sync_full = sync_full  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._gossiper.start = gossiper_start  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._publish_state = publish_state  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._start_client = start_client  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._antientropy.start = start_antientropy  # type: ignore[method-assign]  # noqa: SLF001

    await lifecycle._start_ready(ready_state)
    assert calls == [
        "pool",
        "sync:seed:9000",
        "gossiper",
        "publish:ready",
        "client",
        "antientropy",
    ]

    calls.clear()
    ready_no_seeds = NodeState(
        node_id="node-a",
        phase=MemberPhase.READY,
        epoch=2,
        tokens=("0x1",),
    )
    await lifecycle._start_ready(ready_no_seeds)
    assert calls == ["pool", "gossiper", "publish:ready", "client", "antientropy"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_publish_state_and_sync_full_success_increment_counters(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "joining", "epoch": 3}}
        ),
    )
    state = NodeState(
        node_id="node-a",
        phase=MemberPhase.JOINING,
        epoch=3,
        tokens=("0x1",),
        seeds=("seed:9000",),
    )
    applied = []
    announced = []

    async def apply_member(member) -> None:
        applied.append(member)

    async def announce(member) -> None:
        announced.append(member)

    class _Bootstrapper:
        async def run(self, seeds):
            assert seeds == ("seed:9000",)
            return 2

    lifecycle._topologymgr.apply_member = apply_member  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._gossiper.announce = announce  # type: ignore[method-assign]  # noqa: SLF001
    monkeypatch.setattr(
        "tourillon.core.services.lifecycle.GossipBootstrapper",
        lambda **kwargs: _Bootstrapper(),
    )

    member = await lifecycle._publish_state(state)
    assert member.node_id == "node-a"
    assert applied and announced

    assert await lifecycle._sync_full(state) is True
    assert lifecycle._gossip_stats.bootstrap_ok_total == 2


_JOINING = Member(
    node_id="node-a",
    peer_address="127.0.0.1:7001",
    epoch=1,
    phase=MemberPhase.JOINING,
    tokens=("0x80",),
)


class _JoinStubs:
    def __init__(
        self,
        events: list[str],
        outcomes: list[bool],
        synced: list[bool],
    ) -> None:
        self.events = events
        self.outcomes = outcomes
        self.synced = synced

    async def noop(self) -> None:
        return None

    async def publish_state(self, published: NodeState) -> Member:
        self.events.append(f"publish:{published.phase.value}")
        return _JOINING

    async def sync_full(self, _state: NodeState) -> bool:
        return self.synced.pop(0)

    async def sync_data(self) -> bool:
        return self.outcomes.pop(0)

    async def transition_to(self, _state: NodeState, phase: MemberPhase) -> NodeState:
        return NodeState(node_id="node-a", phase=phase, epoch=2, tokens=("0x80",))

    async def start_client(self) -> None:
        self.events.append("client")

    async def stop_antientropy(self) -> None:
        self.events.append("antientropy-stopped")

    async def stop_gossiper(self) -> None:
        self.events.append("gossiper-stopped")

    def apply(self, lifecycle: NodeLifecycle) -> None:
        lifecycle._peer_pool.start = self.noop  # type: ignore[method-assign]  # noqa: SLF001
        lifecycle._gossiper.start = self.noop  # type: ignore[method-assign]  # noqa: SLF001
        lifecycle._gossiper.stop = self.stop_gossiper  # type: ignore[method-assign]  # noqa: SLF001
        lifecycle._publish_state = self.publish_state  # type: ignore[method-assign]  # noqa: SLF001
        lifecycle._sync_full = self.sync_full  # type: ignore[method-assign]  # noqa: SLF001
        lifecycle._sync_data = self.sync_data  # type: ignore[method-assign]  # noqa: SLF001
        lifecycle._transition_to = self.transition_to  # type: ignore[method-assign]  # noqa: SLF001
        lifecycle._start_client = self.start_client  # type: ignore[method-assign]  # noqa: SLF001
        lifecycle._antientropy.stop = self.stop_antientropy  # type: ignore[method-assign]  # noqa: SLF001


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_perform_join_moves_to_ready_or_failed_from_the_sync_goal(
    tmp_path: Path,
) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "joining", "epoch": 1}}
        ),
    )
    state = NodeState(
        node_id="node-a", phase=MemberPhase.JOINING, epoch=1, tokens=("0x80",)
    )
    events: list[str] = []
    outcomes = [True, False]
    synced = [True, True, False]

    stubs = _JoinStubs(events, outcomes, synced)
    stubs.apply(lifecycle)

    await lifecycle._perform_join(state)  # noqa: SLF001
    # JOINING is announced again once the bootstrap revealed the peers.
    assert events == [
        "publish:joining",
        "publish:joining",
        "client",
        "publish:ready",
    ]

    events.clear()
    await lifecycle._perform_join(state)  # noqa: SLF001
    assert events == [
        "publish:joining",
        "publish:joining",
        "publish:failed",
        "antientropy-stopped",
        "gossiper-stopped",
    ]

    events.clear()
    # Membership sync failed: the data goal is never awaited.
    await lifecycle._perform_join(state)  # noqa: SLF001
    assert events == [
        "publish:joining",
        "publish:failed",
        "antientropy-stopped",
        "gossiper-stopped",
    ]
    assert outcomes == []


class _JoinAntiEntropy:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[str] = []

    async def start(self) -> None:
        self.calls.append("start")

    async def wait_joined(self) -> None:
        self.calls.append("wait_joined")
        if self.error is not None:
            raise self.error


@pytest.mark.tourillon
@pytest.mark.parametrize("error", [None, RuntimeError("source unavailable")])
async def test_sync_data_starts_antientropy_and_awaits_the_join_goal(
    tmp_path: Path, caplog, error: Exception | None
) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {"node": {"node_id": "node-a", "phase": "joining", "epoch": 1}}
        ),
    )
    antientropy = _JoinAntiEntropy(error)
    lifecycle._antientropy = antientropy  # type: ignore[assignment]  # noqa: SLF001

    assert await lifecycle._sync_data() is (error is None)  # noqa: SLF001
    assert antientropy.calls == ["start", "wait_joined"]
    assert ("source unavailable" in caplog.text) is (error is not None)


class _DrainAntiEntropy:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.pending = False
        self.waiting = asyncio.Event()
        self.satisfied = asyncio.Event()
        self.drain_error: RuntimeError | None = None

    async def start(self) -> None:
        self.started = True
        self.stopped = False

    async def stop(self) -> None:
        self.stopped = True

    async def drain(self) -> None:
        self.waiting.set()
        if self.drain_error is not None:
            raise self.drain_error
        if self.pending:
            await self.satisfied.wait()
            self.pending = False


class _DrainStore:
    def __init__(self) -> None:
        self.paused = False
        self.waiting = asyncio.Event()
        self.settled = asyncio.Event()
        self.repairs_stopped = False

    def pause(self, *, readable: bool = True) -> None:
        self.paused = True
        self.readable = readable

    async def drain(self) -> None:
        self.waiting.set()
        await self.settled.wait()

    async def stop_read_repairs(self) -> None:
        self.repairs_stopped = True


async def _draining_lifecycle(tmp_path: Path):
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence(
            {
                "node": {
                    "node_id": "node-a",
                    "phase": "ready",
                    "epoch": 2,
                    "tokens": ["0x80"],
                }
            }
        ),
    )
    for member in (
        Member(
            node_id="node-a",
            peer_address="127.0.0.1:7001",
            epoch=2,
            phase=MemberPhase.READY,
            tokens=("0x80",),
        ),
        Member(
            node_id="node-b",
            peer_address="127.0.0.1:7002",
            epoch=1,
            phase=MemberPhase.READY,
            tokens=("0x0",),
        ),
    ):
        await lifecycle._topologymgr.apply_member(member)
    store, antientropy = _DrainStore(), _DrainAntiEntropy()
    lifecycle._store = store  # type: ignore[assignment]
    lifecycle._antientropy = antientropy  # type: ignore[assignment]
    return lifecycle, store, antientropy


@pytest.mark.tourillon
async def test_drain_waits_repairs_then_the_antientropy_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle, store, antientropy = await _draining_lifecycle(tmp_path)
    antientropy.pending = True
    client_stopped = asyncio.Event()

    async def stop_client() -> None:
        if not client_stopped.is_set():
            assert (await lifecycle._load_state()).phase == MemberPhase.DRAINING
        client_stopped.set()

    monkeypatch.setattr(lifecycle, "_stop_client", stop_client)
    await lifecycle.drain()
    try:
        await asyncio.wait_for(store.waiting.wait(), 1)
        assert store.paused
        assert store.readable
        assert not client_stopped.is_set()
        assert not antientropy.waiting.is_set()
        assert not store.repairs_stopped
        with pytest.raises(DrainError, match="already in progress"):
            await lifecycle.drain()
        with pytest.raises(JoinError, match="Drain operation"):
            await lifecycle.join(["seed:9000"])
        store.settled.set()
        await asyncio.wait_for(antientropy.waiting.wait(), 1)
        assert antientropy.started
        assert (await lifecycle._load_state()).phase == MemberPhase.DRAINING
        assert not lifecycle._drain_task.done()
        assert not store.repairs_stopped
        assert not client_stopped.is_set()
        assert store.readable
        antientropy.satisfied.set()
        await asyncio.wait_for(lifecycle._drain_task, 1)
        state = await lifecycle._load_state()
        assert state.phase == MemberPhase.IDLE
        assert state.epoch == 4
        assert state.seeds == ("127.0.0.1:7002",)
        topology = await lifecycle._topologymgr.snapshot()
        assert {vnode.node_id for vnode in topology.ring} == {"node-b"}
        assert antientropy.stopped
        assert not antientropy.pending
        assert store.repairs_stopped
        assert client_stopped.is_set()
        assert not store.readable
    finally:
        await lifecycle.stop()


@pytest.mark.tourillon
async def test_failed_handoff_stays_draining_and_can_be_retried(
    tmp_path: Path, caplog
) -> None:
    lifecycle, store, antientropy = await _draining_lifecycle(tmp_path)
    store.settled.set()
    antientropy.drain_error = RuntimeError("handoff interrupted")
    await lifecycle.drain()
    await asyncio.wait_for(lifecycle._drain_task, 1)
    assert (await lifecycle._load_state()).phase == MemberPhase.DRAINING
    assert "Drain failed" in caplog.text
    assert "handoff interrupted" in caplog.text
    assert not antientropy.stopped
    assert not store.repairs_stopped
    assert store.readable
    antientropy.drain_error = None
    await lifecycle.drain()
    await asyncio.wait_for(lifecycle._drain_task, 1)
    assert (await lifecycle._load_state()).phase == MemberPhase.IDLE
    await lifecycle.stop()


@pytest.mark.tourillon
async def test_antientropy_start_failure_never_publishes_idle(
    tmp_path: Path, caplog, monkeypatch: pytest.MonkeyPatch
) -> None:
    lifecycle, store, antientropy = await _draining_lifecycle(tmp_path)
    store.settled.set()

    async def fail_start() -> None:
        raise RuntimeError("storage unavailable")

    monkeypatch.setattr(antientropy, "start", fail_start)
    try:
        await lifecycle.drain()
        await asyncio.wait_for(lifecycle._drain_task, 1)
        assert (await lifecycle._load_state()).phase is MemberPhase.DRAINING
        assert "storage unavailable" in caplog.text
        assert not antientropy.waiting.is_set()
        assert store.readable
    finally:
        await lifecycle.stop()


@pytest.mark.tourillon
async def test_shutdown_cancels_waiting_drain_without_publishing_idle(
    tmp_path: Path,
) -> None:
    lifecycle, store, antientropy = await _draining_lifecycle(tmp_path)
    store.settled.set()
    antientropy.pending = True
    try:
        await lifecycle.drain()
        await asyncio.wait_for(antientropy.waiting.wait(), 1)
        drain_task = lifecycle._drain_task
        assert not drain_task.done()
    finally:
        await lifecycle.stop()
    assert drain_task.cancelled()
    assert antientropy.stopped
    assert antientropy.pending
    assert (await lifecycle._load_state()).phase is MemberPhase.DRAINING


@pytest.mark.tourillon
@pytest.mark.parametrize(
    "phase", [MemberPhase.IDLE, MemberPhase.JOINING, MemberPhase.FAILED]
)
async def test_drain_rejects_ineligible_phase_without_transition(
    tmp_path: Path, phase: MemberPhase
) -> None:
    lifecycle = _build_lifecycle(
        _build_config(tmp_path),
        _DocumentStatePersistence(
            {"node": {"node_id": "test-node", "phase": phase.value}}
        ),
    )
    with pytest.raises(DrainError, match="Cannot drain from phase"):
        await lifecycle.drain()
    assert (await lifecycle._load_state()).phase == phase


@pytest.mark.tourillon
async def test_last_node_cannot_silently_discard_its_ranges(tmp_path: Path) -> None:
    lifecycle = _build_lifecycle(
        _build_config(tmp_path),
        _DocumentStatePersistence({"node": {"node_id": "test-node", "phase": "ready"}}),
    )
    with pytest.raises(DrainError, match="another node"):
        await lifecycle.drain()
    assert (await lifecycle._load_state()).phase == MemberPhase.READY


@pytest.mark.tourillon
async def test_pause_stops_admission_without_draining_and_publishes_paused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lifecycle = _build_lifecycle(
        _build_config(tmp_path, node_id="node-a"),
        _DocumentStatePersistence(
            {
                "node": {
                    "node_id": "node-a",
                    "phase": "ready",
                    "epoch": 2,
                    "tokens": ["0x80"],
                }
            }
        ),
    )
    await lifecycle._topologymgr.apply_member(
        Member(
            node_id="node-b",
            peer_address="127.0.0.1:7002",
            epoch=1,
            phase=MemberPhase.READY,
            tokens=("0x0",),
        )
    )

    async def drain() -> None:
        raise AssertionError("pause must not wait for outstanding work")

    monkeypatch.setattr(lifecycle._store, "drain", drain)
    await lifecycle.pause()

    state = await lifecycle._load_state()
    topology = await lifecycle._topologymgr.snapshot()
    assert state.phase == MemberPhase.PAUSED
    assert state.epoch == 3
    assert topology.registry.get("node-a").phase == MemberPhase.PAUSED
    assert not lifecycle._store._accepting_reads
    assert not lifecycle._store._accepting_read_repairs


@pytest.mark.tourillon
async def test_pause_rejects_when_no_other_ready_peer_is_available(
    tmp_path: Path,
) -> None:
    lifecycle = _build_lifecycle(
        _build_config(tmp_path),
        _DocumentStatePersistence({"node": {"node_id": "test-node", "phase": "ready"}}),
    )

    with pytest.raises(PauseError, match="no other reachable READY node"):
        await lifecycle.pause()

    assert (await lifecycle._load_state()).phase == MemberPhase.READY
    assert lifecycle._store._accepting_reads


@pytest.mark.tourillon
async def test_resume_publishes_ready_only_after_catchup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lifecycle = _build_lifecycle(
        _build_config(tmp_path, node_id="node-a"),
        _DocumentStatePersistence(
            {
                "node": {
                    "node_id": "node-a",
                    "phase": "paused",
                    "epoch": 3,
                    "tokens": ["0x80"],
                }
            }
        ),
    )
    await lifecycle._topologymgr.apply_member(
        Member(
            node_id="node-b",
            peer_address="127.0.0.1:7002",
            epoch=1,
            phase=MemberPhase.READY,
            tokens=("0x0",),
        )
    )
    catchup = asyncio.Event()

    async def wait_joined() -> None:
        catchup.set()

    async def start_client() -> None:
        assert (await lifecycle._load_state()).phase == MemberPhase.PAUSED

    monkeypatch.setattr(lifecycle._antientropy, "wait_joined", wait_joined)
    monkeypatch.setattr(lifecycle, "_start_client", start_client)

    await lifecycle.resume()

    assert (await lifecycle._load_state()).phase != MemberPhase.READY
    await asyncio.wait_for(lifecycle._resume_task, 1)
    assert catchup.is_set()
    assert (await lifecycle._load_state()).phase == MemberPhase.READY
    assert lifecycle._store._accepting_reads


@pytest.mark.tourillon
async def test_resume_failure_keeps_node_paused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lifecycle = _build_lifecycle(
        _build_config(tmp_path, node_id="node-a"),
        _DocumentStatePersistence(
            {
                "node": {
                    "node_id": "node-a",
                    "phase": "paused",
                    "epoch": 3,
                    "tokens": ["0x80"],
                }
            }
        ),
    )
    await lifecycle._topologymgr.apply_member(
        Member(
            node_id="node-b",
            peer_address="127.0.0.1:7002",
            epoch=1,
            phase=MemberPhase.READY,
            tokens=("0x0",),
        )
    )

    async def fail_catchup() -> None:
        raise RuntimeError("source unavailable")

    monkeypatch.setattr(lifecycle._antientropy, "wait_joined", fail_catchup)

    await lifecycle.resume()
    await asyncio.wait_for(lifecycle._resume_task, 1)

    assert (await lifecycle._load_state()).phase == MemberPhase.PAUSED
    assert not lifecycle._store._accepting_reads


@pytest.mark.tourillon
async def test_resume_rejects_without_a_reachable_data_source(tmp_path: Path) -> None:
    lifecycle = _build_lifecycle(
        _build_config(tmp_path),
        _DocumentStatePersistence(
            {"node": {"node_id": "test-node", "phase": "paused"}}
        ),
    )

    with pytest.raises(ResumeError, match="no other reachable READY node"):
        await lifecycle.resume()

    assert (await lifecycle._load_state()).phase == MemberPhase.PAUSED


@pytest.mark.tourillon
async def test_stop_during_drain_preserves_draining_for_restart(tmp_path: Path) -> None:
    lifecycle, store, _ = await _draining_lifecycle(tmp_path)
    await lifecycle.drain()
    await asyncio.wait_for(store.waiting.wait(), 1)
    await lifecycle.stop()
    assert not lifecycle.is_draining()
    assert (await lifecycle._load_state()).phase == MemberPhase.DRAINING


@pytest.mark.tourillon
async def test_drained_idle_restart_does_not_bootstrap_ready(
    tmp_path: Path, monkeypatch
) -> None:
    lifecycle, _, antientropy = await _draining_lifecycle(tmp_path)
    state = await lifecycle._load_state()
    state = await lifecycle._transition_to(
        state, MemberPhase.DRAINING, with_seeds=["127.0.0.1:7002"]
    )
    state = await lifecycle._transition_to(state, MemberPhase.IDLE)

    async def noop() -> None:
        pass

    monkeypatch.setattr(lifecycle, "_start_peer", noop)
    monkeypatch.setattr(lifecycle._peer_pool, "start", noop)
    monkeypatch.setattr(lifecycle._gossiper, "start", noop)
    await lifecycle.start()
    assert (await lifecycle._load_state()).phase == MemberPhase.IDLE
    assert not lifecycle.is_draining()
    assert not antientropy.started
    assert not lifecycle._client_started


@pytest.mark.tourillon
async def test_idle_with_persisted_seeds_joins_only_on_explicit_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lifecycle = _build_lifecycle(
        _build_config(tmp_path, node_id="node-a"),
        _DocumentStatePersistence(
            {
                "node": {
                    "node_id": "node-a",
                    "phase": "idle",
                    "epoch": 4,
                    "seeds": ["127.0.0.1:7002"],
                }
            }
        ),
    )
    joined: list[NodeState] = []

    async def start_peer() -> None:
        return None

    async def perform_join(state: NodeState) -> None:
        joined.append(state)

    monkeypatch.setattr(lifecycle, "_start_peer", start_peer)
    monkeypatch.setattr(lifecycle, "_perform_join", perform_join)
    await lifecycle.start()
    assert (await lifecycle._load_state()).phase == MemberPhase.IDLE
    assert not joined
    await lifecycle.join()
    await asyncio.wait_for(lifecycle._join_task, 1)
    assert joined[0].phase == MemberPhase.JOINING
    assert joined[0].seeds == ("127.0.0.1:7002",)


@pytest.mark.tourillon
async def test_restart_resumes_previously_requested_drain(
    tmp_path: Path, monkeypatch
) -> None:
    lifecycle, store, antientropy = await _draining_lifecycle(tmp_path)
    state = await lifecycle._load_state()
    await lifecycle._transition_to(state, MemberPhase.DRAINING)

    async def noop() -> None:
        pass

    monkeypatch.setattr(lifecycle, "_start_peer", noop)
    monkeypatch.setattr(lifecycle, "_start_client", noop)
    monkeypatch.setattr(lifecycle._peer_pool, "start", noop)
    monkeypatch.setattr(lifecycle._gossiper, "start", noop)
    store.settled.set()
    await lifecycle.start()
    await asyncio.wait_for(lifecycle._drain_task, 1)
    assert antientropy.started
    assert antientropy.stopped
    assert (await lifecycle._load_state()).phase == MemberPhase.IDLE
    await lifecycle.stop()


@pytest.mark.tourillon
async def test_stop_without_drain_never_enters_draining(tmp_path: Path) -> None:
    lifecycle, _, _ = await _draining_lifecycle(tmp_path)
    await lifecycle.stop()
    assert (await lifecycle._load_state()).phase == MemberPhase.READY


@pytest.mark.tourillon
def test_drain_seeds_stops_after_five_eligible_peers_in_large_cluster() -> None:
    visited = []

    def members():
        for index in range(10_000):
            visited.append(index)
            yield Member(
                node_id=f"node-{index}",
                peer_address=f"127.0.0.1:{10000 + index}",
                epoch=1,
                phase=MemberPhase.READY,
                tokens=(),
            )

    state = NodeState(node_id="node-0")
    assert NodeLifecycle._drain_seeds(state, members()) == [
        f"127.0.0.1:{10000 + index}" for index in range(1, 6)
    ]
    assert visited == list(range(6))


@pytest.mark.tourillon
def test_drain_seeds_skips_ineligible_peers_and_bounds_fallback() -> None:
    state = NodeState(
        node_id="node-a", seeds=tuple(f"seed-{i}:9000" for i in range(10))
    )
    members = [
        Member(
            node_id=f"node-{phase.value}",
            peer_address=f"{phase.value}:9000",
            epoch=1,
            phase=phase,
            tokens=(),
        )
        for phase in MemberPhase
    ]
    assert NodeLifecycle._drain_seeds(state, members) == [
        member.peer_address
        for member in members
        if member.phase in (MemberPhase.READY, MemberPhase.PAUSED)
    ]
    assert NodeLifecycle._drain_seeds(state, []) == list(state.seeds[:5])
    assert NodeLifecycle._drain_seeds(NodeState(node_id="node-a"), []) == []


_REQUEST_SCOPE: contextvars.ContextVar[str] = contextvars.ContextVar("request_scope")


@pytest.mark.tourillon
async def test_background_task_does_not_inherit_the_request_context() -> None:
    seen: list[str] = []

    async def operation() -> None:
        seen.append(_REQUEST_SCOPE.get("detached"))

    _REQUEST_SCOPE.set("node.join request")
    await _background_task(operation(), name="node-join")
    inherited = asyncio.create_task(operation())
    await inherited

    assert seen == ["detached", "node.join request"]
