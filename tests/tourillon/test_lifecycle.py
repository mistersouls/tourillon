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
import ssl
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

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
from tourillon.core.exceptions import BootstrapError
from tourillon.core.gossip.gossiper import GossipStats
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.member import Member
from tourillon.core.models.member import MemberPhase
from tourillon.core.models.state import NodeState
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.peer.pool import PeerPool
from tourillon.core.probe.detector import FailureDetectorProvider
from tourillon.core.services.lifecycle import NodeLifecycle
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.spaces.ring import Ring
from tourillon.core.spaces.vnode import VNode
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


def _build_lifecycle(config: TourillonConfig, persistence: Persistence) -> NodeLifecycle:
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
        idle_ttl=pool_opts.idle_ttl,
    )
    return NodeLifecycle(
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
        storage=_StorageStub(),  # type: ignore[arg-type]
        gossip_stats=GossipStats(),
        telemetry=build_noop_telemetry(),
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_load_state_defaults_to_config_node_id_when_missing(tmp_path: Path) -> None:
    persistence = _MissingStatePersistence()
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=persistence,
    )

    state = await lifecycle._load_or_create_state()

    assert state.node_id == "node-a"
    assert state.phase == MemberPhase.IDLE
    assert state.prev_phase is None
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
    assert state.prev_phase == MemberPhase.JOINING
    assert state.epoch == 4


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_lifecycle_configures_peer_pool_from_transport_pool_options(tmp_path: Path) -> None:
    config = TourillonConfig(
        node=NodeConfig(id="node-pool"),
        storage=StorageConfig(data_dir=str(tmp_path), group_size="8Mi"),
        tls=TlsConfig(ca_data="", cert_data="", key_data=""),
        server_client=ClientServerConfig(bind="127.0.0.1:7000"),
        server_peer=PeerServerConfig(bind="127.0.0.1:7001", advertise="127.0.0.1:7001"),
        transport=TransportConfig(
            pool=TransportPoolConfig(min_size=2, max_size=7, max_overflow=3, idle_ttl=9.5)
        ),
    )
    lifecycle = _build_lifecycle(config=config, persistence=_MissingStatePersistence())

    assert lifecycle._peer_pool._min_size == 2
    assert lifecycle._peer_pool._max_size == 7
    assert lifecycle._peer_pool._max_overflow == 3
    assert lifecycle._peer_pool._idle_ttl == 9.5


@pytest.mark.tourillon
def test_log_state_tokens_renders_epoch_and_one_line_per_token(
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
        tokens=("0x0000000000000001", "0x000000001234abcd", "0x0000000123456789"),
    )

    caplog.set_level(logging.INFO, logger="tourillon.global")
    lifecycle._log_tokens(state)

    messages = [record.getMessage() for record in caplog.records]
    assert "Node state epoch=7 tokens (3):" in messages
    assert "  0x0000000000000001 (1)" in messages
    assert "  0x000000001234abcd (2)" in messages
    assert "  0x0000000123456789 (3)" in messages


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

    caplog.set_level(logging.INFO, logger="tourillon.global")
    lifecycle._log_tokens(state)

    messages = [record.getMessage() for record in caplog.records]
    assert "Node state epoch=0 tokens (0):" in messages
    assert "  (none)" in messages


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_join_raises_when_no_seeds_configured(tmp_path: Path) -> None:
    from tourillon.core.exceptions import JoinError

    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence({"node": {"node_id": "node-a", "phase": "idle", "epoch": 0}}),
    )

    with pytest.raises(JoinError, match="No seed peers provided"):
        await lifecycle.join()


@pytest.mark.tourillon
def test_is_joining_returns_false_when_no_task(tmp_path: Path) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence({"node": {"node_id": "node-a", "phase": "idle", "epoch": 0}}),
    )

    assert lifecycle.is_joining() is False


@pytest.mark.tourillon
def test_generate_tokens_returns_hex_strings(tmp_path: Path) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence({"node": {"node_id": "node-a", "phase": "idle", "epoch": 0}}),
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
async def test_sync_full_returns_false_when_bootstrapper_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path),
        persistence=_MissingStatePersistence(),
    )
    state = NodeState(node_id="node-a", phase=MemberPhase.JOINING, epoch=1, seeds=("seed1:9001",))

    class _Bootstrapper:
        async def run(self, seeds):
            _ = seeds
            raise BootstrapError("boom")

    monkeypatch.setattr("tourillon.core.services.lifecycle.GossipBootstrapper", lambda **kwargs: _Bootstrapper())

    assert await lifecycle._sync_full(state) is False
    assert lifecycle._gossip_stats.bootstrap_err_total == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_transition_to_persists_phase_tokens_and_seeds(tmp_path: Path) -> None:
    persistence = _DocumentStatePersistence(
        {"node": {"node_id": "node-a", "phase": "idle", "epoch": 0}}
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
    assert transitioned.prev_phase == MemberPhase.IDLE
    assert transitioned.epoch == 1
    assert transitioned.tokens
    assert transitioned.seeds == ("seed:9000",)


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_start_dispatches_to_phase_specific_startup(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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

    async def stop_hint_replayer() -> None:
        seen.append("hint")

    async def stop_gossiper() -> None:
        seen.append("gossiper")

    async def stop_peer() -> None:
        seen.append("peer")

    lifecycle._stop_client = stop_client  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._hint_replayer.stop = stop_hint_replayer  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._gossiper.stop = stop_gossiper  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._stop_peer = stop_peer  # type: ignore[method-assign]  # noqa: SLF001

    await lifecycle.stop()

    assert seen == ["client", "hint", "cancelled", "gossiper", "peer"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_startup_idle_branches_on_seed_configuration(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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
        persistence=_DocumentStatePersistence({"node": {"node_id": "node-a", "phase": "ready", "epoch": 2}}),
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
async def test_startup_joining_and_ready_override_seeds(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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
async def test_start_ready_publish_state_and_sync_success_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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

    async def start_hint_replayer() -> None:
        calls.append("hint")

    lifecycle._peer_pool.start = peer_pool_start  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._sync_full = sync_full  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._gossiper.start = gossiper_start  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._publish_state = publish_state  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._start_client = start_client  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._hint_replayer.start = start_hint_replayer  # type: ignore[method-assign]  # noqa: SLF001

    await lifecycle._start_ready(ready_state)
    assert calls == ["pool", "sync:seed:9000", "gossiper", "publish:ready", "client", "hint"]

    calls.clear()
    ready_no_seeds = NodeState(
        node_id="node-a",
        phase=MemberPhase.READY,
        epoch=2,
        tokens=("0x1",),
    )
    await lifecycle._start_ready(ready_no_seeds)
    assert calls == ["pool", "gossiper", "publish:ready", "client", "hint"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_publish_state_and_sync_full_success_increment_counters(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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


class _TransferRunnerStub:
    def __init__(self) -> None:
        loop = asyncio.get_running_loop()
        self.run_result: asyncio.Future[tuple[list[str], list[str]]] = loop.create_future()
        self.wait_result: asyncio.Future[tuple[list[str], list[str]]] = loop.create_future()
        self.calls: list[tuple[str, ...]] = []
        self.cancelled = False

    async def run(self, current: Ring, target: Ring) -> tuple[list[str], list[str]]:
        self.calls.append(("run", current.serial(), target.serial()))
        return await self.run_result

    async def apply(self, current: Ring, target: Ring) -> None:
        self.calls.append(("apply", current.serial(), target.serial()))

    async def wait(self) -> tuple[list[str], list[str]]:
        self.calls.append(("wait",))
        return await self.wait_result

    async def cancel(self) -> None:
        self.cancelled = True


class _RingTopologyStub:
    def __init__(self, ring: Ring) -> None:
        self._ring = ring
        self.changes: asyncio.Queue[Ring] = asyncio.Queue()

    async def snapshot(self) -> SimpleNamespace:
        return SimpleNamespace(ring=self._ring)

    async def wait_for_ring_change(self, serial: str) -> Ring:
        _ = serial
        return await self.changes.get()


def _joining_lifecycle(tmp_path: Path, ring: Ring) -> tuple[NodeLifecycle, _TransferRunnerStub, _RingTopologyStub]:
    lifecycle = _build_lifecycle(
        config=_build_config(tmp_path, node_id="node-a"),
        persistence=_DocumentStatePersistence({"node": {"node_id": "node-a", "phase": "joining", "epoch": 1}}),
    )
    runner, topology = _TransferRunnerStub(), _RingTopologyStub(ring)
    lifecycle._transfer_runner = runner  # type: ignore[assignment]  # noqa: SLF001
    lifecycle._topologymgr = topology  # type: ignore[assignment]  # noqa: SLF001
    return lifecycle, runner, topology


_JOINING = Member(
    node_id="node-a",
    peer_address="127.0.0.1:7001",
    epoch=1,
    phase=MemberPhase.JOINING,
    tokens=("0x80",),
)


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_run_transfers_returns_the_results_once_transfers_settle(tmp_path: Path) -> None:
    ring = Ring([VNode(token=0, node_id="node-b")])
    lifecycle, runner, _ = _joining_lifecycle(tmp_path, ring)
    runner.run_result.set_result((["r1"], []))

    assert await lifecycle._run_transfers(_JOINING) == (["r1"], [])  # noqa: SLF001
    assert runner.calls == [("run", ring.serial(), ring.add_vnodes(_JOINING.vnodes).serial())]
    assert runner.cancelled is False


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_run_transfers_replans_on_ring_change_and_awaits_the_new_plan(tmp_path: Path) -> None:
    ring = Ring([VNode(token=0, node_id="node-b")])
    changed = ring.add_vnodes([VNode(token=64, node_id="node-c")])
    lifecycle, runner, topology = _joining_lifecycle(tmp_path, ring)
    # The first plan settles while the ring changes: its results are stale.
    runner.run_result.set_result((["stale"], []))
    runner.wait_result.set_result((["r1", "r2"], ["r3"]))
    topology.changes.put_nowait(changed)

    assert await lifecycle._run_transfers(_JOINING) == (["r1", "r2"], ["r3"])  # noqa: SLF001
    assert runner.calls[1:] == [
        ("apply", changed.serial(), changed.add_vnodes(_JOINING.vnodes).serial()),
        ("wait",),
    ]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_run_transfers_cancels_the_transfers_when_the_join_is_cancelled(tmp_path: Path) -> None:
    lifecycle, runner, _ = _joining_lifecycle(tmp_path, Ring([VNode(token=0, node_id="node-b")]))

    task = asyncio.create_task(lifecycle._run_transfers(_JOINING))  # noqa: SLF001
    for _ in range(5):
        await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert runner.cancelled is True


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_perform_join_moves_to_ready_or_failed_from_the_transfer_results(tmp_path: Path) -> None:
    lifecycle, _, _ = _joining_lifecycle(tmp_path, Ring([VNode(token=0, node_id="node-b")]))
    state = NodeState(node_id="node-a", phase=MemberPhase.JOINING, epoch=1, tokens=("0x80",))
    events: list[str] = []
    outcomes = [(["r1"], []), (["r1"], ["r2"])]
    synced = [True, True, False]

    async def noop() -> None:
        return None

    async def publish_state(published: NodeState) -> Member:
        events.append(f"publish:{published.phase.value}")
        return _JOINING

    async def sync_full(_state: NodeState) -> bool:
        return synced.pop(0)

    async def run_transfers(_member: Member) -> tuple[list[str], list[str]]:
        return outcomes.pop(0)

    async def transition_to(_state: NodeState, phase: MemberPhase) -> NodeState:
        return NodeState(node_id="node-a", phase=phase, epoch=2, tokens=("0x80",))

    async def start_client() -> None:
        events.append("client")

    async def start_hint_replayer() -> None:
        events.append("hint")

    async def stop_gossiper() -> None:
        events.append("gossiper-stopped")

    lifecycle._peer_pool.start = noop  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._gossiper.start = noop  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._gossiper.stop = stop_gossiper  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._publish_state = publish_state  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._sync_full = sync_full  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._run_transfers = run_transfers  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._transition_to = transition_to  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._start_client = start_client  # type: ignore[method-assign]  # noqa: SLF001
    lifecycle._hint_replayer.start = start_hint_replayer  # type: ignore[method-assign]  # noqa: SLF001

    await lifecycle._perform_join(state)  # noqa: SLF001
    assert events == ["publish:joining", "client", "publish:ready", "hint"]

    events.clear()
    await lifecycle._perform_join(state)  # noqa: SLF001
    assert events == ["publish:joining", "publish:failed", "gossiper-stopped"]

    events.clear()
    await lifecycle._perform_join(state)  # membership sync failed: no transfer at all  # noqa: SLF001
    assert events == ["publish:joining", "publish:failed", "gossiper-stopped"]
    assert outcomes == []


