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
import secrets
import ssl
from typing import Any

from tourillon.core.exceptions import (
    BootstrapError,
    DrainError,
    JoinError,
    NodeIdMismatchError,
)
from tourillon.core.gossip.bootstrapper import (
    BootstrapError as GossipBootstrapError,
)
from tourillon.core.gossip.bootstrapper import (
    GossipBootstrapper,
)
from tourillon.core.gossip.engine import GossipEngine
from tourillon.core.helpers.utils import split_host_port
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.machinery.config import NodeSize
from tourillon.core.machinery.state import StatePersistence
from tourillon.core.ports.storage import Storage
from tourillon.core.rebalance.rebalancer import Rebalancer
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.ring.vnode import VNode
from tourillon.core.structure.config import TourillonConfig
from tourillon.core.structure.member import Member, MemberPhase, NodeState
from tourillon.core.transport.client import PeerClientPool
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.server import TcpServer
from tourlib.exceptions import TlsValidationError
from tourlib.ports.serializer import Serializer
from tourlib.ports.tls import TlsContext

logger = logging.getLogger(__name__)


class NodeLifecycle:
    def __init__(
        self,
        cfg: TourillonConfig,
        peer_dispatcher: Dispatcher,
        kv_dispatcher: Dispatcher,
        tls_ctx: TlsContext,
        state: StatePersistence,
        serializer: Serializer,
        topology: TopologyManager,
        partitioner: Partitioner,
        storage: Storage,
    ) -> None:
        self._cfg = cfg
        self._tls_ctx = tls_ctx
        self._state = state
        self._topology = topology
        self._serializer = serializer
        self._partitioner = partitioner
        self._probe = ProbeManager()

        ssl_peer, ssl_kv = self._build_ssl(cfg)
        self._ssl_client = tls_ctx.build_client_ssl_context(
            cfg.tls.cert_data,
            cfg.tls.key_data,
            cfg.tls.ca_data,
        )
        self._peer_server = TcpServer(
            peer_dispatcher, ssl_context=ssl_peer, name="Peer"
        )
        self._kv_server = TcpServer(kv_dispatcher, ssl_context=ssl_kv, name="KV")
        self._peer_pool = PeerClientPool(
            ssl_ctx=self._ssl_client,
            connect_timeout=cfg.gossip.bootstrap.connect_timeout,
        )
        self._rebalancer = Rebalancer(
            partitioner=partitioner,
            node_id=cfg.node_id,
            replication_factor=cfg.replication_factor,
            storage=storage,
            peer_pool=self._peer_pool,
            serializer=serializer,
            topology_mgr=topology,
        )
        self._gossip = GossipEngine(
            node_id=cfg.node_id,
            topology_manager=topology,
            pool=self._peer_pool,
            config=cfg.gossip,
            partition_shift=cfg.partition_shift,
            serializer=serializer,
            on_failed=self._on_gossip_failed,
        )
        self._gossip_task: asyncio.Task[None] | None = None
        self._join_task: asyncio.Task[bool] | None = None
        self._drain_task: asyncio.Task[bool] | None = None
        self._peer_started = False
        self._kv_started = False

    async def start(
        self,
        stop_event: asyncio.Event | None = None,
        seeds: list[str] | None = None,
    ) -> None:
        state = await self._load_state()
        state = await self._restart_state(state)
        seeds = seeds or self._cfg.seeds or []
        ev = stop_event or asyncio.Event()
        logger.info(
            "Node %s starting (phase=%s, generation=%d, seeds=%d).",
            self._cfg.node_id,
            state.phase.value,
            state.generation,
            len(seeds),
        )

        if state.phase in (MemberPhase.FAILED, MemberPhase.PAUSED):
            await self._peer_only(ev, state.phase)
            return

        if seeds:
            await self._start_seeded(ev, seeds, state)
        else:
            await self._start_seedless(ev, state)

    async def join(self, seeds_override: list[str] | None = None) -> dict[str, Any]:
        """Trigger a seeded join for a node already running in IDLE phase (RPC path)."""
        current = await self._load_state()
        if current.phase is not MemberPhase.IDLE:
            raise JoinError(
                code="bad_phase",
                message=(
                    f"Node {self._cfg.node_id} can only join from IDLE phase, "
                    f"current={current.phase.value}"
                ),
            )
        seeds: list[str] = seeds_override or self._cfg.seeds or []
        if not seeds:
            raise JoinError(
                code="empty_seeds", message="seeds are required for joining"
            )

        joining = await self._set_phase(current, MemberPhase.JOINING)

        await self._resync(seeds)
        await self._start_gossip()
        await self._announce(joining)

        loop = asyncio.get_running_loop()
        self._join_task = loop.create_task(self._do_join(joining), name="join-to-ready")
        self._join_task.add_done_callback(self._on_join_done)   # type: ignore[arg-type]
        logger.info(
            "Node %s entered JOINING (%d seed(s)).", self._cfg.node_id, len(seeds)
        )
        return {
            "node_id": self._cfg.node_id,
            "phase": MemberPhase.JOINING,
            "changed": True,
        }

    async def drain(self, seeds_override: list[str] | None = None) -> dict[str, Any]:
        """Trigger a drain (RPC path).

        Transition to DRAINING and run the rebalance in background.
        """
        current = await self._load_state()
        if current.phase is not MemberPhase.READY:
            raise DrainError(
                code="bad_phase",
                message=(
                    f"Node {self._cfg.node_id} can only drain from READY phase, "
                    f"current={current.phase.value}"
                ),
            )

        seeds: list[str] = seeds_override or self._cfg.seeds or []
        if not seeds:
            raise DrainError(code="empty_seeds", message="seeds are required for draining")

        draining = await self._set_phase(current, MemberPhase.DRAINING)

        await self._announce(draining)

        loop = asyncio.get_running_loop()
        self._drain_task = loop.create_task(self._do_drain(draining), name="drain-to-idle")
        self._drain_task.add_done_callback(self._on_drain_done)   # type: ignore[arg-type]
        logger.info("Node %s entered DRAINING.", self._cfg.node_id)
        return {"node_id": self._cfg.node_id, "phase": MemberPhase.DRAINING, "changed": True}

    async def start_peer(self) -> None:
        if self._peer_started:
            return
        host, port = split_host_port(self._cfg.peer_server.bind)
        try:
            await self._peer_server.start(host, port)
        except OSError as exc:
            raise BootstrapError(
                f"cannot bind peer listener on {self._cfg.peer_server.bind}: {exc}",
                exit_code=1,
            ) from exc
        self._peer_started = True
        logger.info("Peer listener: %s:%d", host, port)

    async def start_kv(self) -> None:
        if self._kv_started:
            return
        host, port = split_host_port(self._cfg.kv_server.bind)
        try:
            await self._kv_server.start(host, port)
        except OSError as exc:
            raise BootstrapError(
                f"cannot bind KV listener on {self._cfg.kv_server.bind}: {exc}",
                exit_code=1,
            ) from exc
        self._kv_started = True
        logger.info("KV  listener: %s:%d", host, port)

    async def stop_kv(self) -> None:
        if not self._kv_started:
            return
        await self._kv_server.stop()
        self._kv_started = False

    async def stop_peer(self) -> None:
        if not self._peer_started:
            return
        await self._peer_server.stop()
        self._peer_started = False

    async def stop_all(self) -> None:
        if self._join_task and not self._join_task.done():
            self._join_task.cancel()
        if self._drain_task and not self._drain_task.done():
            self._drain_task.cancel()
        if self._gossip_task is not None:
            await self._gossip.stop()
            await asyncio.gather(self._gossip_task, return_exceptions=True)
            self._gossip_task = None
        await self._peer_pool.close_all()
        await self.stop_kv()
        await self.stop_peer()

    @staticmethod
    def check_node_id_consistency(config_node_id: str, state_node_id: str) -> None:
        if config_node_id != state_node_id:
            raise NodeIdMismatchError(
                f"node_id mismatch: config={config_node_id} state={state_node_id}"
            )

    @staticmethod
    def check_tokens_coherence(
        phase: MemberPhase,
        tokens: tuple[int, ...],
        node_size: NodeSize,
    ) -> bool:
        if phase not in (MemberPhase.JOINING, MemberPhase.READY, MemberPhase.DRAINING):
            return True
        return len(tokens) == node_size.token_count

    async def _start_seedless(self, ev: asyncio.Event, state: NodeState) -> None:
        match state.phase:
            case MemberPhase.IDLE:
                await self._seedless_idle(ev, state)
            case MemberPhase.READY:
                await self._seedless_ready(ev, state)
            case _:
                raise BootstrapError(
                    f"cannot start from phase {state.phase.value} without seeds.",
                    exit_code=1,
                )

    async def _start_seeded(
        self, ev: asyncio.Event, seeds: list[str], state: NodeState
    ) -> None:
        match state.phase:
            case MemberPhase.IDLE:
                await self._seeded_idle(ev)
            case MemberPhase.JOINING:
                await self._seeded_joining(ev, seeds, state)
            case MemberPhase.READY:
                await self._seeded_ready(ev, seeds, state)
            case MemberPhase.DRAINING:
                await self._seeded_draining(ev, seeds, state)
            case _:
                raise BootstrapError(
                    f"cannot start from phase {state.phase.value} with seeds.",
                    exit_code=1,
                )

    async def _seedless_idle(self, ev: asyncio.Event, state: NodeState) -> None:
        """IDLE, no seeds → bootstrap as first node."""
        try:
            await self.start_peer()
            ready = await self._set_phase(state, MemberPhase.READY)
            await self._start_gossip()
            await self._announce(ready)
            await self.start_kv()
            logger.info("Node %s is READY (first node).", self._cfg.node_id)
            await ev.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop_all()

    async def _seedless_ready(self, ev: asyncio.Event, state: NodeState) -> None:
        """READY, no seeds → restart as single node."""
        try:
            await self.start_peer()
            await self._topology.apply_member(self._member_from(state))
            await self._start_gossip()
            await self._announce(state)
            await self.start_kv()
            logger.info("Node %s is READY.", self._cfg.node_id)
            await ev.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop_all()

    async def _seeded_idle(self, ev: asyncio.Event) -> None:
        """
        IDLE, with seeds → start peer and wait for explicit join command.
        """
        try:
            await self.start_peer()
            logger.info("Waiting for 'tourctl node join'")
            await ev.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop_all()

    async def _seeded_joining(
        self, ev: asyncio.Event, seeds: list[str], state: NodeState
    ) -> None:
        """JOINING, with seeds → resume an interrupted join."""
        try:
            await self.start_peer()
            await self._topology.apply_member(self._member_from(state))
            await self._resync(seeds)
            await self._start_gossip()
            await self._announce(state)
            await self._do_join(state)
            await ev.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop_all()

    async def _seeded_ready(
        self, ev: asyncio.Event, seeds: list[str], state: NodeState
    ) -> None:
        """READY, with seeds → rejoin the cluster after a restart."""
        try:
            await self.start_peer()
            await self._topology.apply_member(self._member_from(state))
            await self._resync(seeds)
            await self._start_gossip()
            await self._announce(state)
            await ev.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop_all()

    async def _seeded_draining(
        self, ev: asyncio.Event, seeds: list[str], state: NodeState
    ) -> None:
        """DRAINING, with seeds → resume a drain."""
        try:
            await self.start_peer()
            await self._topology.apply_member(self._member_from(state))
            await self._resync(seeds)
            await self._start_gossip()
            await self._announce(state)
            await self.start_kv()
            await self._do_drain(state)
            await ev.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop_all()

    async def _peer_only(self, ev: asyncio.Event, phase: MemberPhase) -> None:
        """FAILED or PAUSED → peer server only."""
        try:
            await self.start_peer()
            logger.info(
                "Node %s: peer-only mode (phase=%s).", self._cfg.node_id, phase.value
            )
            await ev.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop_all()

    async def _do_join(self, state: NodeState) -> bool:
        """JOINING → READY (persist, announce, start KV) or FAILED (persist, announce).

        Precondition: gossip engine is running.
        """
        snapshot = await self._topology.snapshot()
        vnodes = [VNode(state.node_id, t) for t in state.tokens]
        new_ring = snapshot.ring.add_vnodes(vnodes)
        ok = await self._rebalancer.rebalance(
            snapshot.ring, new_ring, snapshot.epoch, wait=True
        )
        snapshot = await self._topology.snapshot()

        if ok:
            ready = self._next_state(state, MemberPhase.READY, snapshot.epoch)
            await self._state.save(ready)
            await self._topology.apply_member(self._member_from(ready))
            await self._announce(ready)
            await self.start_kv()
            logger.info("Node %s is READY.", self._cfg.node_id)
            return True

        failed = self._next_state(state, MemberPhase.FAILED, snapshot.epoch)
        await self._state.save(failed)
        await self._topology.apply_member(self._member_from(failed))
        await self._announce(failed)
        logger.warning("Node %s join rebalance failed → FAILED.", self._cfg.node_id)
        return False

    async def _do_drain(self, state: NodeState) -> bool:
        """DRAINING → IDLE (persist, announce, stop KV) or FAILED (persist, announce).

        Precondition: gossip engine is running and KV server is started.
        """
        snapshot = await self._topology.snapshot()
        old_ring = snapshot.ring
        new_ring = old_ring.drop_nodes({state.node_id})
        ok = await self._rebalancer.rebalance(
            old_ring, new_ring, snapshot.epoch, wait=True
        )
        snapshot = await self._topology.snapshot()

        if ok:
            idle = self._next_state(state, MemberPhase.IDLE, snapshot.epoch)
            await self._state.save(idle)
            await self._topology.apply_member(self._member_from(idle))
            await self._announce(idle)
            await self.stop_kv()
            logger.info("Node %s drain complete → IDLE.", self._cfg.node_id)
            return True

        failed = self._next_state(state, MemberPhase.FAILED, snapshot.epoch)
        await self._state.save(failed)
        await self._topology.apply_member(self._member_from(failed))
        await self._announce(failed)
        await self.stop_kv()
        logger.warning("Node %s drain rebalance failed → FAILED.", self._cfg.node_id)
        return False

    async def _load_state(self) -> NodeState:
        persisted = await self._state.load()
        if persisted is not None:
            self.check_node_id_consistency(self._cfg.node_id, persisted.node_id)
            if not self.check_tokens_coherence(
                persisted.phase, persisted.tokens, self._cfg.node_size
            ):
                raise BootstrapError(
                    "token count mismatch: "
                    f"state has {len(persisted.tokens)} token(s) "
                    f"but node size {self._cfg.node_size.value} requires "
                    f"{self._cfg.node_size.token_count}."
                )
        return persisted or NodeState.first(self._cfg.node_id)

    async def _restart_state(self, state: NodeState) -> NodeState:
        """Increment generation and reset seq=0 at every restart. Persist immediately.

        Does NOT generate tokens — token allocation happens in _set_phase, only
        on IDLE → READY / IDLE → JOINING transitions.
        """
        restarted = NodeState(
            node_id=state.node_id,
            phase=state.phase,
            generation=state.generation + 1,
            seq=0,
            tokens=state.tokens,
            epoch=state.epoch,
            committed_pids=state.committed_pids,
            staging_pids=state.staging_pids,
        )
        await self._state.save(restarted)
        logger.info(
            "Node %s generation bumped to %d.", self._cfg.node_id, restarted.generation
        )
        return restarted

    async def _set_phase(self, state: NodeState, phase: MemberPhase) -> NodeState:
        """Transition from IDLE to the given phase. Persist and register in topology.

        When ``state.phase`` is IDLE, fresh tokens are generated here.
        This is the single allocation point for token generation:
        it fires only on IDLE → READY and IDLE → JOINING.
        """
        tokens = state.tokens
        if state.phase is MemberPhase.IDLE:
            tokens = self._generate_tokens(self._cfg.node_size.token_count)
            logger.info(
                "Generated %d token(s) for node size %s.",
                len(tokens),
                self._cfg.node_size.value,
            )
        new_state = NodeState(
            node_id=state.node_id,
            phase=phase,
            generation=state.generation,
            seq=state.seq + 1,
            tokens=tokens,
            epoch=state.epoch,
            committed_pids=state.committed_pids,
            staging_pids=state.staging_pids,
        )
        await self._state.save(new_state)
        await self._topology.apply_member(self._member_from(new_state))
        return new_state

    async def _start_gossip(self) -> None:
        if self._gossip_task is not None and not self._gossip_task.done():
            return
        self._gossip_task = asyncio.get_running_loop().create_task(
            self._gossip.start(), name="gossip.engine"
        )

    async def _announce(self, state: NodeState) -> None:
        await self._gossip.announce(self._member_from(state))

    async def _resync(self, seeds: list[str]) -> bool:
        bootstrapper = GossipBootstrapper(
            topology_manager=self._topology,
            config=self._cfg.gossip.bootstrap,
            partition_shift=self._cfg.partition_shift,
            ssl_ctx=self._ssl_client,
            serializer=self._serializer,
        )
        try:
            ok = await bootstrapper.run(seeds)
            self._gossip.stats.bootstrap_ok_total += ok
            return True
        except GossipBootstrapError as exc:
            self._gossip.stats.bootstrap_err_total += 1
            logger.warning("Seed bootstrap failed: %s", exc)
            return False

    def _generate_tokens(self, count: int) -> tuple[int, ...]:
        seen: set[int] = set()
        while len(seen) < count:
            seen.add(secrets.randbelow(self._partitioner.space.max))
        return tuple(seen)

    def _member_from(self, state: NodeState) -> Member:
        return Member(
            node_id=state.node_id,
            peer_address=self._cfg.peer_server.advertise,
            generation=state.generation,
            seq=state.seq,
            phase=state.phase,
            tokens=state.tokens,
            partition_shift=self._cfg.partition_shift,
        )

    @staticmethod
    def _next_state(state: NodeState, phase: MemberPhase, epoch: int) -> NodeState:
        return NodeState(
            node_id=state.node_id,
            phase=phase,
            generation=state.generation,
            seq=state.seq + 1,
            tokens=state.tokens,
            epoch=epoch,
            committed_pids=state.committed_pids,
            staging_pids=state.staging_pids,
        )

    def _build_ssl(self, cfg: TourillonConfig) -> tuple[ssl.SSLContext, ssl.SSLContext]:
        try:
            ssl_peer = self._tls_ctx.build_server_ssl_context(
                cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
            )
            ssl_kv = self._tls_ctx.build_server_ssl_context(
                cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
            )
        except TlsValidationError as exc:
            raise BootstrapError(f"TLS error: {exc}") from exc
        return ssl_peer, ssl_kv

    @staticmethod
    def _on_join_done(task: asyncio.Task[bool]) -> None:
        if exc := task.exception():
            logger.error("Background join-to-ready failed.", exc_info=exc)

    @staticmethod
    def _on_drain_done(task: asyncio.Task[bool]) -> None:
        if exc := task.exception():
            logger.error("Background drain-to-idle failed.", exc_info=exc)

    async def _on_gossip_failed(self) -> None:
        state = await self._state.load()
        if state is None or state.phase is MemberPhase.FAILED:
            return
        failed = self._next_state(state, MemberPhase.FAILED, state.epoch)
        await self._state.save(failed)
        await self._topology.apply_member(self._member_from(failed))
        await self._gossip.announce(self._member_from(failed))
