import asyncio
import logging
import secrets
import ssl
from dataclasses import dataclass
from typing import Any

from tourillon.core.exceptions import (
    BootstrapError,
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

_TOKEN_CHECK_PHASES = frozenset(
    {MemberPhase.JOINING, MemberPhase.READY, MemberPhase.DRAINING}
)


@dataclass
class StartupContext:
    cfg: TourillonConfig
    state: NodeState

class NodeStarter:
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
        self._peer_dispatcher = peer_dispatcher
        self._kv_dispatcher = kv_dispatcher
        self._tls_ctx = tls_ctx
        self._state = state
        self._topology = topology or TopologyManager()
        self._probe = ProbeManager()
        self._serializer = serializer
        self._storage = storage

        ssl_peer, ssl_kv = self._build_ssl_contexts(cfg)
        self._ssl_client = self._tls_ctx.build_client_ssl_context(
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
        self._partitioner = partitioner
        self._rebalancer = Rebalancer(
            partitioner=self._partitioner,
            node_id=cfg.node_id,
            replication_factor=cfg.replication_factor,
            storage=self._storage,
            peer_pool=self._peer_pool,
            serializer=self._serializer,
            topology_mgr=self._topology,
        )
        self._gossip = GossipEngine(
            node_id=cfg.node_id,
            topology_manager=self._topology,
            pool=self._peer_pool,
            config=cfg.gossip,
            partition_shift=cfg.partition_shift,
            serializer=self._serializer,
            on_failed=self._on_gossip_failed,
        )
        self._gossip_task: asyncio.Task[None] | None = None
        self._join_task: asyncio.Task[NodeState] | None = None
        self._peer_started = False
        self._kv_started = False
        self._effective_seeds = []

    async def start(
        self,
        stop_event: asyncio.Event | None = None,
        seeds: list[str] | None = None,
    ) -> None:
        context = await self._build_startup_context()
        phase = context.state.phase
        self._effective_seeds = seeds or context.cfg.seeds or self._effective_seeds
        logger.info("Node %s starting (phase: %s).", self._cfg.node_id, phase.value)
        self._log_seed_mode(self._effective_seeds)

        shutdown_event = stop_event or asyncio.Event()
        if self._effective_seeds:
            await self._run_with_seeds(
                shutdown_event, self._effective_seeds, context.state
            )
        else:
            await self._run_without_seeds(shutdown_event, context.state)

    async def join(self, seeds_override: list[str] | None = None) -> dict[str, Any]:
        persisted = await self._state.load()
        current = persisted or NodeState.first(self._cfg.node_id)
        if current.phase is MemberPhase.JOINING:
            raise JoinError(
                code="already_joining",
                message=f"Node {self._cfg.node_id} has already in joining phase"
            )
        if current.phase is MemberPhase.READY:
            raise JoinError(
                code="already_ready",
                message=f"Node {self._cfg.node_id} has already ready phase"
            )
        if current.phase is not MemberPhase.IDLE:
            raise JoinError(
                code="bad_phase",
                message=f"Node {self._cfg.node_id} cannot join from {current.phase.value} phase"
            )

        self._effective_seeds = seeds_override or self._effective_seeds
        if not self._effective_seeds:
            raise JoinError(
                code="empty_seeds",
                message=f"seeds are required for joining but got {self._effective_seeds}"
            )

        joining_state = NodeState(
            node_id=self._cfg.node_id,
            phase=MemberPhase.JOINING,
            generation=current.generation + 1,
            seq=current.seq + 1,
            tokens=self._generate_tokens(self._cfg.node_size.token_count),
            epoch=current.epoch,
            committed_pids=current.committed_pids,
            staging_pids=current.staging_pids,
        )
        await self._state.save(joining_state)
        joining_member = self._member_from_state(joining_state)
        await self._topology.apply_member(joining_member)
        await self._ensure_gossip_running()
        await self._gossip.announce(joining_member)
        self._join_task = self._trigger_join(
            seeds=self._effective_seeds,
            state=joining_state
        )
        logger.info(
            "Node %s entered JOINING with %d seed(s).",
            self._cfg.node_id,
            len(self._effective_seeds),
        )
        resp = {
            "node_id": self._cfg.node_id,
            "phase": MemberPhase.JOINING,
            "changed": True
        }
        return resp

    async def start_peer(self) -> None:
        if self._peer_started:
            return
        peer_host, peer_port = split_host_port(self._cfg.peer_server.bind)
        try:
            await self._peer_server.start(peer_host, peer_port)
        except OSError as exc:
            raise BootstrapError(
                f"cannot bind peer listener on {self._cfg.peer_server.bind}: {exc}",
                exit_code=1,
            ) from exc
        self._peer_started = True
        logger.info("Peer listener: %s:%d", peer_host, peer_port)

    async def start_kv(self) -> None:
        if self._kv_started:
            return
        kv_host, kv_port = split_host_port(self._cfg.kv_server.bind)
        try:
            await self._kv_server.start(kv_host, kv_port)
        except OSError as exc:
            raise BootstrapError(
                f"cannot bind KV listener on {self._cfg.kv_server.bind}: {exc}",
                exit_code=1,
            ) from exc
        self._kv_started = True
        logger.info("KV  listener: %s:%d", kv_host, kv_port)

    async def stop_all(self) -> None:
        if self._join_task is not None and not self._join_task.done():
            self._join_task.cancel()
        if self._gossip_task is not None:
            await self._gossip.stop()
            await asyncio.gather(self._gossip_task, return_exceptions=True)
            self._gossip_task = None
        await self._peer_pool.close_all()
        await self.stop_kv()
        await self.stop_peer()

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
        if phase not in _TOKEN_CHECK_PHASES:
            return True
        return len(tokens) == node_size.token_count

    async def _advance_from_idle(self, state: NodeState, phase: MemberPhase) -> NodeState:
        tokens = self._generate_tokens(self._cfg.node_size.token_count)
        new_state = NodeState(
            node_id=state.node_id,
            phase=phase,
            generation=state.generation + 1,
            seq=1,
            epoch=state.epoch,
            tokens=tokens,
            staging_pids=state.staging_pids,
            committed_pids=state.committed_pids,
        )
        await self._state.save(new_state)
        logger.info(
            "Generated %d token(s) for node size %s.",
            self._cfg.node_size.token_count,
            self._cfg.node_size.value,
        )
        await self._topology.apply_member(self._member_from_state(new_state))
        return new_state

    async def _apply_announce_and_persist(self, state: NodeState) -> None:
        member = self._member_from_state(state)
        await self._topology.apply_member(member)
        await self._gossip.announce(member)
        await self._state.save(state)

    def _build_ssl_contexts(
        self,
        cfg: TourillonConfig,
    ) -> tuple[ssl.SSLContext, ssl.SSLContext]:
        try:
            ssl_peer = self._tls_ctx.build_server_ssl_context(
                cfg.tls.cert_data,
                cfg.tls.key_data,
                cfg.tls.ca_data,
            )
            ssl_kv = self._tls_ctx.build_server_ssl_context(
                cfg.tls.cert_data,
                cfg.tls.key_data,
                cfg.tls.ca_data,
            )
        except TlsValidationError as exc:
            raise BootstrapError(f"TLS error: {exc}") from exc
        return ssl_peer, ssl_kv

    async def _build_startup_context(self) -> StartupContext:
        persisted = await self._state.load()
        if persisted is not None:
            self.check_node_id_consistency(self._cfg.node_id, persisted.node_id)
            if not self.check_tokens_coherence(
                persisted.phase,
                persisted.tokens,
                self._cfg.node_size,
            ):
                raise BootstrapError(
                    "token count mismatch: "
                    f"state has {len(persisted.tokens)} token(s) "
                    f"but node size {self._cfg.node_size.value} requires "
                    f"{self._cfg.node_size.token_count}."
                )

        state = persisted or NodeState.first(self._cfg.node_id)
        return StartupContext(cfg=self._cfg, state=persisted or state)

    async def _ensure_gossip_running(self) -> None:
        if self._gossip_task is not None and not self._gossip_task.done():
            return
        loop = asyncio.get_running_loop()
        self._gossip_task = loop.create_task(self._gossip.start(), name="gossip.engine")

    async def _full_resync(self, seeds: list[str]) -> bool:
        bootstrapper = GossipBootstrapper(
            topology_manager=self._topology,
            config=self._cfg.gossip.bootstrap,
            partition_shift=self._cfg.partition_shift,
            ssl_ctx=self._ssl_client,
            serializer=self._serializer,
        )
        try:
            seeds_ok = await bootstrapper.run(seeds)
            self._gossip.stats.bootstrap_ok_total += seeds_ok
            return True
        except GossipBootstrapError as exc:
            self._gossip.stats.bootstrap_err_total += 1
            logger.warning("Seed bootstrap failed: %s", exc)
            return False

    def _generate_tokens(self, count: int) -> tuple[int, ...]:
        token_set: set[int] = set()
        while len(token_set) < count:
            token_set.add(secrets.randbelow(self._partitioner.space.max))
        return tuple(token_set)

    @staticmethod
    def _increment_state_seq(state: NodeState) -> NodeState:
        return NodeState(
            node_id=state.node_id,
            phase=MemberPhase.READY,
            generation=state.generation,
            seq=state.seq + 1,
            tokens=state.tokens,
            staging_pids=state.staging_pids,
            committed_pids=state.committed_pids,
            epoch=state.epoch,
        )

    async def _joining_to_ready(self, state: NodeState, seeds: list[str]) -> NodeState:
        resync_ok = await self._full_resync(seeds)
        snapshot = await self._topology.snapshot()
        epoch = snapshot.epoch
        if not resync_ok:
            failed_state = self._next_state(state, MemberPhase.FAILED, epoch)
            await self._apply_announce_and_persist(failed_state)
            return failed_state

        old_ring = snapshot.ring
        vnodes = [VNode(state.node_id, t) for t in state.tokens]
        new_ring = old_ring.add_vnodes(vnodes)
        rebalance_ok = await self._rebalancer.rebalance(
            old_ring, new_ring, epoch, wait=True
        )
        snapshot = await self._topology.snapshot()
        epoch = snapshot.epoch

        if not rebalance_ok:
            failed_state = self._next_state(state, MemberPhase.FAILED, epoch)
            await self._apply_announce_and_persist(failed_state)
            return failed_state

        ready_state = self._next_state(state, MemberPhase.READY, epoch)
        await self._apply_announce_and_persist(ready_state)
        return ready_state

    async def _log_partition_ranges(
        self, partitioner: Partitioner, node_id: str
    ) -> None:
        snapshot = await self._topology.snapshot()
        logger.info(
            "Partition ranges owned (%d total partitions):",
            partitioner.total_partitions,
        )
        for partition_range in partitioner.ranges_for(snapshot.ring):
            if partition_range.owner.node_id != node_id:
                continue

            token_prefix = f"{partition_range.owner.token:032x}"[:8]
            token_hex = f"0x{token_prefix}..."
            wrap_marker = "(w)" if partition_range.wraps else ""
            logger.info(
                "  token %s -> pids [%4d-%4d]%s  (%d partitions)",
                token_hex,
                partition_range.start_pid,
                partition_range.end_pid,
                wrap_marker,
                partition_range.count,
            )

    def _log_seed_mode(self, seeds: list[str]) -> None:
        if seeds:
            logger.info(
                "Node %s starting with %d seed(s): %s",
                self._cfg.node_id,
                len(seeds),
                seeds,
            )
            return
        logger.info("Node %s starting without seeds.", self._cfg.node_id)

    def _member_from_state(self, state: NodeState) -> Member:
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
    def _next_state(
        state: NodeState,
        phase: MemberPhase,
        epoch: int,
    ) -> NodeState:
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

    async def _on_gossip_failed(self) -> None:
        state = await self._state.load()
        if state is None or state.phase is MemberPhase.FAILED:
            return
        failed = NodeState(
            node_id=state.node_id,
            phase=MemberPhase.FAILED,
            generation=state.generation,
            seq=state.seq + 1,
            tokens=state.tokens,
            epoch=state.epoch,
            committed_pids=state.committed_pids,
            staging_pids=state.staging_pids,
        )
        await self._state.save(failed)
        failed_member = self._member_from_state(failed)
        await self._topology.apply_member(failed_member)
        await self._gossip.announce(failed_member)

    async def _run_only_peer_with_seeds(
        self,
        shutdown_event: asyncio.Event,
        seeds: list[str],
        phase: MemberPhase,
    ) -> None:
        try:
            await self.start_peer()
            if phase == MemberPhase.IDLE:
                logger.info(
                    "Node %r is idle with %s seed(s); issue 'tourctl node join' to begin seeded join.",
                    self._cfg.node_id,
                    str(seeds),
                )
            await shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop_all()
            logger.info("Shutdown complete.")

    async def _run_transition_with_seeds(
        self,
        shutdown_event: asyncio.Event,
        seeds: list[str],
        state: NodeState
    ) -> None:
        try:
            await self.start_peer()
            logger.info(
                "Node %r is syncing memberships from seeds %s",
                self._cfg.node_id,
                str(seeds),
            )
            await self._full_resync(seeds)
            await self._ensure_gossip_running()
            member = self._member_from_state(state)
            await self._topology.apply_member(member)
            await self._gossip.announce(member)
            logger.info(
                "Topology rebuilt from seeds: %s",
                str(seeds),
            )
            await self._state.save(state)
            logger.info(
                "State persisted (phase: %s, seq: %d, generation: %d).",
                state.phase.value,
                state.seq,
                state.generation,
            )
            await self._log_partition_ranges(self._partitioner, state.node_id)
            await self.start_kv()
            await shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop_all()
            logger.info("Shutdown complete.")

    async def _run_with_seeds(
        self,
        stop_event: asyncio.Event,
        seeds: list[str],
        state: NodeState
    ) -> None:
        phase = state.phase

        match phase:
            case MemberPhase.JOINING:
                await self.start_peer()
                await self._log_partition_ranges(self._partitioner, state.node_id)
                await self._joining_to_ready(state, seeds)
                await self.start_kv()
                await stop_event.wait()
            case MemberPhase.READY | MemberPhase.DRAINING:
                await self._run_transition_with_seeds(stop_event, seeds, state)
                await self.start_peer()
                await self._log_partition_ranges(self._partitioner, state.node_id)
                await self._full_resync(seeds)
                await self._ensure_gossip_running()
                member = self._member_from_state(state)
                await self._topology.apply_member(member)
                await self._gossip.announce(member)
                await self.start_kv()
                await stop_event.wait()
            case _:
                await self._run_only_peer_with_seeds(stop_event, seeds, phase)

    async def _run_idle_without_seeds(
        self,
        shutdown_event: asyncio.Event,
        state: NodeState
    ) -> None:
        partitioner = self._partitioner

        try:
            await self.start_peer()
            bootstrap_state = await self._advance_from_idle(state, MemberPhase.READY)
            await self._log_partition_ranges(partitioner, bootstrap_state.node_id)
            logger.info(
                "State persisted (phase: %s, seq: %d, generation: %d).",
                bootstrap_state.phase.value,
                bootstrap_state.seq,
                bootstrap_state.generation,
            )
            await self._topology.apply_member(self._member_from_state(bootstrap_state))
            logger.info("Topology built without seeds.")
            await self._ensure_gossip_running()
            await self.start_kv()
            logger.info("Node %s is READY.", bootstrap_state.node_id)
            await shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop_all()
            logger.info("Shutdown complete.")

    async def _run_ready_without_seeds(
        self,
        shutdown_event: asyncio.Event,
        state: NodeState
    ) -> None:
        partitioner = self._partitioner

        try:
            await self.start_peer()
            await self._state.save(state)
            logger.info(
                "State persisted (phase: %s, seq: %d, generation: %d).",
                state.phase.value,
                state.seq,
                state.generation,
            )
            await self._topology.apply_member(self._member_from_state(state))
            logger.info(
                "Topology rebuilt without seeds",
            )
            await self._log_partition_ranges(partitioner, state.node_id)
            await self._ensure_gossip_running()
            await self.start_kv()
            logger.info("Node %s is READY.", state.node_id)
            await shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop_all()
            logger.info("Shutdown complete.")

    async def _run_without_seeds(
        self,
        stop_event: asyncio.Event,
        state: NodeState
    ) -> None:
        match state.phase:
            case MemberPhase.IDLE:
                await self._run_idle_without_seeds(stop_event, state)
            case MemberPhase.READY:
                await self._run_ready_without_seeds(stop_event, state)
            case _:
                raise BootstrapError(
                    f"unexpected phase {state.phase.value} - cannot start from this state via 'node start'.",
                    exit_code=1,
                )

    def _trigger_join(
        self,
        seeds: list[str],
        state: NodeState
    ) -> asyncio.Task[NodeState]:
        loop = asyncio.get_running_loop()

        def on_done(t: asyncio.Task) -> None:
            if exc := t.exception():
                logger.error("Error during transition to ready", exc_info=exc)

        task = loop.create_task(
            self._joining_to_ready(state, seeds),
            name="join-to-ready",
        )
        task.add_done_callback(on_done)
        return task
