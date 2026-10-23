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
import ssl
from collections.abc import Iterable
from pathlib import Path

from tourillon.core.antientropy.service import AntiEntropyService
from tourillon.core.config import TourillonConfig
from tourillon.core.exceptions import (
    BootstrapError,
    DrainError,
    JoinError,
    StartupError,
)
from tourillon.core.gossip.bootstrapper import GossipBootstrapper
from tourillon.core.gossip.gossiper import Gossiper, GossipStats
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.log import logger
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.models.state import NodeState
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.partition.placement import PlacementStrategy
from tourillon.core.peer.pool import PeerPool
from tourillon.core.ports import Telemetry
from tourillon.core.ports.storage import Storage
from tourillon.core.replay.hint import HintReplayer
from tourillon.core.services.store import StoreService
from tourillon.core.spaces.ring import Ring
from tourillon.core.transfer.runner import TransferRunner
from tourillon.core.transports.dispatcher import Dispatcher
from tourillon.core.transports.server import EnvelopeServer
from tourlib import Serializer, ValidationError
from tourlib.constant import NODE_STATE_FILENAME
from tourlib.helpers import parse_bind
from tourlib.port import Persistence

MAX_DRAIN_SEEDS = 5


class NodeLifecycle:
    def __init__(
        self,
        config: TourillonConfig,
        peer_dispatcher: Dispatcher,
        client_dispatcher: Dispatcher,
        tls_server: ssl.SSLContext,
        tls_client: ssl.SSLContext,
        persistence: Persistence,
        serializer: Serializer,
        peer_pool: PeerPool,
        topologymgr: TopologyManager,
        partitioner: Partitioner,
        storage: Storage,
        gossip_stats: GossipStats,
        telemetry: Telemetry,
        store: StoreService,
        placement_strategy: PlacementStrategy,
    ) -> None:
        self._config = config
        self._persistence = persistence
        self._topologymgr = topologymgr
        self._partitioner = partitioner
        self._serializer = serializer
        self._peer_pool = peer_pool
        self._tls_client = tls_client
        self._gossip_stats = gossip_stats
        self._telemetry = telemetry
        self._store = store
        self._hint_replayer = HintReplayer(
            storage=storage,
            peer_pool=peer_pool,
            serializer=serializer,
            total_groups=partitioner.total_groups,
            placement_strategy=placement_strategy,
            node_id=config.node.id,
        )
        self._peer_server = EnvelopeServer(
            name="peer",
            dispatcher=peer_dispatcher,
            ssl_context=tls_server,
            telemetry=self._telemetry,
        )
        self._client_server = EnvelopeServer(
            name="client",
            dispatcher=client_dispatcher,
            ssl_context=tls_server,
            telemetry=self._telemetry,
        )
        self._gossiper = Gossiper(
            node_id=self._config.node.id,
            config=config.gossip,
            stats=gossip_stats,
            topologymgr=topologymgr,
            peer_pool=self._peer_pool,
            serializer=serializer,
            telemetry=telemetry,
        )
        self._transfer_runner = TransferRunner(
            node_id=config.node.id,
            replication_factor=config.placement.replication_factor,
            partitioner=partitioner,
            peer_pool=peer_pool,
            serializer=serializer,
            storage=storage,
        )

        # Runs only while the node is READY: a joining node is still being
        # filled by transfers and a draining one hands its data off instead.
        self._antientropy = AntiEntropyService(
            node_id=config.node.id,
            storage=storage,
            partitioner=partitioner,
            topologymgr=topologymgr,
            peer_pool=peer_pool,
            serializer=serializer,
            replication_factor=config.placement.replication_factor,
            config=config.placement.anti_entropy,
        )

        self._state_file = Path(self._config.storage.data_dir) / NODE_STATE_FILENAME
        self._join_task: asyncio.Task[None] | None = None
        self._drain_task: asyncio.Task[None] | None = None
        self._membership_lock = asyncio.Lock()
        self._peer_started = False
        self._client_started = False

    @property
    def antientropy(self) -> AntiEntropyService:
        return self._antientropy

    @property
    def gossiper(self) -> Gossiper:
        return self._gossiper

    async def start(self, seeds: list[str] | None = None) -> None:
        logger.info("Starting node lifecycle for '%s'", self._config.node.id)
        if seeds:
            logger.debug("Lifecycle received seed overrides: %s", seeds)
        state = await self._load_or_create_state()
        logger.info("Applying startup phase '%s'", state.phase.value)
        match state.phase:
            case MemberPhase.IDLE:
                await self._startup_idle(state, seeds)
            case MemberPhase.JOINING:
                await self._startup_joining(state, seeds)
            case MemberPhase.READY:
                await self._startup_ready(state, seeds)
            case MemberPhase.DRAINING:
                await self._startup_draining()
            case MemberPhase.FAILED:
                await self._startup_failed()
            case MemberPhase.PAUSED:
                await self._startup_paused()

        logger.info("Node lifecycle started for '%s'", self._config.node.id)

    async def stop(self) -> None:
        logger.info("Stopping node lifecycle for '%s'", self._config.node.id)
        await self._stop_client()
        if self.is_draining():
            assert self._drain_task is not None
            self._drain_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._drain_task

        await self._antientropy.stop()
        await self._hint_replayer.stop()

        if self.is_joining():
            assert self._join_task is not None
            self._join_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._join_task

        await self._gossiper.stop()
        await self._stop_peer()
        logger.info("Node lifecycle stopped for '%s'", self._config.node.id)

    def is_joining(self) -> bool:
        return self._join_task is not None and not self._join_task.done()

    def is_draining(self) -> bool:
        return self._drain_task is not None and not self._drain_task.done()

    async def join(self, seeds: list[str] | None = None) -> None:
        async with self._membership_lock:
            if self.is_draining():
                raise JoinError("Drain operation already in progress.")
            if self.is_joining():
                logger.warning("Join operation already in progress")
                raise JoinError("Join operation already in progress.")

            state = await self._load_state()
            if state.phase == MemberPhase.DRAINING:
                raise JoinError("Cannot join while the node is draining.")
            effective_seeds = seeds or state.seeds or self._config.node.seeds or []
            if not effective_seeds:
                logger.error(
                    "No seed peers provided for join operation, 'seeds' are required"
                )
                raise JoinError(
                    "No seed peers provided for join operation, 'seeds' are required."
                )

            logger.debug("Reloading node state from '%s'", self._state_file)
            joining_state = await self._transition_to(
                state,
                MemberPhase.JOINING,
                with_tokens=True,
                with_seeds=list(effective_seeds),
            )
            self._log_tokens(joining_state)
            logger.info("Triggering join operation for node '%s'", self._config.node.id)
            self._join_task = asyncio.create_task(
                self._perform_join(joining_state), name="node-join"
            )

    async def drain(self) -> None:
        """Acknowledge an explicit drain request; completion is announced as IDLE."""
        async with self._membership_lock:
            if self.is_joining() or self.is_draining():
                raise DrainError("A membership operation is already in progress.")
            state = await self._load_state()
            if state.phase not in (
                MemberPhase.READY,
                MemberPhase.PAUSED,
                MemberPhase.DRAINING,
            ):
                raise DrainError(f"Cannot drain from phase '{state.phase.value}'.")
            topology = await self._topologymgr.snapshot()
            if not len(topology.ring.drop_nodes({state.node_id})):
                raise DrainError("Cannot drain without another node on the ring.")
            if state.phase != MemberPhase.DRAINING:
                state = await self._transition_to(
                    state,
                    MemberPhase.DRAINING,
                    with_seeds=self._drain_seeds(state, topology.registry),
                )

            await self._antientropy.stop()
            self._store.pause()
            self._hint_replayer.pause_local_writes()
            await self._publish_state(state)
            await self._hint_replayer.start()
            self._drain_task = asyncio.create_task(
                self._perform_drain(state), name="node-drain"
            )

    @staticmethod
    def _drain_seeds(state: NodeState, members: Iterable[Member]) -> list[str]:
        seeds: list[str] = []
        for member in members:
            if member.node_id == state.node_id:
                continue
            if member.phase not in (MemberPhase.READY, MemberPhase.PAUSED):
                continue
            seeds.append(member.peer_address)
            if len(seeds) == MAX_DRAIN_SEEDS:
                break
        return seeds or list(state.seeds[:MAX_DRAIN_SEEDS])

    async def _perform_drain(self, state: NodeState) -> None:
        try:
            await self._store.drain()
            await self._hint_replayer.drain_local_writes()
            await self._drain_transfers(state)
            await self._store.stop_read_repairs()
            await self._hint_replayer.wait()
            await self._hint_replayer.stop()
            self._store.pause(readable=False)
            await self._stop_client()
            idle = await self._transition_to(state, MemberPhase.IDLE)
            await self._publish_state(idle)
        except Exception as exc:
            logger.exception(
                "Drain failed for node '%s'; node remains DRAINING. Reason: %s",
                state.node_id,
                exc,
            )

    async def _drain_transfers(self, state: NodeState) -> None:
        member = await self._publish_state(state)
        _, failed = await self._run_transfers(member, draining=True)
        if failed:
            raise DrainError(f"Data transfer failed for {len(failed)} ranges.")

    def _generate_tokens(self) -> tuple[str, ...]:
        count = self._config.node.size.to_value()
        return tuple(hex(token) for token in self._partitioner.space.position(count))

    async def _load_state(self) -> NodeState:
        raw_state = await self._persistence.load(self._state_file)
        return NodeState.from_document(raw_state)

    async def _load_or_create_state(self) -> NodeState:
        logger.debug("Loading node state from '%s'", self._state_file)

        try:
            return await self._load_state()
        except FileNotFoundError:
            logger.info("State file missing; creating default IDLE state")
            state = NodeState(
                node_id=self._config.node.id,
                phase=MemberPhase.IDLE,
                epoch=0,
            )
            await self._persistence.write(self._state_file, state.to_document())
            logger.debug("Persisted initial node state to '%s'", self._state_file)
            return state

    @staticmethod
    def _log_tokens(state: NodeState) -> None:
        logger.info("Node state epoch=%s tokens (%d):", state.epoch, len(state.tokens))
        if not state.tokens:
            logger.info("  (none)")
            return

        for idx, token in enumerate(state.tokens):
            logger.info("  %.18s (%s)", token, idx + 1)

    async def _perform_join(self, state: NodeState) -> None:
        assert state.phase == MemberPhase.JOINING

        await self._peer_pool.start()
        await self._gossiper.start()
        member = await self._publish_state(state)
        synced = await self._sync_full(state)
        ready = False

        if synced:
            logger.info("Re-synced cluster membership successfully")
            success, failed = await self._run_transfers(member)

            if failed:
                logger.error(
                    "Data transfer failed for %d partitions; node will transition to FAILED",
                    len(failed),
                )
                final = await self._transition_to(state, MemberPhase.FAILED)
            else:
                logger.info(
                    "Data transfer completed successfully for %d partitions",
                    len(success),
                )
                final = await self._transition_to(state, MemberPhase.READY)
                self._store.resume()
                self._hint_replayer.resume_local_writes()
                await self._start_client()
                ready = True
        else:
            final = await self._transition_to(state, MemberPhase.FAILED)

        await self._publish_state(final)
        if not ready:
            logger.error("Join operation failed; node transitioned to FAILED phase")
            await self._gossiper.stop()
            return

        await self._hint_replayer.start()
        await self._antientropy.start()

    async def _run_transfers(
        self, member: Member, *, draining: bool = False
    ) -> tuple[list[str], list[str]]:
        """Re-plan join or drain transfers on ring changes until they settle.

        Transfers that settled before a re-plan are awaited again, so the
        result always covers the latest plan. Cancelling the join cancels the
        transfers: their staged data is kept and resumed by the next attempt.
        """
        topology = await self._topologymgr.snapshot()
        current_ring = topology.ring
        transfer = asyncio.create_task(
            self._transfer_runner.run(
                current_ring, self._transfer_target(current_ring, member, draining)
            ),
            name="transfer-run",
        )
        ring_change: asyncio.Task[Ring] | None = None
        try:
            while True:
                next_ring_change = asyncio.create_task(
                    self._topologymgr.wait_for_ring_change(current_ring.serial()),
                    name="wait-for-ring-change",
                )
                ring_change = next_ring_change
                await asyncio.wait(
                    (transfer, next_ring_change), return_when=asyncio.FIRST_COMPLETED
                )
                if not ring_change.done():
                    return transfer.result()

                current_ring = ring_change.result()
                logger.info("Ring changed during transfer; updating transfer plan")
                await self._transfer_runner.apply(
                    current_ring, self._transfer_target(current_ring, member, draining)
                )
                if transfer.done():
                    transfer = asyncio.create_task(
                        self._transfer_runner.wait(), name="transfer-wait"
                    )
        finally:
            if ring_change is not None:
                ring_change.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ring_change
            if not transfer.done():
                transfer.cancel()
                await self._transfer_runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await transfer

    @staticmethod
    def _transfer_target(current: Ring, member: Member, draining: bool) -> Ring:
        if not draining:
            return current.add_vnodes(member.vnodes)
        target = current.drop_nodes({member.node_id})
        if not len(target):
            raise DrainError("Cannot drain without another node on the ring.")
        return target

    async def _publish_state(self, state: NodeState) -> Member:
        member = Member(
            node_id=state.node_id,
            peer_address=self._config.server_peer.advertise,
            epoch=state.epoch,
            phase=state.phase,
            tokens=state.tokens,
        )
        await self._topologymgr.apply_member(member)
        await self._gossiper.announce(member)
        return member

    async def _start_ready(self, ready_state: NodeState) -> None:
        assert ready_state.phase == MemberPhase.READY

        await self._peer_pool.start()
        if ready_state.seeds:
            await self._sync_full(ready_state)
        await self._gossiper.start()
        await self._publish_state(ready_state)
        self._store.resume()
        self._hint_replayer.resume_local_writes()
        await self._start_client()
        await self._hint_replayer.start()
        await self._antientropy.start()

    async def _startup_idle(
        self, state: NodeState, seeds: list[str] | None = None
    ) -> None:
        await self._start_peer()
        effective_seeds = seeds or state.seeds or self._config.node.seeds
        if effective_seeds:
            logger.info(
                "Seed peers configured; waiting for explicit 'node join' "
                "command to transition to 'joining'"
            )
            return

        ready_state = await self._transition_to(
            state, MemberPhase.READY, with_tokens=True
        )
        self._log_tokens(ready_state)
        await self._start_ready(ready_state)

    async def _startup_joining(
        self, state: NodeState, seeds: list[str] | None = None
    ) -> None:
        await self._start_peer()
        self._log_tokens(state)

        if seeds:
            async with self._persistence.edit(self._state_file) as document:
                node = document.setdefault("node", {})
                node["seeds"] = seeds
                state = NodeState.from_document(document)

        async with self._membership_lock:
            await self._perform_join(state)

    async def _startup_ready(
        self, state: NodeState, seeds: list[str] | None = None
    ) -> None:
        await self._start_peer()

        if seeds:
            async with self._persistence.edit(self._state_file) as document:
                node = document.setdefault("node", {})
                node["seeds"] = seeds
                state = NodeState.from_document(document)

        await self._start_ready(state)

    async def _startup_draining(self) -> None:
        await self._start_peer()
        self._store.pause()
        self._hint_replayer.pause_local_writes()
        state = await self._load_state()
        await self._peer_pool.start()
        if state.seeds and not await self._sync_full(state):
            logger.error(
                "Cannot resume drain before membership sync for '%s'", state.node_id
            )
            return
        await self._gossiper.start()
        await self._publish_state(state)
        await self._start_client()
        await self._hint_replayer.start()
        self._drain_task = asyncio.create_task(
            self._perform_drain(state), name="node-drain"
        )

    async def _startup_failed(self) -> None:
        await self._start_peer()

    async def _startup_paused(self) -> None:
        await self._start_peer()

    async def _start_peer(self) -> None:
        peer_addr = self._config.server_peer.bind
        logger.info("Starting peer listener on %s", peer_addr)
        try:
            host, port = parse_bind(peer_addr)
        except ValueError:
            logger.error("Invalid peer bind format: %s", peer_addr)
            raise ValidationError(
                f"Invalid server_peer.bind format. Expected 'host:port', got '{peer_addr}'"
            ) from None

        try:
            await self._peer_server.start(host, port)
        except OSError as exc:
            logger.error("Peer bind failed on %s: %s", peer_addr, exc)
            raise StartupError(
                f"cannot bind Peer listener on {peer_addr}: {exc}",
            ) from exc

        self._peer_started = True
        peer_host, peer_port = self._peer_server.endpoint
        logger.info("Peer listener started on %s:%s", peer_host, peer_port)

    async def _start_client(self) -> None:
        client_addr = self._config.server_client.bind
        logger.info("Starting client listener on %s", client_addr)
        try:
            host, port = parse_bind(client_addr)
        except ValueError:
            logger.error("Invalid client bind format: %s", client_addr)
            raise ValidationError(
                f"Invalid server_client.bind format. Expected 'host:port', got '{client_addr}'"
            ) from None

        try:
            await self._client_server.start(host, port)
        except OSError as exc:
            logger.error("Client bind failed on %s: %s", client_addr, exc)
            raise StartupError(
                f"cannot bind Client listener on {client_addr}: {exc}",
            ) from exc

        self._client_started = True
        client_host, client_port = self._client_server.endpoint
        logger.info("Client listener started on %s:%s", client_host, client_port)

    async def _stop_client(self) -> None:
        if not self._client_started:
            return
        logger.debug("Stopping client listener")
        await self._client_server.stop()
        self._client_started = False
        logger.debug("Client listener stopped")

    async def _stop_peer(self) -> None:
        if not self._peer_started:
            return
        logger.debug("Stopping peer listener")
        await self._peer_server.stop()
        self._peer_started = False
        logger.debug("Peer listener stopped")

    async def _sync_full(self, state: NodeState) -> bool:
        bootstrapper = GossipBootstrapper(
            node_id=self._config.node.id,
            serializer=self._serializer,
            tls_client=self._tls_client,
            topologymgr=self._topologymgr,
            backoff=self._config.gossip.bootstrap_retry,
        )
        try:
            ok = await bootstrapper.run(state.seeds)
            self._gossip_stats.bootstrap_ok_total += ok
            return True
        except BootstrapError as exc:
            self._gossip_stats.bootstrap_err_total += 1
            logger.error("Gossip sync full failed: %s", exc)
            return False

    async def _transition_to(
        self,
        state: NodeState,
        new_phase: MemberPhase,
        *,
        with_tokens: bool = False,
        with_seeds: list[str] | None = None,
    ) -> NodeState:
        logger.info("Transitioning node phase to '%s'", new_phase.value)

        async with self._persistence.edit(self._state_file) as document:
            node = document.setdefault("node", {})
            node["phase"] = new_phase.value
            node["epoch"] = state.epoch + 1

            if with_tokens:
                node["tokens"] = list(self._generate_tokens())
            if with_seeds is not None:
                node["seeds"] = with_seeds

            document["node"] = node
            logger.debug(
                "Persisted node phase transition to '%s' in '%s'",
                new_phase.value,
                self._state_file,
            )
            return NodeState.from_document(document)
