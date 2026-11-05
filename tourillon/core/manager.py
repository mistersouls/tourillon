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
import logging
import signal
import ssl
from datetime import datetime

from tourillon.core.ae.obligations import ObligationQueue
from tourillon.core.config import TourillonConfig
from tourillon.core.gossip.gossiper import GossipStats
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.log import logger
from tourillon.core.models.clock import HLCClock
from tourillon.core.models.state import NodeState
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.partition.placement import SimplePlacementStrategy
from tourillon.core.peer.pool import PeerPool
from tourillon.core.ports import Telemetry, TelemetryProvider
from tourillon.core.ports.storage import StorageProvider
from tourillon.core.probe.detector import FailureDetectorProvider
from tourillon.core.services.aeinspector import AntiEntropyInspector
from tourillon.core.services.antientropy import AntiEntropyService
from tourillon.core.services.gc import GarbageCollector
from tourillon.core.services.gossip import GossipService
from tourillon.core.services.inspector import NodeInspector
from tourillon.core.services.lifecycle import NodeLifecycle
from tourillon.core.services.memberinspector import MemberInspector
from tourillon.core.services.report import NodeReport
from tourillon.core.services.store import StoreService
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.storage.retention import RetentionPolicy
from tourillon.core.transports.dispatcher import Dispatcher
from tourillon.core.transports.handshakes import HandshakeMonitor
from tourlib.models.ae import (
    AeStatus,
    AeStatusRequest,
    ObligationsPage,
    ObligationsRequest,
)
from tourlib.models.health import NodeHealth
from tourlib.models.inspect import (
    AntiEntropyInfo,
    MembersPage,
    MembersRequest,
    NodeStatus,
)
from tourlib.models.member import MemberReport
from tourlib.port import Persistence, Serializer


class Manager:
    """Manages the Tourillon daemon lifecycle and peer server.

    Coordinates startup, shutdown, and running of the peer server
    that handles envelope-based inter-node communication.
    """

    def __init__(
        self,
        config: TourillonConfig,
        peer_dispatcher: Dispatcher,
        client_dispatcher: Dispatcher,
        tls_server: ssl.SSLContext,
        tls_client: ssl.SSLContext,
        persistence: Persistence,
        serializer: Serializer,
        storage_provider: StorageProvider,
        telemetry_provider: TelemetryProvider,
        certificate_expires_at: datetime,
    ) -> None:
        self._config = config
        self._handshakes = HandshakeMonitor()
        self._handshakes.install(tls_server)
        self._partitioner = Partitioner(
            hashspace=HashSpace(bits=128), pshift=17, gshift=2
        )
        self._topologymgr = TopologyManager()
        self._gossip_stats = GossipStats()
        self._storage = storage_provider.create(config.storage)
        self._telemetry = telemetry_provider.create(config.telemetry)
        self._clock = HLCClock(node_id=self._config.node.id)
        self._fd_provider = FailureDetectorProvider(serializer=serializer)
        self._peer_pool = PeerPool(
            tls_ctx=tls_client,
            topologymgr=self._topologymgr,
            fd_provider=self._fd_provider,
            telemetry=self._telemetry,
            min_size=config.transport.pool.min_size,
            max_size=config.transport.pool.max_size,
            max_overflow=config.transport.pool.max_overflow,
            priority_size=config.transport.pool.priority_size,
            idle_ttl=config.transport.pool.idle_ttl,
        )
        self._placement_strategy = SimplePlacementStrategy(
            replication_factor=config.placement.replication_factor,
            topologymgr=self._topologymgr,
            partitioner=self._partitioner,
            peer_pool=self._peer_pool,
        )
        self._obligations = ObligationQueue(config.placement.anti_entropy.hotqueue_size)
        self._gc = GarbageCollector(
            self._storage,
            self._partitioner,
            config.placement.retention,
            node_id=config.node.id,
            telemetry=self._telemetry,
        )
        self._store = StoreService(
            node_id=config.node.id,
            storage=self._storage,
            retention=RetentionPolicy(config.placement.retention),
            partitioner=self._partitioner,
            placement_strategy=self._placement_strategy,
            clock=self._clock,
            serializer=serializer,
            peer_pool=self._peer_pool,
            read_repair=config.placement.read_repair,
            repair=config.placement.repair,
            obligations=self._obligations,
            telemetry=self._telemetry,
        )
        self._lifecycle = NodeLifecycle(
            placement_strategy=self._placement_strategy,
            config=config,
            peer_dispatcher=peer_dispatcher,
            client_dispatcher=client_dispatcher,
            tls_server=tls_server,
            tls_client=tls_client,
            persistence=persistence,
            serializer=serializer,
            peer_pool=self._peer_pool,
            gossip_stats=self._gossip_stats,
            partitioner=self._partitioner,
            topologymgr=self._topologymgr,
            storage=self._storage,
            telemetry=self._telemetry,
            store=self._store,
            obligations=self._obligations,
            gc=self._gc,
        )
        heartbeats = self._lifecycle.gossiper.heartbeats
        self._inspector = NodeInspector(
            gc_snapshot=self._gc.inspect,
            node_id=self._config.node.id,
            stats=self._gossip_stats,
            partitioner=self._partitioner,
            topologymgr=self._topologymgr,
            peer_pool=self._peer_pool,
            heartbeats=heartbeats,
            antientropy=self._antientropy_info,
            local_member=self._lifecycle.local_member,
        )
        self._ae_inspector = AntiEntropyInspector(
            node_id=self._config.node.id,
            antientropy=self._lifecycle.antientropy,
            total_groups=self._partitioner.total_groups,
            topologymgr=self._topologymgr,
            members=self._inspector,
        )
        self._member_inspector = MemberInspector(
            node_id=config.node.id,
            replication_factor=config.placement.replication_factor,
            partitioner=self._partitioner,
            topologymgr=self._topologymgr,
            members=self._inspector,
            peer_pool=self._peer_pool,
            serializer=serializer,
        )
        self._report = NodeReport(
            inspector=self._inspector,
            ae_inspector=self._ae_inspector,
            antientropy=self._lifecycle.antientropy,
            storage=self._storage,
            total_groups=self._partitioner.total_groups,
            handshakes=self._handshakes,
            certificate_expires_at=certificate_expires_at,
        )
        self._gossip = GossipService(
            node_id=self._config.node.id,
            topologymgr=self._topologymgr,
            relay=self._lifecycle.gossiper.relay,
            heartbeats=heartbeats,
        )
        self._serializer = serializer

    @property
    def node_id(self) -> str:
        """Get the node identifier from config."""
        return self._config.node.id

    @property
    def seeds(self) -> list[str] | None:
        """Get the seed addresses from config."""
        return self._config.node.seeds

    @property
    def serializer(self) -> Serializer:
        """Get the serializer."""
        return self._serializer

    @property
    def gossip(self) -> GossipService:
        """Get the gossip service."""
        return self._gossip

    @property
    def telemetry(self) -> Telemetry:
        """Get the manager-owned telemetry instance."""
        return self._telemetry

    @property
    def store(self) -> StoreService:
        """Get the store service."""
        return self._store

    @property
    def antientropy(self) -> AntiEntropyService:
        """Get the data anti-entropy service."""
        return self._lifecycle.antientropy

    @property
    def gc(self) -> GarbageCollector:
        return self._gc

    async def start(self, seeds: list[str] | None = None) -> None:
        """Start the Tourillon daemon.

        :param seeds: Optional list of seed peer addresses for joining an existing cluster.
        """
        logger.debug("Starting manager for node '%s'", self.node_id)
        if seeds:
            logger.debug("Startup seeds override provided: %s", seeds)
        try:
            latest = await self._storage.latest_hlc(self._partitioner.total_groups)
        except Exception:
            logger.exception(
                "Failed to load latest HLC from storage for node '%s'", self.node_id
            )
            raise
        # A restored clock matters when diagnosing ordering after a restart;
        # a fresh one is the normal first boot.
        logger.log(
            logging.INFO if latest.wall_ms else logging.DEBUG,
            "HLC restored wall_ms=%s counter=%s source_node=%s",
            latest.wall_ms,
            latest.counter,
            latest.node_id or "-",
        )
        self._clock.restore(latest.wall_ms, latest.counter)
        await self._gc.start()
        await self._lifecycle.start(seeds=seeds)
        logger.debug("Manager started for node '%s'", self.node_id)

    async def stop(self) -> None:
        logger.info("Node '%s' shutting down", self.node_id)
        for step in (
            self._lifecycle.stop,
            self._gc.stop,
            # Detached replications must settle while the peer pool and the
            # storage they depend on are still open.
            self._store.close,
            self._peer_pool.shutdown,
            self._storage.close,
            self._telemetry.close,
        ):
            try:
                await step()
            except Exception as exc:
                logger.exception(
                    "shutdown step failed: %s, reason=%s", step.__qualname__, exc
                )
        logger.info("Node '%s' stopped", self.node_id)

    async def serve_forever(self, seeds: list[str] | None = None) -> None:
        """Start and serve the peer server indefinitely.

        :param seeds: Optional list of seed peer addresses for joining.
        """
        stop_requested = asyncio.Event()
        self._install_signal_handlers(stop_requested)
        try:
            await self.start(seeds=seeds)
            logger.info("Node '%s' started; serving until stopped", self.node_id)
            try:
                await stop_requested.wait()
            except KeyboardInterrupt:
                logger.info(
                    "Keyboard interrupt received; shutting down node '%s'", self.node_id
                )
        finally:
            await self.stop()

    @staticmethod
    def _install_signal_handlers(event: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, event.set)

    async def inspect(self) -> NodeStatus:
        return await self._report.status()

    async def health(self) -> NodeHealth:
        return await self._report.health()

    async def members(self, request: MembersRequest) -> MembersPage:
        return await self._inspector.members(request)

    async def ae_status(self, request: AeStatusRequest) -> AeStatus:
        return await self._ae_inspector.status(request)

    async def ae_obligations(self, request: ObligationsRequest) -> ObligationsPage:
        return await self._ae_inspector.obligations(request)

    async def member(self, node_id: str) -> MemberReport:
        return await self._member_inspector.inspect(node_id)

    def _antientropy_info(self) -> AntiEntropyInfo:
        stats = self._lifecycle.antientropy.stats
        queue = self._obligations.to_dict()
        return AntiEntropyInfo(
            queued=int(queue["queued"]),
            inflight=int(queue["inflight"]),
            backlog=bool(queue["backlog"]),
            sweeps=stats.sweeps,
            last_sweep_at=stats.last_sweep_at,
            last_sweep_diverged=stats.last_sweep_diverged,
            delivered=stats.obligations_delivered,
            errors=stats.errors,
            last_failure=stats.last_failure,
        )

    async def join(self, seeds: list[str] | None = None) -> bool:
        return await self._lifecycle.join(seeds=seeds)

    async def cancel_join(self) -> NodeState:
        """Cancel an active join and leave the node out of placement."""
        return await self._lifecycle.cancel_join()

    async def drain(self) -> None:
        await self._lifecycle.drain()

    async def cancel_drain(self) -> NodeState:
        """Cancel an active drain and restore service in the READY phase."""
        return await self._lifecycle.cancel_drain()

    async def pause(self) -> None:
        await self._lifecycle.pause()

    async def resume(self) -> None:
        await self._lifecycle.resume()
