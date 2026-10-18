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
import signal
import ssl
from collections.abc import AsyncIterator

from tourillon.core.config import TourillonConfig
from tourillon.core.gossip.gossiper import GossipStats
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.log import logger
from tourillon.core.models.clock import HLCClock
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.partition.placement import SimplePlacementStrategy
from tourillon.core.peer.pool import PeerPool
from tourillon.core.ports import Telemetry, TelemetryProvider
from tourillon.core.ports.storage import StorageProvider
from tourillon.core.probe.detector import FailureDetectorProvider
from tourillon.core.services.gossip import GossipService
from tourillon.core.services.inspector import NodeInspector
from tourillon.core.services.lifecycle import NodeLifecycle
from tourillon.core.services.store import StoreService
from tourillon.core.services.transfer import TransferService
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.transports.dispatcher import Dispatcher
from tourlib.models.inspect import NodeInfo, NodeInspectRequest
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
    ) -> None:
        self._config = config
        self._partitioner = Partitioner(
            hashspace=HashSpace(bits=128),
            pshift=17,
            gshift=2
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
            idle_ttl=config.transport.pool.idle_ttl,
        )
        self._placement_strategy = SimplePlacementStrategy(
            replication_factor=config.placement.replication_factor,
            topologymgr=self._topologymgr,
            partitioner=self._partitioner,
            peer_pool=self._peer_pool
        )
        self._store = StoreService(
            node_id=config.node.id,
            storage=self._storage,
            partitioner=self._partitioner,
            placement_strategy=self._placement_strategy,
            clock=self._clock,
            serializer=serializer,
            peer_pool=self._peer_pool,
            read_repair=config.placement.read_repair,
            repair=config.placement.repair,
        )
        self._lifecycle = NodeLifecycle(
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
        )
        self._inspector = NodeInspector(
            node_id=self._config.node.id,
            stats=self._gossip_stats,
            partitioner=self._partitioner,
            topologymgr=self._topologymgr,
            peer_pool=self._peer_pool,
        )
        self._gossip = GossipService(
            node_id=self._config.node.id,
            topologymgr=self._topologymgr,
        )
        self._transfer = TransferService(
            topologymgr=self._topologymgr,
            serializer=serializer,
            storage=self._storage,
            partitioner=self._partitioner
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
    def transfer(self) -> TransferService:
        """Get the transfer service."""
        return self._transfer

    @property
    def telemetry(self) -> Telemetry:
        """Get the manager-owned telemetry instance."""
        return self._telemetry

    @property
    def store(self) -> StoreService:
        """Get the store service."""
        return self._store

    async def start(self, seeds: list[str] | None = None) -> None:
        """Start the Tourillon daemon.

        :param seeds: Optional list of seed peer addresses for joining an existing cluster.
        """
        logger.info("Starting manager for node '%s'", self.node_id)
        if seeds:
            logger.debug("Startup seeds override provided: %s", seeds)
        try:
            latest = await self._storage.latest_hlc(self._partitioner.total_groups)
        except Exception:
            logger.exception("Failed to load latest HLC from storage for node '%s'", self.node_id)
            raise
        logger.info(
            "Latest persisted HLC for node '%s': wall_ms=%s counter=%s source_node=%s",
            self.node_id,
            latest.wall_ms,
            latest.counter,
            latest.node_id,
        )
        self._clock.restore(latest.wall_ms, latest.counter)
        await self._lifecycle.start(seeds=seeds)
        logger.info("Manager started for node '%s'", self.node_id)

    async def stop(self) -> None:
        logger.info("Stopping manager for node '%s'", self.node_id)
        for step in (
            self._lifecycle.stop,
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
                logger.exception("shutdown step failed: %s, reason=%s", step.__qualname__, exc)
        logger.info("Manager stopped for node '%s'", self.node_id)

    async def serve_forever(self, seeds: list[str] | None = None) -> None:
        """Start and serve the peer server indefinitely.

        :param seeds: Optional list of seed peer addresses for joining.
        """
        stop_requested = asyncio.Event()
        self._install_signal_handlers(stop_requested)
        try:
            await self.start(seeds=seeds)
            logger.info("Node '%s' is serving forever", self.node_id)
            try:
                await stop_requested.wait()
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received; shutting down node '%s'", self.node_id)
        finally:
            await self.stop()

    @staticmethod
    def _install_signal_handlers(event: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, event.set)

    async def inspect(self, request: NodeInspectRequest) -> AsyncIterator[NodeInfo]:
        async for info in self._inspector.inspect(request):
            yield info

    async def join(self, seeds: list[str] | None = None) -> None:
        await self._lifecycle.join(seeds=seeds)
