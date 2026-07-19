import logging
from typing import Any, AsyncIterator

from tourillon.core.machinery.state import StatePersistence
from tourillon.core.ports.storage import Storage
from tourillon.core.rebalance.transfer import RangeTransfer
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.services.drainer import NodeDrainer
from tourillon.core.services.gossiper import Gossiper
from tourillon.core.services.rebalancer import NodeRebalancer
from tourillon.core.services.starter import NodeStarter
from tourillon.core.structure.config import TourillonConfig
from tourillon.core.transport.dispatcher import Dispatcher
from tourlib.ports.serializer import Serializer
from tourlib.ports.tls import TlsContext

logger = logging.getLogger(__name__)


class NodeManager:
    def __init__(
        self,
        cfg: TourillonConfig,
        peer_dispatcher: Dispatcher,
        kv_dispatcher: Dispatcher,
        tls_ctx: TlsContext,
        state: StatePersistence,
        serializer: Serializer,
        partitioner: Partitioner,
        storage: Storage,
    ) -> None:
        self._cfg = cfg
        self._peer_dispatcher = peer_dispatcher
        self._kv_dispatcher = kv_dispatcher
        self._tls_ctx = tls_ctx
        self._state = state
        self._serializer = serializer
        self._topology_manager = TopologyManager()
        self._partitioner = partitioner

        self._starter = NodeStarter(
            cfg=cfg,
            peer_dispatcher=peer_dispatcher,
            kv_dispatcher=kv_dispatcher,
            tls_ctx=tls_ctx,
            state=state,
            serializer=serializer,
            topology=self._topology_manager,
            partitioner=self._partitioner,
            storage=storage,
        )
        self._drainer = NodeDrainer(state)
        self._gossiper = Gossiper(
            node_id=cfg.node_id,
            partition_shift=cfg.partition_shift,
            topology_manager=self._topology_manager,
            serializer=serializer,
            max_digest_entries=cfg.gossip.max_digest_entries
        )
        self._rebalancer = NodeRebalancer(
            node_id=cfg.node_id,
            state_persistence=state,
            topology_mgr=self._topology_manager,
            partitioner=self._partitioner,
            storage=storage
        )

    async def start(self, stop_event=None, seeds=None) -> None:
        return await self._starter.start(stop_event=stop_event, seeds=seeds)

    async def join(self, seeds_override: list[str] | None = None) -> dict[str, Any]:
        return await self._starter.join(seeds_override)

    async def drain(self) -> None:
        return await self._drainer.drain()

    async def stop(self) -> None:
        return await self._starter.stop_all()

    async def accept_plan(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._rebalancer.accept_plan(payload)

    async def resume_transfer(
        self,
        epoch: int,
        range_transfer: RangeTransfer
    ) -> AsyncIterator[dict[str, Any]]:
        async for resum_payload in self._rebalancer.resume(epoch, range_transfer):
            yield resum_payload

    async def transfer(self, payload: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
        async for transfer_payload in self._rebalancer.transfer(payload):
            yield transfer_payload

    async def commit_transfer(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._rebalancer.commit(payload)

    async def handle_gossip_push(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._gossiper.update_memberships(payload)

    async def handle_gossip_ping(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._gossiper.get_memberships_state(payload)

    async def handle_gossip_digest(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._gossiper.diff(payload)
