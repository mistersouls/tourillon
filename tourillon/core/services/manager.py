import logging
from typing import Any

from tourillon.core.machinery.state import StatePersistence
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
    ) -> None:
        self._cfg = cfg
        self._peer_dispatcher = peer_dispatcher
        self._kv_dispatcher = kv_dispatcher
        self._tls_ctx = tls_ctx
        self._state = state
        self._serializer = serializer
        self._topology_manager = TopologyManager()

        self._starter = NodeStarter(
            cfg=cfg,
            peer_dispatcher=peer_dispatcher,
            kv_dispatcher=kv_dispatcher,
            tls_ctx=tls_ctx,
            state=state,
            serializer=serializer,
            topology=self._topology_manager
        )
        self._drainer = NodeDrainer(state)
        self._gossiper = Gossiper(
            node_id=cfg.node_id,
            partition_shift=cfg.partition_shift,
            topology_manager=self._topology_manager,
            serializer=serializer,
            max_digest_entries=cfg.gossip.max_digest_entries
        )
        self._rebalancer = NodeRebalancer(state)

    async def start(self, stop_event=None, seeds=None):
        return await self._starter.start(stop_event=stop_event, seeds=seeds)

    async def join(self, seeds_override: list[str] | None = None):
        return await self._starter.join(seeds_override)

    async def drain(self):
        return await self._drainer.drain()

    async def rebalance(self):
        return await self._rebalancer.rebalance()

    async def stop(self):
        return await self._starter.stop_all()

    async def handle_gossip_push(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._gossiper.update_memberships(payload)

    async def handle_gossip_ping(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._gossiper.get_memberships_state(payload)

    async def handle_gossip_digest(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._gossiper.diff(payload)
