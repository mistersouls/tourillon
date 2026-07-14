import logging

from tourillon.core.ports.storage import Storage
from tourillon.core.rebalance.applicator import RebalanceApplicator
from tourillon.core.rebalance.planner import RebalancePlanner
from tourillon.core.rebalance.transfer import RebalancePlan
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.transport.client import PeerClientPool
from tourlib.ports.serializer import Serializer

logger = logging.getLogger(__name__)
MAX_FAILED_TRANSFERS_TO_LOG = 10


class Rebalancer:
    def __init__(
        self,
        node_id: str,
        replication_factor: int,
        partitioner: Partitioner,
        storage: Storage,
        peer_pool: PeerClientPool,
        serializer: Serializer,
        topology_mgr: TopologyManager,
    ) -> None:
        self._node_id = node_id
        self._replication_factor = replication_factor
        self._partitioner = partitioner
        self._storage = storage
        self._peer_pool = peer_pool
        self._serializer = serializer
        self._planner = RebalancePlanner(
            partitioner=self._partitioner,
            node_id=self._node_id,
            replication_factor=self._replication_factor,
        )
        self._applicator = RebalanceApplicator(
            node_id=self._node_id,
            total_partitions=self._partitioner.total_partitions,
            pool=self._peer_pool,
            serializer=self._serializer,
            storage=self._storage,
            topology_mgr=topology_mgr,
        )

    async def rebalance(
        self, old_ring: Ring, new_ring: Ring, epoch: int, *, wait: bool = False
    ) -> bool:
        ranges = self._planner.plan(old_ring, new_ring)
        plan = RebalancePlan(ranges=tuple(ranges), epoch=epoch)
        await self._applicator.apply(plan)
        if wait:
            return await self._wait(epoch)
        return True

    async def _wait(self, epoch: int) -> bool:
        success, failed = await self._applicator.wait()

        if len(failed) > 0:
            logged_failures = failed[:MAX_FAILED_TRANSFERS_TO_LOG]
            remaining_failures = len(failed) - len(logged_failures)
            if remaining_failures > 0:
                logger.warning(
                    "Rebalance failed for some transfers: %s ... (%d more) (epoch=%d)",
                    logged_failures,
                    remaining_failures,
                    epoch,
                )
            else:
                logger.warning(
                    "Rebalance failed for some transfers: %s (epoch=%d)",
                    logged_failures,
                    epoch,
                )
            return False

        logger.info(
            "Rebalance completed: %d transfers succeeded, %d failed (epoch=%d)",
            len(success),
            len(failed),
            epoch,
        )
        return True
