import hashlib
from collections.abc import AsyncIterator
from typing import Any

from tourillon.core.exceptions import RebalanceError
from tourillon.core.machinery.namespace import Key, Namespace
from tourillon.core.machinery.state import StatePersistence
from tourillon.core.ports.storage import Storage
from tourillon.core.rebalance.planner import RebalancePlanner
from tourillon.core.rebalance.transfer import RangeTransfer, PartitionTransfer
from tourillon.core.ring.partitioner import Partitioner, PartitionRange
from tourillon.core.ring.placement import PlacementStrategy
from tourillon.core.ring.topology import TopologyManager


class NodeRebalancer:
    def __init__(
        self,
        node_id: str,
        state_persistence: StatePersistence,
        topology_mgr: TopologyManager,
        partitioner: Partitioner,
        storage: Storage
    ) -> None:
        self._node_id = node_id
        self._state_persistence = state_persistence
        self._topology_mgr = topology_mgr
        self._partitioner = partitioner
        self._storage = storage
        self._digests: dict[str, Any] = {}

    async def accept_plan(self, payload: dict[str, Any]) -> dict[str, Any]:
        snapshot = await self._topology_mgr.snapshot()
        if snapshot.epoch != payload['epoch']:
            raise RebalanceError(
                "epoch_mismatch",
                {"expected": payload['epoch'], "actual": snapshot.epoch}
            )

        ingoing: list[RangeTransfer] = []
        outgoing: list[RangeTransfer] = []

        for r_transfer_dict in payload['transfers']:
            if r_transfer_dict["dst"] == self._node_id:
                ingoing.append(RangeTransfer(**r_transfer_dict))
            elif r_transfer_dict["src"] == self._node_id:
                outgoing.append(RangeTransfer(**r_transfer_dict))
            else:
                raise RebalanceError(
                    "transfer_mismatch",
                    {"suspected": r_transfer_dict, "node_id": self._node_id}
                )

        return {"ingoing": ingoing, "outgoing": outgoing, "epoch": snapshot.epoch}

    async def resume(self, epoch: int, range_transfer: RangeTransfer) -> AsyncIterator[dict[str, Any]]:
        for pid in range_transfer.pids(self._partitioner.total_partitions):
            transfer = PartitionTransfer(
                pid=pid,
                src=range_transfer.src,
                dst=range_transfer.dst,
            )
            store = await self._storage.open_by_pid(pid)
            staging = store.staging(epoch)
            resume_from = await staging.last_staged_key()
            yield {
                "transfer_id": transfer.id,
                "resume_from": resume_from.to_dict() if resume_from else None,
            }

    async def transfer(self, payload: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
        pid = payload['pid']
        transfer_id = payload['transfer_id']
        epoch = payload["epoch"]
        resume_from = Key.from_dict(payload['resume_from']) if payload.get('resume_from') else None
        store = await self._storage.open_by_pid(pid)
        digest = self._digests[transfer_id] = hashlib.sha256()

        async for tagged in store.scan(resume_from=resume_from):
            digest.update(tagged.to_bytes(Namespace.TAGS))
            yield {"transfer_id": transfer_id, "records": [tagged.to_dict()], "epoch": epoch}

        yield {"transfer_id": transfer_id, "records": [], "is_last": True, "epoch": epoch}

    async def commit(self, payload: dict[str, Any]) -> dict[str, Any]:
        transfer_id = payload["transfer_id"]
        epoch = payload["epoch"]
        actual_digest = payload["digest"]

        digest = self._digests.pop(transfer_id, hashlib.sha256())
        if actual_digest != digest.hexdigest():
            raise RebalanceError(
                "digest_mismatch",
                {"expected": digest.hexdigest(), "actual": actual_digest, "transfer_id": transfer_id, "epoch": epoch}
            )
        return {"epoch": epoch, "transfer_id": transfer_id}
