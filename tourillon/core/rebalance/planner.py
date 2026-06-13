from tourillon.core.rebalance.transfer import RangeTransfer
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.vnode import VNode


class RebalancePlanner:
    def __init__(self, partitioner: Partitioner, node_id: str, replication_factor: int):
        self._partitioner = partitioner
        self._node_id = node_id
        self._rf = replication_factor

    def plan(self, old_ring: Ring, new_ring: Ring) -> list[RangeTransfer]:
        ranges = []
        news = list(self._partitioner.ranges_for(new_ring))
        olds = list(self._partitioner.ranges_for(old_ring))

        for new in news:
            for old in olds:
                inter = old.intersection(new)
                if inter is None:
                    continue

                old_replicas = self._replica_set(old_ring, old.owner)
                new_replicas = self._replica_set(new_ring, new.owner)
                if old_replicas != new_replicas:
                    start, end = inter
                    ranges.extend(self._pair_transfers(
                        start_pid=start,
                        end_pid=end,
                        old_replicas=old_replicas,
                        new_replicas=new_replicas,
                    ))

        return ranges

    @staticmethod
    def _min_eligible_source(old_replicas: frozenset[str]) -> str | None:
        """Return the lexicographically smallest non-FAILED non-PAUSED node_id.

        Since the planner has no registry access, it cannot inspect phases. The
        caller passes old_replicas as node_ids only. The planner trusts that the
        topology is correct and returns min(old_replicas) as the deterministic
        source. In practice, FAILED/PAUSED filtering is done by the applicator
        when it knows the registry; here we return min unconditionally for
        topology-only determinism.

        Returns None when old_replicas is empty.
        """
        if not old_replicas:
            return None
        return min(old_replicas)

    def _pair_transfers(
        self,
        start_pid: int,
        end_pid: int,
        old_replicas: frozenset[str],
        new_replicas: frozenset[str],
    ) -> list[RangeTransfer]:
        result: list[RangeTransfer] = []

        leaving = sorted(old_replicas - new_replicas)
        entering = sorted(new_replicas - old_replicas)
        if not entering:
            return []

        count = self._partitioner.range_size(start_pid, end_pid)

        for src, dst in zip(leaving, entering, strict=False):
            transfer = RangeTransfer(
                start_pid=start_pid,
                end_pid=end_pid,
                count=count,
                src=src,
                dst=dst
            )
            result.append(transfer)

        unpaired = entering[len(leaving) :]
        if not unpaired:
            return result

        src = self._min_eligible_source(old_replicas)
        if src is None:
            return result

        for dst in unpaired:
            transfer = RangeTransfer(
                start_pid=start_pid, end_pid=end_pid, count=count, src=src, dst=dst
            )
            result.append(transfer)

        return result

    def _replica_set(self, ring: Ring, vnode: VNode) -> frozenset[str]:
        seen = []
        for v in ring.iter_from(vnode):
            if v.node_id not in seen:
                seen.append(v.node_id)
                if len(seen) == self._rf:
                    break
        return frozenset(seen)
