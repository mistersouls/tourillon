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

from tourillon.core.models.retry import BackoffRetry
from tourillon.core.models.transfer import TransferPlan
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.peer import PeerPool
from tourillon.core.ports.storage import Storage
from tourillon.core.spaces.ring import Ring
from tourillon.core.transfer.applicator import DEFAULT_WINDOW_SIZE, TransferApplicator
from tourillon.core.transfer.planner import TransferPlanner
from tourillon.core.transfer.stream import DEFAULT_CHUNK_SIZE
from tourlib import Serializer


class TransferRunner:
    def __init__(
        self,
        node_id: str,
        replication_factor: int,
        partitioner: Partitioner,
        peer_pool: PeerPool,
        storage: Storage,
        serializer: Serializer,
        max_concurrency: int = 4,
        max_chunk_size: int = DEFAULT_CHUNK_SIZE,
        backoff: BackoffRetry | None = None,
        window_size: int = DEFAULT_WINDOW_SIZE,
    ) -> None:
        self._planner = TransferPlanner(
            node_id=node_id,
            partitioner=partitioner,
            replication_factor=replication_factor,
        )
        self._applicator = TransferApplicator(
            node_id=node_id,
            peer_pool=peer_pool,
            storage=storage,
            serializer=serializer,
            partitioner=partitioner,
            max_concurrency=max_concurrency,
            max_chunk_size=max_chunk_size,
            backoff=backoff,
            window_size=window_size,
        )

    async def apply(self, current: Ring, target: Ring) -> None:
        """Apply the transfer from current to target."""
        plan = await self._plan(current, target)
        await self._applicator.apply(plan)

    async def run(self, current: Ring, target: Ring) -> tuple[list[str], list[str]]:
        """Run the transfer from current to target ring."""
        await self.apply(current, target)
        return await self.wait()

    async def wait(self) -> tuple[list[str], list[str]]:
        """Wait for all transfers to complete and return the results."""
        return await self._applicator.wait()

    async def cancel(self) -> None:
        """Cancel the running transfers (node shutdown); staged data is kept for resumption."""
        await self._applicator.cancel()

    async def _plan(self, current: Ring, target: Ring) -> TransferPlan:
        """Plan the transfer from current to target ring."""
        return await asyncio.to_thread(self._planner.plan, current, target)
