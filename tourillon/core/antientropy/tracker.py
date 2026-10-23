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

"""Keep the replica map in step with the ring.

A ring change invalidates the current view at once: running sessions are
told to stop, and the responder answers ``not_ready`` instead of comparing
partitions whose replicas may just have moved. The map is rebuilt only after
``settle_delay`` without further change, so a burst of membership updates
(a node joining announces several phases in a row) costs one rebuild.
"""

import asyncio
import contextlib

from tourillon.core.antientropy.view import ReplicaView, build_view
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.log import logger
from tourillon.core.partition.partitioner import Partitioner


class ViewTracker:
    def __init__(
        self,
        node_id: str,
        topologymgr: TopologyManager,
        partitioner: Partitioner,
        replication_factor: int,
        settle_delay: float,
    ) -> None:
        self._node_id = node_id
        self._topologymgr = topologymgr
        self._partitioner = partitioner
        self._rf = replication_factor
        self._settle_delay = settle_delay
        self._view: ReplicaView | None = None
        # Set when the current view is invalidated; replaced on each rebuild
        # so every view has its own signal.
        self._stale = asyncio.Event()
        self._ready = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def view(self) -> ReplicaView | None:
        """The current view, or None while the ring is settling."""
        return self._view

    @property
    def settle_delay(self) -> float:
        return self._settle_delay

    def stale_signal(self) -> asyncio.Event:
        """Event set as soon as the view current at call time is invalidated."""
        return self._stale

    async def wait_ready(self) -> ReplicaView:
        while self._view is None:
            await self._ready.wait()
        return self._view

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="antientropy-view")

    async def stop(self) -> None:
        task, self._task = self._task, None

        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._invalidate()

    async def _run(self) -> None:
        serial = await self._rebuild()
        while True:
            await self._topologymgr.wait_for_ring_change(serial)
            self._invalidate()
            await asyncio.sleep(self._settle_delay)
            serial = await self._rebuild()

    def _invalidate(self) -> None:
        self._view = None
        self._ready.clear()
        self._stale.set()

    async def _rebuild(self) -> str:
        topology = await self._topologymgr.snapshot()
        # The ring is immutable: hashing it off-loop is safe and keeps a
        # large ring from stalling client requests.
        serial = await asyncio.to_thread(topology.ring.serial)

        view = await build_view(
            topology, serial, self._partitioner, self._rf, self._node_id
        )

        self._stale = asyncio.Event()
        self._view = view
        self._ready.set()

        logger.info(
            "antientropy.view rebuilt serial=%s peers=%d", serial, len(view.shared)
        )

        return serial
