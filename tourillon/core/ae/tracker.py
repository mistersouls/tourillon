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

"""Keep the replica map in step with the ring and its owners' phases.

A ring change, or a ring member changing phase (a JOINING node becoming
READY, a READY one starting to drain), invalidates the current view at once: running sessions are
told to stop, and the responder answers ``not_ready`` instead of comparing
partitions whose replicas may just have moved. The map is rebuilt once the
membership stays quiet for ``settle_quiet``, each further change restarting
that wait, and at the latest ``settle_delay`` after the first change. A burst
of membership updates (a node joining announces several phases in a row)
costs one rebuild, an isolated change is picked up almost at once, and
continuous churn still gets a fresh view.
"""

import asyncio
import contextlib

from tourillon.core.ae.view import ReplicaView, build_view
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
        settle_quiet: float = 0.5,
    ) -> None:
        self._node_id = node_id
        self._topologymgr = topologymgr
        self._partitioner = partitioner
        self._rf = replication_factor
        self._settle_delay = settle_delay
        self._settle_quiet = min(settle_quiet, settle_delay)
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

    @property
    def settle_quiet(self) -> float:
        """Usual wait for a rebuild; the retry hint of refused sessions."""
        return self._settle_quiet

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
        generation = await self._rebuild()
        loop = asyncio.get_running_loop()

        while True:
            generation = await self._topologymgr.wait_for_change(generation)
            self._invalidate()
            logger.info(
                "antientropy.view invalidated generation=%d quiet=%.2fs max=%.2fs",
                generation,
                self._settle_quiet,
                self._settle_delay,
            )

            started = loop.time()
            changes, capped = await self._settle(generation)
            generation = await self._rebuild()
            self._log_settled(loop.time() - started, changes, capped)

    async def _settle(self, generation: int) -> tuple[int, bool]:
        """Wait for ``settle_quiet`` without change, at most ``settle_delay``.

        Returns the number of changes coalesced into the coming rebuild and
        whether the wait was cut short by ``settle_delay``.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._settle_delay
        changes = 1

        while (remaining := deadline - loop.time()) > 0:
            wait = min(self._settle_quiet, remaining)
            try:
                generation = await asyncio.wait_for(
                    self._topologymgr.wait_for_change(generation), wait
                )
            except TimeoutError:
                # A wait shortened by the deadline ends at the cap, not in quiet.
                return changes, wait < self._settle_quiet

            changes += 1
            logger.debug(
                "antientropy.view settle extended generation=%d changes=%d",
                generation,
                changes,
            )

        return changes, self._settle_delay > 0

    def _log_settled(self, elapsed: float, changes: int, capped: bool) -> None:
        if capped:
            logger.warning(
                "antientropy.view rebuilt under churn after %.2fs changes=%d: "
                "membership kept changing for settle_delay",
                elapsed,
                changes,
            )
            return

        logger.info("antientropy.view settled after %.2fs changes=%d", elapsed, changes)

    def _invalidate(self) -> None:
        self._view = None
        self._ready.clear()
        self._stale.set()

    async def _rebuild(self) -> int:
        generation = self._topologymgr.generation
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

        return generation
