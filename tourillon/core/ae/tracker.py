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
        partitioner: Partitioner,
        topologymgr: TopologyManager,
        replication_factor: int,
        settle_delay: float,
        settle_quiet: float = 0.5
    ) -> None:
        self._node_id = node_id
        self._partitioner = partitioner
        self._topologymgr = topologymgr
        self._rf = replication_factor
        self._settle_delay = settle_delay
        self._settle_quiet = settle_quiet

        self._view: ReplicaView | None = None
        self._ready = asyncio.Event()
        self._stale = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="ae-view")

    async def stop(self) -> None:
        task, self._task = self._task, None

        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._invalidate()

    @property
    def view(self) -> ReplicaView | None:
        return self._view

    @property
    def settle_delay(self) -> float:
        return self._settle_delay

    @property
    def settle_quiet(self) -> float:
        return self._settle_quiet

    def stale_signal(self) -> asyncio.Event:
        return self._stale

    async def wait_ready(self) -> ReplicaView:
        while self._view is None:
            await self._ready.wait()
        return self._view

    def _invalidate(self) -> None:
        self._view = None
        self._ready.clear()
        self._stale.set()

    async def _rebuild(self) -> int:
        generation = self._topologymgr.generation
        topology = await self._topologymgr.snapshot()
        view: ReplicaView = await asyncio.to_thread(
            build_view, self._node_id, topology, self._partitioner, self._rf
        )

        self._stale = asyncio.Event()
        self._view = view
        self._ready.set()

        logger.info("ae.view rebuilt serial=%.12s peers=%d", view.serial, len(view.shared))

        return generation

    async def _run(self) -> None:
        generation = await self._rebuild()
        loop = asyncio.get_running_loop()

        while True:
            generation = await self._topologymgr.wait_for_change(generation)
            self._invalidate()
            logger.info(
                "ae.view invalidated generation=%d quiet=%.2fs max=%.2fs",
                generation, self._settle_quiet, self._settle_delay
            )

            started = loop.time()
            changes, capped = await self._settle(generation)
            generation = await self._rebuild()
            elapsed = loop.time() - started

            if capped:
                logger.warning(
                    "ae.view rebuild under churn after %.2fs changes=%d: "
                    "membership kept changing for settle_delay=%.2fs",
                    elapsed, changes, self._settle_delay
                )
            else:
                logger.info(
                    "ae.view settled after %.2fs changes=%d",
                    elapsed, changes
                )

    async def _settle(self, generation: int) -> tuple[int, bool]:
        """
        Wait for ``settle_quiet`` without change, at most ``settle_delay``.

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
                    self._topologymgr.wait_for_change(generation),
                    timeout=wait
                )
            except asyncio.TimeoutError:
                return changes, wait < self._settle_quiet

            changes += 1
            logger.debug(
                "ae.view settle extended generation=%d changes=%d",
                generation, changes
            )

        return changes, self._settle_delay > 0
