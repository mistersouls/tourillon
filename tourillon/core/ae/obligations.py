import asyncio
from collections import OrderedDict
from itertools import batched
from typing import Iterable

from tourillon.core.models.storageschema import Obligation

DEFAULT_HOTQUEUE_SIZE = 4096


class ObligationQueue:
    def __init__(self, capacity: int = DEFAULT_HOTQUEUE_SIZE) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")

        self._capacity = capacity
        self._items: OrderedDict[Obligation, None] = OrderedDict()
        self._inflight = 0
        self._wake = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self.backlog = True
        self.submitted = 0
        self.overflowed = 0

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def idle(self) -> bool:
        """No obligation is queued or being delivered."""
        return not self._items and not self._inflight

    def settle(self, count: int) -> None:
        self._inflight = max(0, self._inflight - count)
        if self.idle:
            self._idle.set()

    def submit(self, obligations: Iterable[Obligation]) -> int:
        """
        Schedule persisted obligations; return how many entered the queue.

        Resubmitting a queued obligation is a no-op. Overflow raises the
        backlog flag instead of dropping anything durable.
        """
        accepted = 0

        for obligation in obligations:
            if obligation in self._items:
                continue

            if len(self._items) >= self._capacity:
                self.backlog = True
                self.overflowed += 1
                continue

            self._items[obligation] = None
            accepted += 1

        if accepted:
            self.submitted += accepted
            self._idle.clear()
            self._wake.set()

        return accepted

    def take(self, limit: int) -> list[Obligation]:
        batch: list[Obligation] = []

        while self._items and len(batch) < limit:
            obligation, _ = self._items.popitem(last=False)
            batch.append(obligation)

        self._inflight += len(batch)

        if not self._items:
            self._wake.clear()

        return batch

    def to_dict(self) -> dict[str, int | bool]:
        return {
            "queued": len(self._items),
            "inflight": self._inflight,
            "capacity": self._capacity,
            "backlog": self.backlog,
            "submitted": self.submitted,
            "overflow": self.overflowed,
        }

    async def wait(self, timeout: float | None = None) -> None:
        """Wait until obligations are submitted, or ``timeout`` elapses."""
        if self._items:
            return

        try:
            await asyncio.wait_for(self._wake.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return

    async def wait_idle(self) -> None:
        """Wait until nothing is queued or in delivery."""
        await self._idle.wait()

    def __len__(self) -> int:
        return len(self._items)
