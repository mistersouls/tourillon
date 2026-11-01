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

"""Bounded in-memory schedule of durable replication obligations.

An obligation states that one node must eventually hold one exact version.
Producers (the write path when a replica misses a write or is replaced by a
handoff, read repair when a replica is behind) persist it in META first and
only then submit it here. The queue therefore never owns an obligation: it
only decides *when* anti-entropy delivers it.

The queue is bounded. An obligation that does not fit is not lost, it stays
in META and the ``backlog`` flag tells the delivery worker to reload META
once the queue drains. The flag starts raised so obligations left by a
previous run are reloaded at startup.
"""

import asyncio
from collections import OrderedDict
from collections.abc import Iterable

from tourillon.core.models.storageschema import ObligationKey

DEFAULT_HOTQUEUE_SIZE = 4096


class ObligationQueue:
    def __init__(self, capacity: int = DEFAULT_HOTQUEUE_SIZE) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")

        self._capacity = capacity
        self._items: OrderedDict[ObligationKey, None] = OrderedDict()
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

    def __len__(self) -> int:
        return len(self._items)

    @property
    def idle(self) -> bool:
        """No obligation is queued or being delivered."""
        return not self._items and not self._inflight

    def submit(self, obligations: Iterable[ObligationKey]) -> int:
        """Schedule persisted obligations; return how many entered the queue.

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

    def take(self, limit: int) -> list[ObligationKey]:
        """Pop up to ``limit`` obligations in submission order.

        The caller must hand the count back through ``settle`` once the
        batch is delivered or abandoned.
        """
        batch: list[ObligationKey] = []

        while self._items and len(batch) < limit:
            obligation, _ = self._items.popitem(last=False)
            batch.append(obligation)

        self._inflight += len(batch)

        if not self._items:
            self._wake.clear()

        return batch

    def settle(self, count: int) -> None:
        self._inflight = max(0, self._inflight - count)
        if self.idle:
            self._idle.set()

    async def wait(self, timeout: float | None = None) -> None:
        """Wait until obligations are submitted, or ``timeout`` elapses."""
        if self._items:
            return

        try:
            await asyncio.wait_for(self._wake.wait(), timeout)
        except TimeoutError:
            return

    async def wait_idle(self) -> None:
        """Wait until nothing is queued or in delivery."""
        await self._idle.wait()

    def to_dict(self) -> dict[str, int | bool]:
        return {
            "queued": len(self._items),
            "inflight": self._inflight,
            "capacity": self._capacity,
            "backlog": self.backlog,
            "submitted": self.submitted,
            "overflowed": self.overflowed,
        }
