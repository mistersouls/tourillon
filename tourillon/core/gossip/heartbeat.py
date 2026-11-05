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
"""Gossip heartbeats: a cluster-wide notion of when a member was last heard.

Each node bumps its own counter once per gossip anti-entropy cycle and ships
the whole ``{node_id: counter}`` table with ``gossip.ping`` and
``gossip.pong``. Tables merge by maximum. When a member's counter grows, the
local monotonic time is recorded: the age of that record is how long ago the
member was last heard of, directly or through any other member.

Counters start from the wall clock in milliseconds, so a restarted node
resumes above the value it had before and is heard again immediately.
"""

import math
import time
from collections.abc import Callable, Iterable, Mapping

from tourillon.core.probe.detector import PeerStatus

# A heartbeat crosses the cluster in about log2(n) gossip rounds; members are
# suspected after that many rounds plus a margin, and dead after three times
# as long.
SUSPECT_MARGIN_ROUNDS = 3
DEAD_FACTOR = 3


class HeartbeatTable:
    def __init__(
        self,
        node_id: str,
        *,
        interval: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        wall: Callable[[], float] = time.time,
    ) -> None:
        if interval <= 0:
            raise ValueError("interval must be > 0")
        self._node_id = node_id
        self._interval = interval
        self._clock = clock
        self._started = clock()
        self._counters: dict[str, int] = {node_id: int(wall() * 1000)}
        self._heard: dict[str, float] = {node_id: self._started}

    @property
    def node_id(self) -> str:
        return self._node_id

    def beat(self) -> int:
        """Advance this node's own counter and return it."""
        counter = self._counters[self._node_id] + 1
        self._counters[self._node_id] = counter
        self._heard[self._node_id] = self._clock()
        return counter

    def table(self) -> dict[str, int]:
        return dict(self._counters)

    def merge(self, table: Mapping[str, int]) -> int:
        """Merge a remote table by maximum; return how many members advanced."""
        now = self._clock()
        advanced = 0
        for node_id, counter in table.items():
            if node_id == self._node_id:
                continue
            if counter <= self._counters.get(node_id, -1):
                continue
            self._counters[node_id] = counter
            self._heard[node_id] = now
            advanced += 1
        return advanced

    def forget(self, keep: Iterable[str]) -> None:
        """Drop the members that are no longer in the registry."""
        known = set(keep) | {self._node_id}
        for node_id in [n for n in self._counters if n not in known]:
            del self._counters[node_id]
            self._heard.pop(node_id, None)

    def age(self, node_id: str, now: float | None = None) -> float | None:
        """Seconds since the member was last heard of; None if never."""
        if node_id == self._node_id:
            return 0.0
        heard = self._heard.get(node_id)
        if heard is None:
            return None
        return max(0.0, (self._clock() if now is None else now) - heard)

    def thresholds(self, members: int) -> tuple[float, float]:
        """Return the (suspect, dead) heartbeat ages for a cluster size."""
        rounds = math.log2(max(2, members)) + SUSPECT_MARGIN_ROUNDS
        suspect = self._interval * rounds
        return suspect, suspect * DEAD_FACTOR

    def status(
        self, node_id: str, members: int, now: float | None = None
    ) -> PeerStatus:
        """Classify a member from its heartbeat age.

        A member never heard of is given the time since this node started:
        right after a start every member is ALIVE until proven silent.
        """
        now = self._clock() if now is None else now
        age = self.age(node_id, now)
        if age is None:
            age = max(0.0, now - self._started)
        suspect, dead = self.thresholds(members)
        if age >= dead:
            return PeerStatus.DEAD
        if age >= suspect:
            return PeerStatus.SUSPECT
        return PeerStatus.ALIVE


def parse_heartbeats(raw: object) -> dict[str, int]:
    """Validate a heartbeat table received over the wire; ignore bad entries."""
    if not isinstance(raw, Mapping):
        return {}
    table: dict[str, int] = {}
    for node_id, counter in raw.items():
        if not isinstance(node_id, str) or not node_id:
            continue
        if not isinstance(counter, int) or isinstance(counter, bool) or counter < 0:
            continue
        table[node_id] = counter
    return table
