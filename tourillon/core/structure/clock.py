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
"""HLCTimestamp and HLCClock — hybrid logical clock for Tourillon records."""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class HLCTimestamp:
    """Immutable hybrid logical clock timestamp.

    Total order: (wall_ms, counter, node_id) — lexicographic. Two timestamps
    from different nodes that share the same (wall_ms, counter) are still
    distinct because node_id is included. The ordering is preserved when
    serialising to bytes for use as ordered index keys.

    Advance timestamps by calling HLCClock.tick() or HLCClock.update(); never
    construct HLCTimestamp directly in application code.
    """

    wall_ms: int
    counter: int
    node_id: str

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serialisable dict for wire encoding."""
        return {
            "wall_ms": self.wall_ms,
            "counter": self.counter,
            "node_id": self.node_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> HLCTimestamp:
        """Reconstruct an HLCTimestamp from its wire dict representation.

        Accepts both ``wall_ms`` (current format) and ``wall`` (legacy alias)
        so that records serialised before the rename can still be decoded.
        """
        wall_raw = data.get("wall_ms") or data.get("wall")
        return cls(
            wall_ms=int(wall_raw),  # type: ignore[arg-type]
            counter=int(data["counter"]),  # type: ignore[arg-type]
            node_id=str(data["node_id"]),
        )


class HLCClock:
    """Mutable stateful wrapper that advances an HLCTimestamp monotonically.

    tick() generates a new timestamp for a local event. update() advances the
    clock when a remote timestamp is observed (causality merge). Both methods
    guarantee that the returned timestamp is strictly greater than any
    previously returned timestamp.

    This class is NOT thread-safe or coroutine-safe; callers are responsible
    for serialising access. For concurrent usage, wrap calls in an asyncio.Lock.
    """

    def __init__(self, node_id: str) -> None:
        self._node_id = node_id
        self._wall_ms: int = 0
        self._counter: int = 0

    def tick(self) -> HLCTimestamp:
        """Return a new timestamp for a local write event.

        The returned timestamp is strictly greater than any previously returned
        timestamp from this clock instance.
        """
        now = int(time.time() * 1000)
        if now > self._wall_ms:
            self._wall_ms = now
            self._counter = 0
        else:
            self._counter += 1
        return HLCTimestamp(
            wall_ms=self._wall_ms,
            counter=self._counter,
            node_id=self._node_id,
        )

    def restore(self, wall_ms: int, counter: int) -> None:
        """Restore lower bounds from persisted state.

        Called once at startup before any tick() or update(). Guarantees that
        no future timestamp can collide with a timestamp emitted before the
        last checkpoint. If the provided wall_ms equals the current _wall_ms,
        the counter is raised to the persisted value so that the next tick()
        starts above the highest previously emitted counter.
        """
        if wall_ms > self._wall_ms:
            self._wall_ms = wall_ms
            self._counter = counter
        elif wall_ms == self._wall_ms:
            self._counter = max(self._counter, counter)

    def snapshot(self) -> tuple[int, int]:
        """Return (wall_ms, counter) for checkpointing to persistent storage."""
        return (self._wall_ms, self._counter)

    def update(self, remote: HLCTimestamp) -> HLCTimestamp:
        """Merge a remote timestamp and return the updated local timestamp.

        Advances the local clock to be strictly after both the current local
        time and the remote timestamp, preserving causality.
        """
        now = int(time.time() * 1000)
        max_wall = max(now, remote.wall_ms, self._wall_ms)
        if max_wall == self._wall_ms and max_wall == remote.wall_ms:
            self._counter = max(self._counter, remote.counter) + 1
        elif max_wall == self._wall_ms:
            self._counter += 1
        elif max_wall == remote.wall_ms:
            self._counter = remote.counter + 1
        else:
            self._counter = 0
        self._wall_ms = max_wall
        return HLCTimestamp(
            wall_ms=self._wall_ms,
            counter=self._counter,
            node_id=self._node_id,
        )
