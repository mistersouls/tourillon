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

import time

from tourlib.models.ts import HLCTimestamp
from tourlib.validation import validate_node_id_size


class HLCClock:
    """
    Mutable stateful wrapper that advances an HLCTimestamp monotonically.

    tick() generates a new timestamp for a local event. update() advances the
    clock when a remote timestamp is observed (causality merge). Both methods
    guarantee that the returned timestamp is strictly greater than any
    previously returned timestamp.
    """

    def __init__(self, node_id: str) -> None:
        validate_node_id_size(node_id)
        self.node_id = node_id
        self._wall_ms = 0
        self._counter = 0

    def tick(self) -> HLCTimestamp:
        """
        Return a new timestamp for a local write event.

        The returned timestamp is strictly greater than any previously returned
        timestamp from this clock instance.
        """
        now_ms = int(time.time() * 1000)
        if now_ms > self._wall_ms:
            self._wall_ms = now_ms
            self._counter = 0
        else:
            self._counter += 1
        return HLCTimestamp(self._wall_ms, self._counter, self.node_id)

    def restore(self, wall_ms: int, counter: int) -> None:
        """
        Restore the clock state from a previously persisted timestamp.

        This method is used during node startup to ensure that the clock
        continues from the last known state, preventing time regressions.
        """
        if wall_ms > self._wall_ms:
            self._wall_ms = wall_ms
            self._counter = counter
        elif wall_ms == self._wall_ms:
            self._counter = max(self._counter, counter)

    def snapshot(self) -> HLCTimestamp:
        """
        Return the current timestamp without advancing the clock.

        This is useful for observing the current state of the clock without
        generating a new event timestamp.
        """
        return HLCTimestamp(self._wall_ms, self._counter, self.node_id)

    def update(self, remote: HLCTimestamp) -> HLCTimestamp:
        """
        Update the clock with a remote timestamp and return a new timestamp.

        The returned timestamp is strictly greater than both the local clock
        and the provided remote timestamp, ensuring causality is preserved.
        """
        now_ms = int(time.time() * 1000)
        max_wall = max(now_ms, remote.wall_ms, self._wall_ms)
        if max_wall == self._wall_ms and max_wall == remote.wall_ms:
            self._counter = max(self._counter, remote.counter) + 1
        elif max_wall == self._wall_ms:
            self._counter += 1
        elif max_wall == remote.wall_ms:
            self._counter = remote.counter + 1
        else:
            self._counter = 0
        self._wall_ms = max_wall
        return HLCTimestamp(self._wall_ms, self._counter, self.node_id)
