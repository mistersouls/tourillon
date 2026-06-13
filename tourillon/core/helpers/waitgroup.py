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
"""WaitGroup[T] — asyncio synchronisation primitive for N concurrent tasks."""

import asyncio


class WaitGroup[T]:
    """
    An asyncio-compatible synchronization primitive for coordinating
    multiple concurrent tasks.

    The WaitGroup operates in independent cycles:
    - A cycle begins when `add()` is called while the internal counter is 0.
    - Tasks call `done(sub, success)` to signal completion.
    - When the counter reaches zero, the cycle completes and `wait()`
      returns the aggregated results for that cycle.

    Results are stored per cycle and automatically cleared when a new
    cycle begins (i.e., when `add()` is called while the counter is 0).

    Recording a result via `done(sub, success)` is OPTIONAL.
    Therefore, the number of recorded results may differ from the number
    of tasks added. It is the caller's responsibility to decide the
    required level of consistency between the number of tasks and the
    number of recorded results.
    """

    def __init__(self) -> None:
        self._counter = 0
        self._lock = asyncio.Lock()
        self._done_event = asyncio.Event()
        self._results: dict[T, bool] = {}  # sub -> success

    async def add(self, n: int = 1) -> None:
        """
        Increments the internal task counter by `n`.

        If the counter is currently zero, this marks the beginning of a
        new cycle and all previous results are cleared.
        """
        if n < 0:
            raise ValueError("Cannot add a negative number of tasks.")
        if n == 0:
            return

        async with self._lock:
            # New cycle -> reset results
            if self._counter == 0:
                self._results = {}

            self._counter += n
            self._done_event.clear()

    async def done(self, sub: T | None = None, success: bool = True) -> None:
        """
        Decrements the internal task counter by one and optionally records
        the result of the completed task.

        Recording a result is optional: if `sub` is None, no entry is
        added to the results dictionary. This means the number of results
        may be smaller than the number of tasks.
        """
        async with self._lock:
            if sub is not None:
                self._results[sub] = success

            self._counter -= 1
            if self._counter < 0:
                raise RuntimeError(
                    "Too many calls to done(): more tasks marked done than were added."
                )

            if self._counter == 0:
                self._done_event.set()

    def is_done(self) -> bool:
        """
        Returns True if all registered tasks have completed.

        This method does not block.
        """
        return self._done_event.is_set()

    async def wait(self) -> tuple[list[T], list[T]]:
        """
        Suspends execution until all added tasks have been marked as done.

        Returns the lists of successful and failed subjects for the current cycle.
        If the cycle is already complete, returns immediately with the results.

        Returns
        -------
        (success, failed) : tuple[list[T], list[T]]
            Two lists containing the subjects of tasks that succeeded and
            failed during the current cycle.
        """

        if self._counter == 0:
            self._done_event.set()

        await self._done_event.wait()

        success = [sub for sub, ok in self._results.items() if ok]
        failed = [sub for sub, ok in self._results.items() if not ok]

        return success, failed
