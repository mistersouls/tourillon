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
"""Phi-accrual failure detector."""

from __future__ import annotations

import math
import time
from collections import deque

_MAX_SAMPLES = 1000


class FailureDetector:
    def __init__(self) -> None:
        self._intervals: deque[float] = deque(maxlen=_MAX_SAMPLES)
        self._last_arrival: float | None = None

    def record_heartbeat(self) -> None:
        now = time.monotonic()
        if self._last_arrival is not None:
            self._intervals.append(now - self._last_arrival)
        self._last_arrival = now

    def phi(self) -> float:
        if not self._intervals or self._last_arrival is None:
            return 0.0
        elapsed = time.monotonic() - self._last_arrival
        mean = sum(self._intervals) / len(self._intervals)
        if mean <= 0.0:
            return 0.0
        return elapsed / (mean * math.log(10))

    @property
    def has_observations(self) -> bool:
        return self._last_arrival is not None

    def is_available(self, threshold: float = 8.0) -> bool:
        return self.phi() < threshold
