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
"""FailureDetector — phi-accrual failure detector (Hayashibara et al., 2004)."""

from __future__ import annotations

import math
import time
from collections import deque

_MAX_SAMPLES = 1000
_MIN_STD_DEV = 0.1  # seconds — prevents division collapse on identical intervals


class FailureDetector:
    """Phi-accrual failure detector monitoring exactly one peer.

    A single instance monitors exactly one peer and knows nothing about that
    peer's identity — identity management is the sole responsibility of
    ProbeManager. The detector models the distribution of heartbeat
    inter-arrival intervals and computes a phi value that grows the longer
    the peer has been silent.

    record_heartbeat() must be called each time a heartbeat arrives from the
    monitored peer. phi() and is_available() may be called at any time.

    The phi formula used is the exponential approximation from the original
    paper: phi(t) = t / (mean * ln(10)), which grows linearly with elapsed
    time and is zero before any heartbeat is recorded.
    """

    def __init__(self) -> None:
        self._intervals: deque[float] = deque(maxlen=_MAX_SAMPLES)
        self._last_arrival: float | None = None

    def record_heartbeat(self) -> None:
        """Record an instantaneous heartbeat arrival.

        Updates the inter-arrival time distribution used to compute phi.
        The first call establishes the baseline; no interval is recorded.
        """
        now = time.monotonic()
        if self._last_arrival is not None:
            self._intervals.append(now - self._last_arrival)
        self._last_arrival = now

    def phi(self) -> float:
        """Return the current suspicion level.

        Return 0.0 if no heartbeats have been recorded yet. The value grows
        unboundedly as the time since the last heartbeat increases relative
        to the historical inter-arrival distribution.
        """
        if not self._intervals or self._last_arrival is None:
            return 0.0
        elapsed = time.monotonic() - self._last_arrival
        mean = _mean(self._intervals)
        if mean <= 0:
            return 0.0
        return elapsed / (mean * math.log(10))

    def is_available(self, threshold: float = 8.0) -> bool:
        """Return True when phi() < threshold.

        The default threshold of 8.0 corresponds to a false-positive
        probability of ~0.003% under a normal inter-arrival distribution.
        """
        return self.phi() < threshold


def _mean(values: deque[float]) -> float:
    """Return the arithmetic mean of values, or 0.0 for an empty sequence."""
    if not values:
        return 0.0
    return sum(values) / len(values)
