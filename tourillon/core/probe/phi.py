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

import math
import time
from collections import deque


class PhiAccrual:
    """Paper-like phi accrual detector using normal CDF over observed intervals.

    The detector tracks heartbeat inter-arrival intervals and computes:
      phi(t) = -log10(1 - F(t))
    where F is the CDF of a normal distribution estimated from local samples.

    This detector is strictly local: each node keeps an independent view per peer.
    """

    def __init__(
        self,
        *,
        max_samples: int = 1000,
        min_stddev: float = 0.1,
        acceptable_heartbeat_pause: float = 0.0,
        min_samples: int = 3,
        threshold: float = 8.0,
    ) -> None:
        if max_samples < 1:
            raise ValueError(f"max_samples must be >= 1, got {max_samples}")
        if min_stddev <= 0:
            raise ValueError(f"min_stddev must be > 0, got {min_stddev}")
        if acceptable_heartbeat_pause < 0:
            raise ValueError(
                "acceptable_heartbeat_pause must be >= 0, got "
                f"{acceptable_heartbeat_pause}"
            )
        if min_samples < 2:
            raise ValueError(f"min_samples must be >= 2, got {min_samples}")
        if threshold <= 0:
            raise ValueError(f"threshold must be > 0, got {threshold}")

        self._intervals: deque[float] = deque(maxlen=max_samples)
        self._last_heartbeat_at: float | None = None
        self._min_stddev = min_stddev
        self._acceptable_pause = acceptable_heartbeat_pause
        self._min_samples = min_samples
        self._threshold = threshold

    @property
    def has_observations(self) -> bool:
        """Return True when enough intervals were observed to estimate a CDF."""
        return len(self._intervals) >= self._min_samples

    @property
    def interval_samples(self) -> int:
        """Return the number of currently retained inter-arrival intervals."""
        return len(self._intervals)

    def record(self, timestamp: float | None = None) -> None:
        """Record a successful heartbeat/response timestamp.

        Call this only when a peer successfully responds. Do not call on send.
        """
        now = time.monotonic() if timestamp is None else timestamp
        if self._last_heartbeat_at is not None:
            interval = now - self._last_heartbeat_at
            if interval > 0:
                self._intervals.append(interval)
        self._last_heartbeat_at = now

    def phi(self, now: float | None = None) -> float:
        """Return the phi suspicion score for the elapsed time since last heartbeat."""
        if self._last_heartbeat_at is None or not self.has_observations:
            return 0.0

        observed_at = time.monotonic() if now is None else now
        elapsed = observed_at - self._last_heartbeat_at - self._acceptable_pause
        if elapsed <= 0:
            return 0.0

        mean, stddev = self._stats()
        if mean <= 0:
            return 0.0

        z = (elapsed - mean) / stddev
        tail_probability = 1.0 - self._normal_cdf(z)
        return -math.log10(max(tail_probability, 1e-15))

    def reset(self) -> None:
        """Reset detector state for this peer."""
        self._intervals.clear()
        self._last_heartbeat_at = None

    def _stats(self) -> tuple[float, float]:
        values = tuple(self._intervals)
        mean = sum(values) / len(values)
        if len(values) < 2:
            return mean, self._min_stddev

        variance = sum((value - mean) ** 2 for value in values) / len(values)
        stddev = max(math.sqrt(variance), self._min_stddev)
        return mean, stddev

    @staticmethod
    def _normal_cdf(z: float) -> float:
        return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))
