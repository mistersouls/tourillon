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
"""Count the TLS handshakes this node's servers rejected.

The event loop swallows server-side handshake failures, so the monitor hooks
the SSL objects the context creates: every ``ssl.SSLError`` raised by a
handshake, other than a request for more data, is recorded before it
propagates. Counts are kept per minute over a sliding hour, so memory stays
bounded whatever the rejection rate.
"""

import ssl
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

WINDOW_S = 3600.0
_BUCKET_S = 60.0


@dataclass(frozen=True)
class Rejections:
    """Rejected handshakes over the last hour, and the latest one."""

    count: int
    last_reason: str | None = None
    last_ago_s: float | None = None


class HandshakeMonitor:
    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        # (bucket start, count), oldest first.
        self._buckets: deque[tuple[float, int]] = deque()
        self._last_reason: str | None = None
        self._last_at: float | None = None

    def install(self, context: ssl.SSLContext) -> None:
        """Make ``context`` report every handshake it rejects to this monitor."""
        monitor = self

        class _MonitoredSSLObject(ssl.SSLObject):
            def do_handshake(self) -> None:
                try:
                    super().do_handshake()
                except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
                    raise
                except ssl.SSLError as exc:
                    monitor.record(exc.reason or str(exc))
                    raise

        context.sslobject_class = _MonitoredSSLObject

    def record(self, reason: str) -> None:
        now = self._clock()
        self._expire(now)
        start = now - now % _BUCKET_S
        if self._buckets and self._buckets[-1][0] == start:
            self._buckets[-1] = (start, self._buckets[-1][1] + 1)
        else:
            self._buckets.append((start, 1))
        self._last_reason = reason
        self._last_at = now

    def snapshot(self) -> Rejections:
        now = self._clock()
        self._expire(now)
        count = sum(n for _, n in self._buckets)
        if not count or self._last_at is None:
            return Rejections(count=0)
        return Rejections(
            count=count,
            last_reason=self._last_reason,
            last_ago_s=max(0.0, now - self._last_at),
        )

    def _expire(self, now: float) -> None:
        while self._buckets and self._buckets[0][0] + _BUCKET_S <= now - WINDOW_S:
            self._buckets.popleft()
