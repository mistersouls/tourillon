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
"""MemberState enumeration and ProbeManager for local reachability classification."""

from __future__ import annotations

import asyncio
from enum import StrEnum

from tourillon.core.lifecycle.phi import FailureDetector

_SUSPECT_THRESHOLD = 8.0


class MemberState(StrEnum):
    """Local reachability classification for a peer, derived from its FailureDetector.

    This is a private, per-observer value and is never gossiped. It is
    distinct from MemberPhase, which is the node's own self-declared state
    broadcast via gossip. Keeping them as separate types prevents callers
    from confusing local suspicion with cluster-wide membership state.
    """

    LIVE = "live"  # phi below threshold; heartbeats arriving normally
    SUSPECT = "suspect"  # phi above threshold; peer may be failing
    UNKNOWN = "unknown"  # no observations yet for this peer


class ProbeManager:
    """Manages one FailureDetector per actively observed peer.

    The ProbeManager is the single source of truth on local reachability.
    It maps each node_id to its own FailureDetector instance. Query methods
    (state_of, is_suspect, …) are awaited by PlacementStrategy while walking
    the ring. Mutation methods (record_heartbeat, record_miss) are called by
    the transport layer and must not be called from placement logic.

    A peer absent from the internal registry returns MemberState.UNKNOWN.
    All internal state is protected by a single asyncio.Lock.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._detectors: dict[str, FailureDetector] = {}

    async def state_of(self, node_id: str) -> MemberState:
        """Return the current MemberState for node_id."""
        async with self._lock:
            return self._state_of_unlocked(node_id)

    async def is_suspect(self, node_id: str) -> bool:
        """Return True if node_id is SUSPECT."""
        async with self._lock:
            return self._state_of_unlocked(node_id) == MemberState.SUSPECT

    async def is_live(self, node_id: str) -> bool:
        """Return True if node_id is LIVE."""
        async with self._lock:
            return self._state_of_unlocked(node_id) == MemberState.LIVE

    async def is_unknown(self, node_id: str) -> bool:
        """Return True if node_id is UNKNOWN."""
        async with self._lock:
            return self._state_of_unlocked(node_id) == MemberState.UNKNOWN

    async def snapshot(self) -> dict[str, MemberState]:
        """Return a snapshot mapping all known node_ids to their current state."""
        async with self._lock:
            return {nid: self._state_of_unlocked(nid) for nid in self._detectors}

    async def record_heartbeat(self, node_id: str) -> None:
        """Record a successful heartbeat arrival for node_id.

        Forwards the arrival time to the corresponding FailureDetector,
        creating a new detector instance if this is the first observation.
        """
        async with self._lock:
            if node_id not in self._detectors:
                self._detectors[node_id] = FailureDetector()
            self._detectors[node_id].record_heartbeat()

    async def record_miss(self, node_id: str) -> None:
        """Signal that an expected heartbeat or response was not received.

        Creates a detector for node_id if it does not exist yet, without
        recording an interval. The absence of a heartbeat causes phi to grow
        on the next phi() call relative to the last recorded arrival.
        """
        async with self._lock:
            if node_id not in self._detectors:
                self._detectors[node_id] = FailureDetector()

    def _state_of_unlocked(self, node_id: str) -> MemberState:
        """Return MemberState for node_id without acquiring the lock."""
        detector = self._detectors.get(node_id)
        if detector is None:
            return MemberState.UNKNOWN
        if detector.is_available(_SUSPECT_THRESHOLD):
            return MemberState.LIVE
        return MemberState.SUSPECT
