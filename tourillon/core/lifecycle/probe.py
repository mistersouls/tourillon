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
"""Probe manager with one detector per peer."""

from __future__ import annotations

import asyncio
from enum import StrEnum

from tourillon.core.lifecycle.phi import FailureDetector

_SUSPECT_THRESHOLD = 8.0


class MemberState(StrEnum):
    LIVE = "live"
    SUSPECT = "suspect"
    UNKNOWN = "unknown"


class ProbeManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._detectors: dict[str, FailureDetector] = {}

    async def state_of(self, node_id: str) -> MemberState:
        async with self._lock:
            return self._state_of_unlocked(node_id)

    async def is_suspect(self, node_id: str) -> bool:
        return await self.state_of(node_id) is MemberState.SUSPECT

    async def is_live(self, node_id: str) -> bool:
        return await self.state_of(node_id) is MemberState.LIVE

    async def is_unknown(self, node_id: str) -> bool:
        return await self.state_of(node_id) is MemberState.UNKNOWN

    async def record_heartbeat(self, node_id: str) -> None:
        async with self._lock:
            detector = self._detectors.setdefault(node_id, FailureDetector())
            detector.record_heartbeat()

    async def record_miss(self, node_id: str) -> None:
        async with self._lock:
            self._detectors.setdefault(node_id, FailureDetector())

    async def phi_of(self, node_id: str) -> float:
        async with self._lock:
            detector = self._detectors.get(node_id)
            return detector.phi() if detector is not None else 0.0

    async def snapshot(self) -> dict[str, MemberState]:
        async with self._lock:
            return {
                node_id: self._state_of_unlocked(node_id) for node_id in self._detectors
            }

    async def all_states_with_phi(self) -> list[tuple[str, MemberState, float]]:
        async with self._lock:
            return [
                (node_id, self._state_of_unlocked(node_id), detector.phi())
                for node_id, detector in self._detectors.items()
            ]

    def _state_of_unlocked(self, node_id: str) -> MemberState:
        detector = self._detectors.get(node_id)
        if detector is None or not detector.has_observations:
            return MemberState.UNKNOWN
        if detector.is_available(_SUSPECT_THRESHOLD):
            return MemberState.LIVE
        return MemberState.SUSPECT
