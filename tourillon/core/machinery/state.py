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
"""State adapter backed by state.toml."""

import asyncio
import dataclasses
import os
from pathlib import Path
from typing import Any, Protocol

from tourillon.core.exceptions import StateError
from tourillon.core.structure.member import MemberPhase, NodeState
from tourlib.ports.loader import ConfigReadWriter

_TMP_SUFFIX = ".tmp"


class StatePersistence(Protocol):
    async def load(self) -> NodeState | None:
        """Load state"""

    async def save(self, state: NodeState) -> None:
        """Save state"""

    async def patch(self, **fields: Any) -> None:
        """Atomically load, replace the given NodeState fields, and save.

        Holds the I/O lock for the entire read-modify-write sequence so that
        a concurrent save() (e.g. a phase transition) cannot be lost between
        the load and the save.  No-op when no state file exists yet.

        Example::

            await state.patch(epoch=7)
            await state.patch(committed_pids=(0, 1, 3), staging_pids=())
        """


class FileStatePersistence:
    def __init__(self, path: Path, config_rw: ConfigReadWriter) -> None:
        self._path = path
        self._config_rw = config_rw
        self._io_lock = asyncio.Lock()

    async def load(self) -> NodeState | None:
        async with self._io_lock:
            return await asyncio.to_thread(self._load_sync)

    async def save(self, state: NodeState) -> None:
        async with self._io_lock:
            await asyncio.to_thread(self._save_sync, state)

    async def patch(self, **fields: Any) -> None:
        async with self._io_lock:
            state = await asyncio.to_thread(self._load_sync)
            if state is None:
                return
            await asyncio.to_thread(self._save_sync, dataclasses.replace(state, **fields))

    def _load_sync(self) -> NodeState | None:
        if not self._path.exists():
            return None
        try:
            raw: dict[str, Any] = self._config_rw.read(self._path)
            return self._parse_state(raw)
        except StateError:
            raise
        except Exception as exc:
            raise StateError(f"Cannot read {self._path}: {exc}") from exc

    def _save_sync(self, state: NodeState) -> None:
        raw = self._encode_state(state)
        try:
            self._config_rw.write(self._path, raw)
        except OSError as exc:
            raise StateError(f"Cannot write {self._path}: {exc}") from exc

    def _parse_state(self, raw: dict[str, Any]) -> NodeState:
        try:
            node = raw["node"]
            topology = raw.get("topology", {})
            rebalance = raw.get("rebalance", {})
            return NodeState(
                node_id=str(node["node_id"]),
                phase=MemberPhase(str(node["phase"])),
                generation=int(node["generation"]),
                seq=int(node["seq"]),
                tokens=tuple(int(token) for token in node.get("tokens", [])),
                epoch=int(topology.get("epoch", 0)),
                committed_pids=tuple(
                    int(pid) for pid in rebalance.get("committed_pids", [])
                ),
                staging_pids=tuple(int(pid) for pid in rebalance.get("staging_pids", [])),
            )
        except Exception as exc:
            raise StateError(f"Malformed {self._path}: {exc}") from exc

    @staticmethod
    def _encode_state(state: NodeState) -> dict[str, Any]:
        return {
            "node": {
                "node_id": state.node_id,
                "phase": state.phase.value,
                "generation": state.generation,
                "seq": state.seq,
                "tokens": list(state.tokens),
            },
            "topology": {"epoch": state.epoch},
            "rebalance": {
                "committed_pids": list(state.committed_pids),
                "staging_pids": list(state.staging_pids),
            },
        }

    @staticmethod
    def _fsync_dir(directory: Path) -> None:
        try:
            fd = os.open(str(directory), os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass


class InMemoryStatePersistence:
    """In-memory state adapter for testing."""

    def __init__(self, initial_state: NodeState | None = None) -> None:
        self._state = initial_state

    async def load(self) -> NodeState | None:
        return self._state

    async def save(self, state: NodeState) -> None:
        self._state = state

    async def patch(self, **fields: Any) -> None:
        if self._state is None:
            return
        self._state = dataclasses.replace(self._state, **fields)
