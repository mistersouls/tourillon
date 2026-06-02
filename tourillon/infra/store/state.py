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

from __future__ import annotations

import asyncio
import os
import tomllib
from pathlib import Path
from typing import Any

import tomli_w

from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.state import StateError

_TMP_SUFFIX = ".tmp"


class FileStateAdapter:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._tmp = path.with_suffix(_TMP_SUFFIX)
        self._io_lock = asyncio.Lock()

    async def load(self) -> NodeState | None:
        async with self._io_lock:
            return await asyncio.to_thread(self._load_sync)

    async def save(self, state: NodeState) -> None:
        async with self._io_lock:
            await asyncio.to_thread(self._save_sync, state)

    def _load_sync(self) -> NodeState | None:
        if not self._path.exists():
            return None
        try:
            raw: dict[str, Any] = tomllib.loads(self._path.read_text(encoding="utf-8"))
            return _parse_state(raw)
        except StateError:
            raise
        except Exception as exc:
            raise StateError(f"Cannot read state.toml: {exc}") from exc

    def _save_sync(self, state: NodeState) -> None:
        raw = _encode_state(state)
        try:
            self._tmp.write_text(tomli_w.dumps(raw), encoding="utf-8")
            os.replace(self._tmp, self._path)
            _fsync_dir(self._path.parent)
        except OSError as exc:
            raise StateError(f"Cannot write state.toml: {exc}") from exc


def _parse_state(raw: dict[str, Any]) -> NodeState:
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
        raise StateError(f"Malformed state.toml: {exc}") from exc


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


def _fsync_dir(directory: Path) -> None:
    try:
        fd = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass
