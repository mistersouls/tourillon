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
"""StatePort Protocol and StateError for persistent node lifecycle state."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from tourillon.core.lifecycle.state import NodeState


class StateError(Exception):
    """Raised by StatePort implementations on any I/O or decode failure."""


class StatePort(Protocol):
    """Read and write the node's persistent lifecycle state.

    load() returns None when no state file exists (first boot, fresh IDLE
    node). save() is always atomic and durable: it writes to a temporary
    file in the same directory, calls os.replace(), then fsyncs the
    containing directory when the platform supports it. No partial writes
    are ever visible on disk.

    Callers must ensure save() completes before opening any socket or
    emitting any gossip record (write-before-announce invariant).
    """

    async def load(self) -> NodeState | None:
        """Return the persisted NodeState, or None if no state file exists."""
        ...

    async def save(self, state: NodeState) -> None:
        """Atomically persist state to disk.

        Raise StateError on any I/O failure (permissions, filesystem full,
        decode error). Never raises on a clean write.
        """
        ...
