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
"""State and process-lock ports."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, Self

if TYPE_CHECKING:
    from tourillon.core.lifecycle.state import NodeState


class StateError(Exception):
    """Raised by StatePort implementations on I/O or decode failure."""


class StatePort(Protocol):
    async def load(self) -> NodeState | None:
        """Return persisted node state or None when state.toml is absent."""

    async def save(self, state: NodeState) -> None:
        """Persist node state atomically and durably."""


class ProcessLockError(Exception):
    """Raised when the process lock cannot be acquired (already held)."""


class ProcessLockPort(Protocol):
    """Exclusive lock on <data_dir>/pid.lock to prevent concurrent daemons.

    Acquire is non-blocking: if the lock is already held elsewhere, raise
    ProcessLockError immediately. The lock is held until release() is called
    or the process terminates (OS cleanup).
    """

    def acquire(self) -> None:
        """Acquire the exclusive lock. Raises ProcessLockError if already held."""

    def release(self) -> None:
        """Release the lock."""

    def __enter__(self) -> Self:
        """Context manager entry — acquire the lock."""

    def __exit__(self, *_: object) -> None:
        """Context manager exit — release the lock."""
