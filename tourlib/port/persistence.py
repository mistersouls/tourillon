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

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Any, Protocol


class Persistence(Protocol):
    """Generic port for path-based persistence with lock and atomicity guarantees.

    Implementations must:
    - Acquire an exclusive file lock before any write or patch operation.
    - Perform writes atomically (write to a temp file, then atomic rename).
    - Release the lock after the operation completes or fails.
    - Guarantee that concurrent readers never observe a partial write.
    """

    def edit(self, path: Path) -> AbstractAsyncContextManager[dict[str, Any]]:
        """
        Acquire lock, read current state, yield mutable document,
        and atomically write back.
        """

    async def load(self, path: Path) -> Mapping[str, Any]:
        """Read and parse data from *path* under an exclusive lock."""

    async def write(self, path: Path, document: Mapping[str, Any]) -> None:
        """Atomically persist *document* to *path* under an exclusive lock."""
