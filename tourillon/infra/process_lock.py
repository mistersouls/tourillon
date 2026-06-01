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
"""File-based process lock using fcntl (POSIX) or msvcrt (Windows)."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from tourillon.core.ports.state import ProcessLockError

logger = logging.getLogger(__name__)


class FileProcessLockAdapter:
    """Implements ProcessLockPort using fcntl.LOCK_EX|LOCK_NB (POSIX)
    or msvcrt.locking (Windows). Writes JSON metadata after acquisition."""

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._lock_file: int | None = None

    def acquire(self) -> None:
        """Acquire the exclusive lock. Raises ProcessLockError if already held."""
        try:
            self._lock_file = os.open(str(self._path), os.O_WRONLY | os.O_CREAT, 0o600)

            if sys.platform == "win32":
                import msvcrt

                try:
                    msvcrt.locking(self._lock_file, msvcrt.LK_NBLCK, 1)
                except OSError as e:
                    os.close(self._lock_file)
                    self._lock_file = None
                    raise ProcessLockError(
                        f"Lock file already held: {self._path}"
                    ) from e
            else:
                import fcntl

                try:
                    fcntl.flock(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as e:
                    os.close(self._lock_file)
                    self._lock_file = None
                    raise ProcessLockError(
                        f"Lock file already held: {self._path}"
                    ) from e

            # Write lock metadata (informational only)
            metadata = {
                "pid": os.getpid(),
                "started_at": datetime.now(UTC).isoformat(),
            }
            os.write(self._lock_file, json.dumps(metadata).encode("utf-8"))
            os.fsync(self._lock_file)

            logger.debug("Acquired process lock: %s", self._path)
        except ProcessLockError:
            raise
        except Exception as e:
            if self._lock_file is not None:
                os.close(self._lock_file)
                self._lock_file = None
            raise ProcessLockError(f"Failed to acquire lock: {e}") from e

    def release(self) -> None:
        """Release the lock."""
        if self._lock_file is not None:
            try:
                if sys.platform != "win32":
                    import fcntl

                    fcntl.flock(self._lock_file, fcntl.LOCK_UN)
            except Exception:
                pass
            finally:
                os.close(self._lock_file)
                self._lock_file = None
            logger.debug("Released process lock: %s", self._path)

    def __enter__(self) -> FileProcessLockAdapter:
        """Context manager entry — acquire the lock."""
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        """Context manager exit — release the lock."""
        self.release()
