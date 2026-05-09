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
"""Process lock using an exclusive file lock on <data_dir>/pid.lock.

The lock is acquired before any socket is bound, any state file is read, or
any TLS context is created. It ensures no two tourillon processes share the
same data_dir. The OS releases the lock automatically when the process dies,
so crash recovery requires no manual intervention.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_LOCK_FILENAME = "pid.lock"


class PidLockError(Exception):
    """Raised when the pid.lock is already held by another process."""


def _open_lock_file(path: Path) -> int:
    """Open the lock file for writing; raise PidLockError on permission failure."""
    try:
        return os.open(str(path), os.O_WRONLY | os.O_CREAT, 0o600)
    except OSError as exc:
        raise PidLockError(
            f"Cannot create data_dir {path.parent}: {exc.strerror.lower()}"
        ) from exc


def _lock_win32(fd: int, path: Path) -> None:
    """Acquire a non-blocking exclusive lock on Windows; raise PidLockError if busy."""
    import msvcrt

    try:
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    except OSError:
        os.close(fd)
        raise PidLockError(
            f"Another tourillon process is already running "
            f"for data_dir {path.parent}"
        ) from None


def _lock_posix(fd: int, path: Path) -> None:
    """Acquire a non-blocking exclusive lock on POSIX; raise PidLockError if busy."""
    import fcntl

    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        raise PidLockError(
            f"Another tourillon process is already running "
            f"for data_dir {path.parent}"
        ) from None


def _acquire_fd_lock(fd: int, path: Path) -> None:
    """Dispatch to the platform-appropriate lock helper."""
    if sys.platform == "win32":
        _lock_win32(fd, path)
    else:
        _lock_posix(fd, path)


def _write_pid_info(fd: int) -> None:
    """Overwrite the lock file content with current PID and start timestamp."""
    payload = json.dumps(
        {"pid": os.getpid(), "started_at": datetime.now(UTC).isoformat()}
    ).encode()
    os.ftruncate(fd, 0)
    os.write(fd, payload)


class PidLock:
    """Exclusive file-based process lock for a data_dir.

    Acquire with acquire() and release with release(). Use as an async
    context manager for automatic cleanup.

    The lock file content is informational JSON:
      {"pid": <int>, "started_at": "<ISO-8601>"}
    Lock validity is determined by the OS file lock, not by the file content.
    """

    def __init__(self, data_dir: str | Path) -> None:
        self._path = Path(data_dir) / _LOCK_FILENAME
        self._fd: int | None = None

    async def acquire(self) -> None:
        """Acquire the exclusive lock; raise PidLockError if already held."""
        await asyncio.to_thread(self._acquire_sync)

    def _acquire_sync(self) -> None:
        fd = _open_lock_file(self._path)
        _acquire_fd_lock(fd, self._path)
        _write_pid_info(fd)
        self._fd = fd
        logger.debug("Process lock acquired on %s.", self._path)

    async def release(self) -> None:
        """Release the lock; idempotent."""
        await asyncio.to_thread(self._release_sync)

    def _release_sync(self) -> None:

        if self._fd is None:
            return
        if sys.platform != "win32":
            import fcntl

            with contextlib.suppress(OSError):
                fcntl.flock(self._fd, fcntl.LOCK_UN)
        with contextlib.suppress(OSError):
            os.close(self._fd)
        self._fd = None
        logger.debug("Process lock released on %s.", self._path)

    async def __aenter__(self) -> PidLock:
        await self.acquire()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.release()
