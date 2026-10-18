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

import asyncio
import os
import tempfile
import tomllib
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from pathlib import Path
from typing import Any

import tomli_w

from tourlib.port import Persistence


class TomlPersistence(Persistence):
    """Path-based TOML persistence with lock + atomic write guarantees."""

    def __init__(self) -> None:
        self._locks: dict[Path, asyncio.Lock] = {}

    def edit(self, path: Path) -> AbstractAsyncContextManager[dict[str, Any]]:
        @asynccontextmanager
        async def _edit() -> AsyncIterator[dict[str, Any]]:
            lock = self._get_lock(path)
            async with lock:
                document = await asyncio.to_thread(self._load_sync, path)
                yield document
                await asyncio.to_thread(self._write_sync, path, document)

        return _edit()

    async def load(self, path: Path) -> Mapping[str, Any]:
        lock = self._get_lock(path)
        async with lock:
            return await asyncio.to_thread(self._load_sync, path)

    async def write(self, path: Path, document: Mapping[str, Any]) -> None:
        lock = self._get_lock(path)
        async with lock:
            await asyncio.to_thread(self._write_sync, path, document)

    @staticmethod
    def _atomic_write(path: Path, raw: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(raw)
            os.replace(tmp, path)
        except Exception:
            with suppress(OSError):
                os.unlink(tmp)
            raise

    def _get_lock(self, path: Path) -> asyncio.Lock:
        lock = self._locks.get(path)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[path] = lock
        return lock

    @staticmethod
    def _load_sync(path: Path) -> dict[str, Any]:
        with path.open("rb") as handle:
            return tomllib.load(handle)

    def _write_sync(self, path: Path, document: Mapping[str, Any]) -> None:
        self._atomic_write(path, tomli_w.dumps(document))
