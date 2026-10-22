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
from pathlib import Path

from tourillon.core.models.clock import HLCTimestamp
from tourillon.core.models.config import StorageConfig
from tourillon.core.models.storageschema import HintKey, HintPrefix, Key, Keyspace
from tourillon.core.ports.storage import SessionProvider, Storage, StorageProvider
from tourillon.infra.storage.lmdb_session import LmdbConfig, LmdbSessionProvider


class LMDBStorage(Storage):
    def __init__(self, config: StorageConfig) -> None:
        self._config = config
        self._providers: dict[int, LmdbSessionProvider] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def session_provider_for(self, group: int) -> SessionProvider:
        return await self._provider_for(group)

    async def latest_hlc(self, total_groups: int) -> HLCTimestamp:
        """Return the latest hybrid logical clock timestamp."""
        latest = HLCTimestamp(wall_ms=0, counter=0, node_id="")
        for group in range(total_groups):
            provider = await self.session_provider_for(group)
            async with provider.session(write=False) as session:
                async for kv in session.iter(
                    keyspace=Keyspace.TAGS, reverse=True, limit=1
                ):
                    key = Key.from_bytes(kv.key, Keyspace.TAGS)
                    if key.ts > latest:
                        latest = key.ts
                async for kv in session.iter(
                    keyspace=Keyspace.META, prefix=HintPrefix()
                ):
                    hint_key = HintKey.from_bytes(kv.key)
                    hlc = hint_key.hlc
                    latest = max(latest, hlc)

        return latest

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        providers = tuple(self._providers.values())
        self._providers.clear()

        for provider in providers:
            await provider.close()

    def _get_lmdb_config(self, group: int) -> LmdbConfig:
        return LmdbConfig(
            path=Path(self._config.data_dir) / str(group),
            map_size=self._config.group_size_bytes(),
        )

    async def _provider_for(self, group: int) -> SessionProvider:
        if self._closed:
            raise RuntimeError("LMDB storage closed")

        provider = self._providers.get(group)
        if provider is not None:
            return provider

        async with self._lock:
            provider = self._providers.get(group)
            if provider is None:
                if self._closed:
                    raise RuntimeError("LMDB storage closed during initialization")
                config = self._get_lmdb_config(group)
                try:
                    provider = LmdbSessionProvider(config, group)
                    self._providers[group] = provider
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to initialize LMDB session provider for group {group}"
                    ) from e
        return provider


class LMDBStorageProvider(StorageProvider):
    def create(self, config: StorageConfig) -> Storage:
        data_dir = Path(config.data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        return LMDBStorage(config)
