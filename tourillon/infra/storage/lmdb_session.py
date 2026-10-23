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
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

import lmdb

from tourillon.core.models.storageschema import Keyspace, KVPair, KVRef
from tourillon.core.ports.storage import Batch, Session, SessionProvider
from tourillon.infra.storage.lmdb_batch import LmdbBatch
from tourillon.infra.storage.lmdb_transaction import LmdbTransaction
from tourlib.contrib.threadpool import AsyncExecutor, AsyncThreadPool
from tourlib.models.types import SupportsBytes


@dataclass(frozen=True, slots=True)
class LmdbConfig:
    path: Path
    map_size: int
    readahead: bool = True
    writemap: bool = False
    sync: bool = True
    lock: bool = True
    max_readers: int = 1000
    max_writers: int = 1


class LmdbSession(Session):
    def __init__(
        self,
        txn: lmdb.Transaction,
        executor: AsyncExecutor,
        dbi_logs: lmdb._Database,
        dbi_tags: lmdb._Database,
        dbi_meta: lmdb._Database,
    ) -> None:
        self._txn = LmdbTransaction(
            txn=txn, dbi_logs=dbi_logs, dbi_tags=dbi_tags, dbi_meta=dbi_meta
        )
        self._executor = executor

    def batch(self) -> Batch:
        return LmdbBatch(self._txn, self._executor)

    async def get(self, ref: KVRef) -> KVPair | None:
        return await self._executor.execute(self._txn.get, ref)

    async def exists(self, ref: KVRef) -> bool:
        return await self._executor.execute(self._txn.exists, ref)

    async def put(self, kv: KVPair, overwrite: bool = True) -> bool:
        return await self._executor.execute(self._txn.put, kv, overwrite=overwrite)

    async def delete(self, ref: KVRef) -> None:
        await self._executor.execute(self._txn.delete, ref)

    async def iter(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        start: bytes | None = None,
        reverse: bool = False,
        batch_size: int = 32,
        limit: int | None = None,
    ) -> AsyncIterator[KVPair]:
        batch_size, remaining = self._setup_limit(limit, batch_size)
        if batch_size <= 0:
            return

        next_key = start

        while True:
            batch = await self._executor.execute(
                self._txn.list, keyspace, prefix, next_key, batch_size, reverse
            )

            if not batch:
                break

            for kv in batch:
                yield kv

                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            next_key = self._next_page_key(batch[-1].key, reverse)

    async def last(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        predicate: Callable[[KVPair], bool] | None = None,
    ) -> KVPair | None:
        return await self._executor.execute(self._txn.last, keyspace, prefix, predicate)

    @staticmethod
    def _next_page_key(key: bytes, reverse: bool) -> bytes:
        if reverse:
            for i in range(len(key) - 1, -1, -1):
                if key[i] != 0x00:
                    return key[:i] + bytes([key[i] - 1]) + b"\xff" * (len(key) - i - 1)
            return b""
        else:
            for i in range(len(key) - 1, -1, -1):
                if key[i] != 0xFF:
                    return key[:i] + bytes([key[i] + 1])
            return key + b"\x00"

    @staticmethod
    def _setup_limit(limit: int | None, batch_size: int) -> tuple[int, int | None]:
        """Return (effective_batch_size, remaining) from raw limit and batch_size.

        When limit is None or negative, the scan is unbounded: the original
        batch_size is preserved and remaining is None. When limit is non-negative,
        batch_size is capped at limit and remaining is initialised to limit soo
        the caller can decrement it after each yielded item.
        """
        if limit is None or limit < 0:
            return batch_size, None
        return min(batch_size, limit), limit


class LmdbSessionProvider(SessionProvider):
    def __init__(self, config: LmdbConfig, group: int) -> None:
        self._closed = False
        self._env = lmdb.open(
            path=str(config.path),
            map_size=config.map_size,
            lock=config.lock,
            writemap=config.writemap,
            sync=config.sync,
            readahead=config.readahead,
            max_readers=config.max_readers,
            subdir=True,
            max_dbs=3,
        )
        self._group = group
        self._dbi_logs = self._env.open_db(Keyspace.LOGS.encode())
        self._dbi_tags = self._env.open_db(Keyspace.TAGS.encode())
        self._dbi_meta = self._env.open_db(Keyspace.META.encode())
        self._read_pool = AsyncThreadPool(max_workers=config.max_readers)
        self._write_pool = AsyncThreadPool(max_workers=config.max_writers)

    @property
    def group_id(self) -> int:
        return self._group

    @asynccontextmanager
    async def session(self, write: bool = False) -> AsyncIterator[Session]:
        if self._closed:
            raise RuntimeError("LMDB session provider closed")

        pool = self._write_pool if write else self._read_pool
        async with pool.executor() as executor:
            # Shield protect the transaction begin: if cancelled before begin() runs,
            # nothing starts. If it runs, we get the transaction.
            txn = await asyncio.shield(executor.execute(self._env.begin, write=write))
            session = LmdbSession(
                txn=txn,
                executor=executor,
                dbi_logs=self._dbi_logs,
                dbi_tags=self._dbi_tags,
                dbi_meta=self._dbi_meta,
            )
            try:
                yield session
            except BaseException:  # Catch CancelledError and other BaseExceptions
                await asyncio.shield(executor.execute(txn.abort))
                raise
            else:
                finalize = txn.commit if write else txn.abort
                await asyncio.shield(executor.execute(finalize))

    async def close(self) -> None:
        if self._closed:
            return

        await self._read_pool.close()
        await self._write_pool.close()
        await asyncio.to_thread(self._env.close)
        self._closed = True
