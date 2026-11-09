import asyncio
import builtins
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lmdb

from tourillon.core.models.config import StorageConfig
from tourillon.core.models.storageschema import (
    Key,
    KeyList,
    Keyspace,
    KVList,
    KVPair,
    KVRef,
    Pid,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports.storage import (
    Storage,
    StorageProvider,
    Transaction,
    Transactional,
)
from tourillon.core.utils.defer import Result
from tourlib.contrib.threadpool import AsyncExecutor, AsyncThreadPool
from tourlib.models.ts import HLCTimestamp
from tourlib.models.types import SupportsBytes


class StorageError(Exception):
    """Custom exception for storage-related errors."""


@dataclass(frozen=True, slots=True)
class LmdbConfig:
    path: Path
    map_size: int
    readahead: bool = True
    writemap: bool = False
    sync: bool = True
    lock: bool = True
    max_readers: int = 16
    max_writers: int = 1


@dataclass(slots=True)
class Task:
    fn: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: Mapping[str, Any]
    future: asyncio.Future[Any]


class LmdbOperation:
    def __init__(
        self,
        txn: lmdb.Transaction,
        dbi_logs: lmdb._Database,
        dbi_tags: lmdb._Database,
        dbi_meta: lmdb._Database,
    ) -> None:
        self._txn = txn
        self._dbis = {
            Keyspace.LOGS: dbi_logs,
            Keyspace.TAGS: dbi_tags,
            Keyspace.META: dbi_meta,
        }

    def exists(self, ref: KVRef) -> bool:
        with self._txn.cursor(db=self._dbis[ref.keyspace]) as cursor:
            return cursor.set_key(ref.key)

    def get(self, ref: KVRef) -> KVPair | None:
        dbi = self._dbis[ref.keyspace]
        value = self._txn.get(ref.key, db=dbi)
        if value is None:
            return None

        return KVPair(ref.key, bytes(value), ref.keyspace)

    def last(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        predicate: Callable[[KVPair], bool] | None = None,
    ) -> KVPair | None:
        with self._txn.cursor(db=self._dbis[keyspace]) as cursor:
            prefix_bytes = prefix.to_bytes() if prefix is not None else None

            if prefix_bytes is not None and not self._cursor_range(
                cursor, prefix_bytes, reverse=True
            ):
                return None

            while True:
                encoded_key = bytes(cursor.key())
                if prefix_bytes is not None and not encoded_key.startswith(
                    prefix_bytes
                ):
                    return None

                kv = KVPair(encoded_key, bytes(cursor.value()), keyspace)
                if predicate is None or predicate(kv):
                    return kv

                if not self._advance_cursor(cursor, reverse=True):
                    return None

    def keys(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        start: bytes | None = None,
        limit: int = 128,
        reverse: bool = False,
    ) -> list[KVRef]:
        if limit <= 0:
            return []

        prefix_bytes = prefix.to_bytes() if prefix is not None else None
        with self._txn.cursor(db=self._dbis[keyspace]) as cursor:
            if not self._position_cursor(cursor, prefix_bytes, start, reverse):
                return []

            iterator = (
                cursor.iternext(values=False)
                if not reverse
                else cursor.iterprev(values=False)
            )
            keys: list[KVRef] = []

            for raw in iterator:
                key = bytes(raw)
                if prefix_bytes is not None and not key.startswith(prefix_bytes):
                    break

                keys.append(KVRef(key, keyspace))
                if len(keys) >= limit:
                    break

            return keys

    def list(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> list[KVPair]:
        if limit is not None and limit <= 0:
            return []

        with self._txn.cursor(db=self._dbis[keyspace]) as cursor:
            prefix_bytes = prefix.to_bytes() if prefix is not None else None

            if not self._position_cursor(cursor, prefix_bytes, start, reverse):
                return []

            items: list[KVPair] = []
            while True:
                key = bytes(cursor.key())
                if prefix_bytes is not None and not key.startswith(prefix_bytes):
                    break

                kv = KVPair(key, bytes(cursor.value()), keyspace)
                items.append(kv)

                if limit is not None and len(items) >= limit:
                    break

                if not self._advance_cursor(cursor, reverse):
                    break

            return items

    def put(self, kv: KVPair, overwrite: bool = True) -> bool:
        return self._txn.put(
            kv.key, kv.value, overwrite=overwrite, db=self._dbis[kv.keyspace]
        )

    def delete(self, ref: KVRef) -> KVPair | None:
        values = self.delete_many([KeyList(ref.keyspace, [ref.key])])
        return values[0] if values else None

    def get_many(self, refs: Sequence[KeyList]) -> builtins.list[KVPair]:
        results = []

        for ref in refs:
            db = self._dbis[ref.keyspace]
            with self._txn.cursor(db=db) as cursor:
                view = cursor.getmulti(ref.items)
                results.extend(
                    KVPair(key=key, value=bytes(value), keyspace=ref.keyspace)
                    for key, value in view
                )

        return results

    def put_many(
        self, kvs: Sequence[KVList], overwrite: bool = True
    ) -> tuple[int, int]:
        total_consumed, total_added = 0, 0

        for kv in kvs:
            db = self._dbis[kv.keyspace]
            with self._txn.cursor(db=db) as cursor:
                consumed, added = cursor.putmulti(kv.items, overwrite=overwrite)
                total_consumed += int(consumed)
                total_added += int(added)

        return total_consumed, total_added

    def delete_many(self, refs: Sequence[KeyList]) -> builtins.list[KVPair | None]:
        results: list[KVPair | None] = []

        for ref in refs:
            with self._txn.cursor(db=self._dbis[ref.keyspace]) as cursor:
                for key in ref.items:
                    value = cursor.pop(key)
                    if value is not None:
                        kv = KVPair(key, bytes(value), ref.keyspace)
                        results.append(kv)
                    else:
                        results.append(None)

        return results

    @staticmethod
    def _advance_cursor(cursor: lmdb.Cursor, reverse: bool) -> bool:
        if reverse:
            return cursor.prev()
        else:
            return cursor.next()

    def _cursor_range(self, cursor: lmdb.Cursor, key: bytes, reverse: bool) -> bool:
        """Position cursor at or near the given key range.

        For reverse iteration, finds the last key >= key by seeking to key+0xFF
        and stepping back. For forward iteration, seeks to key and advances past
        exact matches to start with keys strictly greater than key.
        """
        if reverse:
            key_end = self._upper_bound(key)
            if key_end is not None and cursor.set_range(key_end):
                return cursor.prev()
            return cursor.last()
        else:
            if not cursor.set_range(key):
                return False

            if bytes(cursor.key()) == key:
                return cursor.next()
            return True

    @staticmethod
    def _upper_bound(key: bytes) -> bytes | None:
        for i in range(len(key) - 1, -1, -1):
            if key[i] != 0xFF:
                return key[:i] + bytes([key[i] + 1])

        return None

    def _position_cursor(
        self,
        cursor: lmdb.Cursor,
        prefix: bytes | None,
        start: bytes | None,
        reverse: bool,
    ) -> bool:
        if start is not None:
            return self._cursor_range(cursor, start, reverse)
        if prefix is not None:
            return self._cursor_range(cursor, prefix, reverse)

        if reverse:
            return cursor.last()
        else:
            return cursor.first()


class LmdbTransaction(Transaction):
    def __init__(
        self,
        op: LmdbOperation,
        executor: AsyncExecutor,
    ) -> None:
        self._op = op
        self._executor = executor
        self._items: list[Task] = []
        self._failure: StorageError | None = None

    async def results(self) -> list[Any]:
        return await self.resolve()

    def exists(self, ref: KVRef) -> Result[bool]:
        return self._enqueue(self._op.exists, ref)

    def get(self, ref: KVRef) -> Result[KVPair | None]:
        return self._enqueue(self._op.get, ref)

    def last(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        predicate: Callable[[KVPair], bool] | None = None,
    ) -> Result[KVPair | None]:
        return self._enqueue(
            self._op.last,
            keyspace,
            prefix,
            predicate=predicate,
        )

    def keys(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        start: bytes | None = None,
        limit: int = 128,
        reverse: bool = False,
    ) -> Result[list[KVRef]]:
        return self._enqueue(
            self._op.keys,
            keyspace,
            prefix=prefix,
            start=start,
            limit=limit,
            reverse=reverse,
        )

    def list(
        self,
        keyspace: Keyspace,
        prefix: SupportsBytes | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> Result[list[KVPair]]:
        return self._enqueue(
            self._op.list,
            keyspace,
            prefix=prefix,
            start=start,
            limit=limit,
            reverse=reverse,
        )

    def put(self, kv: KVPair, overwrite: bool = True) -> Result[bool]:
        return self._enqueue(self._op.put, kv, overwrite=overwrite)

    def delete(self, ref: KVRef) -> Result[KVPair | None]:
        return self._enqueue(self._op.delete, ref)

    def get_many(self, refs: Sequence[KeyList]) -> Result[builtins.list[KVPair]]:
        return self._enqueue(self._op.get_many, refs)

    def put_many(
        self, kvs: Sequence[KVList], overwrite: bool = True
    ) -> Result[tuple[int, int]]:
        return self._enqueue(self._op.put_many, kvs, overwrite=overwrite)

    def delete_many(
        self, refs: Sequence[KeyList]
    ) -> Result[builtins.list[KVPair | None]]:
        return self._enqueue(self._op.delete_many, refs)

    async def resolve(self) -> builtins.list[Result[Any]]:
        if self._failure is not None:
            self.cancel()
            raise self._failure

        batch = self._items
        self._items = []

        if not batch:
            return []

        try:
            values = await self._executor.execute(self._execute, batch)
        except Exception as exc:
            error = StorageError(f"Error during batch execution: {exc}")
            self._failure = error

            for item in batch:
                if not item.future.done():
                    item.future.set_exception(error)

            raise error from exc

        for item, value in zip(batch, values):
            if not item.future.done():
                item.future.set_result(value)

        return values

    def cancel(self) -> None:
        for item in self._items:
            if not item.future.done():
                item.future.cancel()
        self._items.clear()

    def _enqueue[T](
        self,
        fn: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> Result[T]:
        future = asyncio.get_running_loop().create_future()
        self._items.append(Task(fn, args, kwargs, future))
        return Result(self, future)

    @staticmethod
    def _execute(batch: builtins.list[Task]) -> builtins.list[Any]:
        results: list[Any] = []

        for item in batch:
            results.append(item.fn(*item.args, **item.kwargs))

        return results


class LmdbTransactional(Transactional):
    def __init__(self, config: LmdbConfig) -> None:
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
        self._dbi_logs = self._env.open_db(Keyspace.LOGS.encode())
        self._dbi_tags = self._env.open_db(Keyspace.TAGS.encode())
        self._dbi_meta = self._env.open_db(Keyspace.META.encode())
        self._read_pool = AsyncThreadPool(max_workers=config.max_readers)
        self._write_pool = AsyncThreadPool(max_workers=config.max_writers)

    @asynccontextmanager
    async def transaction(self, write: bool = False) -> AsyncIterator[Transaction]:
        if self._closed:
            raise RuntimeError("LMDB transactional closed")

        pool = self._write_pool if write else self._read_pool

        async with pool.executor() as executor:
            begin = executor.execute(self._env.begin, write=write)

            try:
                raw = await asyncio.shield(begin)
            except asyncio.CancelledError:
                raw = await asyncio.shield(begin)
                await asyncio.shield(executor.execute(raw.abort))
                raise

            op = LmdbOperation(
                txn=raw,
                dbi_logs=self._dbi_logs,
                dbi_tags=self._dbi_tags,
                dbi_meta=self._dbi_meta,
            )
            txn = LmdbTransaction(op, executor)

            try:
                yield txn
                await txn.resolve()
            except BaseException:
                txn.cancel()
                await asyncio.shield(executor.execute(raw.abort))
                raise
            else:
                finalize = raw.commit if write else raw.abort
                await asyncio.shield(executor.execute(finalize))

    async def close(self) -> None:
        if self._closed:
            return

        await self._write_pool.close()
        await self._read_pool.close()
        await asyncio.to_thread(self._env.close)
        self._closed = True


class LmdbStorage(Storage):
    def __init__(self, config: StorageConfig) -> None:
        self._config = config
        self._stores: dict[int, Transactional] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def transactional_for(self, group: int) -> Transactional:
        return await self._transactional_for(group)

    async def latest_hlc(self, partitioner: Partitioner) -> HLCTimestamp:
        latest = HLCTimestamp(wall_ms=0, counter=0, node_id="")
        for group in range(partitioner.total_groups):
            start_pid, end_pid = partitioner.group_bounds(group)
            transactional = await self._transactional_for(group)

            async with transactional.transaction(write=False) as txn:
                for pid in range(start_pid, end_pid + 1):
                    txn.last(Keyspace.TAGS, prefix=Pid(pid))

                for kv in await txn.results():
                    if kv is None:
                        continue

                    key = Key.from_bytes(kv.key, Keyspace.TAGS)
                    if key.ts > latest:
                        latest = key.ts

        return latest

    async def close(self) -> None:
        """Close the storage and release any resources."""

    def _get_lmdb_config(self, group: int) -> LmdbConfig:
        return LmdbConfig(
            path=Path(self._config.data_dir) / str(group),
            map_size=self._config.group_size_bytes(),
        )

    async def _transactional_for(self, group: int) -> Transactional:
        if self._closed:
            raise RuntimeError("LMDB storage closed")

        if group in self._stores:
            return self._stores[group]

        async with self._lock:
            if group not in self._stores:
                if self._closed:
                    raise RuntimeError("LMDB storage closed during initialization")

                config = self._get_lmdb_config(group)
                try:
                    self._stores[group] = LmdbTransactional(config)
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to initialize LMDB transactional for group {group}"
                    ) from e

            return self._stores[group]


class LmdbStorageProvider(StorageProvider):
    def create(self, config: StorageConfig) -> Storage:
        data_dir = Path(config.data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        return LmdbStorage(config)
