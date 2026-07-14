import asyncio
import logging
from collections.abc import AsyncIterator
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lmdb

from tourillon.core.machinery.namespace import (
    Key,
    Namespace,
    Prefix,
    Tag,
    TaggedRecord,
    Value,
)


@dataclass
class LmdbConfig:
    path: Path
    map_size: int
    readahead: bool = True
    writemap: bool = False
    sync: bool = True
    lock: bool = True
    max_readers: int = 4
    max_writers: int = 1


class LmdbBackendStorage:
    def __init__(self, cfg: LmdbConfig, bucket: str) -> None:
        self._env: lmdb.Environment = lmdb.open(
            str(cfg.path / bucket),
            map_size=cfg.map_size,
            lock=cfg.lock,
            writemap=cfg.writemap,
            sync=cfg.sync,
            readahead=cfg.readahead,
            max_readers=cfg.max_readers,
            subdir=True,
            max_dbs=2
        )
        self._dbi_log = self._env.open_db(Namespace.LOG.encode())
        self._dbi_tag = self._env.open_db(Namespace.TAGS.encode())
        self._dbs: dict[Namespace, Any] = {
            Namespace.LOG: self._dbi_log,
            Namespace.TAGS: self._dbi_tag,
        }
        self._read_pool = ThreadPoolExecutor(max_workers=cfg.max_readers)
        self._write_pool = ThreadPoolExecutor(max_workers=cfg.max_writers)

        self._logger = logging.getLogger(__name__)

    async def put(self, record: TaggedRecord) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._write_pool, self._put_sync, record)

    async def delete(self, record: TaggedRecord) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._write_pool, self._delete_sync, record)

    async def tag(self, key: Key, tag: Tag) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._write_pool, self._tag_sync, key, tag)

    async def iter(
        self,
        namespace: Namespace,
        prefix: Prefix | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
        batch_size: int = 1024,
    ) -> AsyncIterator[TaggedRecord]:
        loop = asyncio.get_running_loop()
        batch_size, remaining = self._setup_limit(limit, batch_size)
        next_key = start

        if batch_size <= 0:
            return

        while True:
            batch = await loop.run_in_executor(
                self._read_pool,
                self._list_record,
                namespace,
                prefix,
                next_key,
                batch_size,
                reverse,
            )

            if not batch:
                break

            for record in batch:
                yield record

                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            next_key = self._next_page_key(batch[-1].key, namespace, reverse)

    async def close(self) -> None:
        def shutdown() -> None:
            self._env.close()
            self._read_pool.shutdown(wait=True)
            self._write_pool.shutdown(wait=True)

        await asyncio.to_thread(shutdown)

    def _put_sync(self, record: TaggedRecord) -> bool:
        with self._env.begin(write=True) as txn:
            key = record.key
            log_ok = txn.put(
                key.to_bytes(Namespace.LOG), record.value.to_bytes(), db=self._dbi_log
            )
            tag_ok = txn.put(
                key.to_bytes(Namespace.TAGS), record.tag.to_bytes(), db=self._dbi_tag
            )
            return log_ok and tag_ok

    def _delete_sync(self, record: TaggedRecord) -> bool:
        with self._env.begin(write=True) as txn:
            key = record.key
            log_ok = txn.delete(key.to_bytes(Namespace.LOG), db=self._dbi_log)
            tag_ok = txn.delete(key.to_bytes(Namespace.TAGS), db=self._dbi_tag)
            return log_ok and tag_ok

    def _tag_sync(self, key: Key, tag: Tag) -> bool:
        with self._env.begin(write=True) as txn:
            if txn.get(key.to_bytes(Namespace.LOG), db=self._dbi_log) is None:
                return False
            if txn.get(key.to_bytes(Namespace.TAGS), db=self._dbi_tag) is None:
                return False
            txn.put(key.to_bytes(Namespace.TAGS), tag.to_bytes(), db=self._dbi_tag)
            return True

    def _fetch_record(
        self,
        txn: lmdb.Transaction,
        cursor: lmdb.Cursor,
        key: Key,
        namespace: Namespace,
    ) -> TaggedRecord | None:
        """Read and decode the value+tag pair for *key* from the open transaction.

        Return None and emit a warning when either side of the pair is missing;
        this indicates data inconsistency between the two DBIs.
        """
        if namespace == Namespace.LOG:
            raw_value = bytes(cursor.value())
            raw_tag = txn.get(key.to_bytes(Namespace.TAGS), db=self._dbi_tag)
        else:
            raw_tag = bytes(cursor.value())
            raw_value = txn.get(key.to_bytes(Namespace.LOG), db=self._dbi_log)

        if raw_tag is None:
            self._logger.warning("Tag missing for key: %s", key)
            return None
        if raw_value is None:
            self._logger.warning("Value missing for key: %s", key)
            return None
        return TaggedRecord(
            key=key,
            value=Value.from_bytes(bytes(raw_value)),
            tag=Tag.from_bytes(bytes(raw_tag)),
        )

    def _collect_record_page(
        self,
        txn: lmdb.Transaction,
        cursor: lmdb.Cursor,
        namespace: Namespace,
        prefix_bytes: bytes | None,
        limit: int | None,
        reverse: bool,
    ) -> list[TaggedRecord]:
        """Walk the already-positioned cursor and collect matching Tagged record records.

        Stops at the prefix boundary, when *limit* is reached, or when the
        cursor is exhausted. The caller is responsible for initial positioning.
        """
        items: list[TaggedRecord] = []
        while True:
            b_key = bytes(cursor.key())
            if prefix_bytes is not None and not b_key.startswith(prefix_bytes):
                break
            key = Key.from_bytes(b_key, namespace)
            record = self._fetch_record(txn, cursor, key, namespace)
            if record is not None:
                items.append(record)
            if limit is not None and len(items) >= limit:
                break
            if not self._advance_cursor(cursor, reverse):
                break
        return items

    def _list_record(
        self,
        namespace: Namespace,
        prefix: Prefix | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> list[TaggedRecord]:
        """Scan namespace entries and return matching Tagged records.

        Walk entries in *prefix* scope starting at *start*, in ascending or
        descending order. Stop early when *limit* items have been collected.
        """
        if limit is not None and limit <= 0:
            return []

        prefix_bytes = prefix.to_bytes() if prefix else None

        with self._env.begin(write=False) as txn:
            cursor = txn.cursor(db=self._dbs[namespace])
            if not self._position_cursor(cursor, prefix_bytes, start, reverse):
                return []
            return self._collect_record_page(
                txn, cursor, namespace, prefix_bytes, limit, reverse
            )

    @staticmethod
    def _setup_limit(limit: int | None, batch_size: int) -> tuple[int, int | None]:
        """Return (effective_batch_size, remaining) from raw limit and batch_size.

        When limit is None or negative, the scan is unbounded: the original
        batch_size is preserved and remaining is None. When limit is non-negative,
        batch_size is capped at limit and remaining is initialised to limit so
        the caller can decrement it after each yielded item.
        """
        if limit is None or limit < 0:
            return batch_size, None
        return min(batch_size, limit), limit

    @staticmethod
    def _next_page_key(last_key: Key, ns: Namespace, reverse: bool) -> bytes:
        """Return the NamespaceLog key that starts the next page.

        Forward scans advance past last_key; reverse scans step one key back.
        """
        if reverse:
            return last_key.decrement_key(ns)
        return last_key.increment_key(ns)

    @staticmethod
    def _advance_cursor(cursor: lmdb.Cursor, reverse: bool) -> bool:
        if reverse:
            return cursor.prev()
        else:
            return cursor.next()

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

    @staticmethod
    def _cursor_range(cursor: lmdb.Cursor, key: bytes, reverse: bool) -> bool:
        if reverse:
            key_end = key + b"\xff"
            if cursor.set_range(key_end):
                return cursor.prev()
            return cursor.last()
        else:
            if not cursor.set_range(key):
                return False
            # Exclusive: advance past the exact boundary key when present.
            if bytes(cursor.key()) == key:
                return cursor.next()
            return True
