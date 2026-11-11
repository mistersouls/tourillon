import time
from contextlib import asynccontextmanager
from typing import AsyncIterator, Iterable

from tourillon.core.models.storageschema import (
    PartitionAddress, Record, Keyspace, Key, KVRef, Tag, Value,
    KVList, KeyList, TagKind, KeyTag
)
from tourillon.core.ports.storage import Storage, Transaction
from tourillon.core.log import logger as _logger
from tourillon.core.storage.cell import cell_keys, DigestCell
from tourlib.models.types import SupportsBytes

logger = _logger.storage()

# 2**22 ms ~ 70 minutes per bucket.
DEFAULT_BUCKET_SHIFT = 22


class Session:
    def __init__(
        self,
        txn: Transaction,
        bucket_shift: int = DEFAULT_BUCKET_SHIFT
    ) -> None:
        self._txn = txn
        self._bucket_shift = bucket_shift

        self._deltas: dict[bytes, DigestCell] = {}
        self._stored: dict[bytes, DigestCell] = {}

    async def get_last_record(self, address: PartitionAddress) -> Record | None:
        """Get the last record for a given partition address."""
        kv_log = await self._txn.last(Keyspace.LOGS, address).get()
        if kv_log is None:
            return None

        key = Key.from_bytes(kv_log.key, Keyspace.LOGS)
        ref = KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS)
        kv_tag = await self._txn.get(ref).get()
        if kv_tag is None:
            logger.warning("No tag found for key: %s", key)
            return None

        tag = Tag.from_bytes(kv_tag.value)
        value = Value.from_bytes(kv_log.value)
        return Record(key=key, tag=tag, value=value)

    async def put_record(self, record: Record) -> bool:
        (written,) = await self._insert_record_many([record])
        return written

    async def put_record_many(self, records: Iterable[Record]) -> list[bool]:
        if not records:
            return []

        written = await self._insert_record_many(records)
        for record, ok in zip(records, written, strict=True):
            if ok and record.tag.kind in TagKind.owned():
                self._update_digest(record.key)

        return written

    async def last_tags_many(self, prefixes: Iterable[SupportsBytes]) -> list[KeyTag]:
        """Get the last tags for multiple prefixes."""
        for prefix in prefixes:
            self._txn.last(Keyspace.TAGS, prefix=prefix)

        return [
            KeyTag(
                key=Key.from_bytes(kv.key, Keyspace.TAGS),
                tag=Tag.from_bytes(kv.value)
            )
            for kv in await self._txn.results()
            if kv is not None
        ]

    async def flush(self) -> None:
        """Flush the session, committing any pending operations."""
        updates: list[tuple[bytes, bytes]] = []
        deletes: list[bytes] = []
        for key in sorted(self._deltas):
            cell = self._stored[key] + self._deltas[key]
            if cell.empty:
                deletes.append(key)
            else:
                updates.append((key, cell.to_bytes()))

        self._txn.delete_many([KeyList(Keyspace.META, deletes)])
        self._txn.put_many([KVList(Keyspace.META, updates)])
        await self._txn.results()
        self._deltas.clear()
        self._stored.clear()

    async def _insert_record_many(self, records: Iterable[Record]) -> list[bool]:
        """Insert multiple records into the storage."""
        refs = self._unknown_cells((record.key for record in records))

        kvlist = (
            KVList(keyspace, [rec.kv(keyspace).item() for rec in records])
            for keyspace in (Keyspace.LOGS, Keyspace.TAGS)
        )
        deferred = self._txn.put_many(kvlist, overwrite=False)

        pairs = self._txn.get_many([KeyList(Keyspace.META, refs)])
        self._stored.update(dict.fromkeys(refs, DigestCell()))
        for pair in await pairs.get():
            self._stored[pair.key] = DigestCell.from_bytes(pair.value)

        result = await deferred.get()
        half = len(result) // 2
        return [
            log and tag
            for log, tag in zip(result[:half], result[half:], strict=True)
        ]

    def _unknown_cells(self, keys: Iterable[Key]) -> list[bytes]:
        refs = {ref for key in keys for ref in cell_keys(key, self._bucket_shift)}
        return sorted(refs - self._stored.keys())

    def _update_digest(self, key: Key, remove: bool =  False) -> None:
        cell = DigestCell.of(key)

        for ref in cell_keys(key, self._bucket_shift):
            current = self._deltas.get(ref, DigestCell())
            self._deltas[ref] = current - cell if remove else current + cell


class SessionProvider:
    def __init__(self, storage: Storage) -> None:
        self._storage = storage

    @asynccontextmanager
    async def session(
        self, group: int, write: bool = False
    ) -> AsyncIterator[Session]:
        transactional = await self._storage.transactional_for(group)
        async with transactional.transaction(write=write) as txn:
            session = Session(txn)
            yield session
            if write:
                await session.flush()
