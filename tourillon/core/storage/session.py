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

import hashlib
from collections import defaultdict
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass

from tourillon.core.log import logger
from tourillon.core.models.storageschema import (
    PID_SIZE,
    Key,
    KeyList,
    Keyspace,
    KeyTag,
    KVList,
    KVPair,
    KVRef,
    MetaKind,
    ObligationKey,
    ObligationPrefix,
    PartitionAddress,
    RawPrefix,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.ports.storage import Storage, Transaction
from tourillon.core.utils.defer import Result
from tourlib.models.ts import HLCTimestamp
from tourlib.models.types import SupportsBytes

_ROOT = b"\x00"
_BUCKET = b"\x01"
ROOT_PREFIX: bytes = MetaKind.DIGEST.value + _ROOT
BUCKET_PREFIX: bytes = MetaKind.DIGEST.value + _BUCKET
DIGEST_BYTES = 16
_COUNT_BYTES = 8
_BUCKET_BYTES = 8

# 2**22 ms ~ 70 minutes per bucket.
DEFAULT_BUCKET_SHIFT = 22

OWNED_KINDS = frozenset({TagKind.LIVE, TagKind.TOMBSTONE})


@dataclass(frozen=True, slots=True)
class DigestCell:
    """Order-independent fingerprint of a set of version keys."""

    count: int = 0
    digest: int = 0

    @classmethod
    def of(cls, key: Key) -> "DigestCell":
        hasher = hashlib.blake2b(key.to_bytes(Keyspace.TAGS), digest_size=DIGEST_BYTES)
        return cls(count=1, digest=int.from_bytes(hasher.digest(), "big"))

    def __add__(self, other: "DigestCell") -> "DigestCell":
        return DigestCell(self.count + other.count, self.digest ^ other.digest)

    def __sub__(self, other: "DigestCell") -> "DigestCell":
        return DigestCell(self.count - other.count, self.digest ^ other.digest)

    @property
    def empty(self) -> bool:
        return self.count == 0 and self.digest == 0

    def to_bytes(self) -> bytes:
        return self.count.to_bytes(_COUNT_BYTES, "big") + self.digest.to_bytes(
            DIGEST_BYTES, "big"
        )

    @classmethod
    def from_bytes(cls, b: bytes) -> "DigestCell":
        if len(b) != _COUNT_BYTES + DIGEST_BYTES:
            raise ValueError(f"digest cell must be 24 bytes, got {len(b)}")

        return cls(
            count=int.from_bytes(b[:_COUNT_BYTES], "big"),
            digest=int.from_bytes(b[_COUNT_BYTES:], "big"),
        )


class Session:
    def __init__(
        self,
        transaction: Transaction,
        bucket_shift: int = DEFAULT_BUCKET_SHIFT,
    ) -> None:
        self._txn = transaction
        self._bucket_shift = bucket_shift

        # Keyed by META key: bucket and root cells share the same bookkeeping.
        self._deltas: dict[bytes, DigestCell] = {}

        # META cells as stored before this transaction, read once per cell.
        self._stored: dict[bytes, DigestCell] = {}

    async def keys(
        self,
        prefix: SupportsBytes | None = None,
        after: bytes | None = None,
        limit: int = 64,
    ) -> list[Key]:
        result = self._txn.keys(Keyspace.TAGS, prefix=prefix, start=after, limit=limit)
        return [Key.from_bytes(ref.key, ref.keyspace) for ref in await result.get()]

    async def tags(
        self,
        prefix: SupportsBytes | None = None,
        after: bytes | None = None,
        limit: int = 64,
    ) -> list[KeyTag]:
        result = self._txn.list(Keyspace.TAGS, prefix=prefix, start=after, limit=limit)
        return [
            KeyTag(Key.from_bytes(kv.key, Keyspace.TAGS), Tag.from_bytes(kv.value))
            for kv in await result.get()
        ]

    async def get_last_record(self, addr: PartitionAddress) -> Record | None:
        kv_log = await self._txn.last(Keyspace.LOGS, addr).get()
        if kv_log is None:
            return None

        key = Key.from_bytes(kv_log.key, Keyspace.LOGS)
        kv_tag = await self._txn.get(
            KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS)
        ).get()
        if kv_tag is None:
            return None

        tag = Tag.from_bytes(kv_tag.value)
        value = Value.from_bytes(kv_log.value)
        return Record(key=key, tag=tag, value=value)

    async def get_latest_records(
        self, addr: PartitionAddress, limit: int
    ) -> list[Record]:
        latest_records = await self.get_latest_record_many([addr], limit)
        return latest_records.get(addr, [])

    async def get_latest_record_many(
        self, addresses: Sequence[PartitionAddress], limit: int
    ) -> Mapping[PartitionAddress, list[Record]]:
        addresses = list(addresses)
        for addr in addresses:
            self._txn.list(Keyspace.LOGS, prefix=addr, limit=limit, reverse=True)

        pages = await self._txn.results()
        logs: list[tuple[PartitionAddress, KVPair]] = [
            (addr, ref)
            for addr, page in zip(addresses, pages, strict=True)
            for ref in page
        ]

        for (
            _,
            log,
        ) in logs:
            key = Key.from_bytes(log.key, log.keyspace)
            self._txn.get(KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))

        tags = await self._txn.results()
        records = defaultdict(list)
        for (addr, log), kv in zip(logs, tags, strict=True):
            if kv is not None:
                record = _record(kv, log)
                if record is not None and record.tag.kind in OWNED_KINDS:
                    records[addr].append(record)

        return records

    async def read_record(self, key: Key) -> Record | None:
        (record,) = await self.read_record_many([key])
        return record

    async def read_record_many(self, keys: Sequence[Key]) -> list[Record | None]:
        """Read exact versions in one batch, preserving order and missing entries."""
        if not keys:
            return []

        for key in keys:
            self._txn.get(KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))
            self._txn.get(KVRef(key.to_bytes(Keyspace.LOGS), Keyspace.LOGS))

        results: list[KVPair | None] = await self._txn.results()
        return [
            _record(tag, log)
            for tag, log in zip(results[::2], results[1::2], strict=True)
        ]

    async def put_record(self, record: Record) -> bool:
        (written,) = await self.put_record_many([record])
        return written

    async def put_record_many(self, records: Sequence[Record]) -> list[bool]:
        if not records:
            return []

        written = await self._insert_many(records)
        for record, ok in zip(records, written, strict=True):
            if ok and record.tag.kind in OWNED_KINDS:
                self._update_digest(record.key)

        return written

    async def remove_records(
        self, keys: Sequence[Key], *, dry_run: bool = False
    ) -> tuple[int, int]:
        """
        Remove owned records and their digest contributions atomically.

        The caller is responsible for deciding which records may be removed.
        Returned bytes include LOGS/TAGS keys and values, not backend overhead
        or digest changes.
        """
        if dry_run:
            found = await self.read_record_many(keys)
            records = [r for r in found if r is not None]
        else:
            records = await self._remove_many(keys)
        size = sum(record.size() for record in records)
        return len(records), size

    async def get_last_obligation(self) -> ObligationKey | None:
        kv = await self._txn.last(Keyspace.META, prefix=ObligationPrefix()).get()
        if kv is not None:
            return ObligationKey.from_bytes(kv.key)
        return None

    async def obligation_keys(
        self,
        after: bytes | None = None,
        limit: int = 64,
    ) -> list[ObligationKey]:
        if limit < 1:
            raise ValueError("limit must be >= 1")

        obligations: list[ObligationKey] = []
        cursor = after
        while len(obligations) < limit:
            remaining = limit - len(obligations)
            page = await self._txn.keys(
                Keyspace.META, prefix=ObligationPrefix(), start=cursor, limit=remaining
            ).get()
            for ref in page:
                try:
                    obligations.append(ObligationKey.from_bytes(ref.key))
                except ValueError:
                    logger.exception(
                        "storage.obligation unreadable key=%s", ref.key.hex()
                    )

            if len(page) < remaining:
                break
            cursor = page[-1].key

        return obligations

    async def put_obligation_many(
        self, obligations: Sequence[ObligationKey]
    ) -> tuple[int, int]:
        if not obligations:
            return 0, 0

        kv_list = KVList(
            Keyspace.META, [(obligation.to_bytes(), b"") for obligation in obligations]
        )
        results = self._txn.put_many([kv_list])
        return await results.get()

    async def remove_obligation_many(self, obligations: Sequence[ObligationKey]) -> int:
        if not obligations:
            return 0

        key_lists = [KeyList(Keyspace.META, [key.to_bytes()]) for key in obligations]
        deleted: list[KVPair | None] = await self._txn.delete_many(key_lists).get()
        return len([pair for pair in deleted if pair is not None])

    async def roots(self, pids: Sequence[int]) -> list[tuple[int, DigestCell]]:
        if not pids:
            return []

        key_list = KeyList(Keyspace.META, [root_key(pid) for pid in pids])
        pairs = await self._txn.get_many([key_list]).get()
        cells = {_root_pid(pair.key): _cell(pair) for pair in pairs}
        return [(pid, cells.get(pid, DigestCell())) for pid in pids]

    async def list_roots(
        self, after: int | None = None, limit: int = 64
    ) -> list[tuple[int, DigestCell]]:
        if limit < 1:
            raise ValueError("limit must be >= 1")

        start = None if after is None else root_key(after)
        result = self._txn.list(
            Keyspace.META, prefix=RawPrefix(ROOT_PREFIX), start=start, limit=limit
        )
        page = await result.get()
        return [(_root_pid(pair.key), _cell(pair)) for pair in page]

    async def list_buckets(
        self, pid: int, after: int | None = None, limit: int = 64
    ) -> list[tuple[int, DigestCell]]:
        if limit < 1:
            return []

        prefix = BUCKET_PREFIX + pid.to_bytes(PID_SIZE, "big")
        start = None if after is None else bucket_key(pid, after)
        result = self._txn.list(
            Keyspace.META, prefix=RawPrefix(prefix), start=start, limit=limit
        )
        page = await result.get()
        return [(_bucket_of(pair.key), _cell(pair)) for pair in page]

    async def flush(self) -> None:
        """Write the updated META cells in one batch."""
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

    async def latest_identities(
        self, address: PartitionAddress, limit: int
    ) -> list[KeyTag]:
        return (await self.latest_identities_many([address], limit))[address]

    async def latest_identities_many(
        self, addresses: Sequence[PartitionAddress], limit: int
    ) -> dict[PartitionAddress, list[KeyTag]]:
        unique = list(dict.fromkeys(addresses))
        for address in unique:
            self._txn.keys(Keyspace.LOGS, prefix=address, limit=limit, reverse=True)

        pages = await self._txn.results()
        keys = [
            (address, Key.from_bytes(ref.key, Keyspace.LOGS))
            for address, page in zip(unique, pages, strict=True)
            for ref in page
        ]
        for _, key in keys:
            self._txn.get(KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))

        tags = await self._txn.results()
        identities: dict[PartitionAddress, list[KeyTag]] = {a: [] for a in unique}
        for (address, key), pair in zip(keys, tags, strict=True):
            if pair is not None:
                tag = Tag.from_bytes(pair.value)
                if tag.kind in OWNED_KINDS:
                    identities[address].append(KeyTag(key, tag))

        return identities

    async def oldest_keys(self, address: PartitionAddress, limit: int) -> list[Key]:
        refs = await self._txn.keys(Keyspace.LOGS, prefix=address, limit=limit).get()
        return [Key.from_bytes(ref.key, Keyspace.LOGS) for ref in refs]

    async def latest_timestamp(self, pids: Sequence[int]) -> HLCTimestamp:
        for pid in pids:
            self._txn.last(
                Keyspace.TAGS, prefix=RawPrefix(pid.to_bytes(PID_SIZE, "big"))
            )

        pairs = await self._txn.results()
        return max(
            (Key.from_bytes(pair.key, Keyspace.TAGS).ts for pair in pairs if pair),
            default=HLCTimestamp(0, 0, ""),
        )

    def _update_digest(self, key: Key, remove: bool = False) -> None:
        cell = DigestCell.of(key)

        for ref in _cell_keys(key, self._bucket_shift):
            current = self._deltas.get(ref, DigestCell())
            self._deltas[ref] = current - cell if remove else current + cell

    async def _insert_many(self, records: Sequence[Record]) -> list[bool]:
        refs = self._unknown_cells([r.key for r in records])
        results: list[Result[bool]] = []

        for record in records:
            results.append(self._txn.put(_log_kv(record), overwrite=False))
            self._txn.put(_tag_kv(record), overwrite=False)

        pairs = self._txn.get_many([KeyList(Keyspace.META, refs)])
        self._stored.update(dict.fromkeys(refs, DigestCell()))
        for pair in await pairs.get():
            self._stored[pair.key] = _cell(pair)

        return [await result.get() for result in results]

    async def _remove_many(self, keys: Sequence[Key]) -> list[Record]:
        records: list[Record] = []

        refs = self._unknown_cells(keys)
        pairs = self._txn.get_many([KeyList(Keyspace.META, refs)])
        self._stored.update(dict.fromkeys(refs, DigestCell()))
        for pair in await pairs.get():
            self._stored[pair.key] = _cell(pair)

        key_lists = [
            KeyList(keyspace, [key.to_bytes(keyspace)])
            for key in keys
            for keyspace in (Keyspace.TAGS, Keyspace.LOGS)
        ]
        deleted: list[KVPair | None] = await self._txn.delete_many(key_lists).get()
        pairs = zip(deleted[::2], deleted[1::2], strict=True)
        for tag, log in pairs:
            record = _record(tag, log)
            if record is not None:
                records.append(record)
                if record.tag.kind in OWNED_KINDS:
                    self._update_digest(record.key, remove=True)

        return records

    def _unknown_cells(self, keys: Sequence[Key]) -> list[bytes]:
        refs = {ref for key in keys for ref in _cell_keys(key, self._bucket_shift)}
        return sorted(refs - self._stored.keys())


class SessionProvider:
    def __init__(self, storage: Storage) -> None:
        self._storage = storage

    @asynccontextmanager
    async def session(
        self, group: int, write: bool = False, bucket_shift: int = DEFAULT_BUCKET_SHIFT
    ) -> AsyncIterator[Session]:
        transactional = await self._storage.transactional_for(group)
        async with transactional.transaction(write=write) as txn:
            session = Session(txn, bucket_shift=bucket_shift)
            yield session
            if write:
                await session.flush()


def root_key(pid: int) -> bytes:
    return ROOT_PREFIX + pid.to_bytes(PID_SIZE, "big")


def bucket_key(pid: int, bucket: int) -> bytes:
    return (
        BUCKET_PREFIX
        + pid.to_bytes(PID_SIZE, "big")
        + bucket.to_bytes(_BUCKET_BYTES, "big")
    )


def bucket_bounds(
    pid: int, bucket: int, shift: int = DEFAULT_BUCKET_SHIFT
) -> tuple[bytes, bytes]:
    """Return the TAGS ``(start, stop)`` bounds, both exclusive, of a bucket.

    TAGS keys are ``pid | wall_ms | ...``, so a bucket is a contiguous range.
    No real key equals the 12-byte ``pid | lo`` start, so the exclusive cursor
    skips nothing.
    """
    pid_bytes = pid.to_bytes(PID_SIZE, "big")
    lo = bucket << shift
    hi = (bucket + 1) << shift

    return pid_bytes + lo.to_bytes(8, "big"), pid_bytes + hi.to_bytes(8, "big")


def _tag_kv(record: Record) -> KVPair:
    return KVPair(
        key=record.key.to_bytes(Keyspace.TAGS),
        value=record.tag.to_bytes(),
        keyspace=Keyspace.TAGS,
    )


def _log_kv(record: Record) -> KVPair:
    return KVPair(
        key=record.key.to_bytes(Keyspace.LOGS),
        value=record.value.to_bytes(),
        keyspace=Keyspace.LOGS,
    )


def _cell_keys(
    key: Key, bucket_shift: int = DEFAULT_BUCKET_SHIFT
) -> tuple[bytes, bytes]:
    pid = key.pid.value
    return root_key(pid), bucket_key(pid, key.ts.wall_ms >> bucket_shift)


def _cell(kv: KVPair | None) -> DigestCell:
    return DigestCell() if kv is None else DigestCell.from_bytes(kv.value)


def _record(tag: KVPair | None, log: KVPair | None) -> Record | None:
    if tag is None or log is None:
        return None

    key = Key.from_bytes(log.key, log.keyspace)
    return Record(
        key=key, tag=Tag.from_bytes(tag.value), value=Value.from_bytes(log.value)
    )


def _root_pid(meta_key: bytes) -> int:
    return int.from_bytes(meta_key[len(ROOT_PREFIX) :], "big")


def _bucket_of(meta_key: bytes) -> int:
    return int.from_bytes(meta_key[len(BUCKET_PREFIX) + PID_SIZE :], "big")
