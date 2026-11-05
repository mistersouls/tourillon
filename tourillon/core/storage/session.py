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
"""Single write path of a partition group.

Every write that makes a version owned (LIVE or TOMBSTONE) goes through
`GroupSession`, so the per-(pid, time bucket) fingerprints kept in META
always match TAGS. A cell is ``(count, xor of blake2b-128(TAGS key))``:
commutative, and exact because a version already owned is never folded twice.

Each pid also keeps a root cell, always equal to the combination of its
bucket cells, so a pid is compared with a single read. Roots are laid out
before buckets in META, so the roots of a pid range are contiguous.
"""

import hashlib
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass

from tourillon.core.log import logger as _logger
from tourillon.core.models.storageschema import (
    PID_SIZE,
    Key,
    Keyspace,
    KeyTag,
    KVPair,
    KVRef,
    MetaKind,
    ObligationKey,
    ObligationPrefix,
    PartitionAddress,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.ports.storage import Session, SessionProvider

logger = _logger.storage()

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


def root_key(pid: int) -> bytes:
    return ROOT_PREFIX + pid.to_bytes(PID_SIZE, "big")


def bucket_key(pid: int, bucket: int) -> bytes:
    return (
        BUCKET_PREFIX
        + pid.to_bytes(PID_SIZE, "big")
        + bucket.to_bytes(_BUCKET_BYTES, "big")
    )


class GroupSession:
    """
    A single group session of new versions in a partition group.

    The META cells of each bucket and of its pid root are read in the same
    batch as the first write that touches them; deltas are accumulated in
    memory and written to META
    once, just before commit. A transaction of new versions therefore costs
    four executor round trips (begin, insert, META write, commit) whatever
    the number of records.
    """

    def __init__(
        self,
        session: Session,
        bucket_shift: int = DEFAULT_BUCKET_SHIFT,
    ) -> None:
        self._bucket_shift = bucket_shift
        self._session = session
        # Keyed by META key: bucket and root cells share the same bookkeeping.
        self._deltas: dict[bytes, DigestCell] = {}
        # META cells as stored before this transaction, read once per cell.
        self._stored: dict[bytes, DigestCell] = {}

    async def put(self, record: Record) -> bool:
        """Write one version; return False when it changes nothing."""
        (written,) = await self.put_many([record])
        return written

    async def put_many(self, records: Sequence[Record]) -> list[bool]:
        """Write versions in one batch; return False for those already stored.

        One batch inserts every tag and value with ``overwrite=False`` and
        reads the META cells of the buckets not yet known. A key identifies a
        single immutable version, so an existing entry already holds the same
        value and tag and is never rewritten. A version is folded into the
        digest only when it is actually inserted, so replays are free and the
        digest stays exact. Returns, per record, whether it was written.
        """
        if not records:
            return []

        tags = [_tag_kv(r) for r in records]
        written = await self._insert(records, tags)

        for record, ok in zip(records, written, strict=True):
            if ok and record.tag.kind in OWNED_KINDS:
                self._fold(record.key)

        return written

    async def latest_identities(
        self, addr: PartitionAddress, limit: int
    ) -> list[KeyTag]:
        identities = await self.latest_identities_many([addr], limit)
        return identities[addr]

    async def latest_identities_many(
        self, addresses: Sequence[PartitionAddress], limit: int
    ) -> dict[PartitionAddress, list[KeyTag]]:
        """Read bounded owned identities for each address without payloads."""
        addresses = list(dict.fromkeys(addresses))
        async with self._session.batch() as batch:
            for address in addresses:
                batch.keys(Keyspace.LOGS, prefix=address, limit=limit, reverse=True)

        pages = await batch.result()
        keys = [
            (address, Key.from_bytes(ref.key, ref.keyspace))
            for address, page in zip(addresses, pages, strict=True)
            for ref in page
        ]
        async with self._session.batch() as batch:
            for _, key in keys:
                batch.get(KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))

        tags = await batch.result()
        identities: dict[PartitionAddress, list[KeyTag]] = {
            addr: [] for addr in addresses
        }
        for (address, key), kv in zip(keys, tags, strict=True):
            if kv is not None:
                tag = Tag.from_bytes(kv.value)
                if tag.kind in OWNED_KINDS:
                    identities[address].append(KeyTag(key, tag))

        return identities

    async def log_page(
        self, after: bytes | None, limit: int, pid: int | None = None
    ) -> list[Key]:
        async with self._session.batch() as batch:
            batch.list(
                Keyspace.TAGS,
                prefix=None if pid is None else Pid(pid),
                start=after,
                limit=limit,
            )

        (page,) = await batch.result()
        return [Key.from_bytes(kv.key, Keyspace.TAGS) for kv in page]

    async def oldest_keys(self, address: PartitionAddress, limit: int) -> list[Key]:
        async with self._session.batch() as batch:
            batch.keys(Keyspace.LOGS, prefix=address, limit=limit)

        (page,) = await batch.result()
        return [Key.from_bytes(ref.key, ref.keyspace) for ref in page]

    async def remove(
        self, keys: Sequence[Key], *, dry_run: bool = False
    ) -> tuple[int, int]:
        """Remove owned records and their digest contributions atomically.

        The caller is responsible for deciding which records may be removed.
        Returned bytes include LOGS/TAGS keys and values, not backend overhead
        or digest changes.
        """
        candidates: list[Key] = []
        size = 0

        for key in dict.fromkeys(keys):
            record = await self.read(key)
            if record is not None and record.tag.kind in OWNED_KINDS:
                candidates.append(key)
                size += len(record.to_bytes(Keyspace.TAGS)) + len(
                    record.key.to_bytes(Keyspace.LOGS)
                )

        if not dry_run:
            await self._remove_records(candidates)

        return len(candidates), size

    async def _remove_records(self, keys: Sequence[Key]) -> None:
        refs = self._unknown_cells(keys)
        async with self._session.batch() as batch:
            for ref in refs:
                batch.get(KVRef(ref, Keyspace.META))

        values = await batch.result()
        self._stored.update(
            (ref, _cell(kv)) for ref, kv in zip(refs, values, strict=True)
        )

        async with self._session.batch() as batch:
            for key in keys:
                batch.delete(KVRef(key.to_bytes(Keyspace.LOGS), Keyspace.LOGS))
                batch.delete(KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))
                for ref in self._cell_keys(key):
                    self._deltas[ref] = self._deltas.get(
                        ref, DigestCell()
                    ) - DigestCell.of(key)

        await batch.result()

    async def get(self, addr: PartitionAddress) -> Record | None:
        kv_log = await self._session.last(
            keyspace=Keyspace.LOGS,
            prefix=addr,
        )
        if kv_log is None:
            return None

        key = Key.from_bytes(kv_log.key, Keyspace.LOGS)
        kv_tag = await self._session.get(
            KVRef(key=key.to_bytes(Keyspace.TAGS), keyspace=Keyspace.TAGS)
        )
        if kv_tag is None:
            return None

        tag = Tag.from_bytes(kv_tag.value)
        value = Value.from_bytes(kv_log.value)
        return Record(key=key, tag=tag, value=value)

    async def read(self, key: Key) -> Record | None:
        """Read the version stored at one exact timestamp.

        `get` answers "what is current for this address"; repair needs "do you
        hold this particular version". The physical key already spells out
        `pid|address|ts`, so the lookup is a point read, not a scan.
        """
        (record,) = await self.get_many([key])
        return record

    async def get_many(self, keys: Sequence[Key]) -> list[Record | None]:
        """Read exact versions in one batch, preserving order and missing entries."""
        if not keys:
            return []

        async with self._session.batch() as batch:
            for key in keys:
                batch.get(KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))
                batch.get(KVRef(key.to_bytes(Keyspace.LOGS), Keyspace.LOGS))

        results: list[KVPair | None] = await batch.result()
        return [
            self._record_at(key, results[2 * i], results[2 * i + 1])
            for i, key in enumerate(keys)
        ]

    async def put_obligations(self, obligations: Sequence[ObligationKey]) -> None:
        """Persist replication obligations; resubmitting one is a no-op."""
        if not obligations:
            return

        async with self._session.batch() as batch:
            for obligation in obligations:
                batch.put(KVPair(obligation.to_bytes(), b"", Keyspace.META))
        await batch.result()

    async def has_obligations(self) -> bool:
        """Check pending replication obligations, excluding digest metadata."""
        found = await self._session.last(Keyspace.META, prefix=ObligationPrefix())
        return found is not None

    async def obligation_page(
        self, start: bytes | None, limit: int, target: str | None = None
    ) -> tuple[list[ObligationKey], bytes | None]:
        """Read a bounded page of obligations after an exclusive cursor.

        ``target`` restricts the page to the obligations owed to that node.
        Malformed entries are logged and skipped; the cursor follows the
        physical page so they cannot stall a scan. None means exhausted.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")

        async with self._session.batch() as batch:
            batch.list(
                Keyspace.META,
                prefix=ObligationPrefix(target),
                start=start,
                limit=limit,
            )
        (page,) = await batch.result()

        obligations = [ob for ob in map(_obligation, page) if ob is not None]
        cursor = page[-1].key if len(page) == limit else None
        return obligations, cursor

    async def remove_obligations(self, obligations: Sequence[ObligationKey]) -> None:
        """Delete fulfilled or obsolete obligations; versions stay untouched."""
        if not obligations:
            return

        async with self._session.batch() as batch:
            for obligation in obligations:
                batch.delete(KVRef(obligation.to_bytes(), Keyspace.META))
        await batch.result()

    async def flush(self) -> None:
        """Persist cumulative digest deltas without committing the transaction.

        Keep the initial cells and total deltas so repeated flushes, including
        after further writes, never count a version twice.
        """
        if not self._deltas:
            return

        await self._flush()

    async def roots(self, pids: Sequence[int]) -> list[DigestCell]:
        """Read the root cell of each pid in one batch; absent roots are empty."""
        if not pids:
            return []

        async with self._session.batch() as batch:
            for pid in pids:
                batch.get(KVRef(root_key(pid), Keyspace.META))

        results: list[KVPair | None] = await batch.result()

        return [_cell(kv) for kv in results]

    async def root_page(
        self, after: int | None, limit: int
    ) -> tuple[list[tuple[int, DigestCell]], int | None]:
        """List non-empty roots in pid order after an exclusive pid cursor.

        Only pids holding at least one owned version have a root, so this
        enumerates the partitions this group actually stores without touching
        TAGS. Returns the next cursor when the page is full.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")

        start = None if after is None else root_key(after)

        async with self._session.batch() as batch:
            batch.list(
                Keyspace.META, prefix=_RawPrefix(ROOT_PREFIX), start=start, limit=limit
            )

        (page,) = await batch.result()

        roots = [(_root_pid(kv.key), _cell(kv)) for kv in page]
        cursor = roots[-1][0] if len(page) == limit else None

        return roots, cursor

    async def bucket_page(
        self, pid: int, after: int | None, limit: int
    ) -> tuple[list[tuple[int, DigestCell]], int | None]:
        """List the non-empty bucket cells of one pid after an exclusive cursor."""
        if limit < 1:
            raise ValueError("limit must be >= 1")

        prefix = BUCKET_PREFIX + pid.to_bytes(PID_SIZE, "big")
        start = None if after is None else bucket_key(pid, after)

        async with self._session.batch() as batch:
            batch.list(
                Keyspace.META, prefix=_RawPrefix(prefix), start=start, limit=limit
            )

        (page,) = await batch.result()

        cells = [(_bucket_of(kv.key), _cell(kv)) for kv in page]
        cursor = cells[-1][0] if len(page) == limit else None

        return cells, cursor

    async def owned_keys(
        self, start: bytes, stop: bytes, limit: int
    ) -> tuple[list[Key], bytes | None]:
        """List owned version keys in the TAGS range ``(start, stop)``.

        ``start`` is an exclusive cursor and ``stop`` an exclusive upper bound;
        both begin with the 4-byte pid, which bounds the scan to one partition.
        Versions that are not LIVE or TOMBSTONE are skipped: they are not part
        of the digest.
        The returned cursor is the last *physical* key read, so a page of
        skipped entries still advances; it is None once ``stop`` is reached.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")

        async with self._session.batch() as batch:
            batch.list(
                Keyspace.TAGS,
                prefix=_RawPrefix(start[:PID_SIZE]),
                start=start,
                limit=limit,
            )

        (page,) = await batch.result()

        keys: list[Key] = []

        for kv in page:
            if kv.key >= stop:
                return keys, None

            key = _owned_key(kv)

            if key is not None:
                keys.append(key)

        cursor = page[-1].key if len(page) == limit else None

        return keys, cursor

    @staticmethod
    def _record_at(key: Key, tag: KVPair | None, log: KVPair | None) -> Record | None:
        if tag is None or log is None:
            return None

        return Record(
            key=key, tag=Tag.from_bytes(tag.value), value=Value.from_bytes(log.value)
        )

    async def _insert(
        self, records: Sequence[Record], tags: Sequence[KVPair]
    ) -> list[bool]:
        """Insert tags and values if absent; preload the META cells needed."""
        refs = self._unknown_cells(
            [r.key for r in records if r.tag.kind in OWNED_KINDS]
        )

        async with self._session.batch() as batch:
            for record, kv in zip(records, tags, strict=True):
                batch.put(kv, overwrite=False)
                batch.put(_log_kv(record), overwrite=False)

            for ref in refs:
                batch.get(KVRef(ref, Keyspace.META))

        results: list = await batch.result()
        n = 2 * len(records)

        for ref, kv in zip(refs, results[n:], strict=True):
            self._stored[ref] = _cell(kv)

        # Flat results: (tags, logs) per record, then META cells; keep tags only.
        return results[:n:2]

    def _cell_keys(self, key: Key) -> tuple[bytes, bytes]:
        """Return the META keys of the root and bucket cells covering `key`."""
        pid = key.pid.value

        return root_key(pid), bucket_key(pid, key.ts.wall_ms >> self._bucket_shift)

    def _unknown_cells(self, keys: Sequence[Key]) -> list[bytes]:
        refs = {ref for key in keys for ref in self._cell_keys(key)}
        return sorted(refs - self._stored.keys())

    def _fold(self, key: Key) -> None:
        cell = DigestCell.of(key)
        for ref in self._cell_keys(key):
            self._deltas[ref] = self._deltas.get(ref, DigestCell()) + cell

    async def _flush(self) -> None:
        """Write the updated META cells in one batch (preloaded by `_insert`)."""
        cells = [
            (ref, self._stored[ref] + self._deltas[ref]) for ref in sorted(self._deltas)
        ]
        updates = [
            (KVRef(key, Keyspace.META), None if cell.empty else cell.to_bytes())
            for key, cell in cells
        ]

        async with self._session.batch() as batch:
            for ref, value in updates:
                if value is None:
                    batch.delete(ref)
                else:
                    batch.put(KVPair(ref.key, value, Keyspace.META))
        await batch.result()


class GroupSessionManager:
    def __init__(self, session_provider: SessionProvider) -> None:
        self._session_provider = session_provider

    @property
    def group_id(self) -> int:
        """Return the group ID for this transaction manager."""
        return self._session_provider.group_id

    @asynccontextmanager
    async def session(self, write: bool = False) -> AsyncIterator[GroupSession]:
        """Flush digest changes before committing; roll back on any failure."""
        async with self._session_provider.session(write=write) as raw:
            session = GroupSession(
                raw,
                bucket_shift=DEFAULT_BUCKET_SHIFT,
            )
            yield session
            if write:
                await session.flush()


def _cell(kv: KVPair | None) -> DigestCell:
    return DigestCell() if kv is None else DigestCell.from_bytes(kv.value)


@dataclass(frozen=True, slots=True)
class _RawPrefix:
    """A literal key prefix accepted by the storage ``prefix`` parameters."""

    value: bytes

    def to_bytes(self) -> bytes:
        return self.value

    @classmethod
    def from_bytes(cls, b: bytes) -> "_RawPrefix":
        return cls(b)


def _root_pid(meta_key: bytes) -> int:
    return int.from_bytes(meta_key[len(ROOT_PREFIX) :], "big")


def _bucket_of(meta_key: bytes) -> int:
    return int.from_bytes(meta_key[len(BUCKET_PREFIX) + PID_SIZE :], "big")


def _owned_key(kv: KVPair) -> Key | None:
    try:
        if Tag.from_bytes(kv.value).kind not in OWNED_KINDS:
            return None
        return Key.from_bytes(kv.key, Keyspace.TAGS)
    except (ValueError, OverflowError) as exc:
        logger.warning("Skipping unreadable tag entry %s: %s", kv.key.hex(), exc)
        return None


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


def _obligation(kv: KVPair) -> ObligationKey | None:
    try:
        return ObligationKey.from_bytes(kv.key)
    except (ValueError, OverflowError) as exc:
        logger.warning("Skipping unreadable obligation %s: %s", kv.key.hex(), exc)
        return None


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
