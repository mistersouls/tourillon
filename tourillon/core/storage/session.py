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
    Hint,
    HintKey,
    HintPrefix,
    HintReference,
    Key,
    Keyspace,
    KeyTag,
    KVPair,
    KVRef,
    MetaKind,
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

    async def exists(self, key: Key) -> bool:
        """Check exact owned-version presence without loading its value."""
        (present,) = await self.exists_many([key])
        return present

    async def exists_many(self, keys: Sequence[Key]) -> list[bool]:
        """Check owned-version presence in one batch without loading values."""
        if not keys:
            return []

        async with self._session.batch() as batch:
            for key in keys:
                batch.get(KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))
                batch.exists(KVRef(key.to_bytes(Keyspace.LOGS), Keyspace.LOGS))

        results = await batch.result()
        return [
            _kind(results[2 * i]) in OWNED_KINDS and results[2 * i + 1]
            for i in range(len(keys))
        ]

    async def stage_many(self, records: Sequence[Record], source: str) -> int:
        """Stage new or already-staged versions; leave active copies untouched.

        Retries may replace a staging wrapper, including its source. Unreadable
        existing tags are logged and replaced, as they cannot be used.
        """
        if not records:
            return 0

        async with self._session.batch() as batch:
            for record in records:
                batch.get(KVRef(record.key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))
        current: list[KVPair | None] = await batch.result()
        pending = [
            record
            for record, kv in zip(records, current, strict=True)
            if self._can_stage(kv)
        ]
        if not pending:
            return 0

        entries = [
            (
                KVPair(
                    record.key.to_bytes(Keyspace.TAGS),
                    Tag.staging(record.tag, source).to_bytes(),
                    Keyspace.TAGS,
                ),
                _log_kv(record),
            )
            for record in pending
        ]

        async with self._session.batch() as batch:
            for tag, log in entries:
                batch.put(tag)
                batch.put(log)
        await batch.result()
        return len(pending)

    async def put(self, record: Record) -> bool:
        """Write one version; return False when it changes nothing."""
        (written,) = await self.put_many([record])
        return written

    async def put_many(self, records: Sequence[Record]) -> list[bool]:
        """Write versions in one batch, plus two only when some already exist.

        1. one batch inserts every tag and value with ``overwrite=False`` and
           reads the META cells of the buckets not yet known: new versions
           are done here;
        2. only for the keys already present, read their current kind;
        3. overwrite the promotable tags (HINT/STAGING -> owned).

        A key identifies a single version, so an existing LOGS entry already
        holds the same value and is never rewritten. Never downgrades an
        owned version (LIVE/TOMBSTONE) and never folds the same version
        twice, so replays are free and the digest stays exact. Returns, per
        record, whether it was written.
        """
        if not records:
            return []

        tags = [_tag_kv(r) for r in records]
        written = await self._insert(records, tags)
        promoted = await self._promotable(records, tags, written)

        if promoted:
            async with self._session.batch() as batch:
                for i in promoted:
                    batch.put(tags[i])
                    written[i] = True
            await batch.result()

        for record, ok in zip(records, written, strict=True):
            if ok and record.tag.kind in OWNED_KINDS:
                self._fold(record.key)

        return written

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

    async def tags(self, keys: Sequence[Key]) -> list[Tag | None]:
        """Read tags without values, preserving input order and duplicates."""
        if not keys:
            return []

        async with self._session.batch() as batch:
            for key in keys:
                batch.get(KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))

        results: list[KVPair | None] = await batch.result()
        return [None if kv is None else Tag.from_bytes(kv.value) for kv in results]

    async def tag_page(
        self, start: bytes, limit: int
    ) -> tuple[list[KeyTag], bytes | None]:
        """Read a bounded TAGS page after an exclusive cursor.

        Log and skip unreadable entries. The next cursor follows the physical
        page, including skipped entries, so corrupt tags cannot stall a scan.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")

        async with self._session.batch() as batch:
            batch.list(Keyspace.TAGS, start=start, limit=limit)
        (page,) = await batch.result()

        entries: list[KeyTag] = []
        for kv in page:
            try:
                entries.append(
                    KeyTag(
                        Key.from_bytes(kv.key, Keyspace.TAGS),
                        Tag.from_bytes(kv.value),
                    )
                )
            except ValueError as exc:
                logger.warning(
                    "Skipping unreadable tag entry %s: %s", kv.key.hex(), exc
                )

        cursor = page[-1].key if len(page) == limit else None
        return entries, cursor

    async def load_records(self, entries: Sequence[KeyTag]) -> list[Record]:
        """Load values for selected tags; log and skip missing or invalid values."""
        if not entries:
            return []

        async with self._session.batch() as batch:
            for entry in entries:
                batch.get(KVRef(entry.key.to_bytes(Keyspace.LOGS), Keyspace.LOGS))
        logs: list[KVPair | None] = await batch.result()

        records: list[Record] = []
        for entry, log in zip(entries, logs, strict=True):
            record = self._scanned_record(entry, log)
            if record is not None:
                records.append(record)
        return records

    async def last_staged_keys(
        self, pids: Sequence[Pid], source: str
    ) -> dict[int, Key]:
        """Return the greatest TAGS key staged by source in each selected pid."""
        if not pids:
            return {}

        async with self._session.batch() as batch:
            for pid in pids:
                batch.last(
                    Keyspace.TAGS,
                    prefix=pid,
                    predicate=lambda kv: self._staged_by(kv, source),
                )
        results: list[KVPair | None] = await batch.result()

        keys = [
            Key.from_bytes(kv.key, Keyspace.TAGS) for kv in results if kv is not None
        ]
        return {key.pid.value: key for key in keys}

    async def put_hint_obligation(self, key: HintKey, reference: HintReference) -> None:
        """Persist a typed delivery obligation without modifying its version."""
        kv = KVPair(key.to_bytes(), reference.to_bytes(), Keyspace.META)
        async with self._session.batch() as batch:
            batch.put(kv)
        await batch.result()

    async def has_hints(self) -> bool:
        """Check delivery obligations, excluding digest metadata."""
        return await self._session.last(Keyspace.META, prefix=HintPrefix()) is not None

    async def hint_page(
        self, start: bytes | None, limit: int
    ) -> tuple[list[Hint], bytes | None]:
        """Read delivery obligations and their versions in one bounded snapshot.

        Log and skip malformed or missing data, advancing past invalid entries.
        A page may contain no valid hints while still having a next cursor.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")

        async with self._session.batch() as batch:
            batch.list(Keyspace.META, prefix=HintPrefix(), start=start, limit=limit)
        (page,) = await batch.result()

        hints = await self._load_hints(page)
        return hints, page[-1].key if page else None

    async def remove_hint_obligations(self, keys: Sequence[HintKey]) -> None:
        """Delete exact delivered obligations, leaving versions and digests intact."""
        if not keys:
            return

        refs = [KVRef(key.to_bytes(), Keyspace.META) for key in keys]
        async with self._session.batch() as batch:
            for ref in refs:
                batch.delete(ref)
        await batch.result()

    async def update_tags(self, entries: Sequence[KeyTag]) -> None:
        """Replace existing tags, retaining values and tracking ownership changes.

        Selection and authorization remain the caller's responsibility.
        Raise ValueError before modifying any tag if a version is incomplete.
        """
        if not entries:
            return

        keys = list(dict.fromkeys(entry.key for entry in entries))
        current = dict(zip(keys, await self.tags(keys), strict=True))
        if any(tag is None for tag in current.values()):
            raise ValueError("Cannot update a version without its TAGS entry")

        await self._require_values(keys)
        await self._load_cells(keys)

        async with self._session.batch() as batch:
            for entry in entries:
                batch.put(
                    KVPair(
                        entry.key.to_bytes(Keyspace.TAGS),
                        entry.tag.to_bytes(),
                        Keyspace.TAGS,
                    )
                )
        await batch.result()

        for entry in entries:
            before = current[entry.key]
            was_owned = before is not None and before.kind in OWNED_KINDS
            is_owned = entry.tag.kind in OWNED_KINDS
            if was_owned != is_owned:
                self._change_digest(entry.key, is_owned)
            current[entry.key] = entry.tag

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
        HINT and STAGING versions are skipped: they are not part of the digest.
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
    def _can_stage(kv: KVPair | None) -> bool:
        if kv is None:
            return True
        try:
            return Tag.from_bytes(kv.value).kind is TagKind.STAGING
        except ValueError as exc:
            logger.warning("Replacing unreadable staged tag %s: %s", kv.key.hex(), exc)
            return True

    @staticmethod
    def _record_at(key: Key, tag: KVPair | None, log: KVPair | None) -> Record | None:
        if tag is None or log is None:
            return None

        return Record(
            key=key, tag=Tag.from_bytes(tag.value), value=Value.from_bytes(log.value)
        )

    @staticmethod
    def _scanned_record(entry: KeyTag, log: KVPair | None) -> Record | None:
        key = entry.key
        if log is None:
            logger.warning(
                "Skipping tag entry without value: pid=%d key=%s", key.pid.value, key
            )
            return None

        try:
            return Record(key=key, tag=entry.tag, value=Value.from_bytes(log.value))
        except (ValueError, OverflowError) as exc:
            logger.warning(
                "Skipping unreadable value: pid=%d key=%s: %s", key.pid.value, key, exc
            )
            return None

    @staticmethod
    def _staged_by(kv: KVPair, source: str) -> bool:
        try:
            tag = Tag.from_bytes(kv.value)
            if tag.kind is not TagKind.STAGING:
                return False
            owner, _ = tag.payload.extract_source_prefix()
            return owner == source
        except ValueError as exc:
            logger.warning("Skipping unreadable staged tag %s: %s", kv.key.hex(), exc)
            return False

    async def _load_hints(self, page: Sequence[KVPair]) -> list[Hint]:
        selected: list[tuple[KVPair, Key]] = []
        for kv in page:
            try:
                HintKey.from_bytes(kv.key)
                reference = HintReference.from_bytes(kv.value)
                selected.append((kv, reference.key))
            except (ValueError, OverflowError) as exc:
                self._invalid_hint(kv, exc)
        if not selected:
            return []

        async with self._session.batch() as batch:
            for _, key in selected:
                batch.get(KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))
                batch.get(KVRef(key.to_bytes(Keyspace.LOGS), Keyspace.LOGS))
        results: list[KVPair | None] = await batch.result()

        hints: list[Hint] = []
        for index, (kv, _) in enumerate(selected):
            hint = self._load_hint(kv, results[index * 2], results[index * 2 + 1])
            if hint is not None:
                hints.append(hint)
        return hints

    @staticmethod
    def _invalid_hint(kv: KVPair, exc: Exception) -> None:
        logger.warning(
            "Cannot decode hint obligation: meta_key=%s error=%s", kv.key.hex(), exc
        )

    def _load_hint(
        self, kv: KVPair, kv_tag: KVPair | None, kv_log: KVPair | None
    ) -> Hint | None:
        try:
            return self._decode_hint(kv, kv_tag, kv_log)
        except (ValueError, OverflowError) as exc:
            self._invalid_hint(kv, exc)
            return None

    @staticmethod
    def _decode_hint(
        kv: KVPair, kv_tag: KVPair | None, kv_log: KVPair | None
    ) -> Hint | None:
        obligation = HintKey.from_bytes(kv.key)
        key = HintReference.from_bytes(kv.value).key
        if kv_tag is None or kv_log is None:
            logger.warning(
                "Hint obligation references a missing version: intent=%s hlc=%s key=%s",
                obligation.intent,
                obligation.hlc,
                key,
            )
            return None

        tag = Tag.from_bytes(kv_tag.value)
        original = Tag.unhint(tag) if tag.kind is TagKind.HINT else tag
        record = Record(
            key=key, value=Value.from_bytes(kv_log.value), tag=Tag.hint(original)
        )
        return Hint(record=record, intent=obligation.intent, hlc=obligation.hlc)

    def _change_digest(self, key: Key, owned: bool) -> None:
        cell = DigestCell.of(key)
        for ref in self._cell_keys(key):
            delta = self._deltas.get(ref, DigestCell())
            self._deltas[ref] = delta + cell if owned else delta - cell

    async def _require_values(self, keys: Sequence[Key]) -> None:
        async with self._session.batch() as batch:
            for key in keys:
                batch.exists(KVRef(key.to_bytes(Keyspace.LOGS), Keyspace.LOGS))

        present: list[bool] = await batch.result()
        if not all(present):
            raise ValueError("Cannot update a version without its LOGS entry")

    async def _load_cells(self, keys: Sequence[Key]) -> None:
        refs = self._unknown_cells(keys)

        if not refs:
            return

        async with self._session.batch() as batch:
            for ref in refs:
                batch.get(KVRef(ref, Keyspace.META))

        results: list[KVPair | None] = await batch.result()

        for ref, kv in zip(refs, results, strict=True):
            self._stored[ref] = _cell(kv)

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

    async def _promotable(
        self, records: Sequence[Record], tags: Sequence[KVPair], inserted: list[bool]
    ) -> list[int]:
        """Return the indexes of conflicting records that upgrade their tag.

        Records are decided in order against a running view of the stored
        kind, so the same key repeated in one call is promoted (and folded)
        at most once.
        """
        conflicts = [i for i, ok in enumerate(inserted) if not ok]
        if not conflicts:
            return []

        async with self._session.batch() as batch:
            for i in conflicts:
                batch.get(KVRef(tags[i].key, Keyspace.TAGS))

        stored: list[KVPair | None] = await batch.result()
        current = {
            tags[i].key: _kind(kv) for i, kv in zip(conflicts, stored, strict=True)
        }

        promoted: list[int] = []
        for i in conflicts:
            kind, before = records[i].tag.kind, current[tags[i].key]
            if kind != before and before not in OWNED_KINDS:
                current[tags[i].key] = kind
                promoted.append(i)

        return promoted

    def _cell_keys(self, key: Key) -> tuple[bytes, bytes]:
        """Return the META keys of the root and bucket cells covering `key`."""
        pid = key.pid.value

        return root_key(pid), bucket_key(pid, key.ts.wall_ms >> self._bucket_shift)

    def _unknown_cells(self, keys: Sequence[Key]) -> list[bytes]:
        refs = {ref for key in keys for ref in self._cell_keys(key)}
        return sorted(refs - self._stored.keys())

    def _fold(self, key: Key) -> None:
        self._change_digest(key, owned=True)

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
            session = GroupSession(raw, bucket_shift=DEFAULT_BUCKET_SHIFT)
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


def _kind(kv: KVPair | None) -> TagKind | None:
    return None if kv is None else Tag.from_bytes(kv.value).kind


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
