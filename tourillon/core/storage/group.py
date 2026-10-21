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

from collections.abc import AsyncIterator, Mapping, Sequence

from tourillon.core.log import logger as _logger
from tourillon.core.models.storageschema import (
    PID_SIZE,
    Hint,
    HintKey,
    HintPrefix,
    HintReference,
    Key,
    Keyspace,
    KVPair,
    KVRef,
    PartitionAddress,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports.storage import Session, SessionProvider

logger = _logger.storage()

SCAN_BATCH_SIZE = 128
COMMIT_BATCH_SIZE = 256


def _decode_tag_kv(
    kv: KVPair, kinds: frozenset[TagKind] | None
) -> tuple[Key, Tag] | None:
    """Decode a TAGS entry and drop it unless its kind is wanted.

    Filtering here, on the tag alone, is what keeps a scan proportional to the
    number of matching entries: the LOGS payload is only fetched for survivors.
    """
    try:
        tag = Tag.from_bytes(kv.value)
        if kinds is not None and tag.kind not in kinds:
            return None
        return Key.from_bytes(kv.key, Keyspace.TAGS), tag
    except ValueError as exc:
        logger.warning("Skipping unreadable tag entry %s: %s", kv.key.hex(), exc)
        return None


def _build_record(key: Key, tag: Tag, kv_log: KVPair | None) -> Record | None:
    if kv_log is None:
        logger.warning(
            "Skipping tag entry without value: pid=%d key=%s", key.pid.value, key
        )
        return None
    try:
        value = Value.from_bytes(kv_log.value)
    except (ValueError, OverflowError) as exc:
        logger.warning(
            "Skipping unreadable value: pid=%d key=%s: %s", key.pid.value, key, exc
        )
        return None
    return Record(key=key, tag=tag, value=value)


async def _load_records(
    session: Session, selected: Sequence[tuple[Key, Tag]]
) -> list[Record]:
    """Batch-load the LOGS payloads of already-selected tags."""
    if not selected:
        return []

    async with session.batch() as batch:
        for key, _ in selected:
            batch.get(KVRef(key=key.to_bytes(Keyspace.LOGS), keyspace=Keyspace.LOGS))
    logs = await batch.result()

    records: list[Record] = []
    for (key, tag), kv_log in zip(selected, logs, strict=True):
        record = _build_record(key, tag, kv_log)
        if record is not None:
            records.append(record)
    return records


class PartitionGroupStore:
    def __init__(self, session_provider: SessionProvider) -> None:
        self._session_provider = session_provider

    async def exists(self, key: Key) -> bool:
        """Check exact owned-version presence without loading its value."""
        async with self._session_provider.session(write=False) as session:
            kv = await session.get(KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))
            if kv is None:
                return False
            tag = Tag.from_bytes(kv.value)
            if tag.kind not in {TagKind.LIVE, TagKind.TOMBSTONE}:
                return False
            return await session.exists(
                KVRef(key.to_bytes(Keyspace.LOGS), Keyspace.LOGS)
            )

    async def put(self, record: Record) -> None:
        async with self._session_provider.session(write=True) as session:
            kv_tag = KVPair(
                key=record.key.to_bytes(Keyspace.TAGS),
                value=record.tag.to_bytes(),
                keyspace=Keyspace.TAGS,
            )
            kv_log = KVPair(
                key=record.key.to_bytes(Keyspace.LOGS),
                value=record.value.to_bytes(),
                keyspace=Keyspace.LOGS,
            )
            await session.put(kv_tag)
            await session.put(kv_log)

    async def get(self, addr: PartitionAddress) -> Record | None:
        async with self._session_provider.session(write=False) as session:
            kv_log = await session.last(
                keyspace=Keyspace.LOGS,
                prefix=addr,
            )
            if kv_log is None:
                return None

            key = Key.from_bytes(kv_log.key, Keyspace.LOGS)
            kv_tag = await session.get(
                KVRef(key=key.to_bytes(Keyspace.TAGS), keyspace=Keyspace.TAGS)
            )
            if kv_tag is None:
                return None

            tag = Tag.from_bytes(kv_tag.value)
            value = Value.from_bytes(kv_log.value)
            return Record(key=key, tag=tag, value=value)

    async def get_at(self, key: Key) -> Record | None:
        """Read the version stored at one exact timestamp.

        `get` answers "what is current for this address"; repair needs "do you
        hold this particular version". The physical key already spells out
        `pid|address|ts`, so the lookup is a point read, not a scan.
        """
        async with self._session_provider.session(write=False) as session:
            kv_log = await session.get(
                KVRef(key=key.to_bytes(Keyspace.LOGS), keyspace=Keyspace.LOGS)
            )
            if kv_log is None:
                return None

            kv_tag = await session.get(
                KVRef(key=key.to_bytes(Keyspace.TAGS), keyspace=Keyspace.TAGS)
            )
            if kv_tag is None:
                return None

            return Record(
                key=key,
                tag=Tag.from_bytes(kv_tag.value),
                value=Value.from_bytes(kv_log.value),
            )


class PartitionGroupStaging:
    """Staging area of one partition group for the records of one source.

    Staged records live in the regular TAGS and LOGS keyspaces with a
    STAGING tag remembering the source and the original tag: they stay
    invisible to reads until `commit` restores the original tag.

    No metadata is kept beside the data. The staged records themselves are
    the durable progress of a transfer (`last_keys`), every operation is
    idempotent and runs in short transactions, so a transfer interrupted at
    any point (network failure, crash, restart) resumes from what is staged.
    """

    def __init__(
        self,
        source: str,
        session_provider: SessionProvider,
        partitioner: Partitioner,
        segment: tuple[int, int] | None = None,
    ) -> None:
        self._source = source
        self._session_provider = session_provider
        self._partitioner = partitioner
        self._start, self._end = self._pid_bounds(segment)

    async def scan(
        self,
        resumes: Mapping[int, Key] | None = None,
        after: Key | None = None,
        kinds: frozenset[TagKind] | None = None,
        batch_size: int = SCAN_BATCH_SIZE,
    ) -> AsyncIterator[Record]:
        """Yield the records of the handled pids in TAGS key order (pid, ts, address).

        Keys lower than or equal to `after`, or to the resume key of their
        pid, are skipped; when `kinds` is set, only records tagged with one
        of them are yielded. Every page is read in its own short read
        transaction, which is never held while the caller consumes records.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")

        jumps = self._resume_jumps(resumes or {})
        position: bytes | None = self._scan_origin(after)
        while position is not None:
            records, position = await self._scan_page(
                position, jumps, kinds, batch_size
            )
            for record in records:
                yield record

    async def stage(self, records: Sequence[Record]) -> int:
        """Stage records received from the source and return how many were written.

        All records are written in one transaction. A record already active
        locally (non STAGING tag) is left untouched: a version is immutable,
        so the local copy is identical and must stay visible. Staging the same
        record twice (retry, resume) rewrites the same entry.
        """
        if not records:
            return 0

        async with self._session_provider.session(write=True) as session:
            existing = await self._existing_tags(session, records)
            pending = [
                record
                for record, kv_tag in zip(records, existing, strict=True)
                if self._needs_staging(kv_tag)
            ]
            await self._put_staged(session, pending)
        return len(pending)

    async def last_keys(self) -> Mapping[int, Key]:
        """Return, for every pid, the greatest key staged from the source."""
        results: dict[int, Key] = {}

        async with self._session_provider.session(write=False) as session:
            async with session.batch() as batch:
                for pid in range(self._start, self._end + 1):
                    batch.last(
                        keyspace=Keyspace.TAGS,
                        prefix=Pid(pid),
                        predicate=self._is_owner,
                    )

            for kv in await batch.result():
                if kv is not None:
                    last_key = Key.from_bytes(kv.key, Keyspace.TAGS)
                    results[last_key.pid.value] = last_key

        return results

    async def commit(self, batch_size: int = COMMIT_BATCH_SIZE) -> int:
        """Activate the records staged from the source and return how many were.

        Pages are activated in key order, each in its own write transaction:
        a crash leaves an activated prefix followed by staged records, and
        running `commit` again finishes the job.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")

        activated = 0
        position: bytes | None = Pid(self._start).to_bytes()
        while position is not None:
            unstaged, position = await self._commit_page(position, batch_size)
            if unstaged:
                await self._flush_tags(unstaged)
                activated += len(unstaged)
        return activated

    async def _scan_page(
        self,
        position: bytes,
        jumps: Mapping[int, bytes],
        kinds: frozenset[TagKind] | None,
        limit: int,
    ) -> tuple[list[Record], bytes | None]:
        async with self._session_provider.session(write=False) as session:
            kvs = await self._list_tags(session, position, limit)
            selected, next_position = self._select_page(kvs, jumps, kinds, limit)
            records = await _load_records(session, selected)
        return records, next_position

    def _select_page(
        self,
        kvs: Sequence[KVPair],
        jumps: Mapping[int, bytes],
        kinds: frozenset[TagKind] | None,
        limit: int,
    ) -> tuple[list[tuple[Key, Tag]], bytes | None]:
        """Pick the records of a page and return where the next page starts.

        The next position is None once the segment or the keyspace is
        exhausted. Reaching a pid whose resume key is ahead of the current
        key jumps straight to that resume key.
        """
        selected: list[tuple[Key, Tag]] = []
        for kv in kvs:
            pid = self._pid_of(kv.key)
            if pid > self._end:
                return selected, None
            jump = jumps.get(pid)
            if jump is not None and kv.key <= jump:
                return selected, jump
            item = _decode_tag_kv(kv, kinds)
            if item is not None:
                selected.append(item)

        if len(kvs) < limit:
            return selected, None
        return selected, kvs[-1].key

    async def _commit_page(
        self,
        position: bytes,
        limit: int,
    ) -> tuple[list[KVPair], bytes | None]:
        async with self._session_provider.session(write=False) as session:
            kvs = await self._list_tags(session, position, limit)

        unstaged: list[KVPair] = []
        for kv in kvs:
            if self._pid_of(kv.key) > self._end:
                return unstaged, None
            item = self._unstage_owned_kv(kv)
            if item is not None:
                unstaged.append(item)

        next_position = kvs[-1].key if len(kvs) == limit else None
        return unstaged, next_position

    async def _flush_tags(self, kv_tags: Sequence[KVPair]) -> None:
        async with self._session_provider.session(write=True) as session:
            async with session.batch() as batch:
                for kv in kv_tags:
                    batch.put(kv)
            await batch.result()

    @staticmethod
    async def _existing_tags(
        session: Session, records: Sequence[Record]
    ) -> list[KVPair | None]:
        async with session.batch() as batch:
            for record in records:
                batch.get(
                    KVRef(
                        key=record.key.to_bytes(Keyspace.TAGS), keyspace=Keyspace.TAGS
                    )
                )
        return await batch.result()

    async def _put_staged(self, session: Session, records: Sequence[Record]) -> None:
        if not records:
            return

        async with session.batch() as batch:
            for record in records:
                batch.put(
                    KVPair(
                        key=record.key.to_bytes(Keyspace.TAGS),
                        value=Tag.staging(record.tag, self._source).to_bytes(),
                        keyspace=Keyspace.TAGS,
                    )
                )
                batch.put(
                    KVPair(
                        key=record.key.to_bytes(Keyspace.LOGS),
                        value=record.value.to_bytes(),
                        keyspace=Keyspace.LOGS,
                    )
                )
        await batch.result()

    @staticmethod
    async def _list_tags(session: Session, start: bytes, limit: int) -> list[KVPair]:
        async with session.batch() as batch:
            batch.list(keyspace=Keyspace.TAGS, start=start, limit=limit)
        (kvs,) = await batch.result()
        return kvs

    @staticmethod
    def _needs_staging(kv_tag: KVPair | None) -> bool:
        if kv_tag is None:
            return True
        try:
            return Tag.from_bytes(kv_tag.value).kind == TagKind.STAGING
        except ValueError:
            return True

    def _unstage_owned_kv(self, kv: KVPair) -> KVPair | None:
        if not self._is_owner(kv):
            return None

        try:
            unstaged = Tag.unstaging(Tag.from_bytes(kv.value), self._source)
        except ValueError as exc:
            logger.warning("Cannot activate staged entry %s: %s", kv.key.hex(), exc)
            return None
        return KVPair(key=kv.key, value=unstaged.to_bytes(), keyspace=Keyspace.TAGS)

    def _is_owner(self, item: KVPair) -> bool:
        try:
            tag = Tag.from_bytes(item.value)
            if tag.kind != TagKind.STAGING:
                return False

            src, _ = tag.payload.extract_source_prefix()
            return src == self._source
        except ValueError:
            return False

    def _resume_jumps(self, resumes: Mapping[int, Key]) -> dict[int, bytes]:
        return {
            pid: key.to_bytes(Keyspace.TAGS)
            for pid, key in resumes.items()
            if self._start <= pid <= self._end
        }

    def _scan_origin(self, after: Key | None) -> bytes:
        """Return the exclusive start of the scan.

        The bare pid prefix sorts before every key of that pid, so starting
        "after" it includes the whole first pid.
        """
        origin = Pid(self._start).to_bytes()
        if after is None:
            return origin
        return max(origin, after.to_bytes(Keyspace.TAGS))

    @staticmethod
    def _pid_of(key: bytes) -> int:
        return int.from_bytes(key[:PID_SIZE], "big")

    def _pid_bounds(self, segment: tuple[int, int] | None) -> tuple[int, int]:
        """Return the inclusive pid bounds handled: the group, clipped to the segment.

        Raise ValueError when the segment is malformed or does not intersect
        the group of the session provider.
        """
        group = self._session_provider.group_id
        group_start, group_end = self._partitioner.group_bounds(group)
        if segment is None:
            return group_start, group_end

        lo, hi = segment
        start, end = max(group_start, lo), min(group_end, hi)
        if lo > hi or start > end:
            raise ValueError(
                f"segment [{lo}, {hi}] is not handled by group {group} [{group_start}, {group_end}]"
            )
        return start, end


class PartitionGroupHint:
    def __init__(self, session_provider: SessionProvider) -> None:
        self._session_provider = session_provider

    async def promote(
        self, records: Sequence[Record], *, writable: bool = True
    ) -> tuple[list[bool], int]:
        """Confirm local copies and activate missing ones when writes are allowed.

        Return confirmations in input order and the number promoted. Obligations
        are untouched. Checks and writes share one short transaction.
        """
        if not records:
            return [], 0
        if any(
            record.tag.kind not in {TagKind.LIVE, TagKind.TOMBSTONE}
            for record in records
        ):
            raise ValueError("promotion requires LIVE or TOMBSTONE records")
        async with self._session_provider.session(write=writable) as session:
            owned = await self._owned_versions(session, records)
            pending = [
                record
                for record, present in zip(records, owned, strict=True)
                if writable and not present
            ]
            await self._promote_records(session, pending)
        return [present or writable for present in owned], len(pending)

    async def _owned_versions(
        self, session: Session, records: Sequence[Record]
    ) -> list[bool]:
        async with session.batch() as batch:
            for record in records:
                batch.get(KVRef(record.key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))
                batch.exists(KVRef(record.key.to_bytes(Keyspace.LOGS), Keyspace.LOGS))
        results = await batch.result()
        return [
            self._owned_tag(results[index * 2]) and results[index * 2 + 1]
            for index in range(len(records))
        ]

    @staticmethod
    def _owned_tag(kv: KVPair | None) -> bool:
        if kv is None:
            return False
        tag = Tag.from_bytes(kv.value)
        return tag.kind in {TagKind.LIVE, TagKind.TOMBSTONE}

    @staticmethod
    async def _promote_records(
        session: Session, records: Sequence[Record]
    ) -> None:
        if not records:
            return
        async with session.batch() as batch:
            for record in records:
                batch.put(
                    KVPair(
                        record.key.to_bytes(Keyspace.TAGS),
                        record.tag.to_bytes(),
                        Keyspace.TAGS,
                    )
                )
                batch.put(
                    KVPair(
                        record.key.to_bytes(Keyspace.LOGS),
                        record.value.to_bytes(),
                        Keyspace.LOGS,
                    )
                )
        await batch.result()

    async def put(self, hint: Hint) -> HintKey:
        """Persist an obligation without downgrading an existing owned version."""
        record = hint.record
        hint_key = HintKey(intent=hint.intent, hlc=hint.hlc)
        reference = HintReference(record.key)
        async with self._session_provider.session(write=True) as session:
            kv_tag = await session.get(
                KVRef(record.key.to_bytes(Keyspace.TAGS), Keyspace.TAGS)
            )
            owned = False
            if kv_tag is not None:
                tag = Tag.from_bytes(kv_tag.value)
                owned = tag.kind in {TagKind.LIVE, TagKind.TOMBSTONE}
            async with session.batch() as batch:
                if not owned:
                    batch.put(
                        KVPair(
                            record.key.to_bytes(Keyspace.TAGS),
                            record.tag.to_bytes(),
                            Keyspace.TAGS,
                        )
                    )
                    batch.put(
                        KVPair(
                            record.key.to_bytes(Keyspace.LOGS),
                            record.value.to_bytes(),
                            Keyspace.LOGS,
                        )
                    )
                batch.put(
                    KVPair(
                        hint_key.to_bytes(),
                        reference.to_bytes(),
                        Keyspace.META,
                    )
                )
            await batch.result()
        return hint_key

    async def pending(self) -> bool:
        """Return whether a persisted delivery obligation remains."""
        async with self._session_provider.session(write=False) as session:
            latest = session.last(Keyspace.META, prefix=HintPrefix())
            return await latest is not None

    async def iter(self, batch_size: int = SCAN_BATCH_SIZE) -> AsyncIterator[Hint]:
        """Read bounded META pages without holding a transaction across delivery."""
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        position: bytes | None = None
        while True:
            async with self._session_provider.session(write=False) as session:
                async with session.batch() as batch:
                    batch.list(
                        Keyspace.META,
                        prefix=HintPrefix(),
                        start=position,
                        limit=batch_size,
                    )
                (page,) = await batch.result()
                hints = await self._load_hints(session, page)
            if not page:
                return
            position = page[-1].key
            for hint in hints:
                if hint is not None:
                    yield hint

    async def _load_hints(
        self, session: Session, page: Sequence[KVPair]
    ) -> list[Hint | None]:
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
        async with session.batch() as batch:
            for _, key in selected:
                batch.get(KVRef(key.to_bytes(Keyspace.TAGS), Keyspace.TAGS))
                batch.get(KVRef(key.to_bytes(Keyspace.LOGS), Keyspace.LOGS))
        results = await batch.result()
        return [
            self._load_hint(kv, results[index * 2], results[index * 2 + 1])
            for index, (kv, _) in enumerate(selected)
        ]

    @staticmethod
    def _invalid_hint(kv: KVPair, exc: Exception) -> None:
        logger.warning(
            "Cannot decode hint obligation: meta_key=%s error=%s",
            kv.key.hex(),
            exc,
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
        reference = HintReference.from_bytes(kv.value)
        key = reference.key
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
            key=key,
            value=Value.from_bytes(kv_log.value),
            tag=Tag.hint(original),
        )
        return Hint(record=record, intent=obligation.intent, hlc=obligation.hlc)

    async def satisfy(self, hints: Sequence[Hint]) -> None:
        """Idempotently delete exact delivered obligations, leaving versions intact."""
        if not hints:
            return
        async with self._session_provider.session(write=True) as session:
            async with session.batch() as batch:
                for hint in hints:
                    key = HintKey(hint.intent, hint.hlc)
                    batch.delete(KVRef(key.to_bytes(), Keyspace.META))
            await batch.result()
