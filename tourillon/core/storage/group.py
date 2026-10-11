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


class PartitionGroupStore:
    def __init__(self, session_provider: SessionProvider) -> None:
        self._session_provider = session_provider

    async def put(self, record: Record) -> None:
        async with self._session_provider.session(write=True) as session:
            kv_tag = KVPair(
                key=record.key.to_bytes(Keyspace.TAGS),
                value=record.tag.to_bytes(),
                keyspace=Keyspace.TAGS
            )
            kv_log = KVPair(
                key=record.key.to_bytes(Keyspace.LOGS),
                value=record.value.to_bytes(),
                keyspace=Keyspace.LOGS
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
            kv_tag = await session.get(KVRef(key=key.to_bytes(Keyspace.TAGS), keyspace=Keyspace.TAGS))
            if kv_tag is None:
                return None

            tag = Tag.from_bytes(kv_tag.value)
            value = Value.from_bytes(kv_log.value)
            return Record(key=key, tag=tag, value=value)


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
            records, position = await self._scan_page(position, jumps, kinds, batch_size)
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
            records = await self._load_records(session, selected)
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
            item = self._decode_tag_kv(kv, kinds)
            if item is not None:
                selected.append(item)

        if len(kvs) < limit:
            return selected, None
        return selected, kvs[-1].key

    async def _load_records(
        self,
        session: Session,
        selected: Sequence[tuple[Key, Tag]],
    ) -> list[Record]:
        if not selected:
            return []

        async with session.batch() as batch:
            for key, _ in selected:
                batch.get(KVRef(key=key.to_bytes(Keyspace.LOGS), keyspace=Keyspace.LOGS))
        logs = await batch.result()

        records: list[Record] = []
        for (key, tag), kv_log in zip(selected, logs, strict=True):
            record = self._build_record(key, tag, kv_log)
            if record is not None:
                records.append(record)
        return records

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
    async def _existing_tags(session: Session, records: Sequence[Record]) -> list[KVPair | None]:
        async with session.batch() as batch:
            for record in records:
                batch.get(KVRef(key=record.key.to_bytes(Keyspace.TAGS), keyspace=Keyspace.TAGS))
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

    @staticmethod
    def _decode_tag_kv(
        kv: KVPair,
        kinds: frozenset[TagKind] | None,
    ) -> tuple[Key, Tag] | None:
        try:
            tag = Tag.from_bytes(kv.value)
            if kinds is not None and tag.kind not in kinds:
                return None
            return Key.from_bytes(kv.key, Keyspace.TAGS), tag
        except ValueError as exc:
            logger.warning("Skipping unreadable tag entry %s: %s", kv.key.hex(), exc)
            return None

    @staticmethod
    def _build_record(key: Key, tag: Tag, kv_log: KVPair | None) -> Record | None:
        if kv_log is None:
            logger.warning("Skipping tag entry without value: pid=%d key=%s", key.pid.value, key)
            return None
        try:
            value = Value.from_bytes(kv_log.value)
        except (ValueError, OverflowError) as exc:
            logger.warning("Skipping unreadable value: pid=%d key=%s: %s", key.pid.value, key, exc)
            return None
        return Record(key=key, tag=tag, value=value)

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

    async def iter(self, batch_size: int = 32) -> AsyncIterator[Record]:
        async with self._session_provider.session(write=False) as session:
            async for kv in session.iter(keyspace=Keyspace.TAGS, batch_size=batch_size):
                record = await self._record(kv, session)
                if record is None or record.tag.kind != TagKind.HINT:
                    continue

                yield record
    
    async def satisfy(self, records: Sequence[Record]) -> bool:
        """Restore satisfied hints to their original tags and return whether any changed.

        Like staging commit, this only rewrites the tag entry in `TAGS`: the
        durable LOGS payload stays untouched so a later GC can clean it up.
        """
        if not records:
            return False

        async with self._session_provider.session(write=True) as session:
            existing = await self._existing_tags(session, records)
            restored = [
                kv
                for record, kv_tag in zip(records, existing, strict=True)
                if (kv := self._restore_hint_kv(record, kv_tag)) is not None
            ]
            if not restored:
                return False

            async with session.batch() as batch:
                for kv in restored:
                    batch.put(kv)
            await batch.result()
        return True

    @staticmethod
    async def _existing_tags(session: Session, records: Sequence[Record]) -> list[KVPair | None]:
        async with session.batch() as batch:
            for record in records:
                batch.get(KVRef(key=record.key.to_bytes(Keyspace.TAGS), keyspace=Keyspace.TAGS))
        return await batch.result()

    @staticmethod
    def _restore_hint_kv(record: Record, kv_tag: KVPair | None) -> KVPair | None:
        if kv_tag is None:
            return None

        try:
            tag = Tag.from_bytes(kv_tag.value)
            if tag.kind != TagKind.HINT:
                return None
            restored = Tag.unhint(tag)
        except ValueError as exc:
            logger.warning(
                "Cannot satisfy hinted entry %s: %s",
                record.key,
                exc
            )
            return None

        return KVPair(
            key=record.key.to_bytes(Keyspace.TAGS),
            value=restored.to_bytes(),
            keyspace=Keyspace.TAGS,
        )

    @staticmethod
    async def _record(kv: KVPair, session: Session) -> Record | None:
        try:
            key = Key.from_bytes(kv.key, Keyspace.TAGS)
            tag = Tag.from_bytes(kv.value)
        except ValueError as exc:
            logger.warning("Skipping unreadable tag entry %s: %s", kv.key.hex(), exc)
            return None

        kv_log = await session.get(KVRef(key=key.to_bytes(Keyspace.LOGS), keyspace=Keyspace.LOGS))
        if kv_log is None:
            logger.warning("Skipping tag entry without value: pid=%d key=%s", key.pid.value, key)
            return None

        try:
            value = Value.from_bytes(kv_log.value)
        except (ValueError, OverflowError) as exc:
            logger.warning("Skipping unreadable value: pid=%d key=%s: %s", key.pid.value, key, exc)
            return None

        return Record(key=key, tag=tag, value=value)
