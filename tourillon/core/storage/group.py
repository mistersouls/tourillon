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
    Hint,
    HintKey,
    HintReference,
    Key,
    Keyspace,
    KeyTag,
    Pid,
    Record,
    Tag,
    TagKind,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.storage.session import GroupSessionManager

logger = _logger.storage()

SCAN_BATCH_SIZE = 128
COMMIT_BATCH_SIZE = 256


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
        session_manager: GroupSessionManager,
        partitioner: Partitioner,
        segment: tuple[int, int] | None = None,
    ) -> None:
        self._source = source
        self._session_manager = session_manager
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

        async with self._session_manager.session(write=True) as session:
            return await session.stage_many(records, self._source)

    async def last_keys(self) -> Mapping[int, Key]:
        """Return, for every pid, the greatest key staged from the source."""
        async with self._session_manager.session(write=False) as session:
            pids = [Pid(pid) for pid in range(self._start, self._end + 1)]
            return await session.last_staged_keys(pids, self._source)

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
            count, position = await self._commit_page(position, batch_size)
            activated += count
        return activated

    async def _scan_page(
        self,
        position: bytes,
        jumps: Mapping[int, bytes],
        kinds: frozenset[TagKind] | None,
        limit: int,
    ) -> tuple[list[Record], bytes | None]:
        async with self._session_manager.session(write=False) as session:
            entries, cursor = await session.tag_page(position, limit)
            selected, next_position = self._select_page(entries, jumps, kinds, cursor)
            records = await session.load_records(selected)
        return records, next_position

    def _select_page(
        self,
        entries: Sequence[KeyTag],
        jumps: Mapping[int, bytes],
        kinds: frozenset[TagKind] | None,
        cursor: bytes | None,
    ) -> tuple[list[KeyTag], bytes | None]:
        """Pick the records of a page and return where the next page starts.

        The next position is None once the segment or the keyspace is
        exhausted. Reaching a pid whose resume key is ahead of the current
        key jumps straight to that resume key.
        """
        selected: list[KeyTag] = []
        for entry in entries:
            pid = entry.key.pid.value
            if pid > self._end:
                return selected, None
            jump = jumps.get(pid)
            if jump is not None and entry.key.to_bytes(Keyspace.TAGS) <= jump:
                return selected, jump
            if kinds is None or entry.tag.kind in kinds:
                selected.append(entry)

        return selected, cursor

    async def _commit_page(
        self,
        position: bytes,
        limit: int,
    ) -> tuple[int, bytes | None]:
        async with self._session_manager.session(write=True) as session:
            entries, cursor = await session.tag_page(position, limit)
            unstaged, next_position = self._activation_page(entries, cursor)
            if unstaged:
                await session.update_tags(unstaged)

        return len(unstaged), next_position

    def _activation_page(
        self, entries: Sequence[KeyTag], cursor: bytes | None
    ) -> tuple[list[KeyTag], bytes | None]:
        unstaged: list[KeyTag] = []
        for entry in entries:
            if entry.key.pid.value > self._end:
                return unstaged, None
            item = self._unstage_owned_tag(entry)
            if item is not None:
                unstaged.append(item)

        return unstaged, cursor

    def _unstage_owned_tag(self, entry: KeyTag) -> KeyTag | None:
        if not self._is_owner(entry.tag):
            return None

        try:
            unstaged = Tag.unstaging(entry.tag, self._source)
        except ValueError as exc:
            logger.warning("Cannot activate staged entry %s: %s", entry.key, exc)
            return None
        return KeyTag(key=entry.key, tag=unstaged)

    def _is_owner(self, tag: Tag) -> bool:
        try:
            if tag.kind != TagKind.STAGING:
                return False

            src, _ = tag.payload.extract_source_prefix()
            return src == self._source
        except ValueError as exc:
            logger.warning("Cannot identify staging source: %s", exc)
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

    def _pid_bounds(self, segment: tuple[int, int] | None) -> tuple[int, int]:
        """Return the inclusive pid bounds handled: the group, clipped to the segment.

        Raise ValueError when the segment is malformed or does not intersect
        the group of the session provider.
        """
        group = self._session_manager.group_id
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
    def __init__(self, session_manager: GroupSessionManager) -> None:
        self._session_manager = session_manager

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
        async with self._session_manager.session(write=writable) as session:
            owned = await session.exists_many([record.key for record in records])
            pending = [
                record
                for record, present in zip(records, owned, strict=True)
                if writable and not present
            ]
            await session.put_many(pending)
        return [present or writable for present in owned], len(pending)

    async def put(self, hint: Hint) -> HintKey:
        """Persist an obligation without downgrading an existing owned version."""
        record = hint.record
        hint_key = HintKey(intent=hint.intent, hlc=hint.hlc)
        reference = HintReference(record.key)
        async with self._session_manager.session(write=True) as session:
            (tag,) = await session.tags([record.key])
            if tag is None or tag.kind not in {TagKind.LIVE, TagKind.TOMBSTONE}:
                await session.put(record)

            await session.put_hint_obligation(hint_key, reference)
        return hint_key

    async def pending(self) -> bool:
        """Return whether a persisted delivery obligation remains."""
        async with self._session_manager.session(write=False) as session:
            return await session.has_hints()

    async def iter(self, batch_size: int = SCAN_BATCH_SIZE) -> AsyncIterator[Hint]:
        """Read bounded META pages without holding a transaction across delivery."""
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        position: bytes | None = None
        while True:
            async with self._session_manager.session(write=False) as session:
                hints, position = await session.hint_page(position, batch_size)
            if position is None:
                return
            for hint in hints:
                yield hint

    async def satisfy(self, hints: Sequence[Hint]) -> None:
        """Idempotently delete exact delivered obligations, leaving versions intact."""
        if not hints:
            return
        async with self._session_manager.session(write=True) as session:
            await session.remove_hint_obligations(
                [HintKey(hint.intent, hint.hlc) for hint in hints]
            )
