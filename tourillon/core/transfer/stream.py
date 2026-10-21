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

from tourillon.core.exceptions import ProcessError, TransferError
from tourillon.core.models.storageschema import Key, Record, TagKind
from tourillon.core.models.transfer import RangeTransfer, TransferChunk
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports.storage import SessionProvider, Storage
from tourillon.core.storage.group import PartitionGroupStaging
from tourlib import Serializer
from tourlib.constant import MAX_ENVELOPE_PAYLOAD, MAX_USER_ENVELOPE_PAYLOAD

# Size of the chunks a stream is cut into; a record frame may span chunks.
DEFAULT_CHUNK_SIZE = 1024 * 1024

# Largest chunk: its bytes plus the chunk metadata (a few hundred bytes) must
# fit in one internode envelope, which carries at most MAX_ENVELOPE_PAYLOAD.
MAX_CHUNK_SIZE = MAX_USER_ENVELOPE_PAYLOAD

# Largest record frame. A record is at most a 16 MiB value plus a key and a
# tag of a few hundred bytes: it always fits under the internode envelope
# limit, so a larger length prefix can only come from a corrupted stream.
MAX_FRAME_SIZE = MAX_ENVELOPE_PAYLOAD

# Records staged per write transaction on the destination.
STAGE_BATCH_SIZE = 256

TRANSFERABLE_KINDS = frozenset({TagKind.LIVE, TagKind.TOMBSTONE})

_LENGTH_BYTES = 4


def _encode_frame(payload: bytes) -> bytes:
    return len(payload).to_bytes(_LENGTH_BYTES, "big") + payload


def check_chunk_size(max_chunk_size: int) -> None:
    """Raise ValueError unless chunks of that size carry frames and fit in one envelope."""
    if max_chunk_size < _LENGTH_BYTES + 1:
        raise ValueError("max_chunk_size too small to carry framed records")
    if max_chunk_size > MAX_CHUNK_SIZE:
        raise ValueError(
            f"max_chunk_size must not exceed {MAX_CHUNK_SIZE} bytes to fit in one envelope"
        )


def _read_frames(raw: bytes) -> tuple[list[bytes], bytes]:
    """Split `raw` into its complete frames and the bytes of the incomplete one."""
    frames: list[bytes] = []
    offset = 0

    while offset + _LENGTH_BYTES <= len(raw):
        size = int.from_bytes(raw[offset : offset + _LENGTH_BYTES], "big")
        if size > MAX_FRAME_SIZE:
            raise ProcessError(
                f"record frame of {size} bytes exceeds {MAX_FRAME_SIZE} bytes"
            )

        end = offset + _LENGTH_BYTES + size
        if end > len(raw):
            break

        frames.append(raw[offset + _LENGTH_BYTES : end])
        offset = end

    return frames, raw[offset:]


class TransferStreamer:
    """Storage side of one range transfer.

    The source streams the transferable records of the segment as
    length-prefixed frames packed into numbered chunks, in TAGS key order
    (pid, ts, address). The destination stages what it receives and
    activates it once the whole range is there. Since the order is fixed,
    the last staged key of every pid is a durable resume point that
    survives network failures, crashes and restarts.
    """

    def __init__(
        self,
        transfer: RangeTransfer,
        storage: Storage,
        serializer: Serializer,
        partitioner: Partitioner,
        max_chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        check_chunk_size(max_chunk_size)

        self._transfer = transfer
        self._storage = storage
        self._serializer = serializer
        self._partitioner = partitioner
        self._max_chunk_size = max_chunk_size

    @property
    def transfer(self) -> RangeTransfer:
        return self._transfer

    async def stream(
        self,
        resumes: Mapping[int, Key] | None = None,
        after: Key | None = None,
        window: int | None = None,
    ) -> AsyncIterator[TransferChunk]:
        """Yield the range as numbered chunks, the last one flagged `last`.

        Keys up to `after`, or up to the resume key of their pid, are
        skipped. With a `window`, the stream stops at the first record
        boundary once `window` bytes were produced and flags its last chunk
        with `more`.
        """
        writer = _ChunkWriter(self._transfer, self._max_chunk_size)
        async for record in self._records(resumes or {}, after):
            for chunk in writer.write(self.encode_record(record)):
                yield chunk
            if window is not None and writer.written >= window:
                yield writer.close(more=True)
                return
        yield writer.close(more=False)

    async def last_keys(self) -> Mapping[int, Key]:
        """Return, for every pid of the segment, the last key staged from the source."""
        last_keys: dict[int, Key] = {}
        for provider in await self.session_providers():
            last_keys.update(await self._staging(provider).last_keys())
        return last_keys

    async def stage(self, records: Sequence[Record]) -> int:
        """Stage records in their group, in bounded batches; return how many were written."""
        staged = 0
        for group, group_records in self._by_group(records).items():
            staging = self._staging(await self._storage.session_provider_for(group))
            for start in range(0, len(group_records), STAGE_BATCH_SIZE):
                staged += await staging.stage(
                    group_records[start : start + STAGE_BATCH_SIZE]
                )
        return staged

    async def commit(self) -> int:
        """Activate every record staged for the range; return how many were activated."""
        activated = 0
        for provider in await self.session_providers():
            activated += await self._staging(provider).commit()
        return activated

    def encode_record(self, record: Record) -> bytes:
        """Return the frame carrying `record`."""
        return _encode_frame(self._serializer.encode(record.to_dict()))

    def decode_record(self, frame: bytes) -> Record:
        """Decode one frame and check that its record belongs to the transfer."""
        record = self._decode_frame(frame)
        lo, hi = self._transfer.segment
        pid = record.key.pid.value
        if not lo <= pid <= hi:
            raise ProcessError(
                f"range={self._transfer.id} received pid {pid} outside its segment"
            )
        if record.tag.kind not in TRANSFERABLE_KINDS:
            raise ProcessError(
                f"range={self._transfer.id} received a {record.tag.kind.name} record"
            )
        return record

    async def session_providers(self) -> list[SessionProvider]:
        """Return the providers of the groups touched by the segment, in pid order."""
        groups = sorted(self._partitioner.groups_for_arc(self._transfer.segment))
        return [await self._storage.session_provider_for(group) for group in groups]

    def _decode_frame(self, frame: bytes) -> Record:
        try:
            data = self._serializer.decode(frame)
            if not isinstance(data, dict):
                raise ValueError("decoded payload is not a mapping")
            return Record.from_dict(data)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise ProcessError(
                f"range={self._transfer.id} received an invalid record frame: {exc}"
            ) from exc

    def _by_group(self, records: Sequence[Record]) -> dict[int, list[Record]]:
        groups: dict[int, list[Record]] = {}
        for record in records:
            group = self._partitioner.group_for(record.key.pid.value)
            groups.setdefault(group, []).append(record)
        return groups

    def _staging(self, session_provider: SessionProvider) -> PartitionGroupStaging:
        """Return the staging of one group, limited to the transfer segment."""
        return PartitionGroupStaging(
            self._transfer.src,
            session_provider,
            self._partitioner,
            segment=self._transfer.segment,
        )

    async def _records(
        self,
        resumes: Mapping[int, Key],
        after: Key | None,
    ) -> AsyncIterator[Record]:
        for provider in await self.session_providers():
            staging = self._staging(provider)
            async for record in staging.scan(
                resumes=resumes,
                after=after,
                kinds=TRANSFERABLE_KINDS,
            ):
                yield record


class _ChunkWriter:
    """Pack a byte stream of frames into numbered chunks of bounded size."""

    def __init__(self, transfer: RangeTransfer, max_chunk_size: int) -> None:
        self._transfer = transfer
        self._max_chunk_size = max_chunk_size
        self._buffer = bytearray()
        self._seq = 0
        self.written = 0

    def write(self, frame: bytes) -> list[TransferChunk]:
        """Buffer `frame` and return the chunks it filled up."""
        self.written += len(frame)
        self._buffer += frame
        if len(self._buffer) < self._max_chunk_size:
            return []

        data = bytes(self._buffer)
        full = len(data) - len(data) % self._max_chunk_size
        self._buffer = bytearray(data[full:])
        return [
            self._chunk(data[offset : offset + self._max_chunk_size])
            for offset in range(0, full, self._max_chunk_size)
        ]

    def close(self, *, more: bool) -> TransferChunk:
        """Return the last chunk of the stream, carrying the buffered bytes."""
        raw = bytes(self._buffer)
        self._buffer = bytearray()
        return self._chunk(raw, last=True, more=more)

    def _chunk(
        self, raw: bytes, *, last: bool = False, more: bool = False
    ) -> TransferChunk:
        chunk = TransferChunk(
            range=self._transfer,
            raw=raw,
            last=last,
            seq=self._seq,
            more=more,
        )
        self._seq += 1
        return chunk


class TransferReceiver:
    """Destination side of one ordered chunk stream (a pulled window or a push session).

    It checks the chunk sequence, rebuilds the frames split across chunks
    and stages records as soon as they are complete. Its state is scoped to
    a single stream on purpose: a retry always starts a new receiver from
    the durable staged state instead of trusting half-received bytes.
    """

    def __init__(self, streamer: TransferStreamer, after: Key | None = None) -> None:
        self._streamer = streamer
        self._pending = b""
        self._next_seq = 0
        self._cursor = after
        self._complete = False
        self._more = False

    @property
    def streamer(self) -> TransferStreamer:
        return self._streamer

    @property
    def cursor(self) -> Key | None:
        """Return the last key received, or the key the stream started after."""
        return self._cursor

    @property
    def complete(self) -> bool:
        """Return True once the last chunk of the stream was received."""
        return self._complete

    @property
    def more(self) -> bool:
        """Return True when the source has records left after this stream."""
        return self._more

    async def receive(self, chunk: TransferChunk) -> int:
        """Stage the records completed by `chunk` and return how many arrived."""
        self._check(chunk)
        frames, pending = _read_frames(self._pending + chunk.raw)
        records = [self._streamer.decode_record(frame) for frame in frames]
        await self._streamer.stage(records)

        self._pending = pending
        self._next_seq += 1
        if records:
            self._cursor = records[-1].key
        if chunk.last:
            self._finish(chunk.more)
        return len(records)

    def _check(self, chunk: TransferChunk) -> None:
        transfer_id = self._streamer.transfer.id
        if chunk.range.id != transfer_id:
            raise ProcessError(
                f"range={transfer_id} received a chunk of range {chunk.range.id}"
            )
        if self._complete:
            raise TransferError(
                f"range={transfer_id} received chunk {chunk.seq} after the last one"
            )
        if chunk.seq != self._next_seq:
            raise TransferError(
                f"range={transfer_id} expected chunk {self._next_seq}, received {chunk.seq}"
            )

    def _finish(self, more: bool) -> None:
        if self._pending:
            raise ProcessError(
                f"range={self._streamer.transfer.id} stream ended inside a record frame "
                f"({len(self._pending)} bytes left)"
            )
        self._complete = True
        self._more = more
