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

import asyncio
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field

from tourillon.core.exceptions import TransferError
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.log import logger
from tourillon.core.models.transfer import (
    RangeTransfer,
    TransferChunk,
    TransferClaim,
    TransferCommit,
    TransferInit,
    TransferInitAccept,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.ports.storage import Storage
from tourillon.core.transfer.stream import (
    DEFAULT_CHUNK_SIZE,
    TransferReceiver,
    TransferStreamer,
    check_chunk_size,
)
from tourlib import Serializer

# Largest window a destination may claim at once: it bounds the memory the
# destination needs and the work wasted when a window is interrupted.
MAX_WINDOW_SIZE = 256 * 1024 * 1024

# Push sessions idle for longer are forgotten; the source reopens them.
DEFAULT_SESSION_TTL = 600.0


@dataclass
class _PushSession:
    id: str
    receiver: TransferReceiver
    touched: float
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class TransferService:
    """Peer-facing side of the range transfers.

    - `claim` serves one window of a range to the destination pulling it;
    - `init`, `chunk` and `commit` receive a range pushed by its source.

    A push session only holds the reassembly state of one stream; what is
    durable lives in the staging area. A lost session (restart, idle
    eviction, newer `init` for the range) is simply reopened by the source,
    which resumes from the keys already staged.
    """

    def __init__(
        self,
        topologymgr: TopologyManager,
        serializer: Serializer,
        storage: Storage,
        partitioner: Partitioner,
        max_chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_window: int = MAX_WINDOW_SIZE,
        session_ttl: float = DEFAULT_SESSION_TTL,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        check_chunk_size(max_chunk_size)
        if max_window < 1:
            raise ValueError("max_window must be >= 1")
        if session_ttl <= 0:
            raise ValueError("session_ttl must be > 0")

        self._topologymgr = topologymgr
        self._storage = storage
        self._serializer = serializer
        self._partitioner = partitioner
        self._max_chunk_size = max_chunk_size
        self._max_window = max_window
        self._session_ttl = session_ttl
        self._clock = clock
        self._sessions: dict[str, _PushSession] = {}

    async def claim(self, request: TransferClaim) -> AsyncIterator[TransferChunk]:
        """Stream one window of the requested range."""
        await self._assert_matching_serial(request.serial)
        streamer = self._streamer(request.range)
        async for chunk in streamer.stream(
            request.resumes,
            after=request.after,
            window=self._window(request.window),
        ):
            yield chunk

    async def init(self, request: TransferInit) -> TransferInitAccept:
        """Open a push session for a range, replacing any previous one."""
        await self._assert_matching_serial(request.serial)
        self._evict_idle_sessions()

        streamer = self._streamer(request.range)
        resumes = await streamer.last_keys()
        session = _PushSession(
            id=uuid.uuid4().hex,
            receiver=TransferReceiver(streamer),
            touched=self._clock(),
        )
        if request.range.id in self._sessions:
            logger.info("range=%s push session restarted by its source", request.range.id)
        self._sessions[request.range.id] = session
        return TransferInitAccept(resumes=resumes, session=session.id)

    async def chunk(self, request: TransferChunk) -> None:
        """Stage one pushed chunk; the chunks of a session are handled one at a time."""
        session = self._session(request.range, request.session)
        async with session.lock:
            await session.receiver.receive(request)
            session.touched = self._clock()

    async def commit(self, request: TransferCommit) -> int:
        """Activate a range fully pushed in a session; return the records activated."""
        session = self._session(request.range, request.session)
        async with session.lock:
            if not session.receiver.complete:
                raise TransferError(
                    f"range={request.range.id} commit requested before the last chunk"
                )
            activated = await session.receiver.streamer.commit()

        if self._sessions.get(request.range.id) is session:
            del self._sessions[request.range.id]
        return activated

    def _session(self, range_: RangeTransfer, session_id: str) -> _PushSession:
        session = self._sessions.get(range_.id)
        if session is None or session.id != session_id:
            raise TransferError(f"range={range_.id} unknown transfer session {session_id!r}")
        return session

    def _evict_idle_sessions(self) -> None:
        now = self._clock()
        expired = [
            range_id
            for range_id, session in self._sessions.items()
            if not session.lock.locked() and now - session.touched > self._session_ttl
        ]
        for range_id in expired:
            del self._sessions[range_id]
            logger.info("range=%s idle push session evicted", range_id)

    def _window(self, requested: int | None) -> int:
        if requested is None or requested < 1:
            return self._max_window
        return min(requested, self._max_window)

    def _streamer(self, range_: RangeTransfer) -> TransferStreamer:
        return TransferStreamer(
            storage=self._storage,
            transfer=range_,
            serializer=self._serializer,
            partitioner=self._partitioner,
            max_chunk_size=self._max_chunk_size,
        )

    async def _assert_matching_serial(self, remote_serial: str) -> None:
        serial = await self._current_serial()
        if remote_serial != serial:
            raise TransferError(
                f"Serial mismatch: remote {remote_serial}, local {serial}"
            )

    async def _current_serial(self) -> str:
        topology = await self._topologymgr.snapshot()
        return topology.ring.serial()



