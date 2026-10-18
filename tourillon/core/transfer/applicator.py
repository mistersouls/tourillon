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
import functools
import random
from collections.abc import Mapping
from contextlib import aclosing
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from tourillon.core.exceptions import ProcessError, TransferError
from tourillon.core.log import logger
from tourillon.core.models.retry import BackoffRetry
from tourillon.core.models.storageschema import Key
from tourillon.core.models.transfer import (
    RangeTransfer,
    TransferChunk,
    TransferClaim,
    TransferCommit,
    TransferHandle,
    TransferInit,
    TransferInitAccept,
    TransferPlan,
    TransferReject,
    TransferState,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.peer import PeerPool
from tourillon.core.ports.storage import Storage
from tourillon.core.transfer.stream import (
    DEFAULT_CHUNK_SIZE,
    TransferReceiver,
    TransferStreamer,
    check_chunk_size,
)
from tourlib import Serializer
from tourlib.constant import ENVELOPE_RESPONSE_TIMEOUT
from tourlib.envelope import Envelope, build_envelope
from tourlib.exceptions import EnvelopeError
from tourlib.transport import EnvelopeClient

# Bytes of record frames claimed per pulled window: bounds the memory used
# by a transfer and the work lost when a window is interrupted.
DEFAULT_WINDOW_SIZE = 32 * 1024 * 1024

# Activating a pushed range may take long: the commit has its own deadline.
DEFAULT_COMMIT_TIMEOUT = 600.0

# Failures worth another attempt: network and transport errors, typed
# rejections from the peer, resources momentarily unavailable (connection
# pool, storage being closed). Any other error fails the transfer.
RETRYABLE_ERRORS: tuple[type[Exception], ...] = (
    TransferError,
    OSError,
    EnvelopeError,
    RuntimeError,
)


def _default_backoff() -> BackoffRetry:
    """Retry forever (max_retries=0): a join must outlive any network partition."""
    return BackoffRetry(initial_delay=0.5, max_delay=30.0, max_retries=0)


def _describe(exc: BaseException) -> str:
    return str(exc) or type(exc).__name__


def _resumes_after(resumes: Mapping[int, Key], after: Key | None) -> dict[int, Key]:
    """Keep the resume keys still needed once every key up to `after` was received."""
    if after is None:
        return dict(resumes)
    return {pid: key for pid, key in resumes.items() if pid > after.pid.value}


class TransferApplicator:
    """Run the range transfers of a plan, concurrently and resiliently.

    A range is pulled when this node is its destination (join) and pushed
    when it is its source (drain). Every attempt resumes from the durable
    staged state, so a transfer survives network failures, peer restarts
    and local crashes. Retryable failures are retried with an exponential
    backoff: forever by default, or until `backoff.max_retries` consecutive
    attempts made no progress. A `ProcessError` (an inconsistency that no
    retry can fix) fails the range at once.
    """

    def __init__(
        self,
        node_id: str,
        peer_pool: PeerPool,
        storage: Storage,
        serializer: Serializer,
        partitioner: Partitioner,
        max_concurrency: int = 4,
        max_chunk_size: int = DEFAULT_CHUNK_SIZE,
        backoff: BackoffRetry | None = None,
        window_size: int = DEFAULT_WINDOW_SIZE,
        commit_timeout: float = DEFAULT_COMMIT_TIMEOUT,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if max_chunk_size < 1024:
            raise ValueError("max_chunk_size must be >= 1024")
        check_chunk_size(max_chunk_size)
        if window_size < 1:
            raise ValueError("window_size must be >= 1")
        if commit_timeout <= 0:
            raise ValueError("commit_timeout must be > 0")

        self._node_id = node_id
        self._peer_pool = peer_pool
        self._storage = storage
        self._serializer = serializer
        self._partitioner = partitioner
        self._max_concurrency = max_concurrency
        self._max_chunk_size = max_chunk_size
        self._backoff = backoff or _default_backoff()
        self._window_size = window_size
        self._commit_timeout = commit_timeout
        self._handles: dict[str, TransferHandle] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._plan = TransferPlan(ranges=[], serial="")
        self._semaphore = asyncio.Semaphore(max_concurrency)

    @property
    def handles(self) -> Mapping[str, TransferHandle]:
        """Return the handles of the ranges of the current plan."""
        return dict(self._handles)

    async def apply(self, plan: TransferPlan) -> None:
        """Make `plan` the current plan.

        Ranges left out of the plan are cancelled, new ranges are started and
        ranges that failed or were cancelled get a new chance. Running and
        completed ranges are kept as they are.
        """
        self._plan = plan
        incoming = {range_.id: range_ for range_ in plan.ranges}

        dropped = [range_id for range_id in self._handles if range_id not in incoming]
        for range_id in dropped:
            self._drop(range_id)

        started = [
            range_
            for range_id, range_ in incoming.items()
            if self._should_start(range_id)
        ]
        for range_ in started:
            self._start(range_)

        logger.info(
            "Applied transfer plan (node=%s, serial=%s): %d started, %d cancelled, "
            "%d kept (concurrency=%d)",
            self._node_id,
            plan.serial,
            len(started),
            len(dropped),
            len(incoming) - len(started),
            self._max_concurrency,
        )

    async def wait(self) -> tuple[list[str], list[str]]:
        """Wait until every range of the current plan has settled.

        Ranges started by a plan applied while waiting are awaited as well.
        Return the completed ranges and the others (failed or cancelled).
        """
        while pending := [task for task in self._tasks.values() if not task.done()]:
            await asyncio.wait(pending)

        success = sorted(
            range_id
            for range_id, handle in self._handles.items()
            if handle.state is TransferState.COMPLETED
        )
        failed = sorted(set(self._handles) - set(success))
        logger.info(
            "Transfer (node=%s, serial=%s) settled: %d succeeded, %d failed",
            self._node_id,
            self._plan.serial,
            len(success),
            len(failed),
        )
        return success, failed

    async def cancel(self) -> None:
        """Cancel every running transfer and wait for them to stop."""
        tasks = [task for task in self._tasks.values() if not task.done()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # A task cancelled before its first step never settled its handle.
        for handle in self._handles.values():
            self._finish(handle, TransferState.CANCELLED)

    def _should_start(self, range_id: str) -> bool:
        handle = self._handles.get(range_id)
        return handle is None or handle.state in {
            TransferState.FAILED,
            TransferState.CANCELLED,
        }

    def _start(self, range_: RangeTransfer) -> None:
        handle = TransferHandle(range=range_, state=TransferState.PENDING)
        task = asyncio.get_running_loop().create_task(
            self._run_transfer(handle),
            name=f"transfer-{range_.id}",
        )
        self._handles[range_.id] = handle
        self._tasks[range_.id] = task
        task.add_done_callback(functools.partial(self._forget_task, range_.id))

    def _drop(self, range_id: str) -> None:
        del self._handles[range_id]
        task = self._tasks.get(range_id)
        if task is not None:
            task.cancel()

    def _forget_task(self, range_id: str, task: asyncio.Task[None]) -> None:
        # A range restarted by a newer plan owns the slot: keep its task.
        if self._tasks.get(range_id) is task:
            del self._tasks[range_id]

    async def _run_transfer(self, handle: TransferHandle) -> None:
        try:
            async with self._semaphore:
                await self._attempt_transfer(handle)
        except asyncio.CancelledError:
            self._finish(handle, TransferState.CANCELLED)
            raise
        except Exception as exc:  # a transfer task must always settle its handle
            logger.exception("range=%s unexpected transfer error", handle.range.id)
            self._finish(handle, TransferState.FAILED, exc)

    async def _attempt_transfer(self, handle: TransferHandle) -> None:
        handle.state = TransferState.STARTED
        handle.started_at = datetime.now(UTC)
        failures = 0
        delay = self._backoff.initial_delay

        while True:
            handle.attempts += 1
            progress = handle.transferred
            try:
                await self._do_transfer(handle)
            except ProcessError as exc:
                self._finish(handle, TransferState.FAILED, exc)
                return
            except RETRYABLE_ERRORS as exc:
                if handle.transferred > progress:
                    failures, delay = 0, self._backoff.initial_delay
                failures += 1
                handle.last_error = _describe(exc)
                if self._retries_exhausted(failures):
                    self._finish(handle, TransferState.FAILED, exc)
                    return
                await self._sleep_before_retry(handle, failures, delay)
                delay = min(delay * self._backoff.factor, self._backoff.max_delay)
            else:
                self._finish(handle, TransferState.COMPLETED)
                return

    def _retries_exhausted(self, failures: int) -> bool:
        """Return True when `failures` attempts in a row made no progress (0: never)."""
        limit = self._backoff.max_retries
        return 0 < limit < failures

    async def _sleep_before_retry(
        self, handle: TransferHandle, failures: int, delay: float
    ) -> None:
        jitter = self._backoff.jitter
        pause = max(0.0, delay * (1 + random.uniform(-jitter, jitter)))
        logger.warning(
            "range=%s attempt %d failed (%d consecutive failure(s) since the last progress): "
            "%s; retrying in %.1fs",
            handle.range.id,
            handle.attempts,
            failures,
            handle.last_error,
            pause,
        )
        await asyncio.sleep(pause)

    def _finish(
        self,
        handle: TransferHandle,
        state: TransferState,
        exc: BaseException | None = None,
    ) -> None:
        if handle.finished_at is not None:
            return

        handle.state = state
        handle.finished_at = datetime.now(UTC)
        if exc is not None:
            handle.last_error = _describe(exc)
        if state is TransferState.FAILED:
            logger.error(
                "range=%s failed after %d attempt(s): %s",
                handle.range.id,
                handle.attempts,
                handle.last_error,
            )
            return
        logger.info(
            "range=%s %s after %d attempt(s), %d bytes transferred",
            handle.range.id,
            state.value.lower(),
            handle.attempts,
            handle.transferred,
        )

    async def _do_transfer(self, handle: TransferHandle) -> None:
        range_ = handle.range
        if range_.src == range_.dst:
            raise ProcessError(f"range={range_.id} has the same source and destination")
        if range_.dst == self._node_id:
            await self._pull(handle)
        elif range_.src == self._node_id:
            await self._push(handle)
        else:
            raise ProcessError(
                f"range={range_.id} neither source nor destination matches this node"
            )

    async def _pull(self, handle: TransferHandle) -> None:
        """Pull the range from its source window by window, then activate it."""
        range_ = handle.range
        streamer = self._streamer(range_)
        resumes = dict(await streamer.last_keys())
        after: Key | None = None
        client = await self._peer_pool.acquire(range_.src)
        try:
            while True:
                receiver = TransferReceiver(streamer, after=after)
                await self._pull_window(handle, client, receiver, resumes)
                if not receiver.more:
                    break
                after = receiver.cursor
                resumes = _resumes_after(resumes, after)
        finally:
            await self._peer_pool.release(range_.src)

        activated = await streamer.commit()
        logger.info(
            "range=%s pulled from %s: %d record(s) activated",
            range_.id,
            range_.src,
            activated,
        )

    async def _pull_window(
        self,
        handle: TransferHandle,
        client: EnvelopeClient,
        receiver: TransferReceiver,
        resumes: Mapping[int, Key],
    ) -> None:
        start = receiver.cursor
        claim = TransferClaim(
            serial=self._plan.serial,
            resumes=resumes,
            range=handle.range,
            after=start,
            window=self._window_size,
        )
        envelope = self._envelope("transfer.claim", claim.to_dict())
        async with aclosing(client.stream(envelope)) as responses:
            async for response in responses:
                chunk = TransferChunk.from_dict(
                    self._expect(response, "transfer.chunk")
                )
                await receiver.receive(chunk)
                handle.transferred += len(chunk.raw)
                if receiver.complete:
                    break

        if not receiver.complete:
            raise TransferError(
                f"range={handle.range.id} stream closed before its last chunk"
            )
        if receiver.more and receiver.cursor == start:
            raise ProcessError(
                f"range={handle.range.id} source announced more records without sending any"
            )

    async def _push(self, handle: TransferHandle) -> None:
        """Push the range to its destination, one acknowledged chunk at a time."""
        range_ = handle.range
        streamer = self._streamer(range_)
        client = await self._peer_pool.acquire(range_.dst)
        try:
            session = await self._open_push_session(client, range_)
            async with aclosing(streamer.stream(session.resumes)) as chunks:
                async for chunk in chunks:
                    payload = replace(chunk, session=session.session).to_dict()
                    await self._request(
                        client, "transfer.chunk", payload, "transfer.chunk.accept"
                    )
                    handle.transferred += len(chunk.raw)

            commit = TransferCommit(range=range_, session=session.session)
            reply = await self._request(
                client,
                "transfer.commit",
                commit.to_dict(),
                "transfer.commit.accept",
                timeout=self._commit_timeout,
            )
        finally:
            await self._peer_pool.release(range_.dst)

        logger.info(
            "range=%s pushed to %s: %s record(s) activated",
            range_.id,
            range_.dst,
            reply.get("activated", "?"),
        )

    async def _open_push_session(
        self,
        client: EnvelopeClient,
        range_: RangeTransfer,
    ) -> TransferInitAccept:
        init = TransferInit(serial=self._plan.serial, range=range_)
        data = await self._request(
            client, "transfer.init", init.to_dict(), "transfer.init.accept"
        )
        return TransferInitAccept.from_dict(data)

    async def _request(
        self,
        client: EnvelopeClient,
        kind: str,
        payload: dict[str, Any],
        expected: str,
        timeout: float = ENVELOPE_RESPONSE_TIMEOUT,
    ) -> dict[str, Any]:
        response = await client.request(self._envelope(kind, payload), timeout=timeout)
        return self._expect(response, expected)

    def _expect(self, response: Envelope, kind: str) -> dict[str, Any]:
        """Return the payload of a `kind` response; raise on a rejection or another kind."""
        if response.kind == kind:
            data = self._serializer.decode(response.payload)
            return data if isinstance(data, dict) else {}
        if response.kind.endswith(".reject"):
            reject = TransferReject.from_dict(self._serializer.decode(response.payload))
            error = ProcessError if reject.fatal else TransferError
            raise error(f"{response.kind}: {reject.reason}")
        raise ProcessError(
            f"unexpected response kind {response.kind!r}, expected {kind!r}"
        )

    def _envelope(self, kind: str, payload: dict[str, Any]) -> Envelope:
        return build_envelope(
            kind=kind,
            payload=self._serializer.encode(payload),
            schema_id=self._serializer.schema_id,
        )

    def _streamer(self, range_: RangeTransfer) -> TransferStreamer:
        return TransferStreamer(
            transfer=range_,
            storage=self._storage,
            serializer=self._serializer,
            partitioner=self._partitioner,
            max_chunk_size=self._max_chunk_size,
        )
