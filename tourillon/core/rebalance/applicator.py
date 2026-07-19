import asyncio
import hashlib
import logging
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any

from tourillon.core.exceptions import ProcessError
from tourillon.core.helpers.backoff import Backoff
from tourillon.core.helpers.waitgroup import WaitGroup
from tourillon.core.machinery.namespace import Key, Namespace, TaggedRecord
from tourillon.core.ports.storage import Storage
from tourillon.core.rebalance.transfer import (
    PartitionTransfer,
    PeerStream,
    RangeTransfer,
    RebalancePlan,
    TransferHandle,
    TransferMessage,
    TransferState,
)
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.storage.staging import PartitionStaging
from tourillon.core.transport.client import PeerClientPool
from tourlib.envelope import Envelope
from tourlib.ports.serializer import Serializer
from tourlib.transport import TcpClient

logger = logging.getLogger(__name__)


class RebalanceApplicator:
    def __init__(
        self,
        node_id: str,
        total_partitions: int,
        pool: PeerClientPool,
        topology_mgr: TopologyManager,
        serializer: Serializer,
        storage: Storage,
        backoff: Backoff | None = None,
        max_concurrency: int = 4,
        max_chunk_bytes: int = 2 * 1024 * 1024,
    ) -> None:
        self._node_id = node_id
        self._total_partitions = total_partitions
        self._pool = pool
        self._topology_mgr = topology_mgr
        self._serializer = serializer
        self._storage = storage
        self._backoff = backoff or Backoff(max_retries=0)
        self._max_chunk_bytes = max_chunk_bytes

        self._epoch = 0
        self._streams: dict[str, PeerStream] = {}
        self._peer_tasks: dict[str, asyncio.Task[None]] = {}
        self._handles: dict[str, TransferHandle] = {}
        self._wg: WaitGroup[str] = WaitGroup()
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def apply(self, plan: RebalancePlan) -> None:
        """Apply a new rebalance plan and manage the lifecycle of transfer tasks.

        This method must be called **sequentially**. Concurrent calls are not safe.
        The caller is responsible for ensuring that `apply(plan1)` completes before
        `apply(plan2)` is invoked.

        To await completion of all transfers, call `wait()` after `apply()`.
        """
        new_epoch = plan.epoch

        logger.info(f"applying new rebalance plan epoch={new_epoch}")

        self._cancel_existing_streams(new_epoch)

        streams: dict[str, PeerStream] = {}
        new_transfers: set[str] = set()
        loop = asyncio.get_running_loop()

        for range_transfer in plan.ranges:
            src = range_transfer.src
            dst = range_transfer.dst

            if src == self._node_id:
                peer = dst
            elif dst == self._node_id:
                peer = src
            else:
                logger.debug(
                    f"neither src={src} nor dst={dst} matches node_id={self._node_id} "
                    f"for range={range_transfer.id}, skipped"
                )
                continue

            if peer not in streams:
                streams[peer] = PeerStream(peer=peer)

            stream = streams[peer]
            stream.transfers.append(range_transfer)
            handles, new = await self._resolve_transfer_handles(
                range_transfer, self._handles, new_epoch
            )
            stream.handles.update(handles)
            new_transfers.update(new)

        self._cancel_outdated_handles(new_transfers, new_epoch)

        self._streams = streams
        self._epoch = new_epoch

        for peer, stream in self._streams.items():
            self._cancel_peer_task(peer)
            task = loop.create_task(
                self._apply_peer(stream),
                name=f"rebalance-peer:{peer}:epoch:{self._epoch}",
            )
            self._peer_tasks[peer] = task
            task.add_done_callback(
                lambda done_task, done_peer=peer: self._on_peer_task_done(
                    done_peer, done_task
                )
            )
            logger.info(
                f"started rebalance for peer={peer} (epoch={self._epoch}), "
                f"ranges={len(stream.transfers)}, transfers={len(stream.handles)}"
            )

    async def wait(self) -> tuple[list[str], list[str]]:
        return await self._wg.wait()

    async def _apply_peer(self, peer_stream: PeerStream) -> None:
        peer = peer_stream.peer
        max_retries = self._backoff.max_retries
        delay = self._backoff.initial

        logger.info(f"starting rebalance for peer={peer}, max_retries={max_retries}")

        for attempt in range(max_retries + 1):
            if peer_stream.cancel_event.is_set():
                logger.debug(f"stream for peer={peer} canceled")
                return

            try:
                await self._apply_peer_once(peer_stream)
                logger.debug(f"stream for peer={peer} succeeded")
                return
            except ProcessError as exc:
                logger.error(f"peer={peer} stream process error: {exc}")
                return
            except Exception as exc:
                logger.warning(
                    f"peer={peer} transfer attempt {attempt + 1}/{max_retries} failed: {exc}"
                )
                logger.error(f"peer={peer}, handles={peer_stream.handles}")
                peer_stream.last_error = str(exc)
                if attempt >= max_retries:
                    logger.error(
                        f"peer={peer} stream attempt reached max_retries={max_retries}: {exc}"
                    )
                    for handle in peer_stream.handles.values():
                        handle.cancel_event.set()

                    return

                try:
                    async with asyncio.timeout(delay):
                        await peer_stream.cancel_event.wait()
                    logger.info(f"stream for peer={peer} canceled")
                    return
                except TimeoutError:
                    pass

                delay = min(delay * self._backoff.step, self._backoff.max_interval)

    async def _apply_peer_once(self, peer_stream: PeerStream) -> None:
        peer = peer_stream.peer
        client = await self._get_client_peer(peer)
        plan_payload = {
            "transfers": [transfer.to_dict() for transfer in peer_stream.transfers],
            "epoch": self._epoch,
        }
        plan_env = Envelope.create(
            kind="rebalance.plan",
            payload=self._serializer.encode(plan_payload),
            schema_id=self._serializer.schema_id,
        )
        streams = client.stream(plan_env)
        logger.debug(f"peer={peer} sent rebalance.plan {plan_payload}")

        resp = await self._next_peer_response(
            streams,
            peer_stream=peer_stream,
            peer=peer,
            require_response=True,
        )
        if resp is None:
            return

        if resp.kind == "rebalance.plan.reject":
            data = self._serializer.decode(resp.payload)
            raise ProcessError(f"plan rejected: {data.get('reason', 'unknown')}")
        if resp.kind != "rebalance.plan.ok":
            raise ProcessError(
                f"peer={peer}, expected response kind 'rebalance.plan.ok' or "
                f"'rebalance.plan.reject', got '{resp.kind}'"
            )

        data = self._serializer.decode(resp.payload)
        self._validate_stream_epoch(peer=peer, kind=resp.kind, epoch=data.get("epoch"))
        msg_plan = TransferMessage(
            client=client,
            kind=resp.kind,
            payload=data,
            correlation_id=plan_env.correlation_id,
        )
        logger.info(f"peer={peer} notify plan is accepted (epoch={self._epoch})")
        await asyncio.gather(
            *[handle.queue.put(msg_plan) for handle in peer_stream.handles.values()]
        )

        while True:
            resp = await self._next_peer_response(
                streams,
                peer_stream=peer_stream,
                peer=peer,
            )
            payload = self._serializer.decode(resp.payload)
            logger.debug(f"*** transfer={payload['transfer_id']}, kind={resp.kind}")
            if resp is None:
                break

            if not peer_stream.handles:
                break
            else:
                logger.debug(f"remaining handles: {len(peer_stream.handles)}")

            await self._route_peer_response(
                resp,
                peer=peer,
                peer_stream=peer_stream,
                client=client,
                correlation_id=plan_env.correlation_id,
            )

    async def _next_peer_response(
        self,
        streams: Any,
        *,
        peer_stream: PeerStream,
        peer: str,
        require_response: bool = False,
    ) -> Envelope | None:
        try:
            resp = await self._wait_for_result(
                anext(streams),
                cancel_event=peer_stream.cancel_event,
            )
        except StopAsyncIteration as exc:
            if require_response:
                raise ProcessError(
                    f"peer={peer} stream closed before plan response"
                ) from exc
            return None

        if resp is None:
            logger.info("stream for peer=%s canceled", peer)

        return resp

    async def _route_peer_response(
        self,
        resp: Envelope,
        *,
        peer: str,
        peer_stream: PeerStream,
        client: TcpClient,
        correlation_id: Any,
    ) -> None:
        data = self._serializer.decode(resp.payload)
        self._validate_stream_epoch(peer=peer, kind=resp.kind, epoch=data.get("epoch"))
        if "transfer_id" not in data:
            logger.warning(
                f"transfer_id missing in response from peer={peer}, kind={resp.kind}"
            )
            return

        transfer_id = data["transfer_id"]
        logger.debug(
            f"received envelope for transfer_id={transfer_id}, kind={resp.kind}"
        )

        handle = peer_stream.handles.get(transfer_id)
        if handle is None:
            logger.debug(f"transfer_id={transfer_id} is dropped or canceled")
            return

        if resp.kind in ("rebalance.commit.ok", "rebalance.commit.reject"):
            peer_stream.handles.pop(transfer_id)

        message = TransferMessage(
            client=client,
            kind=resp.kind,
            payload=data,
            correlation_id=correlation_id,
        )
        await handle.queue.put(message)

    def _validate_stream_epoch(
        self,
        *,
        peer: str,
        kind: str,
        epoch: int,
    ) -> None:
        if epoch != self._epoch:
            raise ProcessError(
                f"peer={peer}, epoch mismatch for kind={kind}: "
                f"expected={self._epoch}, got={epoch}"
            )

    async def _attempt_transfer(self, handle: TransferHandle) -> None:
        """Attempt the transfer with exponential backoff on network errors."""
        transfer_id = handle.transfer.id
        delay = self._backoff.initial
        max_retries = self._backoff.max_retries
        handle.state = TransferState.RUNNING
        handle.started_at = datetime.now(UTC)

        for attempt in range(max_retries + 1):
            if handle.cancel_event.is_set():
                await self._on_cancelled(handle)
                return
            try:
                await self._do_transfer(handle)
                await self._on_committed(handle)
                return
            except ProcessError as exc:
                logger.error(f"transfer={transfer_id} transfer process error: {exc}")
                await self._on_failed(handle, str(exc))
                return
            except Exception as exc:
                logger.warning(
                    f"transfer={transfer_id} transfer attempt "
                    f"{attempt + 1}/{max_retries} failed: {exc}",
                )
                handle.last_error = str(exc)
                if attempt >= max_retries:
                    await self._on_failed(handle, str(exc))
                    return
                try:
                    async with asyncio.timeout(delay):
                        await handle.cancel_event.wait()
                    await self._on_cancelled(handle)
                    return
                except TimeoutError:
                    pass
                delay = min(delay * self._backoff.step, self._backoff.max_interval)

    def _cancel_existing_streams(self, new_epoch: int) -> None:
        for peer, stream in self._streams.items():
            if not stream.cancel_event.is_set():
                stream.cancel_event.set()
                logger.info(
                    f"peer={peer} for stream canceled due to "
                    f"new plan (epoch={new_epoch})"
                )

    def _cancel_peer_task(self, peer: str) -> None:
        task = self._peer_tasks.get(peer)
        if task is None or task.done():
            return
        task.cancel()
        logger.debug(f"peer={peer} apply task cancelled")

    def _cancel_outdated_handles(self, new_transfers: set[str], new_epoch: int) -> None:
        old_transfers = set(self._handles.keys())

        for transfer_id in old_transfers - new_transfers:
            handle = self._handles.pop(transfer_id, None)
            if handle:
                handle.cancel_event.set()
                logger.debug(
                    f"transfer_id={transfer_id} canceled due "
                    f"to new plan (epoch={new_epoch})"
                )

    async def _do_transfer(self, handle: TransferHandle) -> None:
        """Dispatch to the correct protocol path based on transfer direction."""
        if handle.transfer.dst == self._node_id:
            await self._do_incoming_transfer(handle)
        elif handle.transfer.src == self._node_id:
            await self._do_outgoing_transfer(handle)
        else:
            raise ProcessError(
                f"transfer={handle.transfer.id}: neither src nor dst matches this node"
            )

    async def _do_incoming_transfer(self, handle: TransferHandle) -> None:
        pid = handle.transfer.pid
        store = await self._storage.open_by_pid(pid)
        staging = store.staging(handle.epoch)
        digest = hashlib.sha256()
        is_last_seen = False
        received_commit_ok = False

        while not handle.cancel_event.is_set():
            msg = await self._wait_for_result(
                handle.queue.get(),
                cancel_event=handle.cancel_event,
            )
            if msg is None:
                break

            if msg.kind == "rebalance.plan.ok":
                logger.debug(
                    f"transfer={handle.transfer.id} received '{msg.kind}' "
                    "sending 'rebalance.resume' envelope"
                )
                resume_from = await staging.last_staged_key()
                resume_payload = self._serializer.encode(
                    {
                        "epoch": handle.epoch,
                        "transfer_id": handle.transfer.id,
                        "pid": handle.transfer.pid,
                        "resume_from": resume_from.to_dict() if resume_from else None,
                    }
                )
                resume_env = Envelope.create(
                    kind="rebalance.resume",
                    payload=resume_payload,
                    schema_id=self._serializer.schema_id,
                    correlation_id=msg.correlation_id,
                )
                await msg.client.send(resume_env)
                logger.debug(
                    f"transfer={handle.transfer.id} sent '{resume_env.kind}' envelope "
                    f"(epoch={self._epoch})"
                )
                continue

            commit_ok, is_last_seen = await self._handle_incoming_response(
                msg=msg,
                handle=handle,
                staging=staging,
                digest=digest,
                is_last_seen=is_last_seen,
            )

            if commit_ok:
                received_commit_ok = True
                break

        if not received_commit_ok and not handle.cancel_event.is_set():
            raise ProcessError("stream closed before commit acknowledgement")

        if not handle.cancel_event.is_set():
            await staging.commit()

    async def _do_outgoing_transfer(self, handle: TransferHandle) -> None:
        digest = hashlib.sha256()
        received_commit = False

        while not handle.cancel_event.is_set():
            msg = await self._wait_for_result(
                handle.queue.get(),
                cancel_event=handle.cancel_event,
            )
            if msg is None:
                break

            if msg.kind == "rebalance.plan.ok":
                logger.debug(
                    f"transfer={handle.transfer.id} received '{msg.kind}', "
                    f"waiting for 'rebalance.resume' to start sending chunk"
                )

            received_commit = await self._handle_outgoing_response(
                msg=msg,
                handle=handle,
                digest=digest,
            )
            if received_commit:
                break

        if not received_commit and not handle.cancel_event.is_set():
            raise ProcessError(
                f"transfer={handle.transfer.id} stream closed before commit received"
            )

    async def _get_client_peer(self, peer: str) -> TcpClient:
        topology = await self._topology_mgr.snapshot()
        member = topology.registry.get(peer)
        if member is None:
            raise ProcessError(f"Peer not found in topology: {peer}")

        client = await self._pool.acquire(peer, member.peer_address)
        return client

    async def _handle_incoming_response(
        self,
        *,
        msg: TransferMessage,
        handle: TransferHandle,
        staging: PartitionStaging,
        digest: Any,
        is_last_seen: bool,
    ) -> tuple[bool, bool]:
        """Handle one JOIN stream response.

        Returns (commit_ok_received, updated_is_last_seen).
        """
        if msg.kind == "rebalance.transfer":
            data = msg.payload
            await self._stage_chunk(handle, staging, data, digest)
            is_last_seen = await self._send_commit_if_last(
                msg=msg,
                handle=handle,
                digest_hex=digest.hexdigest(),
                is_last_seen=is_last_seen,
            )
            return False, is_last_seen
        if msg.kind == "rebalance.commit.ok":
            return True, is_last_seen
        if msg.kind == "rebalance.commit.reject":
            raise ProcessError("commit rejected: digest mismatch")
        raise ProcessError(f"unexpected envelope kind: {msg.kind}")

    async def _handle_outgoing_response(
        self,
        *,
        msg: TransferMessage,
        handle: TransferHandle,
        digest: Any,
    ) -> bool:
        client = msg.client
        correlation_id = msg.correlation_id
        transfer_id = handle.transfer.id
        data = msg.payload

        if msg.kind == "rebalance.resume":
            resume_data: dict[str, Any] | None = data.get("resume_from")
            resume = Key.from_dict(resume_data) if resume_data else None
            await self._stream_chunks(msg, handle, digest, resume)
            return False
        if msg.kind == "rebalance.commit":
            received_digest = data.get("digest", "")
            expected = digest.hexdigest()
            if expected != received_digest:
                await client.send(
                    Envelope(
                        kind="rebalance.commit.reject",
                        payload=self._serializer.encode(
                            {"epoch": handle.epoch, "transfer_id": transfer_id}
                        ),
                        correlation_id=correlation_id,
                        schema_id=self._serializer.schema_id,
                    )
                )
                raise ProcessError(
                    f"transfer_id={transfer_id} commit rejected: digest mismatch"
                )
            await client.send(
                Envelope(
                    kind="rebalance.commit.ok",
                    payload=self._serializer.encode(
                        {"epoch": handle.epoch, "transfer_id": transfer_id}
                    ),
                    correlation_id=correlation_id,
                    schema_id=self._serializer.schema_id,
                )
            )
            return True
        raise ProcessError(
            f"transfer_id={transfer_id} unexpected envelope kind: {msg.kind}"
        )

    async def _handle_transfer(self, handle: TransferHandle) -> None:
        async with self._semaphore:
            if handle.cancel_event.is_set():
                await self._on_cancelled(handle)
                return

            logger.debug(f"transfer={handle.transfer.id} semaphore acquired")
            await self._attempt_transfer(handle)

    @staticmethod
    async def _wait_for_result[T](
        awaitable: Awaitable[T],
        *,
        cancel_event: asyncio.Event,
    ) -> T | None:
        """Wait for an awaitable result or a cancellation signal.

        Returns the awaited result, or ``None`` if cancellation wins the race.
        This helper is shared by transfer-handle queue reads and peer stream reads.
        """
        result_task = asyncio.ensure_future(awaitable)
        cancel_task = asyncio.create_task(cancel_event.wait())

        done, pending = await asyncio.wait(
            [result_task, cancel_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        if cancel_event.is_set() or result_task not in done:
            return None

        return result_task.result()

    async def _on_cancelled(self, handle: TransferHandle) -> None:
        """Call staging.cleanup() and signal the WaitGroup."""
        pid = handle.transfer.pid
        transfer_id = handle.transfer.id
        epoch = handle.epoch

        try:
            store = await self._storage.open_by_pid(pid)
            staging = store.staging(epoch)
            await staging.cleanup()
        except Exception as ex:
            logger.warning(
                f"transfer={transfer_id} cleanup failed during cancel", exc_info=ex
            )

        handle.state = TransferState.CANCELLED
        handle.finished_at = datetime.now(tz=UTC)
        await self._wg.done(transfer_id, False)

    async def _on_committed(self, handle: TransferHandle) -> None:
        """Mark handle as committed and signal the WaitGroup."""
        handle.state = TransferState.COMMITTED
        handle.finished_at = datetime.now(tz=UTC)
        logger.debug("transfer=%s committed.", handle.transfer.id)
        await self._wg.done(handle.transfer.id, True)

    async def _on_failed(self, handle: TransferHandle, error: str) -> None:
        """Mark handle as failed and signal the WaitGroup."""
        handle.state = TransferState.FAILED
        handle.last_error = error
        handle.finished_at = datetime.now(UTC)
        logger.error(
            "transfer=%s permanently failed (src=%s dst=%s): %s",
            handle.transfer.id,
            handle.transfer.src,
            handle.transfer.dst,
            error,
        )
        await self._wg.done(handle.transfer.id, False)

    def _on_peer_task_done(self, peer: str, task: asyncio.Task[None]) -> None:
        current_task = self._peer_tasks.get(peer)
        if current_task is task:
            self._peer_tasks.pop(peer, None)

        if task.cancelled():
            logger.debug(f"peer={peer} apply task finished as cancelled")
            return

        exc = task.exception()
        if exc is not None:
            logger.error(
                f"peer={peer} apply task terminated with unexpected error",
                exc_info=exc,
            )
            return

        logger.debug(f"peer={peer} apply task completed")

    async def _resolve_transfer_handles(
        self,
        range_transfer: RangeTransfer,
        old_handles: dict[str, TransferHandle],
        epoch: int,
    ) -> tuple[dict[str, TransferHandle], set[str]]:
        handles: dict[str, TransferHandle] = {}
        new: set[str] = set()
        loop = asyncio.get_running_loop()

        for pid in range_transfer.pids(self._total_partitions):
            src = range_transfer.src
            dst = range_transfer.dst
            transfer = PartitionTransfer(pid=pid, src=src, dst=dst)
            transfer_id = transfer.id
            new.add(transfer_id)

            if transfer_id not in old_handles:
                handle = TransferHandle(
                    transfer=transfer,
                    state=TransferState.PENDING,
                    epoch=epoch,
                )
                old_handles[transfer_id] = handle
                handles[transfer_id] = handle
                await self._wg.add(1)
                loop.create_task(self._handle_transfer(handle))
                logger.debug(f"transfer_id={transfer_id} started")
            else:
                handle = old_handles[transfer_id]
                handle.epoch = epoch
                handles[transfer_id] = handle
                if handle.state == TransferState.COMMITTED:
                    # Skip terminal handles that already succeeded.
                    logger.debug(
                        f"transfer_id={transfer_id} already committed, skipping"
                    )
                    handles[transfer_id] = handle
                    continue

                if handle.state in (TransferState.FAILED, TransferState.CANCELLED):
                    handle = TransferHandle(
                        transfer=transfer,
                        state=TransferState.PENDING,
                        epoch=epoch,
                    )
                    old_handles[transfer_id] = handle
                    await self._wg.add(1)
                    loop.create_task(self._handle_transfer(handle))
                    logger.debug(
                        f"transfer_id={transfer_id} restarted from state={handle.state}"
                    )
                    handles[transfer_id] = handle

        return handles, new

    async def _send_commit_if_last(
        self,
        handle: TransferHandle,
        msg: TransferMessage,
        digest_hex: str,
        is_last_seen: bool,
    ) -> bool:
        """Send rebalance.commit when data['is_last'] is True and not yet sent.

        Return the updated is_last_seen flag so the caller can suppress any
        duplicate is_last chunks that may arrive on a resumed stream.
        """
        data = msg.payload
        if not data.get("is_last") or is_last_seen:
            return is_last_seen

        commit_env = Envelope(
            kind="rebalance.commit",
            payload=self._serializer.encode(
                {
                    "epoch": handle.epoch,
                    "transfer_id": handle.transfer.id,
                    "digest": digest_hex,
                }
            ),
            correlation_id=msg.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await msg.client.send(commit_env)
        return True

    async def _send_chunk(
        self,
        msg: TransferMessage,
        handle: TransferHandle,
        seq: int,
        recs: list[dict[str, Any]],
        is_last: bool,
    ) -> None:
        """Push one rebalance.transfer chunk to the destination."""
        payload = self._serializer.encode(
            {
                "epoch": handle.epoch,
                "transfer_id": handle.transfer.id,
                "chunk_seq": seq,
                "is_last": is_last,
                "records": recs,
            }
        )
        chunk_env = Envelope(
            kind="rebalance.transfer",
            payload=payload,
            correlation_id=msg.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await msg.client.send(chunk_env)

    @staticmethod
    async def _stage_chunk(
        handle: TransferHandle,
        staging: PartitionStaging,
        data: dict[str, Any],
        digest: Any,
    ) -> None:
        """Stage records from one transfer chunk."""
        for rec_dict in data.get("records", []):
            rec = TaggedRecord.from_dict(rec_dict)
            await staging.stage(rec)
            digest.update(rec.to_bytes(Namespace.TAGS))
            handle.bytes_done += len(rec.to_bytes(Namespace.TAGS))
        handle.chunks_done += 1
        if data.get("is_last"):
            handle.chunks_total = handle.chunks_done

    async def _stream_chunks(
        self,
        msg: TransferMessage,
        handle: TransferHandle,
        digest: Any,
        resume: Key | None,
    ) -> None:
        """Scan the local store and push rebalance.transfer chunks to the peer.

        ``chunk_bytes`` is tracked as an observability metric and for approximate
        chunk sizing. The actual on-wire payload is produced later when the
        records are serialized into the envelope.
        """
        store = await self._storage.open_by_pid(handle.transfer.pid)
        chunk_buf: list[dict[str, Any]] = []
        chunk_bytes = 0
        chunk_seq = 0

        async for tagged in store.scan(resume_from=resume):
            digest.update(tagged.to_bytes(Namespace.TAGS))
            rec_dict = tagged.to_dict()
            encoded = self._serializer.encode(tagged.to_bytes(Namespace.TAGS))
            encoded_len = len(encoded)

            if chunk_buf and chunk_bytes + encoded_len > self._max_chunk_bytes:
                await self._send_chunk(
                    msg, handle, chunk_seq, chunk_buf, False
                )
                handle.bytes_done += chunk_bytes
                chunk_seq += 1
                handle.chunks_done = chunk_seq
                chunk_buf = []
                chunk_bytes = 0

            chunk_buf.append(rec_dict)
            chunk_bytes += encoded_len

        if chunk_buf:
            await self._send_chunk(msg, handle, chunk_seq, chunk_buf, True)
            handle.bytes_done += chunk_bytes
            chunk_seq += 1
            handle.chunks_done = chunk_seq
        else:
            # Explicitly terminate empty scans with a final marker chunk.
            await self._send_chunk(msg, handle, chunk_seq, [], True)
            chunk_seq += 1
            handle.chunks_done = chunk_seq

        handle.chunks_total = chunk_seq
