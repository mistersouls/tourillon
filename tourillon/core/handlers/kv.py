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
"""KV envelope handlers for the KV server dispatcher.

Handlers:
- KvPutHandler   : kv.put  (client → coordinator)
- KvGetHandler   : kv.get  (client → coordinator)
- KvDeleteHandler: kv.delete (client → coordinator)
- KvReplicateHandler: kv.replicate (coordinator → replica)
- KvHintHandler  : kv.hint (coordinator → handoff)
- KvFetchHandler : kv.fetch (coordinator → replica, read fanout)

All handlers follow the ConnectionHandler protocol: they receive one
Envelope, perform their logic, and send exactly one response Envelope.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from tourillon.core.kv.coordinator import KvCoordinator, KvError
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.record import KvMetadata, StoreKey, Tombstone

if TYPE_CHECKING:
    from tourillon.core.kv.coordinator import KvCoordinator
    from tourillon.core.ports.serializer import SerializerPort
    from tourillon.core.ports.storage import Storage
    from tourillon.core.ports.transport import ReceiveEnvelope, SendEnvelope
    from tourillon.core.ring.partitioner import Partitioner

logger = logging.getLogger(__name__)

_DEFAULT_QUORUM: int = 1


def _hlc_from_dict(raw: dict[str, Any]) -> HLCTimestamp:
    return HLCTimestamp.from_dict(raw)


class KvPutHandler:
    """Handle kv.put: validate, call coordinator, send kv.put.ok or kv.error."""

    def __init__(
        self,
        coordinator: KvCoordinator,
        serializer: SerializerPort,
        default_quorum_write: int = _DEFAULT_QUORUM,
    ) -> None:
        self._coordinator = coordinator
        self._serializer = serializer
        self._default_qw = default_quorum_write

    async def __call__(self, receive: ReceiveEnvelope, send: SendEnvelope) -> None:
        """Handle one kv.put request."""
        req = await receive()
        try:
            data = self._serializer.decode(req.payload)
        except Exception:
            logger.warning("kv.put: malformed payload")
            await self._send_error(send, req, "invalid_key")
            return

        key_raw = data.get("key")
        ks_raw = data.get("keyspace", b"default")
        val_raw = data.get("value")
        qw_raw = data.get("quorum_write")
        qw = int(qw_raw) if qw_raw is not None else self._default_qw

        if not key_raw:
            await self._send_error(send, req, "invalid_key")
            return
        if qw < 1:
            await self._send_error(send, req, "invalid_quorum")
            return

        key = bytes(key_raw) if not isinstance(key_raw, bytes) else key_raw
        keyspace = bytes(ks_raw) if not isinstance(ks_raw, bytes) else ks_raw
        value = bytes(val_raw) if not isinstance(val_raw, bytes) else val_raw

        try:
            result = await self._coordinator.put(key, keyspace, value, qw)
        except KvError as exc:
            await self._send_error(send, req, str(exc))
            return

        payload = self._serializer.encode(result)
        resp = Envelope.create(
            payload,
            kind="kv.put.ok",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(resp)

    async def _send_error(self, send: SendEnvelope, req: Envelope, reason: str) -> None:
        payload = self._serializer.encode({"reason": reason})
        resp = Envelope.create(
            payload,
            kind="kv.error",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(resp)


class KvGetHandler:
    """Handle kv.get: validate, call coordinator, send kv.get.ok or kv.error."""

    def __init__(
        self,
        coordinator: KvCoordinator,
        serializer: SerializerPort,
        default_quorum_read: int = _DEFAULT_QUORUM,
    ) -> None:
        self._coordinator = coordinator
        self._serializer = serializer
        self._default_qr = default_quorum_read

    async def __call__(self, receive: ReceiveEnvelope, send: SendEnvelope) -> None:
        """Handle one kv.get request."""
        req = await receive()
        try:
            data = self._serializer.decode(req.payload)
        except Exception:
            logger.warning("kv.get: malformed payload")
            await self._send_error(send, req, "invalid_key")
            return

        key_raw = data.get("key")
        ks_raw = data.get("keyspace", b"default")
        qr = int(data.get("quorum_read") or self._default_qr)

        if not key_raw:
            await self._send_error(send, req, "invalid_key")
            return
        if qr < 1:
            await self._send_error(send, req, "invalid_quorum")
            return

        key = bytes(key_raw) if not isinstance(key_raw, bytes) else key_raw
        keyspace = bytes(ks_raw) if not isinstance(ks_raw, bytes) else ks_raw

        try:
            result = await self._coordinator.get(key, keyspace, qr)
        except KvError as exc:
            await self._send_error(send, req, str(exc))
            return

        payload = self._serializer.encode(result)
        resp = Envelope.create(
            payload,
            kind="kv.get.ok",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(resp)

    async def _send_error(self, send: SendEnvelope, req: Envelope, reason: str) -> None:
        payload = self._serializer.encode({"reason": reason})
        resp = Envelope.create(
            payload,
            kind="kv.error",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(resp)


class KvDeleteHandler:
    """Handle kv.delete: validate, call coordinator, send kv.delete.ok or kv.error."""

    def __init__(
        self,
        coordinator: KvCoordinator,
        serializer: SerializerPort,
        default_quorum_write: int = _DEFAULT_QUORUM,
    ) -> None:
        self._coordinator = coordinator
        self._serializer = serializer
        self._default_qw = default_quorum_write

    async def __call__(self, receive: ReceiveEnvelope, send: SendEnvelope) -> None:
        """Handle one kv.delete request."""
        req = await receive()
        try:
            data = self._serializer.decode(req.payload)
        except Exception:
            logger.warning("kv.delete: malformed payload")
            await self._send_error(send, req, "invalid_key")
            return

        key_raw = data.get("key")
        ks_raw = data.get("keyspace", b"default")
        qw = int(data.get("quorum_write") or self._default_qw)

        if not key_raw:
            await self._send_error(send, req, "invalid_key")
            return
        if qw < 1:
            await self._send_error(send, req, "invalid_quorum")
            return

        key = bytes(key_raw) if not isinstance(key_raw, bytes) else key_raw
        keyspace = bytes(ks_raw) if not isinstance(ks_raw, bytes) else ks_raw

        try:
            result = await self._coordinator.delete(key, keyspace, qw)
        except KvError as exc:
            await self._send_error(send, req, str(exc))
            return

        payload = self._serializer.encode(result)
        resp = Envelope.create(
            payload,
            kind="kv.delete.ok",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(resp)

    async def _send_error(self, send: SendEnvelope, req: Envelope, reason: str) -> None:
        payload = self._serializer.encode({"reason": reason})
        resp = Envelope.create(
            payload,
            kind="kv.error",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(resp)


class KvReplicateHandler:
    """Handle kv.replicate: write the record locally and ack."""

    def __init__(
        self,
        node_id: str,
        storage: Storage,
        partitioner: Partitioner,
        serializer: SerializerPort,
    ) -> None:
        self._node_id = node_id
        self._storage = storage
        self._partitioner = partitioner
        self._serializer = serializer

    async def __call__(self, receive: ReceiveEnvelope, send: SendEnvelope) -> None:
        """Handle one kv.replicate request."""
        req = await receive()
        try:
            data = self._serializer.decode(req.payload)
        except Exception:
            logger.warning("kv.replicate: malformed payload")
            return

        key_raw = data.get("key")
        ks_raw = data.get("keyspace", b"default")
        hlc_raw = data.get("hlc")
        qw = int(data.get("quorum_write") or 1)
        val_raw = data.get("value")

        if not key_raw or not hlc_raw:
            logger.warning("kv.replicate: missing required fields")
            return

        key = bytes(key_raw) if not isinstance(key_raw, bytes) else key_raw
        keyspace = bytes(ks_raw) if not isinstance(ks_raw, bytes) else ks_raw
        hlc = _hlc_from_dict(hlc_raw)
        meta = KvMetadata(hlc=hlc, quorum_write=qw)
        addr = StoreKey(keyspace=keyspace, key=key)

        try:
            pid = self._partitioner.pid_for_addr(addr)
            store = self._storage.open_partition(pid)
            if val_raw is None:
                await store.delete(addr, meta)
            else:
                val = bytes(val_raw) if not isinstance(val_raw, bytes) else val_raw
                await store.put(addr, val, meta)
        except Exception:
            logger.exception("kv.replicate: store write failed")
            return

        payload = self._serializer.encode({})
        resp = Envelope.create(
            payload,
            kind="kv.replicate.ok",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(resp)


class KvHintHandler:
    """Handle kv.hint: store the hint with HINT tag and ack."""

    def __init__(
        self,
        node_id: str,
        storage: Storage,
        partitioner: Partitioner,
        serializer: SerializerPort,
    ) -> None:
        self._node_id = node_id
        self._storage = storage
        self._partitioner = partitioner
        self._serializer = serializer

    async def __call__(self, receive: ReceiveEnvelope, send: SendEnvelope) -> None:
        """Handle one kv.hint request."""
        req = await receive()
        parsed = self._parse_hint(req.payload)
        if parsed is None:
            return
        addr, meta, for_node, val_raw = parsed
        if not await self._store_hint(addr, meta, for_node, val_raw):
            return
        payload = self._serializer.encode({})
        resp = Envelope.create(
            payload,
            kind="kv.hint.ok",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(resp)

    def _parse_hint(
        self, raw_payload: bytes
    ) -> tuple[StoreKey, KvMetadata, str, bytes | None] | None:
        """Decode and validate a kv.hint payload. Return None on error."""
        try:
            data = self._serializer.decode(raw_payload)
        except Exception:
            logger.warning("kv.hint: malformed payload")
            return None
        key_raw = data.get("key")
        ks_raw = data.get("keyspace", b"default")
        hlc_raw = data.get("hlc")
        for_node = str(data.get("for_node", ""))
        if not key_raw or not hlc_raw or not for_node:
            logger.warning("kv.hint: missing required fields")
            return None
        key = bytes(key_raw) if not isinstance(key_raw, bytes) else key_raw
        keyspace = bytes(ks_raw) if not isinstance(ks_raw, bytes) else ks_raw
        hlc = _hlc_from_dict(hlc_raw)
        meta = KvMetadata(hlc=hlc, quorum_write=int(data.get("quorum_write") or 1))
        addr = StoreKey(keyspace=keyspace, key=key)
        val_raw = data.get("value")
        return addr, meta, for_node, val_raw

    async def _store_hint(
        self,
        addr: StoreKey,
        meta: KvMetadata,
        for_node: str,
        val_raw: bytes | None,
    ) -> bool:
        """Write the hint to local storage. Return False on error."""
        try:
            pid = self._partitioner.pid_for_addr(addr)
            store = self._storage.open_partition(pid)
            hint_ctx = store.hint(for_node)
            if val_raw is None:
                await hint_ctx.delete(addr, meta)
            else:
                val = bytes(val_raw) if not isinstance(val_raw, bytes) else val_raw
                await hint_ctx.put(addr, val, meta)
            return True
        except Exception:
            logger.exception("kv.hint: hint store write failed")
            return False


class KvFetchHandler:
    """Handle kv.fetch: look up the local record and respond with kv.fetch.ok."""

    def __init__(
        self,
        node_id: str,
        storage: Storage,
        partitioner: Partitioner,
        serializer: SerializerPort,
    ) -> None:
        self._node_id = node_id
        self._storage = storage
        self._partitioner = partitioner
        self._serializer = serializer

    async def __call__(self, receive: ReceiveEnvelope, send: SendEnvelope) -> None:
        """Handle one kv.fetch request."""
        req = await receive()
        try:
            data = self._serializer.decode(req.payload)
        except Exception:
            logger.warning("kv.fetch: malformed payload")
            return

        key_raw = data.get("key")
        ks_raw = data.get("keyspace", b"default")

        if not key_raw:
            logger.warning("kv.fetch: missing key")
            return

        key = bytes(key_raw) if not isinstance(key_raw, bytes) else key_raw
        keyspace = bytes(ks_raw) if not isinstance(ks_raw, bytes) else ks_raw
        addr = StoreKey(keyspace=keyspace, key=key)

        try:
            pid = self._partitioner.pid_for_addr(addr)
            store = self._storage.open_partition(pid)
            record = await store.get(addr)
        except Exception:
            logger.exception("kv.fetch: store read failed")
            record = None

        if record is None:
            body: dict[str, Any] = {
                "found": False,
                "value": None,
                "hlc": None,
                "quorum_write": None,
            }
        elif isinstance(record, Tombstone):
            body = {
                "found": True,
                "value": None,
                "hlc": record.metadata.to_dict(),
                "quorum_write": record.quorum_write,
            }
        else:
            body = {
                "found": True,
                "value": record.value,
                "hlc": record.metadata.to_dict(),
                "quorum_write": record.quorum_write,
            }

        payload = self._serializer.encode(body)
        resp = Envelope.create(
            payload,
            kind="kv.fetch.ok",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(resp)


def register_kv_node_handlers(
    dispatcher: Any,  # noqa: ANN401
    node_id: str,
    storage: Storage,
    partitioner: Partitioner,
    serializer: SerializerPort,
) -> None:
    """Register inter-node KV handlers (replicate, hint, fetch) on *dispatcher*.

    These handlers are used for coordinator → replica replication traffic.
    They must be registered on the **peer dispatcher** so the coordinator can
    reach them via ``Member.peer_address`` (which is the peer server address,
    not the KV server address). Registering them on the KV dispatcher as well
    is harmless but not required.
    """
    dispatcher.register(
        "kv.replicate", KvReplicateHandler(node_id, storage, partitioner, serializer)
    )
    dispatcher.register(
        "kv.hint", KvHintHandler(node_id, storage, partitioner, serializer)
    )
    dispatcher.register(
        "kv.fetch", KvFetchHandler(node_id, storage, partitioner, serializer)
    )


def register_kv_handlers(
    dispatcher: Any,  # noqa: ANN401
    coordinator: KvCoordinator,
    node_id: str,
    storage: Storage,
    partitioner: Partitioner,
    serializer: SerializerPort,
    default_quorum_write: int = 1,
    default_quorum_read: int = 1,
) -> None:
    """Register all KV handlers (client-facing + inter-node) on *dispatcher*."""
    dispatcher.register(
        "kv.put", KvPutHandler(coordinator, serializer, default_quorum_write)
    )
    dispatcher.register(
        "kv.get", KvGetHandler(coordinator, serializer, default_quorum_read)
    )
    dispatcher.register(
        "kv.delete", KvDeleteHandler(coordinator, serializer, default_quorum_write)
    )
    register_kv_node_handlers(dispatcher, node_id, storage, partitioner, serializer)
