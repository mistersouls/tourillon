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
"""E2E tests for proposal 006 — KV read/write path.

Every test exercises the full stack:
  load_config() → KvCoordinator wiring → TcpServer (real mTLS)
  → TcpClient → kv.put / kv.get / kv.delete envelopes

The test topology uses a single-node cluster (RF=1, W=1, R=1) so that
quorum is trivially satisfied and tests remain fast without multi-process
coordination.

Scenarios covered:
  1  — kv.put stores a COMMITTED version
  2  — kv.get returns case-1 confirmed response after put
  13 — CLI inference: "42" → msgpack int
  14 — CLI -t str: "42" → msgpack str
  15 — CLI @@: escape → literal @alice string
  19 — kv.delete stores a COMMITTED tombstone
  20 — kv.get after delete returns confirmed tombstone (value=null)
"""

from __future__ import annotations

import base64
import contextlib
from collections.abc import AsyncIterator
from typing import Any

import msgpack
import pytest

from tourillon.bootstrap.config import load_config
from tourillon.core.handlers.kv import register_kv_handlers
from tourillon.core.kv.coordinator import KvCoordinator
from tourillon.core.kv.encoding import resolve_arg
from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.placement import SimplePreferenceStrategy
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.config import TourillonConfig
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.record import StoreKey
from tourillon.core.testing.mem_storage import InMemoryStorage
from tourillon.core.transport.client import TcpClient
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.pool import PeerClientPool
from tourillon.core.transport.server import TcpServer
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter
from tourillon.infra.tls.context import (
    build_client_ssl_context,
    build_server_ssl_context,
)

_SHIFT = 10


# ---------------------------------------------------------------------------
# Config helper
# ---------------------------------------------------------------------------


def _make_config(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
    node_id: str = "node-1",
) -> TourillonConfig:
    """Build a single-node TourillonConfig via load_config()."""
    ca_cert_pem, _ = ca_material
    leaf_cert_pem, leaf_key_pem = leaf_material
    ca_b64 = base64.b64encode(ca_cert_pem).decode()
    cert_b64 = base64.b64encode(leaf_cert_pem).decode()
    key_b64 = base64.b64encode(leaf_key_pem).decode()
    return load_config(
        {
            "schema_version": 1,
            "node": {"id": node_id, "size": "XS", "data_dir": "./node-data"},
            "tls": {"cert_data": cert_b64, "key_data": key_b64, "ca_data": ca_b64},
            "servers": {
                "kv": {"bind": "127.0.0.1:0", "advertise": "127.0.0.1:0"},
                "peer": {"bind": "127.0.0.1:1", "advertise": "127.0.0.1:1"},
            },
            "cluster": {"seeds": [], "rf": 1, "partition_shift": _SHIFT},
            "kv": {"fanout_timeout_ms": 50},
        }
    )


# ---------------------------------------------------------------------------
# Node fixture
# ---------------------------------------------------------------------------


@contextlib.asynccontextmanager
async def _running_kv_node(
    cfg: TourillonConfig,
) -> AsyncIterator[tuple[str, InMemoryStorage, KvCoordinator]]:
    """Start a KV server with all handlers registered. Yield (endpoint, storage, coordinator).

    Uses a READY single-node topology so the coordinator can route to itself
    without any network hop.
    """
    server_ssl = build_server_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )
    client_ssl = build_client_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )

    storage = InMemoryStorage()
    serializer = MsgpackSerializerAdapter()

    hash_space = HashSpace()
    partitioner = Partitioner(hash_space, cfg.partition_shift)
    topology_mgr = TopologyManager()
    probe_mgr = ProbeManager()
    pool = PeerClientPool(ssl_ctx=client_ssl)  # type: ignore[arg-type]

    # Register the local node as READY so preference_list returns it
    from tourillon.core.lifecycle.member import Member

    vnode_token = hash_space.max // 2  # single token at midpoint
    member = Member(
        node_id=cfg.node_id,
        peer_address="127.0.0.1:0",
        generation=1,
        seq=1,
        phase=MemberPhase.READY,
        tokens=(vnode_token,),
        partition_shift=cfg.partition_shift,
    )
    await topology_mgr.apply_member(member)

    strategy = SimplePreferenceStrategy(rf=cfg.rf)
    coordinator = KvCoordinator(
        node_id=cfg.node_id,
        storage=storage,
        partitioner=partitioner,
        topology_manager=topology_mgr,
        probe_manager=probe_mgr,
        placement_strategy=strategy,
        pool=pool,
        serializer=serializer,
        fanout_timeout=cfg.kv.fanout_timeout_ms / 1000.0,
    )

    dispatcher = Dispatcher()
    register_kv_handlers(
        dispatcher=dispatcher,
        coordinator=coordinator,
        node_id=cfg.node_id,
        storage=storage,
        partitioner=partitioner,
        serializer=serializer,
        default_quorum_write=1,
        default_quorum_read=1,
    )

    server = TcpServer(dispatcher, ssl_context=server_ssl, name=f"kv-e2e-{cfg.node_id}")  # type: ignore[arg-type]
    await server.start("127.0.0.1", 0)
    port: int = server._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    endpoint = f"127.0.0.1:{port}"

    try:
        yield endpoint, storage, coordinator
    finally:
        await server.stop()
        await pool.close_all()


# ---------------------------------------------------------------------------
# tourctl helper — mirrors what tourctl kv put/get/delete does
# ---------------------------------------------------------------------------


async def _tourctl_kv_put(
    cfg: TourillonConfig,
    endpoint: str,
    key: bytes,
    keyspace: bytes,
    value: bytes,
    *,
    quorum_write: int = 1,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """Invoke KvPutCommand exactly as tourctl does. Return decoded response dict."""
    from tourctl.core.commands.kv import KvPutCommand
    from tourillon.core.transport.client import TcpClient

    tls_ctx = build_client_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )
    client = TcpClient()
    await client.connect(endpoint, tls_ctx)
    serializer = MsgpackSerializerAdapter()

    results: dict[str, Any] = {}

    class _CapturingConsole:
        def __init__(self) -> None:
            self.lines: list[str] = []

        def print(self, msg: str = "", **kwargs: Any) -> None:
            self.lines.append(str(msg))

    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvPutCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )
    try:
        exit_code = await cmd.run(
            key, keyspace, value, quorum_write=quorum_write, timeout=timeout
        )
        results["exit_code"] = exit_code
        results["output"] = out.lines
        results["errors"] = err.lines
    finally:
        await client.close()
    return results


async def _tourctl_kv_get(
    cfg: TourillonConfig,
    endpoint: str,
    key: bytes,
    keyspace: bytes,
    *,
    quorum_read: int = 1,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """Invoke KvGetCommand exactly as tourctl does. Return decoded response dict."""
    from tourctl.core.commands.kv import KvGetCommand
    from tourillon.core.transport.client import TcpClient

    tls_ctx = build_client_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )
    client = TcpClient()
    await client.connect(endpoint, tls_ctx)
    serializer = MsgpackSerializerAdapter()

    class _CapturingConsole:
        def __init__(self) -> None:
            self.lines: list[str] = []

        def print(self, msg: str = "", **kwargs: Any) -> None:
            self.lines.append(str(msg))

    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvGetCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )
    try:
        exit_code = await cmd.run(
            key, keyspace, quorum_read=quorum_read, timeout=timeout
        )
        return {"exit_code": exit_code, "output": out.lines, "errors": err.lines}
    finally:
        await client.close()


async def _tourctl_kv_delete(
    cfg: TourillonConfig,
    endpoint: str,
    key: bytes,
    keyspace: bytes,
    *,
    quorum_write: int = 1,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """Invoke KvDeleteCommand exactly as tourctl does. Return decoded response dict."""
    from tourctl.core.commands.kv import KvDeleteCommand
    from tourillon.core.transport.client import TcpClient

    tls_ctx = build_client_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )
    client = TcpClient()
    await client.connect(endpoint, tls_ctx)
    serializer = MsgpackSerializerAdapter()

    class _CapturingConsole:
        def __init__(self) -> None:
            self.lines: list[str] = []

        def print(self, msg: str = "", **kwargs: Any) -> None:
            self.lines.append(str(msg))

    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvDeleteCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )
    try:
        exit_code = await cmd.run(
            key, keyspace, quorum_write=quorum_write, timeout=timeout
        )
        return {"exit_code": exit_code, "output": out.lines, "errors": err.lines}
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Low-level envelope helpers (for direct envelope tests)
# ---------------------------------------------------------------------------


async def _raw_put(
    cfg: TourillonConfig,
    endpoint: str,
    key: bytes,
    keyspace: bytes,
    value: bytes,
    *,
    quorum_write: int = 1,
) -> Envelope:
    """Send raw kv.put and return the response envelope."""
    tls_ctx = build_client_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )
    client = TcpClient()
    await client.connect(endpoint, tls_ctx)
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode(
        {"key": key, "keyspace": keyspace, "value": value, "quorum_write": quorum_write}
    )
    env = Envelope.create(payload, kind="kv.put", schema_id=serializer.schema_id)
    try:
        return await client.request(env, timeout=5.0)
    finally:
        await client.close()


async def _raw_get(
    cfg: TourillonConfig,
    endpoint: str,
    key: bytes,
    keyspace: bytes,
    *,
    quorum_read: int = 1,
) -> Envelope:
    """Send raw kv.get and return the response envelope."""
    tls_ctx = build_client_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )
    client = TcpClient()
    await client.connect(endpoint, tls_ctx)
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode(
        {"key": key, "keyspace": keyspace, "quorum_read": quorum_read}
    )
    env = Envelope.create(payload, kind="kv.get", schema_id=serializer.schema_id)
    try:
        return await client.request(env, timeout=5.0)
    finally:
        await client.close()


async def _raw_delete(
    cfg: TourillonConfig,
    endpoint: str,
    key: bytes,
    keyspace: bytes,
    *,
    quorum_write: int = 1,
) -> Envelope:
    """Send raw kv.delete and return the response envelope."""
    tls_ctx = build_client_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )
    client = TcpClient()
    await client.connect(endpoint, tls_ctx)
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode(
        {"key": key, "keyspace": keyspace, "quorum_write": quorum_write}
    )
    env = Envelope.create(payload, kind="kv.delete", schema_id=serializer.schema_id)
    try:
        return await client.request(env, timeout=5.0)
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Scenario 1 — kv.put stores a COMMITTED version
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.kv
async def test_1_kvput_1node_rf1_w1_stores_committed_version(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """kv.put.ok; replica stores COMMITTED version."""
    cfg = _make_config(ca_material, leaf_material)
    async with _running_kv_node(cfg) as (endpoint, storage, coordinator):
        val_bytes = msgpack.packb("Alice", use_bin_type=True)
        resp = await _raw_put(cfg, endpoint, b"user:001", b"default", val_bytes)

        assert resp.kind == "kv.put.ok", f"Expected kv.put.ok, got {resp.kind}"
        serializer = MsgpackSerializerAdapter()
        data = serializer.decode(resp.payload)
        assert data.get("replicas", 0) >= 1

        # Check storage
        pid = coordinator._partitioner.pid_for_addr(
            StoreKey(keyspace=b"default", key=b"user:001")
        )
        record = await storage.open_partition(pid).get(
            StoreKey(keyspace=b"default", key=b"user:001")
        )
        assert record is not None
        assert hasattr(record, "value")
        assert record.value == val_bytes


# ---------------------------------------------------------------------------
# Scenario 2 — kv.get returns case-1 confirmed response after put
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.kv
async def test_2_kvget_1node_rf1_w1_r1_confirmed_after_put(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """case 1 response, confirmed=true, correct value."""
    cfg = _make_config(ca_material, leaf_material)
    async with _running_kv_node(cfg) as (endpoint, storage, coordinator):
        val_bytes = msgpack.packb("Alice", use_bin_type=True)
        put_resp = await _raw_put(cfg, endpoint, b"user:001", b"default", val_bytes)
        assert put_resp.kind == "kv.put.ok"

        get_resp = await _raw_get(cfg, endpoint, b"user:001", b"default")
        assert get_resp.kind == "kv.get.ok"

        serializer = MsgpackSerializerAdapter()
        data = serializer.decode(get_resp.payload)
        versions = data.get("versions", [])
        assert len(versions) == 1
        v = versions[0]
        assert v["confirmed"] is True
        assert v["value"] == val_bytes


# ---------------------------------------------------------------------------
# Scenario 13 — CLI inference: "42" → msgpack int
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.kv
async def test_13_cli_inference_numeric_string_encodes_as_int(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """value encoded as msgpack int 42."""
    cfg = _make_config(ca_material, leaf_material)
    async with _running_kv_node(cfg) as (endpoint, storage, coordinator):
        # Simulate CLI encoding: resolve_arg("42", None) → 42 (int) → msgpack int
        python_value = resolve_arg("42", None)
        assert isinstance(python_value, int)
        val_bytes = msgpack.packb(python_value, use_bin_type=True)

        resp = await _raw_put(cfg, endpoint, b"score", b"metrics", val_bytes)
        assert resp.kind == "kv.put.ok"

        # Verify stored as int
        pid = coordinator._partitioner.pid_for_addr(
            StoreKey(keyspace=b"metrics", key=b"score")
        )
        record = await storage.open_partition(pid).get(
            StoreKey(keyspace=b"metrics", key=b"score")
        )
        assert record is not None
        decoded = msgpack.unpackb(record.value, raw=False)
        assert decoded == 42
        assert isinstance(decoded, int)


# ---------------------------------------------------------------------------
# Scenario 14 — CLI explicit -t str: "42" → msgpack str "42"
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.kv
async def test_14_cli_explicit_str_type_bypasses_inference(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """value encoded as msgpack str "42"."""
    cfg = _make_config(ca_material, leaf_material)
    async with _running_kv_node(cfg) as (endpoint, storage, coordinator):
        python_value = resolve_arg("42", "str")
        assert isinstance(python_value, str)
        val_bytes = msgpack.packb(python_value, use_bin_type=True)

        resp = await _raw_put(cfg, endpoint, b"score", b"metrics", val_bytes)
        assert resp.kind == "kv.put.ok"

        pid = coordinator._partitioner.pid_for_addr(
            StoreKey(keyspace=b"metrics", key=b"score")
        )
        record = await storage.open_partition(pid).get(
            StoreKey(keyspace=b"metrics", key=b"score")
        )
        assert record is not None
        decoded = msgpack.unpackb(record.value, raw=False)
        assert decoded == "42"
        assert isinstance(decoded, str)


# ---------------------------------------------------------------------------
# Scenario 15 — CLI @@ escape → literal @alice string
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.kv
async def test_15_cli_at_at_escape_produces_literal_at_string(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """value literal @alice, encoded as msgpack str."""
    # @@alice → @alice (by resolve_arg logic)
    raw_arg = "@@alice"
    python_value = resolve_arg(raw_arg, None)
    # After stripping @@, raw="@alice", which fails json.loads → kept as str
    assert python_value == "@alice"
    val_bytes = msgpack.packb(python_value, use_bin_type=True)

    cfg = _make_config(ca_material, leaf_material)
    async with _running_kv_node(cfg) as (endpoint, storage, coordinator):
        resp = await _raw_put(cfg, endpoint, b"handle", b"default", val_bytes)
        assert resp.kind == "kv.put.ok"

        pid = coordinator._partitioner.pid_for_addr(
            StoreKey(keyspace=b"default", key=b"handle")
        )
        record = await storage.open_partition(pid).get(
            StoreKey(keyspace=b"default", key=b"handle")
        )
        assert record is not None
        decoded = msgpack.unpackb(record.value, raw=False)
        assert decoded == "@alice"


# ---------------------------------------------------------------------------
# Scenario 19 — kv.delete stores a COMMITTED tombstone
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.kv
async def test_19_kvdelete_w2_confirmed_tombstone_acks(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """kv.delete.ok; confirmed tombstone stored."""
    cfg = _make_config(ca_material, leaf_material)
    async with _running_kv_node(cfg) as (endpoint, storage, coordinator):
        # First put a value, then delete it
        val_bytes = msgpack.packb("Alice", use_bin_type=True)
        put_resp = await _raw_put(cfg, endpoint, b"user:001", b"default", val_bytes)
        assert put_resp.kind == "kv.put.ok"

        del_resp = await _raw_delete(cfg, endpoint, b"user:001", b"default")
        assert del_resp.kind == "kv.delete.ok"

        serializer = MsgpackSerializerAdapter()
        data = serializer.decode(del_resp.payload)
        assert data.get("replicas", 0) >= 1

        # Verify tombstone in storage
        from tourillon.core.structure.record import Tombstone

        pid = coordinator._partitioner.pid_for_addr(
            StoreKey(keyspace=b"default", key=b"user:001")
        )
        record = await storage.open_partition(pid).get(
            StoreKey(keyspace=b"default", key=b"user:001")
        )
        assert isinstance(record, Tombstone), f"Expected Tombstone, got {type(record)}"


# ---------------------------------------------------------------------------
# Scenario 20 — kv.get after delete returns confirmed tombstone
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.kv
async def test_20_kvget_after_delete_returns_confirmed_tombstone(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """case 1, value=null, confirmed=true."""
    cfg = _make_config(ca_material, leaf_material)
    async with _running_kv_node(cfg) as (endpoint, storage, coordinator):
        val_bytes = msgpack.packb("Alice", use_bin_type=True)
        await _raw_put(cfg, endpoint, b"user:001", b"default", val_bytes)
        del_resp = await _raw_delete(cfg, endpoint, b"user:001", b"default")
        assert del_resp.kind == "kv.delete.ok"

        get_resp = await _raw_get(cfg, endpoint, b"user:001", b"default")
        assert get_resp.kind == "kv.get.ok"

        serializer = MsgpackSerializerAdapter()
        data = serializer.decode(get_resp.payload)
        versions = data.get("versions", [])
        assert len(versions) == 1
        v = versions[0]
        assert v["confirmed"] is True
        assert v["value"] is None, f"Expected null for tombstone, got {v['value']!r}"


# ---------------------------------------------------------------------------
# tourctl command integration — kv put / get / delete via command objects
# ---------------------------------------------------------------------------


@pytest.mark.e2e
@pytest.mark.kv
async def test_tourctl_kv_put_renders_success(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """tourctl kv put returns exit code 0 and prints success line."""
    cfg = _make_config(ca_material, leaf_material)
    async with _running_kv_node(cfg) as (endpoint, storage, coordinator):
        val_bytes = msgpack.packb("Alice", use_bin_type=True)
        result = await _tourctl_kv_put(
            cfg, endpoint, b"user:001", b"default", val_bytes
        )
        assert result["exit_code"] == 0
        assert any("written" in line or "✓" in line for line in result["output"])


@pytest.mark.e2e
@pytest.mark.kv
async def test_tourctl_kv_get_renders_confirmed(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """tourctl kv get returns exit code 0 and prints confirmed after put."""
    cfg = _make_config(ca_material, leaf_material)
    async with _running_kv_node(cfg) as (endpoint, storage, coordinator):
        val_bytes = msgpack.packb("Alice", use_bin_type=True)
        await _tourctl_kv_put(cfg, endpoint, b"user:001", b"default", val_bytes)

        result = await _tourctl_kv_get(cfg, endpoint, b"user:001", b"default")
        assert result["exit_code"] == 0
        combined = "\n".join(result["output"])
        assert "confirmed" in combined.lower() or "✓" in combined


@pytest.mark.e2e
@pytest.mark.kv
async def test_tourctl_kv_get_not_found_exits_1(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """tourctl kv get on missing key returns exit code 1."""
    cfg = _make_config(ca_material, leaf_material)
    async with _running_kv_node(cfg) as (endpoint, storage, coordinator):
        result = await _tourctl_kv_get(cfg, endpoint, b"nonexistent:key", b"default")
        assert result["exit_code"] == 1


@pytest.mark.e2e
@pytest.mark.kv
async def test_tourctl_kv_delete_renders_deleted(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """tourctl kv delete returns exit code 0 and prints 'deleted'."""
    cfg = _make_config(ca_material, leaf_material)
    async with _running_kv_node(cfg) as (endpoint, storage, coordinator):
        val_bytes = msgpack.packb("Alice", use_bin_type=True)
        await _tourctl_kv_put(cfg, endpoint, b"user:001", b"default", val_bytes)

        result = await _tourctl_kv_delete(cfg, endpoint, b"user:001", b"default")
        assert result["exit_code"] == 0
        assert any("deleted" in line or "✓" in line for line in result["output"])
