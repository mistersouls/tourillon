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

from __future__ import annotations

import asyncio
import base64
import socket
import ssl
from pathlib import Path

import pytest
import tomli_w
from typer.testing import CliRunner

from tests.helpers.adapters import InMemoryStateAdapter
from tourillon.bootstrap import node_start as node_start_bootstrap
from tourillon.core.lifecycle.bootstrap import Bootstraper, BootstrapError
from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.probe import MemberState, ProbeManager
from tourillon.core.ports.pki import CaRequest, CertRequest
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import PartitionRange
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.ring.vnode import VNode
from tourillon.core.structure.config import (
    NodeSize,
    ServerConfig,
    TlsConfig,
    TourillonConfig,
)
from tourillon.infra.cli.main import app
from tourillon.infra.pki.x509 import (
    CryptographyCaAdapter,
    CryptographyCertIssuerAdapter,
)


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _config(tmp_path: Path) -> Path:
    kv_port = _find_free_port()
    peer_port = _find_free_port()

    ca = CryptographyCaAdapter()
    issuer = CryptographyCertIssuerAdapter()
    ca_cert = tmp_path / "ca.pem"
    ca_key = tmp_path / "ca-key.pem"
    node_cert = tmp_path / "node.pem"
    node_key = tmp_path / "node-key.pem"
    ca.generate_ca(CaRequest("tourillon-ca", 3650, 2048, ca_cert, ca_key))
    issuer.issue_cert(
        CertRequest(
            common_name="node-1",
            san_dns=(),
            san_ip=(),
            valid_days=365,
            ca_cert=ca_cert,
            ca_key=ca_key,
            out_cert=node_cert,
            out_key=node_key,
            key_size=2048,
        )
    )

    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "node": {
            "id": "node-1",
            "size": "M",
            "data_dir": str(data_dir),
            "rf": 3,
            "partition_shift": 10,
            "seeds": [],
        },
        "servers": {
            "kv": {"bind": f"127.0.0.1:{kv_port}"},
            "peer": {
                "bind": f"127.0.0.1:{peer_port}",
                "advertise": f"127.0.0.1:{peer_port}",
            },
        },
        "tls": {
            "cert_data": base64.b64encode(node_cert.read_bytes()).decode("ascii"),
            "key_data": base64.b64encode(node_key.read_bytes()).decode("ascii"),
            "ca_data": base64.b64encode(ca_cert.read_bytes()).decode("ascii"),
        },
    }
    config_path = tmp_path / "config.toml"
    config_path.write_text(tomli_w.dumps(payload), encoding="utf-8")
    return config_path


@pytest.mark.ring
def test_51_node_start_missing_config() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["node", "start", "--config", "missing.toml"])
    assert result.exit_code == 1


@pytest.mark.ring
def test_52_node_start_happy_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _fake_run(
        self,
        *,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        _ = self
        _ = stop_event
        return

    runner = CliRunner()
    config = _config(tmp_path)
    monkeypatch.setattr(node_start_bootstrap.NodeRuntimeLifecycle, "run", _fake_run)
    result = runner.invoke(app, ["node", "start", "--config", str(config)])
    assert result.exit_code == 0
    assert (tmp_path / "data" / "state.toml").exists()


@pytest.mark.ring
async def test_53_probe_manager_misc_methods() -> None:
    pm = ProbeManager()
    await pm.record_miss("node-1")
    assert await pm.is_unknown("node-1") is True
    assert await pm.phi_of("node-1") == 0.0
    await pm.record_heartbeat("node-1")
    assert await pm.is_live("node-1") is True
    assert (await pm.snapshot())["node-1"] in {MemberState.UNKNOWN, MemberState.LIVE}


@pytest.mark.ring
async def test_54_bootstraper_error_helpers() -> None:
    cfg = TourillonConfig(
        node_id="node-1",
        node_size=NodeSize.M,
        data_dir="./data",
        tls=TlsConfig("", "", ""),
        kv_server=ServerConfig("127.0.0.1:7000"),
        peer_server=ServerConfig("127.0.0.1:7001"),
        partition_shift=10,
    )
    boot = Bootstraper(
        cfg,
        InMemoryStateAdapter(None),
        TopologyManager(),
        HashSpace(),
        segment_shift=2,
    )
    with pytest.raises(BootstrapError):
        Bootstraper.from_config_path(Path("config.toml"))
    with pytest.raises(BootstrapError):
        await boot.start_ready_node()
    assert MemberPhase.READY.value == "ready"


@pytest.mark.ring
async def test_55_serve_node_stops_when_stop_event_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    starts: list[tuple[str, str, int]] = []
    stops: list[str] = []

    class _FakeServer:
        def __init__(
            self,
            _dispatcher,
            ssl_context=None,
            max_payload: int = 0,
            name: str = "server",
        ) -> None:
            _ = ssl_context
            _ = max_payload
            self._name = name

        async def start(self, host: str, port: int) -> None:
            starts.append((self._name, host, port))

        async def stop(self) -> None:
            stops.append(self._name)

    cfg = TourillonConfig(
        node_id="node-1",
        node_size=NodeSize.M,
        data_dir="./data",
        tls=TlsConfig("", "", ""),
        kv_server=ServerConfig("127.0.0.1:7000"),
        peer_server=ServerConfig("127.0.0.1:7001"),
        partition_shift=10,
    )
    stop_event = asyncio.Event()
    stop_event.set()

    monkeypatch.setattr(node_start_bootstrap, "TcpServer", _FakeServer)
    await node_start_bootstrap.serve_node(
        cfg,
        ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER),
        ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER),
        stop_event=stop_event,
    )

    assert starts == [
        ("Peer", "127.0.0.1", 7001),
        ("KV", "127.0.0.1", 7000),
    ]
    assert stops == ["KV", "Peer"]


@pytest.mark.ring
async def test_56_runtime_can_restart_kv_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    starts: list[tuple[str, str, int]] = []
    stops: list[str] = []

    class _FakeServer:
        def __init__(
            self,
            _dispatcher,
            ssl_context=None,
            max_payload: int = 0,
            name: str = "server",
        ) -> None:
            _ = ssl_context
            _ = max_payload
            self._name = name

        async def start(self, host: str, port: int) -> None:
            starts.append((self._name, host, port))

        async def stop(self) -> None:
            stops.append(self._name)

    cfg = TourillonConfig(
        node_id="node-1",
        node_size=NodeSize.M,
        data_dir="./data",
        tls=TlsConfig("", "", ""),
        kv_server=ServerConfig("127.0.0.1:7000"),
        peer_server=ServerConfig("127.0.0.1:7001"),
        partition_shift=10,
    )

    monkeypatch.setattr(node_start_bootstrap, "TcpServer", _FakeServer)
    runtime = node_start_bootstrap.NodeRuntimeLifecycle(
        cfg,
        ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER),
        ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER),
    )
    await runtime.start()
    await runtime.restart_kv()
    await runtime.stop_all()

    assert starts == [
        ("Peer", "127.0.0.1", 7001),
        ("KV", "127.0.0.1", 7000),
        ("KV", "127.0.0.1", 7000),
    ]
    assert stops == ["KV", "KV", "Peer"]


@pytest.mark.ring
def test_57_bootstrap_ranges_log_uses_en_dash_and_arrow(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _BootstraperStub:
        ranges = [
            PartitionRange(
                owner=VNode(node_id="node-1", token=0xAF3C12B8),
                start_pid=772,
                end_pid=204,
                count=323,
            )
        ]

    caplog.set_level("INFO", logger="tourillon.bootstrap.node")
    node_start_bootstrap._log_bootstrap_ranges(_BootstraperStub(), partition_shift=10)

    messages = [record.message for record in caplog.records]
    assert "Partition ranges owned (1024 total partitions):" in messages
    assert "token 0xaf3c12b8… → pids [772–204]  (323 partitions)" in messages


@pytest.mark.ring
def test_58_bootstrap_ranges_log_truncates_long_token_preview(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _BootstraperStub:
        ranges = [
            PartitionRange(
                owner=VNode(
                    node_id="node-1",
                    token=0x3CD5E5FC8186E8C45DFDDCBFA956A61A,
                ),
                start_pid=12,
                end_pid=42,
                count=31,
            )
        ]

    caplog.set_level("INFO", logger="tourillon.bootstrap.node")
    node_start_bootstrap._log_bootstrap_ranges(_BootstraperStub(), partition_shift=10)

    messages = [record.message for record in caplog.records]
    assert "token 0x3cd5e5fc… → pids [12–42]  (31 partitions)" in messages
