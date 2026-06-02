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

import base64
import signal
import socket
import subprocess
import time
import tomllib
from pathlib import Path

import pytest
import tomli_w

from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.pki import CaRequest, CertRequest
from tourillon.core.structure.config import NodeSize
from tourillon.infra.pki.x509 import (
    CryptographyCaAdapter,
    CryptographyCertIssuerAdapter,
)
from tourillon.infra.store.state import _encode_state


def _write_config(tmp_path: Path) -> tuple[Path, Path]:
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
            "size": NodeSize.M.value,
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
    return config_path, data_dir


_INTERRUPTED_RETURN_CODES = {0, 1, 3221225786}


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _run_start_and_stop(config_path: Path) -> subprocess.CompletedProcess[str]:
    creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    process = subprocess.Popen(
        ["uv", "run", "tourillon", "node", "start", "--config", str(config_path)],
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creationflags,
    )

    try:
        time.sleep(1.0)

        if process.poll() is None:
            if hasattr(signal, "CTRL_BREAK_EVENT") and creationflags:
                process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                process.send_signal(signal.SIGINT)

        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)

    return subprocess.CompletedProcess(
        process.args,
        process.returncode,
        stdout="",
        stderr="",
    )


@pytest.mark.ring
@pytest.mark.e2e
def test_49_first_start_writes_ready_state(tmp_path: Path) -> None:
    config_path, data_dir = _write_config(tmp_path)
    state_path = data_dir / "state.toml"
    completed = _run_start_and_stop(config_path)
    assert completed.returncode in _INTERRUPTED_RETURN_CODES
    assert state_path.exists()
    state = tomllib.loads(state_path.read_text(encoding="utf-8"))
    assert state["node"]["phase"] == "ready"
    assert len(state["node"]["tokens"]) == NodeSize.M.token_count


@pytest.mark.ring
@pytest.mark.e2e
def test_50_ready_restart_keeps_tokens_unchanged(tmp_path: Path) -> None:
    config_path, data_dir = _write_config(tmp_path)
    state_path = data_dir / "state.toml"
    tokens = (5, 10, 15, 20)
    state = NodeState("node-1", MemberPhase.READY, 1, 0, tokens, 1)
    state_path.write_text(tomli_w.dumps(_encode_state(state)), encoding="utf-8")
    before = state_path.stat().st_mtime_ns

    completed = _run_start_and_stop(config_path)
    assert completed.returncode in _INTERRUPTED_RETURN_CODES
    parsed = tomllib.loads(state_path.read_text(encoding="utf-8"))
    assert tuple(parsed["node"]["tokens"]) == tokens
    assert state_path.stat().st_mtime_ns == before
