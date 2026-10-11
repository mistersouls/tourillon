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

import json
import ssl
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from cyclopts.exceptions import MissingArgumentError

from tests.helpers import generate_ca, start_envelope_server
from tourctl.bootstrap.main import app
from tourctl.bootstrap.usecases.common import GlobalOptions
from tourctl.bootstrap.usecases.node import node_inspect
from tourctl.core.inspect import inspect_node
from tourillon.core.config import ClientConfigGenerator
from tourlib.adapter import CryptographyMtlsAdapter, MsgPackSerializer, TomlPersistence
from tourlib.envelope import Envelope, replay_envelope


async def _build_tourconf(
    tmp_path: Path,
    *,
    client_address: str,
    ca_cert_path: Path,
    ca_key_path: Path,
) -> Path:
    generator = ClientConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    result = await generator.generate(
        output_file=tmp_path / "tourconf.toml",
        common_name="tourctl-inspect",
        ca_certificate_path=ca_cert_path,
        ca_private_key_path=ca_key_path,
        client_address=client_address,
        validity_days=30,
        key_size=2048,
        overwrite=False,
    )
    return result.config_path


def _build_server_ssl_from_ca(
    tmp_path: Path,
    ca_cert_path: Path,
    ca_key_path: Path,
) -> ssl.SSLContext:
    crypto = CryptographyMtlsAdapter()
    server_cert_pem, server_key_pem = crypto.generate_cert(
        common_name="tourillon-server",
        ca_certificate_pem=ca_cert_path.read_bytes(),
        ca_private_key_pem=ca_key_path.read_bytes(),
        validity_days=30,
        key_size=2048,
        client_only=False,
    )

    server_cert_path = tmp_path / "server-cert.pem"
    server_key_path = tmp_path / "server-key.pem"
    server_cert_path.write_bytes(server_cert_pem)
    server_key_path.write_bytes(server_key_pem)

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(server_cert_path), str(server_key_path))
    context.load_verify_locations(cafile=str(ca_cert_path))
    context.verify_mode = ssl.CERT_REQUIRED
    return context


def _node_info_payload() -> dict[str, object]:
    return {
        "node_id": "node-3",
        "snapshot_at": "2026-08-16T09:15:20Z",
        "chunk_index": 0,
        "has_more": False,
        "next_cursor": None,
        "ring_position": {
            "partition_shift": 10,
            "total_partitions": 1024,
            "owned_partitions": 256,
            "owned_percent": 25.0,
            "vnode_ranges": [
                {
                    "token": "0x1a3cb4f2",
                    "arc": [900, 120],
                    "partitions": 244,
                }
            ],
        },
        "members": [
            {"node_id": "node-1", "phase": "READY", "status": "LIVE", "epoch": 3},
            {"node_id": "node-2", "phase": "READY", "status": "SUSPECT", "epoch": 3},
            {"node_id": "node-3", "phase": "READY", "status": "-", "epoch": 3},
        ],
        "self_member": {"node_id": "node-3", "phase": "READY", "status": "-", "epoch": 3},
        "gossip_stats": {
            "known_members": 3,
            "ae_cycles_total": 128,
            "ae_diverged": 4,
            "last_ae_peer": "node-2",
            "last_ae_at": "2026-08-16T09:14:58Z",
            "bootstrap_ok": 1,
            "bootstrap_err": 0,
            "last_hot_failure": None,
            "last_ae_failure": None,
            "last_stop_failure": None,
        },
    }


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_node_inspect_returns_human_readable_output(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    server_ssl = _build_server_ssl_from_ca(tmp_path, ca_cert_path, ca_key_path)
    serializer = MsgPackSerializer()

    async def inspect_handler(request: Envelope) -> AsyncIterator[Envelope]:
        yield replay_envelope(
            kind="node.info",
            request=request,
            payload=serializer.encode(_node_info_payload()),
        )

    server = await start_envelope_server({"node.inspect": inspect_handler}, server_ssl)

    try:
        host, port = server.endpoint
        tourconf = await _build_tourconf(
            tmp_path,
            client_address="127.0.0.1:9999",
            ca_cert_path=ca_cert_path,
            ca_key_path=ca_key_path,
        )
        output = await inspect_node(
            address=f"{host}:{port}",
            tourconf=tourconf,
        )
    finally:
        await server.stop()

    assert "Inspecting" in output
    assert "node-3" in output
    assert "Cluster membership" in output
    assert "node-2" in output
    assert "SUSPECT" in output
    assert "(w)" in output


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_node_inspect_returns_json_output(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    server_ssl = _build_server_ssl_from_ca(tmp_path, ca_cert_path, ca_key_path)
    serializer = MsgPackSerializer()

    async def inspect_handler(request: Envelope) -> AsyncIterator[Envelope]:
        yield replay_envelope(
            kind="node.info",
            request=request,
            payload=serializer.encode(_node_info_payload()),
        )

    server = await start_envelope_server({"node.inspect": inspect_handler}, server_ssl)

    try:
        host, port = server.endpoint
        tourconf = await _build_tourconf(
            tmp_path,
            client_address="127.0.0.1:9999",
            ca_cert_path=ca_cert_path,
            ca_key_path=ca_key_path,
        )
        output = await inspect_node(
            address=f"{host}:{port}",
            tourconf=tourconf,
            as_json=True,
        )
    finally:
        await server.stop()

    payload = json.loads(output)
    assert len(payload) == 1
    assert payload[0]["node_id"] == "node-3"
    assert payload[0]["ring_position"]["partition_shift"] == 10


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_node_inspect_keeps_identity_when_members_filtered_out(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    server_ssl = _build_server_ssl_from_ca(tmp_path, ca_cert_path, ca_key_path)
    serializer = MsgPackSerializer()

    async def inspect_handler(request: Envelope) -> AsyncIterator[Envelope]:
        payload = _node_info_payload()
        payload["members"] = []
        yield replay_envelope(
            kind="node.info",
            request=request,
            payload=serializer.encode(payload),
        )

    server = await start_envelope_server({"node.inspect": inspect_handler}, server_ssl)

    try:
        host, port = server.endpoint
        tourconf = await _build_tourconf(
            tmp_path,
            client_address="127.0.0.1:9999",
            ca_cert_path=ca_cert_path,
            ca_key_path=ca_key_path,
        )
        output = await inspect_node(
            address=f"{host}:{port}",
            tourconf=tourconf,
            phases=("JOINING",),
        )
    finally:
        await server.stop()

    assert "Summary:      phase=READY" in output
    assert "Phase:        READY" in output
    assert "Epoch:        3" in output
    assert "Members:  0 total" in output


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_node_inspect_forwards_filters_to_request_payload(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    server_ssl = _build_server_ssl_from_ca(tmp_path, ca_cert_path, ca_key_path)
    serializer = MsgPackSerializer()
    captured: dict[str, Any] = {}

    async def inspect_handler(request: Envelope) -> AsyncIterator[Envelope]:
        captured["request"] = serializer.decode(request.payload)
        yield replay_envelope(
            kind="node.info",
            request=request,
            payload=serializer.encode(_node_info_payload()),
        )

    server = await start_envelope_server({"node.inspect": inspect_handler}, server_ssl)

    try:
        host, port = server.endpoint
        tourconf = await _build_tourconf(
            tmp_path,
            client_address="127.0.0.1:9999",
            ca_cert_path=ca_cert_path,
            ca_key_path=ca_key_path,
        )
        output = await inspect_node(
            address=f"{host}:{port}",
            phases=("ready", "joining"),
            probe_statuses=("suspect",),
            node_id_prefix="node-",
            only_anomalies=True,
            limit=17,
            cursor="node-2",
            tourconf=tourconf,
        )
    finally:
        await server.stop()

    assert "Inspecting" in output
    assert captured["request"] == {
        "phases": ["ready", "joining"],
        "probe_statuses": ["suspect"],
        "node_id_prefix": "node-",
        "only_anomalies": True,
        "limit": 17,
        "cursor": "node-2",
    }


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_node_usecase_forwards_filter_options(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_inspect_node(**kwargs: Any) -> str:
        captured.update(kwargs)
        return "ok"

    monkeypatch.setattr("tourctl.bootstrap.usecases.node.inspect_node", fake_inspect_node)

    result = await node_inspect(
        address="127.0.0.1:7701",
        json=True,
        phases=["ready", "joining"],
        probe_statuses=["suspect"],
        node_id_prefix="node-",
        only_anomalies=True,
        limit=77,
        cursor="node-2",
        opts=GlobalOptions(tourconf="tourconf.toml", context="prod"),
    )

    assert result == "ok"
    assert captured == {
        "address": "127.0.0.1:7701",
        "as_json": True,
        "phases": ("ready", "joining"),
        "probe_statuses": ("suspect",),
        "node_id_prefix": "node-",
        "only_anomalies": True,
        "limit": 77,
        "cursor": "node-2",
        "tourconf": "tourconf.toml",
        "context_name": "prod",
    }


@pytest.mark.tourctl
def test_tourctl_app_node_inspect_reports_missing_config_path(tmp_path: Path) -> None:
    missing_tourconf = tmp_path / "missing-tourconf.toml"

    with pytest.raises(SystemExit, match="Unable to load the Tourillon client config"):
        app(
            [
                "node",
                "inspect",
                "127.0.0.1:7701",
                "--tourconf",
                str(missing_tourconf),
            ],
            exit_on_error=False,
            print_error=False,
            help_on_error=False,
            result_action="return_value",
        )


@pytest.mark.tourctl
def test_tourctl_app_node_inspect_requires_address_argument() -> None:
    with pytest.raises(MissingArgumentError):
        app(
            [
                "node",
                "inspect",
            ],
            exit_on_error=False,
            print_error=False,
            help_on_error=False,
            result_action="return_value",
        )


