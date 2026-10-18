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
import socket
import ssl
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from tests.helpers import generate_ca, start_envelope_server
from tourclient.exception import UnknownServiceError
from tourctl.bootstrap.main import app
from tourctl.core.probe import _probe_error_message, ping_probe
from tourillon.core.config import ClientConfigGenerator
from tourlib.adapter import CryptographyMtlsAdapter, MsgPackSerializer, TomlPersistence
from tourlib.envelope import Envelope, replay_envelope
from tourlib.exceptions import ProtocolError, ResponseError, ValidationError


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
        common_name="tourctl-probe",
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


def _closed_local_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_probe_ping_returns_json_with_real_runtime(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    server_ssl = _build_server_ssl_from_ca(tmp_path, ca_cert_path, ca_key_path)
    serializer = MsgPackSerializer()
    captured: dict[str, Envelope] = {}

    async def ping_handler(request: Envelope) -> AsyncIterator[Envelope]:
        captured["request"] = request
        ping_data = serializer.decode(request.payload)
        yield replay_envelope(
            kind="probe.pong",
            request=request,
            payload=serializer.encode(
                {
                    "node_id": "probe-server",
                    "timestamp_ms": ping_data["timestamp_ms"],
                    "recv_ms": ping_data["timestamp_ms"] + 7,
                }
            ),
        )

    server = await start_envelope_server({"probe.ping": ping_handler}, server_ssl)

    try:
        host, port = server.endpoint
        tourconf = await _build_tourconf(
            tmp_path,
            client_address=f"{host}:{port}",
            ca_cert_path=ca_cert_path,
            ca_key_path=ca_key_path,
        )
        result = await ping_probe(endpoint="client", tourconf=tourconf)
    finally:
        await server.stop()

    request_data = serializer.decode(captured["request"].payload)
    output = json.loads(result)

    assert output == {
        "node_id": "probe-server",
        "recv_ms": request_data["timestamp_ms"] + 7,
        "timestamp_ms": request_data["timestamp_ms"],
    }


@pytest.mark.tourctl
def test_tourctl_app_probe_ping_reports_missing_config_path(tmp_path: Path) -> None:
    missing_tourconf = tmp_path / "missing-tourconf.toml"

    with pytest.raises(SystemExit, match="Unable to load the Tourillon client config"):
        app(
            [
                "probe",
                "ping",
                "client",
                "--tourconf",
                str(missing_tourconf),
            ],
            exit_on_error=False,
            print_error=False,
            help_on_error=False,
            result_action="return_value",
        )


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_probe_ping_reports_connection_refusal_clearly(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    closed_port = _closed_local_port()
    tourconf = await _build_tourconf(
        tmp_path,
        client_address=f"127.0.0.1:{closed_port}",
        ca_cert_path=ca_cert_path,
        ca_key_path=ca_key_path,
    )

    with pytest.raises(SystemExit, match="refused|Unable to reach"):
        await ping_probe(endpoint="client", tourconf=tourconf)


@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("exc", "context_name", "expected"),
    [
        (KeyError("clusters.default"), None, "Missing client configuration entry"),
        (KeyError("prod"), "prod", "Unknown client context 'prod'"),
        (ValidationError("bad table"), None, "Invalid Tourillon client configuration"),
        (ValueError("bad endpoint"), None, "Invalid Tourillon client configuration"),
        (
            ssl.SSLError("bad tls"),
            None,
            "TLS setup or handshake failed during probe ping",
        ),
        (
            OSError("network down"),
            None,
            "Unable to reach the Tourillon node for probe ping",
        ),
        (
            ResponseError("bad response"),
            None,
            "Tourillon node returned an invalid probe response",
        ),
        (
            ProtocolError("bad frame"),
            None,
            "Tourillon node returned an invalid probe response",
        ),
        (UnknownServiceError("Unknown service: probe"), None, "Unknown service: probe"),
        (RuntimeError("boom"), None, "Probe ping failed: boom"),
    ],
)
def test_probe_error_message_is_clear(
    exc: Exception,
    context_name: str | None,
    expected: str,
) -> None:
    message = _probe_error_message(exc, context_name)
    assert expected in message
