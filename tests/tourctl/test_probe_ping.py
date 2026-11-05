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
import re
import socket
import ssl
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from tests.helpers import generate_ca, start_envelope_server
from tourclient.exception import UnknownServiceError
from tourctl.bootstrap.main import app, main
from tourctl.core.error import PROBE_ERROR_PROFILE, cli_error
from tourctl.core.probe import _format_rtt, ping_probe
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


async def _ping_real_server(
    tmp_path: Path, *, endpoint: str = "client", as_json: bool
) -> tuple[str, dict, str]:
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
        result = await ping_probe(endpoint=endpoint, tourconf=tourconf, as_json=as_json)
    finally:
        await server.stop()

    return result, serializer.decode(captured["request"].payload), f"{host}:{port}"


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_probe_ping_returns_json_with_real_runtime(tmp_path: Path) -> None:
    result, request_data, address = await _ping_real_server(tmp_path, as_json=True)
    output = json.loads(result)

    assert output.pop("rtt_ms") >= 0
    assert output == {
        "address": address,
        "endpoint": "client",
        "node_id": "probe-server",
        "recv_ms": request_data["timestamp_ms"] + 7,
        "timestamp_ms": request_data["timestamp_ms"],
    }


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_probe_ping_prints_one_line_by_default(tmp_path: Path) -> None:
    result, _, address = await _ping_real_server(tmp_path, as_json=False)

    assert re.fullmatch(
        rf"Pong from probe-server at {re.escape(address)} in (<1|\d+)ms\.", result
    )


@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("rtt_ms", "text"), [(0.2, "<1ms"), (1.0, "1ms"), (2.6, "3ms"), (120.4, "120ms")]
)
def test_probe_ping_rounds_the_round_trip(rtt_ms: float, text: str) -> None:
    assert _format_rtt(rtt_ms) == text


@pytest.mark.tourctl
def test_main_probe_ping_reports_errors_as_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing = tmp_path / "missing-tourconf.toml"

    with pytest.raises(SystemExit) as exc_info:
        main(["probe", "ping", "peer", "--tourconf", str(missing), "--json"])

    captured = capsys.readouterr()
    assert exc_info.value.code == 2
    assert captured.out == ""
    assert json.loads(captured.err)["error"]["code"] == "invalid_config"


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
    message = cli_error(exc, context_name, PROBE_ERROR_PROFILE).message
    assert expected in message
