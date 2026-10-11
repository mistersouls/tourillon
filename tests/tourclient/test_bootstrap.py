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

import ssl
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from tests.helpers import generate_ca, start_envelope_server
from tourclient import client
from tourclient.core.helpers import resolve_tourconf_path
from tourillon.core.config import ClientConfigGenerator
from tourlib.adapter import CryptographyMtlsAdapter, MsgPackSerializer, TomlPersistence
from tourlib.envelope import Envelope, replay_envelope


async def _build_tourconf(
    tmp_path: Path,
    *,
    client_address: str = "127.0.0.1:9000",
    ca_cert_path: Path | None = None,
    ca_key_path: Path | None = None,
) -> Path:
    if ca_cert_path is None or ca_key_path is None:
        ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = ClientConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    result = await generator.generate(
        output_file=tmp_path / "tourconf.toml",
        common_name="tourctl-bootstrap",
        ca_certificate_path=ca_cert_path,
        ca_private_key_path=ca_key_path,
        client_address=client_address,
        validity_days=30,
        key_size=2048,
        overwrite=False,
    )
    return result.config_path


def _build_server_ssl_from_ca(tmp_path: Path, ca_cert_path: Path, ca_key_path: Path) -> ssl.SSLContext:
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


@pytest.mark.asyncio
async def test_init_client_uses_explicit_tourconf_path(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    server_ssl = _build_server_ssl_from_ca(tmp_path, ca_cert_path, ca_key_path)
    serializer = MsgPackSerializer()
    captured: dict[str, Envelope] = {}

    async def ping_handler(request: Envelope) -> AsyncIterator[Envelope]:
        captured["request"] = request
        yield replay_envelope(
            kind="probe.pong",
            request=request,
            payload=serializer.encode(
                {
                    "node_id": "server",
                    "timestamp_ms": 1,
                    "recv_ms": 2,
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
        async with client(tourconf=tourconf) as tc:
            result = await tc.probe.ping()
    finally:
        await server.stop()

    request_data = serializer.decode(captured["request"].payload)
    assert request_data["node_id"] == "client"
    assert result.node_id == "server"
    assert result.timestamp_ms == 1
    assert result.recv_ms == 2


@pytest.mark.asyncio
async def test_init_client_uses_tourconf_env_var(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tourconf = await _build_tourconf(tmp_path)
    monkeypatch.setenv("TOURCONF", str(tourconf))

    resolved = resolve_tourconf_path()
    assert resolved == tourconf


def test_resolve_tourconf_path_raises_clear_error_when_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TOURCONF", raising=False)
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path / "home"))

    with pytest.raises(FileNotFoundError, match="tourconf not found"):
        resolve_tourconf_path()


@pytest.mark.asyncio
async def test_init_client_with_service_on_default_client_endpoint(tmp_path: Path) -> None:
    """Client initialization with service='probe' on default 'client' endpoint."""
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    server_ssl = _build_server_ssl_from_ca(tmp_path, ca_cert_path, ca_key_path)
    serializer = MsgPackSerializer()
    captured: dict[str, Envelope] = {}

    async def ping_handler(request: Envelope) -> AsyncIterator[Envelope]:
        captured["request"] = request
        yield replay_envelope(
            kind="probe.pong",
            request=request,
            payload=serializer.encode(
                {
                    "node_id": "server",
                    "timestamp_ms": 1,
                    "recv_ms": 2,
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
        # Explicitly request probe service on client endpoint
        async with client("probe", tourconf=tourconf, endpoint="client") as probe_svc:
            result = await probe_svc.ping()
    finally:
        await server.stop()

    assert result.node_id == "server"


@pytest.mark.asyncio
async def test_init_client_rejects_invalid_endpoint(tmp_path: Path) -> None:
    """Client initialization should reject invalid endpoint values."""
    tourconf = await _build_tourconf(tmp_path)

    from tourlib import ValidationError

    with pytest.raises(ValidationError, match="Endpoint must be either"):
        async with client(tourconf=tourconf, endpoint="invalid"):  # type: ignore
            pass


@pytest.mark.asyncio
async def test_tour_client_dynamic_service_access(tmp_path: Path) -> None:
    """TourClient should allow dynamic service access via __getattr__."""
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    server_ssl = _build_server_ssl_from_ca(tmp_path, ca_cert_path, ca_key_path)
    serializer = MsgPackSerializer()
    captured: dict[str, Envelope] = {}

    async def ping_handler(request: Envelope) -> AsyncIterator[Envelope]:
        captured["request"] = request
        yield replay_envelope(
            kind="probe.pong",
            request=request,
            payload=serializer.encode(
                {
                    "node_id": "server",
                    "timestamp_ms": 1,
                    "recv_ms": 2,
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
        async with client(tourconf=tourconf) as tc:
            # Access probe service dynamically
            result = await tc.probe.ping()
    finally:
        await server.stop()

    assert result.node_id == "server"


@pytest.mark.asyncio
async def test_tour_client_service_caching(tmp_path: Path) -> None:
    """TourClient should cache service instances."""
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    server_ssl = _build_server_ssl_from_ca(tmp_path, ca_cert_path, ca_key_path)
    serializer = MsgPackSerializer()

    async def ping_handler(request: Envelope) -> AsyncIterator[Envelope]:
        yield replay_envelope(
            kind="probe.pong",
            request=request,
            payload=serializer.encode(
                {
                    "node_id": "server",
                    "timestamp_ms": 1,
                    "recv_ms": 2,
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
        async with client(tourconf=tourconf) as tc:
            # Access probe twice
            probe1 = tc.probe
            probe2 = tc.probe
            # Should be the same instance
            assert probe1 is probe2
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tour_client_unknown_service_raises_error(tmp_path: Path) -> None:
    """TourClient should raise AttributeError for unknown services."""
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    server_ssl = _build_server_ssl_from_ca(tmp_path, ca_cert_path, ca_key_path)
    serializer = MsgPackSerializer()

    async def ping_handler(request: Envelope) -> AsyncIterator[Envelope]:
        yield replay_envelope(
            kind="probe.pong",
            request=request,
            payload=serializer.encode(
                {
                    "node_id": "server",
                    "timestamp_ms": 1,
                    "recv_ms": 2,
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
        async with client(tourconf=tourconf) as tc:
            # Try to access unknown service
            with pytest.raises(AttributeError, match="has no service 'nonexistent'"):
                tc.nonexistent
    finally:
        await server.stop()
