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

import base64
import json
import ssl
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from tests.helpers import generate_ca, start_envelope_server
from tourclient.exception import RequestRejectedError
from tourctl.bootstrap.main import app
from tourctl.core.kv import kv_delete, kv_get, kv_put, kv_repair
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
        common_name="tourctl-kv",
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


def _version_document(namespace: Any, key: Any, value: Any) -> dict[str, Any]:
    return {
        "address": {
            "namespace": namespace,
            "key": key,
        },
        "data": value,
        "meta": {
            "ts": {
                "wall_ms": 1,
                "counter": 0,
                "node_id": "n1",
            },
            "quorum_write": 1,
        },
    }


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_tourctl_kv_core_supports_typed_inputs_and_json_output(
    tmp_path: Path,
) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    server_ssl = _build_server_ssl_from_ca(tmp_path, ca_cert_path, ca_key_path)
    serializer = MsgPackSerializer()
    store: dict[tuple[Any, Any], Any] = {}

    async def put_handler(request: Envelope) -> AsyncIterator[Envelope]:
        document = serializer.decode(request.payload)
        key = (document["namespace"], document["key"])
        store[key] = document["value"]
        yield replay_envelope(
            kind="kv.put.accept",
            request=request,
            payload=serializer.encode(_version_document(*key, store[key])),
        )

    async def get_handler(request: Envelope) -> AsyncIterator[Envelope]:
        document = serializer.decode(request.payload)
        key = (document["namespace"], document["key"])
        value = store.get(key)
        payload = None if value is None else _version_document(*key, value)
        yield replay_envelope(
            kind="kv.get.accept",
            request=request,
            payload=serializer.encode(payload),
        )

    async def delete_handler(request: Envelope) -> AsyncIterator[Envelope]:
        document = serializer.decode(request.payload)
        key = (document["namespace"], document["key"])
        store.pop(key, None)
        yield replay_envelope(
            kind="kv.delete.accept",
            request=request,
            payload=serializer.encode(_version_document(*key, b"")),
        )

    server = await start_envelope_server(
        {
            "kv.put": put_handler,
            "kv.get": get_handler,
            "kv.delete": delete_handler,
        },
        server_ssl,
    )

    try:
        host, port = server.endpoint
        tourconf = await _build_tourconf(
            tmp_path,
            client_address=f"{host}:{port}",
            ca_cert_path=ca_cert_path,
            ca_key_path=ca_key_path,
        )
        payload_file = tmp_path / "blob.bin"
        payload_file.write_bytes(b"\x00\x01\xff")

        put_output = await kv_put(
            key="7",
            value=f"@{payload_file}",
            namespace="11",
            key_type="int",
            namespace_type="int",
            as_json=True,
            tourconf=tourconf,
        )
        get_output = await kv_get(
            key="7",
            namespace="11",
            key_type="int",
            namespace_type="int",
            as_json=True,
            tourconf=tourconf,
        )
        get_text_output = await kv_get(
            key="7",
            namespace="11",
            key_type="int",
            namespace_type="int",
            as_json=False,
            tourconf=tourconf,
        )
        delete_output = await kv_delete(
            key="7",
            namespace="11",
            key_type="int",
            namespace_type="int",
            as_json=True,
            tourconf=tourconf,
        )
    finally:
        await server.stop()

    put_json = json.loads(put_output)
    get_json = json.loads(get_output)
    delete_json = json.loads(delete_output)

    expected_b64 = base64.b64encode(b"\x00\x01\xff").decode("ascii")

    assert put_json["address"]["key"] == 7
    assert put_json["address"]["namespace"] == 11
    assert put_json["data"] == {"type": "bytes", "base64": expected_b64}
    assert get_json["data"] == {"type": "bytes", "base64": expected_b64}
    assert delete_json["data"] == {"type": "bytes", "base64": ""}
    assert "Key:" in get_text_output
    assert "Namespace:" in get_text_output
    assert "Value:" in get_text_output
    assert f"{expected_b64} (3 bytes total)" in get_text_output
    assert "Metadata" in get_text_output
    assert "HLC:" in get_text_output
    assert "counter=0" in get_text_output
    assert "node_id=n1" in get_text_output
    assert "Quorum W:    1" in get_text_output


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_kv_put_reports_invalid_base64_value() -> None:
    with pytest.raises(SystemExit, match="Invalid KV input"):
        await kv_put(
            key="alpha",
            value="%%%",
            namespace="default",
            key_type="str",
            value_type="bytes",
            namespace_type="str",
        )


@pytest.mark.tourctl
def test_tourctl_kv_cli_forwards_typed_options(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    captured: dict[str, Any] = {}

    async def fake_kv_get(**kwargs: Any) -> str:
        captured["get"] = kwargs
        return "ok-get"

    async def fake_kv_delete(**kwargs: Any) -> str:
        captured["delete"] = kwargs
        return "ok-delete"

    monkeypatch.setattr("tourctl.bootstrap.usecases.kv.kv_get", fake_kv_get)
    monkeypatch.setattr("tourctl.bootstrap.usecases.kv.kv_delete", fake_kv_delete)

    get_result = app(
        [
            "get",
            "my-key",
            "-n",
            "space-1",
            "--key-type",
            "auto",
            "--namespace-type",
            "str",
            "--json",
        ],
        exit_on_error=False,
        print_error=False,
        help_on_error=False,
        result_action="return_value",
    )
    delete_result = app(
        [
            "delete",
            "my-key",
            "-n",
            "space-1",
            "--key-type",
            "auto",
            "--namespace-type",
            "str",
        ],
        exit_on_error=False,
        print_error=False,
        help_on_error=False,
        result_action="return_value",
    )

    assert get_result is None
    assert delete_result is None
    assert capsys.readouterr().out == "ok-get\nok-delete\n"
    assert captured["get"]["key"] == "my-key"
    assert captured["get"]["namespace"] == "space-1"
    assert captured["get"]["key_type"] == "auto"
    assert captured["get"]["namespace_type"] == "str"
    assert captured["get"]["as_json"] is True
    assert captured["delete"]["key"] == "my-key"
    assert captured["delete"]["namespace"] == "space-1"
    assert captured["delete"]["key_type"] == "auto"
    assert captured["delete"]["namespace_type"] == "str"


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_kv_get_pretty_renders_bytes_fields_as_base64(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_get(**kwargs: Any) -> dict[str, Any] | None:
        _ = kwargs
        return {
            "address": {
                "key": b"\x01\x02",
                "namespace": b"ab",
            },
            "data": b"\x00\xff",
            "meta": {},
        }

    monkeypatch.setattr("tourctl.core.kv._run_get", fake_run_get)

    output = await kv_get(
        key="k",
        namespace="n",
        as_json=False,
    )

    key_b64 = base64.b64encode(b"\x01\x02").decode("ascii")
    ns_b64 = base64.b64encode(b"ab").decode("ascii")
    value_b64 = base64.b64encode(b"\x00\xff").decode("ascii")

    lines = output.splitlines()
    assert len(lines) == 3
    assert lines[0].startswith("Key:")
    assert lines[1].startswith("Namespace:")
    assert lines[2].startswith("Value:")
    assert f"{key_b64} (2 bytes total)" in lines[0]
    assert f"{ns_b64} (2 bytes total)" in lines[1]
    assert f"{value_b64} (2 bytes total)" in lines[2]


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_kv_get_reports_rejected_request(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_run_get(**kwargs: Any) -> dict[str, Any] | None:
        _ = kwargs
        raise RequestRejectedError(
            "kv.get",
            {
                "operation": "read",
                "code": "quorum_not_met",
                "message": "read quorum not met: successes=1, required=2",
                "errors": [
                    {
                        "type": "TimeoutError",
                        "message": "no response from node-b within 30.0s",
                    }
                ],
            },
        )

    monkeypatch.setattr("tourctl.core.kv._run_get", fake_run_get)

    with pytest.raises(SystemExit) as exc_info:
        await kv_get(key="k", namespace="n")

    assert str(exc_info.value) == (
        "KV request failed: read quorum not met: successes=1, required=2 "
        "(no response from node-b within 30.0s)"
    )


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_kv_repair_renders_the_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    async def fake_run_repair(**kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return {"issue": "repaired", "replicas": 3, "token": "tok"}

    monkeypatch.setattr("tourctl.core.kv._run_repair", fake_run_repair)

    output = await kv_repair(token="  tok  ", quorum_write=2)

    assert seen["token"] == "tok"
    assert seen["quorum_write"] == 2
    lines = output.splitlines()
    assert lines[0].startswith("Issue:")
    assert "Repaired." in lines[0]
    assert "3" in lines[1]
    assert "tok" in lines[2]


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_kv_repair_renders_json(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_run_repair(**kwargs: Any) -> dict[str, Any]:
        _ = kwargs
        return {"issue": "already_durable", "replicas": 3, "token": "tok"}

    monkeypatch.setattr("tourctl.core.kv._run_repair", fake_run_repair)

    output = await kv_repair(token="tok", as_json=True)

    assert json.loads(output) == {
        "issue": "already_durable",
        "replicas": 3,
        "token": "tok",
    }


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_kv_repair_rejects_an_empty_token() -> None:
    with pytest.raises(SystemExit) as exc_info:
        await kv_repair(token="   ")

    assert "repair token must not be empty" in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_kv_put_suggests_resuming_an_under_replicated_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_put(**kwargs: Any) -> dict[str, Any] | None:
        _ = kwargs
        raise RequestRejectedError(
            "kv.put",
            {
                "operation": "write",
                "code": "quorum_not_met",
                "message": "write quorum not met: successes=1, required=2",
                "token": "tok-123",
                "errors": [],
            },
        )

    monkeypatch.setattr("tourctl.core.kv._run_put", fake_run_put)

    with pytest.raises(SystemExit) as exc_info:
        await kv_put(key="k", value="v", namespace="n", quorum_write=2)

    assert str(exc_info.value).endswith("tourctl repair tok-123")


@pytest.mark.tourctl
def test_tourctl_repair_cli_forwards_the_token_and_quorum(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    captured: dict[str, Any] = {}

    async def fake_kv_repair(**kwargs: Any) -> str:
        captured.update(kwargs)
        return "ok-repair"

    monkeypatch.setattr("tourctl.bootstrap.usecases.kv.kv_repair", fake_kv_repair)

    result = app(
        ["repair", "tok-123", "-w", "2", "--json"],
        exit_on_error=False,
        print_error=False,
        help_on_error=False,
        result_action="return_value",
    )

    assert result is None
    assert capsys.readouterr().out == "ok-repair\n"
    assert captured["token"] == "tok-123"
    assert captured["quorum_write"] == 2
    assert captured["as_json"] is True
