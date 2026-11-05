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

import asyncio
import json
import socket
import ssl
from pathlib import Path

import pytest

from tests.helpers import generate_ca
from tourclient.exception import RequestRejectedError, UnknownServiceError
from tourctl.bootstrap.main import main
from tourctl.core.context import resolve_namespace
from tourctl.core.error import (
    EXIT_FAILED,
    EXIT_INVALID,
    EXIT_TIMEOUT,
    EXIT_UNREACHABLE,
    KV_ERROR_PROFILE,
    CliError,
    cli_error,
)
from tourillon.core.config import ClientConfigGenerator
from tourlib.adapter import CryptographyMtlsAdapter, TomlPersistence
from tourlib.exceptions import ProtocolError


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def _tourconf(tmp_path: Path, client_address: str) -> Path:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    result = await ClientConfigGenerator(
        CryptographyMtlsAdapter(), TomlPersistence()
    ).generate(
        output_file=tmp_path / "tourconf.toml",
        common_name="tourctl-main",
        ca_certificate_path=ca_cert_path,
        ca_private_key_path=ca_key_path,
        client_address=client_address,
        validity_days=30,
        key_size=2048,
        overwrite=False,
    )
    return result.config_path


def _exit_code(tokens: list[str]) -> int:
    with pytest.raises(SystemExit) as exc_info:
        main(tokens)
    return int(exc_info.value.code)


@pytest.mark.tourctl
def test_missing_config_exits_with_invalid_code_and_text_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing = str(tmp_path / "absent.toml")

    assert _exit_code(["get", "k", "-n", "ns", "--tourconf", missing]) == EXIT_INVALID

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("Error: ")


@pytest.mark.tourctl
def test_json_flag_renders_error_document_on_stderr(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing = str(tmp_path / "absent.toml")

    code = _exit_code(["get", "k", "-n", "ns", "--tourconf", missing, "--json"])

    assert code == EXIT_INVALID
    document = json.loads(capsys.readouterr().err)
    assert document["error"]["code"] == "invalid_config"
    assert document["error"]["message"]
    assert document["error"]["details"] == {}


@pytest.mark.tourctl
def test_node_command_without_address_is_a_usage_error() -> None:
    assert _exit_code(["node", "status"]) == EXIT_INVALID


@pytest.mark.tourctl
def test_unknown_command_is_a_usage_error() -> None:
    assert _exit_code(["no-such-command"]) == EXIT_INVALID


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_unreachable_node_exits_with_unreachable_code(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    tourconf = await _tourconf(tmp_path, f"127.0.0.1:{_free_port()}")

    code = await _run_main_in_thread(["get", "k", "--tourconf", str(tourconf)])

    assert code == EXIT_UNREACHABLE
    assert "Error: " in capsys.readouterr().err


async def _run_main_in_thread(tokens: list[str]) -> int:
    return await asyncio.to_thread(_exit_code, tokens)


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_namespace_defaults_to_selected_context(tmp_path: Path) -> None:
    tourconf = await _tourconf(tmp_path, "127.0.0.1:1")
    text = tourconf.read_text(encoding="utf-8")
    tourconf.write_text(
        text.replace('namespace = "default"', 'namespace = "accounts"'),
        encoding="utf-8",
    )

    resolved = await resolve_namespace(None, tourconf=tourconf, context_name=None)

    assert resolved == "accounts"


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_explicit_namespace_does_not_read_config(tmp_path: Path) -> None:
    missing = tmp_path / "absent.toml"

    resolved = await resolve_namespace("ns", tourconf=missing, context_name=None)

    assert resolved == "ns"


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_unknown_context_fails_namespace_resolution(tmp_path: Path) -> None:
    tourconf = await _tourconf(tmp_path, "127.0.0.1:1")

    with pytest.raises(KeyError):
        await resolve_namespace(None, tourconf=tourconf, context_name="nope")


@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("exc", "code", "error"),
    [
        (FileNotFoundError("tourconf.toml"), EXIT_INVALID, "invalid_config"),
        (KeyError("ctx"), EXIT_INVALID, "invalid_config"),
        (UnknownServiceError("Unknown service: kv"), EXIT_INVALID, "unknown_service"),
        (ssl.SSLError("handshake"), EXIT_UNREACHABLE, "tls_failed"),
        (ConnectionRefusedError("refused"), EXIT_UNREACHABLE, "unreachable"),
        (ProtocolError("bad frame"), EXIT_FAILED, "invalid_response"),
        (RuntimeError("boom"), EXIT_FAILED, "request_failed"),
    ],
)
def test_cli_error_classifies_exceptions(exc: Exception, code: int, error: str) -> None:
    result = cli_error(exc, None, KV_ERROR_PROFILE)

    assert result.code == code
    assert result.error == error


@pytest.mark.tourctl
def test_cli_error_keeps_rejection_code_and_details() -> None:
    rejected = RequestRejectedError(
        "kv.put",
        {"code": "quorum_not_met", "message": "write quorum not met", "token": "t"},
    )

    result = cli_error(rejected, None, KV_ERROR_PROFILE)

    assert result.code == EXIT_FAILED
    assert result.error == "quorum_not_met"
    assert result.details["token"] == "t"
    assert cli_error(result, None) is result


@pytest.mark.tourctl
def test_cli_error_rendering() -> None:
    error = CliError("first line\nhint", EXIT_TIMEOUT, error="wait_timeout")
    plain = CliError("timed out", EXIT_TIMEOUT, prefix=False)

    assert str(error) == "first line\nhint"
    assert error.render_text() == "Error: first line\nhint"
    assert plain.render_text() == "timed out"
    assert json.loads(error.render_json())["error"] == {
        "code": "wait_timeout",
        "message": "first line",
        "details": {},
    }
