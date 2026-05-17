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
"""Unit tests for tourctl KV command objects (KvPutCommand, KvGetCommand, KvDeleteCommand)."""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from tourctl.core.commands.kv import KvDeleteCommand, KvGetCommand, KvPutCommand
from tourillon.core.ports.transport import ResponseTimeoutError
from tourillon.core.structure.envelope import Envelope
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter

pytestmark = pytest.mark.kv

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class _CapturingConsole:
    """Captures print() calls."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def print(self, msg: str = "", **kwargs: Any) -> None:
        self.lines.append(str(msg))

    @property
    def text(self) -> str:
        return "\n".join(self.lines)


class _MockTcpClient:
    """Returns a pre-canned Envelope from request()."""

    def __init__(self, response: Envelope) -> None:
        self._response = response
        self.requests: list[Envelope] = []

    async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
        self.requests.append(env)
        return self._response


class _TimeoutTcpClient:
    """Always raises ResponseTimeoutError."""

    async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
        raise ResponseTimeoutError("timed out")


def _put_ok_response(
    wall: int = 1_000_000,
    counter: int = 0,
    node_id: str = "node-1",
    replicas: int = 2,
    corr: uuid.UUID | None = None,
) -> Envelope:
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode(
        {
            "hlc": {"wall": wall, "counter": counter, "node_id": node_id},
            "replicas": replicas,
        }
    )
    return Envelope.create(
        payload,
        kind="kv.put.ok",
        correlation_id=corr or uuid.uuid4(),
    )


def _get_ok_response(
    versions: list[dict[str, Any]],
    corr: uuid.UUID | None = None,
) -> Envelope:
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode({"versions": versions})
    return Envelope.create(
        payload,
        kind="kv.get.ok",
        correlation_id=corr or uuid.uuid4(),
    )


def _error_response(reason: str, corr: uuid.UUID | None = None) -> Envelope:
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode({"reason": reason})
    return Envelope.create(
        payload,
        kind="kv.error",
        correlation_id=corr or uuid.uuid4(),
    )


def _delete_ok_response(
    wall: int = 2_000_000,
    counter: int = 0,
    node_id: str = "node-1",
    replicas: int = 1,
    corr: uuid.UUID | None = None,
) -> Envelope:
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode(
        {
            "hlc": {"wall": wall, "counter": counter, "node_id": node_id},
            "replicas": replicas,
        }
    )
    return Envelope.create(
        payload,
        kind="kv.delete.ok",
        correlation_id=corr or uuid.uuid4(),
    )


# ---------------------------------------------------------------------------
# KvPutCommand tests
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_kv_put_command_success_exits_0_and_prints_written() -> None:
    """KvPutCommand with kv.put.ok response → exit 0, 'written' in output."""
    serializer = MsgpackSerializerAdapter()
    client = _MockTcpClient(_put_ok_response())
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvPutCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"user:001", b"default", b"Alice")
    assert code == 0
    assert "written" in out.text


@pytest.mark.kv
async def test_kv_put_command_json_output_is_valid_json() -> None:
    """KvPutCommand --json emits valid JSON with hlc and replicas keys."""
    serializer = MsgpackSerializerAdapter()
    client = _MockTcpClient(_put_ok_response(replicas=2))
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvPutCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"k", b"default", b"v", json_output=True)
    assert code == 0
    data = json.loads(out.lines[0])
    assert "hlc" in data
    assert "replicas" in data
    assert data["replicas"] == 2


@pytest.mark.kv
async def test_kv_put_command_kv_error_exits_1() -> None:
    """KvPutCommand with kv.error response → exit 1, error in stderr."""
    serializer = MsgpackSerializerAdapter()
    client = _MockTcpClient(_error_response("quorum_write_unavailable"))
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvPutCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"k", b"default", b"v")
    assert code == 1
    assert "quorum_write_unavailable" in err.text


@pytest.mark.kv
async def test_kv_put_command_timeout_exits_1() -> None:
    """KvPutCommand with timeout → exit 1, timeout message in stderr."""
    serializer = MsgpackSerializerAdapter()
    client = _TimeoutTcpClient()
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvPutCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"k", b"default", b"v")
    assert code == 1
    assert "timeout" in err.text.lower()


# ---------------------------------------------------------------------------
# KvGetCommand tests
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_kv_get_command_confirmed_version_exits_0() -> None:
    """KvGetCommand with confirmed version → exit 0, 'confirmed' in output."""
    import msgpack

    serializer = MsgpackSerializerAdapter()
    val_bytes = msgpack.packb("Alice", use_bin_type=True)
    versions = [
        {
            "confirmed": True,
            "value": val_bytes,
            "hlc": {"wall": 1_000_000, "counter": 0, "node_id": "n1"},
            "count": 2,
            "quorum_write": 2,
        }
    ]
    client = _MockTcpClient(_get_ok_response(versions))
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvGetCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"user:001", b"default")
    assert code == 0
    assert "confirmed" in out.text.lower()


@pytest.mark.kv
async def test_kv_get_command_not_found_exits_1() -> None:
    """KvGetCommand with empty versions → exit 1, 'not found' in stderr."""
    serializer = MsgpackSerializerAdapter()
    client = _MockTcpClient(_get_ok_response([]))
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvGetCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"missing", b"default")
    assert code == 1
    assert "not found" in err.text.lower()


@pytest.mark.kv
async def test_kv_get_command_uncertain_prints_warning() -> None:
    """KvGetCommand with unconfirmed versions → exit 0, 'uncertain' in output."""
    import msgpack

    serializer = MsgpackSerializerAdapter()
    versions = [
        {
            "confirmed": False,
            "value": msgpack.packb("Alice", use_bin_type=True),
            "hlc": {"wall": 1_000_000, "counter": 0, "node_id": "n1"},
            "count": 1,
            "quorum_write": 2,
        },
        {
            "confirmed": False,
            "value": msgpack.packb("Bob", use_bin_type=True),
            "hlc": {"wall": 999_000, "counter": 0, "node_id": "n2"},
            "count": 1,
            "quorum_write": 2,
        },
    ]
    client = _MockTcpClient(_get_ok_response(versions))
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvGetCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"k", b"default")
    assert code == 0
    assert "uncertain" in out.text.lower()


@pytest.mark.kv
async def test_kv_get_command_tombstone_confirmed_prints_deleted() -> None:
    """KvGetCommand with confirmed Tombstone → exit 0, '(deleted)' in output."""
    serializer = MsgpackSerializerAdapter()
    versions = [
        {
            "confirmed": True,
            "value": None,
            "hlc": {"wall": 2_000_000, "counter": 0, "node_id": "n1"},
            "count": 2,
            "quorum_write": 2,
        }
    ]
    client = _MockTcpClient(_get_ok_response(versions))
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvGetCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"user:001", b"default")
    assert code == 0
    assert "deleted" in out.text.lower()


@pytest.mark.kv
async def test_kv_get_command_kv_error_exits_1() -> None:
    """KvGetCommand with kv.error response → exit 1, error in stderr."""
    serializer = MsgpackSerializerAdapter()
    client = _MockTcpClient(_error_response("quorum_read_unavailable"))
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvGetCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"k", b"default")
    assert code == 1
    assert "quorum_read_unavailable" in err.text


@pytest.mark.kv
async def test_kv_get_command_timeout_exits_1() -> None:
    """KvGetCommand with timeout → exit 1."""
    serializer = MsgpackSerializerAdapter()
    client = _TimeoutTcpClient()
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvGetCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"k", b"default")
    assert code == 1


@pytest.mark.kv
async def test_kv_get_command_json_output() -> None:
    """KvGetCommand --json emits valid JSON with 'versions' key."""
    import msgpack

    serializer = MsgpackSerializerAdapter()
    val_bytes = msgpack.packb("Alice", use_bin_type=True)
    versions = [
        {
            "confirmed": True,
            "value": val_bytes,
            "hlc": {"wall": 1_000_000, "counter": 0, "node_id": "n1"},
            "count": 1,
            "quorum_write": 1,
        }
    ]
    client = _MockTcpClient(_get_ok_response(versions))
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvGetCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"k", b"default", json_output=True)
    assert code == 0
    data = json.loads(out.lines[0])
    assert "versions" in data


# ---------------------------------------------------------------------------
# KvDeleteCommand tests
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_kv_delete_command_success_exits_0_and_prints_deleted() -> None:
    """KvDeleteCommand with kv.delete.ok → exit 0, 'deleted' in output."""
    serializer = MsgpackSerializerAdapter()
    client = _MockTcpClient(_delete_ok_response())
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvDeleteCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"user:001", b"default")
    assert code == 0
    assert "deleted" in out.text.lower()


@pytest.mark.kv
async def test_kv_delete_command_json_output_is_valid_json() -> None:
    """KvDeleteCommand --json emits valid JSON."""
    serializer = MsgpackSerializerAdapter()
    client = _MockTcpClient(_delete_ok_response(replicas=1))
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvDeleteCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"k", b"default", json_output=True)
    assert code == 0
    data = json.loads(out.lines[0])
    assert "hlc" in data
    assert "replicas" in data


@pytest.mark.kv
async def test_kv_delete_command_kv_error_exits_1() -> None:
    """KvDeleteCommand with kv.error → exit 1, error in stderr."""
    serializer = MsgpackSerializerAdapter()
    client = _MockTcpClient(_error_response("quorum_write_unavailable"))
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvDeleteCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"k", b"default")
    assert code == 1
    assert "quorum_write_unavailable" in err.text


@pytest.mark.kv
async def test_kv_delete_command_timeout_exits_1() -> None:
    """KvDeleteCommand with timeout → exit 1."""
    serializer = MsgpackSerializerAdapter()
    client = _TimeoutTcpClient()
    out = _CapturingConsole()
    err = _CapturingConsole()
    cmd = KvDeleteCommand(
        client=client, serializer=serializer, console=out, err_console=err
    )

    code = await cmd.run(b"k", b"default")
    assert code == 1


# ---------------------------------------------------------------------------
# Multiple simultaneous correlation IDs
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_kv_put_command_concurrent_requests_each_get_own_response() -> None:
    """5 concurrent KvPutCommand.run() calls each produce exit 0."""
    serializer = MsgpackSerializerAdapter()

    async def _run_one(_: int) -> int:
        client = _MockTcpClient(_put_ok_response())
        out = _CapturingConsole()
        err = _CapturingConsole()
        cmd = KvPutCommand(
            client=client, serializer=serializer, console=out, err_console=err
        )
        return await cmd.run(b"k", b"default", b"v")

    codes = await asyncio.gather(*(_run_one(i) for i in range(5)))
    assert all(c == 0 for c in codes)


# ---------------------------------------------------------------------------
# _encode_arg utility — from tourctl/infra/cli/kv.py
# ---------------------------------------------------------------------------


@pytest.mark.kv
def test_encode_arg_infers_int_and_packs_as_msgpack() -> None:
    """_encode_arg with no type flag infers int and packs with msgpack."""
    import msgpack

    from tourctl.infra.cli.kv import _encode_arg

    result = _encode_arg("42", None)
    assert msgpack.unpackb(result, raw=False) == 42


@pytest.mark.kv
def test_encode_arg_explicit_str_packs_as_msgpack_string() -> None:
    """_encode_arg with -t str packs the value as a msgpack string."""
    import msgpack

    from tourctl.infra.cli.kv import _encode_arg

    result = _encode_arg("42", "str")
    assert msgpack.unpackb(result, raw=False) == "42"


@pytest.mark.kv
def test_encode_arg_bytes_type_returns_raw_bytes() -> None:
    """_encode_arg with -t bytes returns raw decoded bytes without msgpack wrapping."""
    import base64

    from tourctl.infra.cli.kv import _encode_arg

    b64 = base64.b64encode(b"hello").decode()
    result = _encode_arg(b64, "bytes")
    assert result == b"hello"


@pytest.mark.kv
def test_encode_arg_encoding_error_raises_typer_exit() -> None:
    """_encode_arg with invalid value for -t int raises typer.Exit(1)."""
    import typer

    from tourctl.infra.cli.kv import _encode_arg

    with pytest.raises(typer.Exit):
        _encode_arg("not-a-number", "int")


# ---------------------------------------------------------------------------
# _fmt_hlc utility — from tourctl/core/commands/kv.py
# ---------------------------------------------------------------------------


@pytest.mark.kv
def test_fmt_hlc_uses_wall_key() -> None:
    """_fmt_hlc reads legacy 'wall' key from HLC dict (backward compat)."""
    from tourctl.core.commands.kv import _fmt_hlc

    hlc = {"wall": 1_700_000_000_000, "counter": 5, "node_id": "n1"}
    result = _fmt_hlc(hlc)
    assert "#0005" in result
    assert "n1" in result


@pytest.mark.kv
def test_fmt_hlc_falls_back_to_wall_ms_key() -> None:
    """_fmt_hlc also accepts 'wall_ms' key for backwards compatibility."""
    from tourctl.core.commands.kv import _fmt_hlc

    hlc = {"wall_ms": 1_700_000_000_000, "counter": 0, "node_id": "n2"}
    result = _fmt_hlc(hlc)
    assert "n2" in result


@pytest.mark.kv
def test_decode_value_msgpack_bytes_returns_python_object() -> None:
    """_decode_value decodes msgpack bytes to the Python value."""
    import msgpack

    from tourctl.core.commands.kv import _decode_value

    packed = msgpack.packb({"key": "val"}, use_bin_type=True)
    result = _decode_value(packed)
    assert result == {"key": "val"}


@pytest.mark.kv
def test_json_safe_converts_bytes_to_dict() -> None:
    """_json_safe wraps bytes as {"_type": "bytes", "_value": base64}."""
    import base64

    from tourctl.core.commands.kv import _json_safe

    result = _json_safe(b"hello")
    assert result["_type"] == "bytes"
    assert base64.b64decode(result["_value"]) == b"hello"


# ---------------------------------------------------------------------------
# tourctl/infra/cli/kv.py — Typer command layer
# ---------------------------------------------------------------------------

_CONTEXTS_TOML_WITH_KV = """\
current-context = "test"

[[contexts]]
name = "test"

[contexts.cluster]
name = "test-cluster"
ca_data = "FAKE_CA"

[contexts.endpoints]
kv = "127.0.0.1:19999"

[contexts.credentials]
cert_data = "FAKE_CERT"
key_data = "FAKE_KEY"
"""

_CONTEXTS_TOML_NO_KV = """\
current-context = "test"

[[contexts]]
name = "test"

[contexts.cluster]
name = "test-cluster"
ca_data = ""

[contexts.endpoints]
peer = "127.0.0.1:20000"

[contexts.credentials]
cert_data = ""
key_data = ""
"""

_runner_kv = __import__("typer.testing", fromlist=["CliRunner"]).CliRunner(
    env={"NO_COLOR": "1"}
)


@pytest.mark.kv
def test_kv_put_cli_missing_context_file_exits_1(tmp_path: Path) -> None:
    """kv put with missing contexts file exits 1 (no active context)."""
    from tourctl.bootstrap.main import app as tourctl_app

    empty_ctx = tmp_path / "empty.toml"
    result = _runner_kv.invoke(
        tourctl_app,
        ["kv", "put", "mykey", "myval", "--contexts", str(empty_ctx)],
    )
    assert result.exit_code == 1


@pytest.mark.kv
def test_kv_put_cli_no_kv_endpoint_exits_1(tmp_path: Path) -> None:
    """kv put with context that has no kv endpoint exits 1."""
    from tourctl.bootstrap.main import app as tourctl_app

    ctx_file = tmp_path / "contexts.toml"
    ctx_file.write_text(_CONTEXTS_TOML_NO_KV)
    result = _runner_kv.invoke(
        tourctl_app,
        ["kv", "put", "mykey", "myval", "--contexts", str(ctx_file)],
    )
    assert result.exit_code == 1


@pytest.mark.kv
def test_kv_get_cli_no_kv_endpoint_exits_1(tmp_path: Path) -> None:
    """kv get with context that has no kv endpoint exits 1."""
    from tourctl.bootstrap.main import app as tourctl_app

    ctx_file = tmp_path / "contexts.toml"
    ctx_file.write_text(_CONTEXTS_TOML_NO_KV)
    result = _runner_kv.invoke(
        tourctl_app,
        ["kv", "get", "mykey", "--contexts", str(ctx_file)],
    )
    assert result.exit_code == 1


@pytest.mark.kv
def test_kv_delete_cli_no_kv_endpoint_exits_1(tmp_path: Path) -> None:
    """kv delete with context that has no kv endpoint exits 1."""
    from tourctl.bootstrap.main import app as tourctl_app

    ctx_file = tmp_path / "contexts.toml"
    ctx_file.write_text(_CONTEXTS_TOML_NO_KV)
    result = _runner_kv.invoke(
        tourctl_app,
        ["kv", "delete", "mykey", "--contexts", str(ctx_file)],
    )
    assert result.exit_code == 1


@pytest.mark.kv
def test_kv_put_cli_unreachable_exits_1(tmp_path: Path) -> None:
    """kv put with unreachable endpoint exits 1."""
    from unittest.mock import AsyncMock, patch

    from tourctl.bootstrap.main import app as tourctl_app

    ctx_file = tmp_path / "contexts.toml"
    ctx_file.write_text(_CONTEXTS_TOML_WITH_KV)

    with (
        patch("tourctl.infra.cli.kv.build_client_ssl_context", return_value=None),
        patch(
            "tourctl.infra.cli.kv.TcpClient.connect",
            new_callable=AsyncMock,
            side_effect=OSError("refused"),
        ),
    ):
        result = _runner_kv.invoke(
            tourctl_app,
            ["kv", "put", "key1", "val1", "--contexts", str(ctx_file)],
        )
    assert result.exit_code == 1


@pytest.mark.kv
def test_kv_get_cli_unreachable_exits_1(tmp_path: Path) -> None:
    """kv get with unreachable endpoint exits 1."""
    from unittest.mock import AsyncMock, patch

    from tourctl.bootstrap.main import app as tourctl_app

    ctx_file = tmp_path / "contexts.toml"
    ctx_file.write_text(_CONTEXTS_TOML_WITH_KV)

    with (
        patch("tourctl.infra.cli.kv.build_client_ssl_context", return_value=None),
        patch(
            "tourctl.infra.cli.kv.TcpClient.connect",
            new_callable=AsyncMock,
            side_effect=OSError("refused"),
        ),
    ):
        result = _runner_kv.invoke(
            tourctl_app,
            ["kv", "get", "key1", "--contexts", str(ctx_file)],
        )
    assert result.exit_code == 1


@pytest.mark.kv
def test_kv_delete_cli_unreachable_exits_1(tmp_path: Path) -> None:
    """kv delete with unreachable endpoint exits 1."""
    from unittest.mock import AsyncMock, patch

    from tourctl.bootstrap.main import app as tourctl_app

    ctx_file = tmp_path / "contexts.toml"
    ctx_file.write_text(_CONTEXTS_TOML_WITH_KV)

    with (
        patch("tourctl.infra.cli.kv.build_client_ssl_context", return_value=None),
        patch(
            "tourctl.infra.cli.kv.TcpClient.connect",
            new_callable=AsyncMock,
            side_effect=OSError("refused"),
        ),
    ):
        result = _runner_kv.invoke(
            tourctl_app,
            ["kv", "delete", "key1", "--contexts", str(ctx_file)],
        )
    assert result.exit_code == 1


@pytest.mark.kv
def test_kv_put_cli_encoding_error_exits_1(tmp_path: Path) -> None:
    """kv put with encoding error (invalid @file path) exits 1."""
    from tourctl.bootstrap.main import app as tourctl_app

    ctx_file = tmp_path / "contexts.toml"
    ctx_file.write_text(_CONTEXTS_TOML_WITH_KV)

    # @nonexistent_path.txt triggers EncodingError
    result = _runner_kv.invoke(
        tourctl_app,
        [
            "kv",
            "put",
            "mykey",
            "@/nonexistent/path/to/value.txt",
            "--contexts",
            str(ctx_file),
        ],
    )
    assert result.exit_code == 1
