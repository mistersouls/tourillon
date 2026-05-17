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
"""tourctl kv subcommands — kv put, kv get, kv delete."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated

import msgpack
import typer
from rich.console import Console

from tourctl.core.commands.config import ContextsError, load_contexts
from tourctl.core.commands.kv import KvDeleteCommand, KvGetCommand, KvPutCommand
from tourillon.core.kv.encoding import EncodingError, resolve_arg
from tourillon.core.ports.transport import RESPONSE_TIMEOUT
from tourillon.core.transport.client import TcpClient
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter
from tourillon.infra.tls.context import build_client_ssl_context

kv_app = typer.Typer(no_args_is_help=True)

_console = Console()
_err_console = Console(stderr=True)

_DEFAULT_CONTEXTS_PATH = Path.home() / ".config" / "tourillon" / "contexts.toml"


def _encode_arg(raw: str, type_flag: str | None) -> bytes:
    """Encode a CLI argument to msgpack bytes."""
    try:
        value = resolve_arg(raw, type_flag)
    except EncodingError as exc:
        _err_console.print(f"✗  encoding error: {exc}")
        raise typer.Exit(1) from exc
    if isinstance(value, bytes):
        return value
    return msgpack.packb(value, use_bin_type=True)  # type: ignore[no-any-return]


def _get_kv_endpoint(contexts_path: Path) -> str:
    """Load and validate the active context, return its kv endpoint."""
    try:
        contexts_file = load_contexts(contexts_path)
    except ContextsError as exc:
        _err_console.print(f"✗ {exc}")
        raise typer.Exit(1) from exc

    ctx = (
        contexts_file.get(contexts_file.current_context)
        if contexts_file.current_context
        else None
    )
    if ctx is None:
        _err_console.print("✗ No active context. Use `tourctl config use-context`.")
        raise typer.Exit(1)

    endpoint = ctx.endpoints.kv
    if not endpoint:
        _err_console.print("✗ Active context has no kv endpoint.")
        raise typer.Exit(1)
    return endpoint, ctx


@kv_app.command("put")
def kv_put(
    key: Annotated[str, typer.Argument(help="Key to write")],
    value: Annotated[str, typer.Argument(help="Value to store")],
    keyspace: Annotated[
        str, typer.Option("-k", "--keyspace", help="Keyspace (default: 'default')")
    ] = "default",
    quorum_write: Annotated[
        int | None,
        typer.Option("-w", "--quorum-write", help="Write quorum override"),
    ] = None,
    type_flag: Annotated[
        str | None,
        typer.Option(
            "-t", "--type", help="Value type: str|int|float|bool|json|bytes|null"
        ),
    ] = None,
    key_type: Annotated[
        str | None,
        typer.Option("--kt", "--key-type", help="Key type override"),
    ] = None,
    ks_type: Annotated[
        str | None,
        typer.Option("--ks", "--keyspace-type", help="Keyspace type override"),
    ] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
    contexts_path: Annotated[Path, typer.Option("--contexts")] = _DEFAULT_CONTEXTS_PATH,
    timeout: Annotated[float, typer.Option("--timeout")] = RESPONSE_TIMEOUT,
) -> None:
    """Write VALUE under KEY in KEYSPACE."""
    endpoint, ctx = _get_kv_endpoint(contexts_path)
    key_bytes = _encode_arg(key, key_type)
    value_bytes = _encode_arg(value, type_flag)
    ks_bytes = _encode_arg(keyspace, ks_type)

    qw = quorum_write if quorum_write is not None else ctx.kv.quorum_write

    exit_code = asyncio.run(
        _run_put(
            ctx.credentials.cert_data,
            ctx.credentials.key_data,
            ctx.cluster.ca_data,
            endpoint,
            key_bytes,
            ks_bytes,
            value_bytes,
            quorum_write=qw,
            json_output=json_output,
            timeout=timeout,
        )
    )
    raise typer.Exit(exit_code)


@kv_app.command("get")
def kv_get(
    key: Annotated[str, typer.Argument(help="Key to read")],
    keyspace: Annotated[
        str, typer.Option("-k", "--keyspace", help="Keyspace (default: 'default')")
    ] = "default",
    quorum_read: Annotated[
        int | None,
        typer.Option("-r", "--quorum-read", help="Read quorum override"),
    ] = None,
    key_type: Annotated[
        str | None,
        typer.Option("--kt", "--key-type", help="Key type override"),
    ] = None,
    ks_type: Annotated[
        str | None,
        typer.Option("--ks", "--keyspace-type", help="Keyspace type override"),
    ] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
    contexts_path: Annotated[Path, typer.Option("--contexts")] = _DEFAULT_CONTEXTS_PATH,
    timeout: Annotated[float, typer.Option("--timeout")] = RESPONSE_TIMEOUT,
) -> None:
    """Read KEY from KEYSPACE."""
    endpoint, ctx = _get_kv_endpoint(contexts_path)
    key_bytes = _encode_arg(key, key_type)
    ks_bytes = _encode_arg(keyspace, ks_type)

    qr = quorum_read if quorum_read is not None else ctx.kv.quorum_read

    exit_code = asyncio.run(
        _run_get(
            ctx.credentials.cert_data,
            ctx.credentials.key_data,
            ctx.cluster.ca_data,
            endpoint,
            key_bytes,
            ks_bytes,
            quorum_read=qr,
            json_output=json_output,
            timeout=timeout,
        )
    )
    raise typer.Exit(exit_code)


@kv_app.command("delete")
def kv_delete(
    key: Annotated[str, typer.Argument(help="Key to delete")],
    keyspace: Annotated[
        str, typer.Option("-k", "--keyspace", help="Keyspace (default: 'default')")
    ] = "default",
    quorum_write: Annotated[
        int | None,
        typer.Option("-w", "--quorum-write", help="Write quorum override"),
    ] = None,
    key_type: Annotated[
        str | None,
        typer.Option("--kt", "--key-type", help="Key type override"),
    ] = None,
    ks_type: Annotated[
        str | None,
        typer.Option("--ks", "--keyspace-type", help="Keyspace type override"),
    ] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
    contexts_path: Annotated[Path, typer.Option("--contexts")] = _DEFAULT_CONTEXTS_PATH,
    timeout: Annotated[float, typer.Option("--timeout")] = RESPONSE_TIMEOUT,
) -> None:
    """Delete KEY from KEYSPACE (writes a tombstone)."""
    endpoint, ctx = _get_kv_endpoint(contexts_path)
    key_bytes = _encode_arg(key, key_type)
    ks_bytes = _encode_arg(keyspace, ks_type)

    qw = quorum_write if quorum_write is not None else ctx.kv.quorum_write

    exit_code = asyncio.run(
        _run_delete(
            ctx.credentials.cert_data,
            ctx.credentials.key_data,
            ctx.cluster.ca_data,
            endpoint,
            key_bytes,
            ks_bytes,
            quorum_write=qw,
            json_output=json_output,
            timeout=timeout,
        )
    )
    raise typer.Exit(exit_code)


async def _run_put(
    cert_data: str,
    key_data: str,
    ca_data: str,
    endpoint: str,
    key: bytes,
    keyspace: bytes,
    value: bytes,
    *,
    quorum_write: int,
    json_output: bool,
    timeout: float,
) -> int:
    tls_ctx = build_client_ssl_context(cert_data, key_data, ca_data)
    client = TcpClient()
    try:
        await client.connect(endpoint, tls_ctx)
    except OSError as exc:
        _err_console.print(f"✗ {endpoint} is unreachable ({exc}).")
        return 1

    serializer = MsgpackSerializerAdapter()
    cmd = KvPutCommand(
        client=client, serializer=serializer, console=_console, err_console=_err_console
    )
    try:
        return await cmd.run(
            key,
            keyspace,
            value,
            quorum_write=quorum_write,
            json_output=json_output,
            timeout=timeout,
        )
    finally:
        await client.close()


async def _run_get(
    cert_data: str,
    key_data: str,
    ca_data: str,
    endpoint: str,
    key: bytes,
    keyspace: bytes,
    *,
    quorum_read: int,
    json_output: bool,
    timeout: float,
) -> int:
    tls_ctx = build_client_ssl_context(cert_data, key_data, ca_data)
    client = TcpClient()
    try:
        await client.connect(endpoint, tls_ctx)
    except OSError as exc:
        _err_console.print(f"✗ {endpoint} is unreachable ({exc}).")
        return 1

    serializer = MsgpackSerializerAdapter()
    cmd = KvGetCommand(
        client=client, serializer=serializer, console=_console, err_console=_err_console
    )
    try:
        return await cmd.run(
            key,
            keyspace,
            quorum_read=quorum_read,
            json_output=json_output,
            timeout=timeout,
        )
    finally:
        await client.close()


async def _run_delete(
    cert_data: str,
    key_data: str,
    ca_data: str,
    endpoint: str,
    key: bytes,
    keyspace: bytes,
    *,
    quorum_write: int,
    json_output: bool,
    timeout: float,
) -> int:
    tls_ctx = build_client_ssl_context(cert_data, key_data, ca_data)
    client = TcpClient()
    try:
        await client.connect(endpoint, tls_ctx)
    except OSError as exc:
        _err_console.print(f"✗ {endpoint} is unreachable ({exc}).")
        return 1

    serializer = MsgpackSerializerAdapter()
    cmd = KvDeleteCommand(
        client=client, serializer=serializer, console=_console, err_console=_err_console
    )
    try:
        return await cmd.run(
            key,
            keyspace,
            quorum_write=quorum_write,
            json_output=json_output,
            timeout=timeout,
        )
    finally:
        await client.close()
