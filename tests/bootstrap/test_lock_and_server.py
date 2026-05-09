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
"""Tests for PidLock, load_config_file, and TcpServer.start/stop."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
import tomli_w

from tourillon.bootstrap.config import ConfigError, load_config_file
from tourillon.bootstrap.lock import PidLock, PidLockError
from tourillon.core.ports.transport import ConnectionClosedError, ResponseTimeoutError
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.server import TcpServer


@pytest.mark.bootstrap
async def test_pid_lock_acquire_and_release(tmp_path: Path) -> None:
    """PidLock can be acquired and released without error."""
    lock = PidLock(tmp_path)
    await lock.acquire()
    assert (tmp_path / "pid.lock").exists()
    await lock.release()


@pytest.mark.bootstrap
async def test_pid_lock_context_manager(tmp_path: Path) -> None:
    """PidLock works as an async context manager."""
    async with PidLock(tmp_path):
        assert (tmp_path / "pid.lock").exists()


@pytest.mark.bootstrap
async def test_pid_lock_double_acquire_raises(tmp_path: Path) -> None:
    """A second PidLock on the same data_dir raises PidLockError."""
    lock1 = PidLock(tmp_path)
    lock2 = PidLock(tmp_path)
    await lock1.acquire()
    try:
        with pytest.raises(PidLockError, match="already running"):
            await lock2.acquire()
    finally:
        await lock1.release()


@pytest.mark.bootstrap
async def test_pid_lock_release_idempotent(tmp_path: Path) -> None:
    """Calling release() twice does not raise."""
    lock = PidLock(tmp_path)
    await lock.acquire()
    await lock.release()
    await lock.release()  # second release must be silent


@pytest.mark.bootstrap
async def test_pid_lock_fails_if_data_dir_missing(tmp_path: Path) -> None:
    """PidLock raises PidLockError when the data_dir does not exist."""
    missing = tmp_path / "a" / "b" / "c"
    with pytest.raises(PidLockError):
        async with PidLock(missing):
            pass  # pragma: no cover


@pytest.mark.bootstrap
async def test_pid_lock_succeeds_when_data_dir_exists(tmp_path: Path) -> None:
    """PidLock acquires successfully when the data_dir already exists."""
    data_dir = tmp_path / "node-data"
    data_dir.mkdir()
    async with PidLock(data_dir):
        assert (data_dir / "pid.lock").exists()


@pytest.mark.bootstrap
async def test_load_config_file_reads_toml(
    tmp_path: Path,
    valid_config_dict: dict,
) -> None:
    """load_config_file reads a TOML file and returns TourillonConfig."""
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(tomli_w.dumps(valid_config_dict), encoding="utf-8")
    cfg = load_config_file(cfg_path)
    assert cfg.node_id == "node-1"


@pytest.mark.bootstrap
async def test_load_config_file_env_override(
    tmp_path: Path,
    valid_config_dict: dict,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TOURILLON_NODE_ID env var overrides [node].id in config file."""
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(tomli_w.dumps(valid_config_dict), encoding="utf-8")
    monkeypatch.setenv("TOURILLON_NODE_ID", "overridden-node")
    cfg = load_config_file(cfg_path)
    assert cfg.node_id == "overridden-node"


@pytest.mark.bootstrap
async def test_load_config_file_not_found_raises(tmp_path: Path) -> None:
    """load_config_file raises ConfigError when the file does not exist."""
    with pytest.raises(ConfigError, match="not found"):
        load_config_file(tmp_path / "missing.toml")


@pytest.mark.bootstrap
async def test_load_config_file_invalid_toml_raises(tmp_path: Path) -> None:
    """load_config_file raises ConfigError for malformed TOML."""
    bad = tmp_path / "bad.toml"
    bad.write_text("[[[ not valid toml", encoding="utf-8")
    with pytest.raises(ConfigError, match="parse error"):
        load_config_file(bad)


@pytest.mark.bootstrap
async def test_load_config_unsupported_schema_version(
    valid_config_dict: dict,
) -> None:
    """ConfigError raised for unsupported schema_version."""
    import copy

    raw = copy.deepcopy(valid_config_dict)
    raw["schema_version"] = 99
    with pytest.raises(ConfigError, match="schema_version=99"):
        from tourillon.bootstrap.config import load_config

        load_config(raw)


@pytest.mark.bootstrap
async def test_tcpserver_start_and_stop() -> None:
    """TcpServer.start() binds a port; TcpServer.stop() unbinds it."""
    dispatcher = Dispatcher()
    server = TcpServer(dispatcher, ssl_context=None)
    await server.start("127.0.0.1", 0)
    port = _server_port(server)
    # After start, a connection can be established.
    client_r, client_w = await asyncio.open_connection("127.0.0.1", port)
    client_w.close()
    await client_w.wait_closed()
    await server.stop()
    # After stop, new connections are refused (port captured before stop).
    with pytest.raises((ConnectionRefusedError, OSError)):
        await asyncio.open_connection("127.0.0.1", port)


def _server_port(server: TcpServer) -> int:
    assert server._server is not None
    return server._server.sockets[0].getsockname()[1]


@pytest.mark.bootstrap
async def test_tcpserver_stop_idempotent() -> None:
    """Calling TcpServer.stop() on a server that was never started is silent."""
    server = TcpServer(Dispatcher(), ssl_context=None)
    await server.stop()  # must not raise


@pytest.mark.bootstrap
async def test_tcpserver_unknown_kind_closes_connection() -> None:
    """Server closes the connection immediately when it receives an unknown kind."""
    from tourillon.core.structure.envelope import Envelope
    from tourillon.core.transport.client import TcpClient

    dispatcher = Dispatcher()  # no handlers registered
    server = TcpServer(dispatcher, ssl_context=None)
    await server.start("127.0.0.1", 0)
    port = _server_port(server)

    client = TcpClient()
    await client.connect(f"127.0.0.1:{port}", tls_ctx=None)  # type: ignore[arg-type]
    # Send an envelope whose kind is not registered; server should close.
    env = Envelope(kind="unknown.verb", payload=b"")
    fut = asyncio.get_running_loop().create_task(client.request(env, timeout=5.0))
    try:
        with pytest.raises((ConnectionClosedError, ResponseTimeoutError)):
            await asyncio.wait_for(fut, timeout=5.0)
    finally:
        fut.cancel()
        await asyncio.gather(fut, return_exceptions=True)
        await client.close()
        await server.stop()


@pytest.mark.bootstrap
async def test_tcpserver_handler_exception_logs_and_continues() -> None:
    """Server logs a handler exception and the connection stays up for subsequent requests."""
    from tourillon.core.structure.envelope import Envelope
    from tourillon.core.transport.client import TcpClient

    async def bad_handler(receive, send) -> None:  # noqa: ANN001
        await receive()
        raise RuntimeError("intentional handler error")

    async def good_handler(receive, send) -> None:  # noqa: ANN001
        env = await receive()
        from tourillon.core.structure.envelope import Envelope

        await send(Envelope(kind="ok", payload=b"", correlation_id=env.correlation_id))

    dispatcher = Dispatcher()
    dispatcher.register("fail", bad_handler)
    dispatcher.register("ok", good_handler)
    server = TcpServer(dispatcher, ssl_context=None)
    await server.start("127.0.0.1", 0)
    port = _server_port(server)

    client = TcpClient()
    await client.connect(f"127.0.0.1:{port}", tls_ctx=None)  # type: ignore[arg-type]
    # First request triggers the bad handler (exception is logged, not propagated).
    bad_env = Envelope(kind="fail", payload=b"")
    bad_fut = asyncio.get_running_loop().create_task(
        client.request(bad_env, timeout=3.0)
    )
    await asyncio.sleep(0.1)  # give handler time to run and fail
    bad_fut.cancel()
    await asyncio.gather(bad_fut, return_exceptions=True)
    # A subsequent good request should still succeed.
    good_env = Envelope(kind="ok", payload=b"")
    resp = await client.request(good_env, timeout=3.0)
    assert resp.kind == "ok"
    await client.close()
    await server.stop()


@pytest.mark.bootstrap
def test_dispatcher_duplicate_registration_raises() -> None:
    """Registering the same kind twice raises ValueError."""

    async def dummy(receive, send) -> None:  # noqa: ANN001
        pass

    d = Dispatcher()
    d.register("kv.put", dummy)
    with pytest.raises(ValueError, match="already registered"):
        d.register("kv.put", dummy)
