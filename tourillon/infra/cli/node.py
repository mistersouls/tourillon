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
"""tourillon node subcommands — daemon lifecycle."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from pathlib import Path
from typing import Annotated

import typer

from tourillon.bootstrap.config import ConfigError, load_config_file
from tourillon.bootstrap.lock import PidLock
from tourillon.bootstrap.log import setup_logging
from tourillon.core.lifecycle.bootstrap import BootstrapError, run_first_node_bootstrap
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.config import TourillonConfig
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.server import TcpServer
from tourillon.infra.store.state import FileStateAdapter
from tourillon.infra.tls.context import build_server_ssl_context

logger = logging.getLogger(__name__)

node_app = typer.Typer(no_args_is_help=True)


@node_app.command("start")
def node_start(
    config: Annotated[
        Path,
        typer.Option("--config", help="Path to config.toml"),
    ] = Path("config.toml"),
    log_level: Annotated[str, typer.Option("--log-level")] = "INFO",
) -> None:
    """Start the tourillon node daemon."""
    setup_logging(log_level)

    try:
        cfg = load_config_file(config)
    except ConfigError as exc:
        logger.error("Failed to load configuration from %s: %s.", config, exc)
        raise typer.Exit(1) from exc

    if cfg.seeds:
        logger.error(
            "Node '%s' has seeds configured; seeded join is not yet supported by this command.",
            cfg.node_id,
        )
        raise typer.Exit(1)

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_run(cfg))


async def _run(cfg: TourillonConfig) -> None:
    """Run the full node startup and serve loop."""
    data_dir = Path(cfg.data_dir)

    if not data_dir.is_dir():
        logger.error(
            "Data directory does not exist: %s — create it before starting the node.",
            data_dir.resolve(),
        )
        raise typer.Exit(1)

    logger.info("Using data directory: %s.", data_dir.resolve())

    async with PidLock(data_dir):
        logger.debug("PID lock acquired; data directory is %s.", data_dir)
        hash_space = HashSpace()
        topology_mgr = TopologyManager()
        state_port = FileStateAdapter(data_dir / "state.toml")

        try:
            state = await run_first_node_bootstrap(
                cfg, state_port, topology_mgr, hash_space
            )
        except BootstrapError as exc:
            logger.error("Bootstrap failed: %s.", exc)
            raise typer.Exit(exc.exit_code) from exc

        ssl_ctx = build_server_ssl_context(
            cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
        )
        logger.debug("mTLS context loaded from embedded certificate.")

        peer_host, peer_port = _parse_bind(cfg.peer_server.bind)
        kv_host, kv_port = _parse_bind(cfg.kv_server.bind)

        peer_server = TcpServer(Dispatcher(), ssl_ctx, name="Peer")
        kv_server = TcpServer(Dispatcher(), ssl_ctx, name="KV")

        await peer_server.start(peer_host, peer_port)
        await kv_server.start(kv_host, kv_port)

        logger.info(
            "Node '%s' is ready (epoch %d, generation %d).",
            cfg.node_id,
            state.epoch,
            state.generation,
        )

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        _install_signal_handlers(loop, stop)
        logger.debug("Waiting for shutdown signal.")
        await stop.wait()

        logger.info("Shutdown signal received; stopping node '%s'.", cfg.node_id)
        await peer_server.stop()
        await kv_server.stop()
        logger.info("Node '%s' stopped cleanly.", cfg.node_id)


def _parse_bind(address: str) -> tuple[str, int]:
    """Parse a 'host:port' bind string into (host, port)."""
    host, _, port_str = address.rpartition(":")
    return host or "0.0.0.0", int(port_str)


def _install_signal_handlers(
    loop: asyncio.AbstractEventLoop, stop: asyncio.Event
) -> None:
    """Register SIGINT/SIGTERM handlers; ignore errors on platforms that do not support them."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, OSError):
            loop.add_signal_handler(sig, stop.set)
