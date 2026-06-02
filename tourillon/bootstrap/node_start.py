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
"""Node startup orchestration for tourillon node start."""

from __future__ import annotations

import asyncio
import logging
import ssl
from dataclasses import dataclass
from pathlib import Path

from tourillon.bootstrap.config import ConfigError, load_config
from tourillon.core.lifecycle.bootstrap import (
    Bootstraper,
    BootstrapError,
    derive_segment_shift,
)
from tourillon.core.lifecycle.checks import (
    NodeIdMismatchError,
    check_node_id_consistency,
    check_tokens_coherence,
)
from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.ports.state import StateError
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.config import TourillonConfig
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.server import TcpServer
from tourillon.infra.store.state import FileStateAdapter
from tourillon.infra.tls.context import TlsValidationError, build_server_ssl_context

logger = logging.getLogger("tourillon.bootstrap.node")


class NodeStartError(Exception):
    """Fatal startup error mapped to a process exit code by the CLI layer."""

    def __init__(self, message: str, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


@dataclass(frozen=True)
class _StartupContext:
    cfg: TourillonConfig
    phase: MemberPhase
    bootstraper: Bootstraper


class NodeRuntimeLifecycle:
    """Owns peer/KV listener lifecycle for a running node process."""

    def __init__(
        self,
        cfg: TourillonConfig,
        ssl_peer: ssl.SSLContext,
        ssl_kv: ssl.SSLContext,
    ) -> None:
        self._cfg = cfg
        self._peer_server = TcpServer(Dispatcher(), ssl_context=ssl_peer, name="Peer")
        self._kv_server = TcpServer(Dispatcher(), ssl_context=ssl_kv, name="KV")
        self._peer_started = False
        self._kv_started = False

    async def run(self, *, stop_event: asyncio.Event | None = None) -> None:
        await self.start()
        shutdown_event = stop_event or asyncio.Event()
        try:
            await shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop_all()
            logger.info("Shutdown complete.")

    async def start(self) -> None:
        await self.start_peer()
        try:
            await self.start_kv()
        except BootstrapError:
            await self.stop_peer()
            raise
        _log_ready_listeners(self._cfg)

    async def start_peer(self) -> None:
        if self._peer_started:
            return
        peer_host, peer_port = _split_host_port(self._cfg.peer_server.bind)
        try:
            await self._peer_server.start(peer_host, peer_port)
        except OSError as exc:
            raise BootstrapError(
                f"cannot bind peer listener on {self._cfg.peer_server.bind}: {exc}",
                exit_code=1,
            ) from exc
        self._peer_started = True

    async def start_kv(self) -> None:
        if self._kv_started:
            return
        kv_host, kv_port = _split_host_port(self._cfg.kv_server.bind)
        try:
            await self._kv_server.start(kv_host, kv_port)
        except OSError as exc:
            raise BootstrapError(
                f"cannot bind KV listener on {self._cfg.kv_server.bind}: {exc}",
                exit_code=1,
            ) from exc
        self._kv_started = True

    async def restart_kv(self) -> None:
        """Future hook for phase-driven KV listener restarts."""
        await self.stop_kv()
        await self.start_kv()

    async def stop_all(self) -> None:
        await self.stop_kv()
        await self.stop_peer()

    async def stop_kv(self) -> None:
        if not self._kv_started:
            return
        await self._kv_server.stop()
        self._kv_started = False

    async def stop_peer(self) -> None:
        if not self._peer_started:
            return
        await self._peer_server.stop()
        self._peer_started = False


class NodeStartOrchestrator:
    """Bootstraps node state then delegates serving to runtime lifecycle."""

    async def run(
        self,
        config_path: Path,
        *,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        context = await self._build_startup_context(config_path)
        logger.info(
            "Node %s starting (phase: %s).", context.cfg.node_id, context.phase.value
        )
        state = await context.bootstraper.start_node()
        _log_bootstrap_outcome(context.cfg, context.phase, state, context.bootstraper)
        logger.info("Node %s is READY.", context.cfg.node_id)
        ssl_peer, ssl_kv = _build_ssl_contexts(context.cfg)
        runtime = NodeRuntimeLifecycle(context.cfg, ssl_peer, ssl_kv)
        await runtime.run(stop_event=stop_event)

    async def _build_startup_context(self, config_path: Path) -> _StartupContext:
        cfg = load_config(config_path)
        segment_shift = _validate_partition_shifts(
            partition_shift=cfg.partition_shift,
            token_count=cfg.node_size.token_count,
        )

        state_port = FileStateAdapter(Path(cfg.data_dir) / "state.toml")
        persisted = await state_port.load()
        if persisted is not None:
            check_node_id_consistency(cfg.node_id, persisted.node_id)
            if not check_tokens_coherence(
                persisted.phase,
                persisted.tokens,
                cfg.node_size,
            ):
                raise BootstrapError(
                    "token count mismatch: "
                    f"state has {len(persisted.tokens)} token(s) "
                    f"but node size {cfg.node_size.value} requires "
                    f"{cfg.node_size.token_count}."
                )

        bootstraper = Bootstraper(
            cfg=cfg,
            state_port=state_port,
            topology_mgr=TopologyManager(),
            hash_space=HashSpace(bits=128),
            segment_shift=segment_shift,
        )
        phase = persisted.phase if persisted is not None else MemberPhase.IDLE
        return _StartupContext(cfg=cfg, phase=phase, bootstraper=bootstraper)


def _token_preview(token: int) -> str:
    token_hex = f"{token:x}"
    return f"0x{token_hex[:8]}…"


def _split_host_port(bind: str) -> tuple[str, int]:
    host, _, port = bind.rpartition(":")
    return host, int(port)


def _validate_partition_shifts(partition_shift: int, token_count: int) -> int:
    hash_space = HashSpace(bits=128)
    segment_shift = derive_segment_shift(token_count)
    if partition_shift >= hash_space.bits:
        raise ConfigError("partition_shift must be strictly less than hash-space bits")
    if segment_shift >= partition_shift:
        raise ConfigError(
            "derived segment_shift must be strictly less than partition_shift"
        )
    return segment_shift


def _log_bootstrap_ranges(bootstraper: Bootstraper, partition_shift: int) -> None:
    logger.info("Partition ranges owned (%d total partitions):", 1 << partition_shift)
    for item in bootstraper.ranges:
        logger.info(
            "token %s → pids [%d–%d]  (%d partitions)",
            _token_preview(item.owner.token),
            item.start_pid,
            item.end_pid,
            item.count,
        )


def _log_ready_listeners(cfg: TourillonConfig) -> None:
    peer_host, peer_port = _split_host_port(cfg.peer_server.bind)
    kv_host, kv_port = _split_host_port(cfg.kv_server.bind)
    logger.info("Peer listener: %s:%d", peer_host, peer_port)
    logger.info("KV  listener: %s:%d", kv_host, kv_port)


def _log_bootstrap_outcome(
    cfg: TourillonConfig,
    phase: MemberPhase,
    state,
    bootstraper: Bootstraper,
) -> None:
    if phase is MemberPhase.IDLE:
        logger.info(
            "Generated %d token(s) for node size %s.",
            cfg.node_size.token_count,
            cfg.node_size.value,
        )
        _log_bootstrap_ranges(bootstraper, cfg.partition_shift)
        logger.info(
            "State persisted (phase: %s, epoch: %d, generation: %d).",
            state.phase.value,
            state.epoch,
            state.generation,
        )
        return

    logger.info(
        "Topology rebuilt from state.toml: %d vnode(s), epoch %d.",
        len(state.tokens),
        state.epoch,
    )


def _build_ssl_contexts(
    cfg: TourillonConfig,
) -> tuple[ssl.SSLContext, ssl.SSLContext]:
    try:
        ssl_peer = build_server_ssl_context(
            cfg.tls.cert_data,
            cfg.tls.key_data,
            cfg.tls.ca_data,
        )
        ssl_kv = build_server_ssl_context(
            cfg.tls.cert_data,
            cfg.tls.key_data,
            cfg.tls.ca_data,
        )
    except TlsValidationError as exc:
        raise BootstrapError(f"TLS error: {exc}") from exc
    return ssl_peer, ssl_kv


async def serve_node(
    cfg: TourillonConfig,
    ssl_peer: ssl.SSLContext,
    ssl_kv: ssl.SSLContext,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Serve peer and KV listeners until *stop_event* is set or loop cancellation."""
    runtime = NodeRuntimeLifecycle(cfg, ssl_peer, ssl_kv)
    await runtime.run(stop_event=stop_event)


def _log_error(exc: Exception) -> None:
    logger.error("Error: %s", exc)
    if isinstance(exc, NodeIdMismatchError):
        logger.error(
            "This data_dir belongs to a different node. Check your config.toml"
        )
        logger.error("or point data_dir at the correct directory.")


async def run_node_start(
    config_path: Path,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Run complete startup orchestration for `tourillon node start`."""

    try:
        await NodeStartOrchestrator().run(config_path, stop_event=stop_event)
    except (
        ConfigError,
        BootstrapError,
        NodeIdMismatchError,
        StateError,
        ValueError,
    ) as exc:
        _log_error(exc)
        exit_code = exc.exit_code if isinstance(exc, BootstrapError) else 1
        raise NodeStartError(str(exc), exit_code=exit_code) from exc
