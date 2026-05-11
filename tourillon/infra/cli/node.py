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
import functools
import logging
import signal
import sys
from pathlib import Path
from typing import Annotated

import typer

from tourillon.bootstrap.config import ConfigError, load_config_file
from tourillon.bootstrap.lock import PidLock
from tourillon.bootstrap.log import setup_logging
from tourillon.core.gossip.bootstrapper import (
    BootstrapError,
    BootstrapPartitionShiftError,
    GossipBootstrapper,
)
from tourillon.core.gossip.config import GossipConfig
from tourillon.core.gossip.engine import GossipEngine
from tourillon.core.handlers.gossip import register_gossip_handlers
from tourillon.core.handlers.inspect import (
    NodeInspectHandler,
    NodeInspectPeerViewHandler,
)
from tourillon.core.handlers.node_join import NodeJoinHandler
from tourillon.core.lifecycle.bootstrap import run_first_node_bootstrap
from tourillon.core.lifecycle.checks import (
    NodeIdMismatchError,
    check_node_id_consistency,
    check_tokens_coherence,
)
from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.config import NodeSize, TourillonConfig
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.pool import PeerClientPool
from tourillon.core.transport.server import TcpServer
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter
from tourillon.infra.store.state import FileStateAdapter
from tourillon.infra.tls.context import (
    build_client_ssl_context,
    build_server_ssl_context,
)

logger = logging.getLogger(__name__)

node_app = typer.Typer(no_args_is_help=True)

_KV_PHASES = frozenset({MemberPhase.READY, MemberPhase.DRAINING})
_BOOTSTRAP_PHASES = frozenset({MemberPhase.JOINING, MemberPhase.DRAINING})


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
        sys.exit(1)

    logger.info("Using data directory: %s.", data_dir.resolve())

    async with PidLock(data_dir):
        state_port = FileStateAdapter(data_dir / "state.toml")
        persisted = await state_port.load()

        phase = persisted.phase if persisted is not None else MemberPhase.IDLE

        if persisted is not None:
            try:
                check_node_id_consistency(cfg.node_id, persisted.node_id)
            except NodeIdMismatchError:
                logger.error(
                    "node_id mismatch: config=%r state=%r. "
                    "This data_dir belongs to a different node. "
                    "Check your config.toml or point data_dir at the correct directory. "
                    "Exiting.",
                    cfg.node_id,
                    persisted.node_id,
                )
                sys.exit(1)

            node_size = NodeSize(cfg.node_size)
            if not check_tokens_coherence(phase, persisted.tokens, node_size):
                logger.error(
                    "tokens/size mismatch for node %r: NodeSize %r expects %d tokens "
                    "but state has %d. Transitioning to FAILED.",
                    cfg.node_id,
                    str(cfg.node_size),
                    node_size.token_count,
                    len(persisted.tokens),
                )
                failed_state = NodeState(
                    node_id=cfg.node_id,
                    phase=MemberPhase.FAILED,
                    generation=persisted.generation,
                    seq=persisted.seq + 1,
                    tokens=persisted.tokens,
                    epoch=persisted.epoch,
                )
                await state_port.save(failed_state)
                persisted = failed_state
                phase = MemberPhase.FAILED

        await _run_phase(cfg, state_port, persisted, phase)


async def _execute_engine_failed(
    cfg: TourillonConfig,
    state_port: FileStateAdapter,
    state_ref: list[NodeState],
    peer_address: str,
    engine: GossipEngine,
) -> None:
    """Persist FAILED state then announce it via the gossip engine.

    Called by the engine when a partition_shift mismatch is detected from
    peers. Follows the write-before-announce invariant: state is persisted
    before the gossip announce is emitted.
    """
    state = state_ref[0]
    new_seq = state.seq + 1
    failed_state = NodeState(
        node_id=state.node_id,
        phase=MemberPhase.FAILED,
        generation=state.generation,
        seq=new_seq,
        tokens=state.tokens,
        epoch=state.epoch,
    )
    try:
        await state_port.save(failed_state)
    except Exception as exc:
        logger.error("Failed to persist FAILED state: %s.", exc)
    state_ref[0] = failed_state
    failed_member = Member(
        node_id=cfg.node_id,
        peer_address=peer_address,
        generation=state.generation,
        seq=new_seq,
        phase=MemberPhase.FAILED,
        tokens=state.tokens,
        partition_shift=cfg.partition_shift,
    )
    await engine.announce(failed_member)


async def _execute_launch_bootstrap(
    cfg: TourillonConfig,
    topology_mgr: TopologyManager,
    gossip_config: GossipConfig,
    client_ssl_ctx: object,
    serializer: object,
    engine: GossipEngine,
    phase: MemberPhase,
    stop: asyncio.Event,
    state_ref: list[NodeState],
    start_engine_event: asyncio.Event,
    seeds: list[str],
) -> None:
    """Run gossip bootstrap triggered by a node.join handler callback.

    After successful bootstrap, self-registers the local JOINING member in
    TopologyManager (so the fingerprint includes this node), pre-populates the
    engine hot_queue via announce(), and signals start_engine_event so that
    _run_phase can start GossipEngine and begin emitting the gossip.push.

    On unrecoverable errors, sets the stop event to trigger a clean shutdown
    instead of calling sys.exit() — which would raise SystemExit inside a
    background task and produce an "exception was never retrieved" asyncio log.
    """
    bootstrapper = GossipBootstrapper(
        topology_manager=topology_mgr,
        config=gossip_config.bootstrap,
        partition_shift=cfg.partition_shift,
        ssl_ctx=client_ssl_ctx,
        serializer=serializer,
    )
    try:
        ok = await bootstrapper.run(seeds)
        engine.stats.bootstrap_ok_total += ok
        # Re-register our own member after bootstrap to guarantee the
        # fingerprint and member_count reflect the full cluster view.
        # state_ref[0] is updated by NodeJoinHandler.set_state before this
        # task runs, so it carries the correct JOINING generation/seq/tokens.
        peer_address = cfg.peer_server.advertise or cfg.peer_server.bind
        state = state_ref[0]
        own_member = Member(
            node_id=cfg.node_id,
            peer_address=peer_address,
            generation=state.generation,
            seq=state.seq,
            phase=state.phase,
            tokens=state.tokens,
            partition_shift=cfg.partition_shift,
        )
        await topology_mgr.apply_member(own_member)
        # Pre-populate hot_queue so _hot_loop sends gossip.push immediately
        # once the engine starts, propagating our JOINING record to peers.
        await engine.announce(own_member)
        start_engine_event.set()
        logger.info(
            "Node %r remains in phase %r; waiting for rebalance to complete.",
            cfg.node_id,
            str(state.phase),
        )
    except BootstrapPartitionShiftError as exc:
        logger.error(
            "Bootstrap aborted: partition_shift mismatch local=%d seed=%d. Stopping.",
            exc.local_shift,
            exc.seed_shift,
        )
        stop.set()
    except BootstrapError as exc:
        logger.error("Gossip bootstrap failed: %s. Stopping.", exc)
        stop.set()


def _build_peer_dispatcher(
    cfg: TourillonConfig,
    state_ref: list[NodeState],
    state_port: FileStateAdapter,
    topology_mgr: TopologyManager,
    probe_mgr: ProbeManager,
    partitioner: Partitioner,
    client_ssl_ctx: object,
    serializer: MsgpackSerializerAdapter,
    engine: GossipEngine,
    gossip_config: GossipConfig,
    launch_bootstrap: object,
    peer_address: str,
    kv_address: str,
) -> Dispatcher:
    """Construct and return a fully-wired Dispatcher for the peer server.

    Instantiates every peer-server handler (inspect, peer-view, join, gossip)
    and registers them against a fresh Dispatcher. Extracted from _run_phase so
    that the wiring can be tested independently — a TypeError from a mismatched
    constructor argument is caught immediately without requiring real sockets
    or TLS credentials.

    The returned Dispatcher is ready to hand to TcpServer.
    """
    inspect_handler = NodeInspectHandler(
        node_id=cfg.node_id,
        get_state=lambda: state_ref[0],
        topology_manager=topology_mgr,
        probe_manager=probe_mgr,
        partitioner=partitioner,
        peer_address=peer_address,
        kv_address=kv_address,
        size=str(cfg.node_size),
        serializer=serializer,
        tls_ctx=client_ssl_ctx,
        attempt_timeout=cfg.join.attempt_timeout,
        get_gossip_stats=lambda: engine.stats.to_dict(),
    )
    peer_view_handler = NodeInspectPeerViewHandler(
        node_id=cfg.node_id,
        topology_manager=topology_mgr,
        probe_manager=probe_mgr,
        serializer=serializer,
    )
    join_handler = NodeJoinHandler(
        node_id=cfg.node_id,
        get_state=lambda: state_ref[0],
        topology_manager=topology_mgr,
        state_port=state_port,
        config_seeds=cfg.seeds,
        partition_shift=cfg.partition_shift,
        peer_address=peer_address,
        serializer=serializer,
        launch_bootstrap=launch_bootstrap,  # type: ignore[arg-type]
        # Update the shared state reference after IDLE → JOINING so that
        # _execute_launch_bootstrap reads the correct JOINING state (not
        # stale IDLE) when building the member to announce to peers.
        set_state=lambda s: state_ref.__setitem__(0, s),
    )
    join_handler._token_count = cfg.node_size.token_count  # type: ignore[attr-defined]

    peer_dispatcher = Dispatcher()
    register_gossip_handlers(
        peer_dispatcher,
        cfg.node_id,
        topology_mgr,
        cfg.partition_shift,
        gossip_config.max_digest_entries,
        serializer,
    )
    peer_dispatcher.register("node.inspect", inspect_handler)
    peer_dispatcher.register("node.inspect.peer_view", peer_view_handler)
    peer_dispatcher.register("node.join", join_handler)
    return peer_dispatcher


async def _run_phase(
    cfg: TourillonConfig,
    state_port: FileStateAdapter,
    persisted: NodeState | None,
    phase: MemberPhase,
) -> None:
    """Wire all components and enter the serve loop for *phase*."""
    hash_space = HashSpace()
    topology_mgr = TopologyManager()
    probe_mgr = ProbeManager()
    partitioner = Partitioner(hash_space, cfg.partition_shift)
    serializer = MsgpackSerializerAdapter()

    ssl_ctx = build_server_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )
    client_ssl_ctx = build_client_ssl_context(
        cfg.tls.cert_data, cfg.tls.key_data, cfg.tls.ca_data
    )

    peer_address = cfg.peer_server.advertise or cfg.peer_server.bind
    kv_address = cfg.kv_server.advertise or cfg.kv_server.bind

    if persisted is None:
        effective: NodeState = NodeState(
            node_id=cfg.node_id,
            phase=MemberPhase.IDLE,
            generation=0,
            seq=0,
            tokens=(),
            epoch=0,
        )
    else:
        effective = persisted

    state_ref: list[NodeState] = [effective]
    gossip_config = GossipConfig()

    pool = PeerClientPool(ssl_ctx=client_ssl_ctx)
    engine = GossipEngine(
        node_id=cfg.node_id,
        topology_manager=topology_mgr,
        pool=pool,
        config=gossip_config,
        partition_shift=cfg.partition_shift,
        serializer=serializer,
    )
    # Bind on_failed after construction to capture the live engine reference.
    engine._on_failed = functools.partial(  # type: ignore[attr-defined]
        _execute_engine_failed, cfg, state_port, state_ref, peer_address, engine
    )

    # Create stop before launch_bootstrap so the bootstrap task can trigger a
    # clean shutdown via stop.set() instead of sys.exit() from a background task.
    stop = asyncio.Event()
    # Signalled by _execute_launch_bootstrap when IDLE→JOINING bootstrap
    # completes; causes _await_engine_start_or_stop to start GossipEngine.
    start_engine_event = asyncio.Event()

    launch_bootstrap = functools.partial(
        _execute_launch_bootstrap,
        cfg,
        topology_mgr,
        gossip_config,
        client_ssl_ctx,
        serializer,
        engine,
        phase,
        stop,
        state_ref,
        start_engine_event,
    )

    peer_dispatcher = _build_peer_dispatcher(
        cfg=cfg,
        state_ref=state_ref,
        state_port=state_port,
        topology_mgr=topology_mgr,
        probe_mgr=probe_mgr,
        partitioner=partitioner,
        client_ssl_ctx=client_ssl_ctx,
        serializer=serializer,
        engine=engine,
        gossip_config=gossip_config,
        launch_bootstrap=launch_bootstrap,
        peer_address=peer_address,
        kv_address=kv_address,
    )

    peer_host, peer_port = _parse_bind(cfg.peer_server.bind)
    kv_host, kv_port = _parse_bind(cfg.kv_server.bind)

    peer_server = TcpServer(peer_dispatcher, ssl_ctx, name="Peer")
    kv_server = TcpServer(Dispatcher(), ssl_ctx, name="KV")

    await peer_server.start(peer_host, peer_port)

    _log_startup_phase(cfg, phase, effective)

    bind_kv = phase in _KV_PHASES
    if bind_kv:
        await kv_server.start(kv_host, kv_port)

    start_engine = await _startup_phase_logic(
        cfg,
        phase,
        effective,
        state_port,
        topology_mgr,
        client_ssl_ctx,
        serializer,
        state_ref,
        gossip_config,
        engine,
    )

    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop, stop)

    if start_engine:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(_wait_for_stop(stop, engine), name="shutdown.waiter")
            tg.create_task(engine.start(), name="gossip.engine")
    else:
        await _await_engine_start_or_stop(stop, start_engine_event, engine)

    logger.info("Shutdown signal received; stopping node %r.", cfg.node_id)
    await peer_server.stop()
    if bind_kv:
        await kv_server.stop()
    await pool.close_all()
    logger.info("Node %r stopped cleanly.", cfg.node_id)


async def _wait_for_stop(stop: asyncio.Event, engine: GossipEngine) -> None:
    """Wait for stop signal then stop the engine."""
    await stop.wait()
    await engine.stop()


async def _run_seeded_bootstrap(
    cfg: TourillonConfig,
    topology_mgr: TopologyManager,
    gossip_config: GossipConfig,
    client_ssl_ctx: object,
    serializer: object,
) -> None:
    """Run gossip bootstrap for JOINING or DRAINING phase; exit on fatal errors."""
    bootstrapper = GossipBootstrapper(
        topology_manager=topology_mgr,
        config=gossip_config.bootstrap,
        partition_shift=cfg.partition_shift,
        ssl_ctx=client_ssl_ctx,
        serializer=serializer,
    )
    try:
        await bootstrapper.run(cfg.seeds)
    except BootstrapPartitionShiftError as exc:
        logger.error(
            "Bootstrap aborted: partition_shift mismatch local=%d seed=%d. Exiting.",
            exc.local_shift,
            exc.seed_shift,
        )
        sys.exit(1)
    except BootstrapError as exc:
        logger.error("Gossip bootstrap failed: %s. Exiting.", exc)
        sys.exit(1)


async def _await_engine_start_or_stop(
    stop: asyncio.Event,
    start_engine_event: asyncio.Event,
    engine: GossipEngine,
) -> None:
    """Block until stop or until IDLE→JOINING bootstrap completes.

    Called for IDLE nodes with seeds that received a tourctl node join command.
    Uses asyncio.wait to react immediately to whichever event fires first —
    no fixed polling interval. If start_engine_event fires (with or without
    stop also set), GossipEngine is started in a TaskGroup so that the
    hot_queue (pre-populated by _execute_launch_bootstrap) is drained and
    the JOINING member is pushed to the cluster. If only stop fires (no join
    was initiated), the function returns without starting the engine.
    """
    pending = {
        asyncio.ensure_future(stop.wait()),
        asyncio.ensure_future(start_engine_event.wait()),
    }
    done, pending_tasks = await asyncio.wait(
        pending, return_when=asyncio.FIRST_COMPLETED
    )
    # Cancel the task that didn't fire yet.
    for task in pending_tasks:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    # Start the engine if the join completed, even if stop also fired —
    # the _wait_for_stop task inside the TaskGroup will drain it cleanly.
    if not start_engine_event.is_set():
        return
    async with asyncio.TaskGroup() as tg:
        tg.create_task(_wait_for_stop(stop, engine), name="shutdown.waiter")
        tg.create_task(engine.start(), name="gossip.engine")


def _peer_address(cfg: TourillonConfig) -> str:
    """Return the advertised peer address, falling back to the bind address."""
    return cfg.peer_server.advertise or cfg.peer_server.bind


async def _startup_idle(
    cfg: TourillonConfig,
    state_port: FileStateAdapter,
    topology_mgr: TopologyManager,
    state_ref: list[NodeState],
) -> bool:
    """Handle IDLE phase startup; return True if the engine should be started."""
    if cfg.seeds:
        logger.info(
            "Node %r is idle; issue 'tourctl node join' to begin seeded join.",
            cfg.node_id,
        )
        return False
    new_state = await _bootstrap_first_node(cfg, state_port, topology_mgr)
    state_ref[0] = new_state
    logger.info(
        "Node %r is ready (epoch %d, generation %d).",
        cfg.node_id,
        new_state.epoch,
        new_state.generation,
    )
    return True


async def _startup_ready(
    cfg: TourillonConfig,
    state: NodeState,
    topology_mgr: TopologyManager,
) -> bool:
    """Handle READY phase startup; return True (engine always started)."""
    member = Member(
        node_id=cfg.node_id,
        peer_address=_peer_address(cfg),
        generation=state.generation,
        seq=state.seq,
        phase=MemberPhase.READY,
        tokens=state.tokens,
        partition_shift=cfg.partition_shift,
    )
    await topology_mgr.apply_member(member)
    logger.info(
        "Node %r is ready (epoch %d, generation %d).",
        cfg.node_id,
        state.epoch,
        state.generation,
    )
    return True


async def _startup_bootstrap_phase(
    cfg: TourillonConfig,
    state: NodeState,
    phase: MemberPhase,
    topology_mgr: TopologyManager,
    gossip_config: GossipConfig,
    client_ssl_ctx: object,
    serializer: object,
    engine: GossipEngine,
) -> bool:
    """Handle JOINING/DRAINING phase startup; return True (engine always started).

    Self-registers before bootstrap so the local fingerprint includes this node.
    Without this, AE ping/pong returns same=True and peers never learn about
    the joining node. The member is also announced onto the hot-queue so that
    gossip.push is sent to peers immediately on engine start.
    """
    own_member = Member(
        node_id=cfg.node_id,
        peer_address=_peer_address(cfg),
        generation=state.generation,
        seq=state.seq,
        phase=phase,
        tokens=state.tokens,
        partition_shift=cfg.partition_shift,
    )
    await topology_mgr.apply_member(own_member)
    await _run_seeded_bootstrap(
        cfg, topology_mgr, gossip_config, client_ssl_ctx, serializer
    )
    await engine.announce(own_member)
    return True


async def _startup_phase_logic(
    cfg: TourillonConfig,
    phase: MemberPhase,
    state: NodeState,
    state_port: FileStateAdapter,
    topology_mgr: TopologyManager,
    client_ssl_ctx: object,
    serializer: object,
    state_ref: list[NodeState],
    gossip_config: GossipConfig,
    engine: GossipEngine,
) -> bool:
    """Dispatch to the phase-specific startup helper; return True if engine should start."""
    if phase == MemberPhase.IDLE:
        return await _startup_idle(cfg, state_port, topology_mgr, state_ref)
    if phase == MemberPhase.READY:
        return await _startup_ready(cfg, state, topology_mgr)
    if phase in _BOOTSTRAP_PHASES:
        return await _startup_bootstrap_phase(
            cfg,
            state,
            phase,
            topology_mgr,
            gossip_config,
            client_ssl_ctx,
            serializer,
            engine,
        )
    if phase == MemberPhase.PAUSED:
        logger.info(
            "Node %r is paused; resume behaviour is not covered by this proposal.",
            cfg.node_id,
        )
        return False
    if phase == MemberPhase.FAILED:
        logger.warning(
            "Node %r is in phase 'failed'; recovery behaviour is not covered by this proposal.",
            cfg.node_id,
        )
        return False

    return False


async def _bootstrap_first_node(
    cfg: TourillonConfig,
    state_port: FileStateAdapter,
    topology_mgr: TopologyManager,
) -> NodeState:
    """Run first-node bootstrap; exit on any failure."""
    try:
        return await run_first_node_bootstrap(
            cfg, state_port, topology_mgr, HashSpace()
        )
    except Exception as exc:
        logger.error("Bootstrap failed: %s.", exc)
        sys.exit(1)


def _log_startup_phase(
    cfg: TourillonConfig, phase: MemberPhase, state: NodeState
) -> None:
    """Emit the startup info log line for *phase*."""
    if phase == MemberPhase.JOINING:
        logger.info(
            "Node %r starting from phase %r. Resuming gossip bootstrap.",
            cfg.node_id,
            str(phase),
        )
    elif phase == MemberPhase.READY:
        logger.info("Node %r starting from phase %r.", cfg.node_id, str(phase))
        logger.info(
            "Topology rebuilt for node %r: %d vnode(s), epoch %d.",
            cfg.node_id,
            len(state.tokens),
            state.epoch,
        )
    elif phase == MemberPhase.DRAINING:
        logger.info(
            "Node %r starting from phase %r. Resuming drain with gossip bootstrap.",
            cfg.node_id,
            str(phase),
        )
    elif phase == MemberPhase.IDLE and cfg.seeds:
        logger.info(
            "Node %r starting from phase %r with %d seed(s).",
            cfg.node_id,
            str(phase),
            len(cfg.seeds),
        )
    else:
        logger.info("Node %r starting from phase %r.", cfg.node_id, str(phase))


def _parse_bind(address: str) -> tuple[str, int]:
    """Parse a 'host:port' bind string into (host, port)."""
    host, _, port_str = address.rpartition(":")
    return host or "0.0.0.0", int(port_str)


def _install_signal_handlers(
    loop: asyncio.AbstractEventLoop, stop: asyncio.Event
) -> None:
    """Register SIGINT/SIGTERM handlers; ignore errors on unsupported platforms."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, OSError):
            loop.add_signal_handler(sig, stop.set)
