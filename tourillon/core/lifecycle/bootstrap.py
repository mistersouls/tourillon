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
"""First-node bootstrap domain logic — IDLE → READY transition."""

from __future__ import annotations

import logging
import secrets

from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.state import StatePort
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.config import TourillonConfig

logger = logging.getLogger(__name__)


class BootstrapError(Exception):
    """Raised when the node is in a phase that prevents first-node bootstrap.

    exit_code carries the recommended process exit code (always 1 for this
    proposal). The message describes the unexpected phase and the corrective
    action the operator should take.
    """

    def __init__(self, message: str, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


async def run_first_node_bootstrap(
    cfg: TourillonConfig,
    state_port: StatePort,
    topology_mgr: TopologyManager,
    hash_space: HashSpace,
) -> NodeState:
    """Execute first-node bootstrap (IDLE → READY) or crash-recovery restart.

    The function reads the persisted phase first and dispatches to the
    appropriate path. No socket is opened here; socket binding is the
    caller's responsibility after this function returns.

    Raise BootstrapError with exit_code=1 when the persisted phase is
    anything other than IDLE or READY.
    """
    persisted = await state_port.load()
    phase = persisted.phase if persisted is not None else MemberPhase.IDLE
    logger.info("Node '%s' starting from phase '%s'.", cfg.node_id, phase.value)

    if phase == MemberPhase.IDLE:
        return await _bootstrap_idle(cfg, state_port, topology_mgr, hash_space)
    if phase == MemberPhase.READY:
        assert persisted is not None  # guaranteed by phase check above
        return await _restart_ready(cfg, persisted, topology_mgr, hash_space)
    raise BootstrapError(
        f"unexpected phase {phase} — cannot start from this state",
        exit_code=1,
    )


async def _bootstrap_idle(
    cfg: TourillonConfig,
    state_port: StatePort,
    topology_mgr: TopologyManager,
    hash_space: HashSpace,
) -> NodeState:
    """Execute the IDLE → READY transition for a seedless first-node bootstrap.

    Tokens are generated once, the state is persisted atomically before any
    topology mutation, and the node's vnodes are added to the ring last.
    """
    token_count = cfg.node_size.token_count
    logger.debug(
        "Generating %d token(s) for node '%s' in the hash space.",
        token_count,
        cfg.node_id,
    )
    tokens = tuple(secrets.randbelow(hash_space.max) for _ in range(token_count))

    state = NodeState(
        phase=MemberPhase.READY,
        generation=1,
        seq=0,
        tokens=tokens,
        epoch=1,
    )
    # Write-before-announce: persist state before any topology change.
    logger.debug("Persisting state to disk before topology announcement.")
    await state_port.save(state)

    partitioner = Partitioner(hash_space, cfg.partition_shift)
    logger.info(
        "Node '%s' bootstrapped as first node: %d partitions, %d vnode(s).",
        cfg.node_id,
        partitioner.total_partitions,
        token_count,
    )

    peer_address = cfg.peer_server.advertise or cfg.peer_server.bind
    member = Member(
        node_id=cfg.node_id,
        peer_address=peer_address,
        generation=1,
        seq=0,
        phase=MemberPhase.READY,
        tokens=tokens,
    )
    await topology_mgr.apply_member(member)
    return state


async def _restart_ready(
    cfg: TourillonConfig,
    state: NodeState,
    topology_mgr: TopologyManager,
    hash_space: HashSpace,
) -> NodeState:
    """Rebuild topology from a persisted READY state without executing a transition.

    No state is written to disk; epoch and generation are taken from the
    persisted NodeState unchanged.
    """
    partitioner = Partitioner(hash_space, cfg.partition_shift)
    logger.info(
        "Topology rebuilt for node '%s': %d vnode(s), epoch %d.",
        cfg.node_id,
        len(state.tokens),
        state.epoch,
    )
    peer_address = cfg.peer_server.advertise or cfg.peer_server.bind
    member = Member(
        node_id=cfg.node_id,
        peer_address=peer_address,
        generation=state.generation,
        seq=state.seq,
        phase=MemberPhase.READY,
        tokens=state.tokens,
    )
    await topology_mgr.apply_member(member)
    _ = partitioner  # injected for future use by callers
    return state
