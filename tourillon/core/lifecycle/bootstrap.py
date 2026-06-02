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
"""Bootstraper orchestrates the first-node start path."""

from __future__ import annotations

import logging
import math
import secrets
from pathlib import Path

from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.state import StatePort
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner, PartitionRange
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.config import TourillonConfig

logger = logging.getLogger(__name__)


class BootstrapError(Exception):
    exit_code: int

    def __init__(self, message: str, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class Bootstraper:
    def __init__(
        self,
        cfg: TourillonConfig,
        state_port: StatePort,
        topology_mgr: TopologyManager,
        hash_space: HashSpace,
        segment_shift: int,
    ) -> None:
        self._cfg = cfg
        self._state_port = state_port
        self._topology_mgr = topology_mgr
        self._hash_space = hash_space
        self._segment_shift = segment_shift
        self._partitioner = Partitioner(hash_space, cfg.partition_shift, segment_shift)
        self._latest_ranges: list[PartitionRange] = []

    @classmethod
    def from_config_path(cls, config_path: Path) -> Bootstraper:
        raise BootstrapError(
            f"from_config_path is wired in bootstrap layer, got: {config_path}",
            exit_code=2,
        )

    @property
    def ranges(self) -> list[PartitionRange]:
        return list(self._latest_ranges)

    async def start_node(self) -> NodeState:
        persisted = await self._state_port.load()
        phase = persisted.phase if persisted is not None else MemberPhase.IDLE
        if phase is MemberPhase.IDLE:
            return await self.start_first_node()
        if phase is MemberPhase.READY:
            return await self.start_ready_node()
        raise BootstrapError(
            f"unexpected phase {phase.value} — cannot start from this state via 'node start'.",
            exit_code=1,
        )

    async def start_first_node(self) -> NodeState:
        tokens = tuple(
            secrets.randbelow(self._hash_space.max)
            for _ in range(self._cfg.node_size.token_count)
        )
        state = NodeState(
            node_id=self._cfg.node_id,
            phase=MemberPhase.READY,
            generation=1,
            seq=0,
            tokens=tokens,
            epoch=1,
        )
        await self._state_port.save(state)
        await self._topology_mgr.apply_member(self._member_for_state(state))
        topology = await self._topology_mgr.snapshot()
        self._latest_ranges = self._partitioner.ranges_for(
            self._cfg.node_id, topology.ring
        )
        return state

    async def start_ready_node(self) -> NodeState:
        persisted = await self._state_port.load()
        if persisted is None or persisted.phase is not MemberPhase.READY:
            raise BootstrapError(
                "ready restart requested without persisted READY state"
            )
        await self._topology_mgr.apply_member(self._member_for_state(persisted))
        topology = await self._topology_mgr.snapshot()
        self._latest_ranges = self._partitioner.ranges_for(
            self._cfg.node_id, topology.ring
        )
        return persisted

    def _member_for_state(self, state: NodeState) -> Member:
        peer_address = self._cfg.peer_server.advertise or self._cfg.peer_server.bind
        return Member(
            node_id=self._cfg.node_id,
            peer_address=peer_address,
            generation=state.generation,
            seq=state.seq,
            phase=state.phase,
            tokens=state.tokens,
            partition_shift=self._cfg.partition_shift,
        )


def derive_segment_shift(token_count: int) -> int:
    if token_count < 1:
        raise ValueError("token_count must be >= 1")
    if token_count & (token_count - 1) != 0:
        raise ValueError("token_count must be a power of two")
    return int(math.log2(token_count))
