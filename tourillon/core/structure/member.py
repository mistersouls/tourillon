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
"""Member FSM, State and gossip record."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class MemberPhase(StrEnum):
    IDLE = "idle"
    JOINING = "joining"
    READY = "ready"
    PAUSED = "paused"
    DRAINING = "draining"
    FAILED = "failed"


@dataclass(frozen=True)
class Member:
    node_id: str
    peer_address: str
    generation: int
    seq: int
    phase: MemberPhase
    tokens: tuple[int, ...]
    partition_shift: int

    def supersedes(self, other: "Member") -> bool:
        return (self.generation, self.seq) > (other.generation, other.seq)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "peer_address": self.peer_address,
            "phase": self.phase.value,
            "generation": self.generation,
            "seq": self.seq,
            "tokens": list(self.tokens),
            "partition_shift": self.partition_shift,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Member":
        gen = int(data["generation"])
        seq = int(data["seq"])
        if gen < 0 or seq < 0:
            raise ValueError(f"invalid member fields: generation={gen} seq={seq}")
        return Member(
            node_id=data["node_id"],
            peer_address=data["peer_address"],
            generation=gen,
            seq=seq,
            phase=MemberPhase(data["phase"]),
            tokens=tuple(data.get("tokens", [])),
            partition_shift=int(data["partition_shift"]),
        )


@dataclass(frozen=True)
class NodeState:
    node_id: str
    phase: MemberPhase
    generation: int
    seq: int
    tokens: tuple[int, ...]
    epoch: int
    committed_pids: tuple[int, ...] = ()
    staging_pids: tuple[int, ...] = ()

    @classmethod
    def first(cls, node_id: str) -> "NodeState":
        return cls(
            node_id=node_id,
            phase=MemberPhase.IDLE,
            generation=0,
            seq=0,
            tokens=(),
            epoch=0,
            committed_pids=(),
            staging_pids=(),
        )
