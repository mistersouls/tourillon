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

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from tourillon.core.spaces.vnode import VNode


class MemberPhase(StrEnum):
    IDLE = "idle"
    JOINING = "joining"
    READY = "ready"
    PAUSED = "paused"
    DRAINING = "draining"
    # Deprecated: will be removed. A failed join should stay JOINING or go back
    # to IDLE, a failed drain stays DRAINING. Never part of the ring.
    FAILED = "failed"


@dataclass(frozen=True)
class Member:
    node_id: str
    peer_address: str
    epoch: int
    phase: MemberPhase
    tokens: tuple[str, ...]

    @property
    def vnodes(self) -> list[VNode]:
        return [VNode(token=int(token, 16), node_id=self.node_id) for token in self.tokens]

    def supersedes(self, other: "Member") -> bool:
        return self.epoch > other.epoch

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "peer_address": self.peer_address,
            "phase": self.phase.value,
            "epoch": self.epoch,
            "tokens": list(self.tokens),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Member":
        epoch = int(data["epoch"])
        if epoch < 0:
            raise ValueError(f"invalid member fields: epoch={epoch}")

        return Member(
            node_id=data["node_id"],
            peer_address=data["peer_address"],
            epoch=epoch,
            phase=MemberPhase(data["phase"]),
            tokens=tuple(data.get("tokens", [])),
        )
