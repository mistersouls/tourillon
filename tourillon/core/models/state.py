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

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from tourillon.core.models.member import MemberPhase
from tourlib import ValidationError, require_mapping, require_str


@dataclass(frozen=True)
class NodeState:
    """Persisted runtime state for node lifecycle resumption."""

    node_id: str
    phase: MemberPhase = MemberPhase.IDLE
    prev_phase: MemberPhase | None = None
    tokens: tuple[str, ...] = ()
    seeds: tuple[str, ...] = ()
    epoch: int = 0

    def with_seeds(self, seeds: Sequence[str]) -> "NodeState":
        return NodeState(
            node_id=self.node_id,
            phase=self.phase,
            prev_phase=self.prev_phase,
            tokens=self.tokens,
            seeds=tuple(seeds),
            epoch=self.epoch,
        )

    def to_document(self) -> dict[str, Any]:
        node_document: dict[str, Any] = {
            "node_id": self.node_id,
            "phase": self.phase.value,
            "epoch": self.epoch,
        }
        if self.prev_phase is not None:
            node_document["prev_phase"] = self.prev_phase.value
        if self.tokens:
            node_document["tokens"] = list(self.tokens)
        if self.seeds:
            node_document["seeds"] = list(self.seeds)
        return {"node": node_document}

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "NodeState":
        node_document = require_mapping(document, "node")

        return cls(
            node_id=require_str(node_document, "node_id"),
            phase=cls.phase_property(node_document),
            prev_phase=cls.prev_phase_property(node_document),
            epoch=cls.epoch_property(node_document),
            tokens=cls.tokens_property(node_document),
            seeds=cls.seeds_property(node_document),
        )

    @staticmethod
    def phase_property(node_document: Mapping[str, Any]) -> MemberPhase:
        phase_raw = node_document.get("phase")
        if phase_raw is None:
            return MemberPhase.IDLE
        if not isinstance(phase_raw, str) or not phase_raw:
            raise ValidationError("node.phase must be a non-empty string")
        try:
            return MemberPhase(phase_raw.lower())
        except ValueError as exc:
            raise ValidationError("node.phase must be a valid member phase") from exc

    @staticmethod
    def prev_phase_property(node_document: Mapping[str, Any]) -> MemberPhase | None:
        prev_phase_raw = node_document.get("prev_phase")
        if prev_phase_raw is None:
            return None
        if not isinstance(prev_phase_raw, str) or not prev_phase_raw:
            raise ValidationError("node.prev_phase must be a non-empty string")
        try:
            return MemberPhase(prev_phase_raw.lower())
        except ValueError as exc:
            raise ValidationError(
                "node.prev_phase must be a valid member phase"
            ) from exc

    @staticmethod
    def epoch_property(node_document: Mapping[str, Any]) -> int:
        epoch_raw = node_document.get("epoch")
        if epoch_raw is None:
            return 0
        if (
            not isinstance(epoch_raw, int)
            or isinstance(epoch_raw, bool)
            or epoch_raw < 0
        ):
            raise ValidationError("node.epoch must be a non-negative integer")
        return epoch_raw

    @staticmethod
    def tokens_property(node_document: Mapping[str, Any]) -> tuple[str, ...]:
        tokens_raw = node_document.get("tokens")
        if tokens_raw is None:
            return ()

        if not isinstance(tokens_raw, list):
            raise ValidationError("node.tokens must be a list of hex strings")

        tokens: list[str] = []

        for token in tokens_raw:
            if not isinstance(token, str):
                raise ValidationError("node.tokens must be hex strings")

            try:
                int(token, 16)
            except ValueError:
                raise ValidationError(f"invalid hex token: {token}") from None

            tokens.append(token)

        return tuple(tokens)

    @staticmethod
    def seeds_property(node_document: Mapping[str, Any]) -> tuple[str, ...]:
        seeds_raw = node_document.get("seeds")
        if seeds_raw is None:
            return ()
        if not isinstance(seeds_raw, list):
            raise ValidationError("node.seeds must be a list of non-empty strings")
        if any(not isinstance(seed, str) or not seed for seed in seeds_raw):
            raise ValidationError("node.seeds must be a list of non-empty strings")
        return tuple(seeds_raw)
