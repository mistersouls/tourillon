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
"""Startup lifecycle consistency checks."""

from __future__ import annotations

from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.structure.config import NodeSize

_TOKEN_CHECK_PHASES = frozenset(
    {MemberPhase.JOINING, MemberPhase.READY, MemberPhase.DRAINING}
)


class NodeIdMismatchError(Exception):
    """Raised when config and persisted node IDs differ."""


def check_node_id_consistency(config_node_id: str, state_node_id: str) -> None:
    if config_node_id != state_node_id:
        raise NodeIdMismatchError(
            f"node_id mismatch: config={config_node_id} state={state_node_id}"
        )


def check_tokens_coherence(
    phase: MemberPhase,
    tokens: tuple[int, ...],
    node_size: NodeSize,
) -> bool:
    if phase not in _TOKEN_CHECK_PHASES:
        return True
    return len(tokens) == node_size.token_count
