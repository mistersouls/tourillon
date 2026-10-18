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

import pytest

from tourillon.core.models.member import MemberPhase
from tourillon.core.models.state import NodeState
from tourlib.exceptions import ValidationError


@pytest.mark.tourillon
def test_node_state_round_trip_ready_phase() -> None:
    state = NodeState.from_document(
        {
            "node": {
                "node_id": "node-a",
                "phase": "READY",
                "epoch": 3,
            }
        }
    )

    assert state.node_id == "node-a"
    assert state.phase == MemberPhase.READY
    assert state.epoch == 3
    assert state.prev_phase is None
    assert state.to_document() == {
        "node": {
            "node_id": "node-a",
            "phase": "ready",
            "epoch": 3,
        }
    }


@pytest.mark.tourillon
def test_node_state_allows_only_required_node_id() -> None:
    state = NodeState.from_document(
        {
            "node": {
                "node_id": "node-a",
            }
        }
    )

    assert state.to_document() == {
        "node": {
            "node_id": "node-a",
            "phase": "idle",
            "epoch": 0,
        }
    }


@pytest.mark.tourillon
def test_node_state_rejects_empty_phase_when_present() -> None:
    with pytest.raises(ValidationError, match="node.phase must be a non-empty string"):
        NodeState.from_document(
            {
                "node": {
                    "node_id": "node-a",
                    "phase": "",
                }
            }
        )


@pytest.mark.tourillon
def test_node_state_rejects_unknown_phase_when_present() -> None:
    with pytest.raises(
        ValidationError, match="node.phase must be a valid member phase"
    ):
        NodeState.from_document(
            {
                "node": {
                    "node_id": "node-a",
                    "phase": "UNKNOWN",
                }
            }
        )


@pytest.mark.tourillon
def test_node_state_requires_non_negative_integer_epoch() -> None:
    with pytest.raises(
        ValidationError, match="node.epoch must be a non-negative integer"
    ):
        NodeState.from_document(
            {
                "node": {
                    "node_id": "node-a",
                    "phase": "IDLE",
                    "epoch": -1,
                }
            }
        )


@pytest.mark.tourillon
def test_node_state_rejects_unknown_prev_phase_when_present() -> None:
    with pytest.raises(
        ValidationError, match="node.prev_phase must be a valid member phase"
    ):
        NodeState.from_document(
            {
                "node": {
                    "node_id": "node-a",
                    "phase": "READY",
                    "prev_phase": "UNKNOWN",
                }
            }
        )


@pytest.mark.tourillon
def test_node_state_accepts_prev_phase_when_present() -> None:
    state = NodeState.from_document(
        {
            "node": {
                "node_id": "node-a",
                "phase": "PAUSED",
                "epoch": 8,
                "prev_phase": "READY",
            }
        }
    )

    assert state.to_document() == {
        "node": {
            "node_id": "node-a",
            "phase": "paused",
            "epoch": 8,
            "prev_phase": "ready",
        }
    }


@pytest.mark.tourillon
def test_node_state_round_trip_with_tokens() -> None:
    state = NodeState.from_document(
        {
            "node": {
                "node_id": "node-a",
                "tokens": ["1", "2", "3"],
            }
        }
    )

    assert state.tokens == ("1", "2", "3")
    assert state.to_document() == {
        "node": {
            "node_id": "node-a",
            "phase": "idle",
            "tokens": ["1", "2", "3"],
            "epoch": 0,
        }
    }


@pytest.mark.tourillon
def test_node_state_rejects_non_list_tokens() -> None:
    with pytest.raises(
        ValidationError, match="node.tokens must be a list of hex strings"
    ):
        NodeState.from_document(
            {
                "node": {
                    "node_id": "node-a",
                    "tokens": "1,2,3",
                }
            }
        )


@pytest.mark.tourillon
def test_node_state_rejects_invalid_tokens_entries() -> None:
    with pytest.raises(ValidationError, match="node.tokens must be hex strings"):
        NodeState.from_document(
            {
                "node": {
                    "node_id": "node-a",
                    "tokens": [1, -2, True],
                }
            }
        )


@pytest.mark.tourillon
def test_node_state_rejects_invalid_hex_string_token() -> None:
    with pytest.raises(ValidationError, match="invalid hex token"):
        NodeState.from_document(
            {
                "node": {
                    "node_id": "node-a",
                    "tokens": ["not-hex!"],
                }
            }
        )


@pytest.mark.tourillon
def test_node_state_with_seeds_returns_new_state_with_seeds() -> None:
    state = NodeState(node_id="node-a", phase=MemberPhase.IDLE, epoch=0)
    updated = state.with_seeds(["seed-1:7001", "seed-2:7001"])

    assert updated.seeds == ("seed-1:7001", "seed-2:7001")
    assert updated.node_id == state.node_id
    assert updated.phase == state.phase
    assert updated.epoch == state.epoch


@pytest.mark.tourillon
def test_node_state_to_document_includes_seeds() -> None:
    state = NodeState(
        node_id="node-a", phase=MemberPhase.IDLE, epoch=0, seeds=("s1:7001",)
    )
    doc = state.to_document()

    assert doc["node"]["seeds"] == ["s1:7001"]  # type: ignore[index]


@pytest.mark.tourillon
def test_node_state_rejects_empty_prev_phase_string() -> None:
    with pytest.raises(
        ValidationError, match="node.prev_phase must be a non-empty string"
    ):
        NodeState.from_document(
            {
                "node": {
                    "node_id": "node-a",
                    "phase": "ready",
                    "prev_phase": "",
                }
            }
        )


@pytest.mark.tourillon
def test_node_state_rejects_non_list_seeds() -> None:
    with pytest.raises(
        ValidationError, match="node.seeds must be a list of non-empty strings"
    ):
        NodeState.from_document(
            {
                "node": {
                    "node_id": "node-a",
                    "seeds": "not-a-list",
                }
            }
        )


@pytest.mark.tourillon
def test_node_state_rejects_empty_seed_entry() -> None:
    with pytest.raises(
        ValidationError, match="node.seeds must be a list of non-empty strings"
    ):
        NodeState.from_document(
            {
                "node": {
                    "node_id": "node-a",
                    "seeds": ["valid:7001", ""],
                }
            }
        )
