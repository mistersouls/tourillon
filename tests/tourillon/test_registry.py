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

from tourillon.core.gossip.registry import MemberRegistry
from tourillon.core.models.member import Member, MemberPhase


@pytest.mark.tourillon
def test_upsert_inserts_new_member_and_get_returns_it() -> None:
    registry = MemberRegistry()
    member = Member(
        node_id="node-a",
        peer_address="127.0.0.1:7001",
        epoch=1,
        phase=MemberPhase.JOINING,
        tokens=(1, 2),
    )

    changed = registry.upsert(member)

    assert changed is True
    assert registry.get("node-a") == member
    assert len(registry) == 1


@pytest.mark.tourillon
def test_upsert_rejects_older_or_equal_epoch() -> None:
    registry = MemberRegistry()
    current = Member(
        node_id="node-a",
        peer_address="127.0.0.1:7001",
        epoch=2,
        phase=MemberPhase.READY,
        tokens=(1, 2),
    )
    older = Member(
        node_id="node-a",
        peer_address="127.0.0.1:7001",
        epoch=1,
        phase=MemberPhase.DRAINING,
        tokens=(1, 2),
    )
    same = Member(
        node_id="node-a",
        peer_address="127.0.0.1:7001",
        epoch=2,
        phase=MemberPhase.FAILED,
        tokens=(1, 2),
    )

    assert registry.upsert(current) is True
    assert registry.upsert(older) is False
    assert registry.upsert(same) is False
    assert registry.get("node-a") == current


@pytest.mark.tourillon
def test_snapshot_is_independent_copy() -> None:
    registry = MemberRegistry()
    member_a = Member(
        node_id="node-a",
        peer_address="127.0.0.1:7001",
        epoch=1,
        phase=MemberPhase.JOINING,
        tokens=(1,),
    )
    member_b = Member(
        node_id="node-b",
        peer_address="127.0.0.1:7002",
        epoch=1,
        phase=MemberPhase.READY,
        tokens=(2,),
    )

    registry.upsert(member_a)
    snap = registry.snapshot()
    registry.upsert(member_b)

    assert snap.get("node-a") == member_a
    assert snap.get("node-b") is None


@pytest.mark.tourillon
def test_registry_iteration_yields_members() -> None:
    registry = MemberRegistry()
    registry.upsert(
        Member(
            node_id="node-a",
            peer_address="127.0.0.1:7001",
            epoch=1,
            phase=MemberPhase.JOINING,
            tokens=(1,),
        )
    )
    registry.upsert(
        Member(
            node_id="node-b",
            peer_address="127.0.0.1:7002",
            epoch=1,
            phase=MemberPhase.READY,
            tokens=(2,),
        )
    )

    node_ids = {member.node_id for member in registry}
    assert node_ids == {"node-a", "node-b"}
