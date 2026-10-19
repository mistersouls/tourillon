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

from collections.abc import Iterator
from pathlib import Path

import pytest
from pytest_bdd import given, scenarios, then, when

from tests.e2e.journey import Journey, Node, wait_for

pytestmark = [pytest.mark.e2e, pytest.mark.tourctl, pytest.mark.tourillon]
scenarios("features/node_leave.feature")


@pytest.fixture
def leave_journey(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    journey_executables: dict[str, str],
) -> Iterator[Journey]:
    standalone_tools = {
        name
        for name in journey_executables
        if request.config.getoption(f"--{name}-bin") is not None
    }
    journey = Journey(tmp_path, journey_executables, standalone_tools)
    try:
        yield journey
    finally:
        journey.close()


@given("a ready node with an authorized operator")
def ready_node(leave_journey: Journey) -> None:
    leave_journey.generate_ca(leave_journey.ca)
    node = leave_journey.configure("leaving-node")
    leave_journey.start(node)
    wait_for(lambda: leave_journey.ready(node), "node ready", timeout=30)


@given("two ready peers with an authorized operator")
def ready_peers(leave_journey: Journey) -> None:
    leave_journey.generate_ca(leave_journey.ca)
    leaving = leave_journey.configure("leaving-node")
    configure_single_replica(leaving)
    leave_journey.start(leaving)
    wait_for(lambda: leave_journey.ready(leaving), "first node ready", timeout=30)
    remaining = leave_journey.configure("remaining-node", seed=leaving.peer)
    configure_single_replica(remaining)
    leave_journey.start(remaining)
    wait_for(
        lambda: leave_journey.probe(remaining, "peer"),
        "second peer endpoint",
        timeout=30,
    )
    result = leave_journey.ctl_result(
        remaining, "node", "join", remaining.peer, "--seeds", leaving.peer
    )
    assert result.returncode == 0, result.stdout + result.stderr
    wait_for(lambda: leave_journey.ready(remaining), "second node ready", timeout=30)

    def both_ready() -> bool:
        members = leave_journey.inspect(leaving)["members"]
        return len(members) == 2 and all(m["phase"] == "READY" for m in members)

    wait_for(both_ready, "two READY members", timeout=30)


def configure_single_replica(node: Node) -> None:
    document = node.config.read_text(encoding="utf-8")
    assert "replication_factor = 3" in document
    node.config.write_text(
        document.replace("replication_factor = 3", "replication_factor = 1"),
        encoding="utf-8",
    )


@given("single-replica data has been written across both peers")
def write_reference_data(leave_journey: Journey) -> None:
    leaving = leave_journey.nodes["leaving-node"]
    for index in range(24):
        key, value = f"leave-reference-{index}", f"preserved-value-{index}"
        leave_journey.put(leaving, key, value)
        leave_journey.references[key] = value
    assert leave_journey.inspect(leaving)["ring_position"]["owned_partitions"] > 0


@when("the operator requests that the node leave")
def request_leave(leave_journey: Journey) -> None:
    node = leave_journey.nodes["leaving-node"]
    leave_journey.observations["leave"] = leave_journey.ctl_result(
        node, "node", "leave", node.peer
    )


@then("the command acknowledges asynchronous draining initiation")
def acknowledged(leave_journey: Journey) -> None:
    result = leave_journey.observations["leave"]
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Leave initiated" in result.stdout
    assert "draining continues asynchronously" in result.stdout


@then("the departing node reaches idle and the remaining peer observes its departure")
def departure_complete(leave_journey: Journey) -> None:
    leaving = leave_journey.nodes["leaving-node"]
    remaining = leave_journey.nodes["remaining-node"]

    def completed() -> bool:
        assert leave_journey.inspect(leaving)["self_member"]["phase"] == "IDLE"
        members = leave_journey.inspect(remaining)["members"]
        departed = next(m for m in members if m["node_id"] == leaving.name)
        assert departed["phase"] == "IDLE"
        assert leave_journey.ready(remaining)
        return True

    wait_for(completed, "departing node IDLE on both peer views", timeout=60)


@then("all written data remains readable after the departed process is stopped")
def survivor_preserves_data(leave_journey: Journey) -> None:
    leaving = leave_journey.nodes["leaving-node"]
    remaining = leave_journey.nodes["remaining-node"]
    leave_journey.stop(leaving)
    assert leaving.process is not None and leaving.process.poll() is not None
    for key, value in leave_journey.references.items():
        leave_journey.check(remaining, key, value)


@then("the command rejects departure without another node")
def departure_rejected(leave_journey: Journey) -> None:
    result = leave_journey.observations["leave"]
    assert result.returncode != 0
    assert "without another node" in result.stderr
    assert "Traceback" not in result.stderr
    assert leave_journey.ready(leave_journey.nodes["leaving-node"])
