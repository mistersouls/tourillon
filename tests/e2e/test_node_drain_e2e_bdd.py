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
scenarios("features/node_drain.feature")


@pytest.fixture
def drain_journey(
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
def ready_node(drain_journey: Journey) -> None:
    drain_journey.generate_ca(drain_journey.ca)
    node = drain_journey.configure("draining-node")
    drain_journey.start(node)
    wait_for(lambda: drain_journey.ready(node), "node ready", timeout=30)


@given("two ready peers with an authorized operator")
def ready_peers(drain_journey: Journey) -> None:
    drain_journey.generate_ca(drain_journey.ca)
    draining = drain_journey.configure("draining-node")
    configure_single_replica(draining)
    drain_journey.start(draining)
    wait_for(lambda: drain_journey.ready(draining), "first node ready", timeout=30)
    remaining = drain_journey.configure("remaining-node", seed=draining.peer)
    configure_single_replica(remaining)
    drain_journey.start(remaining)
    wait_for(
        lambda: drain_journey.probe(remaining, "peer"),
        "second peer endpoint",
        timeout=30,
    )
    result = drain_journey.ctl_result(
        remaining, "node", "join", remaining.peer, "--seeds", draining.peer
    )
    assert result.returncode == 0, result.stdout + result.stderr
    wait_for(lambda: drain_journey.ready(remaining), "second node ready", timeout=30)

    def both_ready() -> bool:
        members = drain_journey.inspect(draining)["members"]
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
def write_reference_data(drain_journey: Journey) -> None:
    draining = drain_journey.nodes["draining-node"]
    for index in range(24):
        key, value = f"drain-reference-{index}", f"preserved-value-{index}"
        drain_journey.put(draining, key, value)
        drain_journey.references[key] = value
    assert drain_journey.inspect(draining)["ring_position"]["owned_partitions"] > 0


@when("the operator requests that the node drain")
def request_drain(drain_journey: Journey) -> None:
    node = drain_journey.nodes["draining-node"]
    drain_journey.observations["drain"] = drain_journey.ctl_result(
        node, "node", "drain", node.peer
    )


@then("the command acknowledges asynchronous draining initiation")
def acknowledged(drain_journey: Journey) -> None:
    result = drain_journey.observations["drain"]
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Drain initiated" in result.stdout
    assert "draining continues asynchronously" in result.stdout


@then("the draining node reaches idle and the remaining peer observes its completion")
def drain_completed(drain_journey: Journey) -> None:
    draining = drain_journey.nodes["draining-node"]
    remaining = drain_journey.nodes["remaining-node"]

    def completed() -> bool:
        assert drain_journey.inspect(draining)["self_member"]["phase"] == "IDLE"
        members = drain_journey.inspect(remaining)["members"]
        drained = next(m for m in members if m["node_id"] == draining.name)
        assert drained["phase"] == "IDLE"
        assert drain_journey.ready(remaining)
        return True

    wait_for(completed, "drained node IDLE on both peer views", timeout=60)


@then("all written data remains readable after the drained process is stopped")
def survivor_preserves_data(drain_journey: Journey) -> None:
    draining = drain_journey.nodes["draining-node"]
    remaining = drain_journey.nodes["remaining-node"]
    drain_journey.stop(draining)
    assert draining.process is not None and draining.process.poll() is not None
    for key, value in drain_journey.references.items():
        drain_journey.check(remaining, key, value)


@then("the command rejects draining without another node")
def drain_rejected(drain_journey: Journey) -> None:
    result = drain_journey.observations["drain"]
    assert result.returncode != 0
    assert "without another node" in result.stderr
    assert "Traceback" not in result.stderr
    assert drain_journey.ready(drain_journey.nodes["draining-node"])
