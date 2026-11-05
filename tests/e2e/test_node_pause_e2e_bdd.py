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
scenarios("features/node_pause.feature")


@pytest.fixture
def pause_journey(
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


@given("a two-peer cluster with data on the pause target")
def two_peer_cluster(pause_journey: Journey) -> None:
    pause_journey.generate_ca(pause_journey.ca)
    target = pause_journey.configure("pause-target")
    configure_single_replica(target)
    pause_journey.start(target)
    wait_for(lambda: pause_journey.ready(target), "pause target ready", timeout=30)

    peer = pause_journey.configure("pause-peer", seed=target.peer)
    configure_single_replica(peer)
    pause_journey.start(peer)
    wait_for(lambda: pause_journey.probe(peer, "peer"), "second peer", timeout=30)
    joined = pause_journey.ctl_result(
        peer, "node", "join", peer.peer, "--seeds", target.peer
    )
    assert joined.returncode == 0, joined.stdout + joined.stderr
    wait_for(lambda: pause_journey.ready(peer), "second node ready", timeout=30)

    def both_ready() -> bool:
        members = pause_journey.members(target)
        return len(members) == 2 and all(
            member["phase"] == "READY" for member in members
        )

    wait_for(both_ready, "both peers READY", timeout=30)
    pause_journey.put(target, "before-pause", "preserved")


def configure_single_replica(node: Node) -> None:
    document = node.config.read_text(encoding="utf-8")
    assert "replication_factor = 3" in document
    node.config.write_text(
        document.replace("replication_factor = 3", "replication_factor = 1"),
        encoding="utf-8",
    )


@when("the operator pauses the target node")
def request_pause(pause_journey: Journey) -> None:
    target = pause_journey.nodes["pause-target"]
    pause_journey.observations["pause"] = pause_journey.ctl_result(
        target, "node", "pause", target.peer
    )


@then("the target refuses client operations and reports paused")
def target_paused(pause_journey: Journey) -> None:
    target = pause_journey.nodes["pause-target"]
    result = pause_journey.observations["pause"]
    assert result.returncode == 0, result.stdout + result.stderr
    assert "paused" in result.stdout.lower()
    assert pause_journey.inspect(target)["phase"] == "PAUSED"

    refused = pause_journey.ctl_result(
        target,
        "get",
        "before-pause",
        "--namespace",
        "journey",
        "--namespace-type",
        "str",
        "--json",
    )
    assert refused.returncode != 0
    assert "Traceback" not in refused.stderr


@when("the operator resumes the target and waits")
def request_resume(pause_journey: Journey) -> None:
    target = pause_journey.nodes["pause-target"]
    pause_journey.observations["resume"] = pause_journey.ctl_result(
        target, "node", "resume", target.peer, "--wait", "--timeout", "60"
    )


@then("the target reaches ready and the data remains readable")
def target_ready(pause_journey: Journey) -> None:
    target = pause_journey.nodes["pause-target"]
    result = pause_journey.observations["resume"]
    assert result.returncode == 0, result.stdout + result.stderr
    assert "phase=READY" in result.stdout
    assert pause_journey.inspect(target)["phase"] == "READY"
    pause_journey.check(target, "before-pause", "preserved")
