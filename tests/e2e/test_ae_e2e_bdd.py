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

from tests.e2e.journey import Journey, wait_for

pytestmark = [pytest.mark.e2e, pytest.mark.tourillon, pytest.mark.tourctl]
scenarios("features/node_ae.feature")


@pytest.fixture
def ae_journey(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    journey_executables: dict[str, str],
) -> Iterator[Journey]:
    standalone = {
        name
        for name in journey_executables
        if request.config.getoption(f"--{name}-bin") is not None
    }
    journey = Journey(tmp_path, journey_executables, standalone)
    try:
        yield journey
    finally:
        journey.close()


@given("a ready anti-entropy node holding one key")
def ready_node(ae_journey: Journey) -> None:
    ae_journey.generate_ca(ae_journey.ca)
    node = ae_journey.configure("ae-node")
    ae_journey.start(node)
    wait_for(lambda: ae_journey.ready(node), "AE node ready", timeout=30)
    ae_journey.put(node, "ae-key", "value")


@when("the operator reads the anti-entropy status")
def read_status(ae_journey: Journey) -> None:
    node = ae_journey.nodes["ae-node"]
    ae_journey.observations["status"] = ae_journey.ctl(
        node, "ae", "status", node.peer, "--json"
    )
    ae_journey.observations["text"] = ae_journey.ctl_result(
        node, "ae", "status", node.peer
    )


@then("the worker is running and no target is owed data")
def nothing_owed(ae_journey: Journey) -> None:
    status = ae_journey.observations["status"]
    assert status["node_id"] == "ae-node"
    assert status["phase"] == "READY"
    assert status["running"] is True
    assert status["targets"] == 0
    assert status["items"] == []
    assert status["messages"] == []
    text = ae_journey.observations["text"]
    assert text.returncode == 0, text.stderr
    assert "Worker: running" in text.stdout
    assert "No replication obligations are pending." in text.stdout


@then("the obligation listing is empty")
def empty_listing(ae_journey: Journey) -> None:
    node = ae_journey.nodes["ae-node"]
    page = ae_journey.ctl(node, "ae", "obligations", node.peer, "--json")
    assert page["node_id"] == "ae-node"
    assert page["total"] == 0
    assert page["items"] == []
    assert page["cursor"] is None


@then("a forged obligation cursor is rejected as invalid input")
def forged_cursor(ae_journey: Journey) -> None:
    node = ae_journey.nodes["ae-node"]
    result = ae_journey.ctl_result(
        node, "ae", "obligations", node.peer, "--cursor", "forged"
    )
    assert result.returncode == 2, result.stdout + result.stderr
    assert "invalid cursor" in result.stderr
    assert "Traceback" not in result.stderr
