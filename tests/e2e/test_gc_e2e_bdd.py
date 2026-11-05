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
scenarios("features/node_gc.feature")


@pytest.fixture
def gc_journey(
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


@given("a ready GC node with three versions of one key")
def ready_gc_node(gc_journey: Journey) -> None:
    gc_journey.generate_ca(gc_journey.ca)
    node = gc_journey.configure("gc-node")
    document = node.config.read_text(encoding="utf-8")
    assert "replication_factor = 3" in document
    node.config.write_text(
        document.replace("replication_factor = 3", "replication_factor = 1"),
        encoding="utf-8",
    )
    gc_journey.start(node)
    wait_for(lambda: gc_journey.ready(node), "GC node ready", timeout=30)
    for value in ("first", "second", "third"):
        gc_journey.put(node, "gc-key", value)


@when("the operator estimates collection and waits for the result")
def estimate(gc_journey: Journey) -> None:
    node = gc_journey.nodes["gc-node"]
    gc_journey.observations["estimate"] = gc_journey.ctl(
        node, "gc", "run", node.peer, "--dry-run", "--wait", "--json"
    )


@then("the estimate reports one collectible version without deleting it")
def estimate_preserves(gc_journey: Journey) -> None:
    result = gc_journey.observations["estimate"]
    assert result["status"]["state"] == "completed"
    assert result["status"]["scanned"] == 3
    assert result["status"]["collected"] == 1
    assert result["status"]["dry_run"] is True
    gc_journey.check(gc_journey.nodes["gc-node"], "gc-key", "third")


@when("the operator collects history and waits for the result")
def collect(gc_journey: Journey) -> None:
    node = gc_journey.nodes["gc-node"]
    gc_journey.observations["collect"] = gc_journey.ctl(
        node, "gc", "run", node.peer, "--wait", "--json"
    )


@then("one version is collected and current data remains readable")
def current_preserved(gc_journey: Journey) -> None:
    result = gc_journey.observations["collect"]
    assert result["status"]["state"] == "completed"
    assert result["status"]["scanned"] == 3
    assert result["status"]["collected"] == 1
    assert result["status"]["record_bytes"] > 0
    assert result["status"]["dry_run"] is False
    gc_journey.check(gc_journey.nodes["gc-node"], "gc-key", "third")


@then("node inspection shows the completed collection")
def inspect_gc(gc_journey: Journey) -> None:
    node = gc_journey.nodes["gc-node"]
    info = gc_journey.inspect(node)["gc"]
    assert info["running"] is True
    assert info["status"] == gc_journey.observations["collect"]["status"]
    collected = info["status"]["collected"]
    text = gc_journey.ctl_result(node, "node", "status", node.peer)
    assert text.returncode == 0, text.stderr
    assert "Garbage collection:" in text.stdout
    assert f"{collected:,} versions collected" in text.stdout


@then("a later estimate reports no collectible history")
def no_history(gc_journey: Journey) -> None:
    node = gc_journey.nodes["gc-node"]
    result = gc_journey.ctl(
        node, "gc", "run", node.peer, "--dry-run", "--wait", "--json"
    )
    assert result["status"]["scanned"] == 2
    assert result["status"]["collected"] == 0
    scoped = gc_journey.ctl(
        node,
        "gc",
        "run",
        node.peer,
        "--partition",
        "0",
        "--dry-run",
        "--wait",
        "--json",
    )
    assert scoped["status"]["partition"] == 0
    assert scoped["status"]["state"] == "completed"


@when("the operator requests collection of an invalid partition")
def invalid_partition(gc_journey: Journey) -> None:
    node = gc_journey.nodes["gc-node"]
    total = gc_journey.inspect(node)["ring_position"]["total_partitions"]
    gc_journey.observations["invalid"] = gc_journey.ctl_result(
        node, "gc", "run", node.peer, "--partition", str(total)
    )


@then("the command rejects the partition without a traceback")
def rejection(gc_journey: Journey) -> None:
    result = gc_journey.observations["invalid"]
    assert result.returncode != 0
    assert "Partition must be in" in result.stderr
    assert "Traceback" not in result.stderr
