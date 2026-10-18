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

import base64
import hashlib
import re
import shutil
import socket
import time
from pathlib import Path

import pytest
from pytest_bdd import given, scenarios, then, when

from tests.e2e.journey import COMMAND_TIMEOUT, Journey, wait_for
from tests.e2e.reporting import export_journey
from tourlib.adapter import MsgPackSerializer
from tourlib.models.version import Address

pytestmark = [pytest.mark.e2e, pytest.mark.tourillon, pytest.mark.tourctl]
scenarios("features/user_journey.feature")


@pytest.fixture
def journey(
    request: pytest.FixtureRequest,
    journey_executables: dict[str, str],
    tmp_path: Path,
):
    root = tmp_path / "portable journey with spaces"
    root.mkdir()
    standalone_tools = {
        name
        for name in journey_executables
        if request.config.getoption(f"--{name}-bin") is not None
    }
    context = Journey(root, journey_executables, standalone_tools)
    failures = request.session.testsfailed
    try:
        yield context
    finally:
        try:
            context.close()
        finally:
            artifacts = request.config.getoption("--journey-artifacts")
            if artifacts is not None:
                report = export_journey(
                    context, Path(artifacts).resolve(), request.node.nodeid
                )
                print(f"E2E report: {report}")
        total = sum(duration for duration, _ in context.durations)
        slowest = max(context.durations, default=(0.0, "none"))
        print(
            f"E2E: {len(context.durations)} CLI commands, {total:.1f}s; "
            f"slowest {slowest[0]:.1f}s {slowest[1]}"
        )
        if request.session.testsfailed == failures:
            wait_for(
                lambda: remove_workspace(root), "released scenario files", timeout=10
            )
        else:
            print(f"Real-process E2E diagnostics: {root}")


def remove_workspace(root: Path) -> bool:
    try:
        shutil.rmtree(root)
    except PermissionError:
        return False
    return True


@given("a generated node and authorized client configuration")
def configured(journey: Journey) -> None:
    journey.generate_ca(journey.ca)
    journey.configure("A")


@given("an authorized single-node database")
def running(journey: Journey) -> None:
    configured(journey)
    node = journey.nodes["A"]
    journey.start(node)
    wait_for(lambda: journey.ready(node), "A ready on both mTLS endpoints", timeout=30)


@given("reference data written before any other node joins")
def references(journey: Journey) -> None:
    for index in range(12):
        key, value = f"reference-{index}", f"before-join-{index}"
        journey.put(journey.nodes["A"], key, value)
        journey.references[key] = value
    journey.observations["initial-ring"] = journey.inspect(journey.nodes["A"])[
        "ring_position"
    ]


@when("I configure and start node B and request that it join node A")
def join(journey: Journey) -> None:
    a = journey.nodes["A"]
    b = journey.configure("B", seed=a.peer)
    journey.start(b)
    wait_for(lambda: journey.probe(b, "peer"), "B peer endpoint", timeout=30)
    result = journey.ctl_result(b, "node", "join", b.peer, "--seeds", a.peer)
    assert result.returncode == 0, result.stdout + result.stderr


def membership_snapshot(journey: Journey) -> tuple:
    infos = [journey.inspect(node) for node in journey.nodes.values()]
    memberships = []
    for info in infos:
        members = info["members"]
        assert {member["node_id"] for member in members} == {"A", "B"}
        assert {member["phase"] for member in members} == {"READY"}
        memberships.append(
            tuple(sorted((m["node_id"], m["phase"], m["epoch"]) for m in members))
        )
    assert memberships[0] == memberships[1]
    assert (
        sum(info["ring_position"]["owned_partitions"] for info in infos)
        == infos[0]["ring_position"]["total_partitions"]
    )
    return memberships[0]


@then("both nodes converge to the same stable ready membership")
def converged(journey: Journey) -> None:
    previous = None
    stable = 0

    def settled() -> bool:
        nonlocal previous, stable
        current = membership_snapshot(journey)
        stable = stable + 1 if current == previous else 1
        previous = current
        return stable >= 3

    wait_for(settled, "identical READY membership on A and B for three observations")
    for node in journey.nodes.values():
        assert journey.ready(node)


def partition_id(key: str, shift: int) -> int:
    serializer = MsgPackSerializer()
    address = Address(
        key=serializer.encode(key), namespace=serializer.encode("journey")
    )
    token = int.from_bytes(
        hashlib.md5(address.to_bytes(), usedforsecurity=False).digest(), "big"
    )
    return token >> (128 - shift)


def owns_partition(ring: dict, pid: int) -> bool:
    for vnode in ring["vnode_ranges"]:
        arc = vnode["arc"]
        if arc is None:
            continue
        start, end = arc
        if start == end or (start < pid <= end):
            return True
        if start > end and (pid > start or pid <= end):
            return True
    return False


@then("pre-join data including transferred partitions is readable through both nodes")
def transferred(journey: Journey) -> None:
    ring = journey.inspect(journey.nodes["B"])["ring_position"]
    initial = journey.observations["initial-ring"]
    assert initial["owned_partitions"] == initial["total_partitions"]
    moved = [
        key
        for key in journey.references
        if owns_partition(ring, partition_id(key, ring["partition_shift"]))
    ]
    assert (
        moved
    ), "Reference data must include a partition actually transferred from A to B"
    for node in journey.nodes.values():
        for key, value in journey.references.items():
            journey.check(node, key, value)


@when("I write through B, update through A and delete through B")
def mutate(journey: Journey) -> None:
    a, b = journey.nodes["A"], journey.nodes["B"]
    journey.put(b, "written-through-B", "new")
    journey.check(a, "written-through-B", "new")
    journey.put(a, "reference-0", "updated")
    for node in (a, b):
        journey.check(node, "reference-0", "updated")
    tombstone = journey.kv(b, "delete", "reference-1")
    assert tombstone["address"] == {"key": "reference-1", "namespace": "journey"}
    journey.references["reference-0"] = "updated"
    journey.references["reference-1"] = None
    journey.references["written-through-B"] = "new"


@then("both nodes return the exact surviving values and no deleted value")
def surviving(journey: Journey) -> None:
    for node in journey.nodes.values():
        for key, value in journey.references.items():
            journey.check(node, key, value)


@given("a surviving value and a deleted value")
def persisted_values(journey: Journey) -> None:
    a = journey.nodes["A"]
    journey.put(a, "survivor", "durable")
    journey.put(a, "deleted", "must not return")
    journey.kv(a, "delete", "deleted")
    journey.check(a, "deleted", None)
    journey.observations["identity"] = journey.inspect(a)


@when("I stop and restart the node using the same configuration and storage")
def restart(journey: Journey) -> None:
    a = journey.nodes["A"]
    journey.stop(a)
    assert a.process.poll() is not None
    journey.start(a)
    wait_for(lambda: journey.ready(a), "restarted A ready", timeout=30)


@then("the node retains its identity, ring tokens, surviving value and deletion")
def retained(journey: Journey) -> None:
    a = journey.nodes["A"]
    before, after = journey.observations["identity"], journey.inspect(a)
    assert after["node_id"] == before["node_id"] == "A"
    assert after["ring_position"] == before["ring_position"]
    assert after["self_member"]["phase"] == "READY"
    journey.check(a, "survivor", "durable")
    journey.check(a, "deleted", None)


@when("I write explicit string, Unicode, integer, boolean, JSON and binary file values")
def typed(journey: Journey) -> None:
    binary = bytes(range(256)) + b"\x00\xff\r\n" * 64
    file = journey.root / "payload.bin"
    file.write_bytes(binary)
    cases = [
        ("string", "001", "str", "001"),
        (
            "literal",
            "[bold]literal[/bold] :smile:",
            "str",
            "[bold]literal[/bold] :smile:",
        ),
        ("unicode", "Bonjour 世界 👋 é", "str", "Bonjour 世界 👋 é"),
        ("integer", "42", "int", 42),
        ("boolean", "false", "bool", False),
        (
            "json",
            '{"nested":[1,true,null,"é"]}',
            "json",
            {"nested": [1, True, None, "é"]},
        ),
        (
            "binary",
            f"@{file}",
            "bytes",
            {"type": "bytes", "base64": base64.b64encode(binary).decode("ascii")},
        ),
    ]
    for key, raw, kind, expected in cases:
        journey.put(journey.nodes["A"], key, raw, kind=kind, expected=expected)
        journey.references[key] = expected
    journey.observations["binary"] = binary


@then("every typed value is returned exactly with its original type")
def typed_exact(journey: Journey) -> None:
    for key, expected in journey.references.items():
        version = journey.check(journey.nodes["A"], key, expected)
        if key == "binary":
            assert (
                base64.b64decode(version["data"]["base64"], validate=True)
                == journey.observations["binary"]
            )


@when("I write the same key in two namespaces and delete it from one")
def namespaces(journey: Journey) -> None:
    a = journey.nodes["A"]
    journey.put(a, "shared", "first", namespace="tenant-one")
    journey.put(a, "shared", "second", namespace="tenant-two")
    journey.check(a, "shared", "first", namespace="tenant-one")
    journey.check(a, "shared", "second", namespace="tenant-two")
    journey.kv(a, "delete", "shared", namespace="tenant-one")


@then("only that namespace loses its value and the default namespace stays empty")
def isolated(journey: Journey) -> None:
    a = journey.nodes["A"]
    journey.check(a, "shared", None, namespace="tenant-one")
    journey.check(a, "shared", "second", namespace="tenant-two")
    journey.check(a, "shared", None, namespace="default")


@when("I write a value demanding more replicas than the database holds")
def under_replicated(journey: Journey) -> None:
    journey.observations["under-replicated"] = journey.ctl_result(
        journey.nodes["A"],
        "put",
        "fragile",
        "kept",
        "--namespace",
        "journey",
        "--namespace-type",
        "str",
        "--value-type",
        "str",
        "-w",
        "2",
    )


@then("the write fails but the value is readable and the handle resumes it")
def resumed(journey: Journey) -> None:
    node = journey.nodes["A"]
    result = journey.observations["under-replicated"]
    assert result.returncode != 0
    output = result.stdout + result.stderr
    match = re.search(r"tourctl repair (\S+)", output)
    assert match is not None, output

    # The write is never rolled back: the quorum it asked for was not reached,
    # the version it produced still exists.
    journey.check(node, "fragile", "kept")

    outcome = journey.ctl(node, "repair", match.group(1), "--json")
    assert outcome == {
        "issue": "already_durable",
        "replicas": 1,
        "token": match.group(1),
    }, outcome


@when("I start the node with an invalid configuration")
def invalid(journey: Journey) -> None:
    node = journey.nodes["A"]
    original = node.config.read_text(encoding="utf-8")
    assert node.peer in original
    node.config.write_text(
        original.replace(node.peer, "invalid-bind"), encoding="utf-8"
    )
    journey.start(node)
    journey.observations["invalid-exit"] = node.process.wait(timeout=COMMAND_TIMEOUT)


@then("startup fails explicitly and neither endpoint remains listening")
def invalid_failed(journey: Journey) -> None:
    node = journey.nodes["A"]
    assert journey.observations["invalid-exit"] != 0
    logs = "\n".join(
        p.read_text(encoding="utf-8") for p in journey.root.glob("A-*-stderr.log")
    )
    assert "invalid" in logs.lower(), logs
    for address in (node.client, node.peer):
        host, port = address.split(":")
        with socket.socket() as connection:
            connection.settimeout(1)
            assert connection.connect_ex((host, int(port))) != 0
    assert node.process.poll() is not None


@when("I probe the configured node without starting it")
def unreachable(journey: Journey) -> None:
    started = time.monotonic()
    journey.observations["unreachable"] = journey.ctl_result(
        journey.nodes["A"], "probe", "ping", "client"
    )
    journey.observations["elapsed"] = time.monotonic() - started


@then("the command fails within its deadline with an unreachable-node error")
def unreachable_failed(journey: Journey) -> None:
    result = journey.observations["unreachable"]
    assert result.returncode != 0
    assert journey.observations["elapsed"] < COMMAND_TIMEOUT
    assert any(
        word in (result.stdout + result.stderr).lower()
        for word in ("refused", "unable to reach", "unreachable")
    )


@given("a client that trusts the server but is signed by a different authority")
def untrusted(journey: Journey) -> None:
    ca = journey.root / "other-ca"
    journey.generate_ca(ca)
    rogue = journey.client_config(journey.nodes["A"], "untrusted", ca)
    journey.observations["untrusted-config"] = rogue
    # Change only the server trust anchor; retain the other CA's client key/cert.
    trusted_ca = base64.b64encode((journey.ca / "ca-cert.pem").read_bytes()).decode(
        "ascii"
    )
    wrong_ca = base64.b64encode((ca / "ca-cert.pem").read_bytes()).decode("ascii")
    text = rogue.read_text(encoding="utf-8")
    assert wrong_ca in text
    rogue.write_text(text.replace(wrong_ca, trusted_ca), encoding="utf-8")


@when("the untrusted client probes both endpoints")
def rejected_requests(journey: Journey) -> None:
    rogue = journey.observations["untrusted-config"]
    journey.observations["rejections"] = [
        journey.ctl_result(journey.nodes["A"], "probe", "ping", endpoint, config=rogue)
        for endpoint in ("client", "peer")
    ]


@then("both requests fail and the authorized client can still use the database")
def rejected(journey: Journey) -> None:
    for result in journey.observations["rejections"]:
        assert result.returncode != 0
        output = (result.stdout + result.stderr).lower()
        assert any(
            word in output
            for word in ("tls", "ssl", "certificate", "connection", "closed")
        ), output
    a = journey.nodes["A"]
    assert journey.ready(a)
    journey.put(a, "authorized", "still works")
    journey.check(a, "authorized", "still works")
