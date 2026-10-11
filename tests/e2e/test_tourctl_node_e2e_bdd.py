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

import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
from pytest_bdd import given, scenario, then, when

pytestmark = [pytest.mark.tourillon, pytest.mark.e2e]

COMMAND_TIMEOUT_SECONDS = 30
STARTUP_TIMEOUT_SECONDS = 30.0
TERMINATION_TIMEOUT_SECONDS = 10


def _closed_local_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.fixture
def node_command_context(tmp_path: Path) -> dict[str, object]:
    tourillon_cli = shutil.which("tourillon")
    tourctl_cli = shutil.which("tourctl")
    if tourillon_cli is None:
        pytest.skip("`tourillon` command is not available in PATH")
    if tourctl_cli is None:
        pytest.skip("`tourctl` command is not available in PATH")

    context = {
        "output_dir": tmp_path,
        "tourillon_cli": tourillon_cli,
        "tourctl_cli": tourctl_cli,
        "result": None,
        "node_process": None,
        "node_id": None,
        "peer_port": None,
        "tourconf_path": None,
    }
    yield context

    process = context["node_process"]
    if isinstance(process, subprocess.Popen) and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)


@scenario("features/tourillon_tourctl_node.feature", "Display help for tourctl node inspect")
def test_tourctl_node_inspect_help_e2e() -> None:
    pass


@scenario("features/tourillon_tourctl_node.feature", "Display help for tourctl node join")
def test_tourctl_node_join_help_e2e() -> None:
    pass


@scenario(
    "features/tourillon_tourctl_node.feature",
    "Start a node with tourillon and inspect it with tourctl",
)
def test_tourillon_start_and_tourctl_node_inspect_success_e2e() -> None:
    pass


@scenario(
    "features/tourillon_tourctl_node.feature",
    "Start a node with tourillon and trigger join with tourctl",
)
def test_tourillon_start_and_tourctl_node_join_success_e2e() -> None:
    pass


@given("a CA has been generated for tourctl node commands")
def given_generated_ca(node_command_context: dict[str, object]) -> None:
    output_dir = node_command_context["output_dir"]
    tourillon_cli = node_command_context["tourillon_cli"]
    assert isinstance(output_dir, Path)
    assert isinstance(tourillon_cli, str)

    subprocess.run(
        [
            tourillon_cli,
            "config",
            "ca",
            "--output-dir",
            str(output_dir),
            "--common-name",
            "tourillon-node-bdd-ca",
            "--organization",
            "Tourillon BDD",
            "--country",
            "FR",
            "--validity-days",
            "5",
            "--key-size",
            "2048",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )


@given("node and client configs have been generated for a running node command target")
def given_running_node_target_configs(node_command_context: dict[str, object]) -> None:
    output_dir = node_command_context["output_dir"]
    tourillon_cli = node_command_context["tourillon_cli"]
    assert isinstance(output_dir, Path)
    assert isinstance(tourillon_cli, str)

    node_id = "node-ctl-bdd"
    client_port = _closed_local_port()
    peer_port = _closed_local_port()
    node_config_path = output_dir / "node.toml"
    tourconf_path = output_dir / "tourconf.toml"
    data_dir = output_dir / "data"

    subprocess.run(
        [
            tourillon_cli,
            "config",
            "node",
            node_id,
            str(data_dir),
            "8Mi",
            str(output_dir / "ca-cert.pem"),
            str(output_dir / "ca-key.pem"),
            "--output-file",
            str(node_config_path),
            "--client-bind",
            f"127.0.0.1:{client_port}",
            "--peer-bind",
            f"127.0.0.1:{peer_port}",
            "--peer-advertise",
            f"127.0.0.1:{peer_port}",
            "--validity-days",
            "5",
            "--key-size",
            "2048",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )

    subprocess.run(
        [
            tourillon_cli,
            "config",
            "client",
            "--output-file",
            str(tourconf_path),
            "--ca-cert",
            str(output_dir / "ca-cert.pem"),
            "--ca-key",
            str(output_dir / "ca-key.pem"),
            "--common-name",
            "tourctl-node-bdd",
            "--client-address",
            f"127.0.0.1:{client_port}",
            "--peer-address",
            f"127.0.0.1:{peer_port}",
            "--validity-days",
            "5",
            "--key-size",
            "2048",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )

    node_command_context["node_config_path"] = node_config_path
    node_command_context["node_id"] = node_id
    node_command_context["peer_port"] = peer_port
    node_command_context["tourconf_path"] = tourconf_path


@given("a tourillon node is running for node commands")
def given_running_node_for_node_commands(node_command_context: dict[str, object]) -> None:
    tourillon_cli = node_command_context["tourillon_cli"]
    node_config_path = node_command_context["node_config_path"]
    peer_port = node_command_context["peer_port"]
    assert isinstance(tourillon_cli, str)
    assert isinstance(node_config_path, Path)
    assert isinstance(peer_port, int)

    process = subprocess.Popen(
        [tourillon_cli, "node", "start", str(node_config_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    node_command_context["node_process"] = process

    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr = ""
            if process.stderr is not None:
                stderr = process.stderr.read()
            raise AssertionError(f"tourillon node exited before node commands: {stderr}")
        try:
            with socket.create_connection(("127.0.0.1", peer_port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)

    raise AssertionError("tourillon peer listener did not become ready in time")


@when("I request help for the tourctl node inspect command")
def when_request_node_inspect_help(node_command_context: dict[str, object]) -> None:
    tourctl_cli = node_command_context["tourctl_cli"]
    assert isinstance(tourctl_cli, str)
    node_command_context["result"] = subprocess.run(
        [tourctl_cli, "node", "inspect", "--help"],
        capture_output=True,
        text=True,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )


@when("I request help for the tourctl node join command")
def when_request_node_join_help(node_command_context: dict[str, object]) -> None:
    tourctl_cli = node_command_context["tourctl_cli"]
    assert isinstance(tourctl_cli, str)
    node_command_context["result"] = subprocess.run(
        [tourctl_cli, "node", "join", "--help"],
        capture_output=True,
        text=True,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )


@when("I run the tourctl node inspect command against peer endpoint")
def when_run_node_inspect(node_command_context: dict[str, object]) -> None:
    tourctl_cli = node_command_context["tourctl_cli"]
    peer_port = node_command_context["peer_port"]
    tourconf_path = node_command_context["tourconf_path"]
    assert isinstance(tourctl_cli, str)
    assert isinstance(peer_port, int)
    assert isinstance(tourconf_path, Path)
    node_command_context["result"] = subprocess.run(
        [
            tourctl_cli,
            "node",
            "inspect",
            f"127.0.0.1:{peer_port}",
            "--tourconf",
            str(tourconf_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )


@when("I run the tourctl node join command against peer endpoint")
def when_run_node_join(node_command_context: dict[str, object]) -> None:
    tourctl_cli = node_command_context["tourctl_cli"]
    peer_port = node_command_context["peer_port"]
    tourconf_path = node_command_context["tourconf_path"]
    assert isinstance(tourctl_cli, str)
    assert isinstance(peer_port, int)
    assert isinstance(tourconf_path, Path)
    node_command_context["result"] = subprocess.run(
        [
            tourctl_cli,
            "node",
            "join",
            f"127.0.0.1:{peer_port}",
            "--seeds",
            "127.0.0.1:65535",
            "--tourconf",
            str(tourconf_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )


@then("the node command returns help text")
def then_node_command_returns_help(node_command_context: dict[str, object]) -> None:
    result = node_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0
    assert "usage:" in result.stdout.lower()


@then("the node inspect command exits with zero code")
def then_node_inspect_succeeds(node_command_context: dict[str, object]) -> None:
    result = node_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0


@then("the node inspect output includes the running node id")
def then_node_inspect_output_has_node_id(node_command_context: dict[str, object]) -> None:
    result = node_command_context["result"]
    node_id = node_command_context["node_id"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert isinstance(node_id, str)
    output = result.stdout
    assert f"({node_id})" in output
    assert "Snapshot at:" in output
    assert "Status" in output
    assert "Identity" in output
    assert "Ring position" in output
    assert "Cluster membership" in output
    assert "Gossip health" in output
    assert "Notes" in output


@then("the node join command exits with zero code")
def then_node_join_succeeds(node_command_context: dict[str, object]) -> None:
    result = node_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0


@then("the node join output confirms the operation was triggered")
def then_node_join_output_reports_triggered(node_command_context: dict[str, object]) -> None:
    result = node_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert "join operation is successful triggered" in result.stdout.lower()
