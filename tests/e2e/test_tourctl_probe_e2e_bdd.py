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
def probe_command_context(tmp_path: Path) -> dict[str, object]:
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
        "argv": [],
        "result": None,
        "node_process": None,
        "node_config_path": None,
        "node_id": None,
        "client_port": None,
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


@scenario(
    "features/tourillon_tourctl_probe.feature", "Display help for tourctl probe ping"
)
def test_tourctl_probe_ping_help_e2e() -> None:
    pass


@scenario(
    "features/tourillon_tourctl_probe.feature",
    "Generate client config with tourillon and probe with tourctl",
)
def test_tourillon_tourctl_probe_ping_unreachable_e2e() -> None:
    pass


@scenario(
    "features/tourillon_tourctl_probe.feature",
    "Start a node with tourillon and probe it with tourctl",
)
def test_tourillon_start_and_tourctl_probe_success_e2e() -> None:
    pass


@scenario(
    "features/tourillon_tourctl_probe.feature",
    "Start a node with tourillon and probe peer endpoint with tourctl",
)
def test_tourillon_start_and_tourctl_probe_peer_success_e2e() -> None:
    pass


@given("a CA has been generated for tourctl probe")
def given_generated_ca(probe_command_context: dict[str, object]) -> None:
    output_dir = probe_command_context["output_dir"]
    tourillon_cli = probe_command_context["tourillon_cli"]
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
            "tourillon-bdd-ca",
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


@given("a client config has been generated for tourctl probe")
def given_generated_client_config(probe_command_context: dict[str, object]) -> None:
    output_dir = probe_command_context["output_dir"]
    tourillon_cli = probe_command_context["tourillon_cli"]
    assert isinstance(output_dir, Path)
    assert isinstance(tourillon_cli, str)

    closed_port = _closed_local_port()
    tourconf_path = output_dir / "tourconf.toml"
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
            "tourctl-probe-bdd",
            "--client-address",
            f"127.0.0.1:{closed_port}",
            "--peer-address",
            "127.0.0.1:9001",
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

    probe_command_context["argv"] = [
        "probe",
        "ping",
        "client",
        "--tourconf",
        str(tourconf_path),
    ]


@given("node and client configs have been generated for a running probe target")
def given_running_probe_target_configs(
    probe_command_context: dict[str, object],
) -> None:
    output_dir = probe_command_context["output_dir"]
    tourillon_cli = probe_command_context["tourillon_cli"]
    assert isinstance(output_dir, Path)
    assert isinstance(tourillon_cli, str)

    node_id = "node-probe-bdd"
    client_port = _closed_local_port()
    peer_port = _closed_local_port()
    node_config_path = output_dir / "node.toml"
    tourconf_path = output_dir / "tourconf-live.toml"
    data_dir = output_dir / "data"

    subprocess.run(
        [
            tourillon_cli,
            "config",
            "node",
            node_id,
            "--output-file",
            str(node_config_path),
            "--ca-cert",
            str(output_dir / "ca-cert.pem"),
            "--ca-key",
            str(output_dir / "ca-key.pem"),
            "--data-dir",
            str(data_dir),
            "--group-size",
            "8Mi",
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
            "tourctl-probe-bdd-live",
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

    probe_command_context["argv"] = [
        "probe",
        "ping",
        "client",
        "--tourconf",
        str(tourconf_path),
    ]
    probe_command_context["node_config_path"] = node_config_path
    probe_command_context["node_id"] = node_id
    probe_command_context["client_port"] = client_port
    probe_command_context["tourconf_path"] = tourconf_path


@given("a tourillon node is running for probe")
def given_running_node_for_probe(probe_command_context: dict[str, object]) -> None:
    tourillon_cli = probe_command_context["tourillon_cli"]
    node_config_path = probe_command_context["node_config_path"]
    client_port = probe_command_context["client_port"]
    assert isinstance(tourillon_cli, str)
    assert isinstance(node_config_path, Path)
    assert isinstance(client_port, int)

    process = subprocess.Popen(
        [tourillon_cli, "node", "start", str(node_config_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    probe_command_context["node_process"] = process

    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr = ""
            if process.stderr is not None:
                stderr = process.stderr.read()
            raise AssertionError(f"tourillon node exited before probe: {stderr}")
        try:
            with socket.create_connection(("127.0.0.1", client_port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)

    raise AssertionError("tourillon client listener did not become ready in time")


@when("I request help for the tourctl probe ping command")
def when_request_probe_help(probe_command_context: dict[str, object]) -> None:
    tourctl_cli = probe_command_context["tourctl_cli"]
    assert isinstance(tourctl_cli, str)
    probe_command_context["result"] = subprocess.run(
        [tourctl_cli, "probe", "ping", "--help"],
        capture_output=True,
        text=True,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )


@when("I run the tourctl probe ping command against client endpoint")
def when_run_probe_ping(probe_command_context: dict[str, object]) -> None:
    tourctl_cli = probe_command_context["tourctl_cli"]
    argv = probe_command_context["argv"]
    assert isinstance(tourctl_cli, str)
    assert isinstance(argv, list)
    probe_command_context["result"] = subprocess.run(
        [tourctl_cli, *argv],
        capture_output=True,
        text=True,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )


@when("I run the tourctl probe ping command against peer endpoint")
def when_run_probe_ping_peer(probe_command_context: dict[str, object]) -> None:
    tourctl_cli = probe_command_context["tourctl_cli"]
    tourconf_path = probe_command_context["tourconf_path"]
    assert isinstance(tourctl_cli, str)
    assert isinstance(tourconf_path, Path)
    probe_command_context["result"] = subprocess.run(
        [tourctl_cli, "probe", "ping", "peer", "--tourconf", str(tourconf_path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )


@then("the tourctl command returns help text")
def then_tourctl_help_is_displayed(probe_command_context: dict[str, object]) -> None:
    result = probe_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0
    assert "usage:" in result.stdout.lower()


@then("the probe command exits with a non-zero code")
def then_probe_fails(probe_command_context: dict[str, object]) -> None:
    result = probe_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode != 0


@then("the probe error reports an unreachable node")
def then_probe_reports_unreachable_node(
    probe_command_context: dict[str, object],
) -> None:
    result = probe_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    output = f"{result.stdout}\n{result.stderr}".lower()
    assert "refused" in output or "unable to reach" in output


@then("the probe command exits with zero code")
def then_probe_succeeds(probe_command_context: dict[str, object]) -> None:
    result = probe_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0


@then("the probe output includes the running node id")
def then_probe_output_has_node_id(probe_command_context: dict[str, object]) -> None:
    result = probe_command_context["result"]
    node_id = probe_command_context["node_id"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert isinstance(node_id, str)
    assert f'"node_id": "{node_id}"' in result.stdout
