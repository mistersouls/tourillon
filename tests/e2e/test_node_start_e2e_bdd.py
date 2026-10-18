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

import asyncio
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
from pytest_bdd import given, scenario, then, when

from tourillon.core.config import TourillonConfig
from tourlib.adapter import TomlPersistence

pytestmark = [pytest.mark.tourillon, pytest.mark.e2e]

COMMAND_TIMEOUT_SECONDS = 30
STARTUP_TIMEOUT_SECONDS = 30.0
TERMINATION_TIMEOUT_SECONDS = 10


@pytest.fixture
def node_start_context(tmp_path: Path) -> dict[str, object]:
    return {
        "output_dir": tmp_path,
        "argv": [],
        "result": None,
        "config_path": None,
        "node_id": None,
        "client_port": None,
        "stdout": "",
        "startup_returncode": None,
        "startup_stderr": "",
    }


@scenario("features/tourillon_node_start.feature", "Display help for node start")
def test_node_start_help_e2e() -> None:
    pass


@scenario("features/tourillon_node_start.feature", "Require config for node start")
def test_node_start_requires_config_e2e() -> None:
    pass


@scenario("features/tourillon_node_start.feature", "Start node from a generated config file")
def test_node_start_from_generated_config_e2e() -> None:
    pass


@given("a CA has been generated for the node start command")
def given_generated_ca(node_start_context: dict[str, object]) -> None:
    output_dir = node_start_context["output_dir"]
    assert isinstance(output_dir, Path)
    subprocess.run(
        [
            sys.executable,
            "-u",
            "-m",
            "tourillon.bootstrap.main",
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


@given("a node config file has been generated for node start with custom endpoints")
def given_generated_node_config(node_start_context: dict[str, object]) -> None:
    output_dir = node_start_context["output_dir"]
    assert isinstance(output_dir, Path)
    config_path = output_dir / "node.toml"
    data_dir = output_dir / "data"
    subprocess.run(
        [
            sys.executable,
            "-u",
            "-m",
            "tourillon.bootstrap.main",
            "config",
            "node",
            "node-start-bdd",
            "--output-file",
            str(config_path),
            "--ca-cert",
            str(output_dir / "ca-cert.pem"),
            "--ca-key",
            str(output_dir / "ca-key.pem"),
            "--data-dir",
            str(data_dir),
            "--group-size",
            "8Mi",
            "--client-bind",
            "127.0.0.1:7200",
            "--peer-bind",
            "127.0.0.1:7201",
            "--peer-advertise",
            "192.0.2.11:7201",
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
    node_start_context["config_path"] = config_path
    node_start_context["argv"] = ["node", "start", str(config_path)]
    node_start_context["node_id"] = "node-start-bdd"
    node_start_context["client_port"] = 7200


def _run_cli(argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-u", "-m", "tourillon.bootstrap.main", *argv],
        capture_output=True,
        text=True,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )


@when("I request help for the tourillon node start command")
def when_request_help(node_start_context: dict[str, object]) -> None:
    node_start_context["result"] = _run_cli(["node", "start", "--help"])


@when("I run the tourillon node start command without a config file")
def when_run_without_config(node_start_context: dict[str, object]) -> None:
    node_start_context["result"] = _run_cli(["node", "start"])


@when("I run the tourillon node start command")
def when_run_node_start_command(
    node_start_context: dict[str, object],
) -> None:
    argv = node_start_context["argv"]
    node_id = node_start_context["node_id"]
    client_port = node_start_context["client_port"]
    assert isinstance(argv, list)
    assert isinstance(node_id, str)
    assert isinstance(client_port, int)
    process = subprocess.Popen(
        [sys.executable, "-u", "-m", "tourillon.bootstrap.main", *argv],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        try:
            with socket.create_connection(("127.0.0.1", client_port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.2)
    else:
        process.terminate()
        try:
            _, stderr = process.communicate(timeout=TERMINATION_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            _, stderr = process.communicate(timeout=TERMINATION_TIMEOUT_SECONDS)
        raise AssertionError(
            f"node '{node_id}' did not become ready in time: {stderr}"
        )

    process.terminate()
    try:
        stdout, stderr = process.communicate(timeout=TERMINATION_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate(timeout=TERMINATION_TIMEOUT_SECONDS)
    node_start_context["stdout"] = stdout
    node_start_context["startup_returncode"] = process.returncode
    node_start_context["startup_stderr"] = stderr


@then("the command returns help text")
def then_command_returns_help_text(node_start_context: dict[str, object]) -> None:
    result = node_start_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0
    assert "usage:" in result.stdout.lower()


@then("the command fails because config is missing")
def then_command_fails_because_config_missing(node_start_context: dict[str, object]) -> None:
    result = node_start_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode != 0
    assert "config" in result.stderr.lower()


@then("the command reports node startup")
def then_command_reports_node_startup(node_start_context: dict[str, object]) -> None:
    stderr = node_start_context["startup_stderr"]
    node_id = node_start_context["node_id"]
    returncode = node_start_context["startup_returncode"]
    assert isinstance(stderr, str)
    assert isinstance(node_id, str)
    assert isinstance(returncode, int)
    assert f"Manager started for node '{node_id}'" in stderr


@then("the node process is terminated after startup")
def then_node_process_is_terminated(node_start_context: dict[str, object]) -> None:
    returncode = node_start_context["startup_returncode"]
    stderr = node_start_context["startup_stderr"]
    assert isinstance(returncode, int)
    assert returncode is not None
    assert isinstance(stderr, str)


@then("the generated node config preserves the requested endpoints")
def then_generated_node_config_preserves_requested_endpoints(
    node_start_context: dict[str, object],
) -> None:
    config_path = node_start_context["config_path"]
    assert isinstance(config_path, Path)
    config = TourillonConfig.from_document(
        asyncio.run(TomlPersistence().load(config_path))
    )
    output_dir = node_start_context["output_dir"]
    assert isinstance(output_dir, Path)
    assert config.node.id == "node-start-bdd"
    assert config.storage.data_dir == str(output_dir / "data")
    assert config.server_client.bind == "127.0.0.1:7200"
    assert config.server_peer.bind == "127.0.0.1:7201"
    assert config.server_peer.advertise == "192.0.2.11:7201"
