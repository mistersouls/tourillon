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
import shutil
import subprocess
from pathlib import Path

import pytest
from pytest_bdd import given, scenario, then, when

from tourillon.core.config import TourillonConfig
from tourlib.adapter import TomlPersistence

pytestmark = [pytest.mark.tourillon, pytest.mark.e2e]

COMMAND_TIMEOUT_SECONDS = 30


@pytest.fixture
def node_command_context(tmp_path: Path) -> dict[str, object]:
    cli_path = shutil.which("tourillon")
    if cli_path is None:
        pytest.skip("`tourillon` command is not available in PATH")
    return {"output_dir": tmp_path, "argv": [], "cli_path": cli_path, "result": None}


@scenario(
    "features/tourillon_config_node.feature",
    "Generate node config in an empty output directory",
)
def test_config_node_generates_file_e2e() -> None:
    pass


@scenario(
    "features/tourillon_config_node.feature",
    "Refuse overwrite when node config already exists",
)
def test_config_node_refuses_overwrite_e2e() -> None:
    pass


@scenario(
    "features/tourillon_config_node.feature",
    "Fail when CA certificate is missing",
)
def test_config_node_missing_ca_certificate_e2e() -> None:
    pass


@given("a CA has been generated in a temp directory")
def given_generated_ca(node_command_context: dict[str, object]) -> None:
    output_dir = node_command_context["output_dir"]
    cli_path = node_command_context["cli_path"]
    assert isinstance(output_dir, Path)
    assert isinstance(cli_path, str)
    subprocess.run(
        [
            cli_path,
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


@given("valid node command options")
def given_valid_node_command_options(node_command_context: dict[str, object]) -> None:
    output_dir = node_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    data_dir = output_dir / "data"
    node_command_context["argv"] = [
        "config",
        "node",
        "node-bdd-1",
        "--output-file",
        str(output_dir / "node.toml"),
        "--ca-cert",
        str(output_dir / "ca-cert.pem"),
        "--ca-key",
        str(output_dir / "ca-key.pem"),
        "--data-dir",
        str(data_dir),
        "--group-size",
        "8Mi",
        "--client-bind",
        "127.0.0.1:7100",
        "--peer-bind",
        "127.0.0.1:7101",
        "--peer-advertise",
        "192.0.2.10:7101",
        "--node-size",
        "L",
        "--seeds",
        "seed-a:9001",
        "--seeds",
        "seed-b:9001",
        "--validity-days",
        "5",
        "--key-size",
        "2048",
    ]


@given("a node config file already exists at the output path")
def given_existing_node_config(node_command_context: dict[str, object]) -> None:
    output_dir = node_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    data_dir = output_dir / "data"
    (output_dir / "node.toml").write_text("already-there", encoding="utf-8")
    node_command_context["argv"] = [
        "config",
        "node",
        "node-bdd-1",
        "--output-file",
        str(output_dir / "node.toml"),
        "--ca-cert",
        str(output_dir / "ca-cert.pem"),
        "--ca-key",
        str(output_dir / "ca-key.pem"),
        "--data-dir",
        str(data_dir),
        "--group-size",
        "8Mi",
    ]


@given("the CA certificate file has been deleted")
def given_deleted_ca_certificate(node_command_context: dict[str, object]) -> None:
    output_dir = node_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    data_dir = output_dir / "data"
    (output_dir / "ca-cert.pem").unlink()
    node_command_context["argv"] = [
        "config",
        "node",
        "node-bdd-1",
        "--output-file",
        str(output_dir / "node.toml"),
        "--ca-cert",
        str(output_dir / "ca-cert.pem"),
        "--ca-key",
        str(output_dir / "ca-key.pem"),
        "--data-dir",
        str(data_dir),
        "--group-size",
        "8Mi",
    ]


@when("I run the tourillon config node command")
def when_run_config_node(node_command_context: dict[str, object]) -> None:
    argv = node_command_context["argv"]
    cli_path = node_command_context["cli_path"]
    assert isinstance(argv, list)
    assert isinstance(cli_path, str)
    node_command_context["result"] = subprocess.run(
        [cli_path, *argv],
        capture_output=True,
        text=True,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )


@then("the command reports a generated node config file")
def then_command_reports_generated_node_config(
    node_command_context: dict[str, object],
) -> None:
    result = node_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0
    assert "Generated node config file" in result.stdout


@then("the generated node config file exists")
def then_generated_node_config_exists(node_command_context: dict[str, object]) -> None:
    output_dir = node_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    assert (output_dir / "node.toml").exists()


@then("the generated node config uses the requested endpoints and seeds")
def then_generated_node_config_uses_requested_endpoints(
    node_command_context: dict[str, object],
) -> None:
    output_dir = node_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    config = TourillonConfig.from_document(
        asyncio.run(TomlPersistence().load(output_dir / "node.toml"))
    )
    assert config.server_client.bind == "127.0.0.1:7100"
    assert config.server_peer.bind == "127.0.0.1:7101"
    assert config.server_peer.advertise == "192.0.2.10:7101"
    assert config.node.size.value == "L"
    assert config.node.seeds == ["seed-a:9001", "seed-b:9001"]
    assert config.storage.data_dir == str(output_dir / "data")


@then("the command fails with a file exists error")
def then_command_fails_with_file_exists(
    node_command_context: dict[str, object],
) -> None:
    result = node_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode != 0
    assert "FileExistsError" in result.stderr


@then("the error message mentions overwrite refusal")
def then_error_mentions_overwrite_refusal(
    node_command_context: dict[str, object],
) -> None:
    result = node_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert "refusing to overwrite" in result.stderr


@then("the command exits with a non-zero code")
def then_command_exits_non_zero(node_command_context: dict[str, object]) -> None:
    result = node_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode != 0
