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
import subprocess
from pathlib import Path

import pytest
from pytest_bdd import given, scenario, then, when

pytestmark = [pytest.mark.tourillon, pytest.mark.e2e]

COMMAND_TIMEOUT_SECONDS = 30


@pytest.fixture
def client_command_context(tmp_path: Path) -> dict[str, object]:
    command = "tourillon"
    cli_path = shutil.which(command)
    if cli_path is None:
        pytest.skip("`tourillon` command is not available in PATH")
    return {"output_dir": tmp_path, "argv": [], "cli_path": cli_path, "result": None}


@scenario(
    "features/tourillon_config_client.feature",
    "Generate client config in an empty output directory",
)
def test_config_client_generates_files_e2e() -> None:
    pass


@scenario(
    "features/tourillon_config_client.feature",
    "Refuse overwrite when client config already exists",
)
def test_config_client_refuses_overwrite_e2e() -> None:
    pass


@given("a CA has been generated for the client command")
def given_generated_client_ca(client_command_context: dict[str, object]) -> None:
    output_dir = client_command_context["output_dir"]
    cli_path = client_command_context["cli_path"]
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


@given("valid client command options")
def given_valid_client_command_options(client_command_context: dict[str, object]) -> None:
    output_dir = client_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    client_command_context["argv"] = [
        "config",
        "client",
        "--output-file",
        str(output_dir / "client.toml"),
        "--ca-cert",
        str(output_dir / "ca-cert.pem"),
        "--ca-key",
        str(output_dir / "ca-key.pem"),
        "--common-name",
        "tourillon-bdd-client",
        "--validity-days",
        "5",
        "--key-size",
        "2048",
    ]


@given("a client config file already exists at the output path")
def given_existing_client_config(client_command_context: dict[str, object]) -> None:
    output_dir = client_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    (output_dir / "client.toml").write_text("already-there", encoding="utf-8")
    client_command_context["argv"] = [
        "config",
        "client",
        "--output-file",
        str(output_dir / "client.toml"),
        "--ca-cert",
        str(output_dir / "ca-cert.pem"),
        "--ca-key",
        str(output_dir / "ca-key.pem"),
        "--common-name",
        "tourillon-bdd-client",
    ]


@when("I run the tourillon config client command")
def when_run_config_client(client_command_context: dict[str, object]) -> None:
    argv = client_command_context["argv"]
    cli_path = client_command_context["cli_path"]
    assert isinstance(argv, list)
    assert isinstance(cli_path, str)
    client_command_context["result"] = subprocess.run(
        [cli_path, *argv],
        capture_output=True,
        text=True,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )


@then("the command reports a generated client config file")
def then_command_reports_generated_client_config(
    client_command_context: dict[str, object],
) -> None:
    result = client_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0
    assert "Generated client config file" in result.stdout


@then("the generated client config file exists")
def then_generated_client_config_exists(
    client_command_context: dict[str, object],
) -> None:
    output_dir = client_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    assert (output_dir / "client.toml").exists()


@then("the command fails with a file exists error")
def then_command_fails_with_file_exists(client_command_context: dict[str, object]) -> None:
    result = client_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode != 0
    assert "FileExistsError" in result.stderr


@then("the error message mentions overwrite refusal")
def then_error_mentions_overwrite_refusal(client_command_context: dict[str, object]) -> None:
    result = client_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert "refusing to overwrite" in result.stderr


