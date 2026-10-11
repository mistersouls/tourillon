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
def ca_command_context(tmp_path: Path) -> dict[str, object]:
    cli_path = shutil.which("tourillon")
    if cli_path is None:
        pytest.skip("`tourillon` command is not available in PATH")
    return {"output_dir": tmp_path, "argv": [], "cli_path": cli_path, "result": None}


@scenario("features/tourillon_config_ca.feature", "Generate CA files in an empty output directory")
def test_config_ca_generates_files_e2e() -> None:
    pass


@scenario("features/tourillon_config_ca.feature", "Refuse overwrite when files already exist")
def test_config_ca_refuses_overwrite_e2e() -> None:
    pass


@given("a clean CA output directory")
def given_clean_output_dir(ca_command_context: dict[str, object]) -> None:
    output_dir = ca_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    assert output_dir.exists()


@given("valid CA command options")
def given_valid_ca_options(ca_command_context: dict[str, object]) -> None:
    output_dir = ca_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    ca_command_context["argv"] = [
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
    ]


@given("an output directory with existing CA files")
def given_existing_ca_files(ca_command_context: dict[str, object]) -> None:
    output_dir = ca_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    (output_dir / "ca-cert.pem").write_text("already-there", encoding="utf-8")
    (output_dir / "ca-key.pem").write_text("already-there", encoding="utf-8")
    ca_command_context["argv"] = ["config", "ca", "--output-dir", str(output_dir)]


@when("I run the tourillon config ca command")
def when_run_config_ca(ca_command_context: dict[str, object]) -> None:
    argv = ca_command_context["argv"]
    cli_path = ca_command_context["cli_path"]
    assert isinstance(argv, list)
    assert isinstance(cli_path, str)
    ca_command_context["result"] = subprocess.run(
        [cli_path, *argv],
        capture_output=True,
        text=True,
        check=False,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )


@then("the command reports generated CA files")
def then_command_reports_generated_files(ca_command_context: dict[str, object]) -> None:
    result = ca_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0
    assert "Generated mTLS CA files" in result.stdout


@then("the generated CA certificate exists")
def then_generated_cert_exists(ca_command_context: dict[str, object]) -> None:
    output_dir = ca_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    assert (output_dir / "ca-cert.pem").exists()


@then("the generated CA private key exists")
def then_generated_key_exists(ca_command_context: dict[str, object]) -> None:
    output_dir = ca_command_context["output_dir"]
    assert isinstance(output_dir, Path)
    assert (output_dir / "ca-key.pem").exists()


@then("the command fails with a file exists error")
def then_command_fails_with_file_exists(ca_command_context: dict[str, object]) -> None:
    result = ca_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode != 0
    assert "FileExistsError" in result.stderr


@then("the error message mentions overwrite refusal")
def then_error_mentions_overwrite_refusal(ca_command_context: dict[str, object]) -> None:
    result = ca_command_context["result"]
    assert isinstance(result, subprocess.CompletedProcess)
    assert "refusing to overwrite" in result.stderr



