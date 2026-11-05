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

import json
import tomllib
from pathlib import Path

import pytest

from tourctl.bootstrap.main import main
from tourctl.core.config import config_use_context, config_view
from tourlib.adapter import TomlPersistence

_SECRETS = ("Q0EtU0VDUkVU", "Q0VSVC1TRUNSRVQ=", "S0VZLVNFQ1JFVA==")


def _document() -> dict:
    return {
        "version": "1",
        "current-context": "souls",
        "contexts": {
            "souls": {"cluster": "local", "namespace": "toto", "auth": "local-admin"},
            "prod": {"cluster": "prod-eu", "namespace": "default", "auth": "prod"},
        },
        "clusters": {
            "local": {"client": "127.0.0.1:7000", "peer": "127.0.0.1:7001"},
            "prod-eu": {"client": "10.0.0.4:7000", "peer": "10.0.0.4:7001"},
        },
        "auth": {
            name: {"ca_data": _SECRETS[0], "cert_data": _SECRETS[1], "key_data": key}
            for name, key in (("local-admin", _SECRETS[2]), ("prod", _SECRETS[2]))
        },
    }


@pytest.fixture
async def tourconf(tmp_path: Path) -> Path:
    path = tmp_path / "tourconf.toml"
    await TomlPersistence().write(path, _document())
    return path


def _assert_no_secret(text: str) -> None:
    for marker in (*_SECRETS, "ca_data", "cert_data", "key_data"):
        assert marker not in text


@pytest.mark.tourctl
async def test_config_view_lists_every_context_as_a_table(tourconf: Path) -> None:
    text = await config_view(tourconf=tourconf)

    assert text == (
        f"File:            {tourconf}\n"
        "Current context: souls\n"
        "\n"
        "CONTEXT   CLUSTER   CLIENT           PEER             NAMESPACE   AUTH\n"
        "souls     local     127.0.0.1:7000   127.0.0.1:7001   toto        local-admin\n"
        "prod      prod-eu   10.0.0.4:7000    10.0.0.4:7001    default     prod"
    )
    _assert_no_secret(text)


@pytest.mark.tourctl
async def test_config_view_json_never_contains_tls_material(tourconf: Path) -> None:
    text = await config_view(tourconf=tourconf, as_json=True)

    _assert_no_secret(text)
    document = json.loads(text)
    assert document["file"] == str(tourconf)
    assert document["current_context"] == "souls"
    assert document["contexts"][1] == {
        "auth": "prod",
        "client": "10.0.0.4:7000",
        "cluster": "prod-eu",
        "name": "prod",
        "namespace": "default",
        "peer": "10.0.0.4:7001",
    }


@pytest.mark.tourctl
async def test_config_view_resolves_the_tourconf_env_variable(
    tourconf: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TOURCONF", str(tourconf))

    assert (await config_view()).startswith(f"File:            {tourconf}\n")


@pytest.mark.tourctl
def test_config_view_reports_a_missing_file_with_code_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["config", "view", "--tourconf", str(tmp_path / "none.toml")])

    assert exc_info.value.code == 2
    assert capsys.readouterr().err.startswith(
        "Error: Unable to load the Tourillon client config"
    )


@pytest.mark.tourctl
def test_config_view_reports_an_invalid_file_as_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "bad.toml"
    path.write_text('version = "1"\n', encoding="utf-8")

    with pytest.raises(SystemExit) as exc_info:
        main(["config", "view", "--tourconf", str(path), "--json"])

    captured = capsys.readouterr()
    assert exc_info.value.code == 2
    assert captured.out == ""
    assert json.loads(captured.err)["error"]["code"] == "invalid_config"


@pytest.mark.tourctl
def test_main_config_view_prints_the_table(
    tourconf: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["config", "view", "--tourconf", str(tourconf)])

    assert exc_info.value.code == 0

    out = capsys.readouterr().out
    assert "Current context: souls\n" in out
    assert out.endswith("10.0.0.4:7001    default     prod\n")


@pytest.mark.tourctl
async def test_use_context_rewrites_only_the_current_context(tourconf: Path) -> None:
    text = await config_use_context("prod", tourconf=tourconf)

    assert text == "Switched to context prod (cluster prod-eu, namespace default)."
    expected = _document() | {"current-context": "prod"}
    assert tomllib.loads(tourconf.read_text(encoding="utf-8")) == expected


@pytest.mark.tourctl
def test_main_use_context_rejects_an_unknown_context(
    tourconf: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    before = tourconf.read_bytes()

    with pytest.raises(SystemExit) as exc_info:
        main(["config", "use-context", "nope", "--tourconf", str(tourconf)])

    assert exc_info.value.code == 2
    assert capsys.readouterr().err == (
        "Error: unknown context nope. Known: souls, prod.\n"
    )
    assert tourconf.read_bytes() == before


@pytest.mark.tourctl
def test_main_use_context_prints_the_switch(
    tourconf: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["config", "use-context", "prod", "--tourconf", str(tourconf)])

    assert exc_info.value.code == 0

    assert capsys.readouterr().out == (
        "Switched to context prod (cluster prod-eu, namespace default).\n"
    )


@pytest.mark.tourctl
async def test_use_context_reports_a_missing_file(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc_info:
        await config_use_context("prod", tourconf=tmp_path / "none.toml")

    assert exc_info.value.code == 2
