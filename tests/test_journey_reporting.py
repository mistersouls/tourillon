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

from pathlib import Path

from tests.e2e.journey import Journey, Node
from tests.e2e.reporting import export_journey


def test_export_journey_includes_only_reports_and_runtime_logs(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    journey = Journey(
        workspace,
        {"tourillon": "standalone-node", "tourctl": "installed-client"},
        {"tourillon"},
    )
    journey.nodes["A"] = Node(
        "A", workspace / "node.toml", workspace / "client.toml", "client", "peer"
    )
    journey.durations.append((1.25, "tourctl put example"))
    expected_logs = {
        "command-0001.log": "exit=0\nUnicode: \u4e16\u754c\n",
        "command-0002.log": "timeout=15.0s\n",
        "A-1-stdout.log": "node output\n",
        "A-1-stderr.log": "node diagnostics\n",
    }
    for name, content in expected_logs.items():
        (workspace / name).write_text(content, encoding="utf-8")
    for name in ("node.toml", "client.toml", "private-key.pem", "unrelated.log"):
        (workspace / name).write_text("must not be exported", encoding="utf-8")
    ca = workspace / "ca"
    ca.mkdir()
    (ca / "private-key.pem").write_text("must not be exported", encoding="utf-8")

    report = export_journey(journey, tmp_path / "artifacts", "suite::scenario")

    assert {file.name for file in report.parent.iterdir()} == {
        "report.md",
        *expected_logs,
    }
    for name, content in expected_logs.items():
        assert (report.parent / name).read_text(encoding="utf-8") == content
    text = report.read_text(encoding="utf-8")
    assert "# suite::scenario" in text
    assert "standalone, isolated environment" in text
    assert "installed on PATH" in text
    assert "1.250" in text
    assert "command-0002.log" in text


def test_export_journey_separates_scenarios(tmp_path: Path) -> None:
    journey = Journey(tmp_path, {}, set())
    directory = tmp_path / "artifacts"

    first = export_journey(journey, directory, "suite::first")
    second = export_journey(journey, directory, "suite::second")

    assert first.parent != second.parent
    assert first.is_file()
    assert second.is_file()
