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

import hashlib
import shutil
from pathlib import Path

from tests.e2e.journey import Journey


def export_journey(journey: Journey, directory: Path, scenario: str) -> Path:
    identifier = hashlib.sha256(scenario.encode("utf-8")).hexdigest()[:16]
    destination = directory / identifier
    destination.mkdir(parents=True, exist_ok=True)
    logs = sorted(journey.root.glob("command-*.log"))
    for node in journey.nodes.values():
        logs.extend(journey.root.glob(f"{node.name}-*-stdout.log"))
        logs.extend(journey.root.glob(f"{node.name}-*-stderr.log"))
    for log in logs:
        shutil.copyfile(log, destination / log.name)

    lines = [
        f"# {scenario}",
        "",
        "## Executables",
        "",
    ]
    for name, executable in sorted(journey.executables.items()):
        mode = (
            "standalone, isolated environment"
            if name in journey.standalone_tools
            else "installed on PATH"
        )
        lines.append(f"- {name}: `{executable}` ({mode})")
    lines.extend(
        [
            "",
            "Standalone tools run outside the repository with an empty PATH and "
            "without Python, virtual-environment or dynamic-library search overrides.",
            "",
            f"## Commands ({len(journey.durations)})",
            "",
            "| Duration (s) | Command | Log |",
            "| --- | --- | --- |",
        ]
    )
    for index, (duration, command) in enumerate(journey.durations, start=1):
        filename = f"command-{index:04d}.log"
        lines.append(f"| {duration:.3f} | {command} | [{filename}]({filename}) |")
    lines.extend(["", "## Logs", ""])
    lines.extend(f"- [{log.name}]({log.name})" for log in logs)
    report = destination / "report.md"
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report
