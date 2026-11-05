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

"""Text building blocks shared by every tourctl view."""

import json
import time
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

from tourlib.models.ts import HLCTimestamp

_UNITS = ("KiB", "MiB", "GiB", "TiB")
_SPANS = ((86400, "d", 3600, "h"), (3600, "h", 60, "m"), (60, "m", 1, "s"))


def to_json(document: Any) -> str:
    return json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True)


def fields(pairs: Iterable[tuple[str, str]]) -> str:
    """Align ``Label: value`` lines on the longest label."""
    rows = list(pairs)
    width = max((len(label) for label, _ in rows), default=0) + 2
    return "\n".join(f"{label + ':':<{width}}{value}".rstrip() for label, value in rows)


def block(label: str, text: str) -> str:
    """Render a multi-line value under its label, indented by two spaces."""
    body = "\n".join(f"  {line}".rstrip() for line in text.splitlines())
    return f"{label}:\n{body}"


def table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> str:
    """Columns padded to their widest cell, 3 spaces apart; ``-`` for no value."""
    cells = [list(headers)] + [[_cell(value) for value in row] for row in rows]
    widths = [max(len(row[i]) for row in cells) for i in range(len(headers))]
    return "\n".join(
        "   ".join(
            cell.ljust(width) for cell, width in zip(row, widths, strict=True)
        ).rstrip()
        for row in cells
    )


def _cell(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def messages(lines: Iterable[str]) -> str:
    return block("Messages", "\n".join(lines))


def format_wall(wall_ms: int) -> str:
    moment = datetime.fromtimestamp(wall_ms / 1000, tz=UTC)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def format_ts(ts: HLCTimestamp) -> str:
    return f"{format_wall(ts.wall_ms)} c={ts.counter} {ts.node_id}"


def ts_to_json(ts: HLCTimestamp) -> dict[str, Any]:
    return {
        "counter": ts.counter,
        "node_id": ts.node_id,
        "wall": format_wall(ts.wall_ms),
        "wall_ms": ts.wall_ms,
    }


def format_duration(seconds: float) -> str:
    """Two units at most: ``45s``, ``4m``, ``2m10s``, ``3h5m``, ``2d4h``."""
    total = max(0, int(seconds))
    for size, unit, rest_size, rest_unit in _SPANS:
        if total >= size:
            rest = (total % size) // rest_size
            head = f"{total // size}{unit}"
            return f"{head}{rest}{rest_unit}" if rest else head
    return f"{total}s"


def format_since(wall_ms: int, now_ms: int | None = None) -> str:
    now = int(time.time() * 1000) if now_ms is None else now_ms
    delta = (now - wall_ms) / 1000
    if delta >= 0:
        return f"{format_duration(delta)} ago"
    return f"in {format_duration(-delta)}"


def format_size(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    value = float(size)
    for unit in _UNITS:
        value /= 1024
        if value < 1024 or unit == _UNITS[-1]:
            break
    return f"{value:.1f} {unit}"
