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
"""``tourctl node health``: one node's verdict, for supervision probes.

The output is one status line and at most five reasons on stdout; the exit
code follows the supervision convention: 0 OK, 1 WARNING, 2 CRITICAL and 3
UNKNOWN when no verdict could be obtained.
"""

from dataclasses import dataclass
from pathlib import Path

from tourclient import InspectService, client
from tourclient.exception import UnknownServiceError
from tourctl.core.error import EXIT_UNREACHABLE, INSPECT_ERROR_PROFILE, cli_error
from tourctl.core.render import to_json
from tourlib.models.health import CRITICAL, OK, WARNING, NodeHealth

UNKNOWN = "UNKNOWN"
EXIT_CODES = {OK: 0, WARNING: 1, CRITICAL: 2, UNKNOWN: 3}
MAX_REASONS = 5


@dataclass(frozen=True)
class HealthOutput:
    text: str
    exit_code: int


async def _fetch_health(
    address: str, tourconf: str | Path | None, context_name: str | None
) -> NodeHealth:
    async with client(
        "inspect",
        endpoint="peer",
        bind=address,
        tourconf=tourconf,
        context_name=context_name,
    ) as service:
        if not isinstance(service, InspectService):
            raise UnknownServiceError("Unknown service: inspect")
        return await service.health()


async def node_health(
    address: str,
    *,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> HealthOutput:
    """Fetch ``node.health`` from ADDRESS; any failure is an UNKNOWN verdict."""
    try:
        health = await _fetch_health(address, tourconf, context_name)
    except Exception as exc:
        return unknown(address, exc, context_name, as_json=as_json)
    text = health_json(health) if as_json else render_health(health)
    return HealthOutput(text, EXIT_CODES[health.status])


def render_health(health: NodeHealth) -> str:
    lines = [f"{health.status}: {health.node_id}"]
    reasons = health.reasons
    lines += [f"  {reason.message}" for reason in reasons[:MAX_REASONS]]
    if len(reasons) > MAX_REASONS:
        lines.append(f"  ... and {len(reasons) - MAX_REASONS} more")
    return "\n".join(lines)


def health_json(health: NodeHealth) -> str:
    return to_json(
        {
            "node_id": health.node_id,
            "status": health.status,
            "reasons": [
                {"code": reason.code, "message": reason.message}
                for reason in health.reasons
            ],
        }
    )


def unknown(
    address: str, exc: Exception, context_name: str | None, *, as_json: bool
) -> HealthOutput:
    error = cli_error(exc, context_name, INSPECT_ERROR_PROFILE)
    detail = str(exc) or type(exc).__name__
    if isinstance(exc, OSError):
        detail = exc.strerror or detail
    if error.code == EXIT_UNREACHABLE:
        message = f"unable to reach {address} ({detail})"
    else:
        message = error.message
    if not as_json:
        return HealthOutput(f"{UNKNOWN}: {message}", EXIT_CODES[UNKNOWN])
    document = {
        "node_id": None,
        "status": UNKNOWN,
        "reasons": [{"code": error.error, "message": message}],
    }
    return HealthOutput(to_json(document), EXIT_CODES[UNKNOWN])
