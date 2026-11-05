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

import time
from dataclasses import asdict
from pathlib import Path
from typing import Literal

from tourclient import ProbeService, client
from tourclient.exception import UnknownServiceError
from tourctl.core.context import load_client_config
from tourctl.core.error import PROBE_ERROR_PROFILE, cli_error
from tourctl.core.render import to_json
from tourlib.models.probe import ProbePong


async def _run_probe_ping(
    endpoint: Literal["client", "peer"],
    *,
    tourconf: str | Path | None,
    context_name: str | None,
) -> tuple[ProbePong, float]:
    async with client(
        "probe",
        endpoint=endpoint,
        tourconf=tourconf,
        context_name=context_name,
    ) as probe_service:
        if not isinstance(probe_service, ProbeService):
            raise UnknownServiceError("Unknown service: probe")
        started = time.perf_counter()
        pong = await probe_service.ping()
        return pong, (time.perf_counter() - started) * 1000


async def _endpoint_address(
    endpoint: Literal["client", "peer"],
    tourconf: str | Path | None,
    context_name: str | None,
) -> str:
    cluster = (await load_client_config(tourconf)).cluster(context_name)
    return cluster.client if endpoint == "client" else cluster.peer


def _format_rtt(rtt_ms: float) -> str:
    return "<1ms" if rtt_ms < 1 else f"{round(rtt_ms)}ms"


async def ping_probe(
    endpoint: Literal["client", "peer"],
    *,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
    as_json: bool = False,
) -> str:
    """Ping the node behind ``endpoint`` and report who answered and how fast."""
    try:
        address = await _endpoint_address(endpoint, tourconf, context_name)
        pong, rtt_ms = await _run_probe_ping(
            endpoint=endpoint,
            tourconf=tourconf,
            context_name=context_name,
        )
    except Exception as exc:
        raise cli_error(exc, context_name, PROBE_ERROR_PROFILE) from None

    if as_json:
        document = asdict(pong) | {
            "address": address,
            "endpoint": endpoint,
            "rtt_ms": round(rtt_ms, 3),
        }
        return to_json(document)
    return f"Pong from {pong.node_id} at {address} in {_format_rtt(rtt_ms)}."
