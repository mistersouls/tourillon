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
from dataclasses import asdict
from pathlib import Path
from typing import Literal, NoReturn

from tourclient import ProbeService, client
from tourclient.exception import UnknownServiceError
from tourctl.core.error import PROBE_ERROR_PROFILE, handle_error


def _raise_user_error(message: str) -> NoReturn:
    raise SystemExit(message)


def _probe_error_message(exc: Exception, context_name: str | None) -> str:
    if isinstance(exc, UnknownServiceError):
        return str(exc)
    return handle_error(exc, context_name, PROBE_ERROR_PROFILE)


async def _run_probe_ping(
    endpoint: Literal["client", "peer"],
    *,
    tourconf: str | Path | None,
    context_name: str | None,
):
    async with client(
        "probe",
        endpoint=endpoint,
        tourconf=tourconf,
        context_name=context_name,
    ) as probe_service:
        if not isinstance(probe_service, ProbeService):
            raise UnknownServiceError("Unknown service: probe")
        return await probe_service.ping()


async def ping_probe(
    endpoint: Literal["client", "peer"],
    *,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> str:
    """Run a probe.ping request through tourclient and return a JSON document."""
    try:
        probe_pong = await _run_probe_ping(
            endpoint=endpoint,
            tourconf=tourconf,
            context_name=context_name,
        )
    except Exception as exc:
        _raise_user_error(_probe_error_message(exc, context_name))

    return json.dumps(asdict(probe_pong), ensure_ascii=False, indent=2, sort_keys=True)


