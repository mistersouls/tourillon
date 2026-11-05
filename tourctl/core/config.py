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

"""Local views and edits of the tourctl client config (no network call)."""

from pathlib import Path
from typing import Any

from tourclient.core.helpers import resolve_tourconf_path
from tourctl.core.context import load_client_config
from tourctl.core.error import EXIT_INVALID, CliError, cli_error
from tourctl.core.render import fields, table, to_json
from tourlib.adapter import TomlPersistence
from tourlib.client_config import ClientConfig

_HEADERS = ("CONTEXT", "CLUSTER", "CLIENT", "PEER", "NAMESPACE", "AUTH")


async def _load(tourconf: str | Path | None) -> tuple[Path, ClientConfig]:
    try:
        path = resolve_tourconf_path(tourconf)
        return path, await load_client_config(path)
    except Exception as exc:
        raise cli_error(exc, None) from None


def _rows(config: ClientConfig) -> list[dict[str, Any]]:
    rows = []
    for name, context in config.contexts.items():
        cluster = config.clusters[context.cluster]
        rows.append(
            {
                "name": name,
                "cluster": context.cluster,
                "client": cluster.client,
                "peer": cluster.peer,
                "namespace": context.namespace,
                "auth": context.auth,
            }
        )
    return rows


def _render_view(path: Path, config: ClientConfig) -> str:
    head = fields([("File", str(path)), ("Current context", config.current_context)])
    keys = ("name", "cluster", "client", "peer", "namespace", "auth")
    body = table(_HEADERS, ([row[k] for k in keys] for row in _rows(config)))
    return f"{head}\n\n{body}"


async def config_view(
    *, tourconf: str | Path | None = None, as_json: bool = False
) -> str:
    """Describe the contexts of the client config; certificates are never shown."""
    path, config = await _load(tourconf)
    if as_json:
        return to_json(
            {
                "contexts": _rows(config),
                "current_context": config.current_context,
                "file": str(path),
            }
        )
    return _render_view(path, config)


async def config_use_context(name: str, *, tourconf: str | Path | None = None) -> str:
    """Make ``name`` the current context, rewriting only ``current-context``."""
    path, config = await _load(tourconf)
    if name not in config.contexts:
        known = ", ".join(config.contexts)
        raise CliError(
            f"unknown context {name}. Known: {known}.",
            EXIT_INVALID,
            error="unknown_context",
            details={"context": name, "known": list(config.contexts)},
        )
    async with TomlPersistence().edit(path) as document:
        document["current-context"] = name
    context = config.contexts[name]
    return (
        f"Switched to context {name} "
        f"(cluster {context.cluster}, namespace {context.namespace})."
    )
