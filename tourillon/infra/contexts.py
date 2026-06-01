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
"""contexts.toml read/write operations with atomic persistence."""

from __future__ import annotations

import contextlib
import os
import stat
import tempfile
import tomllib
from pathlib import Path
from typing import Any

import tomli_w

from tourillon.core.structure.contexts import (
    ClusterRef,
    ContextEntry,
    ContextsFile,
    CredentialsConfig,
    EndpointsConfig,
)


class ContextsError(Exception):
    """Raised for contexts.toml parsing and consistency errors."""


class ContextsRepository:
    """File-backed repository for ``ContextsFile`` objects."""

    def load(self, path: Path) -> ContextsFile:
        """Return parsed contexts; return empty object when file is absent."""
        if not path.exists():
            return ContextsFile()
        try:
            raw: dict[str, Any] = tomllib.loads(path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise ContextsError(f"Cannot parse {path}: {exc}") from exc

        entries: list[ContextEntry] = []
        for ctx in raw.get("contexts", []):
            cluster_raw = ctx.get("cluster", {})
            endpoint_raw = ctx.get("endpoints", {})
            credentials_raw = ctx.get("credentials", {})
            entries.append(
                ContextEntry(
                    name=ctx["name"],
                    cluster=ClusterRef(
                        name=cluster_raw.get("name", ctx["name"]),
                        ca_data=cluster_raw.get("ca_data", ""),
                    ),
                    endpoints=EndpointsConfig(
                        kv=endpoint_raw.get("kv"),
                        peer=endpoint_raw.get("peer"),
                    ),
                    credentials=CredentialsConfig(
                        cert_data=credentials_raw.get("cert_data", ""),
                        key_data=credentials_raw.get("key_data", ""),
                    ),
                )
            )

        return ContextsFile(
            current_context=raw.get("current-context"),
            contexts=entries,
        )

    def save(self, path: Path, file: ContextsFile) -> None:
        """Atomically write *file* to *path* at mode 0600."""
        payload = _to_toml_payload(file)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        try:
            os.write(fd, tomli_w.dumps(payload).encode("utf-8"))
            os.close(fd)
            os.chmod(tmp_path, stat.S_IRUSR | stat.S_IWUSR)
            os.replace(tmp_path, path)
        except Exception:
            os.close(fd)
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise


def _to_toml_payload(file: ContextsFile) -> dict[str, Any]:
    data: dict[str, Any] = {
        "contexts": [
            {
                "name": entry.name,
                "cluster": {
                    "name": entry.cluster.name,
                    "ca_data": entry.cluster.ca_data,
                },
                "endpoints": {
                    k: v
                    for k, v in {
                        "kv": entry.endpoints.kv,
                        "peer": entry.endpoints.peer,
                    }.items()
                    if v is not None
                },
                "credentials": {
                    "cert_data": entry.credentials.cert_data,
                    "key_data": entry.credentials.key_data,
                },
            }
            for entry in file.contexts
        ]
    }
    if file.current_context is not None:
        data["current-context"] = file.current_context
    return data


def load_contexts(path: Path) -> ContextsFile:
    """Compatibility function over ``ContextsRepository.load``."""
    return ContextsRepository().load(path)


def save_contexts(path: Path, file: ContextsFile) -> None:
    """Compatibility function over ``ContextsRepository.save``."""
    ContextsRepository().save(path, file)
