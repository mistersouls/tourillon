from pathlib import Path
from typing import Any

from tourlib.models import (
    ClusterRef,
    ContextEntry,
    ContextsFile,
    CredentialsConfig,
    EndpointsConfig,
)
from tourlib.ports.loader import ConfigReadWriter


class ContextConfigurer:
    def __init__(self, config_rw: ConfigReadWriter) -> None:
        self._config_rw = config_rw

    def load_contexts(self, path: Path) -> ContextsFile:
        if not path.exists():
            return ContextsFile()

        raw: dict[str, Any] = self._config_rw.read(path)

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

    def save_contexts(self, path: Path, file: ContextsFile) -> None:
        self._config_rw.write(path, file.to_dict())
