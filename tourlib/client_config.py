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

import asyncio
import base64
import shutil
import ssl
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tourlib.validation import require_named_table, require_str


@dataclass(frozen=True)
class ClientAuthConfig:
    """Inline base64-encoded PEM material for a client auth profile."""

    ca_data: str
    cert_data: str
    key_data: str

    def to_document(self) -> dict[str, str]:
        return {
            "ca_data": self.ca_data,
            "cert_data": self.cert_data,
            "key_data": self.key_data,
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "ClientAuthConfig":
        return cls(
            ca_data=require_str(document, "ca_data"),
            cert_data=require_str(document, "cert_data"),
            key_data=require_str(document, "key_data"),
        )

    async def tls_client(self) -> ssl.SSLContext:
        return await self._with_material(self._build_client_context)

    async def _with_material(
        self,
        builder: Callable[[tuple[Path, Path, Path]], ssl.SSLContext],
    ) -> ssl.SSLContext:
        directory = Path(tempfile.mkdtemp(prefix="tourillon-client-tls-"))
        try:
            material = await self._write_material(directory)
            return builder(material)
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    async def _write_material(self, directory: Path) -> tuple[Path, Path, Path]:
        return await asyncio.to_thread(self._write_material_sync, directory)

    def _write_material_sync(self, directory: Path) -> tuple[Path, Path, Path]:
        ca_path = directory / "ca.pem"
        cert_path = directory / "cert.pem"
        key_path = directory / "key.pem"
        ca_path.write_bytes(self._decode(self.ca_data, "ca_data"))
        cert_path.write_bytes(self._decode(self.cert_data, "cert_data"))
        key_path.write_bytes(self._decode(self.key_data, "key_data"))
        return ca_path, cert_path, key_path

    @staticmethod
    def _decode(value: str, field_name: str) -> bytes:
        try:
            return base64.b64decode(value, validate=True)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be valid base64 data") from exc

    @staticmethod
    def _build_client_context(material: tuple[Path, Path, Path]) -> ssl.SSLContext:
        ca_path, cert_path, key_path = material
        context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=str(ca_path),
        )
        context.load_cert_chain(str(cert_path), str(key_path))
        context.check_hostname = False
        context.verify_mode = ssl.CERT_REQUIRED
        return context


@dataclass(frozen=True)
class ClientCluster:
    """Endpoints for a cluster known by the client config."""

    client: str
    peer: str

    def to_document(self) -> dict[str, str]:
        return {"client": self.client, "peer": self.peer}

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "ClientCluster":
        return cls(
            client=require_str(document, "client"), peer=require_str(document, "peer")
        )


@dataclass(frozen=True)
class ClientContext:
    """A named client context referencing a cluster and auth profile."""

    cluster: str
    namespace: str
    auth: str

    def to_document(self) -> dict[str, str]:
        return {
            "cluster": self.cluster,
            "namespace": self.namespace,
            "auth": self.auth,
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "ClientContext":
        return cls(
            cluster=require_str(document, "cluster"),
            namespace=require_str(document, "namespace"),
            auth=require_str(document, "auth"),
        )


@dataclass(frozen=True)
class ClientConfig:
    """Serialized multi-cluster client configuration used by tourctl."""

    version: str
    current_context: str
    contexts: dict[str, ClientContext]
    clusters: dict[str, ClientCluster]
    auth: dict[str, ClientAuthConfig]

    def to_document(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "current-context": self.current_context,
            "contexts": {
                name: value.to_document() for name, value in self.contexts.items()
            },
            "clusters": {
                name: value.to_document() for name, value in self.clusters.items()
            },
            "auth": {name: value.to_document() for name, value in self.auth.items()},
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "ClientConfig":
        version = require_str(document, "version")
        current_context = require_str(document, "current-context")
        contexts = require_named_table(
            document, "contexts", ClientContext.from_document
        )
        clusters = require_named_table(
            document, "clusters", ClientCluster.from_document
        )
        auth = require_named_table(document, "auth", ClientAuthConfig.from_document)
        config = cls(
            version=version,
            current_context=current_context,
            contexts=contexts,
            clusters=clusters,
            auth=auth,
        )
        config._validate_references()
        return config

    def _validate_references(self) -> None:
        if self.current_context not in self.contexts:
            raise ValueError("current-context must reference an existing context")
        for name, context in self.contexts.items():
            if context.cluster not in self.clusters:
                raise ValueError(
                    f"context '{name}' references unknown cluster '{context.cluster}'"
                )
            if context.auth not in self.auth:
                raise ValueError(
                    f"context '{name}' references unknown auth '{context.auth}'"
                )

    def context(self, name: str | None = None) -> ClientContext:
        resolved = self.current_context if name is None else name
        return self.contexts[resolved]

    def cluster(self, name: str | None = None) -> ClientCluster:
        context = self.context(name)
        return self.clusters[context.cluster]

    def auth_config(self, name: str | None = None) -> ClientAuthConfig:
        context = self.context(name)
        return self.auth[context.auth]

    async def tls_client(self, name: str | None = None) -> ssl.SSLContext:
        return await self.auth_config(name).tls_client()


ClientTlsConfig = ClientAuthConfig
