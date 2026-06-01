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
"""Dataclasses representing the contexts.toml operator context file."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ClusterRef:
    """Cluster identity and CA trust anchor."""

    name: str
    ca_data: str  # base64-encoded PEM CA certificate


@dataclass(frozen=True)
class EndpointsConfig:
    """Reachable endpoints for the cluster.

    At least one of kv or peer must be set. A context with only kv is valid
    for tourctl kv commands; a context with only peer is valid for operator
    commands (tourctl node join, inspect, etc.).
    """

    kv: str | None = None
    peer: str | None = None


@dataclass(frozen=True)
class CredentialsConfig:
    """Client certificate material stored inline as base64-encoded PEM."""

    cert_data: str  # base64-encoded PEM client certificate
    key_data: str  # base64-encoded PEM client private key


@dataclass(frozen=True)
class ContextEntry:
    """One named operator context stored in contexts.toml."""

    name: str
    cluster: ClusterRef
    endpoints: EndpointsConfig
    credentials: CredentialsConfig


@dataclass
class ContextsFile:
    """In-memory representation of contexts.toml."""

    current_context: str | None = None
    contexts: list[ContextEntry] = field(default_factory=list)

    def get(self, name: str) -> ContextEntry | None:
        """Return the context named *name*, or None."""
        return next((c for c in self.contexts if c.name == name), None)

    def upsert(self, entry: ContextEntry) -> None:
        """Insert *entry* or replace the existing context with the same name."""
        self.contexts = [c for c in self.contexts if c.name != entry.name]
        self.contexts.append(entry)
