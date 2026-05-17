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
"""Node configuration dataclasses and NodeSize enumeration."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class NodeSize(StrEnum):
    """Token count per physical node on the consistent-hash ring.

    The size is set once at config-generate time and is immutable after the
    node joins a cluster. Changing it requires the node to leave, reconfigure,
    and re-join. A heterogeneous cluster is fully supported; larger nodes
    receive proportionally more ring partitions.
    """

    XS = "XS"  # 1 token
    S = "S"  # 2 tokens
    M = "M"  # 4 tokens  (default)
    L = "L"  # 8 tokens
    XL = "XL"  # 16 tokens
    XXL = "XXL"  # 32 tokens

    @property
    def token_count(self) -> int:
        """Return the number of virtual-node tokens for this size class."""
        return {"XS": 1, "S": 2, "M": 4, "L": 8, "XL": 16, "XXL": 32}[self.value]


@dataclass(frozen=True)
class ServerConfig:
    """Bind and advertise addresses for one TCP listener."""

    bind: str
    advertise: str = ""  # defaults to bind when empty


@dataclass(frozen=True)
class TlsConfig:
    """Node server-side TLS credentials stored inline as base64-encoded PEM.

    No *_file path variants are permitted anywhere; the config must be fully
    self-contained so that copying it to another machine is sufficient.
    """

    cert_data: str  # base64-encoded PEM server certificate
    key_data: str  # base64-encoded PEM private key
    ca_data: str  # base64-encoded PEM CA certificate


@dataclass(frozen=True)
class JoinConfig:
    """Retry and concurrency parameters governing the node-join workflow."""

    max_retries: int = -1  # -1 = unlimited within deadline
    attempt_timeout: float = 10.0  # seconds per attempt
    deadline: float = 120.0
    backoff_base: float = 2.0
    backoff_max: float = 30.0
    max_concurrent: int = 4


@dataclass(frozen=True)
class DrainConfig:
    """Retry and concurrency parameters governing the drain workflow."""

    max_retries: int = -1
    attempt_timeout: float = 30.0
    deadline: float = 300.0
    backoff_base: float = 5.0
    backoff_max: float = 60.0
    max_concurrent: int = 4
    bandwidth_fraction: float = 1.0


@dataclass(frozen=True)
class RebalanceConfig:
    """Operator-tunable parameters for the rebalance engine."""

    max_concurrent_transfers: int = 4  # max parallel in-flight transfers
    max_chunk_bytes: int = 1_048_576  # 1 MiB per transfer chunk


@dataclass(frozen=True)
class KvConfig:
    """Server-side KV tuning parameters from config.toml → [kv]."""

    fanout_timeout_ms: int = 50
    hint_replay_stabilization_ms: int = 2000
    hint_replay_concurrency: int = 16

    _MIN_FANOUT_MS: int = 10  # minimum enforced; node refuses to start otherwise

    def validate(self) -> None:
        """Raise ValueError if any parameter violates its invariant."""
        if self.fanout_timeout_ms < self._MIN_FANOUT_MS:
            raise ValueError(
                f"kv.fanout_timeout_ms must be >= {self._MIN_FANOUT_MS} ms, "
                f"got {self.fanout_timeout_ms}"
            )


@dataclass(frozen=True)
class TourillonConfig:
    """Fully-validated, immutable node configuration.

    Constructed exclusively by tourillon.bootstrap.config.load_config,
    which is the single code path allowed to read config files and
    environment variables. All subsystems receive an instance via
    constructor injection; no subsystem reads config on its own.

    node_size and partition_shift are immutable after the node joins
    a cluster; changing either requires a full decommission and re-join.
    """

    node_id: str
    node_size: NodeSize  # immutable after join; determines token count
    data_dir: str  # owns pid.lock and state.toml
    tls: TlsConfig
    kv_server: ServerConfig
    peer_server: ServerConfig
    seeds: list[str] = field(default_factory=list)
    rf: int = 3
    partition_shift: int = 10  # immutable after cluster bootstrap
    join: JoinConfig = field(default_factory=JoinConfig)
    drain: DrainConfig = field(default_factory=DrainConfig)
    rebalance: RebalanceConfig = field(default_factory=RebalanceConfig)
    kv: KvConfig = field(default_factory=KvConfig)
    schema_version: int = 1
