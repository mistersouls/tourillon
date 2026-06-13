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
"""Node configuration dataclasses."""

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tourillon.core.exceptions import ConfigError
from tourillon.core.machinery.config import NodeSize


@dataclass(frozen=True)
class KvServerConfig:
    """Bind and advertise addresses for one TCP listener."""

    bind: str


@dataclass(frozen=True)
class PeerServerConfig:
    """Bind and advertise addresses for one TCP listener."""

    bind: str
    advertise: str  # defaults to bind when empty


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
class GossipBootstrapConfig:
    """Exponential backoff parameters for the gossip bootstrap retry loop.

    Default parameters cover a window of approximately three minutes
    (1 s → 2 s → 4 s → … → 60 s, ten attempts), long enough for most rolling
    restarts to complete while still failing loudly for genuine outages.
    Setting max_retries=0 enables unlimited retries.
    """

    initial_delay_s: float = 1.0  # delay before first retry
    max_delay_s: float = 60.0  # ceiling for exponential growth
    multiplier: float = 2.0  # doubling factor per attempt
    jitter: float = 0.1  # ±10 % uniform jitter to avoid thundering herd
    max_retries: int = 10  # 0 = unlimited retries
    connect_timeout: float = 10.0  # per-seed connection timeout in seconds


@dataclass(frozen=True)
class GossipConfig:
    """Full configuration for the GossipEngine.

    bootstrap holds the retry parameters for the initial full-resync sequence.
    All other fields govern the steady-state hot-path and anti-entropy cycles.
    """

    bootstrap: GossipBootstrapConfig = field(default_factory=GossipBootstrapConfig)
    anti_entropy_interval: float = 30.0  # seconds between AE cycles
    max_fan_out: int = 6  # ceil(log2(N)) + 1, capped here
    max_payload_bytes: int = 1_048_576  # 1 MiB per gossip.push envelope
    max_digest_entries: int = 4_096  # entries per gossip.digest page
    max_gossip_per_peer_rps: float = 20.0  # rate limit per peer


@dataclass(frozen=True)
class ConfigRequest:
    node_id: str
    size: NodeSize
    data_dir: Path
    cert_valid_days: int
    ca_cert: Path
    ca_key: Path
    out_cert: Path
    out_key: Path
    kv_bind: str
    peer_bind: str
    peer_advertise: str
    replication_factor: int
    partition_shift: int
    seeds: list[str]
    out: Path


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
    data_dir: Path  # owns pid.lock and state.toml
    tls: TlsConfig
    kv_server: KvServerConfig
    peer_server: PeerServerConfig
    seeds: list[str]
    replication_factor: int
    partition_shift: int
    schema_version: int
    gossip: GossipConfig = field(default_factory=GossipConfig)

    def __post_init__(self) -> None:
        if self.segment_shift >= self.partition_shift:
            raise ConfigError(
                "derived segment_shift must be strictly less than partition_shift"
            )

    @property
    def segment_shift(self) -> int:
        token_count = self.node_size.token_count

        if token_count < 1:
            raise ValueError("token_count must be >= 1")
        if token_count & (token_count - 1) != 0:
            raise ValueError("token_count must be a power of two")
        return int(math.log2(token_count))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "node": {
                "id": self.node_id,
                "size": self.node_size.value,
                "data_dir": str(self.data_dir),
                "replication_factor": self.replication_factor,
                "partition_shift": self.partition_shift,
                "seeds": self.seeds,
            },
            "gossip": {
                "anti_entropy_interval": self.gossip.anti_entropy_interval,
                "max_fan_out": self.gossip.max_fan_out,
                "max_payload_bytes": self.gossip.max_payload_bytes,
                "max_digest_entries": self.gossip.max_digest_entries,
                "max_gossip_per_peer_rps": self.gossip.max_gossip_per_peer_rps,
                "bootstrap": {
                    "initial_delay_s": self.gossip.bootstrap.initial_delay_s,
                    "max_delay_s": self.gossip.bootstrap.max_delay_s,
                    "multiplier": self.gossip.bootstrap.multiplier,
                    "jitter": self.gossip.bootstrap.jitter,
                    "max_retries": self.gossip.bootstrap.max_retries,
                    "connect_timeout": self.gossip.bootstrap.connect_timeout,
                },
            },
            "servers": {
                "kv": {"bind": self.kv_server.bind},
                "peer": {
                    "bind": self.peer_server.bind,
                    "advertise": self.peer_server.advertise,
                },
            },
            "tls": {
                "cert_data": self.tls.cert_data,
                "key_data": self.tls.key_data,
                "ca_data": self.tls.ca_data,
            },
        }
