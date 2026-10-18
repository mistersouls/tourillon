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
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from tourillon.core.models.retry import BackoffRetry
from tourillon.core.models.validator import (
    quantity_bytes,
    validate_group_size,
    validate_log_level,
    validate_node_id,
)
from tourlib.validation import ValidationError, require_mapping, require_str


@dataclass(frozen=True)
class PeerServerConfig:
    """Server endpoint configuration with bind and advertise addresses."""

    bind: str
    advertise: str

    def to_document(self) -> dict[str, str]:
        return {
            "bind": self.bind,
            "advertise": self.advertise,
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "PeerServerConfig":
        return cls(
            bind=require_str(document, "bind"),
            advertise=require_str(document, "advertise"),
        )


class NodeSize(StrEnum):
    """
    Token count per physical node on the consistent-hash ring.

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

    def to_value(self) -> int:
        """Return the number of virtual-node tokens for this size class."""
        return {"XS": 1, "S": 2, "M": 4, "L": 8, "XL": 16, "XXL": 32}[self.value]


@dataclass(frozen=True)
class NodeConfig:
    """Node-specific configuration values."""

    id: str
    size: NodeSize = NodeSize.M
    seeds: list[str] | None = None

    def to_document(self) -> dict[str, Any]:
        doc: dict[str, Any] = {"id": self.id, "size": self.size.value}
        if self.seeds is not None:
            doc["seeds"] = self.seeds
        return doc

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "NodeConfig":
        seeds = document.get("seeds")
        if seeds is not None and not isinstance(seeds, list):
            raise ValidationError("seeds must be a list of strings")
        if seeds is not None and not all(isinstance(seed, str) and seed for seed in seeds):
            raise ValidationError("seeds must be a list of strings")

        size_raw = document.get("size", NodeSize.M.value)
        if not isinstance(size_raw, str) or not size_raw.strip():
            raise ValidationError("size must be a non-empty string")
        try:
            size = NodeSize(size_raw.strip().upper())
        except ValueError as exc:
            raise ValidationError("size must be one of: XS, S, M, L, XL, XXL") from exc

        return cls(
            id=validate_node_id(require_str(document, "id"), "node.id"),
            size=size,
            seeds=seeds
        )


@dataclass(frozen=True)
class ClientServerConfig:
    """Client endpoint configuration with bind address."""

    bind: str

    def to_document(self) -> dict[str, str]:
        return {"bind": self.bind}

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "ClientServerConfig":
        return cls(bind=require_str(document, "bind"))


@dataclass(frozen=True)
class StorageConfig:
    """Storage configuration values."""

    data_dir: str
    group_size: str

    def to_document(self) -> dict[str, Any]:
        return {"data_dir": self.data_dir, "group_size": self.group_size}

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "StorageConfig":
        return cls(
            data_dir=cls.validate_data_dir(require_str(document, "data_dir")),
            group_size=validate_group_size(require_str(document, "group_size"), "storage.group_size"),
        )

    def group_size_bytes(self) -> int:
        return quantity_bytes(self.group_size, "storage.group_size")

    @staticmethod
    def validate_data_dir(data_dir: str) -> str:
        normalized = data_dir.strip()
        if not normalized:
            raise ValidationError("storage.data_dir must be a non-empty directory path")

        path = Path(normalized)
        if path.exists() and not path.is_dir():
            raise ValidationError("storage.data_dir must reference a directory path")

        return normalized


@dataclass(frozen=True)
class TlsConfig:
    """Inline base64-encoded PEM material for mTLS."""

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
    def from_document(cls, document: Mapping[str, Any]) -> "TlsConfig":
        return cls(
            ca_data=require_str(document, "ca_data"),
            cert_data=require_str(document, "cert_data"),
            key_data=require_str(document, "key_data"),
        )

    async def tls_server(self) -> ssl.SSLContext:
        return await self._with_material(self._build_server_context)

    async def tls_client(self) -> ssl.SSLContext:
        return await self._with_material(self._build_client_context)

    async def _with_material(
        self,
        builder: Callable[[tuple[Path, Path, Path]], ssl.SSLContext],
    ) -> ssl.SSLContext:
        directory = Path(tempfile.mkdtemp(prefix="tourillon-tls-"))
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
    def _build_server_context(material: tuple[Path, Path, Path]) -> ssl.SSLContext:
        ca_path, cert_path, key_path = material
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(cert_path), str(key_path))
        context.load_verify_locations(cafile=str(ca_path))
        context.verify_mode = ssl.CERT_REQUIRED
        return context

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
class LoggerConfig:
    """Per-logger runtime options."""

    level: str = "INFO"
    enabled: bool = True

    def to_document(self) -> dict[str, Any]:
        return {
            "level": self.level,
            "enabled": self.enabled,
        }

    @classmethod
    def from_document(
        cls,
        document: Mapping[str, Any],
        *,
        field_prefix: str,
    ) -> "LoggerConfig":
        level = document.get("level", "INFO")
        if not isinstance(level, str):
            raise ValidationError(f"{field_prefix}.level must be a non-empty string")

        enabled = document.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValidationError(f"{field_prefix}.enabled must be a boolean")

        return cls(level=validate_log_level(level, f"{field_prefix}.level"), enabled=enabled)


def _default_logger_entries() -> dict[str, LoggerConfig]:
    return {
        "global": LoggerConfig(level="INFO", enabled=True),
        "transport": LoggerConfig(level="INFO", enabled=True),
    }


@dataclass(frozen=True)
class LoggingConfig:
    """Global logging configuration and named logger overrides."""

    level: str = "INFO"
    loggers: dict[str, LoggerConfig] = field(default_factory=_default_logger_entries)

    def to_document(self) -> dict[str, Any]:
        return {
            "level": self.level,
            "loggers": {
                name: logger.to_document()
                for name, logger in self.loggers.items()
            },
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "LoggingConfig":
        level = document.get("level", "INFO")
        if not isinstance(level, str):
            raise ValidationError("logging.level must be a non-empty string")

        loggers_doc = document.get("loggers")
        if loggers_doc is None:
            return cls(level=validate_log_level(level, "logging.level"))
        if not isinstance(loggers_doc, Mapping):
            raise ValidationError("logging.loggers must be a table")

        loggers: dict[str, LoggerConfig] = {}
        for name, value in loggers_doc.items():
            if not isinstance(name, str) or not name:
                raise ValidationError("logging.loggers keys must be non-empty strings")
            if not isinstance(value, Mapping):
                raise ValidationError(f"logging.loggers.{name} must be a table")
            loggers[name] = LoggerConfig.from_document(value, field_prefix=f"logging.loggers.{name}")

        return cls(level=validate_log_level(level, "logging.level"), loggers=loggers)


@dataclass(frozen=True)
class TelemetryConfig:
    """Telemetry runtime toggles and OTLP exporter options."""

    metrics_enabled: bool = False
    tracing_enabled: bool = False
    service_name: str = "tourillon"
    metrics_options: dict[str, Any] = field(default_factory=dict)
    tracing_options: dict[str, Any] = field(default_factory=dict)

    def to_document(self) -> dict[str, Any]:
        return {
            "metrics_enabled": self.metrics_enabled,
            "tracing_enabled": self.tracing_enabled,
            "service_name": self.service_name,
            "metrics_options": dict(self.metrics_options),
            "tracing_options": dict(self.tracing_options),
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "TelemetryConfig":
        metrics_enabled = document.get("metrics_enabled", False)
        if not isinstance(metrics_enabled, bool):
            raise ValidationError("telemetry.metrics_enabled must be a boolean")

        tracing_enabled = document.get("tracing_enabled", False)
        if not isinstance(tracing_enabled, bool):
            raise ValidationError("telemetry.tracing_enabled must be a boolean")

        service_name = document.get("service_name", document.get("prefix", "tourillon"))
        if not isinstance(service_name, str) or not service_name.strip():
            raise ValidationError("telemetry.service_name must be a non-empty string")

        metrics_options = document.get("metrics_options", {})
        if not isinstance(metrics_options, Mapping):
            raise ValidationError("telemetry.metrics_options must be a table")

        tracing_options = document.get("tracing_options", {})
        if not isinstance(tracing_options, Mapping):
            raise ValidationError("telemetry.tracing_options must be a table")

        return cls(
            metrics_enabled=metrics_enabled,
            tracing_enabled=tracing_enabled,
            service_name=service_name.strip(),
            metrics_options=dict(metrics_options),
            tracing_options=dict(tracing_options),
        )


@dataclass
class GossipConfig:
    """Gossip engine runtime options."""

    max_fan_out: int = 6
    hop_limit: int = 3
    ae_interval: float = 30.0
    stop_drain_timeout: float = 5.0
    bootstrap_retry: BackoffRetry = field(default_factory=BackoffRetry)

    def to_document(self) -> dict[str, Any]:
        return {
            "max_fan_out": self.max_fan_out,
            "hop_limit": self.hop_limit,
            "ae_interval": self.ae_interval,
            "stop_drain_timeout": self.stop_drain_timeout,
            "bootstrap_retry": self.bootstrap_retry.to_document(),
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "GossipConfig":
        max_fan_out = document.get("max_fan_out", 6)
        if not isinstance(max_fan_out, int) or max_fan_out < 1:
            raise ValidationError("gossip.max_fan_out must be an integer >= 1")

        hop_limit = document.get("hop_limit", 3)
        if not isinstance(hop_limit, int) or hop_limit < 1:
            raise ValidationError("gossip.hop_limit must be an integer >= 1")

        ae_interval = document.get("ae_interval", 30.0)
        if not isinstance(ae_interval, int | float) or ae_interval <= 0:
            raise ValidationError("gossip.ae_interval must be a number > 0")

        stop_drain_timeout = document.get("stop_drain_timeout", 5.0)
        if not isinstance(stop_drain_timeout, int | float) or stop_drain_timeout <= 0:
            raise ValidationError("gossip.stop_drain_timeout must be a number > 0")

        return cls(
            max_fan_out=max_fan_out,
            hop_limit=hop_limit,
            ae_interval=float(ae_interval),
            stop_drain_timeout=float(stop_drain_timeout),
            bootstrap_retry=BackoffRetry.from_document(
                document.get("bootstrap_retry") or {},
                prefix="gossip.bootstrap_retry.",
            ),
        )


@dataclass(frozen=True)
class TransportPoolConfig:
    """Envelope client pool sizing options."""

    min_size: int = 32
    max_size: int = 256
    max_overflow: int = 8
    idle_ttl: float = 60 * 10

    def to_document(self) -> dict[str, Any]:
        return {
            "min_size": self.min_size,
            "max_size": self.max_size,
            "max_overflow": self.max_overflow,
            "idle_ttl": self.idle_ttl,
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "TransportPoolConfig":
        min_size = document.get("min_size", 32)
        if not isinstance(min_size, int) or min_size < 0:
            raise ValidationError("transport.pool.min_size must be an integer >= 0")

        max_size = document.get("max_size", 256)
        if not isinstance(max_size, int) or max_size < 1:
            raise ValidationError("transport.pool.max_size must be an integer >= 1")

        if min_size > max_size:
            raise ValidationError("transport.pool.min_size must be <= transport.pool.max_size")

        max_overflow = document.get("max_overflow", 8)
        if not isinstance(max_overflow, int) or max_overflow < 0:
            raise ValidationError("transport.pool.max_overflow must be an integer >= 0")

        idle_ttl = document.get("idle_ttl", 60 * 10)
        if not isinstance(idle_ttl, int | float) or idle_ttl <= 0:
            raise ValidationError("transport.pool.idle_ttl must be a number > 0")

        return cls(
            min_size=min_size,
            max_size=max_size,
            max_overflow=max_overflow,
            idle_ttl=float(idle_ttl),
        )


@dataclass(frozen=True)
class TransportConfig:
    """Transport runtime options."""

    pool: TransportPoolConfig = field(default_factory=TransportPoolConfig)

    def to_document(self) -> dict[str, Any]:
        return {
            "pool": self.pool.to_document(),
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "TransportConfig":
        pool_doc = document.get("pool")
        if pool_doc is None:
            return cls()
        if not isinstance(pool_doc, Mapping):
            raise ValidationError("transport.pool must be a table")
        return cls(pool=TransportPoolConfig.from_document(pool_doc))


@dataclass(frozen=True)
class ReadRepairConfig:
    """Read repair runtime options."""

    enabled: bool = True
    concurrency: int = 16
    sample_rate: float = 1.0

    def to_document(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "concurrency": self.concurrency,
            "sample_rate": self.sample_rate,
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "ReadRepairConfig":
        enabled = document.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValidationError("placement.read_repair.enabled must be a boolean")
        concurrency = document.get("concurrency", 16)
        if not isinstance(concurrency, int) or isinstance(concurrency, bool) or concurrency < 1:
            raise ValidationError("placement.read_repair.concurrency must be an integer >= 1")
        sample_rate = document.get("sample_rate", 1.0)
        if isinstance(sample_rate, bool) or not isinstance(sample_rate, (int, float)):
            raise ValidationError("placement.read_repair.sample_rate must be a number in [0.0, 1.0]")
        if not 0.0 <= float(sample_rate) <= 1.0:
            raise ValidationError("placement.read_repair.sample_rate must be a number in [0.0, 1.0]")
        return cls(enabled=enabled, concurrency=concurrency, sample_rate=float(sample_rate))


@dataclass(frozen=True)
class RepairConfig:
    """Client-driven repair runtime options.

    Repair is the most expensive client verb — up to one read *and* one write
    per replica — so it gets a ceiling of its own, lower than the general
    in-flight limit, to keep a burst of repairs from crowding out reads and
    writes.
    """

    concurrency: int = 4

    def to_document(self) -> dict[str, Any]:
        return {
            "concurrency": self.concurrency,
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "RepairConfig":
        concurrency = document.get("concurrency", 4)
        if not isinstance(concurrency, int) or isinstance(concurrency, bool) or concurrency < 1:
            raise ValidationError("placement.repair.concurrency must be an integer >= 1")
        return cls(concurrency=concurrency)


@dataclass(frozen=True)
class PlacementConfig:
    """Placement runtime options."""

    replication_factor: int = 3
    read_repair: ReadRepairConfig = field(default_factory=ReadRepairConfig)
    repair: RepairConfig = field(default_factory=RepairConfig)

    def to_document(self) -> dict[str, Any]:
        return {
            "replication_factor": self.replication_factor,
            "read_repair": self.read_repair.to_document(),
            "repair": self.repair.to_document(),
        }

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "PlacementConfig":
        replication_factor = document.get("replication_factor", 3)
        if not isinstance(replication_factor, int) or replication_factor < 1:
            raise ValidationError("placement.replication_factor must be an integer >= 1")
        read_repair_doc = document.get("read_repair", {})
        if not isinstance(read_repair_doc, Mapping):
            raise ValidationError("placement.read_repair must be a table")
        repair_doc = document.get("repair", {})
        if not isinstance(repair_doc, Mapping):
            raise ValidationError("placement.repair must be a table")
        return cls(
            replication_factor=replication_factor,
            read_repair=ReadRepairConfig.from_document(read_repair_doc),
            repair=RepairConfig.from_document(repair_doc),
        )

@dataclass(frozen=True)
class TourillonConfig:
    """Serialized node identity document used by Tourillon bootstrap."""

    node: NodeConfig
    storage: StorageConfig
    tls: TlsConfig
    server_client: ClientServerConfig
    server_peer: PeerServerConfig
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    gossip: GossipConfig = field(default_factory=GossipConfig)
    transport: TransportConfig = field(default_factory=TransportConfig)
    placement: PlacementConfig = field(default_factory=PlacementConfig)

    def to_document(self) -> dict[str, Any]:
        doc: dict[str, Any] = {
            "node": self.node.to_document(),
            "storage": self.storage.to_document(),
            "tls": self.tls.to_document(),
            "logging": self.logging.to_document(),
            "telemetry": self.telemetry.to_document(),
            "gossip": self.gossip.to_document(),
            "transport": self.transport.to_document(),
            "placement": self.placement.to_document(),
            "servers": {
                "client": self.server_client.to_document(),
                "peer": self.server_peer.to_document(),
            },
        }
        return doc

    @classmethod
    def from_document(cls, document: Mapping[str, Any]) -> "TourillonConfig":
        servers_doc = require_mapping(document, "servers")

        if "client" not in servers_doc:
            raise ValidationError("servers.client configuration is required")
        if "peer" not in servers_doc:
            raise ValidationError("servers.peer configuration is required")

        server_client = ClientServerConfig.from_document(servers_doc["client"])
        server_peer = PeerServerConfig.from_document(servers_doc["peer"])

        return cls(
            node=NodeConfig.from_document(require_mapping(document, "node")),
            storage=StorageConfig.from_document(require_mapping(document, "storage")),
            tls=TlsConfig.from_document(require_mapping(document, "tls")),
            logging=(
                LoggingConfig.from_document(require_mapping(document, "logging"))
                if "logging" in document
                else LoggingConfig()
            ),
            telemetry=(
                TelemetryConfig.from_document(require_mapping(document, "telemetry"))
                if "telemetry" in document
                else TelemetryConfig()
            ),
            gossip=(
                GossipConfig.from_document(require_mapping(document, "gossip"))
                if "gossip" in document
                else GossipConfig()
            ),
            transport=(
                TransportConfig.from_document(require_mapping(document, "transport"))
                if "transport" in document
                else TransportConfig()
            ),
            placement=(
                PlacementConfig.from_document(require_mapping(document, "placement"))
                if "placement" in document
                else PlacementConfig()
            ),
            server_client=server_client,
            server_peer=server_peer,
        )

    async def tls_server(self) -> ssl.SSLContext:
        return await self.tls.tls_server()

    async def tls_client(self) -> ssl.SSLContext:
        return await self.tls.tls_client()
