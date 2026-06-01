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
"""Configuration loading with duration/size parsing and validation."""

from __future__ import annotations

import re
from pathlib import Path
from tomllib import loads

from tourillon.core.structure.config import (
    DrainConfig,
    JoinConfig,
    NodeSize,
    RebalanceConfig,
    ServerConfig,
    TlsConfig,
    TourillonConfig,
)
from tourillon.infra.tls.context import (
    TlsValidationError,
    validate_cert_key_match,
    validate_cert_not_expired,
)


class ConfigError(Exception):
    """Fatal configuration error raised by parse_duration, parse_bytes, load_config."""


def parse_duration(s: str) -> float:
    """Parse a unit-embedded duration string; return seconds as float.

    Accepted suffixes: ms, s, m, h.
    Raise ConfigError on unrecognised suffix or non-numeric prefix.

    Examples:
        parse_duration("500ms") -> 0.5
        parse_duration("10s")   -> 10.0
        parse_duration("2m")    -> 120.0
        parse_duration("1h")    -> 3600.0
    """
    match = re.match(r"^(\d+(?:\.\d+)?)(ms|s|m|h)$", s.strip())
    if not match:
        raise ConfigError(
            f"Invalid duration format: {s!r}. Expected format like '10s', '2m', '1h', or '500ms'."
        )

    value_str, unit = match.groups()
    value = float(value_str)

    multipliers = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}
    return value * multipliers[unit]


def parse_bytes(s: str) -> int:
    """Parse a unit-embedded size string; return bytes as int.

    Accepted suffixes: Ki, Mi, Gi.
    Raise ConfigError on unrecognised suffix or non-numeric prefix.

    Examples:
        parse_bytes("512Ki") -> 524288
        parse_bytes("1Mi")   -> 1048576
        parse_bytes("4Gi")   -> 4294967296
    """
    match = re.match(r"^(\d+)(Ki|Mi|Gi)$", s.strip())
    if not match:
        raise ConfigError(
            f"Invalid size format: {s!r}. Expected format like '512Ki', '1Mi', or '4Gi'."
        )

    value_str, unit = match.groups()
    value = int(value_str)

    multipliers = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3}
    return value * multipliers[unit]


def _read_toml(path: Path) -> dict[str, object]:
    try:
        return loads(path.read_text())
    except Exception as e:
        raise ConfigError(f"Failed to parse TOML file {path}: {e}") from e


def _require_sections(data: dict[str, object]) -> None:
    if "node" not in data:
        raise ConfigError("Missing mandatory section: [node]")
    if "tls" not in data:
        raise ConfigError("Missing mandatory section: [tls]")
    if "servers" in data:
        servers = data["servers"]
        if (
            not isinstance(servers, dict)
            or "kv" not in servers
            or "peer" not in servers
        ):
            raise ConfigError(
                "Missing mandatory section: [servers.kv] or [servers.peer]"
            )
        return
    for section in ["kv_server", "peer_server"]:
        if section not in data:
            raise ConfigError(f"Missing mandatory section: [{section}]")


def _parse_node_size(node_data: dict[str, object]) -> NodeSize:
    try:
        return NodeSize(str(node_data.get("size", "M")))
    except ValueError as e:
        raise ConfigError(f"Invalid node size: {node_data.get('size')}") from e


def _validate_tls_config(tls_data: dict[str, object]) -> None:
    cert_data = str(tls_data["cert_data"])
    key_data = str(tls_data["key_data"])
    try:
        validate_cert_not_expired(cert_data)
        validate_cert_key_match(cert_data, key_data)
    except TlsValidationError as e:
        raise ConfigError(f"TLS validation error: {e}") from e


def _validate_duration_fields(section: dict[str, object], section_name: str) -> None:
    try:
        for field in ["attempt_timeout", "deadline", "backoff_base", "backoff_max"]:
            if field in section:
                parse_duration(str(section[field]))
    except ConfigError as e:
        raise ConfigError(f"Invalid duration in [{section_name}]: {e}") from e


def _validate_rebalance_fields(section: dict[str, object]) -> None:
    try:
        if "max_chunk_bytes" in section:
            parse_bytes(str(section["max_chunk_bytes"]))
    except ConfigError as e:
        raise ConfigError(f"Invalid size in [rebalance]: {e}") from e


def _build_tls(tls_data: dict[str, object]) -> TlsConfig:
    return TlsConfig(
        cert_data=str(tls_data["cert_data"]),
        key_data=str(tls_data["key_data"]),
        ca_data=str(tls_data["ca_data"]),
    )


def _build_server(
    data: dict[str, object], *, default_advertise: str = ""
) -> ServerConfig:
    return ServerConfig(
        bind=str(data["bind"]),
        advertise=str(data.get("advertise", default_advertise)),
    )


def _build_join(data: dict[str, object]) -> JoinConfig:
    return JoinConfig(
        max_retries=int(data.get("max_retries", -1)),
        attempt_timeout=str(data.get("attempt_timeout", "10s")),
        deadline=str(data.get("deadline", "2m")),
        backoff_base=str(data.get("backoff_base", "2s")),
        backoff_max=str(data.get("backoff_max", "30s")),
        max_concurrent=int(data.get("max_concurrent", 4)),
    )


def _build_drain(data: dict[str, object]) -> DrainConfig:
    return DrainConfig(
        max_retries=int(data.get("max_retries", -1)),
        attempt_timeout=str(data.get("attempt_timeout", "30s")),
        deadline=str(data.get("deadline", "5m")),
        backoff_base=str(data.get("backoff_base", "5s")),
        backoff_max=str(data.get("backoff_max", "60s")),
        max_concurrent=int(data.get("max_concurrent", 4)),
        bandwidth_fraction=float(data.get("bandwidth_fraction", 1.0)),
    )


def _build_rebalance(data: dict[str, object]) -> RebalanceConfig:
    return RebalanceConfig(
        max_concurrent_transfers=int(data.get("max_concurrent_transfers", 4)),
        max_chunk_bytes=str(data.get("max_chunk_bytes", "1Mi")),
    )


def load_config(path: Path) -> TourillonConfig:
    """Load, validate, and return an immutable TourillonConfig from *path*.

    Validation steps (all fatal — raise ConfigError):
      1. TOML parse error.
      2. Missing mandatory sections ([node], [kv_server], [peer_server], [tls]).
      3. Invalid NodeSize value.
      4. Invalid duration/size strings (parse_duration / parse_bytes called on each).
      5. TLS cert expired (validate_cert_not_expired).
      6. TLS cert/key mismatch (validate_cert_key_match).
    """
    data = _read_toml(path)
    _require_sections(data)

    node_data = data["node"]
    tls_data = data["tls"]
    join_data = data.get("join", {})
    drain_data = data.get("drain", {})
    rebalance_data = data.get("rebalance", {})
    servers_data = data.get("servers")

    assert isinstance(node_data, dict)
    assert isinstance(tls_data, dict)
    assert isinstance(join_data, dict)
    assert isinstance(drain_data, dict)
    assert isinstance(rebalance_data, dict)
    assert servers_data is None or isinstance(servers_data, dict)

    if isinstance(servers_data, dict):
        kv_server_data = servers_data["kv"]
        peer_server_data = servers_data["peer"]
    else:
        kv_server_data = data["kv_server"]
        peer_server_data = data["peer_server"]

    assert isinstance(kv_server_data, dict)
    assert isinstance(peer_server_data, dict)

    node_size = _parse_node_size(node_data)
    _validate_tls_config(tls_data)
    _validate_duration_fields(join_data, "join")
    _validate_duration_fields(drain_data, "drain")
    _validate_rebalance_fields(rebalance_data)

    return TourillonConfig(
        node_id=node_data["id"],
        node_size=node_size,
        data_dir=str(node_data["data_dir"]),
        tls=_build_tls(tls_data),
        kv_server=_build_server(kv_server_data),
        peer_server=_build_server(peer_server_data),
        seeds=node_data.get("seeds", []),
        rf=int(node_data.get("rf", 3)),
        partition_shift=int(node_data.get("partition_shift", 10)),
        join=_build_join(join_data),
        drain=_build_drain(drain_data),
        rebalance=_build_rebalance(rebalance_data),
        schema_version=data.get("schema_version", 1),
    )
