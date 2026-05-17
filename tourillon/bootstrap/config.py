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
"""Config loading and validation — the sole code path for reading config files.

load_config() accepts a raw dict (parsed TOML) and returns a fully-validated,
immutable TourillonConfig. load_config_file() reads the TOML file, applies
environment-variable overrides, and delegates to load_config().

All subsystems receive a TourillonConfig instance via constructor injection;
no subsystem calls this module directly after startup.
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any

from tourillon.core.structure.config import (
    DrainConfig,
    JoinConfig,
    KvConfig,
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

_SUPPORTED_SCHEMA_VERSION = 1
_VALID_SIZES = " ".join(s.value for s in NodeSize)


class ConfigError(Exception):
    """Raised when the configuration file is invalid, missing fields, or inconsistent."""


def _require(raw: dict[str, Any], *keys: str) -> Any:
    """Navigate nested dict and raise ConfigError if any key is missing."""
    node = raw
    path = []
    for key in keys:
        path.append(key)
        if not isinstance(node, dict) or key not in node:
            field_path = "][".join(path)
            raise ConfigError(f"Missing required field [{field_path}]")
        node = node[key]
    return node


def _parse_duration(value: str | int | float, field: str) -> float:
    """Parse a duration value — accepts float/int seconds or strings like '10s', '2m'."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip()
        if value.endswith("s"):
            return float(value[:-1])
        if value.endswith("m"):
            return float(value[:-1]) * 60
        if value.endswith("h"):
            return float(value[:-1]) * 3600
        try:
            return float(value)
        except ValueError:
            pass
    raise ConfigError(f"Cannot parse duration for {field!r}: {value!r}")


def load_config(raw: dict[str, Any]) -> TourillonConfig:
    """Parse and validate a raw config dict, returning a TourillonConfig.

    Raise ConfigError for any structural, semantic, or TLS validation failure.
    This is the canonical entry point — no socket is bound, no file is opened,
    and no TLS context is created inside this function; it validates only.
    """
    # schema_version
    schema_version = raw.get("schema_version", 1)
    if schema_version != _SUPPORTED_SCHEMA_VERSION:
        raise ConfigError(
            f"Unsupported schema_version={schema_version}; this binary supports version 1"
        )

    # [node]
    node = _require(raw, "node")
    node_id: str = _require(raw, "node", "id")
    if not node_id:
        raise ConfigError("Missing required field [node][id]")

    raw_size = node.get("size", "M")
    try:
        node_size = NodeSize(raw_size)
    except ValueError:
        raise ConfigError(
            f'Invalid node size "{raw_size}". Valid values: {_VALID_SIZES}'
        ) from None

    data_dir: str = node.get("data_dir", "./node-data")

    # [tls]
    tls_raw = _require(raw, "tls")
    for field_name in ("cert_data", "key_data", "ca_data"):
        if not tls_raw.get(field_name):
            raise ConfigError(f"Missing required field [tls][{field_name}]")

    tls = TlsConfig(
        cert_data=tls_raw["cert_data"],
        key_data=tls_raw["key_data"],
        ca_data=tls_raw["ca_data"],
    )

    # TLS validity
    try:
        validate_cert_not_expired(tls.cert_data)
        validate_cert_key_match(tls.cert_data, tls.key_data)
    except TlsValidationError as exc:
        raise ConfigError(str(exc)) from exc

    # [servers.kv] and [servers.peer]
    kv_server, peer_server = _parse_servers(raw)

    # [cluster]
    cluster_raw = raw.get("cluster", {})
    seeds: list[str] = cluster_raw.get("seeds", [])
    rf: int = int(cluster_raw.get("rf", 3))
    partition_shift: int = int(cluster_raw.get("partition_shift", 10))

    # [join]
    join_raw = raw.get("join", {})
    join = JoinConfig(
        max_retries=int(join_raw.get("max_retries", -1)),
        attempt_timeout=_parse_duration(
            join_raw.get("attempt_timeout", 10), "join.attempt_timeout"
        ),
        deadline=_parse_duration(join_raw.get("deadline", 120), "join.deadline"),
        backoff_base=_parse_duration(
            join_raw.get("backoff_base", 2), "join.backoff_base"
        ),
        backoff_max=_parse_duration(
            join_raw.get("backoff_max", 30), "join.backoff_max"
        ),
        max_concurrent=int(join_raw.get("max_concurrent", 4)),
    )

    # [drain]
    drain_raw = raw.get("drain", {})
    drain = _parse_drain(drain_raw)

    # [rebalance]
    rebalance_raw = raw.get("rebalance", {})
    rebalance = RebalanceConfig(
        max_concurrent_transfers=int(rebalance_raw.get("max_concurrent_transfers", 4)),
        max_chunk_bytes=int(rebalance_raw.get("max_chunk_bytes", 1_048_576)),
    )

    # [kv]
    kv_raw = raw.get("kv", {})
    kv = KvConfig(
        fanout_timeout_ms=int(kv_raw.get("fanout_timeout_ms", 50)),
        hint_replay_stabilization_ms=int(
            kv_raw.get("hint_replay_stabilization_ms", 2000)
        ),
        hint_replay_concurrency=int(kv_raw.get("hint_replay_concurrency", 16)),
    )
    try:
        kv.validate()
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc

    return TourillonConfig(
        node_id=node_id,
        node_size=node_size,
        data_dir=data_dir,
        tls=tls,
        kv_server=kv_server,
        peer_server=peer_server,
        seeds=seeds,
        rf=rf,
        partition_shift=partition_shift,
        join=join,
        drain=drain,
        rebalance=rebalance,
        kv=kv,
        schema_version=schema_version,
    )


def _parse_servers(raw: dict[str, Any]) -> tuple[ServerConfig, ServerConfig]:
    """Parse [servers.kv] and [servers.peer] returning (kv_server, peer_server)."""
    _require(raw, "servers")
    kv_raw = _require(raw, "servers", "kv")
    peer_raw = _require(raw, "servers", "peer")
    kv_bind: str = _require(raw, "servers", "kv", "bind")
    peer_bind: str = _require(raw, "servers", "peer", "bind")
    if kv_bind == peer_bind:
        raise ConfigError(
            "servers.kv and servers.peer cannot share the same bind address"
        )
    return (
        ServerConfig(bind=kv_bind, advertise=kv_raw.get("advertise", "")),
        ServerConfig(bind=peer_bind, advertise=peer_raw.get("advertise", "")),
    )


def _parse_drain(drain_raw: dict[str, Any]) -> DrainConfig:
    """Parse the [drain] config section."""
    return DrainConfig(
        max_retries=int(drain_raw.get("max_retries", -1)),
        attempt_timeout=_parse_duration(
            drain_raw.get("attempt_timeout", 30), "drain.attempt_timeout"
        ),
        deadline=_parse_duration(drain_raw.get("deadline", 300), "drain.deadline"),
        backoff_base=_parse_duration(
            drain_raw.get("backoff_base", 5), "drain.backoff_base"
        ),
        backoff_max=_parse_duration(
            drain_raw.get("backoff_max", 60), "drain.backoff_max"
        ),
        max_concurrent=int(drain_raw.get("max_concurrent", 4)),
        bandwidth_fraction=float(drain_raw.get("bandwidth_fraction", 1.0)),
    )


def load_config_file(path: Path) -> TourillonConfig:
    """Read *path* as TOML, apply environment-variable overrides, return TourillonConfig.

    Environment variables (higher precedence than file values):
      TOURILLON_NODE_ID        overrides [node].id
      TOURILLON_DATA_DIR       overrides [node].data_dir
    """
    try:
        raw: dict[str, Any] = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ConfigError(f"Config file not found: {path}") from None
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Config file parse error: {exc}") from exc

    # Environment-variable overrides
    if env_id := os.environ.get("TOURILLON_NODE_ID"):
        raw.setdefault("node", {})["id"] = env_id
    if env_data := os.environ.get("TOURILLON_DATA_DIR"):
        raw.setdefault("node", {})["data_dir"] = env_data

    return load_config(raw)
