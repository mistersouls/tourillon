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
"""Edge-case tests for bootstrap config parsing and protocol-only modules."""

from __future__ import annotations

import base64
from pathlib import Path

import pytest

from tourillon.bootstrap.config import ConfigError, load_config
from tourillon.core.ports import serializer as serializer_port
from tourillon.core.ports import transport as transport_port
from tourillon.core.ports.pki import CaRequest, CertRequest
from tourillon.infra.pki.x509 import (
    CryptographyCaAdapter,
    CryptographyCertIssuerAdapter,
)


@pytest.mark.bootstrap
def test_load_config_missing_sections(tmp_path):
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        "schema_version=1\n[node]\nid='n1'\nsize='M'\ndata_dir='x'\n", encoding="utf-8"
    )
    with pytest.raises(ConfigError, match="Missing mandatory section"):
        load_config(cfg)


@pytest.mark.bootstrap
def test_load_config_invalid_toml(tmp_path):
    cfg = tmp_path / "config.toml"
    cfg.write_text("not = [valid", encoding="utf-8")
    with pytest.raises(ConfigError, match="Failed to parse TOML"):
        load_config(cfg)


@pytest.mark.bootstrap
def test_load_config_legacy_schema_and_validation_paths(tmp_path):
    ca = tmp_path / "ca.pem"
    ca_key = tmp_path / "ca-key.pem"
    node_cert = tmp_path / "node.pem"
    node_key = tmp_path / "node-key.pem"

    ca_adapter = CryptographyCaAdapter()
    ca_adapter.generate_ca(
        CaRequest(
            common_name="ca",
            valid_days=3650,
            key_size=2048,
            out_cert=ca,
            out_key=ca_key,
        )
    )

    issuer = CryptographyCertIssuerAdapter()
    issuer.issue_cert(
        CertRequest(
            common_name="node",
            san_dns=(),
            san_ip=(),
            valid_days=365,
            ca_cert=ca,
            ca_key=ca_key,
            out_cert=node_cert,
            out_key=node_key,
        )
    )

    cert_b64 = base64.b64encode(node_cert.read_bytes()).decode("ascii")
    key_b64 = base64.b64encode(node_key.read_bytes()).decode("ascii")
    ca_b64 = base64.b64encode(ca.read_bytes()).decode("ascii")

    legacy = tmp_path / "legacy.toml"
    legacy.write_text(
        f"""\
schema_version = 1

[node]
id = "node-legacy"
size = "M"
data_dir = "{tmp_path.as_posix()}"

[kv_server]
bind = "0.0.0.0:7000"

[peer_server]
bind = "0.0.0.0:7001"
advertise = "127.0.0.1:7001"

[tls]
cert_data = "{cert_b64}"
key_data = "{key_b64}"
ca_data = "{ca_b64}"

[join]
attempt_timeout = "10s"

[rebalance]
max_chunk_bytes = "1Mi"
""",
        encoding="utf-8",
    )
    cfg = load_config(legacy)
    assert cfg.peer_server.advertise == "127.0.0.1:7001"
    assert cfg.kv_server.advertise == ""

    bad_duration = tmp_path / "bad-duration.toml"
    bad_duration.write_text(
        f"""\
schema_version = 1

[node]
id = "node-duration"
size = "M"
data_dir = "{tmp_path.as_posix()}"

[servers.kv]
bind = "0.0.0.0:7000"

[servers.peer]
bind = "0.0.0.0:7001"
advertise = "127.0.0.1:7001"

[tls]
cert_data = "{cert_b64}"
key_data = "{key_b64}"
ca_data = "{ca_b64}"

[join]
attempt_timeout = "10x"
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="Invalid duration"):
        load_config(bad_duration)

    bad_size = tmp_path / "bad-size.toml"
    bad_size.write_text(
        f"""\
schema_version = 1

[node]
id = "node-size"
size = "M"
data_dir = "{tmp_path.as_posix()}"

[servers.kv]
bind = "0.0.0.0:7000"

[servers.peer]
bind = "0.0.0.0:7001"
advertise = "127.0.0.1:7001"

[tls]
cert_data = "{cert_b64}"
key_data = "{key_b64}"
ca_data = "{ca_b64}"

[rebalance]
max_chunk_bytes = "100MB"
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="Invalid size"):
        load_config(bad_size)


@pytest.mark.bootstrap
def test_protocol_modules_are_importable_and_typed():
    assert hasattr(serializer_port, "SerializerPort")
    assert transport_port.MAX_PAYLOAD_DEFAULT > 0
    assert transport_port.READ_TIMEOUT > 0
    assert Path.__name__ == "Path"
