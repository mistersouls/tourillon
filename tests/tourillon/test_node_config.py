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
import re
from pathlib import Path

import pytest

from tests.helpers import generate_ca
from tourillon.bootstrap.main import app
from tourillon.core.config import (
    GossipConfig,
    LoggerConfig,
    LoggingConfig,
    NodeConfig,
    NodeConfigGenerator,
    NodeSize,
    PeerServerConfig,
    PlacementConfig,
    ReadRepairConfig,
    RepairConfig,
    StorageConfig,
    TelemetryConfig,
    TlsConfig,
    TourillonConfig,
    TransportConfig,
    TransportPoolConfig,
)
from tourillon.core.models import config as config_model
from tourillon.core.models.retry import BackoffRetry
from tourlib.adapter import CryptographyMtlsAdapter, TomlPersistence
from tourlib.exceptions import ValidationError


@pytest.mark.tourillon
def test_generate_node_config_use_case_writes_toml_with_base64_material(
    tmp_path: Path,
) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = NodeConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    output_file = tmp_path / "node.toml"

    result = asyncio.run(
        generator.generate(
            output_file=output_file,
            node_id="node-a",
            ca_certificate_path=ca_cert_path,
            ca_private_key_path=ca_key_path,
            data_dir="data",
            group_size="8Mi",
            validity_days=30,
            key_size=2048,
            overwrite=False,
        )
    )

    doc = asyncio.run(TomlPersistence().load(result.config_path))
    config = TourillonConfig.from_document(doc)

    assert result.config_path.exists()
    assert doc["node"]["id"] == "node-a"
    assert doc["storage"]["data_dir"] == "data"
    assert base64.b64decode(doc["tls"]["ca_data"]).startswith(
        b"-----BEGIN CERTIFICATE-----"
    )
    assert base64.b64decode(doc["tls"]["cert_data"]).startswith(
        b"-----BEGIN CERTIFICATE-----"
    )
    assert base64.b64decode(doc["tls"]["key_data"]).startswith(
        b"-----BEGIN PRIVATE KEY-----"
    )
    assert config.node.id == "node-a"
    assert config.telemetry == TelemetryConfig()


@pytest.mark.tourillon
def test_generate_node_config_use_case_refuses_overwrite_by_default(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = NodeConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    output_file = tmp_path / "node.toml"
    output_file.write_text("already-there", encoding="utf-8")

    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        asyncio.run(
            generator.generate(
                output_file=output_file,
                node_id="node-a",
                ca_certificate_path=ca_cert_path,
                ca_private_key_path=ca_key_path,
                data_dir="data",
            group_size="8Mi",
                validity_days=30,
                key_size=2048,
                overwrite=False,
            )
        )


@pytest.mark.tourillon
def test_cli_tourillon_config_node(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    output_file = tmp_path / "my-node.toml"

    result = app(
        [
            "config",
            "node",
            "node-cli",
            "--output-file",
            str(output_file),
            "--ca-cert",
            str(ca_cert_path),
            "--ca-key",
            str(ca_key_path),
            "--data-dir",
            "data",
            "--group-size",
            "8Mi",
            "--node-size",
            "XL",
            "--validity-days",
            "5",
            "--key-size",
            "2048",
        ],
        exit_on_error=False,
        print_error=False,
        help_on_error=False,
        result_action="return_value",
    )

    doc = asyncio.run(TomlPersistence().load(output_file))

    assert "Generated node config file" in result
    assert output_file.exists()
    assert doc["node"]["id"] == "node-cli"
    assert doc["node"]["size"] == "XL"
    assert doc["storage"]["data_dir"] == "data"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_tourillon_config_tls_contexts_enable_mtls(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = NodeConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    output_file = tmp_path / "node.toml"

    result = await generator.generate(
        output_file=output_file,
        node_id="node-a",
        ca_certificate_path=ca_cert_path,
        ca_private_key_path=ca_key_path,
        data_dir="data",
            group_size="8Mi",
        validity_days=30,
        key_size=2048,
        overwrite=False,
    )
    config = TourillonConfig.from_document(await TomlPersistence().load(result.config_path))
    server_ssl = await config.tls_server()
    client_ssl = await config.tls_client()
    received = bytearray()

    async def handle_client(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        received.extend(await reader.readexactly(1))
        writer.write(b"y")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle_client, host="127.0.0.1", port=0, ssl=server_ssl)
    try:
        socket = server.sockets[0]
        host, port = socket.getsockname()[:2]
        reader, writer = await asyncio.open_connection(
            host=host,
            port=port,
            ssl=client_ssl,
            server_hostname="node-a",
        )
        writer.write(b"x")
        await writer.drain()

        assert await reader.readexactly(1) == b"y"
        assert received == b"x"

        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_tourillon_config_tls_context_removes_temp_material_immediately(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = NodeConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    output_file = tmp_path / "node.toml"

    result = await generator.generate(
        output_file=output_file,
        node_id="node-a",
        ca_certificate_path=ca_cert_path,
        ca_private_key_path=ca_key_path,
        data_dir="data",
            group_size="8Mi",
        validity_days=30,
        key_size=2048,
        overwrite=False,
    )
    config = TourillonConfig.from_document(await TomlPersistence().load(result.config_path))
    temp_dir = tmp_path / "tls-material"

    def fake_mkdtemp(*, prefix: str) -> str:
        assert prefix == "tourillon-tls-"
        temp_dir.mkdir()
        return str(temp_dir)

    monkeypatch.setattr(config_model.tempfile, "mkdtemp", fake_mkdtemp)

    server_ssl = await config.tls_server()

    assert isinstance(server_ssl, object)
    assert not temp_dir.exists()


@pytest.mark.tourillon
def test_tourillon_config_from_document_rejects_missing_node_table() -> None:
    with pytest.raises(ValidationError, match="node must be a table"):
        TourillonConfig.from_document(
            {
                "tls": {
                    "ca_data": "Y2E=",
                    "cert_data": "Y2VydA==",
                    "key_data": "a2V5",
                },
                "storage": {
                    "data_dir": "data",
                },
                "servers": {
                    "client": {
                        "bind": "127.0.0.1:7000",
                    },
                    "peer": {
                        "bind": "127.0.0.1:7001",
                        "advertise": "127.0.0.1:7001",
                    },
                },
            }
        )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_tls_config_rejects_invalid_base64_material() -> None:
    with pytest.raises(ValueError, match="ca_data must be valid base64 data"):
        await TlsConfig(
            ca_data="not-base64!",
            cert_data="Y2VydA==",
            key_data="a2V5",
        ).tls_server()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_toml_persistence_round_trips_typed_values(tmp_path: Path) -> None:
    path = tmp_path / "generic.toml"
    persistence = TomlPersistence()

    await persistence.write(
        path,
        {
            "name": "node-a",
            "enabled": True,
            "retries": 3,
            "ports": [7000, 7001],
        },
    )

    document = await persistence.load(path)

    assert document == {
        "name": "node-a",
        "enabled": True,
        "retries": 3,
        "ports": [7000, 7001],
    }


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_toml_persistence_patch_updates_fields_under_lock(tmp_path: Path) -> None:
    path = tmp_path / "patch.toml"
    persistence = TomlPersistence()
    await persistence.write(path, {"node_id": "node-a", "retries": 1, "enabled": False})

    async with persistence.edit(path) as document:
        document["retries"] = 2
        document["enabled"] = True

    document = await persistence.load(path)

    assert document == {"node_id": "node-a", "retries": 2, "enabled": True}


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_config_with_seeds(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = NodeConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    output_file = tmp_path / "node.toml"

    result = await generator.generate(
        output_file=output_file,
        node_id="node-a",
        ca_certificate_path=ca_cert_path,
        ca_private_key_path=ca_key_path,
        data_dir="data",
            group_size="8Mi",
        seeds=["seed1:9001", "seed2:9001"],
        validity_days=30,
        key_size=2048,
        overwrite=False,
    )

    # Reload and verify seeds and servers
    doc = await TomlPersistence().load(result.config_path)
    config = TourillonConfig.from_document(doc)

    assert config.node.id == "node-a"
    assert config.node.seeds == ["seed1:9001", "seed2:9001"]
    assert config.storage.data_dir == "data"
    assert config.server_client.bind == "127.0.0.1:7000"
    assert config.server_peer.bind == "127.0.0.1:7001"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_config_generate_rejects_invalid_seeds(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = NodeConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )

    with pytest.raises(ValidationError, match="seeds must be a list of non-empty strings"):
        await generator.generate(
            output_file=tmp_path / "node.toml",
            node_id="node-a",
            ca_certificate_path=ca_cert_path,
            ca_private_key_path=ca_key_path,
            data_dir="data",
            group_size="8Mi",
            seeds=["seed1:9001", ""],
            validity_days=30,
            key_size=2048,
            overwrite=False,
        )



@pytest.mark.tourillon
def test_node_config_from_document_with_seeds() -> None:
    doc = {
        "node": {"id": "node-x", "seeds": ["seed1:9001", "seed2:9001"]},
        "storage": {"data_dir": "data", "group_size": "8Mi"},
        "tls": {
            "ca_data": "Y2E=",
            "cert_data": "Y2VydA==",
            "key_data": "a2V5",
        },
        "servers": {
            "client": {
                "bind": "127.0.0.1:7000",
            },
            "peer": {
                "bind": "127.0.0.1:7001",
                "advertise": "127.0.0.1:7001",
            },
        },
    }
    config = TourillonConfig.from_document(doc)

    assert config.node.id == "node-x"
    assert config.node.seeds == ["seed1:9001", "seed2:9001"]
    assert config.storage.data_dir == "data"


@pytest.mark.tourillon
def test_node_config_from_document_without_seeds() -> None:
    doc = {
        "node": {"id": "node-y"},
        "storage": {"data_dir": "data", "group_size": "8Mi"},
        "tls": {
            "ca_data": "Y2E=",
            "cert_data": "Y2VydA==",
            "key_data": "a2V5",
        },
        "servers": {
            "client": {
                "bind": "127.0.0.1:7000",
            },
            "peer": {
                "bind": "127.0.0.1:7001",
                "advertise": "127.0.0.1:7001",
            },
        },
    }
    config = TourillonConfig.from_document(doc)

    assert config.node.id == "node-y"
    assert config.node.seeds is None
    assert config.storage.data_dir == "data"


@pytest.mark.tourillon
def test_node_config_to_document_with_seeds() -> None:
    node = NodeConfig(id="node-z", seeds=["seed1:9001"])
    doc = node.to_document()

    assert doc == {"id": "node-z", "size": "M", "seeds": ["seed1:9001"]}


@pytest.mark.tourillon
def test_node_config_to_document_without_seeds() -> None:
    node = NodeConfig(id="node-w")
    doc = node.to_document()

    assert doc == {"id": "node-w", "size": "M"}


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_config_generate_rejects_invalid_node_size(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = NodeConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )

    with pytest.raises(ValidationError, match="node_size must be one of"):
        await generator.generate(
            output_file=tmp_path / "node.toml",
            node_id="node-a",
            ca_certificate_path=ca_cert_path,
            ca_private_key_path=ca_key_path,
            data_dir="data",
            group_size="8Mi",
            node_size="HUGE",  # type: ignore[arg-type]
            validity_days=30,
            key_size=2048,
            overwrite=False,
        )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_config_generate_rejects_node_id_over_63_bytes(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = NodeConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )

    with pytest.raises(ValidationError, match="node_id must be <= 63 bytes"):
        await generator.generate(
            output_file=tmp_path / "node.toml",
            node_id="a" * 64,
            ca_certificate_path=ca_cert_path,
            ca_private_key_path=ca_key_path,
            data_dir="data",
            group_size="8Mi",
            validity_days=30,
            key_size=2048,
            overwrite=False,
        )


@pytest.mark.tourillon
def test_node_config_from_document_defaults_size_when_missing() -> None:
    config = NodeConfig.from_document({"id": "node-x"})

    assert config.size == NodeSize.M


@pytest.mark.tourillon
def test_node_config_from_document_rejects_invalid_size() -> None:
    with pytest.raises(ValidationError, match="size must be one of"):
        NodeConfig.from_document({"id": "node-x", "size": "GIANT"})


@pytest.mark.tourillon
def test_node_config_from_document_rejects_node_id_over_63_bytes() -> None:
    with pytest.raises(ValidationError, match="node.id must be <= 63 bytes"):
        NodeConfig.from_document({"id": "a" * 64})


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_config_from_document_seeds_validation() -> None:
    """Test NodeConfig validates seeds is a list."""
    # Seeds must be a list, not a string
    with pytest.raises(ValidationError, match="seeds must be a list of strings"):
        NodeConfig.from_document(
            {"id": "node-x", "seeds": "not-a-list"}
        )


@pytest.mark.tourillon
def test_server_config_to_document() -> None:
    """Test ServerConfig serialization."""
    config = PeerServerConfig(bind="127.0.0.1:7001", advertise="127.0.0.1:7001")
    doc = config.to_document()

    assert doc == {
        "bind": "127.0.0.1:7001",
        "advertise": "127.0.0.1:7001",
    }


@pytest.mark.tourillon
def test_server_config_from_document() -> None:
    """Test ServerConfig deserialization."""
    doc = {
        "bind": "0.0.0.0:9001",
        "advertise": "192.168.1.100:9001",
    }
    config = PeerServerConfig.from_document(doc)

    assert config.bind == "0.0.0.0:9001"
    assert config.advertise == "192.168.1.100:9001"


@pytest.mark.tourillon
def test_tourillon_config_with_servers() -> None:
    """Test TourillonConfig with server configurations."""
    doc = {
        "node": {"id": "node-with-servers"},
        "storage": {"data_dir": "data", "group_size": "8Mi"},
        "tls": {
            "ca_data": "Y2E=",
            "cert_data": "Y2VydA==",
            "key_data": "a2V5",
        },
        "servers": {
            "client": {
                "bind": "127.0.0.1:7000",
            },
            "peer": {
                "bind": "127.0.0.1:7001",
                "advertise": "127.0.0.1:7001",
            },
        },
    }
    config = TourillonConfig.from_document(doc)

    assert config.node.id == "node-with-servers"
    assert config.storage.data_dir == "data"
    assert config.server_client.bind == "127.0.0.1:7000"
    assert config.server_peer.bind == "127.0.0.1:7001"
    assert config.server_peer.advertise == "127.0.0.1:7001"


@pytest.mark.tourillon
def test_tourillon_config_requires_servers_client() -> None:
    """Test TourillonConfig requires servers.client configuration."""
    doc = {
        "node": {"id": "node-x"},
        "storage": {"data_dir": "data", "group_size": "8Mi"},
        "tls": {
            "ca_data": "Y2E=",
            "cert_data": "Y2VydA==",
            "key_data": "a2V5",
        },
        "servers": {
            "peer": {
                "bind": "127.0.0.1:7001",
                "advertise": "127.0.0.1:7001",
            },
        },
    }
    with pytest.raises(ValidationError, match="servers.client configuration is required"):
        TourillonConfig.from_document(doc)


@pytest.mark.tourillon
def test_tourillon_config_requires_servers_peer() -> None:
    """Test TourillonConfig requires servers.peer configuration."""
    doc = {
        "node": {"id": "node-x"},
        "storage": {"data_dir": "data", "group_size": "8Mi"},
        "tls": {
            "ca_data": "Y2E=",
            "cert_data": "Y2VydA==",
            "key_data": "a2V5",
        },
        "servers": {
            "client": {
                "bind": "127.0.0.1:7000",
            },
        },
    }
    with pytest.raises(ValidationError, match="servers.peer configuration is required"):
        TourillonConfig.from_document(doc)


@pytest.mark.tourillon
def test_tourillon_config_from_document_with_logging() -> None:
    doc = {
        "node": {"id": "node-log"},
        "storage": {"data_dir": "data", "group_size": "8Mi"},
        "tls": {
            "ca_data": "Y2E=",
            "cert_data": "Y2VydA==",
            "key_data": "a2V5",
        },
        "logging": {
            "level": "INFO",
            "loggers": {
                "global": {"level": "INFO", "enabled": True},
                "transport": {"level": "DEBUG", "enabled": True},
            },
        },
        "servers": {
            "client": {"bind": "127.0.0.1:7000"},
            "peer": {
                "bind": "127.0.0.1:7001",
                "advertise": "127.0.0.1:7001",
            },
        },
    }

    config = TourillonConfig.from_document(doc)

    assert config.logging.level == "INFO"
    assert config.logging.loggers["global"] == LoggerConfig(level="INFO", enabled=True)
    assert config.logging.loggers["transport"] == LoggerConfig(level="DEBUG", enabled=True)


@pytest.mark.tourillon
def test_tourillon_config_defaults_logging_when_missing() -> None:
    doc = {
        "node": {"id": "node-default-log"},
        "storage": {"data_dir": "data", "group_size": "8Mi"},
        "tls": {
            "ca_data": "Y2E=",
            "cert_data": "Y2VydA==",
            "key_data": "a2V5",
        },
        "servers": {
            "client": {"bind": "127.0.0.1:7000"},
            "peer": {
                "bind": "127.0.0.1:7001",
                "advertise": "127.0.0.1:7001",
            },
        },
    }

    config = TourillonConfig.from_document(doc)

    assert config.logging == LoggingConfig()


@pytest.mark.tourillon
def test_tourillon_config_from_document_with_telemetry() -> None:
    doc = {
        "node": {"id": "node-telemetry"},
        "storage": {"data_dir": "data", "group_size": "8Mi"},
        "tls": {
            "ca_data": "Y2E=",
            "cert_data": "Y2VydA==",
            "key_data": "a2V5",
        },
        "telemetry": {
            "metrics_enabled": True,
            "tracing_enabled": False,
            "service_name": "tourillon-node",
            "metrics_options": {"endpoint": "http://localhost:4318/v1/metrics"},
        },
        "servers": {
            "client": {"bind": "127.0.0.1:7000"},
            "peer": {
                "bind": "127.0.0.1:7001",
                "advertise": "127.0.0.1:7001",
            },
        },
    }

    config = TourillonConfig.from_document(doc)

    assert config.telemetry == TelemetryConfig(
        metrics_enabled=True,
        tracing_enabled=False,
        service_name="tourillon-node",
        metrics_options={"endpoint": "http://localhost:4318/v1/metrics"},
    )


@pytest.mark.tourillon
def test_tourillon_config_defaults_telemetry_when_missing() -> None:
    doc = {
        "node": {"id": "node-default-telemetry"},
        "storage": {"data_dir": "data", "group_size": "8Mi"},
        "tls": {
            "ca_data": "Y2E=",
            "cert_data": "Y2VydA==",
            "key_data": "a2V5",
        },
        "servers": {
            "client": {"bind": "127.0.0.1:7000"},
            "peer": {
                "bind": "127.0.0.1:7001",
                "advertise": "127.0.0.1:7001",
            },
        },
    }

    config = TourillonConfig.from_document(doc)

    assert config.telemetry == TelemetryConfig()


@pytest.mark.tourillon
def test_tourillon_config_defaults_gossip_and_transport_when_missing() -> None:
    doc = {
        "node": {"id": "node-default-runtime"},
        "storage": {"data_dir": "data", "group_size": "8Mi"},
        "tls": {
            "ca_data": "Y2E=",
            "cert_data": "Y2VydA==",
            "key_data": "a2V5",
        },
        "servers": {
            "client": {"bind": "127.0.0.1:7000"},
            "peer": {
                "bind": "127.0.0.1:7001",
                "advertise": "127.0.0.1:7001",
            },
        },
    }

    config = TourillonConfig.from_document(doc)

    assert config.gossip == GossipConfig()
    assert config.transport == TransportConfig()
    assert config.placement == PlacementConfig()


@pytest.mark.tourillon
def test_tourillon_config_from_document_with_gossip_and_transport_pool() -> None:
    doc = {
        "node": {"id": "node-runtime"},
        "storage": {"data_dir": "data", "group_size": "8Mi"},
        "tls": {
            "ca_data": "Y2E=",
            "cert_data": "Y2VydA==",
            "key_data": "a2V5",
        },
        "gossip": {
            "max_fan_out": 9,
            "hop_limit": 4,
            "ae_interval": 12.5,
            "stop_drain_timeout": 2.0,
        },
        "transport": {
            "pool": {
                "min_size": 4,
                "max_size": 64,
                "max_overflow": 16,
                "idle_ttl": 42,
            }
        },
        "placement": {
            "replication_factor": 5,
        },
        "servers": {
            "client": {"bind": "127.0.0.1:7000"},
            "peer": {
                "bind": "127.0.0.1:7001",
                "advertise": "127.0.0.1:7001",
            },
        },
    }

    config = TourillonConfig.from_document(doc)

    assert config.gossip == GossipConfig(
        max_fan_out=9,
        hop_limit=4,
        ae_interval=12.5,
        stop_drain_timeout=2.0,
    )
    assert config.transport.pool == TransportPoolConfig(
        min_size=4,
        max_size=64,
        max_overflow=16,
        idle_ttl=42.0,
    )
    assert config.placement == PlacementConfig(replication_factor=5)


@pytest.mark.tourillon
def test_tourillon_config_rejects_invalid_gossip_and_transport_pool_values() -> None:
    base_doc = {
        "node": {"id": "node-invalid-runtime"},
        "storage": {"data_dir": "data", "group_size": "8Mi"},
        "tls": {
            "ca_data": "Y2E=",
            "cert_data": "Y2VydA==",
            "key_data": "a2V5",
        },
        "servers": {
            "client": {"bind": "127.0.0.1:7000"},
            "peer": {
                "bind": "127.0.0.1:7001",
                "advertise": "127.0.0.1:7001",
            },
        },
    }

    with pytest.raises(ValidationError, match="gossip.ae_interval must be a number > 0"):
        TourillonConfig.from_document(
            {
                **base_doc,
                "gossip": {"ae_interval": 0},
            }
        )

    with pytest.raises(
        ValidationError,
        match="transport.pool.min_size must be <= transport.pool.max_size",
    ):
        TourillonConfig.from_document(
            {
                **base_doc,
                "transport": {
                    "pool": {
                        "min_size": 10,
                        "max_size": 1,
                    }
                },
            }
        )

    with pytest.raises(
        ValidationError,
        match="placement.replication_factor must be an integer >= 1",
    ):
        TourillonConfig.from_document(
            {
                **base_doc,
                "placement": {"replication_factor": 0},
            }
        )


@pytest.mark.tourillon
def test_telemetry_config_rejects_non_boolean_metrics_enabled_flag() -> None:
    with pytest.raises(ValidationError, match="telemetry.metrics_enabled must be a boolean"):
        TelemetryConfig.from_document(
            {
                "metrics_enabled": "yes",
                "tracing_enabled": False,
                "prefix": "tourillon",
            }
        )


@pytest.mark.tourillon
def test_telemetry_config_rejects_blank_service_name() -> None:
    with pytest.raises(ValidationError, match="telemetry.service_name must be a non-empty string"):
        TelemetryConfig.from_document(
            {
                "metrics_enabled": True,
                "tracing_enabled": True,
                "service_name": "   ",
            }
        )


@pytest.mark.tourillon
def test_logging_config_rejects_non_boolean_logger_enabled_flag() -> None:
    with pytest.raises(ValidationError, match="logging.loggers.global.enabled must be a boolean"):
        LoggingConfig.from_document(
            {
                "level": "INFO",
                "loggers": {
                    "global": {"level": "INFO", "enabled": "yes"},
                },
            }
        )


@pytest.mark.tourillon
def test_storage_config_to_document() -> None:
    config = StorageConfig(data_dir="/var/lib/tourillon", group_size="16Mi")

    assert config.to_document() == {
        "data_dir": "/var/lib/tourillon",
        "group_size": "16Mi",
    }


@pytest.mark.tourillon
def test_storage_config_from_document() -> None:
    config = StorageConfig.from_document({"data_dir": "C:/tourillon/data", "group_size": "32Mi"})

    assert config.data_dir == "C:/tourillon/data"
    assert config.group_size == "32Mi"


@pytest.mark.tourillon
def test_storage_config_from_document_accepts_ti_and_larger_units() -> None:
    config = StorageConfig.from_document({"data_dir": "C:/tourillon/data", "group_size": "2Ti"})

    assert config.group_size == "2Ti"

    bigger = StorageConfig.from_document({"data_dir": "C:/tourillon/data", "group_size": "1Pi"})

    assert bigger.group_size == "1Pi"


@pytest.mark.tourillon
def test_storage_config_from_document_accepts_relative_path() -> None:
    config = StorageConfig.from_document({"data_dir": "./data", "group_size": "8Mi"})

    assert config.data_dir == "./data"


@pytest.mark.tourillon
def test_storage_config_from_document_rejects_blank_path() -> None:
    with pytest.raises(ValidationError, match="storage.data_dir must be a non-empty directory path"):
        StorageConfig.from_document({"data_dir": "   ", "group_size": "8Mi"})


@pytest.mark.tourillon
def test_storage_config_from_document_rejects_existing_file_path(tmp_path: Path) -> None:
    path = tmp_path / "state.toml"
    path.write_text("x", encoding="utf-8")

    with pytest.raises(ValidationError, match="storage.data_dir must reference a directory path"):
        StorageConfig.from_document({"data_dir": str(path), "group_size": "8Mi"})


@pytest.mark.tourillon
def test_storage_config_from_document_rejects_non_positive_group_size() -> None:
    with pytest.raises(ValidationError, match="storage.group_size must be a positive size string"):
        StorageConfig.from_document({"data_dir": "data", "group_size": "0Mi"})


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_config_generate_rejects_data_dir_that_is_an_existing_file(
    tmp_path: Path,
) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = NodeConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    data_file = tmp_path / "data-file"
    data_file.write_text("not-a-directory", encoding="utf-8")

    with pytest.raises(ValidationError, match="storage.data_dir must reference a directory path"):
        await generator.generate(
            output_file=tmp_path / "node.toml",
            node_id="node-a",
            ca_certificate_path=ca_cert_path,
            ca_private_key_path=ca_key_path,
            data_dir=str(data_file),
            group_size="8Mi",
            validity_days=30,
            key_size=2048,
            overwrite=False,
        )


@pytest.mark.tourillon
def test_gossip_config_parses_bootstrap_retry_overrides() -> None:
    config = GossipConfig.from_document(
        {
            "max_fan_out": 2,
            "bootstrap_retry": {
                "initial_delay": 1.5,
                "max_delay": 20,
                "max_retries": 3,
                "factor": 3.0,
                "jitter": 0,
            },
        }
    )

    assert config.bootstrap_retry == BackoffRetry(
        initial_delay=1.5,
        max_delay=20.0,
        max_retries=3,
        factor=3.0,
        jitter=0.0,
    )
    assert config.to_document()["bootstrap_retry"]["max_retries"] == 3


@pytest.mark.tourillon
def test_gossip_config_defaults_bootstrap_retry_when_missing() -> None:
    assert GossipConfig.from_document({}).bootstrap_retry == BackoffRetry()


@pytest.mark.tourillon
@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"initial_delay": -1}, "gossip.bootstrap_retry.initial_delay must be a number >= 0"),
        ({"factor": "fast"}, "gossip.bootstrap_retry.factor must be a number >= 0"),
        ({"max_retries": 0}, "gossip.bootstrap_retry.max_retries must be an integer >= 1"),
        ({"max_retries": 1.5}, "gossip.bootstrap_retry.max_retries must be an integer >= 1"),
    ],
)
def test_gossip_config_rejects_invalid_bootstrap_retry(document: dict, message: str) -> None:
    with pytest.raises(ValidationError, match=re.escape(message)):
        GossipConfig.from_document({"bootstrap_retry": document})


@pytest.mark.tourillon
def test_read_repair_config_roundtrips_through_its_document() -> None:
    config = ReadRepairConfig(enabled=False, concurrency=4, sample_rate=0.25)

    assert ReadRepairConfig.from_document(config.to_document()) == config


@pytest.mark.tourillon
def test_read_repair_config_defaults_to_enabled_full_sampling() -> None:
    config = ReadRepairConfig.from_document({})

    assert config == ReadRepairConfig(enabled=True, concurrency=16, sample_rate=1.0)


@pytest.mark.tourillon
def test_placement_config_parses_a_nested_read_repair_table() -> None:
    config = PlacementConfig.from_document(
        {"replication_factor": 5, "read_repair": {"sample_rate": 0.5}}
    )

    assert config.replication_factor == 5
    assert config.read_repair.sample_rate == 0.5


@pytest.mark.tourillon
@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"enabled": "yes"}, "placement.read_repair.enabled must be a boolean"),
        ({"concurrency": 0}, "placement.read_repair.concurrency must be an integer >= 1"),
        ({"concurrency": True}, "placement.read_repair.concurrency must be an integer >= 1"),
        ({"sample_rate": 1.5}, r"placement.read_repair.sample_rate must be a number"),
        ({"sample_rate": "half"}, r"placement.read_repair.sample_rate must be a number"),
    ],
)
def test_read_repair_config_rejects_unusable_values(document: dict, message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        ReadRepairConfig.from_document(document)


@pytest.mark.tourillon
def test_placement_config_rejects_a_non_table_read_repair() -> None:
    with pytest.raises(ValidationError, match="placement.read_repair must be a table"):
        PlacementConfig.from_document({"read_repair": True})


@pytest.mark.tourillon
def test_repair_config_roundtrips_through_its_document() -> None:
    config = RepairConfig(concurrency=9)

    assert RepairConfig.from_document(config.to_document()) == config


@pytest.mark.tourillon
def test_repair_config_defaults_to_a_bounded_concurrency() -> None:
    assert RepairConfig.from_document({}) == RepairConfig(concurrency=4)


@pytest.mark.tourillon
def test_placement_config_parses_a_nested_repair_table() -> None:
    config = PlacementConfig.from_document({"repair": {"concurrency": 2}})

    assert config.repair.concurrency == 2


@pytest.mark.tourillon
@pytest.mark.parametrize("document", [{"concurrency": 0}, {"concurrency": True}, {"concurrency": "4"}])
def test_repair_config_rejects_unusable_concurrencies(document: dict) -> None:
    with pytest.raises(ValidationError, match="placement.repair.concurrency must be an integer >= 1"):
        RepairConfig.from_document(document)


@pytest.mark.tourillon
def test_placement_config_rejects_a_non_table_repair() -> None:
    with pytest.raises(ValidationError, match="placement.repair must be a table"):
        PlacementConfig.from_document({"repair": 4})
