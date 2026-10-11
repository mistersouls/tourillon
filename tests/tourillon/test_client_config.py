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
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID
from cyclopts.exceptions import MissingArgumentError

from tests.helpers import generate_ca
from tourillon.bootstrap.main import app
from tourillon.core.config import ClientConfigGenerator
from tourlib import ClientConfig
from tourlib.adapter import CryptographyMtlsAdapter, TomlPersistence


@pytest.mark.tourillon
def test_generate_client_config_use_case_writes_toml_with_base64_material(
    tmp_path: Path,
) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = ClientConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    output_file = tmp_path / "client.toml"

    result = asyncio.run(
        generator.generate(
            output_file=output_file,
            common_name="tourctl-a",
            ca_certificate_path=ca_cert_path,
            ca_private_key_path=ca_key_path,
            validity_days=30,
            key_size=2048,
            overwrite=False,
        )
    )

    doc = asyncio.run(TomlPersistence().load(result.config_path))
    config = ClientConfig.from_document(doc)

    auth = doc["auth"]["tourctl-a"]
    cert_pem = base64.b64decode(auth["cert_data"])
    cert = x509.load_pem_x509_certificate(cert_pem)
    eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value

    assert result.config_path.exists()
    assert doc["version"] == "1"
    assert doc["current-context"] == "tourctl-a"
    assert doc["contexts"]["tourctl-a"] == {
        "cluster": "tourctl-a",
        "namespace": "default",
        "auth": "tourctl-a",
    }
    assert doc["clusters"]["tourctl-a"] == {
        "client": "127.0.0.1:9000",
        "peer": "127.0.0.1:9001",
    }
    assert base64.b64decode(auth["ca_data"]).startswith(
        b"-----BEGIN CERTIFICATE-----"
    )
    assert cert_pem.startswith(b"-----BEGIN CERTIFICATE-----")
    assert base64.b64decode(auth["key_data"]).startswith(
        b"-----BEGIN PRIVATE KEY-----"
    )
    assert ExtendedKeyUsageOID.CLIENT_AUTH in eku
    assert ExtendedKeyUsageOID.SERVER_AUTH not in eku
    assert isinstance(asyncio.run(config.tls_client()), object)


@pytest.mark.tourillon
def test_generate_client_config_use_case_refuses_overwrite_by_default(
    tmp_path: Path,
) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = ClientConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    output_file = tmp_path / "client.toml"
    output_file.write_text("already-there", encoding="utf-8")

    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        asyncio.run(
            generator.generate(
                output_file=output_file,
                common_name="tourctl-a",
                ca_certificate_path=ca_cert_path,
                ca_private_key_path=ca_key_path,
                validity_days=30,
                key_size=2048,
                overwrite=False,
            )
        )


@pytest.mark.tourillon
def test_generate_client_config_use_case_supports_custom_cluster_addresses(
    tmp_path: Path,
) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    generator = ClientConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    output_file = tmp_path / "client.toml"

    asyncio.run(
        generator.generate(
            output_file=output_file,
            common_name="tourctl-custom",
            ca_certificate_path=ca_cert_path,
            ca_private_key_path=ca_key_path,
            validity_days=30,
            key_size=2048,
            client_address="10.1.2.3:9100",
            peer_address="10.1.2.3:9101",
            overwrite=False,
        )
    )

    doc = asyncio.run(TomlPersistence().load(output_file))

    assert doc["clusters"]["tourctl-custom"] == {
        "client": "10.1.2.3:9100",
        "peer": "10.1.2.3:9101",
    }


@pytest.mark.tourillon
def test_cli_tourillon_config_client(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    output_file = tmp_path / "client.toml"

    result = app(
        [
            "config",
            "client",
            "--output-file",
            str(output_file),
            "--ca-cert",
            str(ca_cert_path),
            "--ca-key",
            str(ca_key_path),
            "--common-name",
            "tourillon-cli-client",
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

    assert "Generated client config file" in result
    assert output_file.exists()
    assert doc["version"] == "1"
    assert "contexts" in doc
    assert "clusters" in doc
    assert "auth" in doc


@pytest.mark.tourillon
def test_cli_tourillon_config_client_requires_common_name(tmp_path: Path) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    output_file = tmp_path / "client.toml"

    with pytest.raises(MissingArgumentError, match="--common-name"):
        app(
            [
                "config",
                "client",
                "--output-file",
                str(output_file),
                "--ca-cert",
                str(ca_cert_path),
                "--ca-key",
                str(ca_key_path),
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


@pytest.mark.tourillon
def test_client_config_roundtrip_multi_cluster_schema() -> None:
    config = ClientConfig.from_document(
        {
            "version": "1",
            "current-context": "prod-eu",
            "contexts": {
                "prod-eu": {"cluster": "prod-eu", "namespace": "default", "auth": "souls"},
                "staging-us": {"cluster": "staging-us", "namespace": "sandbox", "auth": "souls"},
            },
            "clusters": {
                "prod-eu": {"client": "eu1.tourillon.io:9000", "peer": "eu1.tourillon.io:9001"},
                "staging-us": {"client": "us-staging.tourillon.io:9000", "peer": "us-staging.tourillon.io:9001"},
            },
            "auth": {
                "souls": {
                    "ca_data": "Y2E=",
                    "cert_data": "Y2VydA==",
                    "key_data": "a2V5",
                }
            },
        }
    )

    assert config.current_context == "prod-eu"
    assert config.context("staging-us").namespace == "sandbox"
    assert config.cluster("staging-us").peer == "us-staging.tourillon.io:9001"
    assert config.auth_config().ca_data == "Y2E="


@pytest.mark.tourillon
def test_client_config_rejects_unknown_current_context() -> None:
    with pytest.raises(
        ValueError, match="current-context must reference an existing context"
    ):
        ClientConfig.from_document(
            {
                "version": "1",
                "current-context": "missing",
                "contexts": {
                    "ctx": {"cluster": "cluster", "namespace": "default", "auth": "auth"}
                },
                "clusters": {"cluster": {"client": "127.0.0.1:9000", "peer": "127.0.0.1:9001"}},
                "auth": {"auth": {"ca_data": "Y2E=", "cert_data": "Y2VydA==", "key_data": "a2V5"}},
            }
        )


@pytest.mark.tourillon
def test_client_config_rejects_unknown_cluster_reference() -> None:
    with pytest.raises(ValueError, match="references unknown cluster"):
        ClientConfig.from_document(
            {
                "version": "1",
                "current-context": "ctx",
                "contexts": {
                    "ctx": {
                        "cluster": "missing-cluster",
                        "namespace": "default",
                        "auth": "auth",
                    }
                },
                "clusters": {"cluster": {"client": "127.0.0.1:9000", "peer": "127.0.0.1:9001"}},
                "auth": {"auth": {"ca_data": "Y2E=", "cert_data": "Y2VydA==", "key_data": "a2V5"}},
            }
        )


@pytest.mark.tourillon
def test_client_config_rejects_unknown_auth_reference() -> None:
    with pytest.raises(ValueError, match="references unknown auth"):
        ClientConfig.from_document(
            {
                "version": "1",
                "current-context": "ctx",
                "contexts": {
                    "ctx": {
                        "cluster": "cluster",
                        "namespace": "default",
                        "auth": "missing-auth",
                    }
                },
                "clusters": {"cluster": {"client": "127.0.0.1:9000", "peer": "127.0.0.1:9001"}},
                "auth": {"auth": {"ca_data": "Y2E=", "cert_data": "Y2VydA==", "key_data": "a2V5"}},
            }
        )


@pytest.mark.tourillon
def test_client_auth_write_material_rejects_invalid_base64(tmp_path: Path) -> None:
    config = ClientConfig.from_document(
        {
            "version": "1",
            "current-context": "ctx",
            "contexts": {
                "ctx": {"cluster": "cluster", "namespace": "default", "auth": "bad"}
            },
            "clusters": {"cluster": {"client": "127.0.0.1:9000", "peer": "127.0.0.1:9001"}},
            "auth": {
                "bad": {
                    "ca_data": "not-base64!",
                    "cert_data": "Y2VydA==",
                    "key_data": "a2V5",
                }
            },
        }
    )

    with pytest.raises(ValueError, match="ca_data must be valid base64 data"):
        config.auth_config()._write_material_sync(tmp_path)


