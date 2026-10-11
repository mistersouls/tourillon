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

from pathlib import Path

import pytest

from tourillon.bootstrap.main import app
from tourillon.core.mtls import CertIssuer
from tourlib.adapter import CryptographyMtlsAdapter


@pytest.mark.tourillon
def test_generate_ca_rejects_validity_days_below_one() -> None:
    adapter = CryptographyMtlsAdapter()
    with pytest.raises(ValueError, match="validity_days must be >= 1"):
        adapter.generate_ca(
            common_name="ca", organization="org", country=None,
            validity_days=0, key_size=2048,
        )


@pytest.mark.tourillon
def test_generate_ca_rejects_key_size_below_2048() -> None:
    adapter = CryptographyMtlsAdapter()
    with pytest.raises(ValueError, match="key_size must be >= 2048"):
        adapter.generate_ca(
            common_name="ca", organization="org", country=None,
            validity_days=1, key_size=1024,
        )


@pytest.mark.tourillon
def test_generate_cert_rejects_empty_common_name(tmp_path: Path) -> None:
    adapter = CryptographyMtlsAdapter()
    ca_cert, ca_key = adapter.generate_ca(
        common_name="ca", organization="org", country=None,
        validity_days=1, key_size=2048,
    )
    with pytest.raises(ValueError, match="common_name must not be empty"):
        adapter.generate_cert(
            common_name="",
            ca_certificate_pem=ca_cert, ca_private_key_pem=ca_key,
            validity_days=1, key_size=2048,
        )


@pytest.mark.tourillon
def test_generate_cert_rejects_validity_days_below_one(tmp_path: Path) -> None:
    adapter = CryptographyMtlsAdapter()
    ca_cert, ca_key = adapter.generate_ca(
        common_name="ca", organization="org", country=None,
        validity_days=1, key_size=2048,
    )
    with pytest.raises(ValueError, match="validity_days must be >= 1"):
        adapter.generate_cert(
            common_name="node",
            ca_certificate_pem=ca_cert, ca_private_key_pem=ca_key,
            validity_days=0, key_size=2048,
        )


@pytest.mark.tourillon
def test_generate_cert_rejects_key_size_below_2048(tmp_path: Path) -> None:
    adapter = CryptographyMtlsAdapter()
    ca_cert, ca_key = adapter.generate_ca(
        common_name="ca", organization="org", country=None,
        validity_days=1, key_size=2048,
    )
    with pytest.raises(ValueError, match="key_size must be >= 2048"):
        adapter.generate_cert(
            common_name="node",
            ca_certificate_pem=ca_cert, ca_private_key_pem=ca_key,
            validity_days=1, key_size=1024,
        )


@pytest.mark.tourillon
def test_generate_ca_use_case_writes_certificate_and_key(tmp_path: Path) -> None:
    use_case = CertIssuer(CryptographyMtlsAdapter())

    result = use_case.ca(
        output_dir=tmp_path,
        common_name="tourillon-test-ca",
        organization="Tourillon Tests",
        country="FR",
        validity_days=30,
        key_size=2048,
        certificate_filename="ca.pem",
        private_key_filename="ca-key.pem",
        overwrite=False,
    )

    cert_bytes = result.certificate_path.read_bytes()
    key_bytes = result.private_key_path.read_bytes()

    assert result.certificate_path.exists()
    assert result.private_key_path.exists()
    assert cert_bytes.startswith(b"-----BEGIN CERTIFICATE-----")
    assert key_bytes.startswith(b"-----BEGIN PRIVATE KEY-----")


@pytest.mark.tourillon
def test_generate_ca_use_case_refuses_overwrite_by_default(tmp_path: Path) -> None:
    use_case = CertIssuer(CryptographyMtlsAdapter())
    (tmp_path / "ca-cert.pem").write_bytes(b"already-there")

    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        use_case.ca(
            output_dir=tmp_path,
            common_name="tourillon-test-ca",
            organization="Tourillon Tests",
            country="FR",
            validity_days=30,
            key_size=2048,
            certificate_filename="ca-cert.pem",
            private_key_filename="ca-key.pem",
            overwrite=False,
        )


@pytest.mark.tourillon
def test_cli_tourillon_config_ca(tmp_path: Path) -> None:
    result = app(
        [
            "config",
            "ca",
            "--output-dir",
            str(tmp_path),
            "--common-name",
            "tourillon-cli-ca",
            "--organization",
            "Tourillon CLI",
            "--country",
            "FR",
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

    cert_path = tmp_path / "ca-cert.pem"
    key_path = tmp_path / "ca-key.pem"

    assert "Generated mTLS CA files" in result
    assert cert_path.exists()
    assert key_path.exists()

