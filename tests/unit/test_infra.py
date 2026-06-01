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
"""Tests for infrastructure adapters."""

import base64

import pytest

from tourillon.core.ports.pki import CaRequest, CertRequest
from tourillon.infra.pki.x509 import (
    CryptographyCaAdapter,
    CryptographyCertIssuerAdapter,
)
from tourillon.infra.tls.context import (
    TlsValidationError,
    build_client_ssl_context,
    build_server_ssl_context,
    validate_cert_key_match,
    validate_cert_not_expired,
)


@pytest.mark.bootstrap
class TestTLSValidation:
    """Tests for TLS certificate validation."""

    def test_validate_cert_with_valid_cert(self, tmp_path):
        """Test validation with a valid certificate."""
        # Generate a CA first
        ca_adapter = CryptographyCaAdapter()
        ca_req = CaRequest(
            common_name="test-ca",
            valid_days=3650,
            key_size=2048,
            out_cert=tmp_path / "ca.pem",
            out_key=tmp_path / "ca-key.pem",
        )
        ca_adapter.generate_ca(ca_req)

        # Read and base64 encode
        cert_pem = (tmp_path / "ca.pem").read_bytes()
        b64_cert = base64.b64encode(cert_pem).decode()

        # Should not raise
        validate_cert_not_expired(b64_cert)

    def test_validate_cert_key_match_valid(self, tmp_path):
        """Test certificate and key matching with valid pair."""
        # Generate CA
        ca_adapter = CryptographyCaAdapter()
        ca_req = CaRequest(
            common_name="test-ca",
            valid_days=3650,
            key_size=2048,
            out_cert=tmp_path / "ca.pem",
            out_key=tmp_path / "ca-key.pem",
        )
        ca_adapter.generate_ca(ca_req)

        # Read and encode
        cert_pem = (tmp_path / "ca.pem").read_bytes()
        key_pem = (tmp_path / "ca-key.pem").read_bytes()
        b64_cert = base64.b64encode(cert_pem).decode()
        b64_key = base64.b64encode(key_pem).decode()

        # Should not raise
        validate_cert_key_match(b64_cert, b64_key)

    def test_validate_cert_key_mismatch(self, tmp_path):
        """Test certificate and key with mismatch raises."""
        # Generate two CAs
        ca_adapter = CryptographyCaAdapter()
        ca_req1 = CaRequest(
            common_name="ca1",
            valid_days=3650,
            key_size=2048,
            out_cert=tmp_path / "ca1.pem",
            out_key=tmp_path / "ca1-key.pem",
        )
        ca_adapter.generate_ca(ca_req1)

        ca_req2 = CaRequest(
            common_name="ca2",
            valid_days=3650,
            key_size=2048,
            out_cert=tmp_path / "ca2.pem",
            out_key=tmp_path / "ca2-key.pem",
        )
        ca_adapter.generate_ca(ca_req2)

        # Mix cert from one with key from another
        cert_pem = (tmp_path / "ca1.pem").read_bytes()
        key_pem = (tmp_path / "ca2-key.pem").read_bytes()
        b64_cert = base64.b64encode(cert_pem).decode()
        b64_key = base64.b64encode(key_pem).decode()

        # Should raise
        with pytest.raises(TlsValidationError):
            validate_cert_key_match(b64_cert, b64_key)

    def test_validate_invalid_base64(self):
        """Test validation with invalid base64."""
        with pytest.raises(TlsValidationError):
            validate_cert_not_expired("not-valid-base64!!!")

    def test_validate_invalid_pem(self):
        """Test validation with invalid PEM content."""
        invalid_pem = base64.b64encode(b"not a valid cert").decode()
        with pytest.raises(TlsValidationError):
            validate_cert_not_expired(invalid_pem)


@pytest.mark.bootstrap
class TestSSLContextBuilding:
    """Tests for SSL context builders."""

    def test_build_server_ssl_context(self, tmp_path):
        """Test building server SSL context."""
        # Generate CA and leaf cert
        ca_adapter = CryptographyCaAdapter()
        ca_req = CaRequest(
            common_name="test-ca",
            valid_days=3650,
            key_size=2048,
            out_cert=tmp_path / "ca.pem",
            out_key=tmp_path / "ca-key.pem",
        )
        ca_adapter.generate_ca(ca_req)

        cert_adapter = CryptographyCertIssuerAdapter()
        cert_req = CertRequest(
            common_name="server",
            san_dns=("localhost",),
            san_ip=("127.0.0.1",),
            valid_days=365,
            ca_cert=tmp_path / "ca.pem",
            ca_key=tmp_path / "ca-key.pem",
            out_cert=tmp_path / "server.pem",
            out_key=tmp_path / "server-key.pem",
            key_size=2048,
        )
        cert_adapter.issue_cert(cert_req)

        # Read and encode all
        cert_pem = (tmp_path / "server.pem").read_bytes()
        key_pem = (tmp_path / "server-key.pem").read_bytes()
        ca_pem = (tmp_path / "ca.pem").read_bytes()

        b64_cert = base64.b64encode(cert_pem).decode()
        b64_key = base64.b64encode(key_pem).decode()
        b64_ca = base64.b64encode(ca_pem).decode()

        ctx = build_server_ssl_context(b64_cert, b64_key, b64_ca)
        assert ctx is not None
        assert ctx.verify_mode

    def test_build_client_ssl_context(self, tmp_path):
        """Test building client SSL context."""
        # Generate CA and certs
        ca_adapter = CryptographyCaAdapter()
        ca_req = CaRequest(
            common_name="test-ca",
            valid_days=3650,
            key_size=2048,
            out_cert=tmp_path / "ca.pem",
            out_key=tmp_path / "ca-key.pem",
        )
        ca_adapter.generate_ca(ca_req)

        cert_adapter = CryptographyCertIssuerAdapter()
        cert_req = CertRequest(
            common_name="client",
            san_dns=(),
            san_ip=(),
            valid_days=365,
            ca_cert=tmp_path / "ca.pem",
            ca_key=tmp_path / "ca-key.pem",
            out_cert=tmp_path / "client.pem",
            out_key=tmp_path / "client-key.pem",
            key_size=2048,
        )
        cert_adapter.issue_cert(cert_req)

        # Read and encode
        cert_pem = (tmp_path / "client.pem").read_bytes()
        key_pem = (tmp_path / "client-key.pem").read_bytes()
        ca_pem = (tmp_path / "ca.pem").read_bytes()

        b64_cert = base64.b64encode(cert_pem).decode()
        b64_key = base64.b64encode(key_pem).decode()
        b64_ca = base64.b64encode(ca_pem).decode()

        ctx = build_client_ssl_context(b64_cert, b64_key, b64_ca)
        assert ctx is not None
        assert ctx.verify_mode
