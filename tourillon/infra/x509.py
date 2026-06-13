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
"""cryptography-library adapters for CertificateAuthorityPort and CertificateIssuerPort."""

import datetime
import ipaddress
import stat
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from tourillon.core.ports.pki import CaRequest, CertRequest, PkiError


class CryptographyX509Issuer:
    """
    Generate a self-signed Certificate Authority and issue
    a certificate signed by the CA described in the request.
    """

    def generate_ca(self, request: CaRequest) -> None:
        """Generate a self-signed CA certificate and private key to disk."""
        try:
            key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=request.key_size,
            )
            subject = issuer = x509.Name(
                [x509.NameAttribute(NameOID.COMMON_NAME, request.common_name)]
            )
            now = self._utcnow()
            cert = (
                x509.CertificateBuilder()
                .subject_name(subject)
                .issuer_name(issuer)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now)
                .not_valid_after(now + datetime.timedelta(days=request.valid_days))
                .add_extension(
                    x509.BasicConstraints(ca=True, path_length=None),
                    critical=True,
                )
                .sign(key, hashes.SHA256())
            )
        except Exception as exc:
            raise PkiError(f"CA generation failed: {exc}") from exc

        key_pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
        cert_pem = cert.public_bytes(serialization.Encoding.PEM)
        self._write_private(request.out_key, key_pem)
        self._write_public(request.out_cert, cert_pem)

    def issue_cert(self, request: CertRequest) -> None:
        """Issue a certificate signed by the CA described in the request."""
        ca_cert, ca_key_obj = self._load_ca_material(request)
        self._validate_ca(ca_cert, ca_key_obj)
        leaf_key, leaf_cert = self._build_leaf_cert(request, ca_cert, ca_key_obj)
        leaf_key_pem = leaf_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
        leaf_cert_pem = leaf_cert.public_bytes(serialization.Encoding.PEM)
        self._write_private(request.out_key, leaf_key_pem)
        self._write_public(request.out_cert, leaf_cert_pem)

    def _build_leaf_cert(
        self,
        request: CertRequest,
        ca_cert: x509.Certificate,
        ca_key_obj: object,
    ) -> tuple[object, x509.Certificate]:
        """Build and sign the leaf certificate; return (leaf_key, leaf_cert)."""
        try:
            now = self._utcnow()
            leaf_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=request.key_size,
            )
            builder = (
                x509.CertificateBuilder()
                .subject_name(
                    x509.Name(
                        [x509.NameAttribute(NameOID.COMMON_NAME, request.common_name)]
                    )
                )
                .issuer_name(ca_cert.subject)
                .public_key(leaf_key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now)
                .not_valid_after(now + datetime.timedelta(days=request.valid_days))
            )
            san_entries = self._build_san_entries(request)
            if san_entries:
                builder = builder.add_extension(
                    x509.SubjectAlternativeName(san_entries), critical=False
                )
            cert = builder.sign(ca_key_obj, hashes.SHA256())  # type: ignore[arg-type]
        except Exception as exc:
            raise PkiError(f"Certificate issuance failed: {exc}") from exc
        return leaf_key, cert

    @staticmethod
    def _build_san_entries(request: CertRequest) -> list[x509.GeneralName]:
        """Return Subject Alternative Name entries from request."""
        entries: list[x509.GeneralName] = []
        for dns in request.san_dns:
            entries.append(x509.DNSName(dns))
        for ip in request.san_ip:
            entries.append(x509.IPAddress(ipaddress.ip_address(ip)))
        return entries

    @staticmethod
    def _load_ca_material(
        request: CertRequest,
    ) -> tuple[x509.Certificate, object]:
        """Load and return the CA certificate and private key from request paths."""
        try:
            ca_cert = x509.load_pem_x509_certificate(request.ca_cert.read_bytes())
            ca_key_obj = serialization.load_pem_private_key(
                request.ca_key.read_bytes(), password=None
            )
        except FileNotFoundError as exc:
            raise PkiError(f"Cannot read CA material: {exc}") from exc
        except Exception as exc:
            raise PkiError(f"Failed to load CA: {exc}") from exc
        return ca_cert, ca_key_obj

    @staticmethod
    def _utcnow() -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)

    def _validate_ca(self, ca_cert: x509.Certificate, ca_key_obj: object) -> None:
        """Raise PkiError when the CA cert is expired or the key does not match."""
        now = self._utcnow()
        if ca_cert.not_valid_after_utc < now:
            raise PkiError(
                f"CA certificate expired on {ca_cert.not_valid_after_utc.date()}"
            )
        ca_pub = ca_cert.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        derived_pub = ca_key_obj.public_key().public_bytes(  # type: ignore[union-attr]
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        if ca_pub != derived_pub:
            raise PkiError("CA private key does not match CA certificate public key")

    @staticmethod
    def _write_private(path: Path | str, data: bytes) -> None:
        """Write *data* to *path* and restrict permissions to 0600."""
        import os
        from pathlib import Path

        p = Path(str(path))  # type: ignore[arg-type]
        try:
            p.write_bytes(data)
            os.chmod(p, stat.S_IRUSR | stat.S_IWUSR)
        except OSError as exc:  # pragma: no cover
            raise PkiError(f"Cannot write to {p}: {exc.strerror.lower()}") from exc

    @staticmethod
    def _write_public(path: Path | str, data: bytes) -> None:
        from pathlib import Path

        p = Path(str(path))  # type: ignore[arg-type]
        try:
            p.write_bytes(data)
        except OSError as exc:  # pragma: no cover
            raise PkiError(f"Cannot write to {p}: {exc.strerror.lower()}") from exc
