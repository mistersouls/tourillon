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

import datetime as dt
import ipaddress
import ssl
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


def _private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _build_ca_certificate(ca_key: rsa.RSAPrivateKey) -> x509.Certificate:
    now = dt.datetime.now(dt.UTC)
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "FR"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Tourillon Test CA"),
            x509.NameAttribute(NameOID.COMMON_NAME, "tourillon-test-ca"),
        ]
    )
    return (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(private_key=ca_key, algorithm=hashes.SHA256())
    )


def _build_leaf_certificate(
    *,
    common_name: str,
    leaf_key: rsa.RSAPrivateKey,
    ca_cert: x509.Certificate,
    ca_key: rsa.RSAPrivateKey,
    is_client: bool,
) -> x509.Certificate:
    now = dt.datetime.now(dt.UTC)
    usage = ExtendedKeyUsageOID.CLIENT_AUTH if is_client else ExtendedKeyUsageOID.SERVER_AUTH
    return (
        x509.CertificateBuilder()
        .subject_name(
            x509.Name(
                [
                    x509.NameAttribute(NameOID.COUNTRY_NAME, "FR"),
                    x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Tourillon Tests"),
                    x509.NameAttribute(NameOID.COMMON_NAME, common_name),
                ]
            )
        )
        .issuer_name(ca_cert.subject)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(leaf_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(private_key=ca_key, algorithm=hashes.SHA256())
    )


def _write_private_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _write_certificate(path: Path, cert: x509.Certificate) -> None:
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def create_mtls_contexts(tmp_path: Path) -> tuple[ssl.SSLContext, ssl.SSLContext]:
    """Create server/client SSL contexts signed by the same test CA."""
    ca_key = _private_key()
    ca_cert = _build_ca_certificate(ca_key)
    server_key = _private_key()
    server_cert = _build_leaf_certificate(
        common_name="tourillon-server",
        leaf_key=server_key,
        ca_cert=ca_cert,
        ca_key=ca_key,
        is_client=False,
    )
    client_key = _private_key()
    client_cert = _build_leaf_certificate(
        common_name="tourillon-client",
        leaf_key=client_key,
        ca_cert=ca_cert,
        ca_key=ca_key,
        is_client=True,
    )

    ca_path = tmp_path / "ca.pem"
    server_cert_path = tmp_path / "server-cert.pem"
    server_key_path = tmp_path / "server-key.pem"
    client_cert_path = tmp_path / "client-cert.pem"
    client_key_path = tmp_path / "client-key.pem"

    _write_certificate(ca_path, ca_cert)
    _write_certificate(server_cert_path, server_cert)
    _write_private_key(server_key_path, server_key)
    _write_certificate(client_cert_path, client_cert)
    _write_private_key(client_key_path, client_key)

    server_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ssl.load_cert_chain(str(server_cert_path), str(server_key_path))
    server_ssl.load_verify_locations(cafile=str(ca_path))
    server_ssl.verify_mode = ssl.CERT_REQUIRED

    client_ssl = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=str(ca_path))
    client_ssl.load_cert_chain(str(client_cert_path), str(client_key_path))
    client_ssl.check_hostname = False

    return server_ssl, client_ssl

