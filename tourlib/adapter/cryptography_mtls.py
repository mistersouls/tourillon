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

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


class CryptographyMtlsAdapter:
    """Unified cryptography-backed implementation for all mTLS operations."""

    def generate_ca(
        self,
        *,
        common_name: str,
        organization: str,
        country: str | None,
        validity_days: int,
        key_size: int,
    ) -> tuple[bytes, bytes]:
        if validity_days < 1:
            raise ValueError("validity_days must be >= 1")
        if key_size < 2048:
            raise ValueError("key_size must be >= 2048")

        ca_key = rsa.generate_private_key(public_exponent=65537, key_size=key_size)
        ca_cert = self._build_ca_certificate(
            ca_key=ca_key,
            common_name=common_name,
            organization=organization,
            country=country,
            validity_days=validity_days,
        )

        return (
            ca_cert.public_bytes(serialization.Encoding.PEM),
            ca_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            ),
        )

    def generate_cert(
        self,
        *,
        common_name: str,
        ca_certificate_pem: bytes,
        ca_private_key_pem: bytes,
        validity_days: int,
        key_size: int,
        client_only: bool = False,
    ) -> tuple[bytes, bytes]:
        if not common_name:
            raise ValueError("common_name must not be empty")
        if validity_days < 1:
            raise ValueError("validity_days must be >= 1")
        if key_size < 2048:
            raise ValueError("key_size must be >= 2048")

        ca_cert = x509.load_pem_x509_certificate(ca_certificate_pem)
        ca_key = serialization.load_pem_private_key(ca_private_key_pem, password=None)
        if not isinstance(ca_key, rsa.RSAPrivateKey):
            raise TypeError("CA private key must be an RSA private key")

        leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=key_size)
        leaf_cert = self._build_leaf_certificate(
            common_name=common_name,
            ca_cert=ca_cert,
            ca_key=ca_key,
            leaf_public_key=leaf_key.public_key(),
            validity_days=validity_days,
            client_only=client_only,
        )

        return (
            leaf_cert.public_bytes(serialization.Encoding.PEM),
            leaf_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            ),
        )

    def _build_ca_certificate(
        self,
        *,
        ca_key: rsa.RSAPrivateKey,
        common_name: str,
        organization: str,
        country: str | None,
        validity_days: int,
    ) -> x509.Certificate:
        now = dt.datetime.now(dt.UTC)
        subject = self._subject(common_name=common_name, organization=organization, country=country)
        return (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(subject)
            .public_key(ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(minutes=1))
            .not_valid_after(now + dt.timedelta(days=validity_days))
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

    @staticmethod
    def _build_leaf_certificate(
        *,
        common_name: str,
        ca_cert: x509.Certificate,
        ca_key: rsa.RSAPrivateKey,
        leaf_public_key: rsa.RSAPublicKey,
        validity_days: int,
        client_only: bool = False,
    ) -> x509.Certificate:
        now = dt.datetime.now(dt.UTC)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
        eku = (
            [ExtendedKeyUsageOID.CLIENT_AUTH]
            if client_only
            else [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]
        )
        return (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(ca_cert.subject)
            .public_key(leaf_public_key)
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(minutes=1))
            .not_valid_after(now + dt.timedelta(days=validity_days))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(leaf_public_key),
                critical=False,
            )
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
                critical=False,
            )
            .add_extension(
                x509.ExtendedKeyUsage(eku),
                critical=False,
            )
            .sign(private_key=ca_key, algorithm=hashes.SHA256())
        )

    @staticmethod
    def _subject(*, common_name: str, organization: str, country: str | None) -> x509.Name:
        parts = [x509.NameAttribute(NameOID.ORGANIZATION_NAME, organization)]
        if country is not None:
            parts.insert(0, x509.NameAttribute(NameOID.COUNTRY_NAME, country))
        parts.append(x509.NameAttribute(NameOID.COMMON_NAME, common_name))
        return x509.Name(parts)



__all__ = ["CryptographyMtlsAdapter"]

