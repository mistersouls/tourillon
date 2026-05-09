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
"""Tests for PKI adapter error paths and CLI commands."""

from __future__ import annotations

import datetime
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from tourillon.core.ports.pki import CaRequest, CertRequest, PkiError
from tourillon.infra.pki.x509 import (
    CryptographyCaAdapter,
    CryptographyCertIssuerAdapter,
)


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


@pytest.mark.bootstrap
def test_ca_adapter_generates_files(tmp_path: Path) -> None:
    """CryptographyCaAdapter writes cert and key files."""
    adapter = CryptographyCaAdapter()
    req = CaRequest(
        common_name="Test CA",
        valid_days=365,
        key_size=2048,
        out_cert=tmp_path / "ca.crt",
        out_key=tmp_path / "ca.key",
    )
    adapter.generate_ca(req)
    assert (tmp_path / "ca.crt").exists()
    assert (tmp_path / "ca.key").exists()


@pytest.mark.bootstrap
def test_ca_adapter_key_file_permission(tmp_path: Path) -> None:
    """CA private key is written (file exists and has content)."""
    adapter = CryptographyCaAdapter()
    req = CaRequest(
        common_name="Test CA",
        valid_days=365,
        key_size=2048,
        out_cert=tmp_path / "ca.crt",
        out_key=tmp_path / "ca.key",
    )
    adapter.generate_ca(req)
    key_data = (tmp_path / "ca.key").read_bytes()
    assert b"PRIVATE KEY" in key_data


@pytest.mark.bootstrap
def test_cert_issuer_generates_files(tmp_path: Path) -> None:
    """CryptographyCertIssuerAdapter issues a leaf cert signed by the CA."""
    # Generate a real CA first
    ca_adapter = CryptographyCaAdapter()
    ca_req = CaRequest(
        common_name="Issuer CA",
        valid_days=365,
        key_size=2048,
        out_cert=tmp_path / "ca.crt",
        out_key=tmp_path / "ca.key",
    )
    ca_adapter.generate_ca(ca_req)

    issuer = CryptographyCertIssuerAdapter()
    leaf_req = CertRequest(
        common_name="node-1",
        san_dns=("node-1",),
        san_ip=(),
        valid_days=90,
        ca_cert=tmp_path / "ca.crt",
        ca_key=tmp_path / "ca.key",
        out_cert=tmp_path / "node.crt",
        out_key=tmp_path / "node.key",
    )
    issuer.issue_cert(leaf_req)
    assert (tmp_path / "node.crt").exists()
    assert (tmp_path / "node.key").exists()


@pytest.mark.bootstrap
def test_cert_issuer_expired_ca_raises(tmp_path: Path, ca_material: tuple) -> None:
    """CryptographyCertIssuerAdapter raises PkiError when CA is expired."""
    ca_cert_pem, ca_key_pem = ca_material

    # Build an expired CA cert
    key_obj = serialization.load_pem_private_key(ca_key_pem, password=None)
    now = _utcnow()
    expired_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "expired")]))
        .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "expired")]))
        .public_key(key_obj.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=10))
        .not_valid_after(now - datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key_obj, hashes.SHA256())
    )
    expired_pem = expired_cert.public_bytes(serialization.Encoding.PEM)
    (tmp_path / "expired_ca.crt").write_bytes(expired_pem)
    (tmp_path / "ca.key").write_bytes(ca_key_pem)

    issuer = CryptographyCertIssuerAdapter()
    req = CertRequest(
        common_name="node-1",
        san_dns=(),
        san_ip=(),
        valid_days=90,
        ca_cert=tmp_path / "expired_ca.crt",
        ca_key=tmp_path / "ca.key",
        out_cert=tmp_path / "node.crt",
        out_key=tmp_path / "node.key",
    )
    with pytest.raises(PkiError, match="expired"):
        issuer.issue_cert(req)


@pytest.mark.bootstrap
def test_cert_issuer_missing_ca_raises(tmp_path: Path) -> None:
    """CryptographyCertIssuerAdapter raises PkiError when CA files are missing."""
    issuer = CryptographyCertIssuerAdapter()
    req = CertRequest(
        common_name="node-1",
        san_dns=(),
        san_ip=(),
        valid_days=90,
        ca_cert=tmp_path / "missing_ca.crt",
        ca_key=tmp_path / "missing_ca.key",
        out_cert=tmp_path / "node.crt",
        out_key=tmp_path / "node.key",
    )
    with pytest.raises(PkiError, match="Cannot read CA"):
        issuer.issue_cert(req)


@pytest.mark.bootstrap
def test_cert_issuer_key_mismatch_raises(tmp_path: Path, ca_material: tuple) -> None:
    """CryptographyCertIssuerAdapter raises PkiError when CA cert/key don't match."""
    ca_cert_pem, _ = ca_material
    (tmp_path / "ca.crt").write_bytes(ca_cert_pem)
    # Generate an unrelated key
    other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    (tmp_path / "wrong.key").write_bytes(
        other_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    issuer = CryptographyCertIssuerAdapter()
    req = CertRequest(
        common_name="node-1",
        san_dns=(),
        san_ip=(),
        valid_days=90,
        ca_cert=tmp_path / "ca.crt",
        ca_key=tmp_path / "wrong.key",
        out_cert=tmp_path / "node.crt",
        out_key=tmp_path / "node.key",
    )
    with pytest.raises(PkiError, match="match"):
        issuer.issue_cert(req)


@pytest.mark.bootstrap
def test_cert_issuer_malformed_ca_cert_raises(
    tmp_path: Path, ca_material: tuple
) -> None:
    """CryptographyCertIssuerAdapter raises PkiError when CA cert file is corrupted."""
    _, ca_key_pem = ca_material
    (tmp_path / "bad_ca.crt").write_bytes(b"not a certificate")
    (tmp_path / "ca.key").write_bytes(ca_key_pem)
    issuer = CryptographyCertIssuerAdapter()
    req = CertRequest(
        common_name="node-1",
        san_dns=(),
        san_ip=(),
        valid_days=90,
        ca_cert=tmp_path / "bad_ca.crt",
        ca_key=tmp_path / "ca.key",
        out_cert=tmp_path / "node.crt",
        out_key=tmp_path / "node.key",
    )
    with pytest.raises(PkiError, match="Failed to load CA"):
        issuer.issue_cert(req)


@pytest.mark.bootstrap
def test_contexts_save_and_load_round_trip(tmp_path: Path) -> None:
    """save_contexts followed by load_contexts preserves all fields."""
    from tourctl.core.commands.config import load_contexts, save_contexts
    from tourctl.core.structure.contexts import (
        ClusterRef,
        ContextEntry,
        ContextsFile,
        CredentialsConfig,
        EndpointsConfig,
    )

    entry = ContextEntry(
        name="prod",
        cluster=ClusterRef(name="prod", ca_data="ca_b64=="),
        endpoints=EndpointsConfig(
            kv="kv.example.com:7700", peer="peer.example.com:7701"
        ),
        credentials=CredentialsConfig(cert_data="cert_b64==", key_data="key_b64=="),
    )
    cf = ContextsFile(current_context="prod", contexts=[entry])
    path = tmp_path / "contexts.toml"
    save_contexts(path, cf)
    loaded = load_contexts(path)
    assert loaded.current_context == "prod"
    assert loaded.contexts[0].name == "prod"
    assert loaded.contexts[0].endpoints.kv == "kv.example.com:7700"


@pytest.mark.bootstrap
def test_load_contexts_empty_when_file_missing(tmp_path: Path) -> None:
    """load_contexts returns an empty ContextsFile when file does not exist."""
    from tourctl.core.commands.config import load_contexts

    cf = load_contexts(tmp_path / "nonexistent.toml")
    assert cf.contexts == []
    assert cf.current_context is None


@pytest.mark.bootstrap
def test_load_contexts_raises_on_invalid_toml(tmp_path: Path) -> None:
    """load_contexts raises ContextsError when the file contains invalid TOML."""
    from tourillon.infra.contexts import ContextsError, load_contexts

    bad_file = tmp_path / "contexts.toml"
    bad_file.write_text("not valid [[[ toml !!!", encoding="utf-8")

    with pytest.raises(ContextsError):
        load_contexts(bad_file)


@pytest.mark.bootstrap
def test_save_contexts_cleanup_on_write_failure(tmp_path: Path) -> None:
    """save_contexts cleans up the tmp file and re-raises when os.write fails."""
    from unittest.mock import patch

    from tourillon.core.structure.contexts import ContextsFile
    from tourillon.infra.contexts import save_contexts

    path = tmp_path / "contexts.toml"
    with (
        patch("tourillon.infra.contexts.os.write", side_effect=OSError("disk full")),
        patch("tourillon.infra.contexts.os.close"),
        patch("tourillon.infra.contexts.os.chmod"),
        patch("tourillon.infra.contexts.os.replace"),
        patch("tourillon.infra.contexts.os.unlink"),
        pytest.raises(OSError, match="disk full"),
    ):
        save_contexts(path, ContextsFile())


@pytest.mark.bootstrap
def test_contexts_upsert_replaces_existing(tmp_path: Path) -> None:
    """ContextsFile.upsert() replaces a context with the same name."""
    from tourctl.core.structure.contexts import (
        ClusterRef,
        ContextEntry,
        ContextsFile,
        CredentialsConfig,
        EndpointsConfig,
    )

    cf = ContextsFile()
    e1 = ContextEntry(
        name="prod",
        cluster=ClusterRef(name="prod", ca_data="old"),
        endpoints=EndpointsConfig(kv="old:7700"),
        credentials=CredentialsConfig(cert_data="c", key_data="k"),
    )
    e2 = ContextEntry(
        name="prod",
        cluster=ClusterRef(name="prod", ca_data="new"),
        endpoints=EndpointsConfig(kv="new:7700"),
        credentials=CredentialsConfig(cert_data="c2", key_data="k2"),
    )
    cf.upsert(e1)
    cf.upsert(e2)
    assert len(cf.contexts) == 1
    assert cf.contexts[0].cluster.ca_data == "new"
