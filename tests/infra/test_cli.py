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
"""Tests for tourillon and tourctl CLI commands via Typer test runner."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from tourctl.bootstrap.main import app as tourctl_app
from tourillon.core.ports.pki import PkiError
from tourillon.core.structure.envelope import Envelope
from tourillon.infra.cli.main import app as tourillon_app

_runner = CliRunner()


@pytest.mark.bootstrap
def test_pki_ca_missing_args_exits_nonzero() -> None:
    """tourillon pki ca without required options exits with non-zero code."""
    result = _runner.invoke(tourillon_app, ["pki", "ca"])
    assert result.exit_code != 0


@pytest.mark.bootstrap
def test_pki_ca_success(tmp_path: Path) -> None:
    """tourillon pki ca with valid paths writes cert and key and exits 0."""
    result = _runner.invoke(
        tourillon_app,
        [
            "pki",
            "ca",
            "--out-cert",
            str(tmp_path / "ca.crt"),
            "--out-key",
            str(tmp_path / "ca.key"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (tmp_path / "ca.crt").exists()


@pytest.mark.bootstrap
def test_pki_ca_pkierror_exits_1(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """tourillon pki ca logs error and exits 1 when PkiError is raised."""
    with (
        patch(
            "tourillon.infra.cli.pki.CryptographyCaAdapter.generate_ca",
            side_effect=PkiError("cannot write"),
        ),
        caplog.at_level(logging.ERROR),
    ):
        result = _runner.invoke(
            tourillon_app,
            [
                "pki",
                "ca",
                "--out-cert",
                str(tmp_path / "ca.crt"),
                "--out-key",
                str(tmp_path / "ca.key"),
            ],
        )
    assert result.exit_code == 1
    assert "cannot write" in caplog.text


@pytest.mark.bootstrap
def test_config_generate_missing_ca_exits_nonzero() -> None:
    """config generate without --ca-cert exits non-zero with helpful message."""
    result = _runner.invoke(
        tourillon_app,
        ["config", "generate", "--node-id", "n1"],
    )
    assert result.exit_code != 0
    assert "--ca-cert" in result.output


@pytest.mark.bootstrap
def test_config_generate_missing_node_id_exits_nonzero(tmp_path: Path) -> None:
    """config generate without --node-id exits non-zero."""
    ca = tmp_path / "ca.crt"
    ca.write_bytes(b"dummy")
    result = _runner.invoke(
        tourillon_app,
        ["config", "generate", "--ca-cert", str(ca), "--ca-key", str(ca)],
    )
    assert result.exit_code != 0


@pytest.mark.bootstrap
def test_config_generate_invalid_size_exits_1(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """config generate with invalid --size value exits 1."""
    ca = tmp_path / "ca.crt"
    ca.write_bytes(b"dummy")
    with caplog.at_level(logging.ERROR):
        result = _runner.invoke(
            tourillon_app,
            [
                "config",
                "generate",
                "--ca-cert",
                str(ca),
                "--ca-key",
                str(ca),
                "--node-id",
                "n1",
                "--size",
                "HUGE",
            ],
        )
    assert result.exit_code == 1
    assert "HUGE" in caplog.text


@pytest.mark.bootstrap
def test_config_generate_pkierror_exits_1(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """config generate logs error and exits 1 when issue_cert raises PkiError."""
    ca = tmp_path / "ca.crt"
    ca.write_bytes(b"dummy")
    with (
        patch(
            "tourillon.infra.cli.config.CryptographyCertIssuerAdapter.issue_cert",
            side_effect=PkiError("cert error"),
        ),
        caplog.at_level(logging.ERROR),
    ):
        result = _runner.invoke(
            tourillon_app,
            [
                "config",
                "generate",
                "--ca-cert",
                str(ca),
                "--ca-key",
                str(ca),
                "--node-id",
                "n1",
                "--out",
                str(tmp_path / "out.toml"),
            ],
        )
    assert result.exit_code == 1
    assert "cert error" in caplog.text


@pytest.mark.bootstrap
def test_config_generate_oserror_exits_1(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """config generate logs error and exits 1 when writing the config file fails."""
    ca = tmp_path / "ca.crt"
    ca.write_bytes(b"dummy")

    def _fake_issue(req) -> None:  # noqa: ANN001
        # Use open() so that the Path.write_bytes patch below does not
        # prevent writing the temp cert/key files themselves.
        with open(str(req.out_cert), "wb") as fh:
            fh.write(b"CERT")
        with open(str(req.out_key), "wb") as fh:
            fh.write(b"KEY")

    with (
        patch(
            "tourillon.infra.cli.config.CryptographyCertIssuerAdapter.issue_cert",
            side_effect=_fake_issue,
        ),
        patch.object(Path, "write_bytes", side_effect=OSError(13, "permission denied")),
        caplog.at_level(logging.ERROR),
    ):
        result = _runner.invoke(
            tourillon_app,
            [
                "config",
                "generate",
                "--ca-cert",
                str(ca),
                "--ca-key",
                str(ca),
                "--node-id",
                "n1",
                "--out",
                str(tmp_path / "out.toml"),
            ],
        )
    assert result.exit_code == 1
    assert "permission denied" in caplog.text


@pytest.mark.bootstrap
def test_tourillon_generate_context_missing_ca_exits_nonzero() -> None:
    """tourillon config generate-context without --ca-cert exits non-zero."""
    result = _runner.invoke(
        tourillon_app,
        ["config", "generate-context", "test", "--kv", "localhost:7700"],
    )
    assert result.exit_code != 0
    assert "--ca-cert" in result.output


@pytest.mark.bootstrap
def test_tourillon_generate_context_no_endpoint_exits_1(tmp_path: Path) -> None:
    """tourillon config generate-context without --kv or --peer exits 1."""
    ca = tmp_path / "ca.crt"
    ca.write_bytes(b"dummy")
    result = _runner.invoke(
        tourillon_app,
        [
            "config",
            "generate-context",
            "test",
            "--ca-cert",
            str(ca),
            "--ca-key",
            str(ca),
            "--out",
            str(tmp_path / "contexts.toml"),
        ],
    )
    assert result.exit_code == 1


@pytest.mark.bootstrap
def test_config_generate_context_pkierror_exits_1(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """config generate-context exits 1 when issue_cert raises PkiError."""
    ca = tmp_path / "ca.crt"
    ca.write_bytes(b"dummy")
    with (
        patch(
            "tourillon.infra.cli.config.CryptographyCertIssuerAdapter.issue_cert",
            side_effect=PkiError("client cert error"),
        ),
        caplog.at_level(logging.ERROR),
    ):
        result = _runner.invoke(
            tourillon_app,
            [
                "config",
                "generate-context",
                "test",
                "--ca-cert",
                str(ca),
                "--ca-key",
                str(ca),
                "--out",
                str(tmp_path / "contexts.toml"),
                "--kv",
                "localhost:7700",
            ],
        )
    assert result.exit_code == 1
    assert "client cert error" in caplog.text


@pytest.mark.bootstrap
def test_config_generate_context_oserror_exits_1(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """config generate-context exits 1 when saving contexts.toml raises OSError."""
    ca = tmp_path / "ca.crt"
    ca.write_bytes(b"dummy")

    def _fake_issue(req) -> None:  # noqa: ANN001
        req.out_cert.write_bytes(b"CERT")
        req.out_key.write_bytes(b"KEY")

    with (
        patch(
            "tourillon.infra.cli.config.CryptographyCertIssuerAdapter.issue_cert",
            side_effect=_fake_issue,
        ),
        patch(
            "tourillon.infra.cli.config.save_contexts",
            side_effect=OSError(13, "permission denied"),
        ),
        caplog.at_level(logging.ERROR),
    ):
        result = _runner.invoke(
            tourillon_app,
            [
                "config",
                "generate-context",
                "test",
                "--ca-cert",
                str(ca),
                "--ca-key",
                str(ca),
                "--out",
                str(tmp_path / "contexts.toml"),
                "--kv",
                "localhost:7700",
            ],
        )
    assert result.exit_code == 1
    assert "permission denied" in caplog.text


@pytest.mark.bootstrap
def test_tourctl_config_list_empty(tmp_path: Path) -> None:
    """tourctl config list with an empty contexts file exits 0."""
    contexts = tmp_path / "contexts.toml"
    result = _runner.invoke(
        tourctl_app,
        ["config", "list", "--contexts", str(contexts)],
    )
    assert result.exit_code == 0


@pytest.mark.bootstrap
def test_tourctl_use_context_unknown_name_exits_1(tmp_path: Path) -> None:
    """tourctl config use-context with unknown name exits 1."""
    contexts = tmp_path / "contexts.toml"
    result = _runner.invoke(
        tourctl_app,
        ["config", "use-context", "nonexistent", "--contexts", str(contexts)],
    )
    assert result.exit_code == 1
    assert "not found" in result.output


@pytest.mark.bootstrap
def test_envelope_create_with_explicit_correlation_id() -> None:
    """Envelope.create() preserves an explicitly supplied correlation_id."""
    cid = uuid.uuid4()
    env = Envelope.create(b"x", kind="kv.get", correlation_id=cid)
    assert env.correlation_id == cid
    assert env.kind == "kv.get"
    assert env.payload == b"x"


@pytest.mark.bootstrap
def test_envelope_create_generates_correlation_id_when_none() -> None:
    """Envelope.create() generates a fresh correlation_id when None is passed."""
    env = Envelope.create(b"", kind="kv.get")
    assert isinstance(env.correlation_id, uuid.UUID)
