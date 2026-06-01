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
"""CLI and contexts.toml tests for bootstrap proposal 001."""

from __future__ import annotations

import os
import stat

import pytest
from typer.testing import CliRunner

from tourctl.bootstrap.main import app as tourctl_app
from tourillon.infra.cli.main import app as tourillon_app
from tourillon.infra.contexts import ContextsError, ContextsRepository, load_contexts


@pytest.mark.bootstrap
class TestContextsRepository:
    def test_load_absent_returns_empty(self, tmp_path):
        path = tmp_path / "contexts.toml"
        loaded = load_contexts(path)
        assert loaded.current_context is None
        assert loaded.contexts == []

    def test_save_then_load_roundtrip(self, tmp_path):
        runner = CliRunner()
        ca_cert = tmp_path / "ca.crt"
        ca_key = tmp_path / "ca.key"
        gen_ca = runner.invoke(
            tourillon_app,
            [
                "pki",
                "ca",
                "--out-cert",
                str(ca_cert),
                "--out-key",
                str(ca_key),
            ],
        )
        assert gen_ca.exit_code == 0, gen_ca.output

        contexts_path = tmp_path / "contexts.toml"
        gen_ctx = runner.invoke(
            tourillon_app,
            [
                "config",
                "generate-context",
                "dev",
                "--ca-cert",
                str(ca_cert),
                "--ca-key",
                str(ca_key),
                "--kv",
                "127.0.0.1:7000",
                "--peer",
                "127.0.0.1:7001",
                "--out",
                str(contexts_path),
                "--set-current",
            ],
        )
        assert gen_ctx.exit_code == 0, gen_ctx.output

        loaded = ContextsRepository().load(contexts_path)
        assert loaded.current_context == "dev"
        assert loaded.get("dev") is not None
        assert loaded.get("dev").endpoints.kv == "127.0.0.1:7000"
        assert loaded.get("dev").endpoints.peer == "127.0.0.1:7001"

        if os.name != "nt":
            mode = stat.S_IMODE(os.stat(contexts_path).st_mode)
            assert mode == 0o600

    def test_load_invalid_toml_raises(self, tmp_path):
        broken = tmp_path / "broken.toml"
        broken.write_text("contexts = [", encoding="utf-8")
        with pytest.raises(ContextsError, match="Cannot parse"):
            ContextsRepository().load(broken)


@pytest.mark.bootstrap
class TestCliCommands:
    def test_tourctl_use_context_success_and_error_paths(self, tmp_path):
        runner = CliRunner()

        missing = runner.invoke(
            tourctl_app,
            [
                "config",
                "use-context",
                "prod",
                "--contexts-file",
                str(tmp_path / "missing.toml"),
            ],
        )
        assert missing.exit_code == 1
        assert "Contexts file not found" in missing.output

        ca_cert = tmp_path / "ca.crt"
        ca_key = tmp_path / "ca.key"
        runner.invoke(
            tourillon_app,
            [
                "pki",
                "ca",
                "--out-cert",
                str(ca_cert),
                "--out-key",
                str(ca_key),
            ],
            catch_exceptions=False,
        )

        contexts_path = tmp_path / "contexts.toml"
        runner.invoke(
            tourillon_app,
            [
                "config",
                "generate-context",
                "prod",
                "--ca-cert",
                str(ca_cert),
                "--ca-key",
                str(ca_key),
                "--peer",
                "127.0.0.1:7001",
                "--out",
                str(contexts_path),
            ],
            catch_exceptions=False,
        )

        unknown = runner.invoke(
            tourctl_app,
            [
                "config",
                "use-context",
                "unknown",
                "--contexts-file",
                str(contexts_path),
            ],
        )
        assert unknown.exit_code == 1
        assert 'Context "unknown" not found' in unknown.output

        switched = runner.invoke(
            tourctl_app,
            [
                "config",
                "use-context",
                "prod",
                "--contexts-file",
                str(contexts_path),
            ],
        )
        assert switched.exit_code == 0
        assert 'Active context set to "prod"' in switched.output
        assert load_contexts(contexts_path).current_context == "prod"

    def test_config_generate_and_generate_context_validation(self, tmp_path):
        runner = CliRunner()
        ca_cert = tmp_path / "ca.crt"
        ca_key = tmp_path / "ca.key"
        runner.invoke(
            tourillon_app,
            [
                "pki",
                "ca",
                "--out-cert",
                str(ca_cert),
                "--out-key",
                str(ca_key),
            ],
            catch_exceptions=False,
        )

        config_path = tmp_path / "node.toml"
        gen_cfg = runner.invoke(
            tourillon_app,
            [
                "config",
                "generate",
                "--ca-cert",
                str(ca_cert),
                "--ca-key",
                str(ca_key),
                "--node-id",
                "node-1",
                "--out",
                str(config_path),
            ],
        )
        assert gen_cfg.exit_code == 0, gen_cfg.output
        text = config_path.read_text(encoding="utf-8")
        assert "cert_data" in text
        assert "key_data" in text
        assert "ca_data" in text

        bad = runner.invoke(
            tourillon_app,
            [
                "config",
                "generate-context",
                "cluster-1",
                "--ca-cert",
                str(ca_cert),
                "--ca-key",
                str(ca_key),
                "--out",
                str(tmp_path / "contexts.toml"),
            ],
        )
        assert bad.exit_code == 1
        assert "--kv or --peer" in bad.output
