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
from typing import Any

import pytest

from tests.helpers import generate_ca
from tourillon.bootstrap import deps
from tourillon.bootstrap.usecases.node import node_start
from tourillon.core.app import Application, ApplicationContext
from tourillon.core.config import NodeConfigGenerator, TelemetryConfig, TourillonConfig
from tourillon.core.manager import Manager
from tourillon.core.ports import TelemetryProvider
from tourillon.core.telemetry import build_noop_telemetry
from tourillon.core.transports.dispatcher import Dispatcher
from tourillon.infra.storage.lmdb_storage import LMDBStorageProvider
from tourlib.adapter import CryptographyMtlsAdapter, MsgPackSerializer, TomlPersistence


class _FakeManager:
    def __init__(self) -> None:
        self.seeds: list[str] | None = None

    async def serve_forever(self, seeds: list[str] | None = None) -> None:
        self.seeds = seeds


class _FakeConfig:
    def __init__(self, node_id: str) -> None:
        self.node = type("Node", (), {"id": node_id})()
        self.logging = object()


class _FakeApp:
    def __init__(self, config: object, manager: object) -> None:
        self._config = config
        self._manager = manager
        self.ctx = ApplicationContext()
        self.loaded_path: Path | None = None
        self.created_config: object | None = None

    async def load_config(self, config_path: Path) -> object:
        self.loaded_path = config_path
        return self._config

    async def create_manager(self, config: object) -> object:
        self.created_config = config
        return self._manager


class _TelemetryProviderSpy:
    def __init__(self) -> None:
        self.calls: list[TelemetryConfig] = []
        self.telemetry = build_noop_telemetry()

    def create(self, config: TelemetryConfig):
        self.calls.append(config)
        return self.telemetry


async def _generate_config_file(tmp_path: Path, node_id: str = "node-app-test") -> Path:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    output_file = tmp_path / "node.toml"
    generator = NodeConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    await generator.generate(
        output_file=output_file,
        node_id=node_id,
        ca_certificate_path=ca_cert_path,
        ca_private_key_path=ca_key_path,
        data_dir=str(tmp_path / "data"),
        group_size="8Mi",
        validity_days=5,
        key_size=2048,
        overwrite=False,
    )
    return output_file


def _build_application(
    telemetry_provider: TelemetryProvider | None = None,
) -> Application:
    return Application(
        crypto=CryptographyMtlsAdapter(),
        persistence=TomlPersistence(),
        serializer=MsgPackSerializer(),
        peer_dispatcher=Dispatcher(),
        client_dispatcher=Dispatcher(),
        storage_provider=LMDBStorageProvider(),
        telemetry_provider=telemetry_provider,
    )


@pytest.mark.tourillon
def test_application_context_get_set_is_write_once() -> None:
    context = ApplicationContext()

    class _Thing:
        pass

    first = object()
    second = object()

    assert context.get(_Thing) is None

    context.set(_Thing, first)
    context.set(_Thing, second)

    assert context.get(_Thing) is first


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_application_load_config_returns_tourillon_config(tmp_path: Path) -> None:
    app = _build_application()
    config_path = await _generate_config_file(tmp_path, node_id="node-load")

    config = await app.load_config(config_path)

    assert isinstance(config, TourillonConfig)
    assert config.node.id == "node-load"
    assert config.storage.data_dir == str(tmp_path / "data")


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_application_create_manager_builds_manager(tmp_path: Path) -> None:
    provider = _TelemetryProviderSpy()
    app = _build_application(telemetry_provider=provider)
    config_path = await _generate_config_file(tmp_path, node_id="node-manager")
    config = await app.load_config(config_path)
    config = TourillonConfig(
        node=config.node,
        storage=config.storage,
        tls=config.tls,
        server_client=config.server_client,
        server_peer=config.server_peer,
        logging=config.logging,
        telemetry=TelemetryConfig(
            metrics_enabled=True,
            tracing_enabled=False,
            service_name="app-test",
        ),
    )

    manager = await app.create_manager(config)

    assert isinstance(manager, Manager)
    assert manager.node_id == "node-manager"
    assert manager.serializer is app._serializer
    assert provider.calls == [config.telemetry]
    assert manager.telemetry is provider.telemetry


@pytest.mark.tourillon
def test_get_manager_returns_manager_from_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_manager = object()
    app = type("FakeDepsApp", (), {"ctx": ApplicationContext()})()
    app.ctx.set(Manager, fake_manager)
    monkeypatch.setattr(deps, "get_app", lambda: app)

    assert deps.get_manager() is fake_manager


@pytest.mark.tourillon
def test_get_manager_raises_when_manager_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = type("FakeDepsApp", (), {"ctx": ApplicationContext()})()
    monkeypatch.setattr(deps, "get_app", lambda: app)

    with pytest.raises(RuntimeError, match="Node manager is not initialized"):
        deps.get_manager()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_start_loads_config_initializes_logging_and_serves(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "node.toml"
    fake_config = _FakeConfig(node_id="node-start")
    fake_manager = _FakeManager()
    fake_app = _FakeApp(config=fake_config, manager=fake_manager)
    seen_logging_configs: list[Any] = []

    monkeypatch.setattr("tourillon.bootstrap.usecases.node.get_app", lambda: fake_app)
    monkeypatch.setattr(
        "tourillon.bootstrap.usecases.node.init_loggers",
        lambda logging_config: seen_logging_configs.append(logging_config),
    )

    await node_start(config=config_path, seeds=["seed-a:9001"])

    assert fake_app.loaded_path == config_path
    assert fake_app.created_config is fake_config
    assert fake_app.ctx.get(Manager) is fake_manager
    assert fake_manager.seeds == ["seed-a:9001"]
    assert seen_logging_configs == [fake_config.logging]
