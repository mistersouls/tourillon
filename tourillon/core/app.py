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

from tourillon.core.config import (
    ClientConfigGenerator,
    NodeConfigGenerator,
    TourillonConfig,
)
from tourillon.core.log import logger
from tourillon.core.manager import Manager
from tourillon.core.mtls import CertIssuer
from tourillon.core.ports import TelemetryProvider
from tourillon.core.ports.storage import StorageProvider
from tourillon.core.telemetry import build_noop_telemetry_provider
from tourillon.core.transports.dispatcher import Dispatcher
from tourlib import Serializer
from tourlib.port import MtlsCrypto, Persistence


class ApplicationContext:
    def __init__(self) -> None:
        self._ctx: dict[type[Any], Any] = {}

    def get(self, cls: type[Any]) -> Any | None:
        """Retrieve an instance of the specified class from the context."""
        return self._ctx.get(cls)

    def set(self, cls: type[Any], value: Any) -> None:
        """Set an instance of the specified class from the context."""
        self._ctx.setdefault(cls, value)


class Application:
    def __init__(
        self,
        crypto: MtlsCrypto,
        persistence: Persistence,
        serializer: Serializer,
        peer_dispatcher: Dispatcher,
        client_dispatcher: Dispatcher,
        storage_provider: StorageProvider,
        telemetry_provider: TelemetryProvider | None = None,
    ) -> None:
        self._crypto = crypto
        self._persistence = persistence
        self._serializer = serializer
        self._storage_provider = storage_provider
        self._telemetry_provider = telemetry_provider or build_noop_telemetry_provider()

        self.cert_issuer = CertIssuer(
            crypto=crypto,
        )
        self.node_config = NodeConfigGenerator(
            crypto=crypto,
            persistence=persistence,
        )
        self.client_config = ClientConfigGenerator(
            crypto=crypto,
            persistence=persistence,
        )
        self.ctx = ApplicationContext()
        self.peer_dispatcher = peer_dispatcher
        self.client_dispatcher = client_dispatcher
        logger.debug(
            "Application initialized (peer_dispatcher=%s, client_dispatcher=%s)",
            type(peer_dispatcher).__name__,
            type(client_dispatcher).__name__,
        )

    async def create_manager(self, config: TourillonConfig) -> Manager:
        """Create a Manager instance for the given configuration."""
        logger.info("Creating manager for node '%s'", config.node.id)
        tls_server = await config.tls_server()
        tls_client = await config.tls_client()
        manager = Manager(
            config=config,
            peer_dispatcher=self.peer_dispatcher,
            client_dispatcher=self.client_dispatcher,
            tls_server=tls_server,
            tls_client=tls_client,
            persistence=self._persistence,
            serializer=self._serializer,
            storage_provider=self._storage_provider,
            telemetry_provider=self._telemetry_provider,
        )
        logger.debug("Manager created for node '%s'", config.node.id)
        return manager

    async def load_config(self, config_path: Path) -> TourillonConfig:
        """Load and parse a Tourillon node configuration document."""
        logger.info("Loading node config from '%s'", config_path)
        config_doc = await self._persistence.load(config_path)
        config = TourillonConfig.from_document(config_doc)
        logger.debug("Loaded config for node '%s'", config.node.id)
        return config
