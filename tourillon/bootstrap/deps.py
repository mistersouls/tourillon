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

import functools

from tourillon.core.app import Application
from tourillon.core.log import logger
from tourillon.core.manager import Manager
from tourillon.core.transports.dispatcher import Dispatcher
from tourillon.infra.adapter.telemetry.otel_provider import OpenTelemetryProvider
from tourillon.infra.storage.lmdb_storage import LMDBStorageProvider
from tourlib.adapter import CryptographyMtlsAdapter, MsgPackSerializer, TomlPersistence


@functools.lru_cache(maxsize=1)
def client_dispatcher() -> Dispatcher:
    logger.debug("Creating client dispatcher")
    return Dispatcher()


@functools.lru_cache(maxsize=1)
def peer_dispatcher() -> Dispatcher:
    logger.debug("Creating peer dispatcher")
    return Dispatcher()


@functools.lru_cache(maxsize=1)
def get_app() -> Application:
    logger.debug("Creating application with default adapters")
    return Application(
        crypto=CryptographyMtlsAdapter(),
        persistence=TomlPersistence(),
        serializer=MsgPackSerializer(),
        peer_dispatcher=peer_dispatcher(),
        client_dispatcher=client_dispatcher(),
        storage_provider=LMDBStorageProvider(),
        telemetry_provider=OpenTelemetryProvider(),
    )


def get_manager() -> Manager:
    app = get_app()
    manager = app.ctx.get(Manager)
    if manager is None:
        logger.error("Manager lookup failed: manager is not initialized")
        raise RuntimeError("Node manager is not initialized.")
    logger.debug("Manager retrieved from application context")
    return manager
