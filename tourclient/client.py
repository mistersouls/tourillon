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

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from tourclient.core import Transport
from tourclient.core.helpers import load_config
from tourclient.core.services.registry import (
    ServiceType,
    get_service,
    validate_service_endpoint,
)
from tourlib import Serializer, ValidationError
from tourlib.adapter import MsgPackSerializer, TomlPersistence
from tourlib.helpers import parse_bind
from tourlib.port import Persistence


class TourClient:
    """High-level client for Tourillon operations."""

    def __init__(
        self,
        transport: Transport,
        serializer: Serializer,
        persistence: Persistence,
        endpoint: Literal["client", "peer"] = "client",
    ) -> None:
        self._transport = transport
        self._serializer = serializer
        self._persistence = persistence
        self._endpoint = endpoint
        self._services_cache: dict[str, ServiceType] = {}

    def __getattr__(self, name: str) -> ServiceType:
        """Dynamically instantiate and cache services.

        Allows accessing services like: tour.probe, tour.rebalance, etc.

        Args:
            name: Service name.

        Returns:
            Instantiated service object (cached).

        Raises:
            AttributeError: If the service is not registered or name starts with _.
        """
        # Avoid infinite recursion for private/protected attributes
        if name.startswith("_"):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

        # Check cache first
        if name in self._services_cache:
            return self._services_cache[name]

        # Try to get service from registry
        try:
            service = get_service(
                name,
                self._endpoint,
                transport=self._transport,
                serializer=self._serializer,
            )
            self._services_cache[name] = service
            return service
        except Exception as exc:
            raise AttributeError(
                f"'{self.__class__.__name__}' has no service '{name}': {exc}"
            ) from exc


@asynccontextmanager
async def client(
    service: str | None = None,
    /,
    *,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
    endpoint: Literal["client", "peer"] = "client",
    bind: str | None = None,
    serializer: Serializer | None = None,
    persistence: Persistence | None = None,
) -> AsyncGenerator[ServiceType | TourClient]:
    """Establish a client session with Tourillon.

    Args:
        service: Registered service name (e.g., "probe"), or None for TourClient.
        tourconf: Path to tourconf.toml, or None to use default resolution.
        context_name: Context name in tourconf, or None for current_context.
        endpoint: Target endpoint ("client" or "peer"). Defaults to "client".
            Must be supported by the requested service.
        bind: Optional explicit `host:port` override for the selected endpoint.
        serializer: Serializer instance, or None for MsgPackSerializer.
        persistence: Persistence backend, or None for TomlPersistence.

    Yields:
        ServiceType instance (ProbeService, TourClient, etc.).

    Raises:
        ValidationError: If endpoint is invalid.
        ValueError: If service is not available on the endpoint.
        UnknownServiceError: If service name is not registered.
        FileNotFoundError: If tourconf cannot be resolved.
    """
    if endpoint not in ("client", "peer"):
        raise ValidationError("Endpoint must be either 'client' or 'peer'")

    validate_service_endpoint(service, endpoint)

    serializer = serializer or MsgPackSerializer()
    persistence = persistence or TomlPersistence()

    client_conf = await load_config(tourconf, persistence=persistence)
    context_name = context_name or client_conf.current_context
    cluster = client_conf.cluster(context_name)
    target_bind = bind or getattr(cluster, endpoint)
    host, port = parse_bind(target_bind)
    ssl_context = await client_conf.tls_client(context_name)
    transport = Transport(
        bind=f"{host}:{port}",
        ssl_context=ssl_context,
        serializer=serializer,
    )

    try:
        await transport.connect()
        if service:
            yield get_service(service, endpoint, transport=transport, serializer=serializer)
        else:
            yield TourClient(
                transport=transport,
                serializer=serializer,
                persistence=persistence,
                endpoint=endpoint,
            )
    finally:
        await transport.close()
