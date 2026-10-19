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

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from tourclient.core.services.inspect import InspectService
from tourclient.core.services.kv import KvService
from tourclient.core.services.node import NodeService
from tourclient.core.services.probe import ProbeService
from tourclient.core.transport import Transport
from tourlib import Serializer

# Service type alias for type hints
ServiceType = ProbeService | InspectService | NodeService | KvService


@dataclass(frozen=True)
class ServiceRegistration:
    """Metadata about a service: name, supported endpoints, factory."""

    name: str
    supported_endpoints: frozenset[Literal["client", "peer"]]
    factory: Callable[[Transport, Serializer], ServiceType]


SERVICE_REGISTRY: dict[str, ServiceRegistration] = {
    "probe": ServiceRegistration(
        name="probe",
        supported_endpoints=frozenset(["client", "peer"]),  # type: ignore[arg-type]
        factory=ProbeService,
    ),
    "inspect": ServiceRegistration(
        name="inspect",
        supported_endpoints=frozenset(["peer"]),  # type: ignore[arg-type]
        factory=InspectService,
    ),
    "node": ServiceRegistration(
        # Join and asynchronous leave share the peer-only operator surface.
        name="node",
        supported_endpoints=frozenset(["peer"]),  # type: ignore[arg-type]
        factory=NodeService,
    ),
    "kv": ServiceRegistration(
        name="kv",
        supported_endpoints=frozenset(["client", "peer"]),  # type: ignore[arg-type]
        factory=KvService,
    ),
}


def get_service(
    service_name: str | None,
    endpoint: Literal["client", "peer"],
    *,
    transport: Transport,
    serializer: Serializer,
) -> ServiceType:
    """Instantiate a service by name, validating it's available on the endpoint.

    Args:
        service_name: Registered service name, or None for error.
        endpoint: Target endpoint ("client" or "peer").
        transport: Transport instance for the service.
        serializer: Serializer instance for the service.

    Returns:
        Instantiated service object.

    Raises:
        ValueError: If service_name is None (use TourClient instead).
        ValueError: If service is not available on the endpoint.
        UnknownServiceError: If the service name is not registered.
    """
    if service_name is None:
        raise ValueError("service_name must be provided (cannot be None)")

    registration = SERVICE_REGISTRY.get(service_name)
    if not registration:
        from tourclient.exception import UnknownServiceError

        raise UnknownServiceError(f"Unknown service: {service_name}")

    if endpoint not in registration.supported_endpoints:
        supported = ", ".join(sorted(registration.supported_endpoints))
        raise ValueError(
            f"Service '{service_name}' is not available on endpoint '{endpoint}'. "
            f"Supported endpoints: {supported}"
        )

    return registration.factory(transport, serializer)


def validate_service_endpoint(
    service: str | None,
    endpoint: Literal["client", "peer"],
) -> None:
    """Validate that the requested service is available on the given endpoint.

    This is a lightweight validation function used before connection.
    For instantiation, use get_service() instead.

    Args:
        service: Service name or None (for default TourClient).
        endpoint: Target endpoint ("client" or "peer").

    Raises:
        ValueError: If service is not available on the endpoint.
    """
    if service is None:
        # TourClient has no endpoint constraints
        return

    registration = SERVICE_REGISTRY.get(service)
    if not registration:
        from tourclient.exception import UnknownServiceError

        raise UnknownServiceError(f"Unknown service: {service}")

    if endpoint not in registration.supported_endpoints:
        supported = ", ".join(sorted(registration.supported_endpoints))
        raise ValueError(
            f"Service '{service}' is not available on endpoint '{endpoint}'. "
            f"Supported endpoints: {supported}"
        )
