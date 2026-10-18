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

import pytest

from tourclient.core.services.registry import SERVICE_REGISTRY
from tourclient.exception import UnknownServiceError


def test_service_registry_contains_probe() -> None:
    """Probe service must be registered."""
    assert "probe" in SERVICE_REGISTRY


def test_service_registry_contains_inspect() -> None:
    """Inspect service must be registered."""
    assert "inspect" in SERVICE_REGISTRY


def test_service_registry_contains_kv() -> None:
    """KV service must be registered."""
    assert "kv" in SERVICE_REGISTRY


def test_service_registry_getitem_returns_probe_registration() -> None:
    """ServiceRegistry.__getitem__ must return the probe registration."""
    registration = SERVICE_REGISTRY["probe"]
    assert registration.name == "probe"
    assert "client" in registration.supported_endpoints
    assert "peer" in registration.supported_endpoints


def test_service_registry_getitem_raises_on_unknown_service() -> None:
    """ServiceRegistry access with unknown service should raise KeyError."""
    with pytest.raises(KeyError):
        _ = SERVICE_REGISTRY["nonexistent"]


def test_service_registry_get_returns_registration() -> None:
    """ServiceRegistry.get() should return registration or None."""
    registration = SERVICE_REGISTRY.get("probe")
    assert registration is not None
    assert registration.name == "probe"


def test_service_registry_get_returns_none_for_unknown() -> None:
    """ServiceRegistry.get() should return None for unknown service."""
    result = SERVICE_REGISTRY.get("nonexistent")
    assert result is None


def test_service_registry_get_with_default() -> None:
    """ServiceRegistry.get() should return default for unknown service."""
    # Create a dummy registration for testing
    from tourclient.core.services.registry import ServiceRegistration

    dummy = ServiceRegistration(
        name="dummy",
        supported_endpoints=frozenset(),
        factory=SERVICE_REGISTRY["probe"].factory,
    )
    result = SERVICE_REGISTRY.get("nonexistent", dummy)
    assert result == dummy


def test_service_registry_contains_uses_in_operator() -> None:
    """ServiceRegistry should support 'in' operator."""
    assert "probe" in SERVICE_REGISTRY
    assert "nonexistent" not in SERVICE_REGISTRY


def test_service_registry_keys() -> None:
    """ServiceRegistry.keys() should return registered service names."""
    keys = list(SERVICE_REGISTRY.keys())
    assert "probe" in keys
    assert "inspect" in keys
    assert "kv" in keys


def test_validate_service_endpoint_allows_none_service() -> None:
    """None service (TourClient) has no endpoint constraints."""
    from tourclient.core.services.registry import validate_service_endpoint

    # Should not raise
    validate_service_endpoint(None, "client")
    validate_service_endpoint(None, "peer")


def test_validate_service_endpoint_allows_probe_on_client() -> None:
    """Probe service should be available on client endpoint."""
    from tourclient.core.services.registry import validate_service_endpoint

    # Should not raise
    validate_service_endpoint("probe", "client")


def test_validate_service_endpoint_allows_probe_on_peer() -> None:
    """Probe service should be available on peer endpoint."""
    from tourclient.core.services.registry import validate_service_endpoint

    # Should not raise
    validate_service_endpoint("probe", "peer")


def test_validate_service_endpoint_allows_inspect_on_peer() -> None:
    """Inspect service should be available on peer endpoint."""
    from tourclient.core.services.registry import validate_service_endpoint

    validate_service_endpoint("inspect", "peer")


def test_validate_service_endpoint_rejects_inspect_on_client() -> None:
    """Inspect service should not be available on client endpoint."""
    from tourclient.core.services.registry import validate_service_endpoint

    with pytest.raises(ValueError, match="not available on endpoint 'client'"):
        validate_service_endpoint("inspect", "client")


def test_validate_service_endpoint_rejects_unknown_service() -> None:
    """Unknown service should raise UnknownServiceError."""
    from tourclient.core.services.registry import validate_service_endpoint

    with pytest.raises(UnknownServiceError, match="Unknown service: nonexistent"):
        validate_service_endpoint("nonexistent", "client")


def test_validate_service_endpoint_allows_kv_on_client_and_peer() -> None:
    """KV service should be available on both client and peer endpoints."""
    from tourclient.core.services.registry import validate_service_endpoint

    validate_service_endpoint("kv", "client")
    validate_service_endpoint("kv", "peer")


def test_service_registry_probe_factory_is_callable() -> None:
    """Probe factory must be a callable."""
    registration = SERVICE_REGISTRY["probe"]
    assert callable(registration.factory)
