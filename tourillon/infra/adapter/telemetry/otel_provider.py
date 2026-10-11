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

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import Meter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Tracer

from tourillon.core.config import TelemetryConfig
from tourillon.core.ports import Telemetry
from tourillon.core.telemetry import build_noop_telemetry
from tourillon.infra.adapter.telemetry.otel_adapter import OpenTelemetryTelemetry
from tourlib.exceptions import ValidationError


class OpenTelemetryProvider:
    """TelemetryProvider implementation backed by the OpenTelemetry SDK."""

    def __init__(self) -> None:
        self._config: TelemetryConfig | None = None
        self._telemetry: Telemetry | None = None

    def create(self, config: TelemetryConfig) -> Telemetry:
        if self._telemetry is not None:
            if config != self._config:
                raise ValidationError(
                    "OpenTelemetryProvider is already initialized with a different telemetry config."
                )
            telemetry = self._telemetry
            assert telemetry is not None
            return telemetry

        resource = Resource.create(attributes={SERVICE_NAME: config.service_name})

        if not config.metrics_enabled and not config.tracing_enabled:
            self._config = config
            self._telemetry = build_noop_telemetry()
            telemetry = self._telemetry
            assert telemetry is not None
            return telemetry

        meter: Meter | None = None
        meter_provider: MeterProvider | None = None
        if config.metrics_enabled:
            meter_provider = self._setup_metrics(resource, config.metrics_options)
            meter = meter_provider.get_meter(config.service_name)

        tracer: Tracer | None = None
        tracer_provider: TracerProvider | None = None
        if config.tracing_enabled:
            tracer_provider = self._setup_tracing(resource, config.tracing_options)
            tracer = tracer_provider.get_tracer(config.service_name)

        close_hook = self._build_close_hook(
            meter_provider=meter_provider,
            tracer_provider=tracer_provider,
        )

        self._config = config
        self._telemetry = OpenTelemetryTelemetry(
            meter=meter,
            tracer=tracer,
            close_hook=close_hook,
        )
        return self._telemetry

    def _setup_metrics(self, resource: Resource, options: Mapping[str, Any]) -> MeterProvider:
        endpoint = self._require_endpoint(options)

        exporter = OTLPMetricExporter(
            endpoint=endpoint,
            certificate_file=options.get("certificate_file"),
            client_key_file=options.get("client_key_file"),
            client_certificate_file=options.get("client_certificate_file"),
            timeout=options.get("timeout"),
        )

        reader = PeriodicExportingMetricReader(
            exporter=exporter,
            export_interval_millis=options.get("export_interval_millis", 30000),
            export_timeout_millis=options.get("export_timeout_millis", 2000),
        )

        provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(provider)
        return provider

    def _setup_tracing(self, resource: Resource, options: Mapping[str, Any]) -> TracerProvider:
        endpoint = self._require_endpoint(options)

        exporter = OTLPSpanExporter(
            endpoint=endpoint,
            certificate_file=options.get("certificate_file"),
            client_key_file=options.get("client_key_file"),
            client_certificate_file=options.get("client_certificate_file"),
            timeout=options.get("timeout"),
        )

        provider = TracerProvider(resource=resource)
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)

        trace.set_tracer_provider(provider)
        return provider

    @staticmethod
    def _build_close_hook(
        *,
        meter_provider: MeterProvider | None,
        tracer_provider: TracerProvider | None,
    ) -> Callable[[], Awaitable[None]] | None:
        if meter_provider is None and tracer_provider is None:
            return None

        async def close_hook() -> None:
            if tracer_provider is not None:
                await asyncio.to_thread(tracer_provider.force_flush)
                await asyncio.to_thread(tracer_provider.shutdown)
            if meter_provider is not None:
                await asyncio.to_thread(meter_provider.force_flush)
                await asyncio.to_thread(meter_provider.shutdown)

        return close_hook

    @staticmethod
    def _require_endpoint(options: Mapping[str, Any]) -> str:
        endpoint: str | None = options.get("endpoint")
        if not endpoint:
            raise ValidationError("OTelTelemetry endpoint is required.")
        return endpoint
