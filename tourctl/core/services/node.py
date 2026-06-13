import ssl
from pathlib import Path
from typing import Any

from tourlib.contexts import ContextConfigurer
from tourlib.envelope import Envelope
from tourlib.exceptions import ConnectionClosedError, ResponseTimeoutError
from tourlib.ports.loader import ConfigReadWriter
from tourlib.ports.serializer import Serializer
from tourlib.ports.tls import TlsContext
from tourlib.transport import TcpClient


class NodeClientService:
    def __init__(
        self,
        config_rw: ConfigReadWriter,
        serializer: Serializer,
        tls_ctx: TlsContext
    ) -> None:
        self._configurer = ContextConfigurer(config_rw)
        self._serializer = serializer
        self._tls = tls_ctx

    async def join(
        self,
        *,
        address: str,
        seeds: str | None,
        context: str | None,
        contexts_file: Path,
        timeout: float,
    ) -> tuple[int, str, bool]:
        try:
            ssl_ctx = self._resolve_ssl_context(context, contexts_file)
        except ValueError as exc:
            return 1, str(exc), True

        payload = self._build_join_payload(seeds)
        try:
            response = await self._request_join(
                address=address,
                timeout=timeout,
                ssl_ctx=ssl_ctx,
                payload=payload,
            )
        except ResponseTimeoutError:
            return 2, f"Error: no response from {address} within {timeout}s.", True
        except (ConnectionClosedError, OSError) as exc:
            return 2, f"Error: connection error to {address}: {exc}", True
        return self._map_response(response)

    def _resolve_ssl_context(
        self,
        context: str | None,
        contexts_file: Path,
    ) -> ssl.SSLContext:
        contexts = self._configurer.load_contexts(contexts_file)
        selected_context = context or contexts.current_context
        if selected_context is None:
            raise ValueError("Error: context required (use --context or use-context).")

        ctx = contexts.get(selected_context)
        if ctx is None:
            raise ValueError(f'Error: context "{selected_context}" not found.')
        if not ctx.cluster.ca_data or not ctx.credentials.cert_data or not ctx.credentials.key_data:
            raise ValueError(
                "Error: context is missing TLS certificate material (ca/cert/key)."
            )
        return self._tls.build_client_ssl_context(
            ctx.credentials.cert_data,
            ctx.credentials.key_data,
            ctx.cluster.ca_data,
        )

    @staticmethod
    def _build_join_payload(seeds: str | None) -> dict[str, object]:
        payload: dict[str, object] = {}
        if seeds is not None:
            payload["seeds_override"] = [
                token.strip() for token in seeds.split(",") if token.strip()
            ]
        return payload

    async def _request_join(
        self,
        *,
        address: str,
        timeout: float,
        ssl_ctx: ssl.SSLContext,
        payload: dict[str, Any],
    ) -> Envelope:
        client = TcpClient()
        try:
            await client.connect(address, ssl_ctx)
            return await client.request(
                Envelope.create(
                    self._serializer.encode(payload),
                    kind="node.join",
                    schema_id=self._serializer.schema_id,
                ),
                timeout=timeout,
            )
        finally:
            await client.close()

    def _map_response(self, response: Envelope) -> tuple[int, str, bool]:
        if response.kind == "node.join.ok":
            data = self._serializer.decode(response.payload)
            node_id = data.get("node_id", "unknown")
            return 0, f"Node {node_id} is now JOINING.", False
        if response.kind == "node.join.error":
            data = self._serializer.decode(response.payload)
            return 1, f'Error: {data.get("message", "join failed")}', True
        return 2, f"Error: unexpected response kind {response.kind}.", True

