---
applyTo: "tourlib/**/*.py,tourclient/**/*.py,tourillon/bootstrap/handlers/**/*.py"
---

# Wire protocol — envelopes, serialization, services

## Frame layout

```
Envelope := header(32B) || kind(UTF-8, kind_len bytes) || payload(payload_len bytes)
header   := struct ">B H B 16s I B 7s"
            proto_version | schema_id | mode | correlation_id(uuid16) | payload_len | kind_len | reserved(7)
mode     := 0 RPC | 1 fire-and-forget
```

Changing the header struct, the field order, `PROTOCOL_VERSION` or `MSGPACK_SCHEMA_ID` is a **breaking protocol change**. It requires an explicit compatibility story and tests for both the old and new shape. Never do it casually.

## Kind naming

- Lowercase dotted `domain.action`, regex-validated on construction; max length is `MAX_KIND_LEN`.
- Responses: `<request kind>.accept` on success, `<request kind>.reject` on a domain failure.
- Streaming responses reuse the request's correlation id and emit multiple envelopes.
- Transport-level failures use `TRANSPORT_ERROR_KIND` (`transport.error`), not an application reject.

## Writing a handler

```python
from tourillon.bootstrap.deps import client_dispatcher, get_manager
from tourlib.envelope import Envelope, replay_envelope

dispatcher = client_dispatcher()   # or peer_dispatcher() for node-to-node kinds


@dispatcher.on("domain.action")
async def action(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    serializer = manager.serializer
    try:
        data = serializer.decode(request.payload)
        result = await manager.<service>.<op>(RequestModel(**data))
        yield replay_envelope(
            kind="domain.action.accept",
            request=request,
            payload=serializer.encode(None if result is None else result.to_dict()),
        )
    except DomainError as exc:
        yield replay_envelope(
            kind=f"{request.kind}.reject",
            request=request,
            payload=serializer.encode(exc.to_dict()),
        )
```

Rules:

- Handlers are **async generators** that `yield` envelopes. Even single-response handlers yield exactly once.
- Always use `replay_envelope` so `proto_version`, `schema_id`, `mode` and `correlation_id` are carried over. Never construct a response `Envelope` by hand.
- Always encode/decode through the injected `Serializer`. Never `json`, never `pickle`, never manual struct packing in a handler.
- Catch the **domain** exception type and reject; let unexpected exceptions propagate to the transport layer.
- Register a kind exactly once — `Dispatcher.register` raises on duplicates.
- Choose the right dispatcher: `client_dispatcher()` for user-facing kinds, `peer_dispatcher()` for internal ones.
- Keep decode → validate → delegate → encode. Business logic belongs in `tourillon/core/services/`, not in the handler.

## Wire models

- Live in `tourlib/models/`, are frozen dataclasses, and validate in `__post_init__`.
- Enforce the size limits from `tourlib/constant.py` (`MAX_NAMESPACE_LEN`, `MAX_KEY_LEN`, `MAX_NODE_ID_LEN_BYTES`, `MAX_USER_ENVELOPE_PAYLOAD`).
- Byte encodings (`Address`, `HLCTimestamp`) are length-prefixed and documented in the module. If you change one, you change the on-disk and on-wire layout — treat it as a breaking change and update the docstring.
- Never trust a decoded payload: a handler or client decoding `dict[str, Any]` must validate before constructing a model. Do not splat untrusted dicts into a dataclass without checking keys.

## Client services and the registry

A client service wraps one domain:

```python
class XService:
    def __init__(self, transport: Transport, serializer: Serializer) -> None: ...

    async def op(self, ...) -> Result:
        envelope = await self._transport.request("domain.action", payload_dict)
        # validate envelope.kind, handle "<kind>.reject" -> RequestRejectedError,
        # anything else -> ResponseError
```

- Register it in `tourclient/core/services/registry.py` with an accurate `supported_endpoints` frozenset.
- `client` endpoint = user-facing API; `peer` endpoint = operator/internal access. Current matrix: `probe` client+peer, `kv` client+peer, `node` peer, `inspect` peer.
- Internal kinds (`gossip.*`, `replication.*`, `transfer.*`) deliberately have **no** client service. Do not add one without a stated reason.
- Always check the response kind before decoding. Map `.reject` to `RequestRejectedError` and unexpected kinds to `ResponseError`.
- Paginated calls (`node.inspect`) must make progress: validate the cursor advances and bound the loop.

## Transport

- `EnvelopeClient` multiplexes by `correlation_id` with pending futures for RPC and queues for streams. Every new correlation id must be cleaned up in a `finally`, including on timeout and cancellation.
- Respect `ENVELOPE_RESPONSE_TIMEOUT` and `ENVELOPE_CONNECTION_TIMEOUT`; a new network call without a timeout is a bug.
- Framing must tolerate partial reads: read the header exactly, validate lengths against the maxima, then read the body exactly. Reject truncated or oversized frames rather than guessing.
- `EnvelopeServer` requires an SSL context with `verify_mode == CERT_REQUIRED`. Never relax mTLS, not even for tests or local development.
- Always close transports in a `finally` or via `async with`.

## Checklist for adding a kind

1. Model in `tourlib/models/`.
2. Handler in `tourillon/bootstrap/handlers/` on the correct dispatcher.
3. Client service method in `tourclient/core/services/`.
4. Registry entry with correct `supported_endpoints`.
5. Tests: handler accept, handler reject, client decode, client error mapping, envelope round-trip.
