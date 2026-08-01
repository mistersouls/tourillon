# Proposal: Bootstrap and Provisioning

- **Author**: Souleymane BA <soulsmister@gmail.com>
- **Status:** Accepted
- **Date:** 2026-05-02
- **Sequence:** 001
- **Schema version:** 2
- **Updated:** 2026-07-21 — config format aligned with implementation: `[node]` holds `replication_factor`, `partition_shift`, `seeds`; `[gossip.bootstrap]` replaces `[join]`; no separate `[drain]` section yet

---

## Summary

Before a Tourillon node can run, three things must exist: a Certificate
Authority, a signed server certificate embedded in a node config file, and a
signed client certificate embedded in an operator context file. This proposal
defines the `tourillon pki ca`, `tourillon config generate`, and
`tourillon config generate-context` commands, the TOML config format, the mTLS
dual-endpoint model, and the low-level transport layer (envelope framing,
dispatcher, TCP server). Everything that follows in
the system — node startup, KV operations, inter-node gossip — depends on the
artifacts this proposal produces.

---

## Motivation

Distributed systems often fail because certificate management is an afterthought.
Tourillon makes it impossible to run without mTLS: there is no plaintext
fallback, no self-signed workaround at connection time, and no separate cert
file paths to lose track of. Every artifact is produced by a single command and
stored inline in the relevant TOML file. An operator who follows the workflow
below cannot accidentally skip TLS.

The config format must also be fully self-contained so that copying a config
file to another machine is sufficient to run the node — no sidecar cert files,
no environment-specific paths, no hidden dependencies.

---

## CLI contract

### `tourillon pki ca`

Creates a new Certificate Authority (self-signed root). The private key is
written at mode `0600`.

```
$ tourillon pki ca \
    --out-cert ./ca.crt \
    --out-key  ./ca.key

✓ CA certificate written to ./ca.crt
✓ CA private key written to ./ca.key  (mode 0600)
```

Error — `--out-cert` / `--out-key` path not writable:

```
$ tourillon pki ca --out-cert /root/ca.crt --out-key /root/ca.key
✗ Cannot write to /root/ca.crt: permission denied
  exit code 1
```

### `tourillon config generate`

Issues a new server certificate signed by the supplied CA and writes a fully
populated `config.toml` containing the certificate, key, and CA inline as
base64-encoded PEM. The output file is written at mode `0600`.

```
$ tourillon config generate \
    --ca-cert  ./ca.crt \
    --ca-key   ./ca.key \
    --node-id  node-1 \
    --size     M \
    --kv-bind  0.0.0.0:7700 \
    --peer-bind 0.0.0.0:7701 \
    --out ./node-1.toml

✓ Certificate issued for node-1
✓ Config written to ./node-1.toml (mode 0600)
```

`--size` accepts `XS`, `S`, `M` (default), `L`, `XL`, `XXL`.

Error — `--ca-cert`/`--ca-key` missing:

```
$ tourillon config generate --node-id node-1 ...
✗ --ca-cert and --ca-key are required.
  exit code 1
```

Error — expired CA supplied:

```
$ tourillon config generate --ca-cert ./old-ca.crt --ca-key ./old-ca.key ...
✗ CA certificate expired on 2025-01-01. Generate a new CA first.
  exit code 1
```

Error — cert/key mismatch:

```
$ tourillon config generate --ca-cert ./ca.crt --ca-key ./wrong.key ...
✗ CA private key does not match CA certificate public key.
  exit code 1
```

### `tourillon config generate-context NAME`

Issues a client certificate signed by the supplied CA and writes a named
context entry in the specified `contexts.toml` file. The context bundles the
cluster CA, one or both endpoint addresses, and the client credential. Writing
is atomic (temp file + `os.replace`) to protect against mid-write crashes.

`--out` is required: the destination file must be stated explicitly so that
the operator is always aware of where credentials are written.

```
$ tourillon config generate-context prod \
    --ca-cert ./ca.crt \
    --ca-key  ./ca.key \
    --kv   kv.prod.example.com:7700 \
    --peer peer.prod.example.com:7701 \
    --out  ~/.config/tourillon/contexts.toml

✓ Client certificate issued
✓ Context "prod" written to ~/.config/tourillon/contexts.toml
```

To switch the active context after creation, use `tourctl config use-context`:

```
$ tourctl config use-context staging
✓ Active context set to "staging"
```


### Config file format (`config.toml`)

```toml
schema_version = 1   # bumped on incompatible format changes; checked at startup

[node]
id                 = "node-1"
size               = "M"         # XS=1 | S=2 | M=4 | L=8 | XL=16 | XXL=32 tokens; immutable after join
data_dir           = "./node-data"   # directory for state.toml and pid.lock
replication_factor = 3
partition_shift    = 10   # log₂(total_partitions); immutable after cluster bootstrap
seeds              = ["node-2.example.com:7701", "node-3.example.com:7701"]

[tls]
cert_data = "<base64-encoded PEM server cert>"
key_data  = "<base64-encoded PEM private key>"
ca_data   = "<base64-encoded PEM CA cert>"

[servers.kv]
bind = "0.0.0.0:7700"

[servers.peer]
bind      = "0.0.0.0:7701"
advertise = "peer.prod.example.com:7701"

[gossip]
anti_entropy_interval   = 30.0    # seconds between AE cycles
max_fan_out             = 6
max_payload_bytes       = 1048576 # 1 MiB per gossip.push envelope
max_digest_entries      = 4096    # entries per gossip.digest page
max_gossip_per_peer_rps = 20.0

[gossip.bootstrap]
initial_delay_s  = 1.0    # delay before first retry
max_delay_s      = 60.0   # ceiling for exponential growth
multiplier       = 2.0    # doubling factor per attempt
jitter           = 0.1    # 10 % uniform jitter
max_retries      = 10     # 0 = unlimited retries
connect_timeout  = 10.0   # per-seed connection timeout in seconds

# [drain] — not yet implemented; future proposal will define drain config
```

> **Note — `[cluster]` section removed**: `seeds`, `replication_factor`, and
> `partition_shift` moved into `[node]`. `[join]` is now `[gossip.bootstrap]`
> with exponential-backoff parameters. `[drain]` does not yet exist; it will
> be defined when the READY → DRAINING operator command is implemented.

### Contexts file format (`contexts.toml`)

```toml
current-context = "prod"

[[contexts]]
name = "prod"

[contexts.cluster]
name    = "prod"
ca_data = "<base64-encoded PEM CA cert>"

[contexts.endpoints]
kv   = "kv.prod.example.com:7700"
peer = "peer.prod.example.com:7701"

[contexts.credentials]
cert_data = "<base64-encoded PEM client cert>"
key_data  = "<base64-encoded PEM client key>"
```

### Four-level config precedence

`CLI flag > environment variable > config file > built-in default`

A value at a higher level always shadows the corresponding value at a lower
level.

### Process lock

On startup, before any socket is bound, the process acquires an exclusive lock
on `<data_dir>/pid.lock` (file-based, non-blocking `fcntl.LOCK_EX | LOCK_NB`
on POSIX, `LockFileEx` on Windows). A second `tourillon` process targeting the
same `data_dir` will fail immediately:

```
✗ Another tourillon process is already running for data_dir "./node-data"
  (pid.lock held). Remove stale lock or choose a different data_dir.
  exit code 1
```

The process lock is held for the entire lifetime of the process. Crash recovery
works automatically: the OS releases the lock when the process dies.

`pid.lock` content: `{"pid": <int>, "started_at": "<ISO-8601>"}` — informational
only; the lock validity is determined by the OS file lock, not the content.

### Dual-endpoint model

Each node exposes two independent TCP listeners:

| Listener | Purpose | Who connects |
|----------|---------|--------------|
| `servers.kv` | KV data-plane traffic: `put`, `get`, `delete` | Application clients, load balancers |
| `servers.peer` | Inter-node traffic (gossip, replication, handoff) + `tourctl` operator commands | Peer nodes, operators |

Both listeners use the same server certificate (node identity) but have
separate `ssl.SSLContext` instances and can enforce separate CA trust anchors.
This allows firewalling the KV plane from the peer plane and issuing separate
client certificate pools for applications vs. operators.

### Transport layer

Every connection — KV or peer — is framed using the `Envelope` wire format.
All multi-byte integer fields are big-endian.

```
 offset   bytes   field
 ──────   ─────   ─────────────────────────────────────────────────────────
 0        1       proto_version   uint8      current = 1
 1        2       schema_id       uint16     payload codec (1 = MessagePack)
 3        16      correlation_id  bytes[16]  UUID v4 (raw); request/response pairing
 19       4       payload_len     uint32     byte length of payload (M)
 23       1       kind_len        uint8      byte length of kind (N); 1 … KIND_MAX_LEN
 24       N       kind            UTF-8      envelope kind string
 24+N     M       payload         bytes      opaque; decoded per schema_id
```

**Constants:**
- `PROTO_VERSION = 1`
- `KIND_MAX_LEN = 64`  (UTF-8 bytes, not characters)

**Field semantics:**
- `proto_version` — allows protocol evolution; a receiver that does not
  support the version closes the connection immediately.
- `schema_id` — identifies the payload codec. `0` = raw bytes (no codec);
  `1` = MessagePack (default). Additional values reserved for future codecs
  (e.g. JSON for debugging). The core layer never hard-codes a codec; it
  delegates to a `SerializerPort`.
- `correlation_id` — a UUID v4 generated by the sender; carried unchanged in
  the response envelope. Enables async multiplexing: multiple in-flight
  request/response pairs on one connection without per-kind state.
- `kind` — identifies the message type (e.g. `kv.put`, `node.join`,
  `rebalance.transfer`). An envelope with an unknown `kind` causes immediate
  connection close; there is no error response.
- `payload` — opaque bytes; structure defined per `kind` and decoded using
  the codec indicated by `schema_id`.

The server component wraps `asyncio.start_server` with a mandatory mTLS
`ssl.SSLContext`. There is no cleartext fallback. The `Dispatcher` routes
envelopes to registered `ConnectionHandler` callables by `kind`. Handlers are
registered at startup and never modified at runtime.

**KV listener lifecycle:** the KV TCP socket is bound and accepting connections
only when `phase = READY`. In every other phase the socket is closed; clients
that attempt to connect will receive a TCP connection refusal at the OS level.
There is no application-level error response — the connection simply does not
exist.

### TCP client

Every outgoing connection — from `tourctl`, an application client, or a peer
node — uses the same `Envelope` framing as the server-side transport. A single
class, `TcpClient`, covers all use cases: one-shot commands (`tourctl kv get`)
and persistent multiplexed connections (gossip, replication, rebalance).

#### Connection model

`TcpClient` opens one mTLS connection and runs a background read loop that
delivers incoming envelopes to the correct caller by matching `correlation_id`.
The connection stays open for as long as the caller holds the client instance.
Callers that need only one round-trip should call `close()` immediately after
receiving the response; callers that send many requests (gossip, rebalance)
keep the connection alive.

#### Request patterns

| Method | Use when |
|--------|----------|
| `request(env, timeout)` | Exactly one response is expected (kv.get, kv.put, kv.delete, node.inspect, …) |
| `stream(env, timeout)` | The server sends N progress envelopes followed by a terminal envelope (node.join, rebalance.transfer.init, …). The caller iterates until it chooses to stop. |

Both methods share the same `correlation_id` mechanism: the client generates a
UUID v4, writes it into the request envelope, and the server copies it
unchanged into every response envelope for that request. The read loop routes
each incoming envelope to the `Future` or `AsyncGenerator` registered for that
`correlation_id`.

#### Timeouts

`RESPONSE_TIMEOUT` is a per-request deadline, measured from the moment the
request envelope is sent. If no matching response arrives within
`RESPONSE_TIMEOUT` seconds, `request()` raises `ResponseTimeoutError` and the
`correlation_id` is deregistered; the connection stays open for other
in-flight requests. `stream()` applies the same timeout to each individual
envelope in the sequence.

`READ_TIMEOUT` is a per-read deadline on the transport: the time allowed to
receive a complete envelope (header + kind + payload) once at least one byte
has arrived. Exceeding it closes the connection.

#### Limits (enforced server-side)

The server tracks the number of distinct in-flight `correlation_id`s per
connection. When that count reaches `MAX_IN_FLIGHT_PER_CONN`, the server
closes the connection immediately and emits a structured log event. The client
receives `ConnectionClosedError` on all pending requests.

`MAX_PAYLOAD_DEFAULT` is a global hard limit on every `Envelope` payload.
The server validates `payload_len` from the fixed header before allocating any
buffer. A violation causes the server to send an error response with
`kind=error.payload_too_large` and the original `correlation_id`, then close.

#### Production constants

| Constant | Default | Enforced by |
|----------|---------|-------------|
| `MAX_PAYLOAD_DEFAULT` | 4 MiB | Server (connection closed after error response) |
| `MAX_IN_FLIGHT_PER_CONN` | 128 | Server (connection closed) |
| `RESPONSE_TIMEOUT` | 30 s | Client (`ResponseTimeoutError`; connection stays open) |
| `READ_TIMEOUT` | 30 s | Server and client (connection closed) |

### Core invariants

- All certificate and key material stored inline as base64-encoded PEM
  exclusively. No `*_file` path variants anywhere.
- Both config files written at mode `0600` because both embed private key
  material.
- `tourillon.bootstrap.config.load_config` is the **only** code path that reads
  the config file or environment variables. All subsystems receive a
  `TourillonConfig` instance via constructor injection.
- `TourillonConfig` must be fully validated **before** any socket is bound,
  any file is opened, or any TLS context is created.
- The process lock on `<data_dir>/pid.lock` is acquired **before** any state
  file is read or written, ensuring no two processes can share the same
  `data_dir`.
- `tourctl` never reads or writes `config.toml` and never issues or stores
  certificates — it only reads and navigates `contexts.toml`.
- `contexts.toml` is never modified by a running `tourillon` node process.
- `[node].size` is immutable after the node joins a cluster. Changing it
  requires the node to leave, reconfigure, and re-join.
- `partition_shift` is immutable after cluster bootstrap. Changing it requires
  a full data migration (not supported).
- `schema_version` in `config.toml` is checked at startup. An unrecognised
  version causes an immediate startup failure with a clear error message.
- The core layer never imports `msgpack` directly. All payload
  serialisation/deserialisation goes through `SerializerPort`. The default
  infra adapter uses MessagePack with `schema_id = 1`.
- The KV TCP socket is bound **only when `phase = READY`**. A closed socket
  is the sole signal to callers that the node is not serving; there is no
  application-layer error or redirect.
- Every protocol violation on the receive path (bad `kind_len`, bad
  `payload_len`, unsupported `proto_version`) causes an immediate connection
  close after sending an error response. The server MUST read the full
  fixed-length header (24 bytes) before performing any validation so that the
  `correlation_id` is always available. After detecting a violation the server
  MUST send a single error response envelope carrying that `correlation_id`
  and a `kind` of the form `error.<event>` (e.g. `error.proto_version_unsupported`,
  `error.payload_too_large`), then close the connection. The client can
  therefore always correlate the error to its outstanding request.
  Exception: unknown `kind` closes the connection immediately without a
  response (the dispatcher has no handler to produce one).
- The receiver MUST validate `kind_len` and `payload_len` from the fixed header
  before allocating any buffer; `readexactly` is called only after validation.
- `MAX_PAYLOAD_DEFAULT` is a global limit applied to every `Envelope`
  regardless of `kind`. There are no per-kind payload limits.
- All traffic, including rebalance and snapshot transfers, travels inside
  `Envelope` messages. There are no out-of-band binary streams. Large logical
  payloads are split into multiple envelopes at the application layer.
- `TcpClient` MUST call `close()` during graceful shutdown; pending entries
  are failed with `ConnectionClosedError` if they cannot complete.

### Error paths

| Condition | What the user sees |
|-----------|-------------------|
| `--ca-cert`/`--ca-key` absent (`config generate`) | `✗ --ca-cert and --ca-key are required` exit 1 |
| `--ca-cert` file missing or unreadable | `✗ Cannot read CA certificate: <path>` exit 1 |
| CA expired | `✗ CA certificate expired on <date>` exit 1 |
| cert/key mismatch | `✗ CA private key does not match CA certificate public key` exit 1 |
| `--out` path not writable | `✗ Cannot write to <path>: permission denied` exit 1 |
| `config.toml` missing mandatory field | `✗ Missing required field [node].id` exit 1 |
| `config.toml` unknown `schema_version` | `✗ Unsupported schema_version=<N>; this binary supports version 1` exit 1 |
| `[node].size` invalid value | `✗ Invalid node size "<value>". Valid values: XS S M L XL XXL` exit 1 |
| TLS cert expired at node startup | `✗ Server certificate expired on <date>` exit 1 |
| Duplicate bind addresses | `✗ servers.kv and servers.peer cannot share the same bind address` exit 1 |
| `data_dir` not writable | `✗ Cannot create data_dir <path>: permission denied` exit 1 |
| `pid.lock` already held | `✗ Another tourillon process is already running for data_dir <path>` exit 1 |
| Unknown `proto_version` in received envelope | Error response `kind=error.proto_version_unsupported` sent with matching `correlation_id`; connection closed; log `{event: "proto_version_unsupported", version: N}` |
| `payload_len` > `MAX_PAYLOAD_DEFAULT` | Error response `kind=error.payload_too_large` sent with matching `correlation_id`; connection closed; log `{event: "payload_too_large", size: N, limit: M}` |
| `kind_len` == 0 or > `KIND_MAX_LEN` | Error response `kind=error.kind_len_invalid` sent with matching `correlation_id`; connection closed; log `{event: "kind_len_invalid", kind_len: N}` |
| `READ_TIMEOUT` exceeded | Connection closed without response (no complete header yet); log `{event: "read_timeout"}` |
| `MAX_IN_FLIGHT_PER_CONN` reached (server) | Connection closed; structured log `{event: "too_many_in_flight", count: N}` |

---

## Design

### Node size (`[node].size`)

Each node is assigned a **size class** that determines how many tokens
(virtual nodes) it claims on the consistent-hash ring. The size is set once at
`tourillon config generate` time and is **immutable** after the node joins a
cluster.

| Size | Tokens | Typical use |
|:----:|:------:|-------------|
| `XS` | 1 | Tiny / development node |
| `S` | 2 | Light workload |
| `M` | 4 | General purpose (default) |
| `L` | 8 | Higher-capacity node |
| `XL` | 16 | Large storage node |
| `XXL` | 32 | Maximum |

A heterogeneous cluster (nodes of different sizes) is fully supported. The
ring assigns proportionally more partitions to larger nodes.

---

## Design decisions

### Decision: inline base64 PEM — no file paths

**Alternatives considered:** `cert_file`, `key_file`, `ca_file` path references
in config (common in tools like nginx, etcd).
**Chosen because:** file paths create a dual-representation problem. The config
becomes invalid if the referenced file is moved, rotated, or absent in a
container. Inline material makes each config file fully self-contained: copying
it to another machine is sufficient. The cost — larger file size — is
negligible for certificates (a few KB).

### Decision: TOML over YAML

**Alternatives considered:** YAML with `PyYAML` or `ruamel.yaml`.
**Chosen because:** `tomllib` is stdlib (Python 3.11+); no additional runtime
dependency. TOML's strict native typing (integers, booleans, arrays) maps
directly onto the typed `TourillonConfig` dataclass and eliminates implicit-
coercion bugs (the YAML Norway problem, `yes`/`no`/`on`/`off` booleans). The
only additional dependency is `tomli-w` for writing TOML, which has no
transitive dependencies.

### Decision: separate KV and peer listeners

**Alternatives considered:** single listener, `kind`-based routing to separate
planes.
**Chosen because:** independent listeners allow separate CA trust anchors,
separate network policies per plane, and separate client certificate pools.
Operating a KV-only client on the data plane does not require peer-plane
certificates, which reduces blast radius if a client credential is compromised.

### Decision: MessagePack as default codec behind a `SerializerPort`

**Alternatives considered:** JSON (human-readable), Protocol Buffers
(schema-first, backward-compatible version negotiation).
**Chosen because:** MessagePack is compact, fast, self-describing enough for
debugging (types are not erased), and requires no schema compilation step. JSON
overhead is significant at the per-envelope level for a high-throughput KV
store. Protobuf adds a schema compilation toolchain.

The **core layer never imports `msgpack` directly**. All codec work goes
through `SerializerPort` (a `Protocol`), and the default infra adapter
(`MsgpackSerializerAdapter`) hard-codes `schema_id = 1`. This inversion allows
tests to inject a trivially-decodable stub serializer without shipping msgpack
as a test dependency, and allows future codecs (e.g. a JSON debug adapter) to
be swapped per connection without modifying business logic.

---

## Interfaces (informative)

```python
import dataclasses
import ssl
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Protocol


# ─── Node size ────────────────────────────────────────────────────────────────

class NodeSize(StrEnum):
    """Token count per physical node.  Immutable after the node joins."""
    XS  = "XS"   #  1 token
    S   = "S"    #  2 tokens
    M   = "M"    #  4 tokens  (default)
    L   = "L"    #  8 tokens
    XL  = "XL"   # 16 tokens
    XXL = "XXL"  # 32 tokens

    @property
    def token_count(self) -> int:
        return {"XS": 1, "S": 2, "M": 4, "L": 8, "XL": 16, "XXL": 32}[self.value]


# ─── Config dataclasses ───────────────────────────────────────────────────────

@dataclass(frozen=True)
class ServerConfig:
    bind: str
    advertise: str = ""   # defaults to bind when empty



@dataclass(frozen=True)
class TlsConfig:
    cert_data: str   # base64 PEM
    key_data: str    # base64 PEM
    ca_data: str     # base64 PEM


@dataclass(frozen=True)
class JoinConfig:
    max_retries: int = -1           # -1 = unlimited within deadline
    attempt_timeout: float = 10.0   # seconds
    deadline: float = 120.0
    backoff_base: float = 2.0
    backoff_max: float = 30.0
    max_concurrent: int = 4


@dataclass(frozen=True)
class DrainConfig:
    max_retries: int = -1
    attempt_timeout: float = 30.0
    deadline: float = 300.0
    backoff_base: float = 5.0
    backoff_max: float = 60.0
    max_concurrent: int = 4
    bandwidth_fraction: float = 1.0


@dataclass(frozen=True)
class TourillonConfig:
    node_id: str
    node_size: NodeSize             # immutable after join; determines token count
    data_dir: str                   # owns pid.lock and state.toml
    tls: TlsConfig
    kv_server: ServerConfig
    peer_server: ServerConfig
    seeds: list[str] = field(default_factory=list)
    rf: int = 3
    partition_shift: int = 10       # immutable after cluster bootstrap
    join: JoinConfig = field(default_factory=JoinConfig)
    drain: DrainConfig = field(default_factory=DrainConfig)
    schema_version: int = 1


# ─── Envelope ─────────────────────────────────────────────────────────────────

PROTO_VERSION: int = 1
KIND_MAX_LEN: int = 64   # maximum byte length of the kind field (UTF-8)


@dataclass(frozen=True)
class Envelope:
    """Wire-level message unit.

    Wire layout (all multi-byte fields big-endian):

     offset   bytes   field
     0        1       proto_version   uint8
     1        2       schema_id       uint16
     3        16      correlation_id  bytes[16]  (UUID v4 raw)
     19       4       payload_len     uint32
     23       1       kind_len        uint8
     24       N       kind            UTF-8 string
     24+N     M       payload         bytes
    """
    kind:           str
    payload:        bytes
    correlation_id: uuid.UUID = field(default_factory=uuid.uuid4)
    schema_id:      int = 1               # 1 = MessagePack (default); 0 = raw bytes
    proto_version:  int = PROTO_VERSION

    def __post_init__(self) -> None:
        kind_bytes = self.kind.encode("utf-8")
        if not (1 <= len(kind_bytes) <= KIND_MAX_LEN):
            raise ValueError(
                f"kind must be 1–{KIND_MAX_LEN} UTF-8 bytes; got {len(kind_bytes)}"
            )


# ─── Serializer port ──────────────────────────────────────────────────────────

class SerializerPort(Protocol):
    """Payload codec.

    The core layer never imports msgpack directly; it depends only on this
    port.  The default infra adapter (MsgpackSerializerAdapter) uses
    MessagePack and declares schema_id = 1.
    """
    schema_id: int   # must match the schema_id written into envelope headers

    def encode(self, obj: Any) -> bytes: ...
    def decode(self, data: bytes) -> Any: ...


# ─── Connection handler ───────────────────────────────────────────────────────

type ReceiveEnvelope = "() -> Awaitable[Envelope]"
type SendEnvelope    = "(Envelope) -> Awaitable[None]"


class ConnectionHandler(Protocol):
    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None: ...


# ─── Transport constants ──────────────────────────────────────────────────────

MAX_PAYLOAD_DEFAULT: int     = 4 * 1024 * 1024   # 4 MiB — global per-envelope payload ceiling
MAX_IN_FLIGHT_PER_CONN: int  = 128               # max distinct in-flight correlation_ids per connection
RESPONSE_TIMEOUT: float      = 30.0              # seconds — per-request deadline (client-side)
READ_TIMEOUT: float          = 30.0              # seconds — per-envelope read deadline (both sides)


# ─── Transport errors ─────────────────────────────────────────────────────────

class TransportError(Exception):
    """Base class for all transport-level errors.  Never sent across the wire."""


class ResponseTimeoutError(TransportError):
    """Raised by TcpClient.request() / .stream() when RESPONSE_TIMEOUT expires.

    The connection stays open; other in-flight requests are unaffected.
    """


class ConnectionClosedError(TransportError):
    """Raised when the remote peer closes the connection before a response arrives.

    All pending request() / stream() calls on this TcpClient are failed with
    this exception.
    """


# ─── TCP client ───────────────────────────────────────────────────────────────

class TcpClient:
    """Multiplexed mTLS client for all outgoing Envelope traffic.

    A single TcpClient instance wraps one TLS connection and a background
    read loop that routes incoming envelopes to the correct caller by
    correlation_id.

    Use request() when exactly one response envelope is expected.
    Use stream() when the server sends N progress envelopes followed by a
    terminal envelope (e.g. node.join, rebalance.transfer.init).

    Both methods apply RESPONSE_TIMEOUT per individual envelope received.
    Callers that need only one round-trip should call close() after use;
    long-lived subsystems (gossip, replication) keep the connection open.
    """

    async def connect(
        self,
        addr: str,
        tls_ctx: ssl.SSLContext,
    ) -> None:
        """Open a TLS connection to addr ("host:port") and start the read loop."""
        ...

    async def request(
        self,
        env: Envelope,
        timeout: float = RESPONSE_TIMEOUT,
    ) -> Envelope:
        """Send env and return the single response whose correlation_id matches.

        Raises ResponseTimeoutError if no matching response arrives within
        timeout seconds; the connection remains open.
        Raises ConnectionClosedError if the connection is lost before the
        response arrives.
        """
        ...

    async def stream(
        self,
        env: Envelope,
        timeout: float = RESPONSE_TIMEOUT,
    ) -> AsyncIterator[Envelope]:
        """Send env and yield every response envelope sharing its correlation_id.

        timeout applies to each individual envelope in the sequence.
        The caller is responsible for detecting the terminal envelope (e.g. a
        kind ending in '.done') and breaking out of the iteration.
        Raises ResponseTimeoutError or ConnectionClosedError on failure.
        """
        ...

    async def close(self) -> None:
        """Close the connection; pending request() / stream() calls receive
        ConnectionClosedError.
        """
        ...

    @property
    def is_connected(self) -> bool: ...


# ─── PKI dataclasses ─────────────────────────────────────────────────────────

@dataclasses.dataclass(frozen=True)
class CaRequest:
    """Parameters required to generate a self-signed Certificate Authority.

    The CA produced from this request is the trust root for all mTLS
    connections in the cluster. The caller is responsible for choosing an
    appropriate validity window: cluster CAs typically use a multi-year
    validity while leaf certs use a shorter window.
    """

    common_name: str
    valid_days: int
    key_size: int
    out_cert: Path
    out_key: Path


@dataclasses.dataclass(frozen=True)
class CertRequest:
    """Parameters required to issue a leaf certificate signed by an existing CA.

    Both san_dns and san_ip may be empty tuples for client certificates.
    For server certificates at least one SAN entry is required; the CLI
    layer enforces this constraint before constructing a CertRequest.

    The CA certificate and private key are read ephemerally during signing
    and must not be stored on any cluster node after this operation completes.
    """

    common_name: str
    san_dns: tuple[str, ...]
    san_ip: tuple[str, ...]
    valid_days: int
    ca_cert: Path
    ca_key: Path
    out_cert: Path
    out_key: Path
    key_size: int = 2048


# ─── PKI errors ───────────────────────────────────────────────────────────────

class PkiError(Exception):
    """Raised by PKI adapters for any certificate generation or I/O failure.

    This exception is the single surface that CLI commands catch. It wraps
    lower-level errors from the cryptography library, OSError, and permission
    failures so that callers never need to import cryptography internals.
    """


# ─── PKI ports ────────────────────────────────────────────────────────────────

class CertificateAuthorityPort(Protocol):
    """Contract for generating a self-signed Certificate Authority key pair.

    Implementors must write both out_cert and out_key to disk with the
    private key file restricted to mode 0600 (owner read/write only). The
    operation must be effectively atomic: on failure, no partial file is
    left on disk. Raise PkiError for any error condition.
    """

    def generate_ca(self, request: CaRequest) -> None:
        """Generate a self-signed CA certificate and private key to disk."""
        ...


class CertificateIssuerPort(Protocol):
    """Contract for issuing a leaf certificate signed by an existing CA.

    The CA private key is consumed ephemerally. The issued certificate and
    its private key are written to disk. Private key mode must be 0600.
    Raise PkiError for cryptographic, I/O, or permission errors.
    """

    def issue_cert(self, request: CertRequest) -> None:
        """Issue a certificate signed by the CA described in the request."""
        ...
```

---

## Test scenarios

| # | Fixture | Action | Expected |
|---|---------|--------|----------|
| 1 | Valid inline PEM config | `load_config(config_dict)` | Returns `TourillonConfig`; all fields correct; `node_size.token_count == 4` for `size="M"` |
| 2 | Config with expired server cert | `load_config` | `ConfigError` mentioning expiry; no socket bound |
| 3 | Config with cert/key mismatch | `load_config` | `ConfigError` mentioning mismatch |
| 4 | Config missing `[node].id` | `load_config` | `ConfigError` naming missing field |
| 5 | Config `[node].size = "HUGE"` | `load_config` | `ConfigError` listing valid values |
| 6 | `servers.kv.bind == servers.peer.bind` | Node startup | Startup fails; both addresses logged |
| 7 | In-memory dispatcher; unknown `kind` sent | Send `Envelope(kind="unknown")` | Connection closed; no application response |
| 8 | `Envelope` construction | `kind` of 65 UTF-8 bytes | `ValueError` at construction time |
| 9 | `Envelope` encoding/decoding round-trip | `encode → decode` via `MsgpackSerializerAdapter` | Decoded value equals original |
| 10 | `Envelope` with `proto_version=99` received | Server reads envelope | Server sends `kind=error.proto_version_unsupported` response with matching `correlation_id`, then closes |
| 11 | `correlation_id` in request envelope | Handler sends response | Response carries identical `correlation_id` |
| 12 | Node not `READY`; client attempts TCP connect to KV port | TCP connect | Connection refused at OS level; no application response |
| 13 `[e2e]` | Running node with mTLS; no client cert | Open TLS connection without client cert | TLS handshake fails; no application-level response |
| 14 `[e2e]` | Full provisioning session | `pki ca → config generate → config generate-context → tourctl config use-context` | All steps succeed; output files at mode 0600 |
| 15 | `TcpClient.request()` with valid mTLS | `request(env, timeout)` | Returns response with matching `correlation_id` |
| 16 | Server; `MAX_IN_FLIGHT_PER_CONN` already reached on a connection | Client sends one more request | Server closes the connection; log `too_many_in_flight`; client receives `ConnectionClosedError` |
| 17 | Server receives envelope with `payload_len` > `MAX_PAYLOAD_DEFAULT` | Receive oversized envelope | Server sends `kind=error.payload_too_large` response with matching `correlation_id`, then closes; log `payload_too_large` |
| 18 | `TcpClient`; connection lost mid-flight | Outstanding `request()` awaiting response | Pending entry fails with `ConnectionClosedError` |
| 19 | `TcpClient.stream()` multi-response | `node.join` request yields 3 progress envelopes then a `.done` | All 4 envelopes delivered in order; caller terminates iteration on `.done` |
| 20 | `TcpClient.request()`; server never responds | Send request; server stays silent until `RESPONSE_TIMEOUT` | `ResponseTimeoutError` raised on the client; connection stays open |

---

## Exit criteria

- [ ] All test scenarios pass.
- [ ] `tourillon pki ca` → `tourillon config generate` → `tourillon config generate-context` → `tourctl config list` completes without error in a single terminal session.
- [ ] Connections without valid mTLS certificates are refused (no application-level response).
- [ ] All config and context files written at mode `0600`.
- [ ] `Envelope` round-trip (encode → wire → decode) preserves `kind`, `payload`, `correlation_id`, `schema_id`.
- [ ] KV TCP connect is refused when phase ≠ `READY`; returns no application response.
- [ ] `TcpClient.request()` round-trip preserves `correlation_id`.
- [ ] Server enforces `MAX_IN_FLIGHT_PER_CONN`; exceeding it closes the connection and emits a `too_many_in_flight` log entry; client receives `ConnectionClosedError`.
- [ ] An oversized envelope payload causes the server to send an error response with matching `correlation_id` before closing; log entry `payload_too_large` emitted.
- [ ] `TcpClient.stream()` delivers all response envelopes in order until the caller terminates the iterator.
- [ ] A request whose `RESPONSE_TIMEOUT` expires raises `ResponseTimeoutError` on the client; the connection remains open.
- [ ] `uv run pytest --cov-fail-under=90` passes.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

Node startup, join, leave, KV operations, ring partitioning, gossip. Certificate
rotation (planned for a later proposal).
