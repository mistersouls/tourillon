# Proposal: Bootstrap & Provisioning

<!-- Naming: proposal-<short-desc>-MMDDYYYY-SEQ.md
     Example: proposal-bootstrap-05312026-001.md     -->

**Author**: Tourillon Contributors <tourillon@example.com>
**Status:** Draft
**Date:** 2026-05-31
**Sequence:** 001

---

## Summary

This proposal specifies the full bootstrap and provisioning surface of a Tourillon node:
PKI certificate generation, `config.toml` and `contexts.toml` file formats, the daemon
startup wiring that loads those files, the `Envelope` wire framing protocol, the
`Dispatcher`/`TcpClient`/`TcpServer` transport layer, mutual TLS (mTLS) context
construction, and the `MsgpackSerializerAdapter` payload codec. It also introduces
`parse_duration` and `parse_bytes` helpers and mandates that all duration and size fields
in `config.toml` use unit-embedded strings (`"10s"`, `"1Mi"`) rather than bare numbers.
It also introduces `TourillonCore`, an immutable bootstrap-level facade that centralises
the concrete dependencies required by transport handlers and dispatcher wiring (storage,
serializer, TLS contexts, connection pools, topology snapshots, clocks, probe manager,
and any other adapters the dispatchers need).

The standard operator workflow is three commands:

1. `tourillon pki ca` — create the cluster CA.
2. `tourillon config generate --ca-cert --ca-key …` — issue a node server certificate
   **and** write `config.toml` in one step.
3. `tourillon config generate-context NAME --ca-cert --ca-key …` — issue a client
   certificate **and** write the operator context into `contexts.toml`.

A standalone `tourillon pki issue` command is available for advanced workflows (cert
rotation, external CA integration) but is not required for the standard path.

Every piece of infrastructure introduced here is used verbatim by all subsequent proposals.
No amendments to this proposal are permitted; later proposals extend it only through
new TOML sections and new `Dispatcher` registrations.

---

## Motivation

Before a Tourillon cluster can do anything useful, three foundational problems must be
solved cleanly:

1. **Identity** — Each node and each operator tool must hold a certificate signed by the
   same cluster CA so that every TCP connection can be mutually authenticated with zero
   shared secrets beyond the CA.

2. **Configuration** — A node needs to know its identity, its bind addresses, its seeds,
   and its operational tunables before it can open any socket. An operator tool needs to
   know which cluster CA to trust and which certificate to present. Both must be expressible
   as self-contained files (no `*_file` path indirections) so that a copy of the file is
   sufficient to reproduce the setup.

3. **Transport** — All inter-node and client-node traffic flows over a single multiplexed
   binary protocol (`Envelope`) with a fixed header. The framing layer, request-response
   multiplexer, and handler dispatcher must be fully specified so that every subsequent
   proposal can add envelope kinds without touching the transport layer.

Without this proposal none of the functional proposals can be implemented.

---

## CLI contract

All commands print to stdout on success and to stderr on failure.
Exit code `0` = success; `1` = user error (bad arguments, file not found);
`2` = internal/crypto error.

### `tourillon pki ca` — generate a self-signed Certificate Authority

```
$ tourillon pki ca [OPTIONS]

  Generate a self-signed CA certificate and private key.
  The CA is the single trust root for all mTLS connections in this cluster.
  Store ca-key.pem offline; it is NOT needed on any cluster node.

Options:
  --out-cert PATH  Output path for the CA certificate (PEM)  [default: ./ca.crt]
  --out-key PATH   Output path for the CA private key (PEM)  [default: ./ca.key]
  --name TEXT      CA common name                            [default: tourillon-ca]
  --days INT       Validity in days                          [default: 3650]
  --key-size INT   RSA key size in bits                      [default: 2048]
  --help           Show this message and exit.
```

Both output files are written at mode `0600`.

**Happy path output (stdout):**
```
✓ CA certificate written to ./ca.crt
✓ CA private key  written to ./ca.key  (mode 0600)
```

**Error – output path not writable (stderr, exit 1):**
```
✗ Cannot write to /root/ca.crt: permission denied
```

**Error – crypto failure (stderr, exit 2):**
```
✗ CA generation failed: <reason>
```

---

### `tourillon pki issue` — issue a leaf certificate signed by an existing CA

Advanced / scripted use only. The standard workflow embeds cert issuance inside
`tourillon config generate` and `tourillon config generate-context`.

```
$ tourillon pki issue [OPTIONS]

  Issue a leaf certificate signed by the cluster CA.
  Useful when integrating with external cert workflows or rotating individual certs.

Options:
  --ca-cert PATH    CA certificate file (PEM)           [required]
  --ca-key PATH     CA private key file (PEM)           [required]
  --name TEXT       Certificate common name             [required]
  --san-dns TEXT    SAN DNS name (repeatable)
  --san-ip TEXT     SAN IP address (repeatable)
  --out-cert PATH   Output certificate path             [default: ./<name>.pem]
  --out-key PATH    Output private key path             [default: ./<name>-key.pem]
  --days INT        Validity in days                    [default: 365]
  --key-size INT    RSA key size in bits                [default: 2048]
  --help            Show this message and exit.
```

**Happy path output (stdout):**
```
✓ Certificate issued for node-1
✓ Certificate written to ./node-1.pem
✓ Private key  written to ./node-1-key.pem  (mode 0600)
```

**Error – CA certificate expired (stderr, exit 2):**
```
✗ CA certificate expired on 2025-01-01. Generate a new CA first.
```

**Error – CA key/cert mismatch (stderr, exit 2):**
```
✗ CA private key does not match CA certificate public key.
```

**Error – CA file not found (stderr, exit 1):**
```
✗ Cannot read CA certificate: <path>: no such file or directory
```

---

### `tourillon config generate` — issue a certificate and generate a node `config.toml`

Issues a new server certificate signed by the supplied CA, then writes a fully
self-contained `config.toml` with the certificate, key, and CA embedded as
base64-encoded PEM. The output file is written at mode `0600`.

```
$ tourillon config generate \
    --ca-cert  ./ca.crt \
    --ca-key   ./ca.key \
    --node-id  node-1 \
    --size     M \
    --kv-bind  0.0.0.0:7000 \
    --peer-bind 0.0.0.0:7001 \
    --out ./node-1.toml
```

```
Options:
  --ca-cert PATH          CA certificate file (PEM)           [required]
  --ca-key PATH           CA private key file (PEM)           [required]
  --node-id TEXT          Node identifier (auto-generated if omitted)
  --size [XS|S|M|L|XL|XXL]  Node size class                  [default: M]
  --data-dir PATH         Data directory                      [default: ./data]
  --kv-bind TEXT          KV listener bind address            [default: 0.0.0.0:7000]
  --kv-advertise TEXT     KV advertise address (defaults to --kv-bind)
  --peer-bind TEXT        Peer listener bind address          [default: 0.0.0.0:7001]
  --peer-advertise TEXT   Peer advertise address (defaults to --peer-bind)
  --seed TEXT             Seed peer address (repeatable)
  --rf INT                Replication factor                  [default: 3]
  --partition-shift INT   Partition shift (immutable after bootstrap) [default: 10]
  --days INT              Certificate validity in days        [default: 365]
  --out PATH              Output config path                  [default: ./config.toml]
  --help                  Show this message and exit.
```

**Happy path output (stdout):**
```
✓ Certificate issued for node-1
✓ Config written to ./node-1.toml  (mode 0600)
```

**Error – CA certificate expired (stderr, exit 2):**
```
✗ CA certificate expired on 2025-01-01. Generate a new CA first.
```

**Error – CA key/cert mismatch (stderr, exit 2):**
```
✗ CA private key does not match CA certificate public key.
```

**Error – required option missing (stderr, exit 1):**
```
✗ --ca-cert and --ca-key are required.
```

**Error – output path not writable (stderr, exit 1):**
```
✗ Cannot write to ./node-1.toml: permission denied
```

---

### `tourillon config generate-context` — issue a client certificate and add a context

Issues a client certificate signed by the supplied CA and writes a named context
entry in the specified `contexts.toml` file. The context bundles the cluster CA,
one or both endpoint addresses, and the client credential. Writing is atomic
(temp file + `os.replace`). The output file is written at mode `0600`.

```
$ tourillon config generate-context prod \
    --ca-cert ./ca.crt \
    --ca-key  ./ca.key \
    --kv   kv.prod.example.com:7000 \
    --peer peer.prod.example.com:7001 \
    --out  ~/.tourillon/contexts.toml
```

```
Options:
  --ca-cert PATH        CA certificate file (PEM)           [required]
  --ca-key PATH         CA private key file (PEM)           [required]
  --kv TEXT             KV endpoint address  (host:port)
  --peer TEXT           Peer endpoint address (host:port)
  --out PATH            Destination contexts.toml           [default: ~/.tourillon/contexts.toml]
  --days INT            Certificate validity in days        [default: 365]
  --set-current         Set this context as the current context
  --help                Show this message and exit.
```

At least one of `--kv` or `--peer` must be supplied; both are permitted.

**Happy path output (stdout):**
```
✓ Client certificate issued
✓ Context "prod" written to ~/.tourillon/contexts.toml
```

**Error – neither --kv nor --peer supplied (stderr, exit 1):**
```
✗ --kv or --peer (or both) must be supplied.
```

**Error – CA key/cert mismatch (stderr, exit 2):**
```
✗ CA private key does not match CA certificate public key.
```

---

### `tourctl config use-context` — switch the active context

```
$ tourctl config use-context NAME [OPTIONS]

  Set the active context in contexts.toml.
  Subsequent tourctl commands that do not pass --context will use this context.

Options:
  --contexts-file PATH  Path to contexts.toml  [default: ~/.tourillon/contexts.toml]
  --help                Show this message and exit.
```

**Happy path output (stdout):**
```
✓ Active context set to "my-cluster".
```

**Error — context name not found (stderr, exit 1):**
```
✗ Context "unknown" not found in /home/user/.tourillon/contexts.toml
```

**Error — contexts file does not exist (stderr, exit 1):**
```
✗ Contexts file not found: /home/user/.tourillon/contexts.toml
```

---

## Design

### Configuration precedence

Four levels, applied in descending priority:

| Priority | Source | Example |
|:---:|---|---|
| 1 | **CLI flag** | `--rf 5` |
| 2 | **Environment variable** | `TOURILLON_RF=5` |
| 3 | **Config file** (`config.toml` / `contexts.toml`) | `rf = 5` |
| 4 | **Built-in default** | `rf: int = 3` in the dataclass |

A value at a higher level always shadows the corresponding value at a lower
level. `load_config` implements this hierarchy; CLI commands may override any
field via their flag set.

### Process lock (`pid.lock`)

Before any socket is bound or any state file is read, the daemon acquires an
exclusive file lock on `<data_dir>/pid.lock`. The lock mechanism uses
`fcntl.LOCK_EX | LOCK_NB` on POSIX and `LockFileEx` with `LOCKFILE_FAIL_IMMEDIATELY`
on Windows (both non-blocking). If the lock cannot be acquired the process exits
immediately with code 1:

```
Error: another tourillon process is already running for data_dir "./node-data"
       (pid.lock held). Remove the stale lock or choose a different data_dir.
```

The lock is held for the **entire process lifetime** — released automatically by the OS
when the process terminates (clean shutdown or crash). No stale-lock cleanup logic is
required: the OS always releases file locks when a process dies.

**Lock file content** (written as JSON after acquiring the lock):
```json
{"pid": 12345, "started_at": "2026-05-31T14:23:45Z"}
```
This content is informational only. The lock validity is determined exclusively by
the OS file lock, not by the file content.

`ProcessLockPort` (in `core/ports/state.py`) is the abstraction used by all callers:

```python
class ProcessLockPort(Protocol):
    def acquire(self) -> None: ...   # raises ProcessLockError if already held
    def release(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, *_: object) -> None: ...
```

`ProcessLockError(Exception)` is raised on acquisition failure and caught by the
bootstrap entry point, which prints the error and exits with code 1.

`FileProcessLockAdapter(path: Path)` implements `ProcessLockPort` on top of
`fcntl.flock` / `msvcrt.locking`.

### Data model

#### `config.toml` wire format

A `config.toml` file is the complete, self-contained configuration for one node.
All TLS material is stored inline as standard base64-encoded PEM (no line breaks
inside the base64 string). All duration fields are unit-embedded strings; all size
fields are unit-embedded strings. No `*_file` path variants exist anywhere in the
schema. `config.toml` is written at mode **0600** because it embeds the node private key;
copying it to a world-readable path is a security misconfiguration.

```toml
schema_version = 1

[node]
id              = "node-1"
size            = "M"
data_dir        = "/var/lib/tourillon/node1"
rf              = 3
partition_shift = 10
seeds           = ["192.168.1.2:7001", "192.168.1.3:7001"]

[kv_server]
bind      = "0.0.0.0:7000"
advertise = "192.168.1.1:7000"

[peer_server]
bind      = "0.0.0.0:7001"
advertise = "192.168.1.1:7001"

[tls]
cert_data = "<base64-encoded PEM — node server certificate>"
key_data  = "<base64-encoded PEM — node private key>"
ca_data   = "<base64-encoded PEM — cluster CA certificate>"

[join]
max_retries     = -1      # -1 = unlimited within deadline
attempt_timeout = "10s"
deadline        = "2m"
backoff_base    = "2s"
backoff_max     = "30s"
max_concurrent  = 4

[drain]
max_retries        = -1
attempt_timeout    = "30s"
deadline           = "5m"
backoff_base       = "5s"
backoff_max        = "60s"
max_concurrent     = 4
bandwidth_fraction = 1.0

[rebalance]
max_concurrent_transfers = 4
max_chunk_bytes          = "1Mi"
```

**Duration unit suffixes** (case-sensitive): `ms` (milliseconds), `s` (seconds),
`m` (minutes), `h` (hours). Examples: `"500ms"`, `"10s"`, `"2m"`, `"1h"`.

**Size unit suffixes** (case-sensitive): `Ki` (kibibytes = 1024), `Mi` (mebibytes = 1024Â²),
`Gi` (gibibytes = 1024Â³). Examples: `"512Ki"`, `"1Mi"`, `"4Gi"`.

A bare integer with no suffix is a fatal config error. An unrecognised suffix is a fatal
config error. Both are reported as `ConfigError` at startup before any socket is opened.

#### `contexts.toml` wire format

Contexts are stored at `~/.tourillon/contexts.toml` by default (the path is overridable
via `--contexts-file`). The file is written atomically using `tempfile + os.replace` at
mode 0600.

```toml
current-context = "my-cluster"

[[contexts]]
name = "my-cluster"

  [contexts.cluster]
  name    = "my-cluster"
  ca_data = "<base64-encoded PEM — cluster CA certificate>"

  [contexts.endpoints]
  kv   = "192.168.1.1:7000"
  peer = "192.168.1.1:7001"

  [contexts.credentials]
  cert_data = "<base64-encoded PEM — client certificate>"
  key_data  = "<base64-encoded PEM — client private key>"
```

`current-context` is optional; when absent `tourctl` commands require `--context <name>`
explicitly. `endpoints.kv` and `endpoints.peer` are independently optional but at least
one must be present.

#### `Envelope` wire format

All Tourillon TCP traffic is framed as `Envelope` messages. The wire layout is
big-endian throughout:

```
offset   bytes   field
0        1       proto_version   uint8    (must equal 1)
1        2       schema_id       uint16   (1 = MessagePack; 0 = raw bytes)
3        16      correlation_id  bytes    (UUID v4, raw 16-byte form)
19       4       payload_len     uint32   (byte count of the payload field)
23       1       kind_len        uint8    (byte count of the kind field, 1–64)
24       N       kind            bytes    (UTF-8 string, N = kind_len)
24+N     M       payload         bytes    (M = payload_len)
```

`kind` is a dot-separated ASCII string (`kv.put`, `gossip.push`, `error.payload_too_large`).
Maximum UTF-8 byte length of `kind` is 64 bytes (`KIND_MAX_LEN`). Minimum is 1 byte.
`payload_len` must not exceed `MAX_PAYLOAD_DEFAULT` (4 MiB) on any connection.

`Envelope` is implemented in `tourillon/core/structure/envelope.py` as a frozen
dataclass. `encode()` serialises to bytes; `decode(data)` deserialises from bytes.
`create(payload, kind=..., correlation_id=None, schema_id=0)` is a convenience constructor.

#### `core/structure/config.py` — duration and size field types

**The following fields change from `float`/`int` to `str`** in this proposal.
The old defaults in parentheses are the
numeric equivalents for reference:

| Class | Field | Old type/default | New type/default |
|---|---|---|---|
| `JoinConfig` | `attempt_timeout` | `float = 10.0` | `str = "10s"` |
| `JoinConfig` | `deadline` | `float = 120.0` | `str = "2m"` |
| `JoinConfig` | `backoff_base` | `float = 2.0` | `str = "2s"` |
| `JoinConfig` | `backoff_max` | `float = 30.0` | `str = "30s"` |
| `DrainConfig` | `attempt_timeout` | `float = 30.0` | `str = "30s"` |
| `DrainConfig` | `deadline` | `float = 300.0` | `str = "5m"` |
| `DrainConfig` | `backoff_base` | `float = 5.0` | `str = "5s"` |
| `DrainConfig` | `backoff_max` | `float = 60.0` | `str = "60s"` |
| `RebalanceConfig` | `max_chunk_bytes` | `int = 1_048_576` | `str = "1Mi"` |

`max_retries`, `max_concurrent`, `bandwidth_fraction`, and `max_concurrent_transfers`
are **not** duration/size fields and retain numeric types.

`TourillonConfig`, `ServerConfig`, and `TlsConfig` are unchanged structurally.
`NodeSize` and its `token_count` property are unchanged.

#### `bootstrap/config.py` — helpers and loader

`parse_duration(s: str) -> float`
: Parse a unit-embedded duration string and return seconds as a float.
  Accepts `ms`, `s`, `m`, `h` suffixes. Raises `ConfigError` on an unrecognised
  suffix or non-numeric prefix. Examples: `"10s"` → `10.0`, `"500ms"` → `0.5`,
  `"2m"` → `120.0`, `"1h"` → `3600.0`.

`parse_bytes(s: str) -> int`
: Parse a unit-embedded size string and return bytes as an integer.
  Accepts `Ki`, `Mi`, `Gi` suffixes. Raises `ConfigError` on an unrecognised
  suffix or non-numeric prefix. Examples: `"1Mi"` → `1048576`, `"4Gi"` → `4294967296`.

`ConfigError(Exception)`
: Fatal configuration error. Raised by `parse_duration`, `parse_bytes`, and
  `load_config` for any invalid value. The bootstrap entry point catches it, prints
  the message to stderr, and exits with code 1.

`load_config(path: Path) -> TourillonConfig`
: Read *path* as TOML, validate all fields, and return an immutable `TourillonConfig`.
  Validates at startup:
  - All duration fields are passed through `parse_duration` (raises `ConfigError` on
    invalid suffix; the string is stored as-is in the returned dataclass).
  - All size fields are passed through `parse_bytes` (raises `ConfigError` on invalid
    suffix; the string is stored as-is).
  - `tls.cert_data` is validated with `validate_cert_not_expired` and
    `validate_cert_key_match` via `infra/tls/context.py`. A `TlsValidationError` is
    re-raised as `ConfigError`.
  - `node.size` must be a valid `NodeSize` value; invalid values raise `ConfigError`.
  - `[node]`, `[kv_server]`, `[peer_server]`, and `[tls]` sections are mandatory;
    `[join]`, `[drain]`, and `[rebalance]` are optional (defaults from the dataclasses
    are used when absent).

#### mTLS and dual-endpoint model

Each running node binds **two independent TCP listeners**, each with its own
`ssl.SSLContext` built from the same `TlsConfig` credentials:

| Listener | Port field | Lifecycle | Purpose |
|---|---|---|---|
| **KV server** | `kv_server` | `READY` and `DRAINING` phases only | Client KV traffic (`kv.*` envelope kinds) |
| **Peer server** | `peer_server` | Always up while the daemon runs | Inter-node control plane (`gossip.*`, `node.*`, `rebalance.*`) |

Both servers use `build_server_ssl_context(cert_data, key_data, ca_data)` from
`infra/tls/context.py`. The KV server and peer server receive separate `ssl.SSLContext`
instances (though constructed identically) so they can be independently started and
stopped.

Clients (`TcpClient`) connect using `build_client_ssl_context(cert_data, key_data,
ca_data)` where the credentials come from `ContextEntry.credentials` and the CA comes
from `ContextEntry.cluster.ca_data`. Hostname verification is **disabled** on both
server and client contexts (`check_hostname = False`); trust is CA-based. Every
certificate holder signed by the cluster CA is a legitimate peer.

`build_server_ssl_context` enforces `CERT_REQUIRED` with no plaintext fallback.
`build_client_ssl_context` also enforces `CERT_REQUIRED`.

#### `Dispatcher` — handler registration

`Dispatcher` is implemented in `tourillon/core/transport/dispatcher.py`. Two APIs exist:

```python
# Decorator style (canonical for all feature modules):
@dispatcher.on("kv.put")
async def handle_kv_put(receive: ReceiveEnvelope, send: SendEnvelope) -> None:
    envelope = await receive()
    ...

# Programmatic style (for tests and dynamic wiring):
dispatcher.register("kv.put", handle_kv_put)
```

`on(kind)` calls `register(kind, fn)` internally. `register` raises `ValueError` if
`kind` is already registered (duplicate registration is a programming error). `lookup(kind)`
returns the handler callable or `None` for unknown kinds. An unknown kind causes immediate
connection close on the server side (no error response sent).

#### `TcpServer` connection lifecycle

`TcpServer` (in `tourillon/core/transport/server.py`) wraps `asyncio.start_server`.
Each accepted connection runs a `_ConnectionSession` that:

1. Reads `Envelope` frames via `read_envelope` from `tourillon/core/transport/framing.py`.
2. Sends a protocol-error response (`error.proto_version_unsupported`, `error.kind_len_invalid`,
   or `error.payload_too_large`) and closes on framing violations.
3. Looks up the envelope kind in the `Dispatcher`; closes without response for unknown kinds.
4. Spawns a handler task per new `correlation_id`.
5. Routes subsequent envelopes with a known `correlation_id` to the running handler's receive queue.
6. Enforces `MAX_IN_FLIGHT_PER_CONN = 128` concurrent handler tasks; closes when exceeded.

`TcpServer.start(host, port)` binds and begins accepting. `TcpServer.stop()` stops
accepting and closes the server socket. The `name` parameter (`"kv"` or `"peer"`) appears
in log messages.

#### `TcpClient` multiplexing

`TcpClient` (in `tourillon/core/transport/client.py`) wraps one mTLS connection:

- `connect(addr, tls_ctx)` — open connection and start background read loop.
- `request(env, timeout)` — send `env`; return the single response matching `correlation_id`.
  Raises `ResponseTimeoutError` after `timeout` seconds; raises `ConnectionClosedError`
  if connection dies before response.
- `stream(env, timeout)` → `AsyncIterator[Envelope]` — send `env`; yield every response
  with the same `correlation_id`. The caller detects the terminal envelope by kind.
  `timeout` applies per-envelope.
- `send(env)` — fire-and-forget; does not register a response handler.
- `close()` — graceful teardown; all pending callers receive `ConnectionClosedError`.

#### `MsgpackSerializerAdapter`

`MsgpackSerializerAdapter` (in `tourillon/infra/serializer/msgpack.py`) implements
`SerializerPort` with `schema_id = 1`. Integers wider than 63 bits (e.g. 128-bit ring
tokens) are transparently encoded as `ExtType(code=1, data=16-byte-big-endian)` and
decoded back to Python `int`. The core layer never imports this class directly; it
receives an instance via constructor injection.

#### `SerializerPort`

`SerializerPort` (in `tourillon/core/ports/serializer.py`) is a `Protocol` with:
- `schema_id: int` — must match the `schema_id` written into `Envelope` headers.
- `encode(obj: Any) -> bytes` — encode to wire bytes.
- `decode(data: bytes) -> Any` — decode from wire bytes.

The core layer holds a `SerializerPort` reference. The infra layer injects
`MsgpackSerializerAdapter()` at startup.

#### `TourillonCore`

`TourillonCore` is the composition root object created by bootstrap once the node has
loaded configuration and built its concrete infrastructure adapters. It exists to keep
the wiring explicit and to avoid module globals, while still giving dispatcher factories
and handlers one stable object to capture.

At minimum, `TourillonCore` centralises the runtime dependencies that handlers need to
close over, such as:

- partition storage / hint storage / staging contexts
- `SerializerPort`
- `Dispatcher` factories or pre-built dispatchers
- TLS contexts and peer connection pools
- topology, partitioner, probe manager, node state, and clock objects

The exact shape of `TourillonCore` is intentionally bootstrap-owned: later proposals may
add fields, but all dispatcher-facing dependencies should flow through this facade rather
than through module globals or ad hoc parameter lists.

### Core invariants

1. **No plaintext connections.** Every TCP listener enforces `ssl.CERT_REQUIRED`.
   A connection without a valid cluster-CA-signed certificate is rejected at the TLS
   handshake. There is no fallback mode.

2. **No `*_file` paths in config.** `TlsConfig`, `ContextEntry`, and `CredentialsConfig`
   store inline base64-encoded PEM only. `load_config` rejects a TOML file that references
   file paths instead of inline data.

3. **Immutable `TourillonConfig`.** `TourillonConfig` and all nested dataclasses are
   `frozen=True`. Once constructed by `load_config`, no subsystem mutates configuration
   at runtime.

4. **Duration/size strings validated at startup.** `load_config` calls `parse_duration`
   and `parse_bytes` on every duration and size field before returning `TourillonConfig`.
   An unrecognised suffix aborts startup with `ConfigError` before any socket is opened.

5. **Atomic `contexts.toml` writes.** `save_contexts` always uses `tempfile.mkstemp` +
   `os.replace` and sets mode 0600. A crash during write cannot corrupt an existing file.

6. **`config.toml` written at mode 0600.** `tourillon config generate` writes the output
   file at mode 0600 because it embeds the node private key. Any tool that writes
   `config.toml` must enforce this permission.

7. **`partition_shift` is immutable after cluster bootstrap.** The `config generate`
   command sets it once; it cannot be changed without a full node decommission.

8. **Handler registration is startup-time only.** `Dispatcher.register` and `Dispatcher.on`
   are called during bootstrap, never at request time. At runtime only `lookup` is called.

9. **Correlation-id routing is per-connection.** A `correlation_id` is unique within a
   single `TcpClient` connection lifetime. The same UUID may be reused on a different
   connection without conflict.

10. **`pid.lock` is acquired before any socket binding or state-file access.** The process
    lock guarantees exclusive access to `data_dir`. No two `tourillon` daemon processes can
    share a `data_dir`; the second process to attempt acquisition exits immediately with
    code 1.

11. **`data_dir` must pre-exist.** The daemon never creates `data_dir`. If `data_dir` is
    absent at startup the process exits with code 1 before acquiring any lock or binding
    any socket. The operator must create it.

### Sequence / flow — node startup

```
1. bootstrap.main reads path to config.toml (env var or CLI flag).
2. load_config(path) → TourillonConfig
   a. Parse TOML.
   b. parse_duration / parse_bytes on all relevant fields → ConfigError on bad suffix.
   c. validate_cert_not_expired(tls.cert_data) → ConfigError on expiry.
   d. validate_cert_key_match(tls.cert_data, tls.key_data) → ConfigError on mismatch.
   e. Return frozen TourillonConfig.
3. Verify data_dir exists (os.path.isdir); if absent → stderr
   "Error: data_dir does not exist: <path>"; exit 1.
   The daemon never creates data_dir; the operator must create it beforehand.
4. Acquire ProcessLock on <data_dir>/pid.lock (FileProcessLockAdapter).
   → If already held → stderr "Error: another tourillon process is already running
     for data_dir <path> (pid.lock held)…"; exit 1.
5. build_server_ssl_context(cert, key, ca) → ssl_ctx_kv  (for kv_server)
6. build_server_ssl_context(cert, key, ca) → ssl_ctx_peer (for peer_server)
7. Build `TourillonCore` from the validated config plus all concrete adapters needed at
   runtime (storage, serializer, TLS contexts, pool, topology manager, probe manager,
   partitioner, clock, node state).
8. Construct `Dispatcher` instances from `TourillonCore` (for example KV and peer-plane
   dispatchers).
9. Register handlers at startup time by closing over the `TourillonCore` instance or by
   instantiating handler objects with explicit constructor injection.
10. Start TcpServer("peer", ssl_ctx_peer) on peer_server.bind → peer listener up.
11. (KV server is bound once the node reaches READY phase — deferred to a later proposal.)
12. Log: "Node <id> listening on peer <advertise>".
```

### Sequence / flow — `tourillon pki ca`

```
1. Parse CLI options (out_dir, name, days, key_size).
2. Compute out_cert = out_dir / "ca.pem", out_key = out_dir / "ca-key.pem".
3. CryptographyCaAdapter().generate_ca(CaRequest(common_name, valid_days, key_size, out_cert, out_key))
   a. Generate RSA key pair.
   b. Build self-signed x509 cert with BasicConstraints(ca=True).
   c. Write ca-key.pem at mode 0600; write ca.pem.
4. Print success lines to stdout.
```

### Sequence / flow — `tourillon config generate`

```
1. Parse CLI options.
2. Read ca_cert, cert, key files as bytes; base64-encode each.
3. validate_cert_not_expired(b64_cert) — error on expiry.
4. validate_cert_key_match(b64_cert, b64_key) — error on mismatch.
5. Auto-generate node_id if not supplied.
6. Build TOML dict representing config.toml.
7. Write to --out path.
8. Print success line to stdout.
```

### Sequence / flow — `tourillon config generate-context`

```
1. Parse CLI options; require at least one of --kv / --peer.
2. Read ca_cert, client_cert, client_key files as bytes; base64-encode each.
3. validate_cert_key_match(b64_client_cert, b64_client_key) — error on mismatch.
4. load_contexts(contexts_file) → ContextsFile (empty if absent).
5. Build ContextEntry(name, ClusterRef(name, ca_data), EndpointsConfig(kv, peer),
                      CredentialsConfig(cert_data, key_data)).
6. file.upsert(entry).
7. If --set-current: file.current_context = name.
8. save_contexts(contexts_file, file).
9. Print success line to stdout.
```

### Sequence / flow — `tourctl config use-context`

```
1. Parse CLI options; resolve contexts_file path.
2. load_contexts(contexts_file) → ContextsFile; raise FileNotFoundError if absent.
3. If name not in {e.name for e in file.contexts}: error "context '<name>' not found"; exit 1.
4. file.current_context = name.
5. save_contexts(contexts_file, file).
6. Print "Active context set to '<name>'." to stdout.
```

### Error paths

| Scenario | Behaviour |
|---|---|
| `tourillon pki ca` — output directory absent | `PkiError` → stderr "Error: cannot write to …: no such file or directory"; exit 1 |
| `tourillon pki ca` — crypto library error | `PkiError` → stderr "Error: CA generation failed: …"; exit 2 |
| `tourillon pki issue` — CA cert expired | `PkiError` → stderr "Error: CA certificate expired on …"; exit 2 |
| `tourillon pki issue` — CA key/cert mismatch | `PkiError` → stderr "Error: CA private key does not match …"; exit 2 |
| `tourillon pki issue` — CA file not found | `PkiError` → stderr "Error: cannot read CA material: …"; exit 1 |
| `tourillon config generate` — cert/key mismatch | `TlsValidationError` → stderr "Error: …"; exit 2 |
| `tourillon config generate` — cert expired | `TlsValidationError` → stderr "Error: …"; exit 2 |
| `load_config` — unknown duration suffix | `ConfigError` → stderr "Error: …"; daemon exits |
| `load_config` — cert expired at daemon startup | `ConfigError` → stderr "Error: …"; daemon exits |
| `tourillon config generate-context` — neither --kv nor --peer | stderr "Error: at least one of --kv or --peer must be supplied."; exit 1 |
| `tourctl config use-context` — context name absent from file | stderr "Error: context \"<name>\" not found in <path>"; exit 1 |
| `tourctl config use-context` — contexts file does not exist | stderr "Error: contexts file not found: <path>"; exit 1 |
| Daemon startup — `data_dir` does not exist | stderr "Error: data_dir does not exist: <path>"; exit 1 (before lock acquisition) |
| Daemon startup — `pid.lock` already held | `ProcessLockError` → stderr "Error: another tourillon process is already running for data_dir <path> (pid.lock held)…"; exit 1 |
| Envelope with proto_version ≠ 1 | Server sends `error.proto_version_unsupported` then closes |
| Envelope with kind_len out of range | Server sends `error.kind_len_invalid` then closes |
| Envelope with payload_len > 4 MiB | Server sends `error.payload_too_large` then closes |
| Envelope kind not registered | Server closes connection silently (no response) |
| `TcpClient.request` — no response within timeout | Raises `ResponseTimeoutError`; connection stays open |
| `TcpClient` — connection dies | All pending `request`/`stream` calls raise `ConnectionClosedError` |

---

## Design decisions

### Decision: Unit-embedded strings for all duration and size fields

**Alternatives considered:**
- (A) Bare `float` seconds / bare `int` bytes, with unit in the key name
  (`attempt_timeout_s`, `max_chunk_bytes`).
- (B) Separate `value` + `unit` sub-tables per field.
- (C) Unit-embedded strings (`"10s"`, `"1Mi"`).

**Chosen because:** (C) is the most readable in TOML, matches conventions established
by Docker, Kubernetes, and Prometheus. A human reading `attempt_timeout = "10s"` cannot
misinterpret the unit. (A) leaks units into key names and makes future unit changes a
breaking rename. (B) is verbose. `parse_duration` and `parse_bytes` are small helpers
that cover the full vocabulary once. The validation step in `load_config` means a bad
suffix never silently becomes a zero or a wrong-scale value.

### Decision: No `*_file` path variants in config or contexts

**Alternatives considered:**
- Allow `cert_file = "/etc/tourillon/node1.pem"` as an alternative to `cert_data`.

**Chosen because:** Inline base64 makes the file fully self-contained. A single copy
of `config.toml` or `contexts.toml` contains everything needed to run the node or
connect as an operator. There is no risk of broken paths when moving files between
machines, and no partial state when a referenced file is deleted.

### Decision: Separate `ssl.SSLContext` per listener

**Alternatives considered:**
- Share a single `SSLContext` between the KV and peer listeners.

**Chosen because:** The KV and peer listeners have independent lifecycles — the peer
server is always up; the KV server is up only when `phase ∈ {READY, DRAINING}`. Sharing
a context would create a coupling between their lifetimes. Separate instances allow the
KV server to be started and stopped independently without affecting the peer server's TLS
state.

### Decision: CA-based identity, hostname verification disabled

**Alternatives considered:**
- Enable `check_hostname = True` and require nodes to use DNS-resolvable hostnames.

**Chosen because:** Tourillon clusters are addressed by IP + port, not DNS names. The
SAN field of node certificates typically contains IP addresses (via `--san-ip`), not DNS
names. Using the CA as the single root of trust (the model used by etcd, CockroachDB,
and Cassandra) is simpler to operate and does not require a DNS infrastructure.

### Decision: `Dispatcher.on()` decorator as canonical registration API

**Alternatives considered:**
- Keep only `register(kind, handler)`.
- Require per-kind handler classes.

**Chosen because:** The decorator style is concise, keeps the handler definition
co-located with its registration, and avoids boilerplate class bodies. `register` is
retained as the lower-level API because it is essential for test wiring (where the
kind string is computed at runtime). No per-handler class is required anywhere in the
system.

### Decision: `TourillonCore` as the bootstrap composition root

**Alternatives considered:**
- Store dependencies in module globals and read them from handlers via `get_core()`.
- Pass every individual dependency separately to each handler factory.
- Use ad hoc containers per dispatcher.

**Chosen because:** A single immutable `TourillonCore` keeps startup wiring explicit while
avoiding global state and repetitive long parameter lists. Dispatcher factories and
handlers can capture one stable object that centralises storage, serializer, topology,
clocks, probe manager, TLS contexts, connection pools, and any other adapters required by
the transport layer. This keeps the composition root at bootstrap time and makes tests
straightforward: a unit test can build a minimal `TourillonCore` fixture and pass it into
the dispatcher factory.

### Decision: `parse_duration` and `parse_bytes` live in `bootstrap/config.py`

**Alternatives considered:**
- Place helpers in `core/structure/config.py` or a shared `core/utils.py`.

**Chosen because:** These helpers are infrastructure concerns — they parse external
configuration that only the bootstrap layer reads. Placing them in `bootstrap/config.py`
keeps the pure `core/structure/config.py` free of parsing logic and matches the layering
rule that `core/` must never import `infra/` or bootstrap concerns.

---

## Interfaces (informative)

### `tourillon/core/structure/config.py` — modified duration/size fields

```python
@dataclass(frozen=True)
class JoinConfig:
    max_retries: int = -1
    attempt_timeout: str = "10s"   # was float = 10.0
    deadline: str = "2m"           # was float = 120.0
    backoff_base: str = "2s"       # was float = 2.0
    backoff_max: str = "30s"       # was float = 30.0
    max_concurrent: int = 4

@dataclass(frozen=True)
class DrainConfig:
    max_retries: int = -1
    attempt_timeout: str = "30s"   # was float = 30.0
    deadline: str = "5m"           # was float = 300.0
    backoff_base: str = "5s"       # was float = 5.0
    backoff_max: str = "60s"       # was float = 60.0
    max_concurrent: int = 4
    bandwidth_fraction: float = 1.0

@dataclass(frozen=True)
class RebalanceConfig:
    max_concurrent_transfers: int = 4
    max_chunk_bytes: str = "1Mi"   # was int = 1_048_576
```

### `tourillon/bootstrap/config.py`

```python
class ConfigError(Exception):
    """Fatal configuration error raised by parse_duration, parse_bytes, load_config."""

def parse_duration(s: str) -> float:
    """Parse a unit-embedded duration string; return seconds as float.

    Accepted suffixes: ms, s, m, h.
    Raise ConfigError on unrecognised suffix or non-numeric prefix.

    Examples:
        parse_duration("500ms") -> 0.5
        parse_duration("10s")   -> 10.0
        parse_duration("2m")    -> 120.0
        parse_duration("1h")    -> 3600.0
    """

def parse_bytes(s: str) -> int:
    """Parse a unit-embedded size string; return bytes as int.

    Accepted suffixes: Ki, Mi, Gi.
    Raise ConfigError on unrecognised suffix or non-numeric prefix.

    Examples:
        parse_bytes("512Ki") -> 524288
        parse_bytes("1Mi")   -> 1048576
        parse_bytes("4Gi")   -> 4294967296
    """

def load_config(path: Path) -> TourillonConfig:
    """Load, validate, and return an immutable TourillonConfig from *path*.

    Validation steps (all fatal — raise ConfigError):
      1. TOML parse error.
      2. Missing mandatory sections ([node], [kv_server], [peer_server], [tls]).
      3. Invalid NodeSize value.
      4. Invalid duration/size strings (parse_duration / parse_bytes called on each).
      5. TLS cert expired (validate_cert_not_expired).
      6. TLS cert/key mismatch (validate_cert_key_match).
    """
```

### `tourillon/core/structure/envelope.py` (informative)

```python
PROTO_VERSION: int = 1
KIND_MAX_LEN: int = 64

@dataclass(frozen=True)
class Envelope:
    kind: str
    payload: bytes
    correlation_id: uuid.UUID = field(default_factory=uuid.uuid4)
    schema_id: int = 1
    proto_version: int = PROTO_VERSION

    def encode(self) -> bytes: ...
    @classmethod
    def decode(cls, data: bytes) -> Self: ...
    @classmethod
    def create(
        cls,
        payload: bytes,
        *,
        kind: str,
        correlation_id: uuid.UUID | None = None,
        schema_id: int = 0,
    ) -> Self: ...
```

### `tourillon/core/transport/dispatcher.py` (informative)

```python
class Dispatcher:
    def on(self, kind: str) -> Callable[[_H], _H]:
        """Decorator — register the decorated async function as the handler for *kind*."""

    def register(self, kind: str, handler: ConnectionHandler) -> None:
        """Register *handler* for *kind*. Raise ValueError if already registered."""

    def lookup(self, kind: str) -> ConnectionHandler | None:
        """Return the handler for *kind*, or None if unknown."""
```

### `tourillon/core/ports/transport.py` (informative)

```python
MAX_PAYLOAD_DEFAULT: int = 4 * 1024 * 1024   # 4 MiB
MAX_IN_FLIGHT_PER_CONN: int = 128
RESPONSE_TIMEOUT: float = 30.0
READ_TIMEOUT: float = 30.0

type ReceiveEnvelope = Callable[[], Awaitable[Envelope]]
type SendEnvelope = Callable[[Envelope], Awaitable[None]]

class ConnectionHandler(Protocol):
    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None: ...
```

### `tourillon/core/ports/serializer.py` (informative)

```python
class SerializerPort(Protocol):
    schema_id: int

    def encode(self, obj: Any) -> bytes: ...
    def decode(self, data: bytes) -> Any: ...
```

### `tourillon/infra/serializer/msgpack.py` (informative)

```python
class MsgpackSerializerAdapter:
    schema_id: int = 1

    def encode(self, obj: Any) -> bytes: ...   # msgpack.packb; uint128 as ExtType(1)
    def decode(self, data: bytes) -> Any: ...  # msgpack.unpackb; ExtType(1) → int
```

### `tourillon/infra/tls/context.py` (informative)

```python
class TlsValidationError(Exception): ...

def validate_cert_not_expired(b64_cert_pem: str) -> None: ...
def validate_cert_key_match(b64_cert_pem: str, b64_key_pem: str) -> None: ...
def build_server_ssl_context(
    b64_cert_pem: str, b64_key_pem: str, b64_ca_pem: str
) -> ssl.SSLContext: ...
def build_client_ssl_context(
    b64_cert_pem: str, b64_key_pem: str, b64_ca_pem: str
) -> ssl.SSLContext: ...
```

### `tourillon/core/ports/pki.py` (informative)

```python
class PkiError(Exception): ...

@dataclass(frozen=True)
class CaRequest:
    common_name: str
    valid_days: int
    key_size: int
    out_cert: Path
    out_key: Path

@dataclass(frozen=True)
class CertRequest:
    common_name: str
    san_dns: tuple[str, ...]
    san_ip: tuple[str, ...]
    valid_days: int
    ca_cert: Path
    ca_key: Path
    out_cert: Path
    out_key: Path
    key_size: int = 2048

class CertificateAuthorityPort(Protocol):
    def generate_ca(self, request: CaRequest) -> None: ...

class CertificateIssuerPort(Protocol):
    def issue_cert(self, request: CertRequest) -> None: ...
```

### `tourillon/core/ports/state.py` (new — process lock port)

```python
class ProcessLockError(Exception):
    """Raised when the process lock cannot be acquired (already held)."""

class ProcessLockPort(Protocol):
    def acquire(self) -> None:
        """Acquire the exclusive lock. Raises ProcessLockError if already held."""
    def release(self) -> None:
        """Release the lock."""
    def __enter__(self) -> Self: ...
    def __exit__(self, *_: object) -> None: ...
```

### `tourillon/infra/process_lock.py` (new — OS file lock adapter)

```python
class FileProcessLockAdapter:
    """Implements ProcessLockPort using fcntl.LOCK_EX|LOCK_NB (POSIX)
    or msvcrt.locking (Windows). Writes JSON metadata after acquisition."""

    def __init__(self, path: Path) -> None: ...
    def acquire(self) -> None: ...   # raises ProcessLockError if already held
    def release(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, *_: object) -> None: ...
```

### `tourillon/core/structure/contexts.py` (informative)

```python
@dataclass(frozen=True)
class ClusterRef:
    name: str
    ca_data: str  # base64-encoded PEM CA certificate

@dataclass(frozen=True)
class EndpointsConfig:
    kv: str | None = None
    peer: str | None = None

@dataclass(frozen=True)
class CredentialsConfig:
    cert_data: str  # base64-encoded PEM client certificate
    key_data: str   # base64-encoded PEM client private key

@dataclass(frozen=True)
class ContextEntry:
    name: str
    cluster: ClusterRef
    endpoints: EndpointsConfig
    credentials: CredentialsConfig

@dataclass
class ContextsFile:
    current_context: str | None = None
    contexts: list[ContextEntry] = field(default_factory=list)

    def get(self, name: str) -> ContextEntry | None: ...
    def upsert(self, entry: ContextEntry) -> None: ...
```

---

## Proposed code organisation

Files **created** by this proposal (in mandatory creation order):

```
tourillon/core/structure/config.py          MODIFIED — duration/size fields → str
tourillon/core/ports/state.py               NEW — ProcessLockPort, ProcessLockError
tourillon/bootstrap/__init__.py             NEW — package marker
tourillon/bootstrap/config.py               NEW — ConfigError, parse_duration,
                                                  parse_bytes, load_config
tourillon/infra/cli/__init__.py             NEW — package marker
tourillon/infra/cli/pki.py                  NEW — tourillon pki ca/issue commands
tourillon/infra/cli/config.py               NEW — tourillon config generate command
tourillon/infra/process_lock.py             NEW — FileProcessLockAdapter
tourctl/__init__.py                         NEW — package marker
tourctl/infra/__init__.py                   NEW — package marker
tourctl/infra/cli/__init__.py               NEW — package marker
tourctl/infra/cli/config.py                 NEW — tourillon config generate-context,
                                                  tourctl config use-context
tests/__init__.py                           NEW — package marker
tests/unit/__init__.py                      NEW — package marker
tests/unit/test_envelope.py                 NEW — scenarios 1–3
tests/unit/test_dispatcher.py               NEW — scenarios 4–6
tests/unit/test_serializer.py               NEW — scenarios 7–8
tests/unit/test_bootstrap_config.py         NEW — scenarios 9–18
tests/unit/test_contexts.py                 NEW — scenarios 19–21
tests/unit/test_process_lock.py             NEW — scenarios 26–28
tests/e2e/__init__.py                       NEW — package marker
tests/e2e/test_pki.py                       NEW — scenarios 22–23
tests/e2e/test_config_generate.py           NEW — scenarios 24–25
```

Files already present and unchanged:

```
tourillon/core/structure/envelope.py        (complete)
tourillon/core/structure/contexts.py        (complete)
tourillon/core/transport/dispatcher.py      (complete)
tourillon/core/transport/client.py          (complete)
tourillon/core/transport/server.py          (complete)
tourillon/core/transport/framing.py         (complete)
tourillon/core/transport/pool.py            (complete)
tourillon/core/ports/serializer.py          (complete)
tourillon/core/ports/pki.py                 (complete)
tourillon/core/ports/transport.py           (complete)
tourillon/infra/pki/x509.py                 (complete)
tourillon/infra/tls/context.py              (complete)
tourillon/infra/serializer/msgpack.py       (complete)
tourillon/infra/contexts.py                 (complete)
```

---

## Test scenarios

All scenarios run with in-memory adapters and the real filesystem (for e2e).
Unit tests use no real sockets or real disk I/O. E2e tests use `tmp_path` (pytest fixture).

| # | Mark | Fixture | Action | Expected |
|---|------|---------|--------|----------|
| 1 | unit | `Envelope("kv.put", b"hello", schema_id=1)` | `.encode()` then `Envelope.decode(result)` | Round-trip produces identical `kind`, `payload`, `correlation_id`, `schema_id`, `proto_version` |
| 2 | unit | — | `Envelope.decode(b"\x01\x00\x01")` (3 bytes, below minimum header) | Raises `ValueError` with "frame too short" |
| 3 | unit | — | `Envelope(kind="x" * 65, payload=b"")` | Raises `ValueError` at construction with "kind must be 1–64" |
| 4 | unit | `Dispatcher()` | `@dispatcher.on("kv.put") async def h(r, s): ...`; then `dispatcher.lookup("kv.put")` | Returns `h` |
| 5 | unit | `Dispatcher()` with `"kv.put"` registered | `dispatcher.register("kv.put", h2)` | Raises `ValueError` with "already registered" |
| 6 | unit | `Dispatcher()` | `dispatcher.lookup("nonexistent")` | Returns `None` |
| 7 | unit | `MsgpackSerializerAdapter()` | `encode({"k": "v", "n": 42})` then `decode(result)` | Round-trip produces `{"k": "v", "n": 42}`; `schema_id == 1` |
| 8 | unit | `MsgpackSerializerAdapter()` | `encode(2**128 - 1)` then `decode(result)` | Round-trip produces `2**128 - 1` (128-bit integer preserved) |
| 9 | unit | — | `parse_duration("10s")` | Returns `10.0` |
| 10 | unit | — | `parse_duration("500ms")` | Returns `0.5` |
| 11 | unit | — | `parse_duration("2m")` | Returns `120.0` |
| 12 | unit | — | `parse_duration("1h")` | Returns `3600.0` |
| 13 | unit | — | `parse_bytes("1Mi")` | Returns `1048576` |
| 14 | unit | — | `parse_bytes("4Gi")` | Returns `4294967296` |
| 15 | unit | — | `parse_duration("5x")` | Raises `ConfigError` with unrecognised suffix message |
| 16 | unit | — | `parse_bytes("100MB")` | Raises `ConfigError` with unrecognised suffix message |
| 17 | unit | Valid TOML on disk with duration strings | `load_config(path)` | Returns `TourillonConfig`; `join.attempt_timeout == "10s"`; cert/key/CA fields non-empty |
| 18 | unit | TOML with `attempt_timeout = "10x"` in `[join]` | `load_config(path)` | Raises `ConfigError` mentioning the unrecognised suffix |
| 19 | unit | Non-existent path | `load_contexts(absent_path)` | Returns `ContextsFile(current_context=None, contexts=[])` |
| 20 | unit | `ContextsFile` with one entry | `save_contexts(tmp_path, cf)` then `load_contexts(tmp_path)` | Round-trip: entry name, endpoints, ca_data, cert_data, key_data all equal |
| 21 | unit | `ContextsFile` with entry named `"a"` | `cf.upsert(ContextEntry(name="a", ...))` | `len(cf.contexts) == 1` and entry is replaced |
| 22 | e2e | `tmp_path` (real filesystem) | `tourillon pki ca --out-dir tmp_path` | `ca.pem` and `ca-key.pem` exist; `ca-key.pem` has mode 0600 |
| 23 | e2e | `tmp_path` + CA from scenario 22 | `tourillon pki issue --ca-cert ca.pem --ca-key ca-key.pem --name node1 --san-ip 127.0.0.1 --out-dir tmp_path` | `node1.pem` and `node1-key.pem` exist; `node1.pem` verifiable against `ca.pem` |
| 24 | e2e | `tmp_path` + CA and node cert from scenarios 22–23 | `tourillon config generate --ca-cert ca.pem --cert node1.pem --key node1-key.pem --out tmp_path/config.toml` | `config.toml` written at mode 0600; `load_config(config.toml)` returns `TourillonConfig` without error; `join.attempt_timeout == "10s"` |
| 25 | e2e | `tmp_path` + CA and client cert/key | `tourillon config generate-context --name my-cluster --ca-cert ca.pem --client-cert client.pem --client-key client-key.pem --peer 127.0.0.1:7001 --contexts-file tmp_path/contexts.toml` | `contexts.toml` written; `load_contexts` returns `ContextsFile` with one entry named `"my-cluster"` |
| 26 | unit | `ContextsFile` with two entries `"a"` and `"b"` saved to `tmp_path/contexts.toml` | `tourctl config use-context b --contexts-file tmp_path/contexts.toml` | `contexts.toml` reloaded; `current_context == "b"` |
| 27 | unit | `ContextsFile` present but without entry `"z"` | `tourctl config use-context z --contexts-file <path>` | Prints error "context \"z\" not found in …"; exit 1 |
| 28 | unit | `FileProcessLockAdapter` on a `tmp_path` directory | `acquire()` twice from different instances targeting the same path | First succeeds; second raises `ProcessLockError` |

---

## Exit criteria

- [ ] All 28 test scenarios pass (`uv run pytest -m "unit or e2e" -x`).
- [ ] `uv run pytest --cov=tourillon --cov=tourctl --cov-fail-under=90` passes.
- [ ] `uv run ruff check tourillon/ tourctl/ tests/` passes with zero violations.
- [ ] `uv run black --check tourillon/ tourctl/ tests/` passes.
- [ ] `tourillon/core/structure/config.py` — all duration/size fields are `str` with unit-embedded defaults.
- [ ] `tourillon/bootstrap/config.py` — `parse_duration`, `parse_bytes`, `ConfigError`, and `load_config` are present and exported.
- [ ] `load_config` rejects a TOML with an unrecognised duration/size suffix with `ConfigError`.
- [ ] `load_config` rejects a TOML with an expired node certificate with `ConfigError`.
- [ ] `tourillon pki ca` writes `ca.pem` (mode default) and `ca-key.pem` (mode 0600).
- [ ] `tourillon pki issue` issues a leaf cert verifiable against the CA cert.
- [ ] `tourillon config generate` writes a self-contained `config.toml` at mode 0600 with base64 inline PEM; no path fields.
- [ ] `tourillon config generate-context` writes an atomic `contexts.toml` at mode 0600.
- [ ] `tourctl config use-context <name>` updates `current-context` field in `contexts.toml` atomically.
- [ ] `tourctl config use-context` with an unknown name exits 1 with a descriptive error.
- [ ] `FileProcessLockAdapter.acquire()` raises `ProcessLockError` when the lock file is already held by another process.
- [ ] Daemon startup aborts with exit 1 and a clear message if `data_dir` does not exist (before lock acquisition).
- [ ] Daemon startup aborts with exit 1 and a clear message if `pid.lock` is already held.
- [ ] No module under `tourillon/core/` imports `msgpack`, `ssl`, `cryptography`, or any `tourillon/infra/` module.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

- Node daemon `main()` entry point and process lifecycle (covered by a later proposal).
- `tourillon node start` command and `IDLE → READY` FSM transition (covered by a later proposal).
- `state.toml` format (covered by a later proposal).
- Gossip protocol and seeded join (`IDLE → JOINING`) (covered by a later proposal).
- `tourctl node join` command (covered by a later proposal).
- `tourctl node inspect` command and `node.inspect` envelope kind (covered by a later proposal).
- KV operations (`kv.put`, `kv.get`, `kv.delete`) (covered by a later proposal).
- `[kv]` config section (covered by a later proposal).
- `[gc]` config section (covered by a later proposal).
- Certificate rotation — the TLS credentials in `config.toml` are static for the node lifetime in this proposal. Rotation requires a node restart.
- Multiple cluster CA support — one CA per `ContextEntry`.
- `TcpClient` connection pooling — `core/transport/pool.py` exists but its wiring is deferred to the proposals that require pooled connections.
- PKCS#8 key format — only TraditionalOpenSSL PEM format is produced by `CryptographyCaAdapter` and `CryptographyCertIssuerAdapter`.
