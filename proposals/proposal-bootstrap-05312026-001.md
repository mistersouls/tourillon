# Proposal: Bootstrap & Provisioning

**Author**: Souleymane BA <soulsmister@gmail.com>
**Status:** Implemented
**Date:** 2026-05-31
**Sequence:** 001

---

## Summary

This proposal defines the bootstrap and provisioning baseline for Tourillon:

- PKI certificate generation (`tourillon pki ca`, `tourillon pki issue`)
- Node config generation (`tourillon config generate`)
- Operator context generation (`tourillon config generate-context`)
- Operator active-context switching (`tourctl config use-context`)
- Node startup wiring through `TourillonCore`, `NodeManager`, and `NodeStarter`
- Envelope framing, dispatch, and server-side transport (`Envelope`, `Dispatcher`, `TcpServer`)
- mTLS validation/context construction through the TLS port and cryptography adapter

This file has been aligned to the current repository structure and naming. Items previously documented here but not currently present (for example: process lock port, `TcpClient`, serializer adapters, duration/bytes parsing helpers) are treated as **deferred** and remain out of scope for this revision.

---

## Motivation

Before higher-level behavior (ring, gossip, KV, rebalance), Tourillon needs a stable bootstrap substrate:

1. **Identity** — CA + leaf cert issuance for nodes and operators.
2. **Configuration** — self-contained TOML documents for node and context setup.
3. **Transport** — a fixed wire envelope and startup-registered dispatch handlers.

This proposal preserves that intent and reflects the implementation as it exists in this repository now.

---

## CLI contract

All commands print success to stdout and failures to stderr.
Current error mapping for PKI flows is implemented in
`tourillon/bootstrap/cli/utils.py::handle_pki_error`:

- exit `1`: message contains `cannot read` or `cannot write`
- exit `2`: all other `PkiError` cases

### `tourillon pki ca`

Implemented in: `tourillon/bootstrap/cli/pki.py`

```bash
tourillon pki ca \
  --out-cert ./ca.crt \
  --out-key ./ca.key \
  [--name tourillon-ca] \
  [--days 3650] \
  [--key-size 2048]
```

**Current options (code-accurate):**

- `--out-cert PATH` (required)
- `--out-key PATH` (required)
- `--name TEXT` (default: `tourillon-ca`)
- `--days INT` (default: `3650`)
- `--key-size INT` (default: `2048`)

Private key output is chmod `0600` by the X509 adapter.

### `tourillon pki issue`

Implemented in: `tourillon/bootstrap/cli/pki.py`

```bash
tourillon pki issue \
  --ca-cert ./ca.crt \
  --ca-key ./ca.key \
  --name node-1 \
  --out-cert ./node-1.crt \
  --out-key ./node-1.key \
  [--san-dns ...] \
  [--san-ip ...] \
  [--days 365] \
  [--key-size 2048]
```

**Current options (code-accurate):**

- `--ca-cert PATH` (required)
- `--ca-key PATH` (required)
- `--name TEXT` (required)
- `--san-dns TEXT` (repeatable, optional)
- `--san-ip TEXT` (repeatable, optional)
- `--out-cert PATH` (required)
- `--out-key PATH` (required)
- `--days INT` (default: `365`)
- `--key-size INT` (default: `2048`)

### `tourillon config generate`

Implemented in: `tourillon/bootstrap/cli/config.py`

```bash
tourillon config generate \
  --ca-cert ./ca.crt \
  --ca-key ./ca.key \
  --node-id node-1 \
  --data-dir ./data/node-1 \
  --out ./config.toml
```

**Current options (code-accurate):**

- `--ca-cert PATH` (required)
- `--ca-key PATH` (required)
- `--node-id TEXT` (required)
- `--size [XS|S|M|L|XL|XXL]` (default: `M`)
- `--data-dir PATH` (required)
- `--kv-bind TEXT` (default: `127.0.0.1:7000`)
- `--peer-bind TEXT` (default: `127.0.0.1:7001`)
- `--peer-advertise TEXT` (optional; defaults internally to `peer_bind`)
- `--seeds TEXT` (repeatable, optional)
- `--rf INT` (default: `3`)
- `--partition-shift INT` (default: `17`)
- `--days INT` (default: `365`)
- `--out PATH` (required)

Implementation detail:

- Command creates temporary cert/key files, issues a node cert, writes a self-contained
  config, then deletes the temporary files.

### `tourillon config generate-context`

Implemented in: `tourillon/bootstrap/cli/config.py`

```bash
tourillon config generate-context <subject> \
  --name prod \
  --ca-cert ./ca.crt \
  --ca-key ./ca.key \
  --kv kv.prod.example.com:7000 \
  --peer peer.prod.example.com:7001 \
  --out ~/.tourillon/contexts.toml
```

**Current parameters/options (code-accurate):**

- positional argument: `sub` (certificate common name / subject)
- `--name TEXT` (required; context name)
- `--ca-cert PATH` (required)
- `--ca-key PATH` (required)
- `--kv TEXT` (required)
- `--peer TEXT` (required)
- `--out PATH` (required)
- `--days INT` (default: `365`)

Note: this currently requires **both** `--kv` and `--peer`.

### `tourctl config use-context`

Implemented in: `tourctl/bootstrap/cli/config.py`

```bash
tourctl config use-context NAME \
  [--contexts-file ~/.tourillon/contexts.toml]
```

**Current options (code-accurate):**

- positional argument: `name`
- `--contexts-file PATH` (default: `~/.tourillon/contexts.toml`)

Behavior:

- exits `1` if file is missing
- exits `1` if context name does not exist
- updates `current-context` and saves file

---

## Design

### Configuration precedence

The repository currently uses command flags + file content directly.
Environment-variable precedence is not yet implemented centrally.

### Data model

#### `config.toml` format (current)

`TourillonConfig` and `ConfigRequest` live in `tourillon/core/structure/config.py`.
`NodeSize` lives in `tourillon/core/machinery/config.py`.

```toml
schema_version = 1

[node]
id = "node-1"
size = "M"
data_dir = "./data/node-1"
replication_factor = 3
partition_shift = 17
seeds = ["127.0.0.1:7001"]

[servers.kv]
bind = "127.0.0.1:7000"

[servers.peer]
bind = "127.0.0.1:7001"
advertise = "127.0.0.1:7001"

[tls]
cert_data = "<base64 PEM>"
key_data = "<base64 PEM>"
ca_data = "<base64 PEM>"
```

Required sections in loader:

- `[node]`
- `[tls]`
- `[servers.kv]` and `[servers.peer]` when `[servers]` is present

#### `contexts.toml` format (current)

Models live in `tourlib/models.py`; orchestration lives in `tourlib/contexts.py`.

```toml
current-context = "prod"

[[contexts]]
name = "prod"

  [contexts.cluster]
  name = "prod"
  ca_data = "<base64 PEM>"

  [contexts.endpoints]
  kv = "kv.prod.example.com:7000"
  peer = "peer.prod.example.com:7001"

  [contexts.credentials]
  cert_data = "<base64 PEM>"
  key_data = "<base64 PEM>"
```

#### `Envelope` wire format (current)

Implemented in `tourillon/core/structure/envelope.py`.

- constants: `PROTO_VERSION = 1`, `KIND_MAX_LEN = 64`
- fixed header format: `!BH16sI`
- constructor validates UTF-8 kind length (1..64 bytes)
- `encode()` / `decode()` implement canonical framing

#### TLS model (current)

- Port: `tourillon/core/ports/tls.py` (`TlsContext`)
- Adapter: `tourillon/infra/tls.py` (`CryptographyTlsContext`)
- Validation error type used by TLS adapter: `tourillon/ports/tls.py::TlsValidationError`

`NodeStarter` builds two server SSL contexts (peer + kv) from the same node TLS material.

#### Transport model (current)

- Dispatcher: `tourillon/core/transport/dispatcher.py`
- Connection contract and constants: `tourillon/core/transport/conn.py`
- Framing I/O: `tourillon/core/transport/framing.py`
- Server: `tourillon/core/transport/server.py`

`Dispatcher.on(kind)` and `Dispatcher.register(kind, handler)` are both available.
Unknown kinds close the connection without response.

#### Bootstrap composition root (current)

- Core facade: `tourillon/core/facade.py`
- Wiring: `tourillon/bootstrap/deps.py`

`get_core(config_path=None)` constructs:

- `TomlConfigReadWriter`
- `CryptographyTlsContext`
- `CryptographyX509Issuer`
- `TourillonCore`
- optional `NodeManager` when `config_path` is provided

---

## Core invariants (current)

1. `TourillonConfig` is immutable (`@dataclass(frozen=True)`).
2. `NodeSize` is a closed `StrEnum` with deterministic `token_count` mapping.
3. TLS cert/key/CA are stored inline as base64 strings in config/context models.
4. `Dispatcher` rejects duplicate kind registration (`ValueError`).
5. Envelope kind length is validated at construction and decode.
6. `TomlConfigReadWriter.write()` performs temp-file + `os.replace()` writes and chmod `0600`.
7. `tourctl config use-context` never silently creates missing context files.

---

## Sequence flows (current)

### Node startup (`tourillon node start`)

1. CLI sets logging via `setup_logging()`.
2. `get_core(config_path=...)` loads config and creates `NodeManager`.
3. `NodeManager.start()` delegates to `NodeStarter.start()`.
4. `NodeStarter` builds peer and kv SSL contexts, constructs `TcpServer` instances.
5. Peer and KV listeners are started according to startup phase/seeds logic.

### `tourillon pki ca`

1. Parse CLI options.
2. Build `CaRequest`.
3. `NodeConfigService.generate_ca()` delegates to `CryptographyX509Issuer.generate_ca()`.
4. Adapter writes CA cert and private key.

### `tourillon pki issue`

1. Parse CLI options.
2. Build `CertRequest`.
3. `NodeConfigService.issue_cert()` delegates to `CryptographyX509Issuer.issue_cert()`.
4. Adapter validates CA material and writes leaf cert + key.

### `tourillon config generate`

1. Parse CLI options into `ConfigRequest`.
2. Issue temporary node cert/key via `issue_cert`.
3. Build `TourillonConfig` with base64 inline TLS blobs.
4. Persist TOML via config read-writer.

### `tourillon config generate-context`

1. Parse positional subject + options.
2. Issue temporary client cert/key.
3. Load existing contexts (or empty model if missing).
4. Upsert `ContextEntry` and save.

---

## Error paths (current)

| Scenario | Behavior |
|---|---|
| PKI cannot read/write files | CLI prints `✗ ...`, exits `1` |
| PKI crypto/validation failure | CLI prints `✗ ...`, exits `2` |
| `tourctl` contexts file missing | prints `✗ Contexts file not found: ...`, exits `1` |
| `tourctl` unknown context | prints `✗ Context "..." not found in ...`, exits `1` |
| Unknown envelope kind on server | connection closes without response |
| Protocol framing violation | server emits protocol error envelope and closes |

---

## Repository map (current)

```text
tourillon/bootstrap/main.py                 Typer root app (config/pki/node)
tourillon/bootstrap/deps.py                 Dependency assembly + dispatcher singletons
tourillon/bootstrap/cli/pki.py              pki ca / pki issue commands
tourillon/bootstrap/cli/config.py           config generate / generate-context commands
tourillon/bootstrap/cli/node.py             node start command
tourillon/bootstrap/cli/utils.py            shared CLI PKI error mapping

tourillon/core/facade.py                    TourillonCore
 tourillon/core/services/config.py          NodeConfigService (config + cert + contexts)
tourillon/core/services/manager.py          NodeManager
tourillon/core/services/starter.py          NodeStarter

tourillon/core/machinery/config.py          NodeSize
tourillon/core/structure/config.py          TourillonConfig + ConfigRequest + ConfigError
tourillon/core/structure/cert.py            CaRequest / CertRequest
tourillon/core/structure/envelope.py        Envelope wire model

tourillon/core/ports/pki.py                 PkiError + X509CertificateIssuer protocol
tourillon/core/ports/tls.py                 TlsContext protocol

tourillon/core/transport/conn.py            transport constants + handler protocol + errors
tourillon/core/transport/dispatcher.py      kind-to-handler registry
tourillon/core/transport/framing.py         async envelope read/write helpers
tourillon/core/transport/server.py          TcpServer implementation

tourillon/infra/x509.py                     CryptographyX509Issuer adapter
tourillon/infra/tls.py                      CryptographyTlsContext adapter

tourlib/models.py                           ContextsFile and nested context models
tourlib/contexts.py                         ContextConfigurer load/save orchestration
tourlib/infra/toml_rw.py                    TOML read/write adapter

tourctl/bootstrap/main.py                   tourctl root app
tourctl/bootstrap/cli/config.py             tourctl config use-context
```

---

## Deferred from earlier revisions

The following items were previously referenced in this file but are not in the current
repository layout for proposal 001 and are therefore deferred to later proposals:

- `ProcessLockPort` / `FileProcessLockAdapter`
- `TcpClient` request/stream API
- serializer port and MessagePack adapter wiring
- duration/bytes unit parser helpers (`parse_duration`, `parse_bytes`)
- additional config sections (`join`, `drain`, `rebalance`) in `config.toml`

---

## Exit criteria (updated)

- [ ] CLI contracts above remain stable unless superseded by a new proposal.
- [ ] Paths and module references in this proposal match the repository layout.
- [ ] Bootstrap commands generate valid self-contained TOML artifacts.
- [ ] Envelope/Dispatcher/TcpServer behavior remains consistent with described invariants.
- [ ] Future extensions add scope via new proposals rather than retroactive path drift.

---

## Out of scope

- Ring ownership and partition placement behavior.
- Gossip and multi-node join protocol details.
- KV command envelope semantics.
- Rebalance/gc/pause feature-specific config sections.
- Client transport pooling and serializer expansion.
