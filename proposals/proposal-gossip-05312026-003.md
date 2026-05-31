# Proposal: Gossip Engine & Seeded Join

<!-- Naming: proposal-<short-desc>-MMDDYYYY-SEQ.md
     Example: proposal-gossip-05312026-003.md     -->

**Author**: Tourillon Contributors <tourillon@example.com>
**Status:** Draft
**Date:** 2026-05-31
**Sequence:** 003

---

## Summary

This proposal specifies the gossip engine and seeded join protocol for Tourillon. It
introduces three complementary anti-entropy paths (`gossip.push`, `gossip.ping/pong`,
`gossip.digest/delta`), the `IDLE → JOINING` phase transition triggered by
`tourctl node join`, and exponential-backoff seed contact configured via the `[join]`
section defined in proposal 001. It also rewrites `core/lifecycle/probe.py` to
support a dual-detector model per peer — a phi-accrual `FailureDetector` (`gossip_fd`)
fed by `gossip.pong` arrivals, and an asymmetric `DataCircuitBreaker` (`data_fd`) fed
by data-plane outcomes. New `ProbeConfig` and
`GossipConfig` sections are added to `config.toml`. The convergence proof rests on the
`Member.supersedes()` comparator (lexicographic `(generation, seq)`), which is
clock-skew-safe because it uses only logical counters. `GossipEngine` and `JoinController` receive a `PeerClientPool` instance (`core/transport/pool.py`) injected at construction; no port abstraction is needed.

No amendments to this proposal are permitted; later proposals extend it only through
new `Dispatcher` registrations and new `[config]` sections.

---

## Motivation

Proposal 002 delivers first-node bootstrap (`IDLE → READY`) and the full ring layer.
Before a second node can join and before KV traffic can be load-balanced across
replicas, three further problems must be solved:

1. **Membership propagation** — Each node must learn the address, phase, and ring
   tokens of every other node without a centralised directory. Gossip dissemination
   provides probabilistic broadcast in `O(log N)` rounds with no single point of
   failure.

2. **Seeded join** — A new node needs at least one existing peer address (a seed) to
   bootstrap its view of the cluster. The seed contact must tolerate transient network
   failures and retry with exponential backoff so that a cluster restart does not
   produce a thundering herd.

3. **Local failure detection** — Before the data plane can route around failed replicas,
   each node must independently maintain a local suspicion model
   for every peer. Proposal 002 introduced a single phi-accrual `FailureDetector` per
   peer driven by gossip heartbeats. This proposal adds a second, asymmetric
   `DataCircuitBreaker` per peer that is fed by data-plane outcomes and is
   intentionally faster to suspect and slower to recover than the gossip detector.

Without this proposal, no second node can discover the cluster, join it, or be
observed as suspect by its peers.

---

## CLI contract

All commands print to stdout on success and to stderr on failure.
Exit code `0` = success; `1` = user or config error; `2` = internal / transport error.

### `tourctl node join` — transition a running IDLE node from IDLE to JOINING

```
$ tourctl node join ADDRESS [OPTIONS]

  Instruct a running IDLE node to transition to JOINING.
  Connects to ADDRESS (the node's peer listener, host:port), sends node.join,
  and waits for the node.join.ack response. Once acknowledged the daemon contacts
  seeds (from its own config.toml, or overridden via --seeds) and begins gossiping.
  The node moves to READY when partition transfer completes.

  Use 'tourctl node inspect' to monitor join progress.

Arguments:
  ADDRESS               Peer address of the target node (host:port)  [required]

Options:
  --seeds TEXT          Comma-separated seed addresses to override config.toml seeds
  --context TEXT        Context name from contexts.toml
  --contexts-file PATH  Path to contexts.toml  [default: ~/.tourillon/contexts.toml]
  --timeout TEXT        Request timeout       [default: 30s]
  --help                Show this message and exit.
```

When `--seeds` is supplied those addresses are used instead of `config.toml` seeds.

**Happy path output (stdout):**

```
Node node-1 is now JOINING.
Contacting seeds: ["192.168.1.2:7001", "192.168.1.3:7001"]
Gossip engine started. Use 'tourctl node inspect' to follow progress.
```

**Error — node is not IDLE (wrong phase) (stderr, exit 1):**

```
Error: node node-1 is in phase "ready"; join requires phase "idle".
Use 'tourctl node inspect' to check current state.
```

**Error — neither --peer nor context peer endpoint supplied (stderr, exit 1):**

```
Error: peer address required: supply --peer or a context with a peer endpoint.
```

**Error — request timeout (stderr, exit 2):**

```
Error: no response from 192.168.1.1:7001 within 30s.
The node may be unreachable or not running. Check peer_server.bind in config.toml.
```

**Error — connection refused (stderr, exit 2):**

```
Error: connection refused: 192.168.1.1:7001
The node may not be running. Start it with 'tourillon node start'.
```

---

## Design

### Data model

#### `ProbeConfig` (new, `core/structure/config.py`)

```toml
[probe]
data_suspect_cooldown      = "30s"  # min elapsed since last failure before recovery
data_suspect_min_successes = 3      # consecutive successes required for recovery
```

```python
@dataclass(frozen=True)
class ProbeConfig:
    data_suspect_cooldown: str = "30s"
    data_suspect_min_successes: int = 3
```

`data_suspect_cooldown` is a duration string; `parse_duration` validates it at startup.

#### `GossipConfig` (new, `core/structure/config.py`)

```toml
[gossip]
push_interval = "1s"    # how often each node pushes to fanout random peers
ping_interval = "500ms" # how often each node pings every known peer
fanout        = 3       # number of peers to push to per push cycle
```

```python
@dataclass(frozen=True)
class GossipConfig:
    push_interval: str = "1s"
    ping_interval: str = "500ms"
    fanout: int = 3
```

Both duration fields are validated by `parse_duration` at startup.

#### `TourillonConfig` additions

```python
@dataclass(frozen=True)
class TourillonConfig:
    # ... all fields from previous proposals ...
    probe: ProbeConfig = field(default_factory=ProbeConfig)     # NEW
    gossip: GossipConfig = field(default_factory=GossipConfig)  # NEW
```

Both sections are optional in TOML; their dataclass defaults apply when absent.

#### `DataCircuitBreaker` (new, `core/lifecycle/probe.py`)

An asymmetric circuit breaker for data-plane reachability. Unlike the phi-accrual
`FailureDetector`, which models a continuous distribution of heartbeat inter-arrivals,
`DataCircuitBreaker` is binary:

- **Fast to suspect**: the first recorded failure immediately transitions to SUSPECT.
- **Slow to recover**: recovery requires **both** `K` consecutive successes **and**
  at least `cooldown` seconds elapsed since the last failure. Either condition alone
  is insufficient.
- The circuit re-opens (SUSPECT) on any new failure, resetting both the consecutive-
  success counter and the cooldown clock.

```
State machine:
  LIVE  --record_failure()-→  SUSPECT
  SUSPECT --record_failure()-→ SUSPECT (resets counter + clock)
  SUSPECT --record_success() Ã— K AND cooldown elapsed-→ LIVE
  SUSPECT --record_success() < K-→ SUSPECT  (counter increments, no state change)
  SUSPECT --record_success() = K but cooldown not elapsed-→ SUSPECT (timer pending)
```

`is_suspect: bool` — `True` when in SUSPECT state; `False` when LIVE.

`record_failure()` — Immediately sets state to SUSPECT; resets consecutive-success
counter; stamps `_last_failure_time = time.monotonic()`.

`record_success()` — Increments `_consecutive_successes`. Transitions to LIVE only
if `_consecutive_successes >= min_successes` **and**
`time.monotonic() - _last_failure_time >= cooldown_s`. Does nothing in LIVE state.

#### Updated `ProbeManager` (rewrite of `core/lifecycle/probe.py`)

`ProbeManager` holds **two independent detectors** per peer:

| Field | Type | Purpose |
|---|---|---|
| `gossip_fd` | `FailureDetector` | Phi-accrual; fed by `gossip.pong` arrivals |
| `data_fd` | `DataCircuitBreaker` | Binary circuit breaker; fed by data-plane outcomes |

**Combined state rule**: `is_suspect(node_id) → gossip_fd.is_suspect OR data_fd.is_suspect`

Where `gossip_fd.is_suspect` means `not gossip_fd.is_available(threshold)` AND
`gossip_fd.has_observations` (unknown peers are not suspect from the gossip path).

**New public API:**

| Method | Description |
|---|---|
| `record_data_failure(node_id)` | Forward failure to `data_fd`; create pair if absent |
| `record_data_success(node_id)` | Forward success to `data_fd`; create pair if absent |
| `record_heartbeat(node_id)` | Forward heartbeat to `gossip_fd`; create pair if absent |
| `record_miss(node_id)` | Create pair without recording an interval (gossip timeout) |
| `state_of(node_id)` | Return combined `MemberState` |
| `is_suspect(node_id)` | `True` if combined state is SUSPECT |
| `is_live(node_id)` | `True` if combined state is LIVE |
| `is_unknown(node_id)` | `True` if combined state is UNKNOWN |
| `phi_of(node_id)` | Return gossip phi; `0.0` when no observations |
| `snapshot()` | `dict[str, MemberState]` combined snapshot |
| `all_states_with_phi()` | `list[tuple[str, MemberState, float, bool]]` — see below |

**`all_states_with_phi()` signature change:**

```python
# Before:
async def all_states_with_phi() -> list[tuple[str, MemberState, float]]:
    # (node_id, combined_state, gossip_phi)

# After (this proposal):
async def all_states_with_phi() -> list[tuple[str, MemberState, float, bool]]:
    # (node_id, combined_state, gossip_phi, data_is_suspect)
```

The 4th element `data_is_suspect: bool` is `DataCircuitBreaker.is_suspect` for the
node. It allows operator tooling to distinguish "gossip-live but data-dead" from
"gossip-dead but data-live" for diagnosis.

**Combined `MemberState` derivation:**

```
gossip observation absent → UNKNOWN (gossip_fd has no observations)
gossip_fd.is_suspect OR data_fd.is_suspect → SUSPECT
else → LIVE
```

Note: `data_fd.is_suspect` (SUSPECT from data plane) does NOT override an UNKNOWN
gossip state. A peer with no gossip observations at all is UNKNOWN regardless of
data outcomes.

#### Gossip message payload dataclasses (`core/gossip/messages.py`)

All payloads are serialised with `SerializerPort` (MessagePack schema). The domain
layer defines frozen dataclasses; the handlers encode/decode via the injected
serializer. No payload dataclass may import `infra/` or `msgpack`.

```python
@dataclass(frozen=True)
class GossipPushPayload:
    sender_id: str
    members: list[dict[str, Any]]  # each dict encodes one Member; see note below

@dataclass(frozen=True)
class GossipPingPayload:
    sender_id: str
    fingerprint: str   # hex string from TopologyManager.member_fingerprint()

@dataclass(frozen=True)
class GossipPongPayload:
    sender_id: str
    fingerprint: str   # local fingerprint at the moment of pong generation
    needs_digest: bool # True when pong.fingerprint != received ping.fingerprint

@dataclass(frozen=True)
class GossipDigestEntry:
    node_id: str
    generation: int
    seq: int

@dataclass(frozen=True)
class GossipDigestPayload:
    sender_id: str
    entries: list[GossipDigestEntry]

@dataclass(frozen=True)
class GossipDeltaPayload:
    sender_id: str
    members: list[dict[str, Any]]   # serialised Member records

@dataclass(frozen=True)
class GossipErrorPayload:
    sender_id: str
    code: str      # "partition_shift_mismatch"
    message: str

@dataclass(frozen=True)
class NodeJoinAckPayload:
    node_id: str
    phase: str    # "joining"

@dataclass(frozen=True)
class NodeJoinErrorPayload:
    code: str          # "invalid_phase"
    message: str
    current_phase: str
```

**Member serialisation note:** `Member` is serialised to/from a plain `dict` inside
gossip push and delta payloads. The mapping is 1-to-1 with `Member`'s fields; `tokens`
is a list of ints (each ≤ 128-bit, encoded as `ExtType(1)` by `MsgpackSerializerAdapter`).
A helper `member_to_dict(m: Member) -> dict` / `dict_to_member(d: dict) -> Member`
lives in `core/gossip/messages.py` and is the single codec for this transformation.
It never imports msgpack; it only produces/consumes plain Python dicts.

#### `GossipEngine` (`core/gossip/engine.py`)

`GossipEngine` drives all gossip activity for a running node. It owns two background
`asyncio.Task` instances (managed via `asyncio.TaskGroup` inside `start()`):

| Task | Interval | Description |
|---|---|---|
| `_push_cycle` | `gossip.push_interval` | Fan-out push to `gossip.fanout` random peers |
| `_ping_cycle` | `gossip.ping_interval` | Ping all known peers; initiate digest on mismatch |

The engine holds references to:
- `TopologyManager` — source of member list and fingerprint
- `ProbeManager` — target of heartbeat and miss recordings
- `SerializerPort` — encode/decode payloads
- `PeerClientPool` — persistent mTLS connection pool; `acquire(node_id, address)` returns a `TcpClient`
- `TourillonConfig` — push interval, ping interval, fanout, join config

**Push cycle** — concurrent fan-out via `WaitGroup[str]`:

```
1. snapshot = await topology_mgr.snapshot()
2. candidates = [m for m in snapshot.registry
                 if m.node_id != self_node_id
                 and m.phase not in {IDLE}]
3. targets = random.sample(candidates, min(fanout, len(candidates)))
4. payload = encode(GossipPushPayload(sender_id, members=list(snapshot.registry)))
5. wg: WaitGroup[str] = WaitGroup()
   await wg.add(len(targets))
   for target in targets:
       async def _push(t=target):
           try:
               client = await pool.acquire(t.node_id, t.peer_address)
               await client.send(Envelope.create(payload, kind="gossip.push", schema_id=1))
               await wg.done(t.node_id, success=True)
           except Exception:
               # log WARNING
               await wg.done(t.node_id, success=False)
       asyncio.create_task(_push())
6. await wg.wait()   # fire-and-forget; no response expected per push
7. await asyncio.sleep(parse_duration(cfg.gossip.push_interval))
```

Connection errors are logged at WARNING level and do not abort the cycle.
`WaitGroup.wait()` is used only for back-pressure — the push cycle starts the next
sleep only after all outgoing sends have been attempted.

**Ping cycle** — concurrent pings via `WaitGroup[str]`:

```
1. snapshot = await topology_mgr.snapshot()
2. candidates = [m for m in snapshot.registry if m.node_id != self_node_id]
3. local_fp = await topology_mgr.member_fingerprint()
4. wg: WaitGroup[str] = WaitGroup()
   await wg.add(len(candidates))
   for peer in candidates:
       async def _ping(p=peer):
           try:
               client = await pool.acquire(p.node_id, p.peer_address)
               ping_env = Envelope.create(
                   encode(GossipPingPayload(sender_id, fingerprint=local_fp)),
                   kind="gossip.ping", schema_id=1,
               )
               pong_env = await client.request(
                   ping_env, timeout=parse_duration(cfg.join.attempt_timeout)
               )
               pong = decode(pong_env.payload, GossipPongPayload)
               await probe_mgr.record_heartbeat(p.node_id)
               if pong.needs_digest:
                   await _initiate_digest(client, p.node_id)
               await wg.done(p.node_id, success=True)
           except (ResponseTimeoutError, ConnectionClosedError):
               await probe_mgr.record_miss(p.node_id)
               await wg.done(p.node_id, success=False)
       asyncio.create_task(_ping())
5. await wg.wait()
6. await asyncio.sleep(parse_duration(cfg.gossip.ping_interval))
```

`WaitGroup[str]` is keyed by `node_id`. `success_nodes` (from `wg.wait()`) are the
peers that replied within `attempt_timeout`; they have been fed to `record_heartbeat`.
`failed_nodes` have been fed to `record_miss`.

**Digest initiation (`_initiate_digest`):**

```
1. snapshot = await topology_mgr.snapshot()
2. entries = [GossipDigestEntry(m.node_id, m.generation, m.seq)
              for m in snapshot.registry]
3. digest_env = Envelope.create(
       encode(GossipDigestPayload(sender_id, entries=entries)),
       kind="gossip.digest", schema_id=1,
   )
4. delta_env = await client.request(digest_env, timeout=...)
5. delta = decode(delta_env.payload, GossipDeltaPayload)
6. members = [dict_to_member(d) for d in delta.members]
7. await topology_mgr.merge_registry(members)
```

#### `JoinController` and `ExponentialBackoff` (`core/gossip/join.py`)

`JoinController` handles the `IDLE → JOINING` transition atomically:

```
1. Load state from state_port; assert phase == IDLE (raise JoinError otherwise).
2. new_generation = persisted.generation + 1 (or 1 if no prior state)
3. tokens = tuple(secrets.randbelow(hash_space.max) for _ in range(node_size.token_count))
4. new_state = NodeState(node_id, JOINING, new_generation, seq=0, tokens, ...)
5. await state_port.save(new_state)              # write-before-announce
6. member = Member(node_id, peer_address, new_generation, 0, JOINING, tokens, partition_shift)
7. await topology_mgr.apply_member(member)       # IDLE→JOINING; no ring change; no epoch++
8. Start GossipEngine (if not already running).
9. Return new_state.
```

`ExponentialBackoff` computes retry delays with full jitter:

```
delay(attempt) = min(backoff_base_s * 2^attempt, backoff_max_s) * uniform(0.8, 1.2)
```

`next_delay(attempt: int) → float | None`:
- Returns the jittered delay in seconds for `attempt` ≥ 0.
- Returns `None` when the deadline has been exceeded.

Seed contact loop (called from `GossipEngine` startup):

```
deadline_at = time.monotonic() + parse_duration(cfg.join.deadline)
attempt = 0
while time.monotonic() < deadline_at:
    for seed in cfg.seeds:
        try:
            client = await connector.connect(seed)
            push_env = Envelope.create(
                encode(GossipPushPayload(sender_id, members=[self_member])),
                kind="gossip.push", schema_id=1,
            )
            await client.send(push_env)
            return   # contacted at least one seed successfully
        except (ConnectionError, ResponseTimeoutError, ConnectionClosedError):
            continue
    delay = _backoff.next_delay(attempt)
    if delay is None:
        break
    attempt += 1
    await asyncio.sleep(delay)
raise JoinTimeoutError(
    f"could not contact any seed within deadline "
    f"{cfg.join.deadline!r}; tried seeds: {cfg.seeds}"
)
```

`JoinError(Exception)` — raised when `transition_idle_to_joining()` is called but
the current phase is not IDLE.

`JoinTimeoutError(Exception)` — raised when all seeds are unreachable and the
deadline is exceeded.

#### Socket lifecycle

| Phase | Peer server | KV server |
|---|---|---|
| `IDLE` | **bound** | not bound |
| `JOINING` | **bound** | not bound |
| `READY` | **bound** | **bound** |
| `DRAINING` | **bound** | **bound** |
| `PAUSED` | **bound** | out of scope |
| `FAILED` | not bound | not bound |

The peer server is always bound while the daemon process is running, including during
`JOINING`. This ensures that gossip push/ping/digest from other nodes can reach a
joining node immediately, which speeds convergence. The KV server is bound only when
`phase ∈ {READY, DRAINING}`. This invariant is enforced by the bootstrap wiring:

- The bootstrap sequence binds the KV server only after reaching READY.
- `node_join` (this proposal) does **not** bind the KV server; it only starts
  the gossip engine.
- Binding the KV server as part of the `JOINING → READY` transition is out of scope
  for this proposal.

#### Gossip handler grouping (`core/gossip/handlers/`)
Handlers are organised in a package with one module per envelope prefix. Each module
owns a module-level `Dispatcher` instance and registers its handlers via `@<name>.on(kind)`
at import time. No `register()` function.
```python
# core/gossip/handlers/gossip.py
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.types import ReceiveEnvelope, SendEnvelope
gossip = Dispatcher()
@gossip.on("gossip.push")
async def push(receive: ReceiveEnvelope, send: SendEnvelope) -> None: ...
@gossip.on("gossip.ping")
async def ping(receive: ReceiveEnvelope, send: SendEnvelope) -> None: ...
@gossip.on("gossip.digest")
async def digest(receive: ReceiveEnvelope, send: SendEnvelope) -> None: ...
@gossip.on("gossip.error")
async def error(receive: ReceiveEnvelope, send: SendEnvelope) -> None:
    # Receives unsolicited gossip.error (e.g. partition_shift_mismatch).
    # Logs at ERROR level; no response sent.
    ...
```
```python
# core/gossip/handlers/node.py
from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.types import ReceiveEnvelope, SendEnvelope
node = Dispatcher()
@node.on("node.join")
async def join(receive: ReceiveEnvelope, send: SendEnvelope) -> None: ...
```
```python
# core/gossip/handlers/__init__.py
from tourillon.core.gossip.handlers.gossip import gossip
from tourillon.core.gossip.handlers.node import node
__all__ = ["gossip", "node"]
```

#### `PeerClientPool` (`core/transport/pool.py`)

```python
```

`GossipEngine` and `JoinController` hold a `PeerClientPool` reference injected at
construction time. `pool.acquire(node_id, address)` returns a connected `TcpClient`;
the pool owns the mTLS context.

### Envelope kinds

All payloads are msgpack-serialised via `SerializerPort`.

| Kind | Direction | Request/Response | Description |
|---|---|---|---|
| `gossip.push` | A → B | fire-and-forget | A fans out its full member list to B |
| `gossip.ping` | A → B | request | A asks B if views are in sync (sends local fingerprint) |
| `gossip.pong` | B → A | response to `gossip.ping` | B replies with its fingerprint; `needs_digest=True` triggers digest exchange |
| `gossip.digest` | A → B | request | A sends its `(node_id, generation, seq)` digest to B |
| `gossip.delta` | B → A | response to `gossip.digest` | B replies with all members newer than the digest |
| `gossip.error` | B → A | unsolicited or response | B sends a structured error (e.g. `partition_shift_mismatch`) to A |
| `node.join` | tourctl → daemon | request | CLI requests IDLE → JOINING transition |
| `node.join.ack` | daemon → tourctl | response to `node.join` | Daemon confirms transition to JOINING |
| `node.join.error` | daemon → tourctl | response to `node.join` | Daemon rejects join (e.g. wrong phase) |

`gossip.pong` and `gossip.delta` are not registered as inbound handlers — they are
received as return values of `client.request()` on the initiating side. `gossip.error`
**is** registered as an inbound handler because it can arrive unsolicited when a peer
rejects a fire-and-forget `gossip.push`.

---

### Core invariants

1. **Write-before-announce for JOINING.** `state_port.save(new_state)` must complete
   before `topology_mgr.apply_member(member)` is called in `JoinController`. A crash
   after save but before `apply_member` leaves the node in a safely-restartable
   JOINING state; on restart the gossip engine resumes from the persisted JOINING state.

2. **IDLE → JOINING is the only legal join source phase.** `JoinController` raises
   `JoinError` if the current phase is anything other than IDLE. The `node.join`
   handler propagates this as `node.join.error` with `code="invalid_phase"`.

3. **JOINING tokens are immutable.** Tokens are generated once at the start of the
   JOINING transition and never regenerated. Restart from a JOINING state reads the
   persisted tokens from `state.toml`.

4. **Clock-skew-safe convergence.** Merge decisions use only the `(generation, seq)`
   pair from `Member.supersedes()`, never wall-clock time. A node with a skewed
   system clock cannot make a stale record appear newer.

5. **Monotone merge.** `MemberRegistry.upsert()` only replaces a record if the new
   one strictly supersedes the existing one. This means concurrent gossip rounds
   from different peers always converge to the same final state regardless of
   delivery order.

6. **Partition-shift mismatch is fatal.** A gossip push handler that receives a
   `Member` with `partition_shift != cfg.partition_shift` must reply with
   `GossipErrorPayload(code="partition_shift_mismatch")` and ignore the member.
   The misconfigured remote node is responsible for detecting this condition through
   its own gossip path and transitioning to FAILED.

7. **DataCircuitBreaker resets on new failure.** Any call to `record_failure()` while
   SUSPECT resets both `_consecutive_successes = 0` and `_last_failure_time`, preventing
   a partially-recovered node from being declared LIVE after a single bad fanout write.

8. **`GossipEngine` does not feed `data_fd`.** Gossip control messages (`gossip.push`,
   `gossip.ping`, `gossip.pong`, `gossip.digest`, `gossip.delta`, `node.join`) do
   **not** call `record_data_failure` or `record_data_success`. Only data-plane
   outcomes (rebalance transfer errors, KV fanout errors) feed the `DataCircuitBreaker`.

9. **Handler registration is startup-time only.** `core/gossip/handlers/` modules are imported once during bootstrap;
   `gossip = Dispatcher()` and `node = Dispatcher()` are module-level singletons
   populated at import time. Dependencies (topology_mgr, probe_mgr, join_controller)
   are fixed for the process lifetime.

### Sequence / flow — `tourctl node join`

```
CLI side:
  1. Parse options; ADDRESS is the positional peer address argument.
  2. If --seeds given, override seeds list; otherwise seeds come from config.toml.
  3. load_contexts(contexts_file) → ContextsFile; select context if --context given.
  4. Build ssl_ctx via build_client_ssl_context(cert_data, key_data, ca_data).
  5. TcpClient.connect(ADDRESS, ssl_ctx).
  6. Construct Envelope(kind="node.join", payload=encode({"seeds_override": seeds}), ...).
  7. response = await client.request(join_env, timeout=parse_duration(options.timeout))
  8. If response.kind == "node.join.ack": print success lines; exit 0.
  9. If response.kind == "node.join.error": decode NodeJoinErrorPayload; print error; exit 1.

Daemon side (node_join):
  1. Receive "node.join" envelope via receive().
  2. Acquire JoinController.
  3. persisted = await state_port.load()
  4. current_phase = persisted.phase if persisted else MemberPhase.IDLE
  5. If current_phase != IDLE:
       payload = NodeJoinErrorPayload(code="invalid_phase",
                                      current_phase=str(current_phase), ...)
       await send(Envelope.create(encode(payload), kind="node.join.error", ...))
       return
  6. new_state = await join_controller.transition_idle_to_joining()
         # saves state.toml, applies member, starts GossipEngine
  7. payload = NodeJoinAckPayload(node_id=cfg.node_id, phase="joining")
  8. await send(Envelope.create(encode(payload), kind="node.join.ack", ...))
```

### Sequence / flow — `gossip.push` handler

```
1. env = await receive()
2. push = decode(env.payload, GossipPushPayload)
3. members = [dict_to_member(d) for d in push.members]
4. For each member in members:
     if member.partition_shift != cfg.partition_shift:
         error_payload = GossipErrorPayload(sender_id=cfg.node_id,
                                             code="partition_shift_mismatch",
                                             message=...)
         await send(Envelope.create(encode(error_payload), kind="gossip.error", ...))
         return   # ignore the entire push
5. accepted = await topology_mgr.merge_registry(members)
   # no response for successful push (fire-and-forget)
```

### Sequence / flow — `gossip.ping` handler

```
1. env = await receive()
2. ping = decode(env.payload, GossipPingPayload)
3. local_fp = await topology_mgr.member_fingerprint()
4. needs_digest = (ping.fingerprint != local_fp)
5. pong = GossipPongPayload(sender_id=cfg.node_id,
                             fingerprint=local_fp,
                             needs_digest=needs_digest)
6. await send(Envelope.create(encode(pong), kind="gossip.pong", schema_id=1,
                               correlation_id=env.correlation_id))
7. await probe_mgr.record_heartbeat(ping.sender_id)
   # feeds gossip_fd for the pinging node
```

### Sequence / flow — `gossip.digest` handler

```
1. env = await receive()
2. digest = decode(env.payload, GossipDigestPayload)
3. snapshot = await topology_mgr.snapshot()
4. sender_known = {e.node_id: (e.generation, e.seq) for e in digest.entries}
5. delta_members = []
   for member in snapshot.registry:
       known = sender_known.get(member.node_id)
       if known is None or (member.generation, member.seq) > known:
           delta_members.append(member_to_dict(member))
6. delta = GossipDeltaPayload(sender_id=cfg.node_id, members=delta_members)
7. await send(Envelope.create(encode(delta), kind="gossip.delta", schema_id=1,
                               correlation_id=env.correlation_id))
```

### Convergence guarantee

**Theorem**: Any two nodes A and B that share at least one common gossip peer will
converge to identical member views after at most `diameter(G)` gossip rounds, where
`G` is the gossip overlay graph.

**Proof sketch**: `MemberRegistry.upsert()` is a join-semilattice operation with
`(generation, seq)` as the partial order. For a fixed node_id, the set of possible
records is totally ordered by `supersedes()`. The merge function is monotone-increasing:
`upsert(m, existing)` returns `max(m, existing)` under this order. Since the order is
a total order, any two nodes that have exchanged members (directly or transitively)
will agree on `max(m)` after the exchange. By induction over the graph diameter, full
convergence is reached in `O(log N)` rounds.

**Clock-skew safety**: The comparison pair `(generation, seq)` contains only
monotonically-increasing logical counters. `generation` increments on every new join;
`seq` increments on every gossip-propagated state change within a generation.
Neither counter is derived from wall-clock time. A node with a misconfigured clock
cannot cause its records to appear newer than they are.

### Error paths

| Scenario | Behaviour |
|---|---|
| `tourctl node join` — ADDRESS not supplied | stderr "Error: ADDRESS is required"; exit 1 |
| `tourctl node join` — node in non-IDLE phase | Daemon responds `node.join.error(code="invalid_phase")`; tourctl prints error; exit 1 |
| `tourctl node join` — request timeout | stderr "Error: no response from … within …"; exit 2 |
| `tourctl node join` — connection refused | stderr "Error: connection refused: …"; exit 2 |
| `gossip.push` — partition_shift mismatch | Handler sends `gossip.error(code="partition_shift_mismatch")`; push ignored |
| `gossip.push` — member record older than known state | `MemberRegistry.upsert()` returns `False`; topology unchanged (no-op) |
| `gossip.ping` — sender unknown to registry | Handler still responds with pong; sender is added to probe_mgr via `record_heartbeat` |
| `gossip.digest` — empty digest (sender has no members) | Handler sends `gossip.delta` with all local members (delta = full registry) |
| Seed contact — all seeds unreachable within deadline | GossipEngine raises `JoinTimeoutError`; daemon logs error; node remains JOINING |
| Seed contact — partial seed failure (some respond) | Join succeeds on first successful contact; remaining seeds skipped for this attempt |
| `DataCircuitBreaker.record_failure()` while LIVE | Immediately transitions to SUSPECT; `_last_failure_time` stamped |
| `ProbeManager` — node absent from `_pairs` | All query methods return UNKNOWN; mutation methods create pair lazily |
| `load_config` — unrecognised duration suffix in `[probe]` | `ConfigError` at startup; daemon exits before binding any socket |
| `load_config` — unrecognised duration suffix in `[gossip]` | `ConfigError` at startup; daemon exits before binding any socket |

---

## Design decisions

### Decision: Dual detectors per peer rather than a single unified detector

**Alternatives considered:**
- (A) Extend the phi-accrual threshold dynamically based on data-plane observations.
- (B) Replace the phi-accrual detector entirely with a circuit-breaker model.
- (C) Two independent detectors — phi-accrual for gossip, asymmetric circuit breaker
  for data plane.

**Chosen because:** (C) models the two failure modes accurately. The phi-accrual
detector is ideal for continuous, periodic signals (gossip heartbeats); it handles
variable inter-arrival distributions gracefully and avoids false positives under
transient delays. The asymmetric circuit breaker is ideal for discrete, binary
outcomes (write succeeded / failed); fast suspicion limits write loss, while
slow recovery prevents oscillation under flapping conditions. Conflating them with
(A) or discarding gossip-level health with (B) loses diagnostic precision.

### Decision: `all_states_with_phi()` returns a 4-tuple with explicit `data_is_suspect`

**Alternatives considered:**
- Return a `PeerProbeState` dataclass instead of a tuple.
- Omit `data_is_suspect` and leave diagnosis to per-detector queries.

**Chosen because:** Operator tooling needs to display both detectors in a
compact tabular format. A 4-tuple is the minimal addition that preserves
backward-compatibility with existing call sites that destructure 3-tuples (they will
receive a `ValueError` at destructuring time, which is an explicit compile/test failure
rather than silent data loss). A dataclass would require callers to import from
`core/lifecycle/probe.py`, which they already do; so the 4-tuple is not a meaningful
constraint. The plain tuple is chosen for simplicity: no new dataclass, no extra import.

**Note for callers:** any existing code that destructures `all_states_with_phi()` as
`(node_id, state, phi)` must be updated to `(node_id, state, phi, data_is_suspect)`.
This is an intentional breaking change that forces explicit acknowledgment of the dual
detector model.

### Decision: `gossip.pong` carries `needs_digest: bool` rather than a separate `gossip.sync_request` message

**Alternatives considered:**
- Separate `gossip.sync_request` message to request a digest exchange.
- Piggybacking the `needs_digest` flag on the pong.

**Chosen because:** The ping-pong round already establishes a live connection between
two peers with matching correlation IDs. Piggy-backing `needs_digest` on the pong
avoids a full extra round-trip. The pong receiver already has the client open and can
immediately follow up with a `gossip.digest` request without establishing a second
connection.

### Decision: Fire-and-forget push (no push acknowledgment)

**Alternatives considered:**
- Require a push acknowledgment (`gossip.push.ack`) to confirm receipt.

**Chosen because:** An acknowledgment would add latency and connection overhead to
every gossip push. The phi-accrual detector (`gossip.ping/pong`) already provides
reachability confirmation. Push convergence does not require every push to succeed;
the probabilistic fan-out and the periodic ping-triggered digest exchange guarantee
that diverged views are reconciled within a bounded number of rounds.

### Decision: `PeerClientPool` as the gossip layer's networking abstraction
**Alternatives considered:**
- A `PeerConnectorPort` Protocol in `core/ports/` with a `TlsPeerConnector` infra adapter.
- Pass `ssl.SSLContext` directly to `GossipEngine`.
**Chosen because:** `PeerClientPool` is already the shared connection pool used by gossip,
rebalance, and replication. Adding a Protocol layer on top only introduces indirection without
new capability � the pool is testable with `ssl_ctx=None`. Reusing it directly keeps one
fewer abstraction and ensures all subsystems share the same per-node connection, avoiding
duplicate mTLS handshakes.### Decision: Seed contact in `GossipEngine` startup rather than in `JoinController`

**Alternatives considered:**
- `JoinController.transition_idle_to_joining()` blocks until at least one seed is
  contacted (before returning to the `node.join` handler).

**Chosen because:** Seed contact may involve many retries with exponential backoff,
potentially taking up to `join.deadline` (default 2 minutes). Blocking the
`handle_node_join` handler for that duration would tie up the peer socket connection
to `tourctl` and provide no progress feedback. Decoupling seed contact into an
async background task inside `GossipEngine` means `node.join.ack` is returned
immediately after the JOINING state is persisted, giving the operator a fast
acknowledgment while the gossip engine works in the background.

### Decision: `ExponentialBackoff` uses full jitter rather than pure exponential

**Alternatives considered:**
- Pure exponential: `delay = base * 2^attempt`.
- Equal-jitter: `delay = (base * 2^attempt) / 2 + uniform(0, base * 2^attempt / 2)`.
- Full jitter: `delay = uniform(0.8 * capped, 1.2 * capped)` where
  `capped = min(base * 2^attempt, backoff_max)`.

**Chosen because:** Full jitter provides the best protection against thundering herds
in a multi-node cluster restart scenario (AWS Architecture Blog, 2015: "Exponential
Backoff and Jitter"). The Â±20% band ensures that nodes starting simultaneously
quickly desynchronise, preventing all of them from contacting the same seed at
exactly the same instant.

### Decision: `partition_shift` mismatch on push — reply with error and ignore

**Alternatives considered:**
- Silently ignore the push with no response.
- Close the connection.

**Chosen because:** Sending `gossip.error` gives the operator immediate, observable
feedback (visible in the receiving node's logs) that a node is misconfigured. Silent
ignore would make this class of misconfiguration very hard to diagnose. Closing the
connection would disrupt other in-flight messages on the same correlation context.

---

## Interfaces (informative)

### `tourillon/core/structure/config.py` — additions

```python
@dataclass(frozen=True)
class ProbeConfig:
    data_suspect_cooldown: str = "30s"       # duration string
    data_suspect_min_successes: int = 3

@dataclass(frozen=True)
class GossipConfig:
    push_interval: str = "1s"    # duration string
    ping_interval: str = "500ms" # duration string
    fanout: int = 3

@dataclass(frozen=True)
class TourillonConfig:
    # ... all fields from previous proposals ...
    probe: ProbeConfig = field(default_factory=ProbeConfig)
    gossip: GossipConfig = field(default_factory=GossipConfig)
```

### `tourillon/core/lifecycle/probe.py` — rewrite

```python
class DataCircuitBreaker:
    """Asymmetric circuit breaker for data-plane reachability.

    Fast to suspect (1 failure → SUSPECT immediately).
    Slow to recover (K consecutive successes AND cooldown elapsed).
    """

    def __init__(self, min_successes: int = 3, cooldown_s: float = 30.0) -> None: ...

    @property
    def is_suspect(self) -> bool: ...

    def record_failure(self) -> None: ...

    def record_success(self) -> None: ...


class MemberState(StrEnum):
    LIVE    = "live"
    SUSPECT = "suspect"
    UNKNOWN = "unknown"


class ProbeManager:
    """Manages a (gossip_fd, data_fd) detector pair per peer."""

    def __init__(self, cfg: ProbeConfig | None = None) -> None: ...

    async def state_of(self, node_id: str) -> MemberState: ...
    async def is_suspect(self, node_id: str) -> bool: ...
    async def is_live(self, node_id: str) -> bool: ...
    async def is_unknown(self, node_id: str) -> bool: ...
    async def record_heartbeat(self, node_id: str) -> None: ...
    async def record_miss(self, node_id: str) -> None: ...
    async def record_data_failure(self, node_id: str) -> None: ...
    async def record_data_success(self, node_id: str) -> None: ...
    async def phi_of(self, node_id: str) -> float: ...
    async def snapshot(self) -> dict[str, MemberState]: ...
    async def all_states_with_phi(self) -> list[tuple[str, MemberState, float, bool]]:
        """Return (node_id, combined_state, gossip_phi, data_is_suspect) tuples."""
        ...
```

### `tourillon/core/gossip/messages.py`

```python
def member_to_dict(m: Member) -> dict[str, Any]: ...
def dict_to_member(d: dict[str, Any]) -> Member: ...

@dataclass(frozen=True)
class GossipPushPayload:
    sender_id: str
    members: list[dict[str, Any]]

@dataclass(frozen=True)
class GossipPingPayload:
    sender_id: str
    fingerprint: str

@dataclass(frozen=True)
class GossipPongPayload:
    sender_id: str
    fingerprint: str
    needs_digest: bool

@dataclass(frozen=True)
class GossipDigestEntry:
    node_id: str
    generation: int
    seq: int

@dataclass(frozen=True)
class GossipDigestPayload:
    sender_id: str
    entries: list[GossipDigestEntry]

@dataclass(frozen=True)
class GossipDeltaPayload:
    sender_id: str
    members: list[dict[str, Any]]

@dataclass(frozen=True)
class GossipErrorPayload:
    sender_id: str
    code: str
    message: str

@dataclass(frozen=True)
class NodeJoinAckPayload:
    node_id: str
    phase: str

@dataclass(frozen=True)
class NodeJoinErrorPayload:
    code: str
    message: str
    current_phase: str
```

### `tourillon/core/gossip/join.py`

```python
class JoinError(Exception):
    """Raised when join is attempted from a non-IDLE phase."""

class JoinTimeoutError(Exception):
    """Raised when all seeds are unreachable within the join deadline."""

class ExponentialBackoff:
    def __init__(
        self,
        base_s: float,
        max_s: float,
        deadline_s: float,
    ) -> None: ...

    def next_delay(self, attempt: int) -> float | None:
        """Return jittered delay for this attempt; None if deadline exceeded."""
        ...

class JoinController:
    def __init__(
        self,
        cfg: TourillonConfig,
        state_port: StatePort,
        topology_mgr: TopologyManager,
        hash_space: HashSpace,
    ) -> None: ...

    async def transition_idle_to_joining(self) -> NodeState:
        """Atomically execute the IDLE → JOINING phase transition.

        Raises JoinError when current phase is not IDLE.
        Write-before-announce: state is persisted before topology is mutated.
        """
        ...
```

### `tourillon/core/gossip/engine.py`

```python
class GossipEngine:
    def __init__(
        self,
        node_id: str,
        cfg: TourillonConfig,
        topology_mgr: TopologyManager,
        probe_mgr: ProbeManager,
        serializer: SerializerPort,
        pool: PeerClientPool,
    ) -> None: ...

    async def start(self) -> None:
        """Start the push and ping background tasks via asyncio.TaskGroup.

        Each task uses WaitGroup[str] internally for concurrent fanout within
        a single push or ping cycle.
        """
        ...

    async def stop(self) -> None:
        """Cancel background tasks and await their completion."""
        ...
```

### `tourillon/core/gossip/handlers/`

```python
# core/gossip/handlers/gossip.py
gossip = Dispatcher()

@gossip.on("gossip.push")
async def push(receive: ReceiveEnvelope, send: SendEnvelope) -> None: ...

@gossip.on("gossip.ping")
async def ping(receive: ReceiveEnvelope, send: SendEnvelope) -> None: ...

@gossip.on("gossip.digest")
async def digest(receive: ReceiveEnvelope, send: SendEnvelope) -> None: ...

@gossip.on("gossip.error")
async def error(receive: ReceiveEnvelope, send: SendEnvelope) -> None: ...
```

```python
# core/gossip/handlers/node.py
node = Dispatcher()

@node.on("node.join")
async def join(receive: ReceiveEnvelope, send: SendEnvelope) -> None: ...
```

### `tourillon/core/ports/connector.py`

```python
from tourillon.core.transport.client import TcpClient

```

### `tourillon/infra/transport/connector.py`

```python
import ssl

class TlsPeerConnector:
    """PeerClientPool is used directly; see core/transport/pool.py."""

    def __init__(self, ssl_ctx: ssl.SSLContext) -> None: ...

    async def connect(self, address: str) -> TcpClient: ...
```

---

## Proposed code organisation

Files **created or modified** by this proposal (in mandatory creation order):

```
tourillon/core/structure/config.py               MODIFIED — adds ProbeConfig, GossipConfig;
                                                             adds both to TourillonConfig
tourillon/core/lifecycle/probe.py                CREATE — DataCircuitBreaker,
                                                             dual-detector ProbeManager,
                                                             all_states_with_phi() → 4-tuple
tourillon/core/transport/pool.py                 already present — PeerClientPool (no changes)
tourillon/core/gossip/__init__.py                NEW — package marker
tourillon/core/gossip/messages.py                NEW — GossipPushPayload, PingPayload,
                                                        PongPayload, DigestPayload,
                                                        DeltaPayload, ErrorPayload,
                                                        NodeJoinAckPayload,
                                                        NodeJoinErrorPayload,
                                                        member_to_dict, dict_to_member
tourillon/core/gossip/join.py                    NEW — JoinError, JoinTimeoutError,
                                                        ExponentialBackoff, JoinController
tourillon/core/gossip/engine.py                  NEW — GossipEngine
tourillon/core/gossip/handlers/
    __init__.py                  NEW — re-exports gossip, node dispatchers
    gossip.py                    NEW — gossip = Dispatcher(); push, ping,
                                         digest, error handlers
    node.py                      NEW — node = Dispatcher(); join handler
tourillon/infra/transport/__init__.py            NEW — package marker (if absent)
tourillon/infra/transport/connector.py           NEW — TlsPeerConnector
tourctl/infra/cli/node.py                        MODIFIED — adds tourctl node join command
tests/unit/test_data_circuit_breaker.py          NEW — scenarios 1–5
tests/unit/test_probe_dual.py                    NEW — scenarios 6–10
tests/unit/test_gossip_handlers.py               NEW — scenarios 11–17, 23
tests/unit/test_join_controller.py               NEW — scenarios 18–22, 24–27
tests/e2e/test_node_join.py                      NEW — scenario 28
```

Files already present and unchanged:

```
tourillon/core/structure/waitgroup.py            (complete — WaitGroup[T])
tourillon/core/lifecycle/member.py               (complete — Member, MemberPhase)
tourillon/core/lifecycle/registry.py             (complete — MemberRegistry)
tourillon/core/lifecycle/phi.py                  (complete — FailureDetector)
tourillon/core/ring/topology.py                  (complete — TopologyManager, Topology)
tourillon/core/transport/dispatcher.py           (complete — Dispatcher)
tourillon/core/structure/envelope.py             (complete — Envelope)
tourillon/bootstrap/config.py                    (complete — parse_duration, ConfigError)
```

---

## Test scenarios

All scenarios run with in-memory adapters unless marked `[e2e]`.
E2e tests use `tmp_path` (pytest fixture) and real subprocess / filesystem.

| # | Mark | Fixture | Action | Expected |
|---|------|---------|--------|----------|
| 1 | unit | `DataCircuitBreaker(min_successes=3, cooldown_s=0.0)` | `dcb.is_suspect` on fresh instance | Returns `False` (starts LIVE; no failures yet) |
| 2 | unit | `DataCircuitBreaker(min_successes=3, cooldown_s=0.0)` | `dcb.record_failure(); dcb.is_suspect` | Returns `True` immediately (1 failure → SUSPECT) |
| 3 | unit | `DataCircuitBreaker(min_successes=3, cooldown_s=0.0)` with one prior failure | `dcb.record_success(); dcb.record_success(); dcb.is_suspect` | Returns `True` (K=3 successes not yet reached; 2 < 3) |
| 4 | unit | `DataCircuitBreaker(min_successes=3, cooldown_s=0.0)` with one prior failure | `dcb.record_success()` called 3 times | `dcb.is_suspect` returns `False` (3 successes ≥ K AND cooldown=0s already elapsed) |
| 5 | unit | `DataCircuitBreaker(min_successes=3, cooldown_s=3600.0)` with one prior failure | `dcb.record_success()` called 3 times immediately | `dcb.is_suspect` remains `True` (K successes reached but cooldown=1h not elapsed) |
| 6 | unit | `ProbeManager(ProbeConfig())` | `await pm.state_of("n1")` with no prior observations | Returns `MemberState.UNKNOWN` |
| 7 | unit | `ProbeManager(ProbeConfig())` | `await pm.record_heartbeat("n1")`; then `await pm.state_of("n1")` | Returns `MemberState.LIVE` (gossip_fd has first observation; phi = 0.0 < threshold) |
| 8 | unit | `ProbeManager(ProbeConfig())` with `"n1"` already LIVE via gossip heartbeat | `await pm.record_data_failure("n1")`; then `await pm.is_suspect("n1")` | Returns `True` (data_fd suspects n1; combined OR rule applies) |
| 9 | unit | `ProbeManager(ProbeConfig())` | `await pm.record_heartbeat("n2")`; `await pm.all_states_with_phi()` | Returns list with one 4-tuple `("n2", MemberState.LIVE, 0.0, False)`; 4th element is `data_fd.is_suspect == False` |
| 10 | unit | `ProbeManager(ProbeConfig())` with no prior failures for `"n3"` | `await pm.record_data_failure("n3")`; then `await pm.record_data_success("n3")`; `await pm.record_data_success("n3")`; `await pm.is_suspect("n3")` | Returns `True` (only 2 consecutive successes; K=3 not reached) |
| 11 | unit | In-memory `TopologyManager()`; `push_handler` from `handlers.register(...)` | Deliver `gossip.push` envelope containing one `Member(node_id="n2", phase=READY, gen=1, seq=1, ...)` | `topology_mgr.snapshot().registry.get("n2")` returns the member; `merge_registry` call count is 1 |
| 12 | unit | In-memory `TopologyManager()` seeded with `Member("n2", gen=1, seq=5)` | Deliver `gossip.push` containing `Member("n2", gen=1, seq=3)` (stale) | Registry unchanged (`get("n2").seq == 5`); `merge_registry` called but accepted count is 0 |
| 13 | unit | `gossip.push` handler; local `cfg.partition_shift = 10` | Deliver `gossip.push` containing `Member(partition_shift=8)` | Handler calls `send()` with envelope `kind="gossip.error"`; decoded payload has `code="partition_shift_mismatch"`; member not merged |
| 14 | unit | `gossip.ping` handler; `ProbeManager()` | Deliver `gossip.ping(sender_id="n2", fingerprint="abc123")` | Handler sends `gossip.pong` response with same `correlation_id`; `pong.sender_id == cfg.node_id`; `probe_mgr.record_heartbeat("n2")` called once |
| 15 | unit | `gossip.ping` handler; local fingerprint `"xyz789"` | Deliver `gossip.ping(sender_id="n2", fingerprint="abc123")` | Decoded `GossipPongPayload.needs_digest == True`; `pong.fingerprint == "xyz789"` (local fingerprint) |
| 16 | unit | `gossip.digest` handler; local registry has `Member("n1", gen=1, seq=5)` | Deliver `gossip.digest` with entry `("n1", gen=1, seq=2)` (stale entry) | Handler sends `gossip.delta`; decoded `GossipDeltaPayload.members` contains exactly one entry for `"n1"` with `gen=1, seq=5` |
| 17 | unit | `gossip.digest` handler; digest entries match local registry exactly | Deliver `gossip.digest` with up-to-date entries for all local members | Handler sends `gossip.delta` with `members == []` (empty; nothing to send) |
| 18 | unit | `JoinController`; `InMemoryStateAdapter(None)`; empty `TopologyManager()` | `await join_controller.transition_idle_to_joining()` | Returns `NodeState(phase=JOINING, generation=1, seq=0)`; `len(state.tokens) == cfg.node_size.token_count`; `state_port.save()` called before `topology_mgr.apply_member()` (write-before-announce) |
| 19 | unit | `JoinController`; `InMemoryStateAdapter(NodeState(phase=JOINING, ...))` | `await join_controller.transition_idle_to_joining()` | Raises `JoinError` (current phase is JOINING, not IDLE) |
| 20 | unit | `ExponentialBackoff(base_s=2.0, max_s=30.0, deadline_s=999.0)` | `[backoff.next_delay(i) for i in range(5)]` | First five values are approximately `2.0Â±20%, 4.0Â±20%, 8.0Â±20%, 16.0Â±20%, 30.0Â±20%` (cap reached at attempt 4) |
| 21 | unit | `ExponentialBackoff(base_s=2.0, max_s=30.0, deadline_s=0.001)` | `backoff.next_delay(0)` after `time.monotonic() > deadline_at` | Returns `None` (deadline exceeded; no further delay) |
| 22 | unit | `JoinController`; `InMemoryStateAdapter(None)`; all seed connections raise `ConnectionClosedError`; `cfg.join.deadline = "100ms"` | `await join_controller.transition_idle_to_joining()` then seed loop exhausts deadline | `JoinTimeoutError` raised; JOINING state was persisted (write-before-announce satisfied); topology has JOINING member |
| 23 | unit | `gossip.push` handler; topology seeded with one READY member | Deliver `gossip.push` containing `Member("n3", phase=JOINING, gen=1, seq=0, tokens=(5,))` | `"n3"` inserted into registry with `phase=JOINING`; ring size unchanged (JOINING nodes not added to ring via `TopologyManager` rule) |
| 24 | unit | `MemberRegistry()` with `Member("n4", gen=2, seq=0)` | `registry.upsert(Member("n4", gen=1, seq=99))` | Returns `False`; stored member still has `gen=2` (higher generation always wins regardless of seq) |
| 25 | unit | `TopologyManager()`; two members with identical `node_id` but different `seq` | `await tm.merge_registry([m_gen1_seq3, m_gen1_seq5])` | Registry stores `seq=5`; `merge_registry` returns `1` (only one accepted) |
| 26 | unit | `node.join` handler; `InMemoryStateAdapter(NodeState(phase=READY, ...))` | Deliver `node.join` envelope | Handler sends `node.join.error` response; `decoded.code == "invalid_phase"`; `decoded.current_phase == "ready"` |
| 27 | unit | `DataCircuitBreaker(min_successes=3, cooldown_s=0.0)` recovered to LIVE after 3 successes | `dcb.record_failure()` | `dcb.is_suspect` returns `True` again immediately; `_consecutive_successes` reset to 0 |
| 28 | e2e | Running first-node daemon (proposal 002 bootstrap); valid `contexts.toml` with peer endpoint | `tourctl node join --peer <peer_addr> --contexts-file tmp/contexts.toml` | Exit code 0; stdout contains "is now JOINING"; `state.toml` in `data_dir` has `phase = "joining"` and non-empty `tokens` array |

---

## Exit criteria

- [ ] All 28 test scenarios pass (`uv run pytest -m "unit or e2e" -x`).
- [ ] `uv run pytest --cov=tourillon --cov=tourctl --cov-fail-under=90` passes.
- [ ] `uv run ruff check tourillon/ tourctl/ tests/` passes with zero violations.
- [ ] `uv run black --check tourillon/ tourctl/ tests/` passes.
- [ ] `core/structure/config.py` — `ProbeConfig` and `GossipConfig` dataclasses exist;
  both are fields on `TourillonConfig` with `field(default_factory=...)`.
- [ ] `load_config` calls `parse_duration` on `ProbeConfig.data_suspect_cooldown`,
  `GossipConfig.push_interval`, and `GossipConfig.ping_interval`; invalid suffixes
  raise `ConfigError` before any socket is opened.
- [ ] `DataCircuitBreaker.record_failure()` sets `is_suspect = True` regardless of
  prior state.
- [ ] `DataCircuitBreaker` recovery requires BOTH `_consecutive_successes >= min_successes`
  AND `time.monotonic() - _last_failure_time >= cooldown_s`.
- [ ] `ProbeManager.is_suspect(node_id)` returns `True` when either `gossip_fd` or
  `data_fd` suspects `node_id`.
- [ ] `ProbeManager.all_states_with_phi()` returns 4-tuples
  `(node_id, MemberState, float, bool)`.
- [ ] `JoinController.transition_idle_to_joining()` calls `state_port.save()` before
  `topology_mgr.apply_member()` (write-before-announce).
- [ ] `JoinController` raises `JoinError` when current phase is not IDLE.
- [ ] `TopologyManager.apply_member()` with `IDLE → JOINING` does not add vnodes to
  the ring and does not increment the epoch (not changed by this proposal).
- [ ] `handlers/gossip.py::push` rejects members with a mismatched `partition_shift` by
  sending `gossip.error(code="partition_shift_mismatch")` and ignoring the push.
- [ ] `handlers/gossip.py::error` logs the received error at `ERROR` level and sends no response.
- [ ] `handlers/gossip.py::ping` calls `probe_mgr.record_heartbeat(sender_id)` and sends a
  `gossip.pong` response with the local fingerprint.
- [ ] `handlers/gossip.py::digest` sends a `gossip.delta` containing only members newer
  than the digest entries supplied by the sender.
- [ ] `ExponentialBackoff.next_delay()` returns `None` after the deadline is exceeded.
- [ ] `GossipEngine` calls `pool.acquire(node_id, address)` and sends via the returned `TcpClient`; it does not import `ssl`.
- [ ] `GossipEngine` push and ping cycles use `WaitGroup[str]` (from `core/structure/waitgroup.py`) to fan out concurrently; `wg.wait()` is awaited before the next `asyncio.sleep`.
- [ ] `core/gossip/handlers/` — `gossip = Dispatcher()` and `node = Dispatcher()` are module-level; handlers registered at import time via `@gossip.on(kind)` / `@node.on(kind)`; no `register()` function.
- [ ] No module under `tourillon/core/` imports `infra/`, `msgpack`, `ssl`, or
  `tomllib`/`tomli_w` directly.
- [ ] `tourctl node join` exits with code 1 when the node is not IDLE.
- [ ] `tourctl node join` exits with code 2 on connection refused or timeout.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

- `JOINING → READY` transition — covered by a later proposal.
- `DRAINING → IDLE` transition — covered by a later proposal.
- `PAUSED` phase transitions and `node.pause` / `node.resume` commands — covered by
  a later proposal.
- Data-plane outcomes feeding `record_data_failure` / `record_data_success` — covered
  by later proposals (rebalance transfer errors, KV fanout errors / read repair).
- `tourctl node inspect` and `node.inspect` envelope kind — covered by a later proposal.
- KV operations (`kv.put`, `kv.get`, `kv.delete`) — covered by a later proposal.
- `[kv]` and `[gc]` config sections — covered by later proposals.
- `BackendStorage` port and `PartitionStore` implementation — covered by a later proposal.
- Gossip tombstone eviction — covered by a later proposal.
- Certificate rotation — static for the node lifetime in this proposal; requires a
  node restart.
- Gossip message signing or authentication beyond mTLS — the cluster CA already
  provides mutual authentication for every TCP connection.
- Per-peer connection pooling in `GossipEngine` — `core/transport/pool.py` exists;
  wiring is deferred to a later proposal requiring persistent pooled connections.
- Gossip fanout tuning and adaptive failure detection threshold — operator tooling
  for tuning is deferred to a future observability proposal.
