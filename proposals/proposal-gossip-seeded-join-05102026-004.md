# Proposal: Gossip Engine & Seeded Node Join

**Author**: Souleymane BA <soulsmister@gmail.com>
**Status:** Accepted — Incomplete (rebalance pending, see notice below)
**Date:** 2026-05-10
**Sequence:** 004
**Schema version:** 2

---

> ⚠️ **Incomplete proposal.** The `JOINING → READY` transition requires
> partition data transfer coordinated by the rebalance protocol. That portion
> is **out of scope** for this proposal and will be specified in a dedicated
> rebalance proposal. Until that proposal is accepted and implemented, a node
> that completes gossip bootstrap remains in `JOINING` and does **not** advance
> to `READY` automatically. Its peer server stays bound and accepts operator
> commands; its KV server is not started.

---

## Summary

This proposal extends proposal 002 (Ring & First-Node Bootstrap) with three
tightly coupled mechanisms:

1. **Seeded node join** — the `tourctl node join [--seeds ...]` command that
   triggers the `IDLE → JOINING` transition on a running daemon and initiates
   gossip bootstrap against the cluster seeds. A node with seeds configured
   never auto-joins on startup; it always waits for an explicit operator
   command.

2. **Gossip bootstrap with exponential backoff** — the full-resync sequence sent
   to every seed when phase is `JOINING` or `DRAINING` at startup, with
   configurable backoff and a maximum retry count before the daemon exits.

3. **Gossip engine** — the three-path anti-entropic engine (`gossip.push`,
   `gossip.ping/pong`, `gossip.digest/delta`) that propagates `Member` state
   changes across all nodes and keeps the cluster topology converged.

The socket lifecycle is clarified across all phases: the peer server is bound
for every phase so the node can always receive operator commands; the KV server
is bound exclusively when `phase ∈ {READY, DRAINING}`.

Together these pieces allow a second (or Nth) node to find the cluster, download
a consistent view of the membership registry, and reach `JOINING` — at which
point the rebalance proposal will complete the transition to `READY`.

---

## Motivation

Proposal 002 established the ring layer and `TopologyManager`. It defined how
one node computes its own ring and persists its own state. It did not define how
that state reaches other nodes. Without a propagation mechanism each node is an
island: it can never learn that a peer transitioned from `JOINING` to `READY`,
that a draining node released its partitions, or that a new member joined the
cluster.

Gossip is the only membership-propagation mechanism in Tourillon. There is no
coordinator, no leader, and no broadcast channel. Every node must reach a
consistent view of the cluster solely by exchanging messages with a bounded
number of peers. The design must:

- converge rapidly (within seconds) even on 10 000-node clusters without
  flooding every node on every change;
- keep steady-state bandwidth near zero when member state is stable;
- survive network partitions and recover correct state on reconnection;
- tolerate clock skew — merge order must never depend on wall-clock time;
- not introduce any plaintext fallback or coordinator node.

Proposal 002 also deferred the seeded join path entirely. This proposal fills
that gap: it defines the explicit operator gesture (`tourctl node join`) and the
`IDLE → JOINING` transition, giving `tourillon node start` a clear, safe
behaviour for every persisted phase it may encounter.

---

## CLI contract

### `tourillon node start` — startup behaviour by phase

`tourillon node start` reads `config.toml`, acquires the exclusive process lock
(`pid.lock`), verifies that `data_dir` exists, then drives the node lifecycle
according to the **persisted phase** and the presence of seeds. The daemon emits
zero terminal output outside of the Python `logging` subsystem. `setup_logging()`
is the first call in every Typer command.

`data_dir` must already exist when `tourillon node start` is called. If the
directory is absent the process logs an error with the resolved absolute path
and exits with code 1 before acquiring any lock or binding any socket.

#### Accepted phases and their startup behaviour

| Persisted phase | Seeds configured? | Startup action |
|---|:---:|---|
| `IDLE` | No | **First-node bootstrap** (proposal 002): `IDLE → READY` directly. Bind peer server + KV server. |
| `IDLE` | Yes | Bind **peer server only**. Log waiting message. No phase transition. Await `tourctl node join`. |
| `JOINING` | — | **Gossip bootstrap** with backoff (see Design). Bind **peer server only**. Remain in `JOINING` until rebalance completes (out of scope). |
| `READY` | — | **Crash-recovery**: rebuild topology from persisted state. Bind peer server + KV server. No transition. |
| `DRAINING` | — | **Gossip bootstrap** with backoff. Bind peer server + KV server. Resume drain protocol (out of scope). |
| `PAUSED` | — | Bind **peer server only**. Log paused message. No transition. Not covered by this proposal. |
| `FAILED` | — | Bind **peer server only**. Log failed WARNING. No transition. Not covered by this proposal. |

#### IDLE with seeds — waiting for join

```
$ tourillon node start --config config.toml   # seeds set, phase=idle
2026-05-10T14:22:00 INFO     [tourillon.infra.cli.node] Using data directory: /srv/node-2.
2026-05-10T14:22:00 INFO     [tourillon.core.lifecycle.bootstrap] Node 'node-2' starting from phase 'idle' with 2 seed(s).
2026-05-10T14:22:00 INFO     [tourillon.core.transport.server] Peer server listening on 0.0.0.0:7701.
2026-05-10T14:22:00 INFO     [tourillon.infra.cli.node] Node 'node-2' is idle; issue 'tourctl node join' to begin seeded join.
```

The daemon blocks in its serve loop. The KV server is **not** bound.

#### JOINING restart — immediate gossip bootstrap

```
$ tourillon node start --config config.toml   # phase=joining (crash-restart)
2026-05-10T14:22:00 INFO     [tourillon.infra.cli.node] Using data directory: /srv/node-2.
2026-05-10T14:22:00 INFO     [tourillon.core.lifecycle.bootstrap] Node 'node-2' starting from phase 'joining'. Resuming gossip bootstrap.
2026-05-10T14:22:00 INFO     [tourillon.core.transport.server] Peer server listening on 0.0.0.0:7701.
2026-05-10T14:22:01 INFO     [tourillon.core.gossip.engine] Gossip bootstrap complete. seeds_ok=2 seeds_err=0
2026-05-10T14:22:01 INFO     [tourillon.core.gossip.engine] GossipEngine started.
2026-05-10T14:22:01 INFO     [tourillon.core.lifecycle.bootstrap] Node 'node-2' remains in phase 'joining'; waiting for rebalance to complete.
```

#### READY restart — crash recovery

```
$ tourillon node start --config config.toml   # phase=ready
2026-05-10T14:22:00 INFO     [tourillon.infra.cli.node] Using data directory: /srv/node-2.
2026-05-10T14:22:00 INFO     [tourillon.core.lifecycle.bootstrap] Node 'node-2' starting from phase 'ready'.
2026-05-10T14:22:00 INFO     [tourillon.core.lifecycle.bootstrap] Topology rebuilt for node 'node-2': 4 vnode(s), epoch 3.
2026-05-10T14:22:00 INFO     [tourillon.core.transport.server] Peer server listening on 0.0.0.0:7701.
2026-05-10T14:22:00 INFO     [tourillon.core.transport.server] KV server listening on 0.0.0.0:7700.
2026-05-10T14:22:01 INFO     [tourillon.core.gossip.engine] GossipEngine started.
2026-05-10T14:22:01 INFO     [tourillon.infra.cli.node] Node 'node-2' is ready (epoch 3, generation 1).
```

#### DRAINING restart — gossip bootstrap + KV server

```
$ tourillon node start --config config.toml   # phase=draining
2026-05-10T14:22:00 INFO     [tourillon.infra.cli.node] Using data directory: /srv/node-2.
2026-05-10T14:22:00 INFO     [tourillon.core.lifecycle.bootstrap] Node 'node-2' starting from phase 'draining'. Resuming drain with gossip bootstrap.
2026-05-10T14:22:00 INFO     [tourillon.core.transport.server] Peer server listening on 0.0.0.0:7701.
2026-05-10T14:22:00 INFO     [tourillon.core.transport.server] KV server listening on 0.0.0.0:7700.
2026-05-10T14:22:01 INFO     [tourillon.core.gossip.engine] Gossip bootstrap complete. seeds_ok=2 seeds_err=0
2026-05-10T14:22:01 INFO     [tourillon.core.gossip.engine] GossipEngine started.
```

#### PAUSED restart

```
$ tourillon node start --config config.toml   # phase=paused
2026-05-10T14:22:00 INFO     [tourillon.infra.cli.node] Using data directory: /srv/node-2.
2026-05-10T14:22:00 INFO     [tourillon.core.lifecycle.bootstrap] Node 'node-2' starting from phase 'paused'.
2026-05-10T14:22:00 INFO     [tourillon.core.transport.server] Peer server listening on 0.0.0.0:7701.
2026-05-10T14:22:00 INFO     [tourillon.infra.cli.node] Node 'node-2' is paused; resume behaviour is not covered by this proposal.
```

#### FAILED restart

```
$ tourillon node start --config config.toml   # phase=failed
2026-05-10T14:22:00 INFO     [tourillon.infra.cli.node] Using data directory: /srv/node-2.
2026-05-10T14:22:00 INFO     [tourillon.core.lifecycle.bootstrap] Node 'node-2' starting from phase 'failed'.
2026-05-10T14:22:00 INFO     [tourillon.core.transport.server] Peer server listening on 0.0.0.0:7701.
2026-05-10T14:22:00 WARNING  [tourillon.infra.cli.node] Node 'node-2' is in phase 'failed'; recovery behaviour is not covered by this proposal.
```

#### Gossip bootstrap with backoff — retry log

```
2026-05-10T14:22:01 WARNING  [tourillon.core.gossip.engine] Gossip bootstrap attempt 1/10 failed; retrying in 1.0s.
2026-05-10T14:22:03 WARNING  [tourillon.core.gossip.engine] Gossip bootstrap attempt 2/10 failed; retrying in 2.1s.
...
2026-05-10T14:22:35 ERROR    [tourillon.core.gossip.engine] Gossip bootstrap failed after 10 attempts: no seed responded. Exiting.
```

#### Graceful shutdown (Ctrl-C / SIGTERM)

```
2026-05-10T14:23:50 INFO     [tourillon.infra.cli.node] Shutdown signal received; stopping node 'node-2'.
2026-05-10T14:23:50 INFO     [tourillon.core.transport.server] Peer server stopped.
2026-05-10T14:23:50 INFO     [tourillon.core.transport.server] KV server stopped.
2026-05-10T14:23:50 INFO     [tourillon.infra.cli.node] Node 'node-2' stopped cleanly.
```

---

### `tourctl node join` — seeded join command

```
tourctl node join <peer-address> [--seeds ADDRESS ...]
```

`tourctl node join` connects **directly** to the target node's peer server at
`<peer-address>` (e.g. `10.0.0.2:7701`), sends a `node.join` envelope, and
waits for the acknowledgement. There is no forwarding: the operator always
specifies the exact bind address of the node to join. TLS credentials are taken
from the active context.

The `--seeds` flag overrides the seeds embedded in the target node's
`config.toml` for this join attempt. If `--seeds` is omitted, the daemon uses
the seeds from its own `config.toml`.

The command returns immediately after the target acknowledges the join request
(`node.join.ok`). The target executes the `IDLE → JOINING` transition and
gossip bootstrap asynchronously. The operator monitors progress via
`tourctl node inspect <node-id>` (the node-id is shown in the command output).

```
$ tourctl node join 10.0.0.2:7701 --seeds 10.0.0.1:7701
Connecting to 10.0.0.2:7701 …

Seeded join initiated for node 'node-2'.
Phase → joining.
Monitor progress with: tourctl node inspect node-2
```

Error cases:

```
# Target is not IDLE
$ tourctl node join 10.0.0.2:7701
Error: node 'node-2' is in phase 'joining'; already joining.

# No seeds in config and no --seeds provided
$ tourctl node join 10.0.0.2:7701
Error: no seeds available; provide --seeds or configure [cluster].seeds.

# Cannot reach the target
$ tourctl node join 10.0.0.99:7701
✗ Cannot connect to 10.0.0.99:7701: Connection refused
```

---

### `tourctl node inspect` — gossip_stats field

The `NodeInspectResponse` gains a `gossip_stats` object populated at all times
once the engine has started. The `tourctl node inspect` command renders it as
an additional section:

```
$ tourctl node inspect node-1

Node:      node-1
Phase:     ready
...

Gossip stats:
  Known members:      10000
  Push sent total:    4521
  Push recv total:    4398
  AE cycles total:    88     diverged: 3
  Last AE peer:       node-42   at 2026-05-10T14:23:45Z
  Bootstrap seeds ok: 3   err: 1
```

When `--json` is passed the raw `gossip_stats` object is included in the JSON
response as documented in the Interfaces section.

---

## Design

### Socket lifecycle

The peer server and KV server are bound independently according to phase. This
table is authoritative; no other code path may deviate from it.

| Phase | Peer server | KV server | Reason |
|---|:---:|:---:|---|
| `IDLE` | ✓ | ✗ | Awaits `node.join`; no data-plane traffic. |
| `JOINING` | ✓ | ✗ | Downloading partition snapshot; not yet serving reads or writes. |
| `READY` | ✓ | ✓ | Fully operational. |
| `DRAINING` | ✓ | ✓ | Still serves reads; writes redirected to handoff target. |
| `PAUSED` | ✓ | ✗ | Peer server bound for operator commands; recovery out of scope. |
| `FAILED` | ✓ | ✗ | Peer server bound for operator commands; recovery out of scope. |

The peer server is bound unconditionally so the node can receive commands from
`tourctl` at any phase without an additional "bind-if" condition in the startup
sequence. The KV server is bound **only when `phase ∈ {READY, DRAINING}`** and
is closed immediately on transition out of those phases.

### Startup integrity checks

Before executing the phase-based startup logic (binding any server or launching
any gossip task), `tourillon node start` runs two integrity checks. These checks
guard against node misconfiguration, directory reuse between nodes, and state
corruption that would silently corrupt the ring.

#### node_id consistency — config vs state

If `state.toml` is present and `NodeState.node_id != config.node_id`, the
process logs an ERROR and **exits with code 1**. A state directory that belongs
to a different node must never be loaded — doing so would inject wrong tokens
and a wrong generation into the ring and gossip registry with no way to repair
it at runtime.

```
ERROR [tourillon.core.lifecycle.bootstrap] node_id mismatch: config='node-2'
      state='node-1'. This data_dir belongs to a different node. Check your
      config.toml or point data_dir at the correct directory. Exiting.
```

This check applies regardless of the persisted phase. There is no graceful
degradation for a node_id mismatch — it is always an operator error.

#### tokens / NodeSize coherence

For every phase **except `IDLE`, `PAUSED`, and `FAILED`**, the number of tokens
persisted in `state.toml` must equal the expected token count for the configured
`NodeSize`:

```
NodeSize(config.node_size).token_count == len(NodeState.tokens)
```

`NodeSize` is a `StrEnum` whose variants each carry a `token_count` class
attribute (defined by proposal 002). `config.node_size` is the `[node].size`
field from `config.toml`. No helper function is needed — the check is a direct
attribute read: `NodeSize(config.node_size).token_count`.

If the counts diverge and the current phase is `JOINING`, `READY`, or
`DRAINING`, the node transitions to `FAILED` (increments `seq`, writes state
atomically, then continues as a `FAILED` node — peer server only, no gossip
bootstrap, WARNING logged).

`IDLE` is exempt because a node that has never joined has zero tokens. `PAUSED`
and `FAILED` are exempt because they are already degraded states; applying a
further transition would obscure the original failure cause. For those two
phases the check is skipped and the node starts inert as described in the phase
table.

```
ERROR [tourillon.core.lifecycle.bootstrap] tokens/size mismatch for node 'node-2':
      NodeSize 'medium' expects 16 tokens but state has 8. Transitioning to FAILED.
WARNING [tourillon.infra.cli.node] Node 'node-2' is in phase 'failed'; recovery
        behaviour is not covered by this proposal.
```

---

### Seeded join sequence — `IDLE → JOINING`

Precondition: persisted phase is `IDLE`; at least one seed address is available
(from `config.toml` or the `--seeds` flag of `tourctl node join`). The peer
server is already bound and serving.

`tourctl` connects **directly** to the target node's bind address. There is
no forwarding — the operator always specifies the exact peer address of the
node to join. The handler always handles the request on the node that received
it.

#### Local handling

```
1. Guard: phase == IDLE.
   If phase != IDLE → respond node.join.error code: wrong_phase.
2. Resolve seeds: envelope.seeds if non-empty, else config.seeds.
   If empty → respond node.join.error code: no_seeds.
3. Generate tokens: [secrets.randbelow(2**bits) for _ in range(token_count)].
4. Increment generation (exactly once for this join lifecycle).
5. Increment seq (phase transition).
6. Build NodeState(phase=JOINING, generation=N, seq=S, tokens=T, epoch unchanged).
7. Call StatePort.save(state) — atomic write + fsync before any response is sent.
8. Respond node.join.ok to tourctl. Join request is acknowledged.
9. Launch gossip bootstrap as a background task inside the running TaskGroup.
```

**Generation rule:** `Member.generation` is incremented exactly once at step 4,
at the moment `IDLE → JOINING` is triggered by `node.join`. On crash-restart
with persisted phase `JOINING`, the generation is reused unchanged — it is never
re-incremented on retry or restart. This mirrors the rule stated in proposal 002
for the first-node path.

**Crash-restart with phase `JOINING`:** the daemon skips steps 1–8 entirely and
proceeds directly to step 9 (gossip bootstrap), reusing the persisted generation
and tokens.

### Gossip bootstrap — full-resync

The gossip bootstrap always sends an empty `gossip.digest` (`members: []`) to
every seed, regardless of whether a local registry exists. This unconditional
full-resync prevents a crashed node from propagating stale entries it held before
the crash.

```
Bootstrap attempt (single pass across all seeds — seeds contacted concurrently):

  seeds_ok = 0

  async with asyncio.TaskGroup() as tg:
      for seed_address in seeds:
          tg.create_task(_bootstrap_from_seed(seed_address))

  async def _bootstrap_from_seed(seed_address: str) -> None:
      nonlocal seeds_ok
      try:
          client = TcpClient(ssl_ctx=ssl_ctx)   # not pooled
          async with asyncio.timeout(connect_timeout):
              await client.connect(seed_address)
          await client.stream(gossip.digest, members=[])
          async for delta in ...:               # gossip.delta pages
              for member in delta.members:
                  if member.partition_shift != local_partition_shift:
                      logger.error(
                          "Bootstrap seed %s returned member %r with "
                          "partition_shift=%d; local partition_shift=%d. "
                          "Cluster is incompatible with this node's config.",
                          seed_address, member.node_id,
                          member.partition_shift, local_partition_shift,
                      )
                      raise BootstrapPartitionShiftError(
                          seed_address, member.partition_shift, local_partition_shift
                      )
              await topology_manager.merge_registry(delta.members)
              if not delta.has_more:
                  break
          seeds_ok += 1
      except BootstrapPartitionShiftError:
          raise   # propagate immediately — do not retry
      except (OSError, TimeoutError, ResponseTimeoutError) as exc:
          logger.warning("Seed %s unreachable: %s.", seed_address, exc)
      finally:
          await client.close()                   # always closed, success or failure

  if seeds_ok == 0:
      raise BootstrapAttemptError("no seed responded")
  logger.info(
      "Gossip bootstrap complete. seeds_ok=%d seeds_err=%d",
      seeds_ok, len(seeds) - seeds_ok,
  )
```

`BootstrapPartitionShiftError` is a distinct exception that propagates out of
the `TaskGroup` immediately — it is never retried. The retry loop in
`GossipBootstrapper.run` re-raises it as-is without applying backoff. The daemon
startup coroutine catches it, performs the same clean shutdown sequence
(`PeerClientPool`, TLS context, `pid.lock`), logs an ERROR identifying both the
local and seed `partition_shift`, and calls `sys.exit(1)`.

The partition_shift check runs **before** `merge_registry` on each delta page so
that no incompatible member is ever written to the local registry.

### Gossip bootstrap backoff and retry

When a bootstrap attempt fails (all seeds unreachable), the daemon schedules a
retry with exponential backoff. This tolerates transient network partitions,
rolling restarts of seed nodes, and TLS certificate propagation delays.

```
Retry loop (GossipBootstrapper.run):

  delay = config.initial_delay_s
  for attempt in range(1, config.max_retries + 1):   # or infinity if max_retries == 0
      try:
          await _attempt(seeds)
          return   # success
      except BootstrapPartitionShiftError:
          raise   # never retried — incompatible cluster, exit immediately
      except BootstrapAttemptError:
          if config.max_retries > 0 and attempt >= config.max_retries:
              raise BootstrapError(f"no seed responded after {attempt} attempts")
          jittered = delay * (1 + random.uniform(-config.jitter, config.jitter))
          logger.warning(
              "Gossip bootstrap attempt %d/%s failed; retrying in %.1fs.",
              attempt,
              config.max_retries or "∞",
              jittered,
          )
          await asyncio.sleep(jittered)
          delay = min(delay * config.multiplier, config.max_delay_s)
```

`BootstrapError` (raised after all retries are exhausted) propagates to the
daemon startup coroutine, which performs a clean shutdown sequence — closes
`PeerClientPool`, releases the TLS context, and releases `pid.lock` — then logs
an ERROR and calls `sys.exit(1)`. Individual bootstrap connections are already
closed by the `finally` block in each seed task before the error propagates.
The `BootstrapAttemptError` is internal to `GossipBootstrapper` and is never
raised to callers directly.

Default `GossipBootstrapConfig` parameters cover a window of approximately three
minutes (1 s → 2 s → 4 s → … → 60 s, ten attempts), long enough for most
rolling restarts to complete while still failing loudly for genuine outages.

Both `JOINING` and `DRAINING` restarts use the same retry loop and the same
config parameters. There is no distinction between the two in the bootstrap
layer.

---

### Data model

#### `Member` — the gossiped record (existing)

Gossip propagates only `Member` records defined in
`tourillon/core/lifecycle/member.py`. No new wire structure is introduced for
members.

```python
@dataclass(frozen=True)
class Member:
    node_id:         str
    peer_address:    str
    generation:      int
    seq:             int             # Member.seq — the sole sequence counter
    phase:           MemberPhase     # IDLE | JOINING | READY | PAUSED | DRAINING | FAILED
    tokens:          tuple[int, ...]
    partition_shift: int             # cluster-wide hash-space shift; must be identical on all nodes
```

`partition_shift` is the base-2 logarithm of the number of partitions in the
consistent-hashing ring (i.e. `num_partitions = 2 ** partition_shift`). It is
read from `[cluster].partition_shift` in `config.toml` at startup and included
in every gossiped `Member` record so that every receiving node can verify
cluster-wide coherence. A node that receives Members with a mismatched
`partition_shift` sends `gossip.error code: partition_shift_mismatch` (as
responder) or closes the connection and logs WARNING (as initiator) — it does
**not** transition to FAILED itself. Only the node that receives
`gossip.error code: partition_shift_mismatch` in response to its own emitted
data transitions to FAILED (see *partition_shift mismatch* in Error paths and
Core invariants).

`seq` is the node's sole sequence counter. It is persisted as `NodeState.seq`
in `state.toml` (section `[node]`). There is no separate `[gossip]` section —
a single `seq` field exists, it belongs to `Member`, and it is written atomically
with `phase` in every `FileStateAdapter.save()` call.

`MemberPhase` is **self-declared**: each node publishes its own phase.
`MemberState` (LIVE / SUSPECT / UNKNOWN) is produced locally by `ProbeManager`
from `FailureDetector.phi()` and is never gossiped.

#### Record versioning

Merge order for two records of the same `node_id` is determined by
`Member.supersedes()` (already implemented):

```
(generation_A, seq_A) > (generation_B, seq_B)   # lexicographic
```

A higher `generation` always wins over any `seq`. Wall-clock time plays no role.
This rule is the sole merge rule and is already implemented in
`MemberRegistry.upsert()`.

#### `MemberDigestEntry` — compact version-vector entry

Used only inside `gossip.digest` envelopes. Never persisted or sent alone.

```python
@dataclass(frozen=True)
class MemberDigestEntry:
    """Compact version summary for anti-entropy version vector."""
    node_id:    str
    generation: int
    seq:        int
```

Wire size with msgpack: ~25–30 bytes per entry. 10 000 entries ≈ 300 KB —
acceptable for the occasional anti-entropy exchange, never sent on every cycle.

#### `topology.epoch` — ring version

`topology.epoch` is a monotone integer incremented by `TopologyManager`
**only when the ring is mutated**:

| Transition | Ring mutated? | Epoch++ ? |
|---|:---:|:---:|
| `JOINING → READY` | ✓ `add_vnodes` | ✓ |
| `DRAINING → IDLE` | ✓ `drop_nodes` | ✓ |
| Any other phase transition | ✗ | ✗ |

Epoch equals the ring version, not the cluster state version. Member-state
divergence (phase, seq differences without a ring mutation) is captured by the
fingerprint, not by epoch.

#### Fingerprint — lazy cache in `TopologyManager`

A SHA-256 digest of the member registry, computed lazily and cached. Invalidated
on every accepted mutation (`_apply()` returns `True`). Recomputed only at the
next `member_fingerprint()` call.

The fingerprint **does not belong to `Topology`**. A snapshot frozen at instant T
would carry a stale fingerprint the moment `TopologyManager` accepts the next
member. `GossipEngine` always calls `TopologyManager.member_fingerprint()` immediately
before building a `gossip.ping` payload, never reading it from a snapshot.

Algorithm:
```
for each member, sorted alphabetically by node_id:
    sha256.update(node_id_utf8 + uint64_be(generation) + uint64_be(seq))
```

`epoch` is excluded intentionally: mixing epoch and member state into a single
hash would cause every ring mutation to trigger a full member sync even when no
member record changed.

---

### Core invariants

| Invariant | Rule |
|---|---|
| Explicit join command | A node with seeds configured never initiates `IDLE → JOINING` autonomously. It always waits for `tourctl node join <node-id>`. |
| Single-hop forwarding | A contact node that receives `node.join` for a different `target_node_id` forwards the envelope to the target's `peer_address` and relays the response. It never re-forwards. |
| Peer server always bound | The peer server is bound for every phase so the node can receive operator commands and forwarded requests at any time. |
| KV server phase guard | The KV server is bound only when `phase ∈ {READY, DRAINING}`. Never opened for IDLE, JOINING, PAUSED, or FAILED. |
| Self-declared phase | `MemberPhase` is published by the node that owns it. No external actor may override another node's phase. |
| `MemberState` is local | LIVE / SUSPECT / UNKNOWN is never gossiped, never included in `Member`, never transmitted on the wire. |
| Merge by `supersedes()` | `generation` then `seq`, lexicographic. Wall-clock time never influences merge order. |
| `seq` is `Member.seq` | There is exactly one sequence counter per node. It is `Member.seq`, persisted in `NodeState.seq`. No separate gossip counter exists. |
| Fingerprint outside snapshot | `member_fingerprint()` is always read live from `TopologyManager`, never from a `Topology` snapshot. |
| `epoch` advances on ring mutation only | `IDLE → JOINING`, `READY → PAUSED`, `READY → DRAINING`, `JOINING → FAILED` do not advance epoch. |
| mTLS mandatory | All gossip and join connections enforce mutual TLS. No plaintext fallback. |
| `gossip.error` closes the connection | The responder closes after sending; the initiator closes on receipt. `PeerClientPool` detects `is_connected == False` and recreates the connection on the next `acquire()`. |
| Write-before-announce | `NodeState` (including incremented `seq`) is persisted via `StatePort.save()` before `GossipEngine.announce()` is called. |
| Bootstrap full-resync | A restarting node always sends an empty digest to every seed, regardless of whether a local registry exists. This prevents propagation of stale entries from a crashed registry. |
| JOINING restart — no re-increment | On crash-restart with `phase=JOINING`, the persisted `generation` and `tokens` are reused unchanged. Never re-incremented on retry or restart. |
| Backoff before BootstrapError | The daemon retries with exponential backoff up to `max_retries` before raising `BootstrapError` and exiting with code 1. |
| generation exactly once | `Member.generation` is incremented exactly once per join lifecycle, at `IDLE → JOINING` (triggered by `node.join`). |
| No `GossipEngine` for first node | A node that bootstrapped as first node (no seeds) skips the gossip bootstrap sequence and starts `GossipEngine` directly with its own `Member` record. |
| node_id consistency before start | If `state.toml` exists and `NodeState.node_id != config.node_id`, the process exits with code 1 before binding any socket. This check applies regardless of phase. |
| tokens/size coherence before start | For `JOINING`, `READY`, and `DRAINING` phases, `len(NodeState.tokens)` must equal `NodeSize(config.node_size).token_count`. Mismatch triggers an immediate `→ FAILED` transition (write-before-announce) before any server is bound. `IDLE`, `PAUSED`, and `FAILED` phases are exempt. |
| `partition_shift` cluster-wide uniform | Every `Member` record carries the node's configured `partition_shift`. A node that receives Members with a mismatched `partition_shift` (as responder to `gossip.push`, or as initiator receiving `gossip.delta`) sends `gossip.error code: partition_shift_mismatch` or closes the connection and logs WARNING. It does **not** transition to FAILED itself. Only the node that **receives** `gossip.error code: partition_shift_mismatch` in response to its own emitted data transitions to FAILED via the standard write-before-announce → announce → stop pattern. |
| `partition_shift` check before `merge_registry` in bootstrap | During gossip bootstrap, `partition_shift` is validated on each delta member **before** `merge_registry` is called. A mismatch raises `BootstrapPartitionShiftError` immediately — no incompatible member is ever written to the local registry. |
| FAILED phase is inert | A node in FAILED phase does not apply any incoming gossip records to its registry. Received `gossip.push` and `gossip.delta` messages are silently ignored (no registry update, no re-propagation, no error response). The peer server remains bound for operator commands only. |

---

### Bandwidth strategy — three paths

#### Hot path — `gossip.push` (event-driven)

Triggered immediately on every local `seq` increment (phase transition, restart
with a new `peer_address`). Sends the changed `Member` records to K randomly
selected eligible peers. In steady state only rare phase changes trigger this
path; payload size is always a few hundred bytes.

#### Anti-entropy path — SHA-256 quick-check then version vector

Triggered periodically (`anti_entropy_interval`, default 30 s) toward one
randomly selected peer. Uses the SHA-256 fingerprint and current `epoch` as a
cheap divergence check before any version-vector exchange. In steady state the
response has `same = true` and the total cost is a few dozen bytes.

#### Bootstrap path — full-resync on startup

When phase is `JOINING` or `DRAINING` at startup, the node sends an empty
`gossip.digest` (`members: []`) to every seed in `config.toml`. This triggers
a full delta download from each responding seed. The local registry is then the
union of all received deltas, merged via `supersedes()`. Retried with
exponential backoff until at least one seed responds or `max_retries` is
exhausted (see Gossip bootstrap backoff and retry).

Bootstrap connections are direct `TcpClient` instances opened outside
`PeerClientPool` — the node does not yet know the seeds' `node_id`s, and the
pool is keyed by `node_id`. Each connection is closed after the digest/delta
exchange, whether it succeeds or not (`finally` block). Once `GossipEngine`
starts, seed connections are managed by the pool via their `node_id`s discovered
in the deltas.

If **no seed** responds after all retries, `BootstrapError` is raised. The
daemon performs a clean shutdown sequence — closes `PeerClientPool`, releases
the TLS context, and releases `pid.lock` — then logs an ERROR and calls
`sys.exit(1)`.

---

### Protocol wire — envelope kinds

#### Join domain — 3 kinds

| Kind | Direction | Purpose |
|---|---|---|
| `node.join` | `tourctl` → peer server | Trigger `IDLE → JOINING` and gossip bootstrap. |
| `node.join.ok` | peer server → `tourctl` | Join acknowledged; phase is now `joining`. |
| `node.join.error` | peer server → `tourctl` | Join rejected; carries `code` and `message`. |

##### `node.join`

```json
{
  "seeds": ["10.0.0.1:7701", "10.0.0.2:7701"]
}
```

`seeds` is optional: an absent or empty list means "use seeds from the target's
`config.toml`". At least one seed must be available from either source, or the
handler replies `node.join.error code: no_seeds`.

##### `node.join.ok`

```json
{ "node_id": "node-2", "phase": "joining" }
```

##### `node.join.error`

```json
{
  "code":    "wrong_phase",
  "message": "node 'node-2' is in phase 'joining'; already joining"
}
```

| `code` | Trigger |
|---|---|
| `wrong_phase` | Phase is not `IDLE` when `node.join` is received. |
| `no_seeds` | No seeds provided in the envelope and none configured in `config.toml`. |
| `internal_error` | Unexpected error during state persistence. |

#### Gossip domain — 7 kinds

The gossip domain uses **7 kinds** (3 initiator request kinds + 3 response kinds
+ 1 common error kind):

| Kind | Direction | API | Terminates when | Purpose |
|---|---|---|---|---|
| `gossip.push` | A → K peers | `request()` | one `gossip.push.ok` or `gossip.error` | Epidemic propagation of changed `Member` records |
| `gossip.push.ok` | B → A | _(response)_ | — | Confirms accepted count; triggers re-propagation if `accepted > 0` and `ttl > 1` |
| `gossip.ping` | A → 1 peer | `request()` | one `gossip.pong` or `gossip.error` | Divergence quick-check: epoch + fingerprint + member_count |
| `gossip.pong` | B → A | _(response)_ | — | `same: true` → done; `same: false` → open digest/delta |
| `gossip.digest` | A → B | `stream()` | last `gossip.delta` with `has_more: false` or `gossip.error` | One page of the version vector (empty list for bootstrap) |
| `gossip.delta` | B → A | _(stream item)_ | `has_more: false` | Full `Member` records where B is ahead; streamed on the same `correlation_id` |
| `gossip.error` | B → A | _(common response)_ | — | Application-level error for any gossip kind; carries rejected kind, code, and message; always closes the connection |

#### `gossip.push`

```json
{
  "sender":  "node-1",
  "ttl":     3,
  "members": [
    {
      "node_id":         "node-1",
      "peer_address":    "10.0.0.1:7701",
      "phase":           "ready",
      "generation":      1,
      "seq":             146,
      "tokens":          [1024, 5120, 9216],
      "partition_shift": 12
    }
  ]
}
```

If records exceed `max_payload_bytes`, the sender emits multiple `gossip.push`
envelopes with distinct `correlation_id`s, each awaiting its own `gossip.push.ok`.

Re-propagation stops when `accepted == 0` (the peer already had all versions)
even if `ttl > 1`.

#### `gossip.push.ok`

```json
{ "sender": "node-2", "accepted": 1, "ignored": 0 }
```

#### `gossip.ping`

```json
{
  "sender":       "node-1",
  "epoch":        4,
  "fingerprint":  "a3f4c2b1d5e6f78900112233445566778899aabbccddeeff0011223344556677",
  "member_count": 10000
}
```

#### `gossip.pong`

```json
{
  "sender":       "node-2",
  "epoch":        4,
  "fingerprint":  "a3f4c2b1d5e6f78900112233445566778899aabbccddeeff0011223344556677",
  "member_count": 10000,
  "same":         true
}
```

`same = false` when fingerprints differ, epochs differ, or member counts differ.
The initiator then opens the digest/delta exchange.

#### `gossip.digest`

```json
{
  "sender":        "node-1",
  "has_more":      true,
  "after_node_id": "node-m",
  "members": [
    { "node_id": "node-a", "generation": 1, "seq": 146 },
    { "node_id": "node-m", "generation": 2, "seq": 3   }
  ]
}
```

Entries are sorted alphabetically by `node_id` so every member has an equal
chance of inclusion in each page. Pagination uses `after_node_id` as a cursor;
successive pages are sent as new `stream()` calls after the previous one
terminates with `has_more: false`.

An empty `members` list (`[]`) is valid and used exclusively during bootstrap.

#### `gossip.delta`

```json
{
  "sender":   "node-2",
  "has_more": false,
  "members": [
    {
      "node_id":         "node-3",
      "peer_address":    "10.0.0.3:7701",
      "phase":           "ready",
      "generation":      1,
      "seq":             92,
      "tokens":          [2048, 6144],
      "partition_shift": 12
    }
  ]
}
```

#### `gossip.error`

```json
{
  "sender":        "node-2",
  "rejected_kind": "gossip.push",
  "code":          "payload_too_large",
  "message":       "members count 8192 exceeds limit 4096"
}
```

| `code` | Trigger |
|---|---|
| `payload_too_large` | Member or digest count exceeds configured threshold |
| `rate_limited` | Sender exceeds `max_gossip_per_peer_rps` |
| `invalid_member` | `generation < 0` or `seq < 0` in a received `Member` |
| `partition_shift_mismatch` | A received `Member` carries a `partition_shift` that differs from the local node's configured value |
| `unknown_kind` | Gossip kind not recognized by the responder |
| `internal_error` | Unexpected responder error (e.g. topology not yet initialized) |

---

### Fan-out strategy

K eligible peers (`READY`, `JOINING`, `DRAINING`) selected via
`random.sample(peers, min(K, len(peers)))`.

K = `min(ceil(log2(N)) + 1, max_fan_out)`, configurable, default 6.
For N = 10 000: K = 6. The `min(K, len(peers))` guard prevents `ValueError`
when fewer than K eligible peers exist.

`FAILED`, `PAUSED`, and `IDLE` nodes are never selected as gossip targets and
never emit gossip themselves.

**`DRAINING → IDLE` special case:** the departing node emits one final
`gossip.push` at the standard TTL to announce its `IDLE` phase, then closes its
gossip port and stops `GossipEngine`. Re-propagation is performed by the
receiving peers — the fact that the sender stops immediately does not impede
cluster-wide diffusion.

---

### `PeerClientPool` — shared mTLS connection pool

`PeerClientPool` resides in `tourillon/core/transport/` alongside `TcpClient`.
It is shared across gossip, rebalance, and replication; there is no
`infra/transport/` package.

It maintains one `TcpClient` per `node_id`. On unexpected disconnection
(`is_connected == False`) the stale client is closed and a fresh one is created
at the next `acquire()`. At most one active `TcpClient` exists per `node_id`:
concurrent `acquire()` callers for the same `node_id` serialise on a per-node
`asyncio.Lock`.

On connection failure the exception propagates to the caller. The pool never
masks network errors.

---

### `GossipEngine` — responsibilities

`GossipEngine` is the **sole component authorised to emit gossip envelopes**.

It reads topology via `TopologyManager.snapshot()` and writes received records
via `TopologyManager.apply_member()` (single record) or
`TopologyManager.merge_registry()` (batch from a `gossip.delta`). It holds no
member registry of its own — all state lives in `TopologyManager`. All outgoing
connections use `PeerClientPool`.

Before passing any received `Member` to `TopologyManager`, the gossip handler
checks `member.partition_shift == local_partition_shift`. The action taken
depends on **which role the local node plays** at the moment of detection:

#### As responder — Members received in `gossip.push`

The remote node (initiator) sent its own Member records as payload. The local
node detects the mismatch, responds with `gossip.error code: partition_shift_mismatch`,
and closes the connection. **The local node does not transition to FAILED.** The
mismatch is logged as WARNING. The remote initiator receives the `gossip.error`
in response to its own push — that triggers its FAILED transition (see below).

#### As initiator — Members received in `gossip.delta`

The local node initiated the digest exchange and the remote responder sent back
delta records. If the local node detects a mismatch in the received delta it
closes the connection and logs WARNING. **The local node does not transition to
FAILED** — it cannot know whether it or the remote node is misconfigured from a
delta alone. The mismatch will eventually be resolved when the misconfigured node
pushes its data and receives a `partition_shift_mismatch` error in response.

#### On receiving `gossip.error code: partition_shift_mismatch`

This is the **only trigger** for a `partition_shift`-related FAILED transition.
Receiving this error code means a peer explicitly rejected data the local node
emitted — the local node's `partition_shift` was rejected by the cluster. The
local node executes the standard critical-transition pattern:

1. Increments its own `seq`.
2. Writes `phase=FAILED` to `StatePort` (write-before-announce invariant).
3. Calls `GossipEngine.announce(failed_member)` — enqueues the FAILED record on
   `hot_queue` exactly like any other phase transition.
4. Calls `GossipEngine.stop()` — which drains `hot_queue` fully before
   cancelling both loops (see `stop()` contract below). The FAILED announcement
   is therefore guaranteed to be sent before the engine shuts down.

This containment model prevents cascade failures: a single misconfigured node
emitting wrong `partition_shift` values cannot cause any receiver to go FAILED.
Only the node whose data is **explicitly rejected** by peers transitions to
FAILED. This is the **same write-before-announce → announce → stop pattern**
used for all hot-path transitions; no special-case logic exists at any call site.

From step 2 onwards the node is in FAILED phase. Any gossip records that arrive
concurrently are **silently ignored and not applied** — FAILED is always inert:
once a node is FAILED it does not update its registry in response to received
messages. Only operator commands (via the peer server) can change its state, and
that path is out of scope for this proposal.

Two independent loops run inside an `asyncio.TaskGroup`:
- `_hot_loop`: drains `hot_queue` and sends `gossip.push` to K peers. Calls
  `hot_queue.task_done()` after each item is processed (sent or intentionally
  dropped on send failure), enabling `stop()` to use `hot_queue.join()` as a
  reliable flush barrier.
- `_ae_loop`: fires every `anti_entropy_interval` seconds, runs ping/pong/digest/delta.

Internal state:
- `hot_queue: asyncio.Queue[Member]` — records awaiting immediate propagation.
- `seq: int` — the node's own `Member.seq`, read from `NodeState.seq` at startup.

---

### Integration with `TopologyManager` and rebalance

After `TopologyManager.apply_member()` returns `True` and the accepted member's
phase changed in a ring-mutating way (`JOINING → READY` or `DRAINING → IDLE`),
`TopologyManager` notifies `RebalanceApplicator.apply(new_plan)`. Both components
reside in `core/`; this coupling does not cross the `core/infra` boundary.

After a local phase transition, `RebalanceApplicator` calls
`engine.announce(updated_member)` for immediate push propagation.

The `JOINING → READY` transition (requiring partition data transfer and
coordinated rebalance) is **out of scope** for this proposal. A node that
completes gossip bootstrap remains in `JOINING` until the rebalance proposal is
implemented. `GossipEngine` starts its two loops regardless — a `JOINING` node
participates fully in gossip propagation — but its vnodes are not added to the
routing ring until `READY`.

`WaitGroup[T]` (defined in `tourillon/core/structure/`) coordinates topology
convergence across concurrent rebalance tasks.

---

### Error paths

| Condition | Behaviour |
|---|---|
| `state.toml` exists and `NodeState.node_id != config.node_id` | ERROR logged with both values; process exits with code 1. No socket is bound, no lock is held beyond the check. |
| `len(NodeState.tokens) != NodeSize(config.node_size).token_count` and phase ∈ {JOINING, READY, DRAINING} | ERROR logged; `seq` incremented; state written with `phase=FAILED`; startup continues as FAILED (peer server only, no gossip). |
| `len(NodeState.tokens) != NodeSize(config.node_size).token_count` and phase ∈ {IDLE, PAUSED, FAILED} | Check skipped; node starts inert in its current phase. |
| `partition_shift` mismatch detected **as responder** (Members received in `gossip.push`) | Sends `gossip.error code: partition_shift_mismatch`; closes the connection; logs WARNING. Local node does **not** transition to FAILED. |
| `partition_shift` mismatch detected **as initiator** (Members received in `gossip.delta`) | Closes the connection; logs WARNING. No `gossip.error` sent (initiator has no response slot on a stream). Local node does **not** transition to FAILED. |
| `partition_shift` mismatch detected **during bootstrap** (Members received in seed's `gossip.delta`) | Raises `BootstrapPartitionShiftError`; connection closed via `finally`; ERROR logged with both values; no registry write; never retried; daemon performs clean shutdown and calls `sys.exit(1)`. |
| `gossip.error code: partition_shift_mismatch` **received** (in response to an emitted `gossip.push`) | The local node's data was rejected — local `partition_shift` is wrong. Applies write-before-announce → `announce(failed_member)` → `stop()` (drains `hot_queue`, then cancels both loops). All subsequent incoming gossip records are silently ignored (FAILED is inert). |
| `node.join` received with wrong phase | Responds `node.join.error code: wrong_phase`; connection closed normally; no state change. |
| `node.join` received with no seeds | Responds `node.join.error code: no_seeds`; no state change. |
| Seed unreachable during bootstrap attempt | Logged as WARNING; other seeds tried in the same attempt. |
| All seeds fail in one attempt → `BootstrapAttemptError` | Retry with exponential backoff. |
| `max_retries` exhausted → `BootstrapError` | ERROR logged; daemon exits 1. |
| `gossip.error` received | Connection closed by both sides. `PeerClientPool` recreates on next `acquire()`. |
| `gossip.push` payload too large | Sender splits into multiple envelopes with distinct `correlation_id`s. |
| `gossip.digest` page too large | Sender paginates via `after_node_id` cursor. |
| Stale record (`supersedes()` → False) | Silently ignored by `MemberRegistry.upsert()`. No error emitted. |
| No eligible peers | Both loops idle. One `logger.debug` per cycle maximum. |
| `RESPONSE_TIMEOUT` on outgoing call | `ResponseTimeoutError` raised; caller logs WARNING and skips this peer for this cycle. |
| PAUSED or FAILED at startup | Peer server bound; no gossip bootstrap; daemon waits for operator command. |

---

## Design decisions

### Decision: Explicit join command — no auto-join on startup

**Alternatives considered:** auto-join on first startup when seeds are configured
(as many distributed databases do).

**Chosen because:** auto-join is dangerous in a leaderless cluster. A daemon
restarting after a crash or misconfiguration should never automatically mutate
cluster membership without operator intent. The `tourctl node join <peer-address>`
command makes the join an explicit, auditable operator action with a clear paper
trail in the log. It also separates "daemon is running and ready to receive
commands" from "operator has decided to join this node to the cluster", making
the lifecycle observable and controllable at every step.

### Decision: `tourctl node join` connects directly to the target — no forwarding

**Alternatives considered:** tourctl connects to any known live peer (contact),
which then forwards the `node.join` to the actual target using a registry
lookup (single-hop forwarding).

**Chosen because:** forwarding requires the contact to already have a gossip
record for the target. A brand-new node that has never joined the cluster has
no record in any peer's registry, making forwarding impossible for the primary
use case. Requiring the operator to specify the target's peer address directly
is explicit, unambiguous, and always works regardless of the node's history. The
operator deploying a new node always knows its bind address. The TLS credentials
for the mTLS connection are still drawn from the active context, keeping
credential management centralised.

### Decision: Peer server bound for every phase, KV server phase-guarded

**Alternatives considered:** bind both servers only when READY; use in-process
queues for pre-READY commands; restrict tourctl to out-of-band channels.

**Chosen because:** the peer server must be available at `IDLE` for `tourctl` to
deliver the `node.join` command, at `PAUSED` and `FAILED` for operator commands,
and at `JOINING` while gossip is in progress. Binding it unconditionally removes
all "bind-if" branching from the startup path and makes the invariant trivially
verifiable. The KV guard is enforced at the server binding level — misbehaving
clients can never reach KV handlers when phase is wrong.

### Decision: Exponential backoff with configurable max retries

**Alternatives considered:** exit immediately on first bootstrap failure; retry
indefinitely without backoff.

**Chosen because:** exiting immediately makes the daemon fragile during rolling
restarts and transient network events. Unlimited retries without backoff can
flood seed nodes and make the daemon silently stuck. Exponential backoff with a
configurable cap gives the operator control over the resilience/fail-fast
trade-off. The default (1 s → 60 s, ten attempts, ±10 % jitter, ~3-minute
window) covers most rolling restarts while failing loudly for genuine outages.
Setting `max_retries=0` allows unlimited retries for operators who prefer it.

### Decision: JOINING and DRAINING restarts both require gossip bootstrap

**Alternatives considered:** trust the persisted registry on restart; skip
bootstrap for DRAINING since it was recently READY.

**Chosen because:** a crashed node cannot know what changed in the cluster during
its downtime. Always performing a full-resync from seeds guarantees a current
view before the node participates in any protocol. The one-shot bootstrap cost
is bounded and occurs only at startup.

### Decision: Three-path bandwidth strategy

**Alternatives considered:** pure periodic anti-entropy only; pure push with TTL
only; per-change broadcast.

**Chosen because:** pure periodic anti-entropy has high latency for urgent changes
(phase transitions that must trigger rebalance). Pure push with TTL causes
O(N²) traffic in steady state. Per-change broadcast does not scale beyond a few
hundred nodes. The three-path design gives event-driven speed for urgent changes,
low steady-state cost via fingerprint quick-check, and guaranteed eventual
convergence via the full version-vector exchange.

### Decision: Fingerprint outside `Topology` snapshot

**Alternatives considered:** store `fingerprint` as a field of `Topology`.

**Chosen because:** `Topology` is a frozen snapshot. A fingerprint computed at
snapshot time would be stale by the time `GossipEngine` sends it across an
`await` boundary. Always reading `member_fingerprint()` live from `TopologyManager`
under its lock guarantees freshness.

### Decision: Bootstrap full-resync (always send empty digest)

**Alternatives considered:** differential resync on restart (send existing local
registry in the digest, receive only what changed).

**Chosen because:** eliminating the differential case removes a whole class of
bugs where a crashed node propagates stale entries it had before the crash. The
extra cost is a one-shot full-registry download per seed, which is bounded and
occurs only at startup.

### Decision: node_id mismatch is a fatal startup error

**Alternatives considered:** log a warning and continue with the state file's
node_id; rename the mismatched state file and create a fresh one.

**Chosen because:** a state file that was written for a different node contains
wrong tokens and generation numbers. Loading it would silently corrupt the ring
with ghost vnodes belonging to neither the configured node nor the actual owner.
There is no safe automatic recovery — an operator must inspect the filesystem to
understand what happened. Exiting immediately with a clear error message and both
node_id values gives the operator the context they need without hiding the
problem.

### Decision: tokens/size mismatch → FAILED (not exit) for active phases

**Alternatives considered:** exit with code 1; log a warning and continue.

**Chosen because:** JOINING, READY, and DRAINING nodes may hold live data or be
part of active rebalance operations. Exiting abruptly risks leaving the cluster
in an inconsistent rebalance state. Transitioning to FAILED keeps the peer
server reachable for operator commands and gossip while clearly signalling that
the node cannot safely serve traffic. PAUSED and FAILED are exempt because they
are already degraded states where the token integrity invariant may not hold for
legitimate historical reasons (e.g. a configuration change applied after a pause
that is not yet ratified).

### Decision: partition_shift carried in every Member record

**Alternatives considered:** validate partition_shift only during bootstrap from
a separate cluster-info envelope; use a separate `ring.schema` gossip kind.

**Chosen because:** embedding `partition_shift` directly in `Member` leverages
the existing gossip propagation path without any new envelope kind. Every node
already receives every member record; adding one integer field costs ~2 bytes per
entry in msgpack. The validation happens naturally at the point of ingestion,
covers both push and delta paths, and triggers the FAILED transition before any
ring computation can proceed with the conflicting value. The value is read once
from `[cluster].partition_shift` in `config.toml` at startup and treated as a
runtime constant.

---

## Interfaces (informative)

```python
# tourillon/core/lifecycle/member.py  (updated)
@dataclass(frozen=True)
class Member:
    """Gossiped membership record for a single node.

    partition_shift is a cluster-wide constant: all nodes must declare the same
    value. A receiving node that observes a mismatch transitions itself to FAILED.
    """

    node_id:         str
    peer_address:    str
    generation:      int
    seq:             int
    phase:           MemberPhase
    tokens:          tuple[int, ...]
    partition_shift: int


# tourillon/core/lifecycle/checks.py  (new)
def check_node_id_consistency(config_node_id: str, state_node_id: str) -> None:
    """Raise NodeIdMismatchError if config and state node_ids differ.

    Called during startup before any socket is bound. Never returns normally
    on mismatch — the caller must exit with code 1.
    """
    ...


def check_tokens_coherence(
    phase: MemberPhase,
    tokens: tuple[int, ...],
    node_size: NodeSize,
) -> bool:
    """Return True when the tokens/size invariant is satisfied for the phase.

    Returns False (caller must transition to FAILED) when all of the following
    hold: phase ∈ {JOINING, READY, DRAINING} and len(tokens) != node_size.token_count.
    Always returns True for IDLE, PAUSED, and FAILED phases (exempt).
    """
    ...


class NodeIdMismatchError(Exception):
    """Raised by check_node_id_consistency when config and state node_ids differ."""


```
# tourillon/core/gossip/config.py
@dataclass(frozen=True)
class GossipBootstrapConfig:
    """Exponential backoff parameters for the gossip bootstrap retry loop."""

    initial_delay_s: float = 1.0    # delay before first retry
    max_delay_s:     float = 60.0   # ceiling for exponential growth
    multiplier:      float = 2.0    # doubling factor per attempt
    jitter:          float = 0.1    # ±10 % uniform jitter to avoid thundering herd
    max_retries:     int   = 10     # 0 = unlimited retries


@dataclass(frozen=True)
class GossipConfig:
    """Full configuration for the GossipEngine."""

    bootstrap:               GossipBootstrapConfig = field(default_factory=GossipBootstrapConfig)
    anti_entropy_interval:   float = 30.0    # seconds between AE cycles
    max_fan_out:             int   = 6       # ceil(log2(N)) + 1, capped here
    max_payload_bytes:       int   = 1_048_576  # 1 MiB per gossip.push envelope
    max_digest_entries:      int   = 4_096   # entries per gossip.digest page
    max_gossip_per_peer_rps: float = 20.0    # rate limit per peer


# tourillon/core/gossip/bootstrapper.py
class BootstrapAttemptError(Exception):
    """Raised when a single bootstrap attempt fails (all seeds unreachable)."""


class BootstrapPartitionShiftError(Exception):
    """Raised when a seed delta contains a Member with a mismatched partition_shift.

    Never retried. Propagates immediately out of GossipBootstrapper.run so the
    daemon can exit with a clear diagnostic rather than exhausting retries.
    """


class BootstrapError(Exception):
    """Raised when bootstrap fails after exhausting all retries."""


class GossipBootstrapper:
    """Encapsulates the gossip bootstrap retry loop.

    Attempts full-resync from each seed address. Retries on total failure using
    GossipBootstrapConfig parameters. Raises BootstrapError after max_retries.
    """

    def __init__(
        self,
        topology_manager: TopologyManager,
        config: GossipBootstrapConfig,
        ssl_ctx: ssl.SSLContext,
    ) -> None: ...

    async def run(self, seeds: list[str]) -> int:
        """Run the bootstrap retry loop.

        Return seeds_ok count on success. Raise BootstrapError when
        max_retries is exhausted without any seed responding.
        """
        ...


# tourillon/core/handlers/node_join.py
class NodeJoinHandler:
    """Peer-server handler for the node.join envelope kind.

    tourctl connects directly to the target node's peer server address;
    there is no forwarding or registry lookup. The handler always handles
    the request on the node that received it.

    Guards that phase == IDLE, executes the IDLE → JOINING transition
    atomically (StatePort.save() before responding), then enqueues gossip
    bootstrap as a background task. Responds node.join.ok on success;
    node.join.error otherwise. Never mutates state if any guard fails.
    """

    async def handle(
        self,
        envelope: Envelope,
        seeds_override: list[str],
    ) -> None: ...


@dataclass(frozen=True)
class MemberDigestEntry:
    """Compact version summary for anti-entropy version vector."""
    node_id:    str
    generation: int
    seq:        int


class GossipEngine:
    """Anti-entropic gossip engine driving cluster membership convergence.

    Two independent loops run inside an asyncio.TaskGroup:
    - _hot_loop: drains hot_queue and sends gossip.push to K peers.
    - _ae_loop:  fires every anti_entropy_interval seconds; runs ping → pong →
                 digest → delta if divergence detected.

    All member state is delegated to TopologyManager. Internal state is limited
    to hot_queue and the node's own seq counter.
    """

    def __init__(
        self,
        node_id: str,
        topology_manager: TopologyManager,
        pool: PeerClientPool,
        state_port: StatePort,
        config: GossipConfig,
    ) -> None: ...

    async def start(self) -> None:
        """Start both loops in a TaskGroup. Blocks until stop() is called."""

    async def stop(self) -> None:
        """Drain hot_queue then cancel both loops and await clean shutdown.

        Draining the hot_queue before cancellation guarantees that any
        announce() call made before stop() — including critical phase-transition
        announcements such as FAILED — is fully sent to peers before the engine
        terminates. This contract makes the write-before-announce → announce →
        stop pattern safe and symmetric for all callers, with no special-case
        flush logic needed at the call site.

        Implementation contract: _hot_loop must call hot_queue.task_done() after
        each item has been sent (or after a send failure that the engine decides
        to swallow). stop() calls await hot_queue.join() before cancelling the
        TaskGroup, which blocks until all task_done() calls have been received.
        """

    async def announce(self, member: Member) -> None:
        """Enqueue member for immediate hot-path propagation.

        Called after every local phase transition. seq is already incremented
        and persisted before this call (write-before-announce invariant).
        """


# TopologyManager batch apply (informative — defined in proposal 002)
# Signature reproduced here for GossipEngine callers:
async def merge_registry(members: Iterable[Member]) -> int:
    """Apply each Member via _apply() and return the total count accepted.

    Return value mirrors MemberRegistry.upsert() semantics: only records
    that supersede the current version are counted. Used by the bootstrap
    path and the gossip.delta handler.
    """


class PeerClientPool:
    """Persistent TcpClient pool, one client per node_id.

    Shared across gossip, rebalance, and replication. Resides in
    tourillon/core/transport/ alongside TcpClient.
    """

    async def acquire(self, node_id: str, address: str) -> TcpClient:
        """Return the existing TcpClient for node_id, or create a new one.

        If the client is present and is_connected, return it immediately.
        Otherwise close the stale client (if any), create a fresh TcpClient,
        connect under asyncio.timeout(), and store it. Concurrency-safe: a
        per-node asyncio.Lock serialises concurrent callers for the same node_id.
        """

    async def release(self, node_id: str) -> None:
        """Close and remove the TcpClient for node_id.

        Called when a node completes DRAINING → IDLE to prevent a stale
        client from persisting in the pool.
        """

    async def close_all(self) -> None:
        """Close all TcpClients. Called at daemon shutdown."""


class WaitGroup[T]:
    """asyncio synchronisation primitive for coordinating N concurrent tasks.

    A cycle begins when add() is called with counter == 0.
    done(sub, success) decrements the counter and optionally records a result.
    wait() suspends until the counter reaches zero, then returns
    (success_list, failed_list). Results are reset at each new cycle.
    """

    async def add(self, n: int = 1) -> None: ...
    async def done(self, sub: T | None = None, success: bool = True) -> None: ...
    async def wait(self) -> tuple[list[T], list[T]]: ...


# gossip_stats shape (included in NodeInspectResponse)
gossip_stats = {
    "known_members":       int,
    "push_sent_total":     int,
    "push_recv_total":     int,
    "ae_cycles_total":     int,
    "ae_diverged":         int,
    "last_ae_peer":        str | None,
    "last_ae_at":          str | None,   # ISO 8601 UTC
    "bootstrap_ok_total":  int,
    "bootstrap_err_total": int,
}
```

---

## Proposed code organisation

```
tourillon/
  core/
    gossip/
      __init__.py
      config.py         # GossipBootstrapConfig, GossipConfig
      bootstrapper.py   # GossipBootstrapper, BootstrapAttemptError, BootstrapError
      engine.py         # GossipEngine — hot loop + AE loop
      digest.py         # MemberDigestEntry, pagination helpers
    handlers/
      gossip.py         # All gossip peer-server handlers:
                        #   gossip.push, gossip.ping, gossip.digest (responder side)
      node_join.py      # node.join handler: forwarding + IDLE → JOINING transition
    transport/
      pool.py           # PeerClientPool
    structure/
      waitgroup.py      # WaitGroup[T]
  infra/
    cli/
      node.py           # tourillon node start — startup dispatch by phase

tourctl/
  core/
    commands/
      node_join.py      # node join command logic
  infra/
    cli/
      node.py           # tourctl node join <node-id> Typer command
```

---

## Test scenarios

All scenarios run with in-memory adapters (fake `TcpClient`, fake
`TopologyManager`) unless marked `[e2e]`.

| # | Fixture | Action | Expected |
|---|---------|--------|----------|
| 1 | node-2 IDLE, seeds configured | `tourillon node start` | Peer server bound on 7701; KV server **not** bound; log "waiting for tourctl node join" |
| 2 | `tourctl` connects directly to node-2 at `10.0.0.2:7701`; node-2 IDLE | `tourctl node join 10.0.0.2:7701 --seeds 10.0.0.1:7701` | node-2 handles locally; `StatePort.save()` with `phase=joining`, `generation=1`; `node.join.ok` returned |
| 3 | node-2 JOINING; `node.join` received | `tourctl node join 10.0.0.2:7701` | `node.join.error code: wrong_phase`; no state change |
| 4 | node-2 IDLE; no seeds in config, no `--seeds` | `tourctl node join 10.0.0.2:7701` | `node.join.error code: no_seeds`; no state change |
| 5 | node-2 JOINING (crash-restart) | `tourillon node start` | Peer server bound; KV server **not** bound; gossip bootstrap starts immediately; generation **not** re-incremented; tokens unchanged |
| 6 | node-2 READY (crash-restart) | `tourillon node start` | Peer server + KV server bound; topology rebuilt from state; no `StatePort.save()`; `GossipEngine` started |
| 7 | node-2 DRAINING (crash-restart) | `tourillon node start` | Peer server + KV server bound; gossip bootstrap starts; phase remains `draining` |
| 8 | node-2 PAUSED | `tourillon node start` | Peer server bound; KV server **not** bound; no gossip bootstrap; log indicates behaviour out of scope |
| 9 | node-2 FAILED | `tourillon node start` | Peer server bound; KV server **not** bound; no gossip bootstrap; WARNING logged |
| 10 | node-2 in phase JOINING; `node.join` received | `tourctl node join node-2` | `node.join.error code: wrong_phase`; no state change |
| 11 | node-2 IDLE; no seeds in config, no `--seeds` | `tourctl node join node-2` | `node.join.error code: no_seeds`; no state change |
| 12 | Bootstrap: 3 seeds configured, 1 unreachable | `GossipBootstrapper.run()` | Unreachable seed logged as WARNING; 2 succeed; `seeds_ok=2`; no retry needed; returns 2 |
| 13 | Bootstrap: all 3 seeds unreachable, `max_retries=3` | `GossipBootstrapper.run()` | 3 attempts each logged as WARNING with increasing delay; `BootstrapError` raised after attempt 3; daemon exits 1 |
| 14 | Bootstrap: all seeds fail on attempt 1, 1 seed responds on attempt 2 | `GossipBootstrapper.run()` | Attempt 1 → WARNING + backoff sleep; attempt 2 → `seeds_ok=1`; success; no `BootstrapError` |
| 15 | Bootstrap backoff timing: `initial_delay_s=1.0`, `multiplier=2.0`, `max_retries=3` | Measure elapsed time between attempts | Delays ≈ 1 s (±10 %), 2 s (±10 %) before final failure; total elapsed ≈ 3 s |
| 16 | Bootstrap: seed has 5 members, joining node has empty registry | `GossipBootstrapper.run()` | Full delta received; `merge_registry()` applies 5 members; `known_members=5` |
| 17 | Bootstrap: node crashed with `seq=5`, seed has `seq=8` for same node | `GossipBootstrapper.run()` (always full-resync) | Stale record superseded; updated record applied; generation unchanged |
| 18 | node-1 READY, node-2 JOINING | node-1 transitions READY → DRAINING; announces via hot path | node-2 receives `gossip.push`; `merge_registry()` updates node-1 phase to DRAINING |
| 19 | node-1 `seq=50`; peer has `seq=200` for same `node_id` | node-1 pushes its own record | `supersedes()` → False → `accepted=0` → no re-propagation |
| 20 | node-1 `seq=200`; peer has `seq=50` | node-1 pushes its own record | `supersedes()` → True → `accepted=1` → re-propagation if `ttl > 1` |
| 21 | Two nodes, identical registry and epoch | Anti-entropy ping cycle fires | `gossip.pong` has `same=true`; no digest/delta exchange; total bytes < 200 |
| 22 | Two nodes, node-2 has stale member `seq=50` vs `seq=100` | Anti-entropy ping cycle fires | `gossip.pong` has `same=false`; digest/delta exchange; node-2 receives updated Member |
| 23 | Two nodes, epoch divergence (node-2 hasn't applied READY yet) | Anti-entropy ping cycle fires | `gossip.pong` has `same=false` (epoch differs); digest/delta exchange; both converge |
| 24 | node-1 sends `gossip.digest` with `has_more=true`; `after_node_id="node-m"` | node-2 processes first page | node-2 responds with delta; node-1 sends second page; exchange completes with `has_more=false` |
| 25 | `gossip.push` payload exceeds `max_payload_bytes` | node-1 pushes 500 members at once | Sender splits into multiple envelopes; each awaits its own `gossip.push.ok` |
| 26 | `gossip.digest` page exceeds `max_digest_entries` | Oversized page sent | Responder replies `gossip.error code: payload_too_large`; connection closed; pool recreates on next `acquire()` |
| 27 | Responder receives `Member` with `generation=-1` | Any gossip kind | Responder sends `gossip.error code: invalid_member`; connection closed |
| 28 | Rate limit exceeded: peer sends >20 rounds/s | Responder tracks rate per peer | Responder sends `gossip.error code: rate_limited`; connection closed |
| 29 | 0 eligible peers (all IDLE or FAILED) | Hot loop and AE loop tick | No outgoing envelopes; one `logger.debug` per cycle; no crash |
| 30 | `DRAINING → IDLE` transition | Node announces final IDLE phase | One `gossip.push` with TTL=3 emitted; `GossipEngine.stop()` called; port closed |
| 31 | `gossip.error` received mid-session | Hot-path or AE cycle in progress | Connection closed; `PeerClientPool` recreates client on next `acquire()`; next cycle resumes normally |
| 32 | `RESPONSE_TIMEOUT` on outgoing `gossip.ping` | `ResponseTimeoutError` raised | Logged as WARNING; this peer skipped for this AE cycle; next cycle unaffected |
| 33 | node-1 restarts with `seq=50`; peer has `seq=200` for node-1 | node-1 pushes `seq=50` | Peers reject (superseded); node-1 receives `seq=200` at next AE cycle; emits `seq=201` on next transition |
| 34 | `gossip_stats` fields | `GossipEngine` runs for N cycles | `push_sent_total`, `ae_cycles_total`, `ae_diverged` match actual activity |
| 35 `[e2e]` | Two-node cluster, real mTLS sockets | `tourctl node join node-2`; both nodes reach JOINING; gossip runs | Both nodes see each other via `tourctl node inspect`; KV server not bound (rebalance pending) |
| 36 | `state.toml` has `node_id="node-1"`, `config.toml` has `node_id="node-2"` | `tourillon node start` | Process logs ERROR with both ids; exits with code 1; no socket bound |
| 37 | `state.toml` has `node_id="node-2"`, `config.toml` has `node_id="node-2"` | `tourillon node start` | node_id check passes; startup continues normally |
| 38 | `config.node_size=medium` expects 16 tokens; `state.tokens` has 8 entries; `phase=READY` | `tourillon node start` | ERROR logged; `seq` incremented; state written with `phase=FAILED`; peer server bound; KV server NOT bound |
| 39 | `config.node_size=medium` expects 16 tokens; `state.tokens` has 8 entries; `phase=IDLE` | `tourillon node start` | Tokens/size check skipped; node starts normally in IDLE (awaiting `tourctl node join`) |
| 40 | `config.node_size=medium` expects 16 tokens; `state.tokens` has 8 entries; `phase=PAUSED` | `tourillon node start` | Tokens/size check skipped; node starts inert in PAUSED; peer server bound; no gossip |
| 41 | `config.node_size=medium` expects 16 tokens; `state.tokens` has 8 entries; `phase=FAILED` | `tourillon node start` | Tokens/size check skipped; node remains FAILED; peer server bound; WARNING logged |
| 42 | node-1 (`pshift=12`) receives `gossip.push` from node-2 (`pshift=10`) — node-1 is **responder** | node-1 gossip push handler | node-1 sends `gossip.error code: partition_shift_mismatch`; node-1 does **not** transition to FAILED; node-2 receives the error and transitions to FAILED via write-before-announce → announce → stop |
| 43 | Two nodes, identical `partition_shift=12` | Normal gossip cycle | No mismatch detected; gossip proceeds normally |
| 44 | node-1 (`pshift=12`) receives `gossip.delta` containing node-2's member (`pshift=10`) — node-1 is **initiator** of the digest | Anti-entropy AE cycle | node-1 closes connection; logs WARNING; node-1 does **not** transition to FAILED; node-2 eventually pushes and gets rejected → node-2 goes FAILED |
| 45 | node-2 (`pshift=10`) receives `gossip.error code: partition_shift_mismatch` in response to its own `gossip.push` | node-2 hot-path response handler | node-2 applies write-before-announce → `announce(failed_member)` → `stop()` (hot_queue drained); peers receive FAILED push for node-2 |
| 46 | Joining node (`pshift=10`) bootstraps against seed whose delta contains members with `pshift=12` | `GossipBootstrapper.run()` | `BootstrapPartitionShiftError` raised immediately; no registry write; no retry; ERROR logged with both pshift values; daemon exits 1 |
| 47 | Joining node (`pshift=12`) bootstraps against seed whose delta contains members with `pshift=12` | `GossipBootstrapper.run()` | No mismatch; delta applied normally; `seeds_ok=1`; bootstrap succeeds |

---

## Exit criteria

- [ ] All test scenarios pass.
- [ ] `uv run pytest -m gossip --cov-fail-under=90` passes.
- [ ] `uv run pre-commit run --all-files` passes.
- [ ] `tourctl node join <peer-address>` sends `node.join` directly to the target node's peer server; target persists `phase=joining`; gossip bootstrap runs asynchronously.
- [ ] No forwarding logic exists anywhere in the codebase for `node.join`.
- [ ] `tourillon node start` with seeds and `phase=IDLE` binds **only** the peer server and logs the waiting message.
- [ ] `tourillon node start` with `phase=JOINING` runs gossip bootstrap immediately without waiting for `tourctl`.
- [ ] `tourillon node start` with `phase=DRAINING` binds peer server + KV server and runs gossip bootstrap.
- [ ] `tourillon node start` with `phase=PAUSED` or `phase=FAILED` binds only the peer server; no gossip bootstrap.
- [ ] KV server is never bound for `IDLE`, `JOINING`, `PAUSED`, or `FAILED`.
- [ ] Bootstrap retries with exponential backoff up to `max_retries`; exits 1 after exhaustion.
- [ ] `BootstrapError` is never raised while retries remain.
- [ ] `Member.generation` incremented exactly once per join lifecycle at `node.join` receipt on the target; never on crash-restart or retry.
- [ ] `NodeInspectResponse` includes `gossip_stats`; `tourctl node inspect` renders the Gossip stats section.
- [ ] `gossip.error` always closes the connection on both sides.
- [ ] All gossip peer-server handlers live in `tourillon/core/handlers/gossip.py`; node join handler lives in `tourillon/core/handlers/node_join.py`.
- [ ] No `print()` or `Console.print()` calls anywhere in `tourillon/core/gossip/` or `tourillon/core/transport/`.
- [ ] `PeerClientPool` concurrent `acquire()` stress test: N coroutines requesting the same `node_id` simultaneously produce exactly one `TcpClient`.
- [ ] `tourillon node start` exits with code 1 when `state.toml` contains a `node_id` that differs from `config.node_id`; no socket is bound.
- [ ] `tourillon node start` transitions to `FAILED` when `len(NodeState.tokens) != NodeSize(config.node_size).token_count` for phases `JOINING`, `READY`, and `DRAINING`; the peer server is bound but the KV server is not.
- [ ] `tourillon node start` skips the tokens/size check for `IDLE`, `PAUSED`, and `FAILED` phases; those nodes start inert in their current phase without any state mutation.
- [ ] `Member.partition_shift` is present in every gossiped record (`gossip.push`, `gossip.delta`); any record missing this field is rejected as `invalid_member`.
- [ ] On `partition_shift` mismatch: a node acting as **responder** to `gossip.push` sends `gossip.error code: partition_shift_mismatch` and does NOT transition to FAILED; a node acting as **initiator** that detects mismatch in `gossip.delta` closes and logs WARNING without transitioning to FAILED; only the node that **receives** `gossip.error code: partition_shift_mismatch` in response to its own emitted data transitions to FAILED via write-before-announce → announce → stop.
- [ ] During **gossip bootstrap**, `partition_shift` is checked on each delta member before `merge_registry`; a mismatch raises `BootstrapPartitionShiftError` immediately (no retry, no registry write); ERROR is logged with both values; daemon exits 1 via the clean shutdown sequence.

---

## Out of scope

- **`JOINING → READY` transition** — requires partition data transfer coordinated
  by the rebalance protocol. A node completing gossip bootstrap remains in
  `JOINING` until the rebalance proposal is implemented and the transfer
  completes. This is the primary gap in the current proposal.
- **Hinted handoff** — covered by the KV proposal.
- **`FAILED` member expiration** — covered by a dedicated lifecycle proposal.
  `FAILED` members remain in the registry indefinitely until that proposal
  defines an explicit expiration policy.
- **Zone-biased fan-out** — `Member.labels` is not yet defined. The current
  `random.sample()` fan-out remains valid until a labels proposal is accepted.
- **`tourctl node resume`** — covered by the pause/resume proposal. The `PAUSED`
  phase is recognised by this proposal (peer server bound, no gossip bootstrap)
  but the resume command itself is not defined here.
- **`tourctl node leave` / `READY → DRAINING`** — covered by the leave proposal.
