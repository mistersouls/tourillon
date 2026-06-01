# Proposal: Pause & Resume

<!-- Naming: proposal-<short-desc>-MMDDYYYY-SEQ.md
     Example: proposal-pause-05312026-007.md     -->

**Author**: Tourillon Contributors <dev@tourillon.io>
**Status:** Draft
**Date:** 2026-05-31
**Sequence:** 007

---

## Summary

This proposal introduces `tourctl node pause <addr>` and `tourctl node resume <addr>`,
letting operators temporarily suspend a node's participation in data-plane operations
without taking it fully offline. The node's original lifecycle phase (`READY`, `DRAINING`,
or `JOINING`) is persisted in `state.toml` so that a `resume` command — or an automatic
recovery after a crash — always returns the node to exactly the right phase. `PAUSED` is
a first-class `MemberPhase` value that gossip propagates via the normal `gossip.push` path;
the `PlacementStrategy` detects it and routes writes to a handoff target without any
additional mechanism.
The peer-plane pause/resume handlers are expected to be registered by the `Bootstraper`
introduced in proposal 001, using the same `TourillonCore` facade that centralises the
state port, topology, probe manager, and listener wiring.

---

## Motivation

Operators need a way to silence a node during maintenance (rolling upgrades, disk
replacement, emergency patch application) without draining it. Draining is irreversible
within a session: once a node starts shedding partitions it cannot reclaim them without
a full re-join. Pause is reversible: all topology state is retained, and the operator
restores service with a single command. Without this capability, the only safe maintenance
path is drain + decommission + re-join, which causes unnecessary data movement and
rebalance traffic.

---

## CLI contract

### Pause a node

```
$ tourctl node pause 10.0.0.3:7100
Paused node 10.0.0.3:7100 (was ready).
```

### Resume a node

```
$ tourctl node resume 10.0.0.3:7100
Resumed node 10.0.0.3:7100 (returning to ready).
```

### Errors

```
# Node already paused
$ tourctl node pause 10.0.0.3:7100
Error: node is already paused (paused_from=ready).

# Resume on a non-paused node
$ tourctl node resume 10.0.0.3:7100
Error: node is not paused (current phase: ready).

# Drain while paused
$ tourctl node drain 10.0.0.3:7100
Error: node is paused; resume before draining.

# Pause from an ineligible phase (IDLE / FAILED)
$ tourctl node pause 10.0.0.3:7100
Error: cannot pause node in phase idle.
```

All errors are written to **stderr**; exit code is **1** on error, **0** on success.

The `--json` flag emits a machine-readable object on stdout:

```json
{"ok": true, "paused_from": "ready"}
{"ok": false, "error": "node is already paused (paused_from=ready)"}
```

---

## Design

### Data model

#### `MemberPhase.PAUSED`

`PAUSED` is defined in `tourillon/core/lifecycle/member.py` as `"paused"`. This
proposal wires up all transitions to and from it.

#### `NodeState.paused_from` (new field)

```python
# tourillon/core/lifecycle/state.py
@dataclass(frozen=True)
class NodeState:
    ...
    paused_from: MemberPhase | None = None  # set at pause; cleared at resume
```

`paused_from` is `None` when the node is not paused. It is set to the source phase
(`READY`, `DRAINING`, or `JOINING`) at the moment `node.pause` is applied, and cleared
back to `None` when `node.resume` is applied. It is persisted in the `[node]` section of
`state.toml` as the string `paused_from = "ready"` (or `"draining"` / `"joining"`); the
key is absent entirely when the node is not paused (never written as `paused_from = ""`).

#### `_parse_state` / `_encode_state` (updated)

`tourillon/infra/store/state.py` is updated:

- `_parse_state`: reads `node.get("paused_from")`, passes it through `MemberPhase(...)` if
  present, otherwise leaves it `None`.
- `_encode_state`: includes `"paused_from": state.paused_from.value` in the `[node]` dict
  only when `state.paused_from is not None`.

#### Envelope kinds

| Kind | Direction | Payload fields |
|---|---|---|
| `node.pause` | tourctl → peer | *(empty)* |
| `node.pause.response` | peer → tourctl | `ok: bool`, `paused_from: str \| None`, `error: str \| None` |
| `node.resume` | tourctl → peer | *(empty)* |
| `node.resume.response` | peer → tourctl | `ok: bool`, `resumed_to: str \| None`, `error: str \| None` |

All four envelope kinds travel on the **peer plane** (mTLS peer listener).

### FSM transitions

Legal pause and resume transitions:

| Source phase | Pause guard | Post-pause phase | Resume guard | Post-resume phase |
|---|---|---|---|---|
| `READY` | phase == READY | `PAUSED` | paused_from == READY | `READY` |
| `DRAINING` | phase == DRAINING | `PAUSED` | paused_from == DRAINING | `DRAINING` |
| `JOINING` | phase == JOINING | `PAUSED` | paused_from == JOINING | `JOINING` |

**Illegal transitions:**

- Pause when already `PAUSED` → error: `"node is already paused (paused_from=<phase>)"`.
- Pause from `IDLE` or `FAILED` → error: `"cannot pause node in phase <phase>"`.
- Resume when not `PAUSED` → error: `"node is not paused (current phase: <phase>)"`.
- `node.drain` while `PAUSED` → error: `"node is paused; resume before draining"`.
  The drain handler checks `phase == PAUSED` before any other drain logic.

`PAUSED` has **no legal exits besides the three resume transitions** above. No other
handler may transition away from `PAUSED`.

### Behaviour while PAUSED

#### `READY → PAUSED`

- **KV socket**: stopped/closed. When the node transitions from `READY` to `PAUSED`, the
  KV coordinator listener MUST be stopped if it is running to ensure the node does not
  accept coordinator/client requests while paused. Where possible the listener should be
  closed gracefully to allow active RPCs to complete; operators should expect client
  connections to be disconnect when pause is applied.
- **Reads**: rejected. A paused node does not serve KV reads; coordinators must route
  client reads to other nodes in the cluster.
- **Writes**: the `PlacementStrategy` observes `phase == PAUSED`, which is a member of
  `_ALWAYS_HANDOFF_PHASES`, and routes all writes to a handoff target. The paused node
  does not accept new writes in this state.
- **Rebalance**: suspended. Any in-progress outgoing rebalance plan is not advanced
  while the node is paused.

#### `DRAINING → PAUSED`

- **KV socket**: stopped/closed (was open during draining). On transition to `PAUSED` the
  KV coordinator listener MUST be stopped if running; active client connections should be
  closed gracefully where possible.
- **Reads/writes**: a paused node does not accept coordinator reads or writes; write
  routing uses handoff targets as above.
- **Ongoing partition transfers**: suspended at their current `RUNNING` chunk boundary.
  The chunk cursor is not advanced. When the operator calls `resume`, the transfer loop
  resumes from the last committed cursor position. No data is lost or duplicated.

#### `JOINING → PAUSED`

- **KV socket**: **closed**. The node had not yet started serving KV traffic (join is
  incomplete), so the socket is not opened on pause entry.
- **Incoming partition transfers**: suspended. The remote sender observes no progress on
  the transfer stream and backs off with its normal retry/timeout logic. When the operator
  calls `resume`, the transfer acceptor is re-registered and the sender retries.

### Write routing while PAUSED

`_ALWAYS_HANDOFF_PHASES` (defined in `core/ring/placement.py`) includes `MemberPhase.PAUSED`.
`SimplePreferenceStrategy.preference_list()` evaluates `member.phase in _ALWAYS_HANDOFF_PHASES`
for every primary replica; a `PAUSED` primary therefore always gets a handoff target, regardless
of its source phase. No changes to `placement.py` are required by this proposal.

Gossip propagates the `PAUSED` phase to all peers via the normal `gossip.push` path. Once
other coordinators observe the `PAUSED` phase in the registry, they immediately begin routing
writes around the paused node.

### Handler registration

Handlers live in `tourillon/core/lifecycle/handlers.py` and are registered with the
peer-plane `Dispatcher` via `@dispatcher.on(kind)`. Handler names follow the
concise style used elsewhere in the codebase (e.g. `pause(...)`, `resume(...)`):

```python
@dispatcher.on("node.pause")
async def pause(receive, send) -> None: ...

@dispatcher.on("node.resume")
async def resume(receive, send) -> None: ...
```

A `register(dispatcher)` module-level function calls both registrations so the bootstrap
sequence needs only one call.

### Crash recovery

On restart the daemon reads `state.toml`. If `phase == "paused"` and `paused_from` is
present, the node remains in `PAUSED` until an operator issues `node.resume`. It does
**not** auto-resume on restart. This is intentional: a paused node that crashes and
restarts should not silently rejoin the data plane before the operator has verified it
is ready.

### Gossip propagation

No new gossip message kind is required. The `gossip.push` path carries the
`Member` record, which includes `phase`. When the node transitions to `PAUSED`, its
`Member.phase` is updated to `MemberPhase.PAUSED` (with incremented `seq`), and the
next gossip push propagates the change to all peers. `paused_from` is a local-only field
stored in `state.toml`; it is **not** gossiped (peers only need to know the node is
paused, not which phase it paused from).

To improve shutdown safety and match the behaviour described in the gossip proposal,
the node should attempt a small number of successful gossip rounds before fully
stopping peer-plane activity. Concretely, on applying `PAUSED` the gossip engine SHOULD
attempt N successful push rounds (recommended default: N = 3) to a set of known peers
and only consider the transition fully propagated after receiving success responses
from those rounds. If a round fails, the engine should retry with exponential backoff
for a bounded period; the node must not proceed to close peer-plane resources that would
prevent further propagation until the propagation procedure either completes successfully
or the operator forces the pause to take effect. This helps ensure that other coordinators
observe the `PAUSED` phase promptly and route writes away from the paused node.

### `TopologyManager.apply_member()`

When `apply_member()` receives a `Member` with `phase == PAUSED`, it applies the member
record to the registry as normal. The topology ring is not altered: the paused node retains
its tokens. `PlacementStrategy` handles the write-routing consequence without any special
casing in `TopologyManager`.

### Error paths

| Condition | Response payload | stderr output | Exit code |
|---|---|---|---|
| Node already paused | `ok=false, error="node is already paused (paused_from=<phase>)"` | same | 1 |
| Node not in pausable phase | `ok=false, error="cannot pause node in phase <phase>"` | same | 1 |
| Node not paused on resume | `ok=false, error="node is not paused (current phase: <phase>)"` | same | 1 |
| Drain while paused | `ok=false, error="node is paused; resume before draining"` | same | 1 |
| Transport / mTLS error | *(no envelope)* | `Error: connection failed: <reason>` | 1 |

---

## Design decisions

### Decision: `paused_from` stored locally, not gossiped

**Alternatives considered:** Gossip the full `paused_from` value alongside `phase` by adding
a field to `Member`.

**Chosen because:** Peers need only the `PAUSED` phase to make routing decisions; the source
phase is a recovery hint for the local node only. Keeping it local avoids changing the
`Member` wire format and the gossip merge logic. The `Member` dataclass stays stable.

### Decision: No auto-resume on restart

**Alternatives considered:** Auto-resume to `paused_from` if the daemon restarts while paused.

**Chosen because:** Auto-resume after a crash would reintroduce the node to the data plane
without operator intervention. Operators pause nodes for reasons (disk replacement, active
incident) that may persist across a restart. Requiring an explicit `resume` command keeps
the operator in control.

### Decision: KV socket stopped on pause transitions (except `JOINING → PAUSED`)

**Alternatives considered:** Keep the KV socket open for `READY → PAUSED` and
`DRAINING → PAUSED` to avoid disrupting client connections.

**Chosen because:** pausing is an operator-controlled state meant to remove the node
from serving as a coordinator. To avoid accidental acceptance of coordinator/client
requests while paused, the KV coordinator listener is stopped on pause entry when it
is running. Long-running client connections are a downsides; operators should plan
for client reconnection as part of maintenance procedures. `JOINING → PAUSED` remains
closed as before because the socket was never opened while joining.

### Decision: KV socket closed for `JOINING → PAUSED`

**Chosen because:** A `JOINING` node never opened its KV socket (KV is bound only when
`phase ∈ {READY, DRAINING}`), so there is nothing to keep open. This is consistent with
the node lifecycle invariant.

---

## Interfaces (informative)

```python
# tourillon/core/lifecycle/state.py  (updated)
from __future__ import annotations
from dataclasses import dataclass, field
from tourillon.core.lifecycle.member import MemberPhase

@dataclass(frozen=True)
class NodeState:
    node_id: str
    phase: MemberPhase
    generation: int
    seq: int
    tokens: tuple[int, ...]
    epoch: int
    committed_pids: tuple[int, ...] = ()
    staging_pids: tuple[int, ...] = ()
    paused_from: MemberPhase | None = None  # NEW


# tourillon/infra/store/state.py  (_parse_state / _encode_state updated)
def _parse_state(raw: dict[str, Any]) -> NodeState:
    node = raw["node"]
    pf_raw = node.get("paused_from")
    paused_from = MemberPhase(pf_raw) if pf_raw else None
    ...
    return NodeState(..., paused_from=paused_from)


def _encode_state(state: NodeState) -> dict[str, Any]:
    node_section: dict[str, Any] = {
        "node_id": state.node_id,
        "phase": state.phase.value,
        "generation": state.generation,
        "seq": state.seq,
        "tokens": list(state.tokens),
    }
    if state.paused_from is not None:
        node_section["paused_from"] = state.paused_from.value
    return {
        "node": node_section,
        "topology": {"epoch": state.epoch},
        "rebalance": {
            "committed_pids": list(state.committed_pids),
            "staging_pids": list(state.staging_pids),
        },
    }


# tourillon/core/lifecycle/handlers.py  (new file)
from tourillon.core.transport.dispatcher import Dispatcher

_PAUSABLE_PHASES = frozenset({
    MemberPhase.READY, MemberPhase.DRAINING, MemberPhase.JOINING
})

async def handle_node_pause(receive, send) -> None:
    """Handle node.pause envelope on the peer plane."""
    ...

async def handle_node_resume(receive, send) -> None:
    """Handle node.resume envelope on the peer plane."""
    ...

def register(dispatcher: Dispatcher) -> None:
    dispatcher.on("node.pause")(handle_node_pause)
    dispatcher.on("node.resume")(handle_node_resume)


# tourctl commands (tourctl/infra/cli/node.py)
import typer

app = typer.Typer()

@app.command("pause")
def cmd_pause(addr: str, json_out: bool = typer.Option(False, "--json")) -> None:
    """Pause the node at addr."""
    ...

@app.command("resume")
def cmd_resume(addr: str, json_out: bool = typer.Option(False, "--json")) -> None:
    """Resume the node at addr."""
    ...
```

---

## Test scenarios

All scenarios run with in-memory adapters unless marked `[e2e]`.

| # | Fixture | Action | Expected |
|---|---------|--------|----------|
| 1 | Node in `READY` phase | Send `node.pause` | Phase transitions to `PAUSED`; `paused_from == READY`; response `ok=true, paused_from="ready"` |
| 2 | Node in `DRAINING` phase | Send `node.pause` | Phase transitions to `PAUSED`; `paused_from == DRAINING`; response `ok=true, paused_from="draining"` |
| 3 | Node in `JOINING` phase | Send `node.pause` | Phase transitions to `PAUSED`; `paused_from == JOINING`; response `ok=true, paused_from="joining"` |
| 4 | Node in `PAUSED` (from `READY`) | Send `node.resume` | Phase transitions to `READY`; `paused_from` cleared to `None`; response `ok=true, resumed_to="ready"` |
| 5 | Node in `PAUSED` (from `DRAINING`) | Send `node.resume` | Phase transitions to `DRAINING`; response `ok=true, resumed_to="draining"` |
| 6 | Node in `PAUSED` (from `JOINING`) | Send `node.resume` | Phase transitions to `JOINING`; response `ok=true, resumed_to="joining"` |
| 7 | Node already `PAUSED` | Send `node.pause` again | Response `ok=false`, error contains `"already paused"`; phase unchanged |
| 8 | Node in `READY` (not paused) | Send `node.resume` | Response `ok=false`, error contains `"not paused"`; phase unchanged |
| 9 | Node in `IDLE` phase | Send `node.pause` | Response `ok=false`, error contains `"cannot pause node in phase idle"` |
| 10 | Node in `FAILED` phase | Send `node.pause` | Response `ok=false`, error contains `"cannot pause node in phase failed"` |
| 11 | Node in `PAUSED` | Send `node.drain` | Response `ok=false`, error contains `"node is paused; resume before draining"` |
| 12 | `READY → PAUSED` then save/load `state.toml` | Reload `NodeState` from disk | `phase == PAUSED`, `paused_from == READY` |
| 13 | `PAUSED` node in `PlacementStrategy` | Build preference list | Node appears with `handoff != None`; write is routed to handoff target |
| 14 | Not-paused node in `PlacementStrategy` | Build preference list | `paused_from` absence has no effect; normal preference list returned |
| 15 | Gossip push with `PAUSED` member | Peer calls `apply_member()` | Registry updated; `Member.phase == PAUSED`; ring tokens unchanged |
| 16 | `state.toml` with `paused_from` absent | `_parse_state` | `NodeState.paused_from is None` |
| 17 | `state.toml` with `paused_from = "ready"` | `_parse_state` | `NodeState.paused_from == MemberPhase.READY` |
| 18 | `NodeState` with `paused_from=None` | `_encode_state` | TOML `[node]` section does not contain key `paused_from` |
| 19 | `NodeState` with `paused_from=DRAINING` | `_encode_state` | TOML `[node]` section contains `paused_from = "draining"` |
| 20 | `[e2e]` Two-node cluster, node A paused | `tourctl kv put` routed through node A's partition | Write succeeds via handoff to node B |

---

## Exit criteria

- [ ] All 20 test scenarios pass.
- [ ] `uv run pytest -m pause -x` passes.
- [ ] `uv run pytest --cov-fail-under=90` passes.
- [ ] `uv run ruff check tourillon/ tourctl/ tests/` passes with zero warnings.
- [ ] `uv run black --check tourillon/ tourctl/ tests/` passes.
- [ ] `uv run pre-commit run --all-files` passes.
- [ ] `NodeState.paused_from` field is present in `core/lifecycle/state.py`.
- [ ] `_parse_state` reads `paused_from` from `[node]` TOML section without error when absent.
- [ ] `_encode_state` omits `paused_from` key when value is `None`.
- [ ] `node.pause` and `node.resume` handlers registered in `core/lifecycle/handlers.py`.
- [ ] Drain-while-paused guard present in the drain handler.
- [ ] `tourctl node pause` and `tourctl node resume` commands exist with `--json` flag.

---

## Out of scope

- Scheduled or timed pause (pause for N minutes then auto-resume).
- Remote pause triggered by the failure detector.
- `PAUSED → FAILED` transition (failure while paused is detected by gossip phi-accrual
  independently; no direct transition is modelled here).
- Any change to the gossip wire format or the `Member` dataclass.
- Read-traffic routing changes while paused (reads on a `READY → PAUSED` node proceed
  normally; no special read-repair suppression).
