# Tourillon

**Tourillon** is a **distributed key‑value database** designed to be **massively scalable**, **highly available**,
and suitable for **multi‑tenant** environments through isolated **namespaces**.
It follows a **leaderless architecture** with **no central controller**.

---

## Core Objectives

- Provide a **simple, fast, and extensible** distributed KV-store.  
- Support **namespaces** to isolate data and enable multi‑tenant or shared environments.  
- Build **HA clusters** capable of scaling to **10,000 nodes** or more.  
- Offer a minimalistic, predictable API that integrates easily with existing distributed systems.  
- Allow configurable consistency levels (eventual, strong, tunable).  
- Minimize external dependencies to remain lightweight and portable.

---

## Architectural Principles

### Namespaces
- Logical isolation of data.  
- Native multi‑tenant support.  
- Independent management of quotas, replication, and ACLs.

### Distribution
- Automatic key partitioning.  
- Dynamic rebalancing when nodes join or leave.  
- Deterministic routing based on hashing.
- **Leaderless operation**: coordination is decentralized, with **no single central controller**.

### Scalability
- Architecture designed to reach **10K+ nodes**.  
- Network protocols optimized for very large clusters.  
- Minimal global metadata to avoid bottlenecks.

### High Availability
- **Automatic rebalance** of partitions when topology changes.  
- **Label‑based anti‑affinity** ensuring replicas of the same shard do not land on nodes sharing common failure domains (rack, zone, region, provider, etc.).  
- Namespace-level configurable replication.  
- Failure detection and automatic cluster reconfiguration.

---

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — how the node, client and CLI actually fit together: ring and partitioning, read/write paths, gossip, transfers, wire protocol, storage model.
- [`docs/BACKLOG.md`](docs/BACKLOG.md) — verified list of missing features, open defects and unsettled design questions.
- [`AGENTS.md`](AGENTS.md) — working agreement, conventions and commands. Read this before contributing.

---

## Contributions

Contributions are welcome once the foundational components are in place.

This repository is set up for AI-assisted development. [`AGENTS.md`](AGENTS.md) is the shared context for coding agents,
with path-scoped rules in [`.github/instructions/`](.github/instructions/) and reusable task runbooks in
[`.github/prompts/`](.github/prompts/). Human contributors should follow the same conventions.

### Quick start

```bash
uv sync --extra dev   # requires Python 3.14+
uv run pytest         # full suite with the 90% coverage gate
```

> Note: the repository carries pre-existing, repo-wide lint and formatting debt (see `docs/BACKLOG.md`, item D1).
> Run `black` and `ruff` on the files you changed — not across the whole tree.

---

## License

Apache 2.0
