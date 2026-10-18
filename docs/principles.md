# Product objectives and architectural principles

This document is the durable reference for Tourillon's product objectives and
architectural direction. Changes to these principles should be deliberate and
reviewed as product-level decisions.

These objectives describe the intended design and long-term goals; they are
not a claim that every capability is already implemented or production-ready.
See the [architecture reference](architecture.md) for mechanisms currently
implemented and the [backlog](backlog.md) for known gaps and open questions.

## Core objectives

- Provide a **simple, fast, and extensible** distributed key-value store.
- Support **namespaces** to isolate data and enable multi-tenant or shared
  environments.
- Build high-availability clusters capable of scaling to **10,000 nodes** or
  more.
- Offer a minimalistic, predictable API that integrates easily with existing
  distributed systems.
- Allow configurable consistency levels (eventual, strong, tunable).
- Minimize external dependencies to remain lightweight and portable.

## Architectural principles

### Namespaces

- Logical isolation of data.
- Native multi-tenant support.
- Independent management of quotas, replication, and access control lists
  (ACLs).

### Distribution

- Automatic key partitioning.
- Dynamic rebalancing when nodes join or leave.
- Deterministic routing based on hashing.
- **Leaderless operation**: coordination is decentralized, with **no single
  central controller**.

### Scalability

- Architecture designed to reach **10K+ nodes**.
- Network protocols optimized for very large clusters.
- Minimal global metadata to avoid bottlenecks.

### High availability

- **Automatic rebalance** of partitions when topology changes.
- **Label-based anti-affinity** ensuring replicas of the same shard do not land
  on nodes sharing common failure domains (rack, zone, region, provider, etc.).
- Namespace-level configurable replication.
- Failure detection and automatic cluster reconfiguration.
