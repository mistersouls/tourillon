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

## Contributions

Contributions are welcome once the foundational components are in place.

---

## License

Apache 2.0
