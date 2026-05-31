# Tourillon

## What is Tourillon?

Tourillon is a leaderless, peer-to-peer distributed key-value database focused on one promise: your data evolves predictably, even under failures, churn, and network instability.

You can send requests to any node, scale horizontally behind standard load balancers, and keep deterministic convergence across replicas.

## Why users choose Tourillon

- Predictable outcomes: replicas converge deterministically.
- No leader bottleneck: every node can serve requests.
- Built for real-world instability: reboots, network issues, and churn are first-class conditions.
- Secure by default: mandatory mutual TLS (mTLS) for all communications.
- Protocol clarity: binary-safe, self-describing messages for robust interoperability.

## Typical use cases

- Distributed metadata/state storage for platform services.
- Multi-node environments where deterministic conflict behavior matters.
- Infrastructure running behind standard load balancers with frequent scaling events.

## Core Guarantees

- Any node can serve client requests; no central leader is required.
- Data partitioning and replication are driven by a consistent-hashing ring.
- Per-key updates are append-only versions ordered deterministically.
- Concurrent writes can produce multiple valid versions returned on `GET`.
- Hinted handoff preserves update ordering when recovering from temporary failures.
- Join/leave events trigger deterministic rebalance operations.
- All communications use mandatory mutual TLS (mTLS).
- Protocol messages are binary-safe and self-describing.
