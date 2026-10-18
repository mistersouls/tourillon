# Current capabilities and limitations

Tourillon is a development preview, not a production-ready service. Releases
may introduce incompatible changes. Do not use it for critical or
irreplaceable data.

## What you can try today

- Run identical peer nodes without electing a leader or operating a central
  controller.
- Write, read, and delete key-value data through `tourctl`.
- Distribute and replicate data across nodes using hash-based placement.
- Request numeric read and write quorums for individual operations.
- Authenticate client and peer connections with mandatory mTLS.
- Inspect nodes and use join-time data transfers, hinted handoff, background
  read repair, and on-demand repair of an under-replicated write.

These mechanisms are implemented; their presence is not a guarantee of
production reliability or a particular performance level.

## Important boundaries

| Area | Current limitation |
| --- | --- |
| Namespaces | Label data but do not provide tenant isolation, namespace ACLs, or quotas. |
| Authorization | mTLS authenticates connections; a trusted certificate is not a namespace-scoped permission. |
| Consistency | Quorums are per-request integers, not named strong/eventual consistency guarantees. |
| Conflicts | Concurrent updates are resolved automatically; application-level merge and preservation of every concurrent value are not provided. |
| Placement | Replicas are not placed according to rack, zone, or region anti-affinity. |
| Membership changes | Transfers are join-driven; graceful node removal and general steady-state rebalancing are not implemented. |
| Repair | Read repair and explicit write repair exist; proactive replica-data anti-entropy outside the read path is not implemented. |
| Scale | 10,000 nodes is a design objective, not a benchmarked or supported deployment size. |

The [Docker Compose example](../examples/docker/README.md) is a single-node
trial, not a production deployment or a demonstration of multi-node fault
tolerance. A successful local write/read is not evidence of behavior under
network partitions or node failures.

## Quorum output

`Quorum W` in the client output is the write quorum requested for that stored
version, **not the number of replicas that acknowledged it**. Request models
default an omitted write quorum to `1`; reads also default to a quorum of `1`.
For compatibility, handlers treat an explicit `null` quorum as omitted. An
empty answer from a replica counts as a read response.
Older versions written before this default was introduced may retain a `0`
marker. Stored markers are not rewritten on reads.

For a single-node example, request one acknowledgement explicitly:

```sh
tourctl put greeting "Hello, Tourillon!" --quorum-write 1 --tourconf ./tourconf.toml
```

## Next steps

Use the [installation guide](installation.md) or the Docker example to try the
current release. The [product vision](principles.md) describes future goals,
not additional capabilities available today.

For implementation details and contributor work items, see the
[architecture reference](architecture.md) and [internal backlog](backlog.md).
