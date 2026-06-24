# Protocol Layering Contract

`GrainFS` exposes an S3 interface. The distributed object storage layer owns
placement, replication, recovery, and durability. Protocol adapters translate
client semantics into `storage.Backend` calls and must not persist authoritative
data outside that path.

```text
               S3 API
                  |
                  v
          storage.Backend
                  |
                  v
        ClusterCoordinator
                  |
    +-------------+-------------+
    |                           |
    v                           v
meta-Raft control plane    data Raft groups
bucket assignment,         object metadata, object bytes,
shard groups,              EC shard placement, snapshots
node address book
```

## Invariants

1. Bucket-scoped object operations route through `ClusterCoordinator`.
2. Bucket assignment is durable in meta-Raft before object IO depends on it.
3. `GrainFS` can backpressure bulk data streams without blocking meta-Raft requests.

## Not In Scope

This contract does not require key-range sharding, a second control-plane
process, or a separate table storage engine.
