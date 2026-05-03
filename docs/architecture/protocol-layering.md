# Protocol Layering Contract

GrainFS exposes several protocols, but they are not independent storage engines.
The distributed object storage layer owns placement, replication, recovery, and
durability. Protocol adapters translate client semantics into `storage.Backend`
calls and must not persist authoritative data outside that path.

```text
S3 API              NFSv4              NBD / volume              Iceberg REST Catalog
  |                   |                    |                              |
  +-------------------+--------------------+------------------------------+
                                      |
                                      v
                              storage.Backend
                                      |
                                      v
                            ClusterCoordinator
                                      |
                    +-----------------+-----------------+
                    |                                   |
                    v                                   v
              meta-Raft control plane             data Raft groups
      bucket assignment, shard groups,       object metadata, object bytes,
      node address book, Iceberg pointers    EC shard placement, snapshots
```

## Invariants

1. Bucket-scoped object operations route through `ClusterCoordinator`.
2. Bucket assignment is durable in meta-Raft before object IO depends on it.
3. NFSv4 and NBD mount the same backend used by S3.
4. Iceberg catalog state stores table pointers in meta-Raft; metadata JSON files
   remain ordinary objects in the warehouse bucket.
5. Bulk data streams may be backpressured without blocking meta-Raft requests.

## Not In Scope

This contract does not require key-range sharding, a second control-plane
process, or a separate table storage engine.
