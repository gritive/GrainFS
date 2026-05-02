# Iceberg Any-Node Table API Design

Date: 2026-05-02
Status: approved design

## Goal

Make the Iceberg REST Catalog metadata API work when clients call any GrainFS
node in a cluster. Reads and writes must be safe across leader and follower
nodes without enabling node-local catalog state that can split-brain.

This design covers cluster-wide Iceberg catalog metadata only:

- namespaces
- table records
- table metadata pointer compare-and-swap
- table and namespace deletion

It does not add a DuckDB 3-node e2e in this pass. The existing local DuckDB e2e
remains the high-level client smoke test, while this work adds cluster-level
server integration tests for any-node semantics.

## Current State

The landed Iceberg REST Catalog stores namespace and table records in
`internal/icebergcatalog.Store`, backed by a local BadgerDB. `grainfs serve`
wires this store for local/no-peers mode. In multi-peer cluster mode,
`/iceberg/*` is intentionally disabled because independent Badger stores on
each node would create divergent catalog state.

The cluster already has a meta-Raft control plane for membership, shard groups,
bucket assignments, load snapshots, and rebalance plans. That control plane is
the right home for Iceberg catalog metadata because table metadata pointers are
small, cluster-wide state that must be committed through a quorum.

## Chosen Approach

Add Iceberg catalog state and commands to meta-Raft.

Local mode keeps using the existing Badger-backed `icebergcatalog.Store`.
Cluster mode uses a new meta-Raft backed catalog implementation. The server
talks to both through a shared catalog interface.

```text
HTTP /iceberg/*
  -> iceberg catalog interface
      -> local mode: Badger Store
      -> cluster mode: MetaCatalog
           read: local applied MetaFSM state
           write: meta-Raft propose or forward to meta-Raft leader
```

This avoids a second Raft group and avoids leader-local Badger state. It also
keeps existing object storage behavior unchanged: metadata JSON files are still
written through the GrainFS backend and table pointer changes are committed
through the catalog.

## Catalog Interface

Introduce an interface in `internal/icebergcatalog` that matches the current
store surface:

```go
type Catalog interface {
    Warehouse() string
    CreateNamespace(ctx context.Context, namespace []string, properties map[string]string) error
    LoadNamespace(ctx context.Context, namespace []string) (map[string]string, error)
    ListNamespaces(ctx context.Context) ([][]string, error)
    DeleteNamespace(ctx context.Context, namespace []string) error
    CreateTable(ctx context.Context, ident Identifier, in CreateTableInput) (*Table, error)
    LoadTable(ctx context.Context, ident Identifier) (*Table, error)
    ListTables(ctx context.Context, namespace []string) ([]Identifier, error)
    DeleteTable(ctx context.Context, ident Identifier) error
    CommitTable(ctx context.Context, ident Identifier, in CommitTableInput) (*Table, error)
}
```

`Store` implements this interface unchanged. A new cluster implementation,
`MetaCatalog`, reads from `cluster.MetaFSM` and writes through `cluster.MetaRaft`.

`internal/server` should depend on the interface rather than on `*Store`, so
the same handlers serve local and cluster modes.

## MetaFSM State

Add two maps to `cluster.MetaFSM`:

```go
icebergNamespaces map[string]IcebergNamespaceEntry
icebergTables     map[string]IcebergTableEntry
```

Keys are canonical strings:

- namespace key: namespace parts joined with `\x1f`
- table key: namespace key + `\x1f` + table name

State records:

```go
type IcebergNamespaceEntry struct {
    Namespace  []string
    Properties map[string]string
}

type IcebergTableEntry struct {
    Identifier       icebergcatalog.Identifier
    MetadataLocation string
    Metadata         json.RawMessage
    Properties       map[string]string
}
```

All getters return defensive copies. Snapshot and restore include both maps so
restart and follower replay preserve catalog state.

## Meta-Raft Commands

Add five meta-Raft command types:

1. `IcebergCreateNamespace`
2. `IcebergDeleteNamespace`
3. `IcebergCreateTable`
4. `IcebergCommitTable`
5. `IcebergDeleteTable`

Reads do not create commands. `LoadNamespace`, `ListNamespaces`, `LoadTable`,
and `ListTables` read the local applied `MetaFSM` state.

Apply rules mirror the existing local store:

- create namespace: existing namespace returns `ErrNamespaceExists`
- delete namespace: missing namespace returns `ErrNamespaceNotFound`
- delete namespace with tables: returns `ErrNamespaceNotEmpty`
- create table: missing namespace returns `ErrNamespaceNotFound`
- create table when table exists: returns `ErrTableExists`
- commit table: missing table returns `ErrTableNotFound`
- commit table with stale expected metadata location: returns `ErrCommitFailed`
- delete table: missing namespace or table returns the existing typed errors

## Write Result Delivery

Meta-Raft must return exact catalog errors to the HTTP caller. `ProposeWait`
only says whether the log entry committed and applied locally, so Iceberg
commands include a request ID. The proposing node registers a waiter for that
request ID before proposing the command.

```text
HTTP handler
  -> build Iceberg command with requestID
  -> register result waiter(requestID)
  -> propose command locally or forward to leader
  -> apply loop mutates FSM and publishes result(requestID)
  -> HTTP returns exact Iceberg JSON response
```

The waiter is local to the node that will perform the proposal. If a follower
forwards the command to the leader, the leader owns the waiter and returns the
typed result to the follower in the forwarding reply.

Waiters must be bounded by the request context. On timeout or node shutdown,
the caller returns 503 `ServiceUnavailableException`.

## Any-Node Writes

Follower writes forward the encoded meta command to the current meta-Raft
leader over QUIC.

Add a new transport stream:

```go
StreamMetaProposeForward = 0x0a
```

Forward request:

```text
MetaProposeForwardRequest {
  command: []byte
}
```

Forward reply:

```text
MetaProposeForwardReply {
  status: OK | NotLeader | Error
  leaderHint: string
  errorType: string
  errorMessage: string
}
```

Write flow:

```text
client calls any node
  -> node builds Iceberg meta command
  -> if node is meta-Raft leader:
       ProposeWait + wait local apply result
     else:
       forward command to known leader
       leader ProposeWait + wait leader apply result
  -> return exact catalog result to original HTTP caller
```

Leader discovery uses `metaRaft.Node().LeaderID()`. If no leader is known, the
node retries briefly and then returns 503. If the forward target replies
`NotLeader` with a leader hint, the caller retries once against the hint.

## Read Consistency

Reads use the local applied meta-Raft state. This is intentionally not a
linearizable read design.

Guarantees:

- Write responses wait until the proposing node has applied the committed entry.
- A read on the same node after a successful write sees that write.
- Reads on another follower may be stale for a short raft apply lag.
- All followers converge through the meta-Raft log.

This keeps read latency low and matches the current scope. A future design can
add leader-backed or ReadIndex-backed catalog reads if strict linearizability is
needed for every read on every node.

## Metadata JSON Object Writes

Table creation and commit still write Iceberg metadata JSON objects through the
existing GrainFS backend before committing the table pointer.

If the metadata object write succeeds but the pointer CAS fails, an orphan
metadata JSON object can remain. That is acceptable for this pass because the
table pointer does not move, so catalog correctness is preserved. Cleanup can be
handled later by lifecycle or catalog garbage collection.

## Error Mapping

The existing Iceberg JSON error boundary remains:

- `ErrNamespaceNotFound` -> 404 `NoSuchNamespaceException`
- `ErrNamespaceExists` -> 409 `AlreadyExistsException`
- `ErrNamespaceNotEmpty` -> 409 `NamespaceNotEmptyException`
- `ErrTableNotFound` -> 404 `NoSuchTableException`
- `ErrTableExists` -> 409 `AlreadyExistsException`
- `ErrCommitFailed` -> 409 `CommitFailedException`
- no meta-Raft leader or forward failure -> 503 `ServiceUnavailableException`
- malformed request -> 400 `BadRequestException`

## Cluster Wiring

Local/no-peers mode:

- opens the current Badger-backed `Store`
- uses the existing local DuckDB e2e unchanged

Cluster mode:

- creates `MetaCatalog` after `MetaRaft` starts
- registers the meta propose forward handler on the shared QUIC stream router
- wires `server.WithIcebergCatalog(...)` instead of leaving the Iceberg store nil
- enables `/iceberg/*` on all nodes

Existing local catalog data is not auto-migrated into meta-Raft in this pass.
Cluster catalog state starts empty unless created through the cluster API.

## Tests

### MetaFSM Unit Tests

- create/list/load namespace
- create/list/load table
- commit CAS success
- stale commit CAS failure
- delete table
- delete non-empty namespace conflict
- delete empty namespace success
- snapshot/restore preserves Iceberg catalog maps

### MetaCatalog and Server Unit Tests

- handlers work through the catalog interface
- local `Store` still satisfies the interface
- cluster `MetaCatalog` returns the same typed errors as `Store`
- follower write path forwards to a leader mock
- read path uses local FSM state
- Iceberg JSON errors remain stable

### Cluster Integration Tests

Run a 3-node cluster and verify:

- node A creates namespace/table
- node B lists and loads namespace/table
- node C commits table metadata pointer
- node A sees the committed pointer after apply convergence
- node B deletes table and namespace
- stale CAS through a non-leader node returns 409 `CommitFailedException`

The existing local DuckDB e2e remains in place and should continue to pass.

## Rollout

This is an additive cluster feature:

- local mode behavior remains unchanged
- cluster mode changes `/iceberg/*` from 501 to active catalog endpoints
- cluster catalog state is replicated through meta-Raft
- read consistency is documented as local-applied follower reads

No background migration runs in this pass.

