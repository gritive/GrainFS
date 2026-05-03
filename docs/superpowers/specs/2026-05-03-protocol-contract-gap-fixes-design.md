# Protocol Contract Gap Fixes Design

Date: 2026-05-03
Branch: master

## Goal

Fix three architecture gaps found after the protocol-layering hardening work:

1. Versioned object operations must route through the same multi-Raft forwarding path as ordinary object operations.
2. Singleton Iceberg catalog wiring must use meta-Raft catalog pointers, matching the cluster-mode contract, and migrate existing singleton local catalog rows into that path.
3. `grainfs cluster join` must perform the real meta-join RPC instead of acting as a placeholder.

The common theme is contract consistency. Public protocol adapters should not behave differently depending on which node receives the request or whether the server started as a singleton.

## Not In Scope

- Streaming remote `GetObject` responses beyond the current forwarded reply cap.
- Changing bucket-to-group assignment semantics.
- Reworking dynamic group rebalancing or joint consensus.

## Architecture

The target architecture stays unchanged:

```text
S3 / NFS / NBD / Iceberg
        |
        v
 storage.Backend
        |
        v
 ClusterCoordinator
   |              |
   v              v
meta-Raft      data Raft groups
catalog ptrs   object metadata and bytes
bucket map
```

This change fills missing paths instead of adding a new layer.

## Versioned Object Forwarding

`ClusterCoordinator` already routes normal object operations through:

```text
ClusterCoordinator
  -> routeBucket(bucket)
  -> local GroupBackend when self is group leader
  -> ForwardSender
  -> QUIC StreamProposeGroupForward
  -> ForwardReceiver on the group leader
  -> GroupBackend
```

The same pattern must cover:

- `GetObjectVersion(bucket, key, versionID)`
- `DeleteObjectVersion(bucket, key, versionID)`
- `ListObjectVersions(bucket, prefix, maxKeys)`

### Wire Changes

Extend the existing forward wire API with new operations:

- `ForwardOpGetObjectVersion`
- `ForwardOpDeleteObjectVersion`
- `ForwardOpListObjectVersions`

Add request helpers for:

- bucket + key + version ID
- bucket + prefix + max keys

Add reply helpers for object-version lists. This must use a distinct
`ForwardObjectVersionMeta` table and `ForwardReply.versions` vector, not the
existing `ForwardObjectMeta`, because `storage.ObjectVersion` needs
`is_latest` and `is_delete_marker` fields. Remote `ListObjectVersions` must not
infer those fields from ETag or ordering.

Object body replies should follow the existing `GetObject` reply pattern and
validate that metadata size matches returned body bytes. Remote
`GetObjectVersion` intentionally inherits the current forwarded read cap
(`DefaultMaxForwardReplyBytes`, currently 64 MiB). If the version body exceeds
that cap, the receiver must return the existing entity-too-large status rather
than truncating or allocating an unbounded reply. Streaming remote reads remain
out of scope for this PR.

### Error Handling

Forwarded versioned operations should preserve the local leader behavior:

- missing bucket maps to `storage.ErrNoSuchBucket`
- missing object or version maps to the storage-layer not-found error used by local calls
- non-leader replies use the existing `ForwardStatusNotLeader` redirect path
- missing group or non-voter replies stay mapped to `ErrUnknownGroup`

No new retry loop is needed. The existing one-redirect policy is enough.

## Iceberg Singleton Catalog

`cmd/grainfs/serve.go` currently uses a local `icebergcatalog.Store` when no peers are configured and the raft address was not explicit. That violates the protocol layering contract because cluster mode uses `MetaCatalog`.

Change singleton server wiring to use `cluster.MetaCatalog` as well. The singleton already starts a local meta-Raft node on a loopback QUIC address, so catalog pointer commands can use the same meta-Raft path as cluster mode.

The Iceberg metadata JSON files remain normal objects under the warehouse bucket. Only catalog pointers move through meta-Raft.

### Legacy Singleton Migration

Existing old singleton Iceberg catalog rows must be migrated in this PR. On singleton startup, before serving Iceberg traffic with `MetaCatalog`, run a compatibility migration from the old local `icebergcatalog.Store` keys in the same Badger database into meta-Raft.

The migration should:

- export legacy namespace records and table records through a new `icebergcatalog.Store` export API, instead of duplicating private Badger key formats outside the package
- propose namespaces first, then table pointers, through the same meta-Raft Iceberg commands used by `MetaCatalog`
- preserve namespace properties, table identifiers, table metadata locations, and table properties
- backfill the warehouse metadata object from the legacy table `Metadata` JSON when `MetadataLocation` does not already resolve through the configured backend
- leave old local Store keys in place after a successful migration; they become unused once singleton wiring switches to `MetaCatalog`

Idempotency rules:

- if the target meta-Raft namespace already exists with the same properties, skip it
- if the target meta-Raft namespace exists with different properties, keep the existing meta-Raft value and log a warning; namespace properties are not enough to justify startup failure
- if the target meta-Raft table already exists with the same metadata location and properties, skip it
- if the target meta-Raft table already exists with a different metadata location, fail startup with a clear conflict error
- if a table metadata object is missing and the legacy Store has empty metadata JSON, fail startup; otherwise write the legacy JSON to the parsed `s3://bucket/key` location before proposing the table

The migration must be retry-safe. Re-running startup after a partial migration should either skip already-applied entries or continue from the next missing entry without duplicating tables.

## Cluster Join CLI

`grainfs cluster join <peer>` should perform a real join:

```text
grainfs cluster join <peer>
  -> start local QUIC listener on --raft-addr
  -> connect to peer
  -> send JoinRequest(nodeID, raftAddr)
  -> leader MetaJoinReceiver calls MetaRaft.Join
  -> return accepted, already-member, or rejected status
```

The CLI should keep its current flags:

- `--data-dir`
- `--node-id`
- `--raft-addr`
- `--cluster-key`

On success it prints a concise accepted/already-member message. On rejection it exits non-zero with the join status and message.

Implementation should reuse the same join sender behavior used by `serve --join`; do not create a second join protocol.
Extract the existing `performMetaJoin` flow into a shared `cmd/grainfs` helper
used by both `serve --join` and `grainfs cluster join`. Do not copy the logic
into `cluster_join.go`; timeout, leader-hint handling, and rejection errors
should have one implementation.

## Tests

Unit tests:

- `ClusterCoordinator.GetObjectVersion` forwards when self is not the group leader.
- `ClusterCoordinator.DeleteObjectVersion` forwards when self is not the group leader.
- `ClusterCoordinator.ListObjectVersions` forwards when self is not the group leader.
- Remote `GetObjectVersion` returns the existing entity-too-large error when
  the version body exceeds the forwarded reply cap.
- `ForwardReceiver` dispatches the three versioned operations.
- `icebergcatalog.Store` export returns legacy namespaces and tables with
  metadata locations, metadata JSON, and properties intact.
- The singleton Iceberg migration backfills a missing warehouse metadata object
  from legacy table JSON before proposing the meta-Raft table pointer.
- The singleton Iceberg migration is idempotent for already-migrated matching
  namespaces and tables, and fails fast on conflicting table metadata
  locations.
- `grainfs cluster join` has a command-level test proving `runClusterJoin`
  calls the shared real join path through an injected seam or dialer. Testing
  only the shared helper is not enough because the original regression lives in
  the command wiring.

E2E or targeted integration tests:

- Create a versioned bucket and object through one node, then read, list, and delete versions through a different node.
- Create an Iceberg table through the old local Store fixture, restart singleton
  with the new wiring, and verify the table loads through `MetaCatalog`.

## Failure Modes

- A forwarded version lookup targets a follower: the receiver returns `NotLeader` with a hint; the sender retries once.
- A bucket assignment exists but the group is not locally instantiated yet: routing returns an existing group but forwarding can return not-voter or unreachable; the caller sees the existing storage/forwarding error rather than silent success.
- Singleton Iceberg meta-Raft has no leader yet during startup: catalog requests fail as unavailable until meta-Raft elects, matching cluster behavior.
- Legacy singleton migration finds a table pointer conflict in meta-Raft:
  startup fails instead of silently choosing either pointer.
- Legacy singleton migration finds missing warehouse metadata and no legacy JSON:
  startup fails because the new `MetaCatalog` could not load that table.
- Join command contacts a follower: `MetaJoinSender` follows the leader hint when present.

## Follow-Up Work

- Streaming remote `GetObject` and `GetObjectVersion` responses beyond the current forwarded reply cap.

## GSTACK REVIEW REPORT

| Review | Skill | Scope | Runs | Verdict | Notes |
| --- | --- | --- | ---: | --- | --- |
| Eng Review | `/plan-eng-review` | Architecture, data flow, edge cases, tests | 1 | CLEAR | 5 issues found and amended: distinct version metadata wire schema, shared join implementation, command-level join test, forwarded read cap behavior, legacy Iceberg migration with metadata backfill. |
