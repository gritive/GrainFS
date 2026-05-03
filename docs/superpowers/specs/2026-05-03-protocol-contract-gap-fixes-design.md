# Protocol Contract Gap Fixes Design

Date: 2026-05-03
Branch: master

## Goal

Fix three architecture gaps found after the protocol-layering hardening work:

1. Versioned object operations must route through the same multi-Raft forwarding path as ordinary object operations.
2. Singleton Iceberg catalog wiring must use meta-Raft catalog pointers, matching the cluster-mode contract.
3. `grainfs cluster join` must perform the real meta-join RPC instead of acting as a placeholder.

The common theme is contract consistency. Public protocol adapters should not behave differently depending on which node receives the request or whether the server started as a singleton.

## Not In Scope

- Migrating existing singleton Iceberg catalog rows from the old local `icebergcatalog.Store` into meta-Raft.
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

Add reply helpers for object-version lists. Object body replies should follow the existing `GetObject` reply pattern and validate that metadata size matches returned body bytes.

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

Existing old local Iceberg catalog data is not migrated in this PR. That is a compatibility follow-up because it needs explicit source detection, conflict handling, and rollback behavior.

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

## Tests

Unit tests:

- `ClusterCoordinator.GetObjectVersion` forwards when self is not the group leader.
- `ClusterCoordinator.DeleteObjectVersion` forwards when self is not the group leader.
- `ClusterCoordinator.ListObjectVersions` forwards when self is not the group leader.
- `ForwardReceiver` dispatches the three versioned operations.
- `grainfs cluster join` uses the real meta-join path, or the join helper is extracted enough to test without launching a full server.

E2E or targeted integration tests:

- Create a versioned bucket and object through one node, then read, list, and delete versions through a different node.
- Create and load an Iceberg table in singleton mode and verify the server uses the `MetaCatalog` path.

## Failure Modes

- A forwarded version lookup targets a follower: the receiver returns `NotLeader` with a hint; the sender retries once.
- A bucket assignment exists but the group is not locally instantiated yet: routing returns an existing group but forwarding can return not-voter or unreachable; the caller sees the existing storage/forwarding error rather than silent success.
- Singleton Iceberg meta-Raft has no leader yet during startup: catalog requests fail as unavailable until meta-Raft elects, matching cluster behavior.
- Join command contacts a follower: `MetaJoinSender` follows the leader hint when present.

## Follow-Up Work

Migrate old singleton local Iceberg catalog data from `icebergcatalog.Store` into meta-Raft. This should be a separate compatibility task with detection, idempotency, and rollback notes.
