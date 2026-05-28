# Raft Log Store At-Rest Encryption Design

**Date:** 2026-05-28
**Status:** Design ready; implementation pending
**Base:** `origin/master` at `2dafe129` (`v0.0.408.0`)

## Problem

Raft v2 log entries persist command payloads to Badger before the FSM applies
them. Those command payloads include live object metadata such as bucket names,
object keys, object sizes, etags, multipart state, placement references, IAM
metadata, and cluster configuration patches.

Snapshots are already envelope-sealed, and several materialized FSM values are
now DEK-sealed. The raft log command copy is still plaintext on disk:

- `internal/cluster/raftfactory.go` opens raft v2 stores with
  `badgerutil.SmallOptions(storeDir)`.
- `internal/raft/logstore_badger.go` appends encoded entries with raw
  `txn.Set(s.makeKey(e.Index), encoded[i])`.
- `internal/badgerutil.RaftLogOptions` exists but only configures durability
  knobs (`SyncWrites`, `NumVersionsToKeep`), not encryption.

This leaves a short-to-medium-lived plaintext copy of metadata between command
append and snapshot compaction. Operators who enable at-rest encryption expect
the raft log to follow the same trust boundary as snapshots and materialized
state.

## Goals

- Encrypt raft v2 Badger stores at the storage layer, below raft replication.
- Keep raft command bytes plaintext in memory and at the FSM boundary so apply
  remains deterministic and existing command codecs stay unchanged.
- Avoid boot circularity: the raft store must open before replay can rebuild the
  DEK keeper, so the raft store encryption key cannot depend on the live DEK.
- Make key rotation and KEK prune safe: a rotated/pruned KEK must not make a
  node unable to reopen its raft logs.
- Preserve raft durability semantics: append success still means the entry is
  durable before the follower acknowledges replication.

## Non-Goals

- Do not change raft wire formats, raft command FlatBuffers, or FSM apply
  semantics.
- Do not encrypt individual `LogEntry.Command` payloads in the raft layer. That
  would force every raft peer to share decryption context before apply and would
  complicate deterministic replay.
- Do not use the active DEK as the Badger encryption key. The DEK is recovered
  from raft snapshot/log state; using it to open that log is circular.
- Do not try to migrate existing unencrypted stores in place in the first
  implementation. Use the existing greenfield format boundary pattern unless a
  later migration spec explicitly chooses otherwise.

## Design

Use Badger native encryption for raft v2 stores:

```go
opts := badgerutil.RaftLogOptions(storeDir, true).
    WithEncryptionKey(masterKey)
```

Badger treats the provided key as the database master key and internally rotates
data keys. Badger master-key rotation is an offline database operation, so
GrainFS should keep the Badger master key stable across normal KEK rotations.

### Raft Store Master Key

Introduce a node-local random 32-byte raft-store master key:

```text
<dataDir>/keys.d/raft-store.key.enc
```

The plaintext master key is never replicated. It is sealed under the cluster
KEK store, with authenticated context:

```text
purpose = "grainfs-raft-store-key-v1"
cluster_id
node_id
key_id = "raft-store"
```

The sidecar records the KEK version used to seal it. On boot:

1. Load the KEK store from `keys/`.
2. Load and open `keys.d/raft-store.key.enc`.
3. If absent on an empty raft store, generate a new random 32-byte key and seal
   it under the active KEK.
4. If absent while raft store files already exist, refuse to start with a
   restore-from-backup error. Never generate a new key over existing encrypted
   raft files.
5. Pass the plaintext key only into Badger options, then zeroize local byte
   slices after `badger.Open` returns.

This keeps raft log encryption local to each node. Raft determinism is preserved
because replicated command bytes are unchanged; only the local Badger files are
encrypted.

### KEK Rotation and Prune

KEK rotation re-wraps cluster DEKs. It must also re-wrap the node-local
`raft-store.key.enc` before a KEK generation can be pruned. The plaintext
Badger master key does not change during normal KEK rotation, so existing Badger
files remain readable and Badger does not need offline master-key rotation for
ordinary KEK lifecycle.

Prune safety rule:

```text
refuse prune of KEK version V if any local raft-store key sidecar is still
sealed under V
```

If the key sidecar is rewrapped under the active KEK, pruning the old KEK is
safe for raft log reopen.

### Store Coverage

Apply this to every raft v2 Badger store opened by:

- meta raft: `cluster.NewMetaRaft` -> `newRaftNodeV2` -> `openRaftV2Stores`
- data-group raft: `cluster.group_lifecycle` -> `newRaftNode`
- direct test/serveruntime raft entry points:
  `NewRaftV2NodeForServeruntime`

The first implementation should thread an explicit key source into the raft
factory instead of reaching into global state. Tests can pass deterministic
keys; production gets the node-local key loader.

### Format Boundary

The implementation should bump the at-rest format version and fail loud when an
existing unencrypted raft store is opened under the encrypted-store binary.

Recommended rule:

- Empty raft store + no key sidecar: generate key and open encrypted Badger.
- Encrypted raft store + key sidecar: open encrypted Badger.
- Existing raft store + no key sidecar: fail with a message that names
  `keys.d/raft-store.key.enc` and explains that in-place migration is not
  supported in this release.

## Verification

Unit tests:

- Badger options helper adds `WithEncryptionKey` only when a 16/24/32-byte key
  is provided.
- Raft store key sidecar round-trips under the active KEK and rejects wrong
  cluster/node context.
- Existing raft store without sidecar fails loud.
- KEK prune refuses a version still used by `raft-store.key.enc`.
- Raft log append/read persists across reopen with encryption enabled.
- Wrong raft-store key fails to open an encrypted raft store.

Integration tests:

- Single-node restart: create object metadata, restart, verify raft replay and
  object read/list still work.
- Multi-node smoke: leader writes object metadata, follower restarts, catches up,
  and serves metadata after reopening encrypted raft logs.

Manual/inspection check:

- After writes, raw Badger value-log/table files must not contain the plaintext
  object key used by the test fixture.

## Risks

- Badger master-key rotation is offline. That is acceptable because GrainFS KEK
  rotation only rewraps the stable node-local raft-store key; it does not change
  the Badger master key.
- Losing `keys.d/raft-store.key.enc` bricks local encrypted raft logs. This is
  equivalent to losing other local key material and must be documented in backup
  guidance.
- All raft stores on a node can share one local raft-store master key in the
  first implementation. If per-store compartmentalization is later required,
  the sidecar can grow `key_id` records without changing raft command formats.
