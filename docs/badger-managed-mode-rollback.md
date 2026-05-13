# BadgerDB Managed Mode — Always Active (v0.0.172.0+)

## What It Does

GrainFS runs Raft log GC unconditionally. Periodically, committed Raft log entries
below the quorum watermark (`QuorumMinMatchIndex()`) are deleted from the BadgerDB
log store at `data/raft/`. GC only runs when a snapshot covering the watermark
exists (so lagging followers can recover via InstallSnapshot if needed).

The GC interval is controlled by `--raft-log-gc-interval` (default 30s).
Set to `0` to disable future GC runs without requiring a restart (but this
disables GC permanently until the next restart).

## On-Disk Format

Every Raft log store is opened with `raft:meta:managed=true`. Attempting to open
a managed store without the managed-mode option will fail with a clear error.

## If a Node Has Corrupted Post-GC State

If a node's Raft log was GC'd and the node cannot recover via InstallSnapshot:

1. Stop the node.
2. Delete `data/raft/`.
3. Restart. The node will rejoin as a fresh follower and catch up via snapshot.

## Monitoring

```promql
# Cluster should stay healthy during GC
grainfs_split_brain_suspected == 0

# Leader should remain stable
changes(grainfs_raft_term_total[10m]) == 0
```
