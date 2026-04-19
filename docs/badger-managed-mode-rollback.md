# BadgerDB Managed Mode — Activation, Monitoring, and Rollback

## What It Does

When `--badger-managed-mode` is set, GrainFS periodically deletes committed Raft
log entries from the BadgerDB log store. The GC watermark is the highest log index
that at least a quorum of nodes have replicated (`QuorumMinMatchIndex()`). Entries
below that watermark are safe to remove from persistent storage.

This prevents the Raft log from growing unboundedly. Without GC, every write since
cluster bootstrap is retained in `data/raft/` forever.

## On-Disk Format Change

Once the store is opened with `--badger-managed-mode`, a metadata key
(`raft:meta:managed=true`) is written. Restarting without `--badger-managed-mode`
will be rejected with:

```
data dir opened in managed=true; use --badger-managed-mode or start fresh
```

This is intentional — it prevents silent data loss from restarting a GC'd node
in non-managed mode, which would make it look like log entries are simply missing.

## How to Enable

Add to your startup flags:

```
--badger-managed-mode
--raft-log-gc-interval 30s   # optional; default 30s
```

**Recommended rollout**: enable one node at a time. Verify the node restarts
cleanly before enabling on the next.

## Verification Queries (Prometheus)

After enabling, confirm GC is running and cluster health is intact:

```promql
# GC-related logs (check for "raft: log GC complete" in structured logs)
# Cluster should remain healthy throughout
grainfs_split_brain_suspected == 0

# Leader should be stable
changes(grainfs_raft_term_total[10m]) == 0
```

Check that `grainfs_balancer_pending_tasks` is not growing unexpectedly after GC.

## Cut-Over Checklist

Run this checklist before enabling managed mode in production:

- [ ] All nodes running and healthy (`grainfs_split_brain_suspected == 0`)
- [ ] No pending migrations (`grainfs_balancer_pending_tasks == 0`)
- [ ] Backup of `data/raft/` taken on all nodes
- [ ] Test node enabled and restarted cleanly
- [ ] `raft:meta:managed=true` key confirmed in test node's DB (use `badger-info`)
- [ ] GC log entries observed: `raft: log GC complete watermark=N`
- [ ] All remaining nodes enabled one at a time

## Rollback Procedure

**WARNING**: You cannot simply remove `--badger-managed-mode` from a node that has
already run GC. The node will fail to start with a format mismatch error.

**Option A: Keep managed mode but disable future GC** (safest):
Set `--raft-log-gc-interval 0`. The flag remains set (format is compatible) but
no new GC runs.

**Option B: Wipe and rejoin** (data loss for that node's Raft log):
1. Stop the node
2. Delete `data/raft/`
3. Restart without `--badger-managed-mode`
4. The node will rejoin as a fresh follower and catch up via AppendEntries or snapshot

**Option C: Restore from backup**:
1. Stop the node
2. Restore `data/raft/` from pre-GC backup
3. Restart without `--badger-managed-mode`
4. Verify node rejoins and catches up

## Recovery After GC + Restart

A node that restarts after GC will have a non-zero `firstIndex` in its Raft log
(matching the GC watermark). The leader handles this by sending log entries starting
from the follower's `firstIndex`. If the leader's in-memory log doesn't go far back
enough, it will send a snapshot instead.

This is automatic — no operator action required. Monitor the follower's commit index:

```promql
# Verify follower is catching up after restart
# (no GrainFS-specific metric; watch grainfs_http_requests_total for recovery signal)
```
