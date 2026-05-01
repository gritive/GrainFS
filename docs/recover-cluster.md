# Recover Cluster

RecoverCluster is an offline disaster-recovery flow for the case where a
GrainFS cluster has lost Raft majority and cannot elect a leader.

It does not repair a live cluster in place. It promotes the latest persisted
metadata Raft snapshot from an offline source data directory into a fresh
single-node target.

## Drill

1. Stop all GrainFS processes that can access the source data directory.
2. Pick an empty target directory.
3. Inspect the source without mutation:

```bash
grainfs recover cluster plan \
  --source-data /var/lib/grainfs \
  --target-data /var/lib/grainfs-recovered \
  --new-node-id node-recovered \
  --new-raft-addr 10.0.0.10:19100 \
  --badger-managed-mode
```

4. Execute only after reviewing the plan:

```bash
grainfs recover cluster execute \
  --source-data /var/lib/grainfs \
  --target-data /var/lib/grainfs-recovered \
  --new-node-id node-recovered \
  --new-raft-addr 10.0.0.10:19100 \
  --badger-managed-mode
```

5. Start the recovered node from the target directory. It starts read-only.

6. After external data-plane checks, mark it writable:

```bash
grainfs recover cluster verify \
  --target-data /var/lib/grainfs-recovered \
  --mark-writable
```

## Safety Rules

- `plan` opens the source Raft store read-only and reads snapshot metadata only.
- `execute` refuses dirty targets containing `meta`, `raft`, `groups`, `data`,
  `blobs`, `wal`, or `snapshots`.
- V1 refuses sources with `groups/*` because per-group Raft recovery needs its
  own consistency design.
- Snapshots captured during joint consensus are refused unless the operator
  explicitly reruns `plan`/`execute` with `--strip-joint-state` to recover as a
  clean single-node cluster.
- Active recovered snapshot membership is rewritten to exactly one voter: the
  new node ID. Original membership is preserved in
  `recovery/recovercluster.json`.
- Recovered targets write `writable=false` and `serve` wraps the storage backend
  with a write gate until verification marks the marker writable.

## Rollback

The source directory is not changed by `plan` or `execute`. If recovery output
looks wrong, stop the recovered node and discard the target directory. Re-run
`plan` with corrected arguments before creating a new target.
