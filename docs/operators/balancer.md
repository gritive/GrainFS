# Balancer Operator Runbook

`GrainFS` auto-balancer reduces disk usage skew across cluster nodes. This
runbook covers the operator checks, debugging steps, and tuning knobs.

## Contents

1. [Overview](#overview)
2. [Configuration Flags](#configuration-flags)
3. [Healthy Operation Checks](#healthy-operation-checks)
4. [Metric Interpretation](#metric-interpretation)
5. [Alert Response](#alert-response)
6. [Tuning Guide](#tuning-guide)
7. [Emergency Stop](#emergency-stop)

## Overview

The balancer runs only on the Raft leader.

Flow:

```text
GossipSender  -> broadcasts each node's DiskUsedPct over QUIC
GossipReceiver -> updates NodeStatsStore
BalancerProposer -> evaluates disk skew every 30 seconds
  imbalance > 20% -> proposes CmdMigrateShard through Raft
  imbalance < 5%  -> stops proposing migrations
FSM.applyMigrateShard -> MigrationExecutor.Execute()
  Step 1: copy shards from source to destination
  Step 2: propose CmdMigrationDone through Raft
  Step 3: wait for FSM commit through NotifyCommit
  Step 4: delete the source shard
```

If the migration channel is full, `GrainFS` persists the task under the BadgerDB
`pending-migration:` keyspace and recovers it after restart.

## Configuration Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--balancer-enabled` | `true` | Enables the balancer. |
| `--balancer-gossip-interval` | `30s` | Gossip and disk-skew evaluation interval. |
| `--balancer-imbalance-trigger-pct` | `20.0` | Starts migration when `max-min` disk usage exceeds this percentage. |
| `--balancer-imbalance-stop-pct` | `5.0` | Stops migration proposals once skew drops below this percentage. |
| `--balancer-migration-rate` | `1` | Maximum proposals per tick. Reserved for rate limiting. |
| `--balancer-leader-tenure-min` | `5m` | Minimum leader tenure before load-based leader transfer. |
| `--balancer-warmup-timeout` | `60s` | Grace period after node start to avoid false migrations during join or recovery. |
| `--balancer-cb-threshold` | `0.90` | Disk usage fraction above which a node is excluded as a migration target. |
| `--balancer-migration-max-retries` | `3` | Maximum shard-write retries. Uses exponential backoff with +/-20% jitter; `ErrPermanent` fails immediately. |
| `--balancer-migration-pending-ttl` | `5m` | TTL for stale pending migrations. Extended once after the Raft completion proposal, then cancelled on the second expiry. |

Conservative production example:

```bash
CLUSTER_KEY=$(openssl rand -hex 32)
grainfs serve \
  --data ./data \
  --port 9000 \
  --cluster-key "$CLUSTER_KEY" \
  --balancer-gossip-interval=60s \
  --balancer-imbalance-trigger-pct=30.0 \
  --balancer-imbalance-stop-pct=10.0 \
  --balancer-leader-tenure-min=10m
```

## Healthy Operation Checks

### Check Logs

```bash
# Balancer startup
journalctl -u grainfs | grep 'component=balancer'

# Migration activity
journalctl -u grainfs | grep 'migrate\|migration'
```

Expected sequence:

```text
INFO  balancer started component=balancer gossip_interval=30s trigger_pct=20
INFO  migration execute... component=migration task=mybucket/mykey/
INFO  migration complete  component=migration
```

### Check Pending Migrations

Many `pending-migration:` keys means the migration channel is staying under
backpressure.

```bash
badger-cli list --prefix pending-migration: --db /data/meta | wc -l
```

## Metric Interpretation

| Metric | Meaning | Alert trigger |
| --- | --- | --- |
| `grainfs_balancer_gossip_total` | Gossip broadcast count. | Sustained gossip errors. |
| `grainfs_balancer_migrations_proposed_total` | Number of proposed `CmdMigrateShard` commands. | Informational. |
| `grainfs_balancer_migrations_done_total` | Completed migration count. | Informational. |
| `grainfs_balancer_migrations_failed_total` | Failed migration count. | Investigate if nonzero and sustained. |
| `grainfs_balancer_imbalance_pct` | Current `max-min` disk skew percentage. | Sustained value above trigger percentage. |
| `grainfs_balancer_pending_tasks` | Number of `pending-migration:` keys. | Investigate if sustained above 10. |
| `grainfs_balancer_leader_transfers_total` | Load-based leader transfer count. | Frequent increases indicate cluster instability or load concentration. |

## Alert Response

### Alert: Migration Has Not Completed For Hours

1. Check leader logs:

   ```bash
   journalctl -u grainfs | grep 'migration.*failed\|migration.*error' | tail -20
   ```

2. Check source-node QUIC connectivity:

   ```bash
   grainfs cluster --endpoint <data>/admin.sock peers
   ```

3. Check free space on the destination node:

   ```bash
   df -h /data
   ```

4. Stop new migrations if the incident is active:

   ```bash
   systemctl edit grainfs --force  # add --balancer-enabled=false
   systemctl restart grainfs
   ```

### Alert: Pending Migration Keys Keep Growing

The migration channel has capacity 256. Sustained growth usually means shard
copy speed is below proposal speed, or the gossip interval is too short.

Reduce proposal frequency:

```bash
systemctl edit grainfs --force  # add --balancer-gossip-interval=120s
systemctl restart grainfs
```

### Alert: Leader Transfers Are Frequent

If `grainfs_balancer_leader_transfers_total` grows frequently, one node is
likely receiving too much request load.

1. Check each node's `requests_per_sec` through gossip logs or Prometheus.
2. Look for hot bucket or key patterns in Prometheus metrics or gossip logs.
3. Review client load-balancing configuration.

## Tuning Guide

### Small Clusters, 3-5 Nodes

```bash
--balancer-gossip-interval=30s
--balancer-imbalance-trigger-pct=20.0
--balancer-imbalance-stop-pct=5.0
```

### Larger Clusters, 10+ Nodes

Use a slower cadence to avoid migration storms.

```bash
--balancer-gossip-interval=60s
--balancer-imbalance-trigger-pct=30.0
--balancer-imbalance-stop-pct=10.0
--balancer-leader-tenure-min=10m
```

### After Adding A Node

The balancer waits through the warm-up window before proposing migrations. This
prevents false moves while a joining or recovering node is still settling.

## Emergency Stop

Stop the balancer before incident debugging if migration traffic could make the
incident harder to read.

```bash
# Disable future balancer proposals. Requires restart.
grainfs serve --balancer-enabled=false ...

# Existing shard copies are not interrupted. Let them finish.

# Check pending migration keys only if the restart does not recover them.
```

Forced interruption can leave copied shards on the destination. The scrubber is
expected to clean orphan shards.

## Test And Development

Use `GRAINFS_TEST_DISK_PCT` to exercise balancer behavior without filling a real
disk.

```bash
CLUSTER_KEY=$(openssl rand -hex 32)
GRAINFS_TEST_DISK_PCT=80 grainfs serve \
  --data ./data \
  --port 9000 \
  --cluster-key "$CLUSTER_KEY" \
  --peers peer-a:9001
```

`GRAINFS_TEST_DISK_PCT` replaces the `DiskCollector` `syscall.Statfs` call. An
invalid value fails startup.

Check disk usage in Prometheus:

```promql
grainfs_disk_used_pct{node_id="your-node-id"}
```
