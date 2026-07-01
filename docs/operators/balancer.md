# Balancer Operator Runbook

`GrainFS` balancer runtime gossips disk usage, capability evidence, and request
rate across cluster nodes. The old Raft-backed shard migration path is retired:
the runtime still tracks disk skew, but it no longer moves object shards.

## Contents

1. [Overview](#overview)
2. [Configuration Flags](#configuration-flags)
3. [Healthy Operation Checks](#healthy-operation-checks)
4. [Metric Interpretation](#metric-interpretation)
5. [Alert Response](#alert-response)
6. [Tuning Guide](#tuning-guide)
7. [Emergency Stop](#emergency-stop)

## Overview

The balancer actor runs only on the Raft leader. Gossip senders, receivers, disk
collectors, and request-rate collectors still run as part of the balancer runtime.

Flow:

```text
GossipSender  -> broadcasts each node's DiskUsedPct over the cluster transport
GossipReceiver -> updates NodeStatsStore
BalancerProposer -> evaluates disk skew every 30 seconds
  imbalance > 20% -> marks balancer active and updates metrics
  imbalance < 5%  -> clears active hysteresis
RequestRateCollector -> writes RequestsPerSec for hot-node read reranking
```

Legacy `CmdMigrateShard` / `CmdMigrationDone` Raft command slots replay as
no-ops. Existing `pending-migration:` BadgerDB keys are inert legacy state; new
runtime code no longer creates or recovers them.

### Load signal (RequestsPerSec) and leader transfer

Besides disk usage, each node measures its own request rate off the hot path (a
30s rollup of the service-request counter) and writes it to the stats store, which
gossip propagates cluster-wide. This `RequestsPerSec` signal feeds BoundedLoads
hot-node read reranking and is exported as `grainfs_node_requests_per_sec`.

**Load-based leader transfer is disabled by default.** Transferring the (control-
plane) meta-Raft leadership in response to a (data-plane) S3 request-load signal is
unvalidated and risks election churn, so the `selectPeerByLoad → TransferLeadership`
path is gated off and never fires regardless of load. Disk-skew observability and
hot-node read reranking are unaffected. The `--balancer-leader-tenure-min` flag and
`grainfs_balancer_leader_transfers_total` metric below describe that gated path; it
is inert until the behavior is validated and the gate is enabled in code.

## Configuration Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--balancer-enabled` | `true` | Enables balancer runtime collection and skew evaluation. |
| `--balancer-gossip-interval` | `30s` | Gossip and disk-skew evaluation interval. |
| `--balancer-imbalance-trigger-pct` | `20.0` | Marks balancer active when `max-min` disk usage exceeds this percentage. |
| `--balancer-imbalance-stop-pct` | `5.0` | Clears active hysteresis once skew drops below this percentage. |
| `--balancer-migration-rate` | `1` | Legacy inert knob from the retired shard migration path. |
| `--balancer-leader-tenure-min` | `5m` | Minimum leader tenure before load-based leader transfer. |
| `--balancer-warmup-timeout` | `60s` | Grace period after node start before skew evaluation activates. |
| `--balancer-cb-threshold` | `0.90` | Disk usage fraction used by legacy destination circuit-breaker metrics. |
| `--balancer-migration-max-retries` | `3` | Legacy inert knob from the retired shard migration executor. |
| `--balancer-migration-pending-ttl` | `5m` | Legacy inert knob from retired pending-migration recovery. |

Conservative production example:

```bash
grainfs serve \
  --data ./data \
  --port 9000 \
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

# Skew/load activity
journalctl -u grainfs | grep 'component=balancer'
```

Expected sequence:

```text
INFO  balancer started component=balancer gossip_interval=30s trigger_pct=20
```

### Check Legacy Pending Migrations

`pending-migration:` keys are legacy inert state after shard migration retirement.
New code should not create more of them.

```bash
badger-cli list --prefix pending-migration: --db /data/meta | wc -l
```

## Metric Interpretation

| Metric | Meaning | Alert trigger |
| --- | --- | --- |
| `grainfs_balancer_gossip_total` | Gossip broadcast count. | Sustained gossip errors. |
| `grainfs_balancer_migrations_proposed_total` | Legacy retired migration counter. | Should remain `0`. |
| `grainfs_balancer_migrations_done_total` | Legacy retired migration counter. | Should remain `0`. |
| `grainfs_balancer_migrations_failed_total` | Legacy retired migration counter. | Should remain `0`. |
| `grainfs_balancer_shard_write_errors_total` | Legacy retired migration copy counter. | Should remain `0`. |
| `grainfs_balancer_shard_write_retries_total` | Legacy retired migration copy retry counter. | Should remain `0`. |
| `grainfs_balancer_migration_pending_ttl_expired_total` | Legacy retired pending-migration TTL counter. | Should remain `0`. |
| `grainfs_balancer_imbalance_pct` | Current `max-min` disk skew percentage. | Sustained value above trigger percentage. |
| `grainfs_balancer_pending_tasks` | Legacy `pending-migration:` key count. | Any growth means a stale binary is still running. |
| `grainfs_balancer_leader_transfers_total` | Load-based leader transfer count. Stays `0` while the gate is off (default). | Frequent increases indicate cluster instability or load concentration (only possible once the gate is enabled). |
| `grainfs_node_requests_per_sec` | Locally-measured request rate per node, gossiped as the load signal. | Large per-node skew indicates load concentration. |

## Alert Response

### Alert: Legacy Migration Counters Increase

The current binary no longer proposes or executes balancer shard migrations. If
legacy migration counters increase, a stale binary is still running somewhere.

1. Check deployed versions on all nodes.

2. Check leader logs:

   ```bash
   journalctl -u grainfs | grep 'migration.*failed\|migration.*error' | tail -20
   ```

2. Check source-node cluster transport connectivity:

   ```bash
   grainfs cluster --endpoint <data>/admin.sock peers
   ```

3. Check free space on the destination node:

   ```bash
   df -h /data
   ```

3. Stop stale balancer migration code by upgrading or disabling the old node:

   ```bash
   systemctl edit grainfs --force  # add --balancer-enabled=false
   systemctl restart grainfs
   ```

### Alert: Pending Migration Keys Keep Growing

New binaries no longer write `pending-migration:` keys. Sustained growth means a
stale binary is still proposing retired migration commands.

Disable or upgrade the stale node:

```bash
systemctl edit grainfs --force  # add --balancer-gossip-interval=120s
systemctl restart grainfs
```

### Alert: Leader Transfers Are Frequent

Load-based leader transfer is gated off by default, so
`grainfs_balancer_leader_transfers_total` stays `0` and this alert cannot fire in a
default deployment. If the gate has been enabled in code and the counter grows
frequently, one node is likely receiving too much request load.

1. Check each node's `grainfs_node_requests_per_sec` in Prometheus or gossip logs.
2. Look for hot bucket or key patterns in Prometheus metrics or gossip logs.
3. Review client load-balancing configuration.
4. Consider disabling the leader-transfer gate again until load is rebalanced.

## Tuning Guide

### Small Clusters, 3-5 Nodes

```bash
--balancer-gossip-interval=30s
--balancer-imbalance-trigger-pct=20.0
--balancer-imbalance-stop-pct=5.0
```

### Larger Clusters, 10+ Nodes

Use a slower cadence to reduce skew-evaluation churn.

```bash
--balancer-gossip-interval=60s
--balancer-imbalance-trigger-pct=30.0
--balancer-imbalance-stop-pct=10.0
--balancer-leader-tenure-min=10m
```

### After Adding A Node

The balancer waits through the warm-up window before activating disk-skew
observability. It does not move shards.

## Emergency Stop

Stop the balancer before incident debugging if gossip/load collection could make
the incident harder to read.

```bash
# Disable balancer runtime collection. Requires restart.
grainfs serve --balancer-enabled=false ...
```

## Test And Development

Use `GRAINFS_TEST_DISK_PCT` to exercise balancer behavior without filling a real
disk.

```bash
# Multi-node: stage the shared cluster transport key on disk before boot
# (every peer must hold the same keys.d/current.key).
mkdir -p ./data/keys.d
openssl rand -hex 32 > ./data/keys.d/current.key
GRAINFS_TEST_DISK_PCT=80 grainfs serve \
  --data ./data \
  --port 9000 \
  --peers peer-a:9001
```

`GRAINFS_TEST_DISK_PCT` replaces the `DiskCollector` `syscall.Statfs` call. An
invalid value fails startup.

Check disk usage in Prometheus:

```promql
grainfs_disk_used_pct{node_id="your-node-id"}
```
