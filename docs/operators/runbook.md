# `GrainFS` Production Deployment Runbook

## Overview

This runbook documents the step-by-step procedure to deploy `GrainFS` to production.
Record drill dates, runners, and target environments in the deployment log for
the cluster.

---

## Admin UDS Bootstrap And Permissions

The admin Unix socket at `<data-dir>/admin.sock` is the **sole bootstrap path** for new
clusters. It is also used by `grainfs cluster ...`, `grainfs iam ...` subcommands.

### Permissions

`GrainFS` creates the socket with mode `0660` (hard-fail on chmod failure). Operators in the
admin group can connect; others cannot.

- Default: socket owned by the user running `grainfs serve`, group is the user's primary
  group.
- Multi-operator setups: pass `--admin-group <groupname>` to chown the socket to a shared
  group. All operators must belong to that group.
- File mode is hard-coded; do not chmod looser.

### Bootstrap a new cluster

```bash
# 1) Start grainfs (genesis self-seeds the cluster transport key).
grainfs serve --data ./data --port 9000 &

# 2) Create the first admin SA (returns one-time secret_key).
grainfs iam sa create admin --endpoint ./data/admin.sock
# {"sa_id":"sa-default","access_key":"GRAIN...","secret_key":"<one-time>","grants":[{"bucket":"*","role":"admin"}]}

# 3) Use the credentials for S3 traffic.
export AWS_ACCESS_KEY_ID=GRAIN...
export AWS_SECRET_ACCESS_KEY=<secret>
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
```

### Race condition

If two operators concurrently call `iam sa create` on a fresh cluster, only the first
proposal wins (idempotent FSM Apply on `DefaultSAID`). The second operator receives
`409 Conflict`; their displayed access_key is invalid. Re-run `iam sa create <name>` to
create a regular SA via the non-bootstrap path.

---

## Pre-Flight Checklist

Complete ALL items before proceeding with deployment. If ANY item fails, do NOT deploy.

### Infrastructure Readiness

- [ ] **Server resources**: Minimum 4 CPU, 8GB RAM, 100GB disk
- [ ] **Network connectivity**: Port 9000 is reachable by S3 clients. Keep NFSv4 2049, NBD 10809, and 9P listeners on loopback, private networks, or firewall-restricted addresses when enabled.
- [ ] **Disk mounting**: Data directory mounted on reliable storage (SSD recommended)
- [ ] **Backup repository**: Restic repo initialized and accessible
- [ ] **Monitoring**: Prometheus scraping configured and receiving data

### Configuration Verification

- [ ] **Environment variables set**:
  ```bash
  export GRAINFS_DATA_DIR=/path/to/production/data
  export GRAINFS_PORT=9000
  export GRAINFS_ACCESS_KEY="$(secret-manager read grainfs/access-key)"
  export GRAINFS_SECRET_KEY="$(secret-manager read grainfs/secret-key)"
  # For a multi-node cluster with an externally-managed transport key, stage it
  # on disk before boot (a file, never an argv/env literal). A single genesis
  # node self-seeds and needs no staging.
  install -d -m 0700 "$GRAINFS_DATA_DIR/keys.d"
  secret-manager read grainfs/cluster-key > "$GRAINFS_DATA_DIR/keys.d/current.key"
  chmod 0600 "$GRAINFS_DATA_DIR/keys.d/current.key"
  ```
- [ ] **Cluster membership** (if applicable):
  ```bash
  # Verify peer IPs are correct
  cat /etc/grainfs/peers.txt
  ```
### Safety Checks

- [ ] **Verify existing data** (if migrating):
  ```bash
  ls -la $GRAINFS_DATA_DIR/badger
  ls -la $GRAINFS_DATA_DIR/blobs
  ```
  Expected: Data directories exist and are non-empty

---

## Zero-Ops Incident Checks

Use the incident API after startup, repair drills, corruption drills, or resource warnings to confirm the cluster is not silently degraded.

```bash
curl -s http://localhost:9000/api/incidents | jq .
```

Expected healthy steady state: an empty list, or only historical incidents in terminal states such as `fixed` or `isolated` with a clear `next_action`.

For a repaired missing shard, verify the incident proof before closing the operational event:

```bash
INCIDENT_ID=<incident-id>
RECEIPT_ID=$(curl -s http://localhost:9000/api/incidents/$INCIDENT_ID | jq -r '.proof.receipt_id')
curl -s -H "Authorization: <sigv4 header>" http://localhost:9000/api/receipts/$RECEIPT_ID | jq .
```

If an incident is `proof-unavailable`, check HealReceipt signing and persistence before treating the repair as audit-complete. If an incident is `isolated`, review the named object version and restore or delete it according to the data owner policy; unrelated objects in the bucket should continue serving. If a corruption incident is `needs-human`, the automatic isolation action failed; restore the object from a clean copy or delete the quarantined version before closing the event.

For EC scrub skips caused by legacy raw shards without CRC envelopes, watch the unverified-shard counter before deciding whether to run a rewrite or migration:

```bash
curl -s http://localhost:9000/metrics | grep '^grainfs_ec_scrub_unverified_shards_total'
```

If `reason="legacy_no_crc"` is non-zero, scrub can read the shard bytes but cannot prove bit-level integrity. Treat those shards as migration candidates, not healthy repaired data.

### Service Performance Metrics

Use service metrics when a generic HTTP latency or error-rate alert fires and you need to isolate the affected surface.

Metric families:

- `grainfs_service_requests_total{service,operation,method,status_class}`
- `grainfs_service_request_duration_seconds_bucket{service,operation,method,status_class,le}`
- `grainfs_service_request_bytes_total{service,operation,method,status_class}`
- `grainfs_service_response_bytes_total{service,operation,method,status_class}`

Useful queries:

```promql
histogram_quantile(0.99, sum(rate(grainfs_service_request_duration_seconds_bucket[5m])) by (service, operation, le))
```

```promql
sum(rate(grainfs_service_requests_total{status_class=~"4xx|5xx"}[5m])) by (service, operation, status_class)
```

```promql
sum(rate(grainfs_service_response_bytes_total[5m])) by (service, operation)
```

Labels intentionally exclude bucket, key, access key, raw path, and error strings. Treat `service` and `operation` values as the stable operator-facing contract.

### Operator State Metrics

Use operator state metrics when the symptom is "is the cluster healthy?" rather
than a specific HTTP operation. These metrics are intentionally aggregate-only:
bucket names, volume names, peer addresses, keys, paths, access keys, raw errors,
and dynamic Raft group IDs never appear as labels. Drill into named resources
through the admin APIs after an aggregate metric indicates a problem.

Metric families:

- `grainfs_server_up{node_id}`
- `grainfs_server_info{node_id,version}`
- `grainfs_cluster_members{state}`
- `grainfs_cluster_quorum_available`
- `grainfs_raft_role{node_id,group,role}`
- `grainfs_raft_term{node_id,group}`
- `grainfs_raft_commit_index{node_id,group}`
- `grainfs_raft_applied_index{node_id,group}`
- `grainfs_raft_apply_lag{node_id,group}`
- `grainfs_buckets_by_state{state}`
- `grainfs_volumes_by_health{health}`
- `grainfs_volume_capacity_bytes_total`
- `grainfs_volume_allocated_bytes_total`
- `grainfs_operator_state_scrape_errors_total{source}`

Useful queries:

```promql
min(grainfs_cluster_quorum_available) == 0
```

```promql
sum(grainfs_cluster_members{state=~"unhealthy_.*_peer|unknown_peer"}) by (state)
```

```promql
sum(grainfs_raft_role{role="leader"}) by (group)
```

```promql
max(grainfs_raft_apply_lag) by (node_id, group)
```

```promql
sum(grainfs_volumes_by_health{health!="healthy"}) by (health)
```

```promql
increase(grainfs_operator_state_scrape_errors_total{source="buckets"}[10m])
```

After a node restart, committed objects are durable on disk (small / no-redundancy shards were fsynced at write time; large redundant shards are EC-reconstructable) — there is no data WAL replay since S4. Missing or corrupt EC shards are healed lazily: read-time EC reconstruction serves reads, and the periodic placement monitor + background scrubber proactively repair between boots.

The periodic placement monitor proactively detects and repairs segment (`<key>/segments/<blobID>`) and coalesced (`<key>/coalesced/<id>`) EC shards for **latest-version** objects between boots, complementing read-time reconstruction. Non-latest-version segment/coalesced shards are not proactively scanned by the placement monitor; they remain covered by read-time EC reconstruction. Corrupt segment or coalesced shards trigger quarantine of the parent object (object-level, using the scanned version). The metric `grainfs_placement_monitor_invalid_ec_ref_total{kind="segment|coalesced"}` is incremented when a ref has malformed placement (`len(NodeIDs) != ECData+ECParity`); a non-zero rate indicates corrupt object metadata and warrants investigation.

The placement monitor quarantines an object **only** on confirmed shard corruption (CRC mismatch, structural decode failure, truncation, or AEAD auth failure). Every other non-ENOENT local shard read error — `EIO`, `EMFILE` ("too many open files"), `EBUSY`, permission denied, or unknown — is treated as **transient**: it is logged at Debug level, counted by `grainfs_placement_monitor_transient_read_error_total{kind="object_version|segment|coalesced|unknown"}`, and skipped (the next scan retries it), **not** quarantined. This prevents a transient or failing-disk node-day from mass-quarantining otherwise healthy objects. A sustained transient rate is a signal about the **node's** disk/FD health (failing disk, fd exhaustion), not about the objects — investigate the node (`dmesg`, SMART, open-fd count, `--max-open-files`), not the named objects.

## NFS Multi-Bucket Export

Use explicit exports for every bucket mounted through NFSv4:

```bash
export GRAINFS_ADMIN_SOCKET=<data>/admin.sock
grainfs nfs export list
grainfs nfs export add <bucket>
grainfs nfs debug <bucket>
```

Triage:

- `No such file or directory` at the pseudo-root: check `grainfs nfs debug <bucket>` and register the bucket if `registered=false`.
- Stale handles after export changes: unmount/remount affected clients and check the export generation in `grainfs nfs debug`.
- Backend exists but NFS is missing: create the export; S3 bucket creation alone does not expose the bucket over NFS.

Metrics:

- `grainfs_nfs_exports_total{state}`
- `grainfs_nfs_export_propagation_seconds`
- `grainfs_nfs_lookup_unknown_export_total`
- `grainfs_nfs_revoked_stateids_total{reason}`

## NFS Write Buffer

NFS WRITE ops are coalesced into a local file under `<data>/nfs-writebuf/`. The buffer flushes to backend `PutObject` on:

- NFS COMMIT op
- `SETATTR` truncate (discard semantics — buffered writes are dropped)
- Idle timeout (`--nfs-write-buffer-idle`, default 30s)
- Server shutdown drain

### Disk sizing

Plan for `max(concurrent NFS objects) × max(object size)` of disk under `<data>/nfs-writebuf/`. The idle timeout bounds dwell time. For most workloads the live set is small (open files only); long-lived idle keys are flushed on the next idle tick.

### Cluster mode limitation

Buffering is per-node. Two NFS clients mounted to **different** GrainFS nodes that write the **same** key may see last-write-wins on flush (no cross-node coordination yet). For strict consistency in a multi-node cluster: pin all NFS clients to a single GrainFS node, or wait for cluster-scoped buffer support.

### Recovery

On startup the buffer dir is scanned and leftover files are flushed to backend. If a flush fails (backend unreachable), files are renamed to `<sha1>.failed.<reason>.<timestamp>` so subsequent writes for the same key cannot collide. Inspect `*.failed.*` files manually and either:

- Replay them via `dd if=<failed-file> | aws s3 cp - s3://<bucket>/<key>` (read the `.meta` sidecar for `bucket`/`key`), or
- Delete them after confirming the data is recoverable from elsewhere.

### Disable buffering

Set `--nfs-write-buffer-idle=0` to disable. NFS WRITE then falls back to the per-write RMW path (older behaviour, ~10× slower on sequential workloads, no buffer correctness concerns).

Grafana example: `docs/observability/nfs-multi-export.json`.

For `fd_exhaustion_risk`, inspect the decision text first. It includes current FD usage, projected threshold ETA when available, and best-effort categories such as `socket`, `badger`, or `nfs_session`.

```bash
curl -s http://localhost:9000/api/incidents/fd-<node-id> | jq .
curl -s http://localhost:9000/metrics | grep '^grainfs_fd_'
lsof -p $(pgrep -n grainfs) | awk '{print $5}' | sort | uniq -c | sort -nr | head
```

If the incident is `diagnosed`, reduce connection churn or raise the process FD limit before the ETA expires. If it is `blocked`, raise `LimitNOFILE`/`ulimit -n`, check for socket/session leaks, and restart gracefully after draining traffic. The watcher resolves the incident after FD usage stays below the warning threshold for `--fd-recovery-window`.

For Badger startup recovery, inspect the role and action first:

```bash
curl -s http://localhost:9000/api/incidents | jq '.[] | select(.cause | startswith("badger_"))'
grep -E "badger role startup probe|badger startup recovery|optional badger role disabled" /var/log/grainfs/production.log
```

Failures that happen before the incident-state DB opens are also written under `<data>/.recovery/entries/`. Do not delete that directory during triage; the next boot that can open incident-state imports pending entries and marks them under `<data>/.recovery/imported/`.

- `badger_startup_blocked` or `badger_open_failed` with `block_startup`: restore the named Badger role from a clean snapshot, or fix the disk, lock, or permission error before retrying startup.
- `badger_read_only_admitted` or log message `badger startup recovery read-only gate enabled`: read paths remain available, but storage writes and mutating admin APIs return HTTP 503 with code `RecoveryReadOnly`. Repair or restore the failed group-state role, then restart and verify normal writes.
- `disable_feature`: the server started without the optional role. Receipts or incident-state behavior may be unavailable until the role directory is repaired and the process restarts.

---

## Deployment Procedure

### Step 1: Stop Existing `GrainFS` Process (if upgrading)

```bash
# Find existing process
PID=$(pgrep -f "grainfs serve")
if [ -n "$PID" ]; then
  echo "Stopping existing GrainFS (PID: $PID)"
  kill -TERM $PID

  # Wait for graceful shutdown (max 30 seconds)
  for i in {1..30}; do
    if ! ps -p $PID > /dev/null; then
      echo "GrainFS stopped gracefully"
      break
    fi
    echo "Waiting for shutdown... ($i/30)"
    sleep 1
  done

  # Force kill if still running
  if ps -p $PID > /dev/null; then
    echo "Force killing GrainFS"
    kill -9 $PID
  fi
fi
```

---

### Step 2: Deploy New Binary

```bash
# Backup old binary
cp /usr/local/bin/grainfs /usr/local/bin/grainfs.backup.$(date +%Y%m%d-%H%M%S)

# Copy new binary
cp grainfs /usr/local/bin/grainfs
chmod +x /usr/local/bin/grainfs

# Verify version
grainfs --version
```

Expected: Version string matches expected deployment version

**v0.0.106.0+ rolling upgrade gate (IAM bucket-scoped keys):** Create scoped
keys (`grainfs iam key create --bucket <name>`) only after every node runs
v0.0.106.0 or newer. v0.0.105.0 and older followers treat the new Raft command
`IAMKeyCreateScoped` (type 30) as an unknown graceful no-op and emit a warning.
The leader returns success, but some followers may not store the key, so the key
can disappear after a leadership change. During mixed-version windows, create
only unrestricted keys without `--bucket`.

---

### Step 3: Start `GrainFS`

`grainfs serve` no longer accepts `--access-key`/`--secret-key`. The first cluster
operator runs the admin SA bootstrap (see "Admin UDS Bootstrap And Permissions"
above) immediately after the first node starts. S3 clients, such as
`aws --endpoint-url`, then use the resulting `access_key` and `secret_key`;
`GrainFS` stores only the HMAC hash. Export those credentials as
`$GRAINFS_ACCESS_KEY` and `$GRAINFS_SECRET_KEY` for the rest of this runbook's
`aws` examples.

**Local mode:** (genesis self-seeds the transport key)
```bash
grainfs serve \
  --data $GRAINFS_DATA_DIR \
  --port $GRAINFS_PORT \
  > /var/log/grainfs/production.log 2>&1 &
```

**Cluster mode:** (each node reads the staged `keys.d/current.key` from above)
```bash
# First node
grainfs serve \
  --data $GRAINFS_DATA_DIR \
  --port $GRAINFS_PORT \
  --node-id node-1 \
  --raft-addr node-1.example.com:9001 \
  > /var/log/grainfs/production.log 2>&1 &

# Additional nodes join through any existing member's Raft address
grainfs serve \
  --data $GRAINFS_DATA_DIR \
  --port $GRAINFS_PORT \
  --node-id node-2 \
  --raft-addr node-2.example.com:9001 \
  --join node-1.example.com:9001 \
  > /var/log/grainfs/production.log 2>&1 &
```

### Optional: Pull-through cache for migration (v0.0.123.0+)

> **Rolling-upgrade ordering:** `bucket-upstream` records propagate via
> MetaCmdType IDs 32 and 33, introduced in v0.0.123.0. During a mixed-version
> upgrade, do not issue `grainfs bucket upstream put/delete` while any node still
> runs v0.0.122 or earlier. Older followers ignore the Raft entry during apply.
> Snapshot replay restores the records on the next snapshot install, but the
> follower view remains inconsistent during the apply gap. Wait until every node
> reports v0.0.123.0 or newer before configuring bucket upstreams. v0.0.133.0
> moved the CLI and admin path; FSM and snapshot formats did not change, so
> v0.0.122 through v0.0.133 retain Raft compatibility.

> **Unknown MetaCmd alert:** `grainfs_unknown_metacmd_total{type}` increments
> when a node ignores a Raft metadata command it does not recognize or handle.
> Treat `GrainFSUnknownMetaCmdIgnored` as a version-skew or implementation-gap
> warning: confirm every node's version, pause use of the new feature, and
> finish the rolling upgrade before relying on state from that MetaCmd. `GrainFS`
> tracks transport capability-exchange rejections separately with
> `grainfs_transport_ce_total{role,outcome,reason}`.

> **Capability gate rejection:** `grainfs_capability_reject_total{capability,scope,severity,operation,forced}` increments when an admin/API path tries to use a feature that not every required Raft member can apply. For hard persisted features, do not force the operation. Finish the rolling upgrade, confirm every required node advertises the capability, then retry. Admin UDS errors include missing/stale node IDs; public data-plane errors intentionally do not expose node topology.

If migrating from another S3-compatible source, register the upstream per
bucket via the admin UDS. `GrainFS` removed the `--upstream*` cmdline flags in
v0.0.123.0; the IAM-managed approach replaces them.

```bash
# Register the upstream for bucket "legacy-data".
grainfs bucket upstream put legacy-data \
  --endpoint /grainfs/data/admin.sock \
  --endpoint-url http://upstream-minio:9000 \
  --access-key legacy-ak \
  --secret-key legacy-sk

grainfs bucket upstream get legacy-data --endpoint /grainfs/data/admin.sock
grainfs bucket upstream list --endpoint /grainfs/data/admin.sock
grainfs bucket upstream delete legacy-data --endpoint /grainfs/data/admin.sock
```

Pull-through is read-only and on-miss only: the first GET on a missing
object proxies upstream and stores locally; subsequent GETs hit local
cache. Migration "completion" semantics (cutover, progress) are not yet
implemented. Track migration progress from the upstream side until `GrainFS` has
first-class cutover and progress reporting.

---

### Step 4: Post-Deployment Verification

**Wait for startup (10 seconds):**
```bash
sleep 10
```

**Health check:**
```bash
# Check process is running
pgrep -f "grainfs serve"
```
Expected: Process ID printed

**API health check:**
```bash
# Using AWS CLI
AWS_ACCESS_KEY_ID=$GRAINFS_ACCESS_KEY \
AWS_SECRET_ACCESS_KEY=$GRAINFS_SECRET_KEY \
AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://localhost:9000 s3 ls
```
Expected: No error, bucket list returned (may be empty)

**Verify metrics:**
```bash
curl http://localhost:9000/metrics | grep grainfs_up
```
Expected: `grainfs_up 1`

For cluster scrub trigger execution, also check the bounded executor metrics:

```bash
curl http://localhost:9000/metrics | grep '^grainfs_execution_cluster_'
```

Watch queue depth, retries, timeouts, worker failures, aggregation failures, and
job duration before treating cluster scrub admission or completion as healthy.

For multi-Raft deployments, also inspect data-group Raft health:

```bash
curl http://localhost:9000/v1/cluster/health | jq '.data_groups'
```

`leaderless > 0` means at least one data group has no observed leader. `lagging > 0`
means at least one group has follower replication lag. Check `groups[].issues`
for the concrete per-group reason, such as `leaderless`, `unwired`, or
`peer_lag`, before debugging object write failures from logs.

---

## Growing the Cluster (Adding Placement Groups)

`GrainFS` (v0.0.543.0+, Phase 7) can add raft placement groups to a running cluster without remapping existing objects. The object→group placement is pinned per topology *generation*; new groups become a new generation, and reads probe newest-generation-first (existing objects stay served from the prior generation).

Two steps:

1. **Form the new groups.** Add nodes to the cluster (zero-CA join). New shard groups are formed automatically as nodes join (`expandShardGroupsForJoinedNode`). At this point the groups exist but are not yet used for object placement.

2. **Activate them.** Run the operator command to record the current shard groups as a new placement generation:

```bash
grainfs cluster expand-placement --endpoint <data>/admin.sock
```

The command reports `previous groups`, `new groups`, and the `active set`. It is a no-op when no new groups are present. If a newly-joined group is *wider* (more peers) than the existing groups, narrower groups drop out of the active set (the command prints a `WARNING` with the dropped groups — they stop receiving new writes but their existing objects stay readable).

Notes:

- **Irreversible:** a placement generation, once recorded, is never removed. There is no group-*removal* path.
- **Run on the leader.** The command proposes through the meta-raft; on a follower it returns an error — target the current leader (`grainfs cluster status`).
- **Reads stay correct** during and after growth: existing objects are found via the older generation; the cross-generation last-writer-wins fence resolves the brief add-window.
- **Not yet validated:** multi-node concurrent growth under heavy load and throughput parity were not benchmarked (the fence arms per-node on the generation-count transition). Grow during a maintenance window for the first time.

---

## Rollback Procedure

If ANY verification step fails, execute rollback immediately.

### Option 1: Binary Rollback (if issue is with new binary)

```bash
# Stop new binary
pgrep -f "grainfs serve" | xargs kill

# Restore old binary
LATEST_BACKUP=$(ls -t /usr/local/bin/grainfs.backup.* | head -1)
cp $LATEST_BACKUP /usr/local/bin/grainfs

# Restart with old binary (reads the on-disk keys.d/current.key)
grainfs serve \
  --data $GRAINFS_DATA_DIR \
  --port $GRAINFS_PORT \
  > /var/log/grainfs/production.log 2>&1 &
```

---

## Membership Operations (Day-2)

### Evicting a permanently-dead node

If a cluster node fails past recovery, remove it from the meta-Raft voter set so
quorum math reflects the surviving members. Run the command **on the leader
node**. Use the admin Unix socket at `<data-dir>/admin.sock`; `GrainFS` creates it
with mode 0660 plus the admin group.

Export the socket path once for the procedure:

```bash
export ENDPOINT=/var/run/grainfs/admin.sock   # or <data-dir>/admin.sock
```

1. Identify the dead voter. `cluster peers` lists the current metaRaft voter
   set; cross-reference it with `cluster status` or external monitoring. Normal
   rows use node IDs. `GrainFS` shows unresolved legacy raft-address rows as
   `unresolved_legacy` so operators can still see the row that blocks membership
   mutation:

   ```bash
   grainfs cluster --endpoint $ENDPOINT peers
   #   NODE_ID          RAFT_ADDR          ROLE      STATE
   #   node-2           127.0.0.1:19102    follower  configured
   #   127.0.0.1:19103  127.0.0.1:19103    follower  unresolved_legacy

   grainfs cluster --endpoint $ENDPOINT status --format json   # includes peer_snapshot
   ```

2. The server runs pre-flight checks automatically. The peer snapshot
   membership-mutation policy counts `self` and rows with fresh successful
   metaRaft AppendEntries evidence as `live`; it treats `configured` rows as
   unknown. Failed heartbeats alone do not mark a peer display-down for this
   policy. If removal would drop the post-removal voter count below quorum, or
   another unresolved legacy row makes identity ambiguous, the command refuses
   unless `--force`:

   ```bash
   grainfs cluster --endpoint $ENDPOINT remove-peer node-2 --yes
   ```

3. Verify that the voter set shrank and `GrainFS` recorded an audit event:

   ```bash
   grainfs cluster --endpoint $ENDPOINT peers
   grainfs cluster --endpoint $ENDPOINT events --type cluster-remove-peer --since 1h
   ```

**`--force` semantics**: bypasses pre-flight only. It does not bypass the
engine. Use it when the operator has confirmed the peer is permanently lost and
the joint-consensus commit can still progress, such as 3-of-5 alive while
removing 1 dead voter. Clusters that have lost quorum cannot be recovered with
`remove-peer --force`; there is currently no built-in offline recovery command
(the previous `recover cluster` flow was removed in v0.0.343.0 pending a redesign
around failure-domain boundaries) — restore from a backup or rebuild.

**Removing the leader**: the engine commits the joint Cnew, then the leader
steps down via commit-time wakeup. The remaining voters elect a new leader. The
operator does not need a separate `transfer-leader` step.

### Adding a node with Zero-CA invite join

Use Zero-CA invite join when adding a brand-new node without copying
`keys/0.key`, `cluster.id`, or `keys.d/current.key` to the node first. The
detailed procedure is in [`zero-ca-cluster-join.md`](zero-ca-cluster-join.md).

Mint an invite on the leader:

```bash
grainfs cluster invite create --endpoint $ENDPOINT --ttl 1h
```

Start the joining node with the printed bundle:

```bash
GRAINFS_INVITE_BUNDLE='<bundle-token>' grainfs serve \
  --data /var/lib/grainfs-b \
  --node-id node-b \
  --raft-addr node-b:7001
```

Verify membership:

```bash
grainfs cluster --endpoint $ENDPOINT peers
```

If the cluster is ready to remove the shared cluster-key accept path, run:

```bash
grainfs cluster --endpoint $ENDPOINT complete-cutover
```

Successful output:

```text
Zero-CA cutover complete: cluster key dropped, connections recycled.
```

### Revoking a Zero-CA node identity

Use `revoke-node` when the node identity itself should be blocked from future
membership, not only removed from the current voter set. The command removes the
peer from meta-Raft membership, burns pending invites for the same node ID, adds
the node transport SPKI to the replicated denylist, and closes cached cluster
transport connections to that peer.

Run the command on the leader through the admin socket:

```bash
grainfs cluster --endpoint $ENDPOINT revoke-node node-2
```

Verify that the node no longer appears in the voter set:

```bash
grainfs cluster --endpoint $ENDPOINT peers
```

---

## NFSv4 Conformance Testing

`GrainFS` tracks NFSv4 RFC 8881 attribute behavior in `docs/reference/nfsv4-attribute-audit.md`. Update the audit in the same PR as any NFS attribute behavior change.

The external pynfs suite is advisory and non-blocking:

```bash
make test-pynfs-colima
```

The runner clones the pinned upstream pynfs commit, starts a local `GrainFS` server, creates a test bucket/export, and writes results to `tests/conformance/results/summary.json` plus a timestamped log. Failures should be copied into `TODOS.md` follow-ups; they do not block ordinary PRs unless a PR explicitly changes NFS protocol behavior.

---

## Monitoring Setup

### Prometheus Alerts

Ensure alerts from `alerts/prometheus/rules.yml` are configured:

```bash
# Verify alerts are loaded
curl http://prometheus:9090/api/v1/rules | grep grainfs
```

Expected: `GrainFS` alert rules appear in output

### Log Aggregation

Forward logs to monitoring system:

```bash
# rsyslog configuration (example)
echo "if \$programname == 'grainfs' then @@log-server:514" >> /etc/rsyslog.d/grainfs.conf
systemctl restart rsyslog
```

---

## Common Issues and Fixes

### Issue: `GrainFS` won't start

**Symptoms:** Process exits immediately, "address already in use"

**Diagnosis:**
```bash
# Check if port is in use
lsof -i :9000
```

**Fix:**
```bash
# Kill conflicting process
kill -9 $(lsof -t -i :9000)
```

### Issue: High memory usage

**Symptoms:** OOM kills, swap usage

**Diagnosis:**
```bash
# Check memory
free -h
# Check GrainFS memory
ps aux | grep grainfs
```

**Fix:**
- Increase server memory
- Reduce cache size (if configurable)
- Restart `GrainFS`

### Issue: Slow API response

**Symptoms:** P99 latency > 100ms

**Diagnosis:**
```bash
# Check disk I/O
iostat -x 1
# Check network
ss -s
```

**Fix:**
- Check disk saturation (move to faster storage)
- Check network bandwidth
- Check for lock contention

### Issue: AppendObject HTTP 503 SlowDown

**Symptoms:** `503 SlowDown` responses on AppendObject requests; clients reporting
`Retry-After: 1` backoff loops; `grainfs_cluster_append_forward_buffer_rejected_total`
counter climbing.

**Diagnosis:**
```bash
curl http://<node>:9000/metrics | grep -E 'grainfs_cluster_append_forward_buffer_(inflight_bytes|rejected_total)'
```

`inflight_bytes` near the configured pool size means the forward buffer is
saturated. This is expected backpressure under sustained high concurrency, but
chronic saturation means the pool is undersized for the workload.

**Fix:**
- Increase pool: `--cluster-append-forward-buffer-total-bytes` (default 512 MiB).
- If individual requests are large, raise per-request cap:
  `--cluster-append-forward-buffer-max-per-request` (default 64 MiB).
- If clients want bigger objects, raise per-object cap:
  `--append-size-cap-bytes` (default 5 TiB).
- Calibrate with `warp append --concurrent 32 --duration 60s --obj.size '1-16MiB'`
  and target rejection ratio < 1%.

### Issue: Disk usage drift on AppendObject buckets

**Symptoms:** disk consumption growing faster than committed object size;
`grainfs_scrub_orphan_segments_found_total` increasing across scrub cycles.

**Diagnosis:** AppendObject best-effort cleanup failed on one of the 3 hot paths
(propose rejection, coalesce-time EC shard write, coalesce post-unlink). The
scrubber sweeps raw segment orphans automatically — track:

```bash
curl http://<node>:9000/metrics | grep -E 'grainfs_scrub_orphan_segment(s_found|s_deleted|_sweep_capped|_walk_errors|_delete_errors)'
```

Found > deleted over multiple cycles indicates the sweep cap is the bottleneck.
Walk/delete errors > 0 indicates filesystem permission or I/O issues.

**Fix:**
- Default age gate is 5 minutes. Long-running writes >5min may need a longer
  gate: `--scrub-orphan-age 10m` (and 1s in tests).
- A retention window gates deletion on top of the age gate: an orphan is deleted
  only after it has stayed unreferenced for `--segment-gc-retention` (default
  24h). Set `--segment-gc-retention 0` to disable the time-based grace period
  (the 5-minute age gate still applies). If disk reclamation lags after a large
  delete/overwrite, this window is usually why — shorten it deliberately, not
  reflexively (it protects in-flight reads and recent-write margin).
- Deletion is reference-counted: a segment is removed only when no live object
  version and no snapshot references it. If `found` stays high but `deleted` is
  zero, the segments are still referenced (expected) rather than stuck.
- GC only runs when the node's metadata view is caught-up. Single-node always
  qualifies; in a cluster the sweep runs on the group-0 leader only (followers
  and non-group-0 segments are not yet reclaimed — tracked as multi-group fan-out
  follow-up). On a lagging or non-leader node the whole sweep cycle is skipped
  fail-closed, so found/deleted counters staying flat there is expected.
- Sweep cap is 50 per cycle (cycle-shared across buckets). If
  `OrphanSegmentSweepCappedTotal` is climbing, shorten scrub interval rather
  than raising the cap (cap protects I/O burst).
- EC shard orphans from coalesce-time failures are NOT covered by this sweep
  (separate follow-up).

---

### EC-redundancy upgrade sweep (relocating genesis 1+0 objects)

**What it does:** when a cluster that started genuinely single-node (genesis boot,
no `--bootstrap-expect-nodes`) later grows, objects written during the single-node
window live in a non-redundant `1+0` group — a single-node loss would lose them. The
background sweep (default ON) detects these and re-encodes them into a redundant wide
EC group, preserving identity (key/version/ETag/size/content-type/metadata/tags/ACL
**and LastModified**). The old shards are reclaimed by the orphan-segment sweep above.

**Track:**
```bash
curl http://<node>:9000/metrics | grep -E 'grainfs_ec_redundancy_upgrade_(relocated|failed)_total'
```
`relocated_total` rising then plateauing = the backlog of pre-growth `1+0` objects is
being drained. `failed_total` rising = relocations are erroring (check logs for
`redundancy-upgrade: relocate failed`); benign skips (object changed underneath / no
longer a candidate) are NOT counted as failures.

**Controls:**
- `--ec-redundancy-upgrade=false` — kill switch (default on).
- `--ec-redundancy-upgrade-max` (default 8) — relocations per scrub cycle; this moves
  real data, so it is a deliberate slow drip. Raise only if the backlog drains too
  slowly and foreground I/O has headroom.
- `--ec-redundancy-upgrade-min-age` (default 5m) — an object must be at least this old
  before relocation, so the sweep never races an in-flight write. `0` relocates
  immediately (tests only).

**Safety:** the sweep runs only on the caught-up leader of each group (no two nodes
relocate the same object); an interrupted relocation leaves reclaimable orphans and
the object stays readable via its old placement; and a concurrent client overwrite
always wins (the relocation preserves the original ModTime, which loses the
quorum-meta last-writer-wins to any newer write). Common multi-node patterns
(form-then-write, `--bootstrap-expect-nodes`) never produce `1+0` objects, so the
sweep is a no-op there. Non-latest versions in versioning-enabled buckets are not yet
covered (tracked follow-up).

---

## Host Deployment Procedures

**Prerequisites:**
```bash
# Build the binary locally from the repo root.
make build
```

**Deployment:**
```bash
# Create host data directory
mkdir -p /var/lib/grainfs

# Start GrainFS directly (genesis self-seeds the transport key)
./bin/grainfs serve --data /var/lib/grainfs --port 9000
```

After the server starts, bootstrap the admin SA once via the host-side admin socket:
```bash
grainfs iam sa create admin --endpoint /var/lib/grainfs/admin.sock
```
Export the returned credentials as `$GRAINFS_ACCESS_KEY`/`$GRAINFS_SECRET_KEY`
for subsequent S3 client commands.

**Health check:**
```bash
# API health check
AWS_ACCESS_KEY_ID=$GRAINFS_ACCESS_KEY \
AWS_SECRET_ACCESS_KEY=$GRAINFS_SECRET_KEY \
AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://localhost:9000 s3 ls
```

**Rollback:**
```bash
# Replace ./bin/grainfs with the previous binary, then restart the service
# (reads the on-disk keys.d/current.key):
./bin/grainfs serve --data /var/lib/grainfs --port 9000
```

### Kubernetes Deployment

**Prerequisites:**
```bash
# Install kubectl
# Install helm (if using Helm charts)

# Create namespace
kubectl create namespace grainfs

# Store the cluster PSK used by the deployment example below.
kubectl create secret generic grainfs-secrets \
  -n grainfs \
  --from-literal=cluster-key="$(openssl rand -hex 32)"
```

**Deployment:**
```bash
# Create PersistentVolumeClaim
kubectl apply -f k8s/pvc.yaml -n grainfs

# Deploy GrainFS
kubectl apply -f k8s/deployment.yaml -n grainfs

# Expose service
kubectl apply -f k8s/service.yaml -n grainfs

# Bootstrap admin SA (run once after first deploy)
kubectl exec deploy/grainfs -n grainfs -- grainfs iam sa create admin --endpoint /grainfs/data/admin.sock
```
Export the returned credentials as `$GRAINFS_ACCESS_KEY`/`$GRAINFS_SECRET_KEY`
for subsequent S3 client commands.

**Example k8s/deployment.yaml:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: grainfs
  namespace: grainfs
spec:
  replicas: 1
  selector:
    matchLabels:
      app: grainfs
  template:
    metadata:
      labels:
        app: grainfs
    spec:
      containers:
      - name: grainfs
        image: grainfs:latest
        args:
        - serve
        - --data
        - /grainfs/data
        - --port
        - "9000"
        ports:
        - containerPort: 9000
          name: s3
        - containerPort: 2049
          name: nfsv4
        volumeMounts:
        - name: data
          mountPath: /grainfs/data
        # Project the cluster transport key into keys.d/current.key before boot
        # (a file, never an argv/env literal). Omit this mount for a single
        # genesis node, which self-seeds its own key.
        - name: cluster-key
          mountPath: /grainfs/data/keys.d/current.key
          subPath: cluster-key
          readOnly: true
        resources:
          requests:
            memory: "2Gi"
            cpu: "500m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /
            port: 9000
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /
            port: 9000
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: grainfs-data
      - name: cluster-key
        secret:
          secretName: grainfs-secrets
          items:
          - key: cluster-key
            path: cluster-key
            mode: 0600
```

**Rollback:**
```bash
# Rollback to previous deployment
kubectl rollout undo deployment/grainfs -n grainfs

# Or rollback to specific revision
kubectl rollout history deployment/grainfs -n grainfs
kubectl rollout undo deployment/grainfs --to-revision=2 -n grainfs
```

---

## Post-Deployment Tasks

### Document Deployment

Update deployment log:

Record the deployment date, operator, `GrainFS` version, verification result,
and any issues found.

---

## Validation Status

Track deployment drills in the deployment log. Drill procedures are being
redesigned alongside the cluster-aware backup/restore work — see CHANGELOG
v0.0.320.0.

---

## Cluster Key Rotation

The cluster transport key (`keys.d/current.key`) is the PSK used to derive the
cluster TLS identity (certificate and SPKI). A different key creates a different
cluster identity; nodes that keep the old key fail authentication against nodes
that use the new key.

**v0.0.39 and newer support online rolling rotation.** The PSK can be replaced
without S3, NFS, or NBD downtime. The CLI sends rotation commands through the
meta-Raft leader's localhost-only Unix socket (`$DATA/rotate.sock`, mode 0600).

### Online rotation (recommended)

Prerequisite: every node runs v0.0.39 or newer and is healthy. Identify the
leader with:
`grainfs cluster --endpoint <data-dir>/admin.sock status`.

> **Required for multi-node clusters:** rotation works correctly only when every
> peer already has the new PSK in its own `keys.d/next.key`. The CLI writes the
> key only to the leader's disk, so steps 1 and 2 must pre-distribute the same
> PSK file to every peer. If a peer is missing the file, the leader still
> advances phases, but follower workers fail with ENOENT and do not switch their
> transport identity. That can split cluster networking. Under the Plan C ack
> model, Raft commit is the implicit ack; the leader does not detect per-peer
> apply failures.

1. **Generate a new key**:
   ```
   openssl rand -hex 32 > /tmp/grainfs-new-psk  # 32-byte PSK = 64 hex chars
   ```

2. **Distribute the new PSK to every peer node, including the leader**:
   ```bash
   # For each peer, use a secure channel such as SSH, Ansible, or Vault.
   for HOST in node1 node2 node3; do
     ssh "$HOST" "umask 077 && mkdir -p /path/to/data/keys.d && cat > /path/to/data/keys.d/next.key" < /tmp/grainfs-new-psk
     ssh "$HOST" "chmod 600 /path/to/data/keys.d/next.key"
   done
   ```
   Every node must have the same 64-character hex PSK in
   `<DATA>/keys.d/next.key`. The file content is the hex string followed by a
   newline. File mode must be 0600.

3. **Start rotation on the leader node**:
   ```
   # The leader's keys.d/next.key should already exist; otherwise the CLI creates it.
   ./grainfs cluster rotate-key begin --new-key=$(cat /tmp/grainfs-new-psk) --endpoint=/path/to/data/rotate.sock
   ```
   The output includes `rotation_id`, `OLD SPKI`, and `NEW SPKI`. The CLI
   returns after submitting the request; the cluster advances in the background
   through rotation states and then returns to steady state.

   **If a follower is missing `next.key` or has an SPKI mismatch during rotation,
   that follower cannot swap transport identity.** It loses connectivity to the
   other nodes. Abort immediately, then repeat step 2.

4. **Monitor progress**:
   ```
   ./grainfs cluster rotate-key status --endpoint=/path/to/data/rotate.sock
   ```
   - `state=begun`: workers add NEW to the accept set, while nodes still present OLD.
   - `state=switched`: nodes present NEW, while workers still accept OLD.
   - `state=steady`: nodes present NEW, and `GrainFS` keeps OLD in `keys.d/previous.key`.

   A five-second grace period between state changes gives workers time to
   complete disk I/O and swap transport identity. A healthy rotation usually
   finishes in about 10 to 15 seconds.

5. **No per-node config update is needed.** `GrainFS` writes the rotated NEW key
   to each node's `keys.d/current.key` during rotation, so a restart picks it up
   from disk automatically. If you stage the key from an external secret store,
   update that store to the NEW value for future fresh provisioning.

6. **Verify**: use `grainfs cluster --endpoint <data-dir>/admin.sock status` to
   confirm all peers are healthy, then use
   `grainfs cluster rotate-key status --endpoint <data-dir>/rotate.sock` to
  confirm `state=steady`.

### Failure handling during rotation

- **Operator abort**:
  ```
  ./grainfs cluster rotate-key abort --reason=<reason> --endpoint=/path/to/data/rotate.sock
  ```
  - If status shows `state=begun`: roll back to OLD and discard NEW.
  - If status shows `state=switched`: forward-roll to NEW because some peers may
    already present NEW, making revert unsafe (D18).

- **Global timeout**: if rotation exceeds 30 minutes, the leader automatically
  issues an abort.

- **Down peer**: if a node stops responding during rotation, Raft commit does
  not progress and the rotation stalls. Recover the node or remove it from the
  cluster, then resume.

### Offline fallback (all nodes older than v0.0.39)

1. Stop every node. This causes S3, NFS, and NBD downtime.
2. Write the new key to every node's `keys.d/current.key`, then restart each node.
3. Confirm peer reconnection with `grainfs cluster --endpoint <data-dir>/admin.sock status`.

---

## NFS / 9P Mount Operations

### Creating a Mount SA

A Mount SA scopes NFS/9P access to a named principal with an attached IAM policy.
Use the `NFSMountOnly` builtin for NFSv4 clients and `9PAttachOnly` for 9P clients.

```bash
# Create the Mount SA with a numeric UID hint (advisory, for AUTH_SYS mapping).
grainfs iam mount-sa create alice-mount --uid 1000 --endpoint <data>/admin.sock

# Attach a builtin policy.
grainfs iam mount-sa policy attach alice-mount NFSMountOnly --endpoint <data>/admin.sock

# Register the target bucket as an NFS export if not already present.
grainfs nfs export add my-bucket --endpoint <data>/admin.sock

# List existing Mount SAs.
grainfs iam mount-sa list --endpoint <data>/admin.sock
```

Mount path for NFSv4: `:<bucket>/<mount-sa>` (e.g. `localhost:/my-bucket/alice-mount`).
Mount path for 9P: `aname=<mount-sa>@<bucket>` (e.g. `aname=alice-mount@my-bucket`).

### Cross-namespace policy rejection

Attaching a regular IAM policy (action namespace `s3:*`) to a Mount SA returns
HTTP 412 Precondition Failed. Mount SAs accept only policies whose actions
are in the `grainfs:` namespace (`NFSMountOnly`, `9PAttachOnly`, or a custom
policy that uses only `grainfs:NFSRead` / `grainfs:NFSWrite` / `grainfs:9PAttach`).

Remediation: create or use a policy in the `grainfs:` action namespace, then retry
`grainfs iam mount-sa policy attach`.

### Read-only export

```bash
# Register as read-only from the start.
grainfs nfs export add my-bucket --ro --endpoint <data>/admin.sock

# Flip an existing export to read-only (with optional quiesce).
grainfs nfs export update my-bucket --ro --quiesce-wait 30s --endpoint <data>/admin.sock
```

Clients on a read-only export receive `NFS4ERR_ROFS` / 9P `EROFS` on writes.

### Auditing NFS/9P access

The `audit.s3` Iceberg table records every NFS and 9P operation with
`source = 'nfs4'` or `source = '9p'` and the client `source_ip`:

```bash
grainfs audit query "
  SELECT sa_id, source, source_ip, operation, bucket, ts
  FROM grainfs_iceberg.audit.s3
  WHERE source IN ('nfs4', '9p')
  ORDER BY ts DESC
  LIMIT 20
" --endpoint <data>/admin.sock
```

### TLS posture gate for authenticated clusters

For network-exposed deployments, run NFS/9P behind a private network boundary
or TLS-terminating proxy. Recommended hardening:

- A TLS certificate is on disk (`<data>/tls/cert.pem`), or
- `trusted-proxy.cidr` is set (TLS is terminated by a front-end proxy).

Boot error prefix: `NFS/9P boot: auth required + no TLS cert + no trusted proxy`.

Remediation options:

```bash
# Option A: place the TLS cert (and restart).
cp server.crt <data>/tls/cert.pem
cp server.key <data>/tls/key.pem

# Option B: configure a trusted proxy CIDR (hot-applied, no restart needed).
grainfs config set trusted-proxy.cidr 10.0.0.0/8 --endpoint <data>/admin.sock
```

See also: `docs/operators/deploy-production-cluster.md` for TLS posture details.

---

## Keystore disk full

The keystore directory (`<dataDir>/keys/`) requires at least 64 KiB of free
space to perform a KEK rotation: the leader writes the new `keys/<V>.key` and
re-wraps every live DEK before committing the raft command. The leader probes
every voter's keystore partition before accepting a rotation.

Each node also keeps `<dataDir>/keys.d/raft-store.key.enc`, a node-local
sidecar that seals the Badger encryption key for that node's raft v2 stores.
Back up `keys/`, `keys.d/raft-store.key.enc`, `cluster.id`, `raft/`, and
`meta_raft/` together for node-level restore. Restoring encrypted raft logs
without the matching raft-store sidecar is unsupported.

If `grainfs_keystore_disk_free_bytes < 65536` on any node, the rotation propose
is rejected and the response names the offending node id. New writes and reads
continue to work — only rotation is blocked.

Recover by freeing space on that node's data partition (rotate/ship logs, prune
old snapshots, or grow the volume), then retry:

```bash
grainfs encrypt kek rotate --i-know --endpoint <data>/admin.sock
```

Monitor `grainfs_keystore_disk_free_bytes` per node so the condition is caught
before an operator attempts a rotation.

---

## DEK rotation cadence

Each DEK encrypts object data under XAES-256-GCM with a 192-bit nonce. The
extended nonce makes random-nonce collision negligible at any practical seal
volume, so there is no nonce-exhaustion cliff forcing rotation. The per-generation
seal count below is a cumulative-usage signal for rotation hygiene and
compromise-recovery, not a hard limit.

**Data-DEK rotation is deferred in this release.** The `encryption.rotate-dek`
trigger is intentionally rejected (`grainfs config set encryption.rotate-dek now`
returns a "deferred — not supported in this release" error) — the append-only
at-rest writers pin the DEK generation at open. Data WAL segment creation now
persists the active generation in its encrypted segment header, but live rotation
still needs a rollover or seal-under-pinned-generation boundary. Node-local data
WALs and cluster-shard data WALs use distinct AEAD namespaces (`datawal/node`
and `datawal/shard`), so frames cannot be swapped between those physical WAL
families even when sequence numbers match. Every remaining ciphertext-bearing
lane must have equivalent generation framing before rotation is re-enabled. Because
XAES removed the nonce-exhaustion cliff, the seal count below is **observability
only** (cumulative-usage / compromise-recovery signal), not an action threshold.
**KEK rotation** (`cluster rotate-key`, which re-wraps the existing DEKs without
changing the DEK keys) remains fully available and is unaffected.

GrainFS tracks seals per active DEK generation and surfaces a risk band in
`grainfs encrypt kek status` (the `dek_generations` section, `nonce=` column)
and in Prometheus (`grainfs_kek_seal_count{dek_generation="<active>"}`):

| seal_count          | band    | meaning (observability only)                |
| ------------------- | ------- | ------------------------------------------- |
| `< 100,000,000`     | `ok`    | normal cumulative usage                     |
| `100M – 1,000M`     | `warn`  | high cumulative usage — note for capacity/compromise review |
| `>= 1,000,000,000`  | `alert` | very high cumulative usage — track for the future rotation capability |

These bands are informational under XAES (no nonce cliff). Alert on them only for
usage visibility; there is no operator rotation action in this release. Track when
ANY DEK generation's seal count crosses a threshold:

```promql
max(grainfs_kek_seal_count) >= 100000000   # warn
max(grainfs_kek_seal_count) >= 1000000000  # alert
```

`grainfs_kek_seal_count` is collected live from each node's `/metrics` endpoint
at scrape time, so these alerts fire autonomously without any polling of the
`grainfs encrypt kek status` admin endpoint.

The seal count is keyed by DEK generation because seal volume is per-DEK-key. A
KEK rotation re-wraps the existing DEKs without changing the DEK keys, so the
per-DEK seal count PERSISTS across a KEK rotation. With data-DEK rotation deferred,
the active generation stays fixed, so the band reflects true cumulative nonce usage
for that generation (which, under XAES, carries no exhaustion risk).

When data-DEK rotation is re-enabled in a future release (with per-segment
generation framing), it will add a new DEK generation in parallel — new seals use
it immediately while existing objects stay readable under their original
generation — without blocking client I/O.

---

## KEK retire / prune

KEK removal is two-phase: `grainfs encrypt kek retire --version N` marks an old
KEK version draining, then `grainfs encrypt kek prune --version N` permanently
removes it once every voter attests no active lease holds it.

> **Prune-refusal for object snapshots is now ENFORCED.** `grainfs encrypt kek prune`
> refuses if any voter reports a retained object-metadata snapshot
> (`<data>/snapshots/snapshot-*.json.zst`) sealed under the target version. The error
> names the blocking node and count, e.g.:
> `KEKPrune: node 127.0.0.1:7001 has 2 retained object snapshot(s) sealed under version 1`
>
> Both cluster-metadata (Raft FSM) and object-metadata snapshots embed a per-snapshot
> ephemeral DEK wrapped by the KEK version active at snapshot time. Data DEKs are
> rewrapped automatically before retire; **object snapshots are not** — they are sealed
> once and never rewrapped. Pruning the KEK version while a snapshot references it
> would make that snapshot permanently unreadable on restore.
>
> Pre-prune checklist (defense-in-depth; the automatic guard below catches remaining cases):
> 1. Confirm `grainfs_snapshot_legacy_plaintext_reads_total` has been flat at zero
>    across a full snapshot cycle (runtime signal that all active snapshots are
>    enveloped — necessary but not sufficient alone).
> 2. Review your snapshot retention policy: if you keep snapshots older than the last
>    KEK rotation, those snapshots reference the old version. Delete them or let them
>    expire before pruning.
> 3. Then prune: `grainfs encrypt kek prune --version N`. The cluster automatically
>    refuses prune if any voter still holds a snapshot sealed under version N — delete
>    the snapshot and retry.

> **Prune-refusal for raft-store keys is also enforced.** If a node's
> `keys.d/raft-store.key.enc` is still sealed under the target KEK version,
> prune fails with an error naming the raft-store sidecar. KEK rotation rewraps
> this sidecar automatically; retry prune after the rotation has applied on all
> voters.

> **Prune may need retries on a busy cluster.** Prune validates the voter set
> against the raft committed index at probe time and rejects if the committed
> index advanced mid-probe (a possible membership change). On a cluster under
> continuous writes the committed index advances constantly, so prune can be
> rejected repeatedly. Retry during a quiet window, or briefly quiesce writes.
