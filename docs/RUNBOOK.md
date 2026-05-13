# GrainFS Production Deployment Runbook

## Overview

This runbook documents the step-by-step procedure to deploy GrainFS to production. It has been validated through actual deployment drills.

**Last validated:** [Date of drill execution]
**Validated by:** [Who ran the drill]
**Environment:** [Production/staging description]

---

## Admin UDS — Bootstrap & Permissions

The admin Unix socket at `<data-dir>/admin.sock` is the **sole bootstrap path** for new
clusters. It is also used by `grainfs cluster ...`, `grainfs iam ...` subcommands.

### Permissions

The socket is created with mode `0660` (hard-fail on chmod failure). Operators in the
admin group can connect; others cannot.

- Default: socket owned by the user running `grainfs serve`, group is the user's primary
  group.
- Multi-operator setups: pass `--admin-group <groupname>` to chown the socket to a shared
  group. All operators must belong to that group.
- File mode is hard-coded; do not chmod looser.

### Bootstrap a new cluster

```bash
# 1) Start grainfs (no auth flags needed).
grainfs serve --data ./data --port 9000 &

# 2) Create the first admin SA (returns one-time secret_key).
grainfs iam sa create admin --endpoint ./data/admin.sock
# {"sa_id":"sa-default","access_key":"GRAIN...","secret_key":"<one-time>","grants":[{"bucket":"*","role":"admin"}]}

# 3) Use the credentials for S3 traffic.
aws --endpoint-url http://localhost:9000 \
    --access-key-id GRAIN... --secret-access-key <secret> \
    s3 mb s3://my-bucket
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
- [ ] **Network connectivity**: Ports 9000 (S3 API), 9002 (NFS), [NBD port] accessible
- [ ] **Disk mounting**: Data directory mounted on reliable storage (SSD recommended)
- [ ] **Backup repository**: Restic repo initialized and accessible
- [ ] **Monitoring**: Prometheus scraping configured and receiving data

### Configuration Verification

- [ ] **Environment variables set**:
  ```bash
  export GRAINFS_DATA_DIR=/path/to/production/data
  export GRAINFS_PORT=9000
  export GRAINFS_ACCESS_KEY=<from-secrets-manager>
  export GRAINFS_SECRET_KEY=<from-secrets-manager>
  ```
- [ ] **Cluster membership** (if applicable):
  ```bash
  # Verify peer IPs are correct
  cat /etc/grainfs/peers.txt
  ```
- [ ] **Backup credentials**:
  ```bash
  export RESTIC_REPOSITORY=<backup-repo>
  export RESTIC_PASSWORD=<from-secrets-manager>
  ```

### Safety Checks

- [ ] **Run diagnostics**:
  ```bash
  grainfs doctor --data $GRAINFS_DATA_DIR
  ```
  Expected: All checks pass (overall_health: pass)

- [ ] **Verify existing data** (if migrating):
  ```bash
  ls -la $GRAINFS_DATA_DIR/badger
  ls -la $GRAINFS_DATA_DIR/blobs
  ```
  Expected: Data directories exist and are non-empty

- [ ] **Test backup access**:
  ```bash
  restic snapshots --repo $RESTIC_REPOSITORY
  ```
  Expected: Can list snapshots (no errors)

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

- `badger_startup_blocked` or `badger_open_failed` with `block_startup`: restore the named Badger role from a clean snapshot, or fix the disk, lock, or permission error before retrying startup.
- `badger_read_only_admitted` or log message `badger startup recovery read-only gate enabled`: read paths remain available, but storage writes and mutating admin APIs return HTTP 503 with code `RecoveryReadOnly`. Repair or restore the failed group-state role, then restart and verify normal writes.
- `disable_feature`: the server started without the optional role. Receipts, dedup, or incident-state behavior may be unavailable until the role directory is repaired and the process restarts.

---

## Deployment Procedure

### Step 1: Create Pre-Deployment Backup

**CRITICAL:** Always create a backup before any deployment.

```bash
# Tag backup with deployment timestamp
TAG="pre-deploy-$(date +%Y%m%d-%H%M%S)"
grainfs backup \
  --repo $RESTIC_REPOSITORY \
  --data $GRAINFS_DATA_DIR \
  --tag "$TAG"
```

**Verify backup was created:**
```bash
restic snapshots --repo $RESTIC_REPOSITORY --latest
```

Expected: Snapshot with tag `$TAG` appears in list

**If backup fails:** DO NOT proceed. Investigate and fix backup issue first.

---

### Step 2: Stop Existing GrainFS Process (if upgrading)

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

### Step 3: Deploy New Binary

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

**v0.0.106.0+ rolling upgrade gate (IAM bucket-scoped keys):** Scoped key 발급
(`grainfs iam key create --bucket <name>`)은 모든 노드를 v0.0.106.0+ 로 올린
뒤에만 수행하라. v0.0.105.0 이하 follower는 새 raft cmd `IAMKeyCreateScoped`
(type 30) 를 unknown으로 graceful no-op 처리하고 warn 로그를 남긴다 — leader는
성공 응답을 반환하지만 일부 follower 상태에 키가 반영되지 않아 leadership
변경 시 키가 사라질 수 있다. Mixed-version window 동안에는 `--bucket` 플래그
없이 발급되는 unrestricted 키만 안전하다.

---

### Step 4: Start GrainFS

`grainfs serve` no longer accepts `--access-key`/`--secret-key`. The first cluster
operator runs the admin SA bootstrap (see "Admin UDS — Bootstrap & Permissions"
above) immediately after the first node starts; the resulting `access_key`/
`secret_key` are then used by **S3 clients** (e.g., `aws --endpoint-url`) and
exported as `$GRAINFS_ACCESS_KEY`/`$GRAINFS_SECRET_KEY` for the rest of this
runbook's `aws` examples.

**Local mode:**
```bash
grainfs serve \
  --data $GRAINFS_DATA_DIR \
  --port $GRAINFS_PORT \
  > /var/log/grainfs/production.log 2>&1 &
```

**Cluster mode:**
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

> **Rolling-upgrade ordering:** `bucket-upstream` records propagate via a new MetaCmdType (IDs 32/33) introduced in v0.0.123.0. While a cluster is mid-upgrade — some nodes still on v0.0.122 or earlier — DO NOT issue `grainfs bucket upstream put/delete` commands. Pre-v0.0.123 followers will silently no-op the raft entry on apply (rolling-upgrade safety design). The records are recovered correctly via snapshot replay on next snapshot install, but during the apply gap the follower's view is inconsistent. Wait until every node reports v0.0.123.0+ before configuring bucket upstreams. The CLI/admin path was relocated in v0.0.133.0 (see ADR 0010); FSM/snapshot format is unchanged so v0.0.122 ↔ v0.0.133 raft compatibility is preserved.

If migrating from another S3-compatible source, register the upstream per
bucket via the admin UDS. The `--upstream*` cmdline flags were removed in
v0.0.123.0; the IAM-managed approach replaces them.

```bash
# Register the upstream for bucket "legacy-data".
grainfs bucket upstream put legacy-data \
  --endpoint /grainfs/data/admin.sock \
  --upstream-url http://upstream-minio:9000 \
  --access-key legacy-ak \
  --secret-key-stdin <<<"legacy-sk"

grainfs bucket upstream get legacy-data --endpoint /grainfs/data/admin.sock
grainfs bucket upstream list --endpoint /grainfs/data/admin.sock
grainfs bucket upstream delete legacy-data --endpoint /grainfs/data/admin.sock
```

Pull-through is read-only and on-miss only: the first GET on a missing
object proxies upstream and stores locally; subsequent GETs hit local
cache. Migration "completion" semantics (cutover, progress) are not yet
implemented — track via your own counters from the upstream side. See
ADR 0009 for the deferred work.

---

### Step 5: Post-Deployment Verification

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
aws --endpoint-url http://localhost:9000 \
  --access-key-id $GRAINFS_ACCESS_KEY \
  --secret-access-key $GRAINFS_SECRET_KEY \
  s3 ls
```
Expected: No error, bucket list returned (may be empty)

**Diagnostics check:**
```bash
grainfs doctor --data $GRAINFS_DATA_DIR
```
Expected: Overall health: pass

**Verify metrics:**
```bash
curl http://localhost:9000/metrics | grep grainfs_up
```
Expected: `grainfs_up 1`

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

# Restart with old binary
grainfs serve \
  --data $GRAINFS_DATA_DIR \
  --port $GRAINFS_PORT \
  > /var/log/grainfs/production.log 2>&1 &
```

### Option 2: Data Rollback (if data corruption)

```bash
# Stop GrainFS
pgrep -f "grainfs serve" | xargs kill

# Restore from pre-deployment backup
RESTORE_SNAPSHOT=$(restic snapshots --repo $RESTIC_REPOSITORY --json | jq -r '.[] | select(.tags? | index("pre-deploy-'$(date +%Y%m%d)'")) | .id' | head -1)

grainfs restore \
  --repo $RESTIC_REPOSITORY \
  --snapshot $RESTORE_SNAPSHOT \
  --target $GRAINFS_DATA_DIR.restored

# Verify restored data
grainfs doctor --data $GRAINFS_DATA_DIR.restored

# Switch to restored data
mv $GRAINFS_DATA_DIR $GRAINFS_DATA_DIR.failed
mv $GRAINFS_DATA_DIR.restored $GRAINFS_DATA_DIR

# Restart
grainfs serve \
  --data $GRAINFS_DATA_DIR \
  --port $GRAINFS_PORT \
  > /var/log/grainfs/production.log 2>&1 &
```

---

## Membership Operations (Day-2)

### Evicting a permanently-dead node

When a cluster node fails irrecoverably (hardware loss, corrupted disk past recovery), remove it from the meta-Raft voter set so quorum math reflects the surviving members. Run **on the leader node** — the endpoint is the admin Unix socket (mode 0660 + admin-group) at `<data-dir>/admin.sock`.

The examples below assume the socket path is exported once for the procedure:

```bash
export ENDPOINT=/var/run/grainfs/admin.sock   # or <data-dir>/admin.sock
```

1. Identify the dead voter. `cluster peers` lists the current metaRaft voter set; cross-reference with `cluster status` (or the operator's external monitoring) to identify which one is the dead node. Normal rows use node IDs. Legacy raft-address rows that cannot be resolved are shown as `unresolved_legacy` so operators can still see the row that blocks membership mutation:

   ```bash
   grainfs cluster --endpoint $ENDPOINT peers
   #   NODE_ID          RAFT_ADDR          ROLE      STATE
   #   node-2           127.0.0.1:19102    follower  configured
   #   127.0.0.1:19103  127.0.0.1:19103    follower  unresolved_legacy

   grainfs cluster --endpoint $ENDPOINT status --format json   # includes peer_snapshot
   ```

2. Pre-flight check is automatic. The server uses the peer snapshot membership-mutation policy: `self` and rows with fresh successful metaRaft AppendEntries evidence count as `live`, while `configured` rows are treated as unknown. Failed heartbeats alone do not make a peer display-down for this policy. If removal would drop the post-removal voter count below quorum, or another unresolved legacy row makes identity ambiguous, the command refuses unless `--force`:

   ```bash
   grainfs cluster --endpoint $ENDPOINT remove-peer node-2 --yes
   ```

3. Verify the voter set shrank and an audit event was recorded:

   ```bash
   grainfs cluster --endpoint $ENDPOINT peers
   grainfs cluster --endpoint $ENDPOINT events --type cluster-remove-peer --since 1h
   ```

**`--force` semantics**: bypasses pre-flight only — does not bypass the engine. Use when the operator has independently confirmed the peer is permanently lost and the joint-consensus commit can still progress (e.g., 3-of-5 alive removing 1 dead). For clusters that have already lost quorum, `recover cluster` (offline snapshot recovery) is the right tool, not `remove-peer --force`.

**Removing the leader**: safe — the engine commits the joint Cnew, then the leader steps down via commit-time wakeup and a new leader is elected from the remaining voters. Operator does not need a separate `transfer-leader` step.

---

## Monitoring Setup

### Prometheus Alerts

Ensure alerts from `alerts/prometheus/rules.yml` are configured:

```bash
# Verify alerts are loaded
curl http://prometheus:9090/api/v1/rules | grep grainfs
```

Expected: GrainFS alert rules appear in output

### Log Aggregation

Forward logs to monitoring system:

```bash
# rsyslog configuration (example)
echo "if \$programname == 'grainfs' then @@log-server:514" >> /etc/rsyslog.d/grainfs.conf
systemctl restart rsyslog
```

---

## Common Issues and Fixes

### Issue: GrainFS won't start

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
- Restart GrainFS

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
- Check for lock contention (grainfs doctor)

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

# Start GrainFS directly
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
aws --endpoint-url http://localhost:9000 \
  --access-key-id $GRAINFS_ACCESS_KEY \
  --secret-access-key $GRAINFS_SECRET_KEY \
  s3 ls
```

**Rollback:**
```bash
# Replace ./bin/grainfs with the previous binary, then restart the service:
./bin/grainfs serve --data /var/lib/grainfs --port 9000
  grainfs:previous-version \
  serve \
  --data /data \
  --port 9000
```

### Kubernetes Deployment

**Prerequisites:**
```bash
# Install kubectl
# Install helm (if using Helm charts)

# Create namespace
kubectl create namespace grainfs
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
        ports:
        - containerPort: 9000
        - containerPort: 9002
          name: nfs
        env:
        - name: GRAINFS_DATA_DIR
          value: /grainfs/data
        - name: GRAINFS_PORT
          value: "9000"
        volumeMounts:
        - name: data
          mountPath: /grainfs/data
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

### Create Post-Deployment Backup

After successful deployment and verification, create another backup:

```bash
TAG="post-deploy-$(date +%Y%m%d-%H%M%S)"
grainfs backup \
  --repo $RESTIC_REPOSITORY \
  --data $GRAINFS_DATA_DIR \
  --tag "$TAG"
```

### Document Deployment

Update deployment log:

```markdown
## Deployment Log - YYYY-MM-DD

**Deployed by:** [Name]
**Version:** [GrainFS version]
**Pre-deploy backup:** [snapshot ID]
**Post-deploy backup:** [snapshot ID]
**Verification:** All checks passed
**Issues:** None
```

---

## Validation Status

This runbook has been validated through [N] deployment drills. See `docs/DRILL_MANUAL.md` for drill procedures and results.

---

## Cluster Key Rotation

`--cluster-key`는 클러스터 TLS identity (cert + SPKI)를 결정론적으로 도출하는 PSK이다.
다른 키 → 다른 클러스터 identity. 옛 키를 가진 노드는 새 키 노드와 인증 실패한다.

**v0.0.39 부터 무중단(rolling) rotation 지원.** S3/NFS/NBD downtime 없이 PSK를
교체할 수 있다. CLI는 meta-raft leader의 localhost-only Unix socket
(`$DATA/rotate.sock`, mode 0600)을 통해 명령을 전송한다.

### Online rotation (권장)

전제: 모든 노드가 v0.0.39+ 이며 정상 작동 중. Leader 식별:
`grainfs cluster --endpoint <data-dir>/admin.sock status`.

> ⚠ **다중 노드 클러스터 필수 절차**: 회전은 모든 peer가 새 PSK를 자신의
> `keys.d/next.key`로 가지고 있을 때만 정상 동작한다. CLI는 leader의 디스크에만
> 키를 쓰므로, 절차 1~2 단계에서 **반드시 모든 peer에게 같은 PSK 파일을 미리
> 배포해야 한다**. 미배포 시 leader는 phase를 자동 진행하지만 follower 워커들은
> ENOENT로 실패하고 transport identity 전환이 일어나지 않아 cluster network
> split 발생. (Plan C ack 모델: raft commit이 묵시적 ack — peer 적용 실패는
> leader가 감지하지 못함.)

1. **새 키 생성**:
   ```
   openssl rand -hex 32 > /tmp/grainfs-new-psk  # 32-byte PSK = 64 hex chars
   ```

2. **모든 peer 노드에 새 PSK 배포** (leader 포함):
   ```bash
   # 각 peer에 대해 — secure channel (ssh / ansible / vault) 사용
   for HOST in node1 node2 node3; do
     ssh "$HOST" "umask 077 && mkdir -p /path/to/data/keys.d && cat > /path/to/data/keys.d/next.key" < /tmp/grainfs-new-psk
     ssh "$HOST" "chmod 600 /path/to/data/keys.d/next.key"
   done
   ```
   각 노드의 `<DATA>/keys.d/next.key` 파일에 동일한 64자 hex PSK가 쓰여 있어야
   함. 파일 내용은 hex 문자열 + 줄바꿈. 권한 0600.

3. **Leader 노드에서 회전 시작**:
   ```
   # leader의 keys.d/next.key는 이미 있어야 함 — 없으면 CLI가 자동으로 생성
   ./grainfs cluster rotate-key begin --new-key=$(cat /tmp/grainfs-new-psk) --endpoint=/path/to/data/rotate.sock
   ```
   출력에 `rotation_id`, `OLD SPKI`, `NEW SPKI` 표시. CLI는 즉시 반환되며
   클러스터가 background에서 자동으로 phase 1→2→3→steady 진행.

   **회전 중 follower의 `next.key`가 없거나 SPKI 불일치이면 해당 follower는
   transport를 swap하지 못한다.** 다른 노드와 통신 단절. 즉시 `abort` 수행 후
   2단계 재배포.

3. **진행 상황 모니터링**:
   ```
   ./grainfs cluster rotate-key status --endpoint=/path/to/data/rotate.sock
   ```
   - phase=2 (begun): accept set에 NEW 추가, 여전히 OLD 제시
   - phase=3 (switched): 활성 cert를 NEW로 전환, OLD는 accept 유지
   - phase=1 (steady): NEW가 활성, OLD는 `keys.d/previous.key` 보관

   각 phase 사이 5초 grace로 워커가 디스크 I/O + transport identity swap
   완료. 정상 진행 시 ~10–15초 내 완료.

4. **모든 노드의 영구 설정에 새 키 반영** — 다음 재시작 시에도 새 키가
   유지되도록 환경변수, 시스템d 유닛, `--cluster-key` 플래그를 업데이트한다.
   디스크의 `keys.d/current.key`는 회전 직후 자동으로 NEW로 갱신되므로 플래그 없이
   재시작해도 회전 후 NEW를 사용한다 (D10 — disk wins).

5. **검증**: `grainfs cluster --endpoint <data-dir>/admin.sock status` 로 모든
   peer 정상, `grainfs cluster rotate-key status --endpoint <data-dir>/rotate.sock`
   으로 phase=1 (steady) 확인.

### 회전 중 실패 처리

- **운영자 취소**:
  ```
  ./grainfs cluster rotate-key abort --reason=<설명> --endpoint=/path/to/data/rotate.sock
  ```
  - phase 2에서 abort: OLD로 롤백 (NEW 폐기)
  - phase 3에서 abort: NEW로 forward-roll (일부 peer가 이미 NEW를 제시 중이라
    revert가 안전하지 않음 — D18)

- **Global timeout**: 회전이 30분을 초과하면 leader가 자동으로 abort 발행.

- **Down peer**: 한 노드가 회전 중 응답하지 않으면 raft commit 자체가 진행되지
  않아 phase가 멈춘다. 해당 노드를 복구하거나 클러스터에서 제거한 뒤 재개.

### Offline fallback (모든 노드가 v0.0.39 미만일 때)

1. 모든 노드 정지 (S3/NFS/NBD downtime 발생).
2. 모든 노드를 새 `--cluster-key`로 재시작.
3. `grainfs cluster --endpoint <data-dir>/admin.sock status`로 peer 재연결 확인.
