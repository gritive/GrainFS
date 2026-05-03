# GrainFS Production Deployment Runbook

## Overview

This runbook documents the step-by-step procedure to deploy GrainFS to production. It has been validated through actual deployment drills.

**Last validated:** [Date of drill execution]
**Validated by:** [Who ran the drill]
**Environment:** [Production/staging description]

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

If an incident is `proof-unavailable`, check HealReceipt signing and persistence before treating the repair as audit-complete. If an incident is `isolated`, review the named object version and restore or delete it according to the data owner policy; unrelated objects in the bucket should continue serving.

For `fd_exhaustion_risk`, inspect the decision text first. It includes current FD usage, projected threshold ETA when available, and best-effort categories such as `socket`, `badger`, or `nfs_session`.

```bash
curl -s http://localhost:9000/api/incidents/fd-<node-id> | jq .
curl -s http://localhost:9000/metrics | grep '^grainfs_fd_'
lsof -p $(pgrep -n grainfs) | awk '{print $5}' | sort | uniq -c | sort -nr | head
```

If the incident is `diagnosed`, reduce connection churn or raise the process FD limit before the ETA expires. If it is `blocked`, raise `LimitNOFILE`/`ulimit -n`, check for socket/session leaks, and restart gracefully after draining traffic. The watcher resolves the incident after FD usage stays below the warning threshold for `--fd-recovery-window`.

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

---

### Step 4: Start GrainFS

**Local mode:**
```bash
grainfs serve \
  --data $GRAINFS_DATA_DIR \
  --port $GRAINFS_PORT \
  --access-key $GRAINFS_ACCESS_KEY \
  --secret-key $GRAINFS_SECRET_KEY \
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
  --access-key $GRAINFS_ACCESS_KEY \
  --secret-key $GRAINFS_SECRET_KEY \
  > /var/log/grainfs/production.log 2>&1 &

# Additional nodes join through any existing member's Raft address
grainfs serve \
  --data $GRAINFS_DATA_DIR \
  --port $GRAINFS_PORT \
  --node-id node-2 \
  --raft-addr node-2.example.com:9001 \
  --join node-1.example.com:9001 \
  --access-key $GRAINFS_ACCESS_KEY \
  --secret-key $GRAINFS_SECRET_KEY \
  > /var/log/grainfs/production.log 2>&1 &
```

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
  --access-key $GRAINFS_ACCESS_KEY \
  --secret-key $GRAINFS_SECRET_KEY \
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

## Container Deployment Procedures

### Docker Deployment

**Prerequisites:**
```bash
# Install Docker
# (platform-specific commands)

# Build the image locally from the repo root.
docker build -t grainfs:latest .
```

**Deployment:**
```bash
# Create data volume
docker volume create grainfs-data

# Start GrainFS container
docker run -d \
  --name grainfs \
  --restart unless-stopped \
  -p 9000:9000 \
  -v grainfs-data:/data \
  -e GRAINFS_ACCESS_KEY=<from-secrets-manager> \
  -e GRAINFS_SECRET_KEY=<from-secrets-manager> \
  grainfs:latest
```

The image default command is `serve --data /data --port 9000 --nfs4-port 0 --nbd-port 0`.
It is intentionally S3-only so the non-root container can start without binding privileged
NFSv4 port 2049 or opening Linux block devices. To expose NFS or NBD, override the command
and run with the container privileges required by your platform.

**Health check:**
```bash
# Check container is running
docker ps | grep grainfs

# Check logs
docker logs grainfs

# API health check
aws --endpoint-url http://localhost:9000 \
  --access-key-id $GRAINFS_ACCESS_KEY \
  --secret-access-key $GRAINFS_SECRET_KEY \
  s3 ls
```

**Rollback:**
```bash
# Stop current container
docker stop grainfs
docker rm grainfs

# Start previous version
docker run -d \
  --name grainfs \
  --restart unless-stopped \
  -p 9000:9000 \
  -v grainfs-data:/grainfs/data \
  grainfs:previous-version \
  serve \
  --data /grainfs/data \
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
# Create ConfigMap for configuration
kubectl create configmap grainfs-config \
  --from-literal=access-key="$GRAINFS_ACCESS_KEY" \
  --from-literal=secret-key="$GRAINFS_SECRET_KEY" \
  --namespace=grainfs

# Create PersistentVolumeClaim
kubectl apply -f k8s/pvc.yaml -n grainfs

# Deploy GrainFS
kubectl apply -f k8s/deployment.yaml -n grainfs

# Expose service
kubectl apply -f k8s/service.yaml -n grainfs
```

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
        env:
        - name: GRAINFS_ACCESS_KEY
          valueFrom:
            configMapKeyRef:
              name: grainfs-config
              key: access-key
        - name: GRAINFS_SECRET_KEY
          valueFrom:
            configMapKeyRef:
              name: grainfs-config
              key: secret-key
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

전제: 모든 노드가 v0.0.39+ 이며 정상 작동 중. Leader 식별: `grainfs cluster status`.

1. **새 키 생성** (또는 자동):
   ```
   openssl rand -hex 32  # 32-byte PSK = 64 hex chars
   ```

2. **Leader 노드에서 회전 시작**:
   ```
   ./grainfs cluster rotate-key begin --new-key=<64-hex> --data=/path/to/data
   # 또는 자동 생성:
   ./grainfs cluster rotate-key begin --generate --data=/path/to/data
   ```
   출력에 `rotation_id`, `OLD SPKI`, `NEW SPKI` 표시. CLI는 즉시 반환되며
   클러스터가 background에서 자동으로 phase 1→2→3→steady 진행.

3. **진행 상황 모니터링**:
   ```
   ./grainfs cluster rotate-key status --data=/path/to/data
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

5. **검증**: `grainfs cluster status` 로 모든 peer 정상, `grainfs cluster
   rotate-key status` 로 phase=1 (steady) 확인.

### 회전 중 실패 처리

- **운영자 취소**:
  ```
  ./grainfs cluster rotate-key abort --reason=<설명> --data=/path/to/data
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
3. `grainfs cluster status`로 peer 재연결 확인.
