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
