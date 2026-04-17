# Data Loss Scenario Drill Manual

## Overview

This manual defines disaster recovery drills to validate that GrainFS can survive real-world data loss scenarios. Each drill is executed manually to prove that backups work, recovery procedures are correct, and the runbook is accurate.

**Drill philosophy:** Failures are inevitable. Recovery procedures are hypotheses. Drills are experiments that prove or disprove those hypotheses.

---

## Drill Format

Each drill follows this structure:

1. **Setup** - Pre-drill preparations
2. **Inject Failure** - Simulate the disaster scenario
3. **Execute Recovery** - Follow runbook procedures
4. **Verify Recovery** - Confirm data integrity
5. **Document Learnings** - Capture what breaks and what needs fixing

**Rule:** If a drill reveals a problem with the runbook or automation, fix BOTH:
- Fix the system (code/config)
- Update the runbook (documentation)

Then **re-run the drill** to validate the fix.

---

## Pre-Drill Checklist

Before running ANY drill, verify:

- [ ] **Test environment**: Use isolated test environment (NOT production)
- [ ] **Test data**: Populate with realistic test data (not empty)
- [ ] **Backup**: Create baseline backup before drill
- [ ] **Time box**: Allocate 2-4 hours per drill (drills can uncover complex issues)
- [ ] **Observer**: Have a second person observe and document (if possible)

---

## Drill #1: Disk Failure Simulation

**Scenario:** Storage disk fails completely. All data is lost. Must restore from backup.

**Severity:** SEV1 - Complete data loss

**Recovery Time Objective (RTO):** 30 minutes
**Recovery Point Objective (RPO):** Last backup (1 hour ago)

### Setup

1. **Start test environment:**
   ```bash
   # Create test data directory
   TEST_DIR=/tmp/drill-disk-fail
   mkdir -p $TEST_DIR

   # Start GrainFS
   grainfs serve \
     --data $TEST_DIR \
     --port 9001 \
     > /tmp/grainfs-drill.log 2>&1 &
   GRAINFS_PID=$!

   # Wait for startup
   sleep 5
   ```

2. **Populate test data:**
   ```bash
   # Create test bucket and objects
   aws --endpoint-url http://localhost:9001 s3 mb s3://drill-test

   # Create 100 test objects
   for i in {1..100}; do
     echo "Test data $i" > /tmp/test-object-$i
     aws --endpoint-url http://localhost:9001 s3 cp \
       /tmp/test-object-$i s3://drill-test/object-$i
   done

   echo "Created 100 test objects"
   ```

3. **Create baseline backup:**
   ```bash
   # Initialize restic repo (if needed)
   export RESTIC_REPOSITORY=/tmp/drill-backup
   export RESTIC_PASSWORD=drill-test-password
   restic init --repo $RESTIC_REPOSITORY || true

   # Create backup
   grainfs backup \
     --repo $RESTIC_REPOSITORY \
     --data $TEST_DIR \
     --tag drill-baseline

   echo "Baseline backup created"
   restic snapshots --repo $RESTIC_REPOSITORY
   ```

### Inject Failure

**⏱️ START TIMER:**
```bash
DRILL_START=$(date +%s)
echo "Drill timer started: $DRILL_START"
```

1. **Stop GrainFS:**
   ```bash
   kill $GRAINFS_PID
   ```

2. **Simulate disk failure (delete all data):**
   ```bash
   echo "Simulating disk failure..."
   rm -rf $TEST_DIR
   ls -la $TEST_DIR
   ```
   Expected: "No such file or directory"

3. **Verify data loss:**
   ```bash
   # Attempt to start GrainFS (should fail or start with empty state)
   grainfs serve --data $TEST_DIR --port 9001 &
   sleep 3

   # Try to access data (should fail or return empty)
   aws --endpoint-url http://localhost:9001 s3 ls s3://drill-test
   ```
   Expected: Bucket doesn't exist or is empty

### Execute Recovery

Follow the rollback procedure from `docs/RUNBOOK.md`:

1. **Stop failed instance:**
   ```bash
   pgrep -f "grainfs serve" | xargs kill
   ```

2. **Restore from backup:**
   ```bash
   # Find baseline snapshot
   SNAPSHOT=$(restic snapshots --repo $RESTIC_REPOSITORY --json | \
     jq -r '.[] | select(.tags? | index("drill-baseline")) | .id')

   echo "Restoring from snapshot: $SNAPSHOT"

   # Restore to new directory
   grainfs restore \
     --repo $RESTIC_REPOSITORY \
     --snapshot $SNAPSHOT \
     --target $TEST_DIR.restored
   ```

3. **Swap directories:**
   ```bash
   mv $TEST_DIR $TEST_DIR.failed
   mv $TEST_DIR.restored $TEST_DIR
   ```

4. **Start GrainFS:**
   ```bash
   grainfs serve \
     --data $TEST_DIR \
     --port 9001 \
     > /tmp/grainfs-drill-recovery.log 2>&1 &
   GRAINFS_PID=$!
   sleep 5
   ```

### Verify Recovery

1. **Check process:**
   ```bash
   ps -p $GRAINFS_PID
   ```
   Expected: Process is running

2. **Verify data integrity:**
   ```bash
   # List objects
   aws --endpoint-url http://localhost:9001 s3 ls s3://drill-test

   # Verify all 100 objects exist
   OBJECT_COUNT=$(aws --endpoint-url http://localhost:9001 s3 ls s3://drill-test | wc -l)
   echo "Recovered objects: $OBJECT_COUNT"
   ```
   Expected: 100 objects

3. **Verify object content:**
   ```bash
   # Spot-check 10 random objects
   for i in {1..10}; do
     OBJ_NUM=$((RANDOM % 100 + 1))
     aws --endpoint-url http://localhost:9001 s3 cp \
       s3://drill-test/object-$OBJ_NUM - | \
       grep -q "Test data $OBJ_NUM" && \
       echo "✓ Object $OBJ_NUM verified" || \
       echo "✗ Object $OBJ_NUM CORRUPTED"
   done
   ```
   Expected: All spot-checked objects have correct content

4. **Run diagnostics:**
   ```bash
   grainfs doctor --data $TEST_DIR
   ```
   Expected: Overall health: pass

**⏱️ STOP TIMER AND CALCULATE RTO:**
```bash
DRILL_END=$(date +%s)
RTO_SECONDS=$((DRILL_END - DRILL_START))
RTO_MINUTES=$((RTO_SECONDS / 60))
echo "RTO achieved: ${RTO_MINUTES} minutes ($RTO_SECONDS seconds)"

if [ $RTO_SECONDS -gt 1800 ]; then
  echo "❌ RTO FAILED: ${RTO_MINUTES} minutes > 30 minutes target"
  exit 1
else
  echo "✅ RTO PASSED: ${RTO_MINUTES} minutes < 30 minutes target"
fi
```

Expected: RTO < 30 minutes

### Document Learnings

Record results in `docs/DISASTER_RECOVERY.md`:

```markdown
## Drill #1: Disk Failure - YYYY-MM-DD

**Outcome:** PASSED / FAILED

**RTO achieved:** X minutes (target: 30 minutes)
**RPO achieved:** Last backup (X minutes before failure)

**What worked:**
- [List what went smoothly]

**What broke:**
- [List any issues encountered]

**Fixes applied:**
- [List code/runbook changes made]

**Re-run required:** Yes / No
```

**Cleanup:**
```bash
# Stop test instance
kill $GRAINFS_PID

# Remove test data
rm -rf $TEST_DIR $TEST_DIR.failed /tmp/test-object-*
```

---

## Drill #2: BadgerDB Corruption

**Scenario:** Metadata database (BadgerDB) becomes corrupted. Cannot start GrainFS. Must diagnose and recover.

**Severity:** SEV1 - Complete system outage

**Recovery Time Objective (RTO):** 30 minutes
**Recovery Point Objective (RPO):** Last backup (1 hour ago)

### Setup

Same as Drill #1

### Inject Failure

**⏱️ START TIMER:**
```bash
DRILL_START=$(date +%s)
echo "Drill timer started: $DRILL_START"
```

1. **Stop GrainFS:**
   ```bash
   kill $GRAINFS_PID
   ```

2. **Corrupt BadgerDB:**
   ```bash
   echo "Simulating BadgerDB corruption..."
   # Corrupt a value log file
   truncate -s-1 $TEST_DIR/badger/*.vlog

   # Or write random data
   dd if=/dev/urandom of=$TEST_DIR/badger/000001.vlog bs=1M count=1
   ```

3. **Attempt startup (should fail):**
   ```bash
   grainfs serve --data $TEST_DIR --port 9001 > /tmp/corrupt-startup.log 2>&1 &
   sleep 5

   # Check logs for errors
   grep -i "corrupt\|error\|invalid" /tmp/corrupt-startup.log
   ```
   Expected: BadgerDB reports corruption

### Execute Recovery

1. **Run diagnostics:**
   ```bash
   grainfs doctor --data $TEST_DIR
   ```
   Expected: Reports "badgerdb" check as fail

2. **Attempt BadgerDB repair (if supported):**
   ```bash
   # BadgerDB has a repair command
   # Note: This is placeholder - actual command depends on BadgerDB version
   badgerdb repair --dir $TEST_DIR/badger
   ```

3. **If repair fails, restore from backup:**
   ```bash
   # Follow same restore procedure as Drill #1
   grainfs restore \
     --repo $RESTIC_REPOSITORY \
     --target $TEST_DIR.restored

   mv $TEST_DIR $TEST_DIR.corrupt
   mv $TEST_DIR.restored $TEST_DIR
   ```

### Verify Recovery

Follow same verification procedure as Drill #1 (process check, data integrity, diagnostics), including RTO measurement:

```bash
# [Include all verification steps from Drill #1: Verify Recovery section]

# ⏱️ STOP TIMER AND CALCULATE RTO:
DRILL_END=$(date +%s)
RTO_SECONDS=$((DRILL_END - DRILL_START))
RTO_MINUTES=$((RTO_SECONDS / 60))
echo "RTO achieved: ${RTO_MINUTES} minutes ($RTO_SECONDS seconds)"

if [ $RTO_SECONDS -gt 1800 ]; then
  echo "❌ RTO FAILED: ${RTO_MINUTES} minutes > 30 minutes target"
  exit 1
else
  echo "✅ RTO PASSED: ${RTO_MINUTES} minutes < 30 minutes target"
fi
```

Expected: RTO < 30 minutes

### Document Learnings

Record in `docs/DISASTER_RECOVERY.md`

---

## Drill #3: Accidental Data Deletion

**Scenario:** Operator accidentally deletes critical bucket. Must recover from backup.

**Severity:** SEV2 - Partial data loss (recoverable)

**Recovery Time Objective (RTO):** 30 minutes
**Recovery Point Objective (RPO):** Last backup (1 hour ago)

### Setup

Same as Drill #1, but create a "critical" bucket:
```bash
aws --endpoint-url http://localhost:9001 s3 mb s3://production-critical

# Add important data
for i in {1..50}; do
  dd if=/dev/urandom of=/tmp/critical-$i bs=1M count=10
  aws --endpoint-url http://localhost:9001 s3 cp \
    /tmp/critical-$i s3://production-critical/data-$i
done
```

### Inject Failure

**⏱️ START TIMER:**
```bash
DRILL_START=$(date +%s)
echo "Drill timer started: $DRILL_START"
```

```bash
# Simulate accidental deletion
aws --endpoint-url http://localhost:9001 s3 rb s3://production-critical --force

# Verify deletion
aws --endpoint-url http://localhost:9001 s3 ls s3://production-critical
```
Expected: "NoSuchBucket"

### Execute Recovery

1. **Find snapshot before deletion:**
   ```bash
   # List snapshots with timestamps
   restic snapshots --repo $RESTIC_REPOSITORY

   # Find snapshot from just before deletion
   # Assume deletion happened at 10:00, find 09:00 snapshot
   ```

2. **Restore specific files:**
   ```bash
   # Restore only the critical bucket data
   SNAPSHOT=<snapshot-id-from-before-deletion>

   restic restore \
     --repo $RESTIC_REPOSITORY \
     --snapshot $SNAPSHOT \
     --target /tmp/restore \
     --include "$TEST_DIR/badger/production-critical*"

   # Copy restored metadata back
   cp -r /tmp/restore/$(basename $TEST_DIR)/badger/* $TEST_DIR/badger/
   ```

3. **Restart GrainFS:**
   ```bash
   pgrep -f "grainfs serve" | xargs kill
   grainfs serve --data $TEST_DIR --port 9001 &
   sleep 5
   ```

### Verify Recovery

```bash
# Verify bucket exists
aws --endpoint-url http://localhost:9001 s3 ls s3://production-critical

# Verify object count
OBJECT_COUNT=$(aws --endpoint-url http://localhost:9001 s3 ls s3://production-critical | wc -l)
echo "Recovered objects: $OBJECT_COUNT (expected: 50)"
```

**⏱️ STOP TIMER AND CALCULATE RTO:**
```bash
DRILL_END=$(date +%s)
RTO_SECONDS=$((DRILL_END - DRILL_START))
RTO_MINUTES=$((RTO_SECONDS / 60))
echo "RTO achieved: ${RTO_MINUTES} minutes ($RTO_SECONDS seconds)"

if [ $RTO_SECONDS -gt 1800 ]; then
  echo "❌ RTO FAILED: ${RTO_MINUTES} minutes > 30 minutes target"
  exit 1
else
  echo "✅ RTO PASSED: ${RTO_MINUTES} minutes < 30 minutes target"
fi
```

Expected: RTO < 30 minutes

### Document Learnings

Record in `docs/DISASTER_RECOVERY.md`

---

## Drill #4: Network Partition During Write

**Scenario:** Network partition occurs during active write operation. Must verify no data loss or corruption.

**Severity:** SEV2 - Potential data inconsistency

**Recovery Time Objective (RTO):** 30 minutes
**Recovery Point Objective (RPO):** Last successful write

### Setup

Requires toxiproxy (from Phase 1):
```bash
# Start toxiproxy
toxiproxy-server -port 8474 > /tmp/toxiproxy.log 2>&1 &
TOXI_PID=$!
sleep 2

# Create proxy
curl -X POST http://localhost:8474/proxies \
  -d '{"name":"grainfs","upstream":"localhost:9001","listen":"127.0.0.1:9002"}'
```

Start GrainFS on port 9001, use port 9002 as proxy endpoint

### Inject Failure

**⏱️ START TIMER:**
```bash
DRILL_START=$(date +%s)
echo "Drill timer started: $DRILL_START"
```

```bash
# Start large write operation
dd if=/dev/urandom of=/tmp/large-file bs=10M count=1

# Start upload (in background)
aws --endpoint-url http://localhost:9002 s3 cp \
  /tmp/large-file s3://drill-test/large-file &
UPLOAD_PID=$!

# Wait 2 seconds, then inject partition
sleep 2
curl -X POST http://localhost:8474/proxies/grainfs/toxics \
  -d '{"name":"partition","type":"slow_close","toxicity":1.0}'

# Wait for upload to complete/fail
wait $UPLOAD_PID
```

### Execute Recovery

1. **Remove partition:**
   ```bash
   curl -X DELETE http://localhost:8474/proxies/grainfs/toxics/partition
   ```

2. **Wait for recovery:**
   ```bash
   sleep 10
   ```

3. **Verify cluster health:**
   ```bash
   grainfs doctor --data $TEST_DIR
   ```

### Verify Recovery

```bash
# Check for partial uploads
aws --endpoint-url http://localhost:9002 s3 ls s3://drill-test

# If partial upload exists, check size
aws --endpoint-url http://localhost:9002 s3 ls s3://drill-test/large-file

# Verify no corruption (if upload succeeded)
aws --endpoint-url http://localhost:9002 s3 cp \
  s3://drill-test/large-file - | \
  cmp - /tmp/large-file && echo "✓ Data intact" || echo "✗ Data corrupted"
```

**⏱️ STOP TIMER AND CALCULATE RTO:**
```bash
DRILL_END=$(date +%s)
RTO_SECONDS=$((DRILL_END - DRILL_START))
RTO_MINUTES=$((RTO_SECONDS / 60))
echo "RTO achieved: ${RTO_MINUTES} minutes ($RTO_SECONDS seconds)"

if [ $RTO_SECONDS -gt 1800 ]; then
  echo "❌ RTO FAILED: ${RTO_MINUTES} minutes > 30 minutes target"
  exit 1
else
  echo "✅ RTO PASSED: ${RTO_MINUTES} minutes < 30 minutes target"
fi
```

Expected: RTO < 30 minutes

### Document Learnings

Record in `docs/DISASTER_RECOVERY.md`

**Cleanup:**
```bash
kill $TOXI_PID
```

---

## Drill #5: Full System Recovery (End-to-End)

**Scenario:** Complete system loss. New server provisioned. Must restore everything from scratch.

**Severity:** SEV1 - Complete disaster

**Recovery Time Objective (RTO):** 30 minutes
**Recovery Point Objective (RPO):** Last backup (1 hour ago)

### Setup

No setup - this starts from zero

### Inject Failure

N/A - simulating fresh server

### Execute Recovery

**⏱️ START TIMER:**
```bash
DRILL_START=$(date +%s)
echo "Drill timer started: $DRILL_START"
```

1. **Install dependencies:**
   ```bash
   # Install GrainFS
   cp grainfs /usr/local/bin/grainfs
   chmod +x /usr/local/bin/grainfs

   # Install restic
   # (platform-specific commands)
   ```

2. **Configure environment:**
   ```bash
   export RESTIC_REPOSITORY=<backup-repo>
   export RESTIC_PASSWORD=<from-secrets-manager>
   export GRAINFS_ACCESS_KEY=<from-secrets-manager>
   export GRAINFS_SECRET_KEY=<from-secrets-manager>
   ```

3. **Restore data:**
   ```bash
   mkdir -p /grainfs/data

   grainfs restore \
     --repo $RESTIC_REPOSITORY \
     --target /grainfs/data
   ```

4. **Start GrainFS:**
   ```bash
   grainfs serve \
     --data /grainfs/data \
     --port 9000
   ```

### Verify Recovery

Run full verification suite:
```bash
# Health check
grainfs doctor --data /grainfs/data

# API check
aws --endpoint-url http://localhost:9000 s3 ls

# Data integrity check (spot-check random buckets/objects)
```

**⏱️ STOP TIMER AND CALCULATE RTO:**
```bash
DRILL_END=$(date +%s)
RTO_SECONDS=$((DRILL_END - DRILL_START))
RTO_MINUTES=$((RTO_SECONDS / 60))
echo "RTO achieved: ${RTO_MINUTES} minutes ($RTO_SECONDS seconds)"

if [ $RTO_SECONDS -gt 1800 ]; then
  echo "❌ RTO FAILED: ${RTO_MINUTES} minutes > 30 minutes target"
  exit 1
else
  echo "✅ RTO PASSED: ${RTO_MINUTES} minutes < 30 minutes target"
fi
```

Expected: RTO < 30 minutes

### Document Learnings

Record in `docs/DISASTER_RECOVERY.md`

---

## Drill #6: Binary Rollback Validation

**Purpose:** Validate that the binary rollback procedure from the runbook actually works.

**Scenario:** Deploy a new binary that has a critical bug, then rollback to the previous version.

**Severity:** SEV1 - Rollback failure is a production catastrophe

**RTO Target:** < 15 minutes (rollback must be faster than forward deployment)

### Setup

1. **Build two test binaries:**
   ```bash
   # Build "current" binary (with bug)
   make build
   cp grainfs grainfs.current

   # Build "previous" binary (working version)
   git checkout HEAD~1  # One commit back
   make build
   cp grainfs grainfs.previous

   git checkout -  # Return to current commit
   ```

2. **Start test environment with "previous" binary:**
   ```bash
   TEST_DIR=/tmp/drill-rollback
   mkdir -p $TEST_DIR

   ./grainfs.previous \
     --data $TEST_DIR \
     --port 9001 \
     > /tmp/grainfs-previous.log 2>&1 &
   GRAINFS_PID=$!

   sleep 5
   ```

3. **Create test data:**
   ```bash
   aws --endpoint-url http://localhost:9001 s3 mb s3://rollback-test
   echo "Test data before rollback" > /tmp/test-data
   aws --endpoint-url http://localhost:9001 s3 cp /tmp/test-data s3://rollback-test/data
   ```

4. **Create pre-"deployment" backup:**
   ```bash
   export RESTIC_REPOSITORY=/tmp/drill-backup
   export RESTIC_PASSWORD=drill-test-password
   restic init --repo $RESTIC_REPOSITORY || true

   grainfs backup \
     --repo $RESTIC_REPOSITORY \
     --data $TEST_DIR \
     --tag pre-rollback-baseline
   ```

### Inject Failure (Deploy Bad Binary)

**⏱️ START TIMER:**
```bash
ROLLBACK_START=$(date +%s)
echo "Rollback drill timer started: $ROLLBACK_START"
```

1. **Stop "previous" binary:**
   ```bash
   kill $GRAINFS_PID
   sleep 2
   ```

2. **Deploy "current" binary (simulated broken version):**
   ```bash
   ./grainfs.current \
     --data $TEST_DIR \
     --port 9001 \
     > /tmp/grainfs-broken.log 2>&1 &
   GRAINFS_PID=$!

   sleep 5
   ```

3. **Verify bad binary is running (or note startup failure):**
   ```bash
   ps -p $GRAINFS_PID
   aws --endpoint-url http://localhost:9001 s3 ls
   ```
   (This simulates discovering the new binary is broken)

### Execute Rollback

Follow the runbook's binary rollback procedure:

1. **Stop broken binary:**
   ```bash
   pgrep -f "grainfs serve" | xargs kill
   ```

2. **Restore previous binary:**
   ```bash
   # Simulate restoring from backup
   cp grainfs.previous /usr/local/bin/grainfs
   chmod +x /usr/local/bin/grainfs
   ```

3. **Start with previous binary:**
   ```bash
   ./grainfs.previous \
     --data $TEST_DIR \
     --port 9001 \
     > /tmp/grainfs-rollback.log 2>&1 &
   GRAINFS_PID=$!

   sleep 5
   ```

### Verify Rollback

1. **Process check:**
   ```bash
   ps -p $GRAINFS_PID
   ```
   Expected: Process is running

2. **Data integrity check:**
   ```bash
   aws --endpoint-url http://localhost:9001 s3 ls s3://rollback-test
   aws --endpoint-url http://localhost:9001 s3 cp s3://rollback-test/data - | grep -q "Test data before rollback" && \
     echo "✅ Data intact after rollback" || \
     echo "❌ Data corrupted after rollback"
   ```
   Expected: Data intact

**⏱️ STOP TIMER AND CALCULATE RTO:**
```bash
ROLLBACK_END=$(date +%s)
RTO_SECONDS=$((ROLLBACK_END - ROLLBACK_START))
RTO_MINUTES=$((RTO_SECONDS / 60))
echo "Rollback RTO achieved: ${RTO_MINUTES} minutes ($RTO_SECONDS seconds)"

if [ $RTO_SECONDS -gt 900 ]; then
  echo "❌ ROLLBACK RTO FAILED: ${RTO_MINUTES} minutes > 15 minutes target"
  exit 1
else
  echo "✅ ROLLBACK RTO PASSED: ${RTO_MINUTES} minutes < 15 minutes target"
fi
```

### Document Learnings

Record in `docs/DISASTER_RECOVERY.md`

**Cleanup:**
```bash
kill $GRAINFS_PID
rm -rf $TEST_DIR /tmp/grainfs-*.log
```

---

## Drill #7: Data Rollback Validation

**Purpose:** Validate that the data rollback procedure (restoring to a previous snapshot) actually works.

**Scenario:** Corruption is discovered 1 day after deployment. Must restore to yesterday's backup.

**Severity:** SEV1 - Data rollback failure is a production catastrophe

**RTO Target:** < 30 minutes

### Setup

Same setup as Drill #1 (Disk Failure), but with **multiple backups over time**:

1. **Start test environment:**
   ```bash
   TEST_DIR=/tmp/drill-data-rollback
   mkdir -p $TEST_DIR

   grainfs serve \
     --data $TEST_DIR \
     --port 9001 \
     > /tmp/grainfs-drill.log 2>&1 &
   GRAINFS_PID=$!

   sleep 5
   ```

2. **Create Day 1 data:**
   ```bash
   aws --endpoint-url http://localhost:9001 s3 mb s3://rollback-test

   # Create initial data
   for i in {1..20}; do
     echo "Day 1 data $i" > /tmp/day1-data-$i
     aws --endpoint-url http://localhost:9001 s3 cp /tmp/day1-data-$i s3://rollback-test/day1-$i
   done
   ```

3. **Backup Day 1:**
   ```bash
   export RESTIC_REPOSITORY=/tmp/drill-backup
   export RESTIC_PASSWORD=drill-test-password
   restic init --repo $RESTIC_REPOSITORY || true

   grainfs backup \
     --repo $RESTIC_REPOSITORY \
     --data $TEST_DIR \
     --tag day1-backup

   echo "Day 1 backup created"
   restic snapshots --repo $RESTIC_REPOSITORY --json | jq -r '.[].time'
   ```

4. **Simulate time passing (Day 2):**
   ```bash
   sleep 2  # In reality, this would be 24 hours later

   # Add Day 2 data
   for i in {1..20}; do
     echo "Day 2 data $i" > /tmp/day2-data-$i
     aws --endpoint-url http://localhost:9001 s3 cp /tmp/day2-data-$i s3://rollback-test/day2-$i
   done
   ```

5. **Backup Day 2:**
   ```bash
   grainfs backup \
     --repo $RESTIC_REPOSITORY \
     --data $TEST_DIR \
     --tag day2-backup

   echo "Day 2 backup created"
   restic snapshots --repo $RESTIC_REPOSITORY --json | jq -r '.[].time'
   ```

### Inject Failure (Data Corruption Detected)

**⏱️ START TIMER:**
```bash
ROLLBACK_START=$(date +%s)
echo "Data rollback drill timer started: $ROLLBACK_START"
```

1. **Simulate corruption detection:**
   ```bash
   echo "⚠️  Corruption detected in Day 2 data! Need to rollback to Day 1."
   ```

2. **Stop GrainFS:**
   ```bash
   kill $GRAINFS_PID
   ```

### Execute Data Rollback

Follow the runbook's data rollback procedure:

1. **Find Day 1 snapshot:**
   ```bash
   # Find snapshot with "day1-backup" tag
   SNAPSHOT=$(restic snapshots --repo $RESTIC_REPOSITORY --json | \
     jq -r '.[] | select(.tags? | index("day1-backup")) | .id')

   echo "Restoring to snapshot: $SNAPSHOT"
   ```

2. **Stop GrainFS (if still running):**
   ```bash
   pgrep -f "grainfs serve" | xargs kill 2>/dev/null || true
   ```

3. **Restore to temporary location:**
   ```bash
   grainfs restore \
     --repo $RESTIC_REPOSITORY \
     --snapshot $SNAPSHOT \
     --target $TEST_DIR.restored
   ```

4. **Verify restored data:**
   ```bash
   grainfs doctor --data $TEST_DIR.restored
   ```

5. **Swap to restored data:**
   ```bash
   mv $TEST_DIR $TEST_DIR.corrupted
   mv $TEST_DIR.restored $TEST_DIR
   ```

6. **Start GrainFS with rolled-back data:**
   ```bash
   grainfs serve \
     --data $TEST_DIR \
     --port 9001 \
     > /tmp/grainfs-rollback.log 2>&1 &
   GRAINFS_PID=$!

   sleep 5
   ```

### Verify Rollback

1. **Process check:**
   ```bash
   ps -p $GRAINFS_PID
   ```
   Expected: Process is running

2. **Verify Day 1 data exists:**
   ```bash
   aws --endpoint-url http://localhost:9001 s3 ls s3://rollback-test
   ```
   Expected: Should see `day1-data-*` files, NOT `day2-data-*`

3. **Verify Day 1 data integrity:**
   ```bash
   # Check a few Day 1 objects
   for i in {1..5}; do
     aws --endpoint-url http://localhost:9001 s3 cp s3://rollback-test/day1-data-$i - | \
       grep -q "Day 1 data $i" && \
       echo "✅ Day 1 data-$i intact" || \
       echo "❌ Day 1 data-$i corrupted"
   done
   ```
   Expected: All Day 1 objects intact

4. **Verify Day 2 data is gone (RPO validation):**
   ```bash
   aws --endpoint-url http://localhost:9001 s3 ls s3://rollback-test | grep "day2-data" || \
     echo "✅ Day 2 data correctly absent (RPO achieved: last backup)"
   ```
   Expected: No Day 2 data present

5. **Run diagnostics:**
   ```bash
   grainfs doctor --data $TEST_DIR
   ```
   Expected: Overall health: pass

**⏱️ STOP TIMER AND CALCULATE RTO:**
```bash
ROLLBACK_END=$(date +%s)
RTO_SECONDS=$((ROLLBACK_END - ROLLBACK_START))
RTO_MINUTES=$((RTO_SECONDS / 60))
echo "Data rollback RTO achieved: ${RTO_MINUTES} minutes ($RTO_SECONDS seconds)"

if [ $RTO_SECONDS -gt 1800 ]; then
  echo "❌ ROLLBACK RTO FAILED: ${RTO_MINUTES} minutes > 30 minutes target"
  exit 1
else
  echo "✅ ROLLBACK RTO PASSED: ${RTO_MINUTES} minutes < 30 minutes target"
fi
```

**Calculate RPO:**
```bash
# Time since Day 2 backup (simulated as 2 hours ago)
RPO_HOURS=2
echo "RPO achieved: $RPO_HOURS hours (within 24-hour target)"
```

### Document Learnings

Record in `docs/DISASTER_RECOVERY.md`

**Cleanup:**
```bash
kill $GRAINFS_PID
rm -rf $TEST_DIR $TEST_DIR.corrupted /tmp/grainfs-*.log /tmp/day*-data-*
```

---

## Drill Execution Log

Track all drill executions:

| Drill # | Scenario            | Date       | Outcome   | RTO   | Issues | Re-run |
| ------- | ------------------- | ---------- | --------- | ----- | ------ | ------ |
| 1       | Disk Failure        | YYYY-MM-DD | PASS/FAIL | X min | ...    | Yes/No |
| 2       | BadgerDB Corruption | YYYY-MM-DD | PASS/FAIL | X min | ...    | Yes/No |
| 3       | Accidental Deletion | YYYY-MM-DD | PASS/FAIL | X min | ...    | Yes/No |
| 4       | Network Partition   | YYYY-MM-DD | PASS/FAIL | X min | ...    | Yes/No |
| 5       | Full Recovery       | YYYY-MM-DD | PASS/FAIL | X min | ...    | Yes/No |
| 6       | Binary Rollback     | YYYY-MM-DD | PASS/FAIL | X min | ...    | Yes/No |
| 7       | Data Rollback       | YYYY-MM-DD | PASS/FAIL | X min | ...    | Yes/No |

**Pass criteria:** All drills must PASS with RTO < 30 minutes before production deployment.

---

## After Drills: Update Documentation

After each drill, update these documents:

1. **RUNBOOK.md** - Fix any incorrect procedures or missing steps
2. **DISASTER_RECOVERY.md** - Document what broke and how it was fixed
3. **DRILL_MANUAL.md** - Update drill procedures if they were unclear

**Principle:** Documentation is living. Drills expose gaps. Fix the gaps immediately.
