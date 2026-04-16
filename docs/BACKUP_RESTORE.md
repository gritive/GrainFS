# GrainFS Backup and Restore Guide

## Overview

GrainFS uses [restic](https://restic.net/) for backup and restore. Restic is a modern, secure, and deduplicated backup tool that supports multiple storage backends.

## Prerequisites

### Install Restic

**macOS:**
```bash
brew install restic
```

**Linux:**
```bash
apt-get install restic
# or
yum install restic
```

**Verify installation:**
```bash
restic version
```

## Quick Start

### 1. Initialize Backup Repository

First, initialize a restic repository where backups will be stored:

**Local filesystem:**
```bash
export RESTIC_REPOSITORY=/backup/grainfs
export RESTIC_PASSWORD=your-secure-password
restic init
```

**S3-compatible storage:**
```bash
export RESTIC_REPOSITORY=s3:s3.amazonaws.com/your-bucket/grainfs
export RESTIC_AWS_ACCESS_KEY_ID=your-access-key
export RESTIC_AWS_SECRET_ACCESS_KEY=your-secret-key
export RESTIC_PASSWORD=your-secure-password
restic init
```

### 2. Create Backup

Use the `grainfs backup` command to create a backup:

```bash
grainfs backup \
  --repo /backup/grainfs \
  --data /path/to/grainfs/data \
  --tag "manual-backup-$(date +%Y%m%d)"
```

**Output:**
```
🔙 Backing up GrainFS data...
Repository: /backup/grainfs
Data directory: /path/to/grainfs/data
Tag: manual-backup-20260417

repository <repo-id> opened successfully, password is correct
created new cache in /home/user/.cache/restic
no parent snapshot found, will read all files
[0:00] 100.00%  1.234 GiB / 1.234 GiB  234.567 MiB/s  ETA 0:00
snapshot <snapshot-id> saved

✅ Backup completed successfully
```

### 3. List Backups

View all snapshots:

```bash
restic snapshots --repo /backup/grainfs
```

**Output:**
```
ID        Time                 Host        Tags        Paths
---------------------------------------------------------------------
abc123    2026-04-17 10:00:00  server1     manual      /grainfs/data
def456    2026-04-17 11:00:00  server1     manual      /grainfs/data
ghi789    2026-04-17 12:00:00  server1     scheduled   /grainfs/data
---------------------------------------------------------------------
3 snapshots
```

### 4. Restore Backup

Restore the latest backup:

```bash
grainfs restore \
  --repo /backup/grainfs \
  --target /path/to/restore
```

Restore a specific snapshot:

```bash
grainfs restore \
  --repo /backup/grainfs \
  --snapshot abc123 \
  --target /path/to/restore
```

**After restore:**
```bash
# Start GrainFS with restored data
grainfs serve --data /path/to/restore
```

## Automated Backups

### Using Cron

**Daily backup at 2 AM:**

Add to crontab (`crontab -e`):

```cron
0 2 * * * /usr/local/bin/grainfs backup --repo /backup/grainfs --data /grainfs/data --tag daily >> /var/log/grainfs-backup.log 2>&1
```

**Hourly backup:**

```cron
0 * * * * /usr/local/bin/grainfs backup --repo /backup/grainfs --data /grainfs/data --tag hourly >> /var/log/grainfs-backup.log 2>&1
```

### Using systemd Timer

Create `/etc/systemd/system/grainfs-backup.service`:

```ini
[Unit]
Description=GrainFS Backup
After=network.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/grainfs backup --repo /backup/grainfs --data /grainfs/data --tag daily
Environment=RESTIC_PASSWORD=your-secure-password
```

Create `/etc/systemd/system/grainfs-backup.timer`:

```ini
[Unit]
Description=Daily GrainFS Backup
Requires=grainfs-backup.service

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

Enable and start:

```bash
systemctl enable grainfs-backup.timer
systemctl start grainfs-backup.timer
```

## Backup Verification

Regularly verify your backups:

### 1. Check Snapshot Integrity

```bash
restic check --repo /backup/grainfs
```

### 2. Test Restore

Run the automated backup test:

```bash
make test-backup
```

Or manually:

```bash
# Restore to temporary directory
grainfs restore --repo /backup/grainfs --target /tmp/grainfs-restore-test

# Start GrainFS and verify data
grainfs serve --data /tmp/grainfs-restore-test --port 9001
```

### 3. Verify Backup Metrics

```bash
# List backup statistics
restic stats --repo /backup/grainfs

# Output:
# Total Size: 1.234 GiB
# Total Files: 45678
```

## Disaster Recovery

### Scenario 1: Disk Failure

**Symptoms:** GrainFS won't start, disk errors

**Recovery:**
1. Replace failed disk
2. Reinstall GrainFS
3. Restore from backup:
   ```bash
   grainfs restore --repo /backup/grainfs --target /new/disk
   grainfs serve --data /new/disk
   ```

### Scenario 2: Data Corruption

**Symptoms:** Objects return wrong data, errors in logs

**Recovery:**
1. Stop GrainFS
2. Run diagnostics:
   ```bash
   grainfs doctor --data /grainfs/data
   ```
3. Identify corruption source
4. Restore from last good backup:
   ```bash
   grainfs restore --repo /backup/grainfs --snapshot <good-snapshot-id> --target /grainfs/data-restored
   ```
5. Verify and switch to restored data

### Scenario 3: Accidental Deletion

**Symptoms:** Important bucket/object deleted

**Recovery:**
1. Find snapshot before deletion:
   ```bash
   restic snapshots --repo /backup/grainfs
   ```
2. Restore specific file:
   ```bash
   restic restore <snapshot-id> --repo /backup/grainfs --target /tmp --include "/grainfs/data/badger/deleted-file"
   ```
3. Copy restored file back to data directory

## Best Practices

### 1. Backup Frequency

| Data Change Rate | Backup Frequency | Retention |
|-----------------|-----------------|-----------|
| Low (< 1%/day)  | Daily           | 30 daily  |
| Medium (1-10%)  | Hourly          | 24 hourly + 30 daily |
| High (> 10%)    | Every 15 min    | 48 snapshots + 30 daily |

### 2. Retention Policy

Automatically forget old snapshots:

```bash
# Keep 7 daily, 4 weekly, 6 monthly backups
restic forget --repo /backup/grainfs \
  --keep-daily 7 \
  --keep-weekly 4 \
  --keep-monthly 6 \
  --prune
```

### 3. Security

- Use strong RESTIC_PASSWORD (32+ characters)
- Store password in secure location (password manager, secrets manager)
- Encrypt backup repository (restic uses AES-256 by default)
- Restrict backup repository permissions (chmod 700)

### 4. Monitoring

Monitor backup jobs:

```bash
# Check last backup time
restic snapshots --repo /backup/grainfs --latest --json | jq '.[0].time'
```

Set up alerts for:
- Backup failures (check exit codes)
- Backup age (> 24 hours = warning)
- Repository size (disk space)

### 5. Testing

- Test restore monthly
- Run `grainfs doctor` after restore
- Verify object counts and sample data
- Document restore time (RTO)

## Troubleshooting

### Backup Fails with "no such file or directory"

**Cause:** Data directory path incorrect

**Fix:**
```bash
# Verify path
ls -la /grainfs/data

# Use correct path
grainfs backup --repo /backup/grainfs --data /correct/path
```

### Restore Fails with "repository not found"

**Cause:** RESTIC_REPOSITORY incorrect

**Fix:**
```bash
# List available repos
ls -la /backup/

# Use correct repo path
export RESTIC_REPOSITORY=/correct/backup/path
```

### Backup is Slow

**Cause:** Network latency, slow disk

**Solutions:**
- Use local backup, then sync to cloud
- Increase restic backup concurrency:
  ```bash
  restic backup --repo /backup/grainfs /grainfs/data --limit-upload 2048
  ```
- Use faster storage (SSD vs HDD)

### Out of Memory During Backup

**Cause:** Large files, limited RAM

**Solution:**
```bash
# Limit memory usage
export GOGC=10  # More aggressive GC
grainfs backup --repo /backup/grainfs --data /grainfs/data
```

## References

- [Restic Documentation](https://restic.readthedocs.io/)
- [Restic Backup Strategies](https://restic.readthedocs.io/en/stable/080_examples.html)
- [S3-Compatible Storage](https://restic.readthedocs.io/en/stable/030_preparing_a_new_repo.html#s3)
- [GrainFS SLI/SLO](./SLI_SLO.md) - RTO/RPO definitions
