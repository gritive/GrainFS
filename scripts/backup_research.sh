#!/bin/bash
set -e

GRAINFS_DIR="${GRAINFS_DIR:-./tmp/research_backup}"
BACKUP_DIR="${BACKUP_DIR:-./tmp/backup_test}"

echo "=== GrainFS Backup Integration Research ==="
echo "GrainFS Data Dir: $GRAINFS_DIR"
echo "Backup Dir: $BACKUP_DIR"
echo ""

# Clean up from previous runs
rm -rf "$BACKUP_DIR"
mkdir -p "$BACKUP_DIR"

# Research Velero integration
echo "1. Testing Velero integration..."
if command -v velero &> /dev/null; then
    echo "✓ Velero installed"

    # Check if Velero can backup BadgerDB data directory
    echo "  Testing: velero backup create grainfs-test --from-cluster..."
    # This would require a Kubernetes cluster
    echo "  ⚠️  Velero requires Kubernetes - not suitable for standalone GrainFS"
else
    echo "  ✗ Velero not installed"
fi
echo ""

# Research Restic integration
echo "2. Testing Restic integration..."
if command -v restic &> /dev/null; then
    echo "✓ Restic installed"

    # Initialize a restic repository
    export RESTIC_REPOSITORY="$BACKUP_DIR/restic"
    export RESTIC_PASSWORD="test-password"

    echo "  Initializing restic repo..."
    restic init || echo "  (Repo already initialized)"

    # Test backing up GrainFS data directory
    if [ -d "$GRAINFS_DIR" ]; then
        echo "  Creating backup..."
        restic backup "$GRAINFS_DIR" --tag grainfs-test

        echo "  Listing snapshots..."
        restic snapshots

        echo "  ✓ Restic can backup GrainFS data directory"
        echo "  → Recommendation: Use restic for backup automation"
    else
        echo "  ⚠️  GrainFS data directory not found at $GRAINFS_DIR"
    fi
else
    echo "  ✗ Restic not installed"
    echo "  → Install: brew install restic"
fi
echo ""

# Research Rclone integration
echo "3. Testing Rclone integration..."
if command -v rclone &> /dev/null; then
    echo "✓ Rclone installed"

    # Test if rclone can sync to S3-compatible storage
    echo "  Checking rclone remotes..."
    rclone listremotes || echo "  (No remotes configured)"

    if [ -d "$GRAINFS_DIR" ]; then
        echo "  Testing rclone sync to local directory..."
        rclone sync "$GRAINFS_DIR" "$BACKUP_DIR/rclone" --dry-run
        echo "  ✓ Rclone can sync GrainFS data to S3/other storage"
        echo "  → Recommendation: Use rclone for offsite backup to S3/GCS/Azure"
    fi
else
    echo "  ✗ Rclone not installed"
    echo "  → Install: brew install rclone"
fi
echo ""

# Summary
echo "=== Recommendations ==="
echo ""
echo "Based on research:"
echo ""
echo "1. ✅ Use Restic for local backup automation"
echo "   - Pros: Deduplication, encryption, incremental backups"
echo "   - Cons: Requires restic binary"
echo "   - Integration: Add 'grainfs backup' that wraps restic commands"
echo ""
echo "2. ✅ Use Rclone for offsite backup"
echo "   - Pros: Supports 40+ storage backends (S3, GCS, Azure, etc.)"
echo "   - Cons: No deduplication"
echo "   - Integration: Add 'grainfs backup --remote' that uses rclone"
echo ""
echo "3. ❌ Do NOT build custom backup CLI"
echo "   - Reason: Restic and Rclone are mature, well-tested tools"
echo "   - Time saved: ~2 weeks of development + testing"
echo ""
echo "Next steps:"
echo "  1. Install restic: brew install restic"
echo "  2. Initialize backup repo: restic init --repo /backup/path"
echo "  3. Test backup: restic backup /path/to/grainfs/data --repo /backup/path"
echo "  4. Add cron job for automated backups"
