#!/bin/bash
set -eo pipefail

echo "=== GrainFS NBD E2E Test ==="

DATA_DIR=$(mktemp -d)
MOUNT_DIR=$(mktemp -d)
S3_PORT=9000
NBD_PORT=10809
NBD_DEV=/dev/nbd0
SERVER_PID=""

cleanup() {
    echo "Cleaning up..."
    umount "$MOUNT_DIR" 2>/dev/null || true
    nbd-client -d "$NBD_DEV" 2>/dev/null || true
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR" "$MOUNT_DIR"
}
trap cleanup EXIT

# Load NBD kernel module
if ! lsmod 2>/dev/null | grep -q nbd; then
    modprobe nbd max_part=0 2>/dev/null || {
        echo "SKIP: NBD kernel module not available (needs --privileged and host kernel support)"
        echo "=== Docker NBD Test: SKIP (module unavailable) ==="
        exit 0
    }
fi
echo "OK: NBD kernel module loaded"

# 1. Start GrainFS server with NBD enabled
echo ""
echo "--- Starting GrainFS server ---"
grainfs serve \
    --data "$DATA_DIR" \
    --port "$S3_PORT" \
    --nbd-port "$NBD_PORT" \
    --no-encryption \
    --nfs-port 0 &
SERVER_PID=$!

# Wait for server to be ready
echo "Waiting for server..."
for i in $(seq 1 30); do
    if curl -sf http://127.0.0.1:${S3_PORT}/metrics > /dev/null 2>&1; then
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "FAIL: Server did not start within 30s"
        exit 1
    fi
    sleep 1
done
echo "OK: Server started (PID=$SERVER_PID)"

# 2. Connect nbd-client to GrainFS NBD server
echo ""
echo "--- Connecting NBD client ---"
nbd-client 127.0.0.1 "$NBD_PORT" "$NBD_DEV" -b 4096
echo "OK: NBD client connected to $NBD_DEV"

# 3. Format the block device
echo ""
echo "--- Formatting block device ---"
mkfs.ext4 -q "$NBD_DEV"
echo "OK: ext4 filesystem created"

# 4. Mount and do file I/O
echo ""
echo "--- Mounting and testing file I/O ---"
mount "$NBD_DEV" "$MOUNT_DIR"
echo "OK: Mounted at $MOUNT_DIR"

# Write a test file
TEST_DATA="Hello from GrainFS NBD E2E test! $(date)"
echo "$TEST_DATA" > "$MOUNT_DIR/test.txt"
echo "OK: Wrote test.txt"

# Read it back and verify
READ_DATA=$(cat "$MOUNT_DIR/test.txt")
if [ "$READ_DATA" = "$TEST_DATA" ]; then
    echo "OK: Read back matches written data"
else
    echo "FAIL: Data mismatch"
    echo "  Written: $TEST_DATA"
    echo "  Read:    $READ_DATA"
    exit 1
fi

# Write a larger file (1MB)
dd if=/dev/urandom of="$MOUNT_DIR/large.bin" bs=1024 count=1024 2>/dev/null
LARGE_SIZE=$(stat -c%s "$MOUNT_DIR/large.bin")
if [ "$LARGE_SIZE" -eq 1048576 ]; then
    echo "OK: 1MB file written and verified"
else
    echo "FAIL: Large file size mismatch: $LARGE_SIZE"
    exit 1
fi

# List files
FILE_COUNT=$(ls "$MOUNT_DIR" | grep -cv lost+found)
if [ "$FILE_COUNT" -eq 2 ]; then
    echo "OK: File listing correct ($FILE_COUNT files)"
else
    echo "FAIL: Expected 2 files, got $FILE_COUNT"
    exit 1
fi

# Delete a file
rm "$MOUNT_DIR/test.txt"
if [ ! -f "$MOUNT_DIR/test.txt" ]; then
    echo "OK: File deleted successfully"
else
    echo "FAIL: File still exists after delete"
    exit 1
fi

# Sync and unmount
sync
umount "$MOUNT_DIR"
echo "OK: Unmounted cleanly"

# Disconnect NBD
nbd-client -d "$NBD_DEV"
echo "OK: NBD client disconnected"

echo ""
echo "=== Docker NBD Test: PASS ==="
