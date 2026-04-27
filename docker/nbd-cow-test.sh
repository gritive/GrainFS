#!/bin/bash
# Docker-based CoW snapshot+rollback E2E test via NBD for GrainFS.
# Requires: --privileged container with NBD kernel module access.
set -eo pipefail

echo "=== GrainFS NBD CoW E2E Test ==="

DATA_DIR=$(mktemp -d)
S3_PORT=9000
NBD_PORT=10809
NBD_DEV=/dev/nbd0
NBD_SIZE=$((4 * 1024 * 1024)) # 4MB
SERVER_PID=""

cleanup() {
    echo "Cleaning up..."
    nbd-client -d "$NBD_DEV" 2>/dev/null || true
    [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

# Load NBD kernel module
if ! lsmod 2>/dev/null | grep -q nbd; then
    modprobe nbd max_part=0 2>/dev/null || {
        echo "SKIP: NBD kernel module not available (needs --privileged and host kernel support)"
        echo "=== Docker NBD CoW Test: SKIP (module unavailable) ==="
        exit 0
    }
fi
echo "OK: NBD kernel module loaded"

nbd-client -d "$NBD_DEV" 2>/dev/null || true

# Start GrainFS with NBD
echo ""
echo "--- Starting GrainFS server ---"
grainfs serve \
    --data "$DATA_DIR" \
    --port "$S3_PORT" \
    --nbd-port "$NBD_PORT" \
    --nbd-volume-size "$NBD_SIZE" \
    --nfs-port 0 &
SERVER_PID=$!

# Wait for HTTP
echo "Waiting for server..."
for i in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:${S3_PORT}/metrics" >/dev/null 2>&1; then break; fi
    [ "$i" -eq 30 ] && { echo "FAIL: Server did not start within 30s"; exit 1; }
    sleep 1
done
echo "OK: HTTP endpoint ready"

# Wait for NBD port
echo "Waiting for NBD port $NBD_PORT..."
for i in $(seq 1 30); do
    if (exec 3<>/dev/tcp/127.0.0.1/${NBD_PORT}) 2>/dev/null; then
        exec 3<&-; exec 3>&-; break
    fi
    [ "$i" -eq 30 ] && { echo "FAIL: NBD port not ready within 30s"; exit 1; }
    sleep 1
done
echo "OK: NBD port ready"

# Connect nbd-client
echo ""
echo "--- Connecting NBD client ---"
nbd-client 127.0.0.1 "$NBD_PORT" "$NBD_DEV" -b 4096
echo "OK: NBD client connected"

# Write original pattern (0xAA) to first 4KB block
echo ""
echo "--- Writing original pattern ---"
python3 -c "import sys; sys.stdout.buffer.write(b'\xaa' * 4096)" \
    | dd of="$NBD_DEV" bs=4096 count=1 2>/dev/null
sync
echo "OK: Original pattern (0xAA x 4096) written"

# Create snapshot via HTTP
echo ""
echo "--- Creating snapshot ---"
SNAP_RESP=$(curl -sf -X POST "http://127.0.0.1:${S3_PORT}/volumes/default/snapshots" \
    -H "Content-Type: application/json")
SNAP_ID=$(echo "$SNAP_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "OK: Snapshot created: $SNAP_ID"

# Overwrite with modified pattern (0xBB)
echo ""
echo "--- Overwriting with modified pattern ---"
python3 -c "import sys; sys.stdout.buffer.write(b'\xbb' * 4096)" \
    | dd of="$NBD_DEV" bs=4096 count=1 2>/dev/null
sync
echo "OK: Modified pattern (0xBB) written"

# Verify modified pattern is readable
dd if="$NBD_DEV" bs=4096 count=1 2>/dev/null | python3 -c "
import sys
data = sys.stdin.buffer.read()
assert len(data) == 4096, f'expected 4096, got {len(data)}'
assert data == b'\xbb' * 4096, f'expected 0xBB pattern before rollback, got {data[:8].hex()}'
print('OK: Modified pattern (0xBB) verified before rollback')
"

# Rollback via HTTP
echo ""
echo "--- Rolling back to snapshot ---"
curl -sf -X POST "http://127.0.0.1:${S3_PORT}/volumes/default/snapshots/${SNAP_ID}/rollback" \
    -H "Content-Type: application/json" -o /dev/null
echo "OK: Rollback request sent"

# Disconnect and reconnect nbd-client to flush kernel page cache
echo ""
echo "--- Reconnecting NBD client (flush kernel cache) ---"
nbd-client -d "$NBD_DEV"
sleep 1
nbd-client 127.0.0.1 "$NBD_PORT" "$NBD_DEV" -b 4096
blockdev --flushbufs "$NBD_DEV" 2>/dev/null || true
echo "OK: NBD client reconnected"

# Verify original pattern restored
echo ""
echo "--- Verifying original pattern restored ---"
dd if="$NBD_DEV" bs=4096 count=1 iflag=direct 2>/dev/null | python3 -c "
import sys
data = sys.stdin.buffer.read()
assert len(data) == 4096, f'expected 4096, got {len(data)}'
assert data == b'\xaa' * 4096, f'expected 0xAA pattern after rollback, got {data[:8].hex()}'
print('OK: Original pattern (0xAA) restored after rollback')
"

echo ""
echo "=== Docker NBD CoW Test: PASS ==="
