#!/bin/bash
set -eo pipefail

echo "=== GrainFS NBD E2E Test ==="

DATA_DIR=$(mktemp -d)
S3_PORT=9000
NBD_PORT=10809
PPROF_PORT=6060
NBD_DEV=/dev/nbd0
# Small volume: only a few blocks needed for pattern test
NBD_SIZE=$((4 * 1024 * 1024)) # 4MB
SERVER_PID=""

cleanup() {
    echo "Cleaning up..."
    nbd-client -d "$NBD_DEV" 2>/dev/null || true
    if [ -n "$SERVER_PID" ]; then
        # Wait for CPU profile collection to finish before killing the server
        if [ -n "$CPU_PROFILE_PID" ]; then
            echo "Waiting for CPU profile to complete..."
            wait "$CPU_PROFILE_PID" 2>/dev/null && echo "pprof saved: /tmp/grainfs-nbd-cpu.out" \
                || echo "cpu profile collection failed"
        fi
        # Collect remaining pprof profiles before killing the server
        if [ "${GRAINFS_PPROF:-0}" = "1" ]; then
            echo "--- Collecting pprof profiles ---"
            for profile in mutex allocs heap goroutine; do
                out="/tmp/grainfs-nbd-${profile}.out"
                curl -sf "http://127.0.0.1:${PPROF_PORT}/debug/pprof/${profile}" -o "$out" \
                    && echo "pprof saved: $out" \
                    || echo "pprof fetch failed: $profile"
            done
            echo "Analyse:"
            echo "  go tool pprof -top /tmp/grainfs-nbd-cpu.out    # CPU hotspots"
            echo "  go tool pprof -top /tmp/grainfs-nbd-mutex.out  # lock contention"
            echo "  go tool pprof -top /tmp/grainfs-nbd-allocs.out # alloc hotspots"
        fi
        kill "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$DATA_DIR"
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

# Ensure clean state: disconnect any stale nbd connection from previous runs
nbd-client -d "$NBD_DEV" 2>/dev/null || true

# 1. Start GrainFS server with NBD enabled
echo ""
echo "--- Starting GrainFS server ---"
SERVE_ARGS=(grainfs serve
    --data "$DATA_DIR"
    --port "$S3_PORT"
    --nbd-port "$NBD_PORT"
    --nbd-volume-size "$NBD_SIZE"
    --nfs-port 0)
if [ "${GRAINFS_PPROF:-0}" = "1" ]; then
    SERVE_ARGS+=(--pprof-port "$PPROF_PORT")
    echo "pprof enabled on port $PPROF_PORT"
fi
"${SERVE_ARGS[@]}" &
CPU_PROFILE_PID=""
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
echo "OK: S3 endpoint ready (PID=$SERVER_PID)"

# Wait for NBD port to be listening — the S3 endpoint can become ready before
# the NBD server finishes initialization, causing `nbd-client` below to race.
echo "Waiting for NBD port $NBD_PORT..."
for i in $(seq 1 30); do
    # bash built-in TCP probe; avoids requiring nc/ncat in the image
    if (exec 3<>/dev/tcp/127.0.0.1/${NBD_PORT}) 2>/dev/null; then
        exec 3<&-
        exec 3>&-
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "FAIL: NBD port $NBD_PORT not listening within 30s"
        exit 1
    fi
    sleep 1
done
echo "OK: NBD port ready"

# Start CPU profile collection in background (captures I/O test load)
if [ "${GRAINFS_PPROF:-0}" = "1" ]; then
    curl -sf "http://127.0.0.1:${PPROF_PORT}/debug/pprof/profile?seconds=20" \
        -o /tmp/grainfs-nbd-cpu.out &
    CPU_PROFILE_PID=$!
    echo "CPU profile collection started (PID=$CPU_PROFILE_PID, 20s window)"
fi

# 2. Connect nbd-client to GrainFS NBD server
echo ""
echo "--- Connecting NBD client ---"
nbd-client 127.0.0.1 "$NBD_PORT" "$NBD_DEV" -b 4096
echo "OK: NBD client connected to $NBD_DEV"

# 3. Write a known pattern to the block device
echo ""
echo "--- Testing raw block I/O (dd pattern write/read) ---"

PATTERN_FILE=$(mktemp)
VERIFY_FILE=$(mktemp)

# Write 64KB of zeros to offset 0
dd if=/dev/zero of="$NBD_DEV" bs=4096 count=16 2>/dev/null
echo "OK: Wrote 64KB zeros"

# Write a recognizable pattern: 4KB block of 0xAA bytes at offset 0
python3 -c "import sys; sys.stdout.buffer.write(b'\xaa' * 4096)" | dd of="$NBD_DEV" bs=4096 count=1 2>/dev/null
echo "OK: Wrote 4KB pattern (0xAA) at offset 0"

# Write a different pattern at offset 4096
python3 -c "import sys; sys.stdout.buffer.write(b'\xbb' * 4096)" | dd of="$NBD_DEV" bs=4096 seek=1 count=1 2>/dev/null
echo "OK: Wrote 4KB pattern (0xBB) at offset 4096"

# Read back and verify first block
dd if="$NBD_DEV" bs=4096 count=1 2>/dev/null | python3 -c "
import sys
data = sys.stdin.buffer.read()
assert len(data) == 4096, f'expected 4096 bytes, got {len(data)}'
assert data == b'\\xaa' * 4096, 'block 0 mismatch'
print('OK: Block 0 (0xAA pattern) verified')
"

# Read back and verify second block
dd if="$NBD_DEV" bs=4096 skip=1 count=1 2>/dev/null | python3 -c "
import sys
data = sys.stdin.buffer.read()
assert len(data) == 4096, f'expected 4096 bytes, got {len(data)}'
assert data == b'\\xbb' * 4096, 'block 1 mismatch'
print('OK: Block 1 (0xBB pattern) verified')
"

rm -f "$PATTERN_FILE" "$VERIFY_FILE"

# 4. Disconnect NBD
echo ""
echo "--- Disconnecting NBD client ---"
nbd-client -d "$NBD_DEV"
echo "OK: NBD client disconnected"

echo ""
echo "=== Docker NBD Test: PASS ==="
