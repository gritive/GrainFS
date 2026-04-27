#!/bin/bash
# Docker-based Dedup E2E test via NBD for GrainFS.
# Tests: SavingsRatio, ReadConsistency, OverwriteRefcount.
# Requires: --privileged container with NBD kernel module access.
set -eo pipefail

echo "=== GrainFS NBD Dedup E2E Test ==="

DATA_DIR=$(mktemp -d)
S3_PORT=9000
NBD_PORT=10809
NBD_DEV=/dev/nbd0
# 8 blocks minimum: 4-block dedup test + overwrite refcount test needs a few more
NBD_SIZE=$((1 * 1024 * 1024)) # 1MB
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
        echo "=== Docker NBD Dedup Test: SKIP (module unavailable) ==="
        exit 0
    }
fi
echo "OK: NBD kernel module loaded"

nbd-client -d "$NBD_DEV" 2>/dev/null || true

# Start GrainFS with --dedup + NBD
echo ""
echo "--- Starting GrainFS server (dedup enabled) ---"
grainfs serve \
    --data "$DATA_DIR" \
    --port "$S3_PORT" \
    --nbd-port "$NBD_PORT" \
    --nbd-volume-size "$NBD_SIZE" \
    --nfs-port 0 \
    --dedup &
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

# Helper: get allocated_blocks for "default" volume
get_allocated_blocks() {
    curl -sf "http://127.0.0.1:${S3_PORT}/volumes/default" \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['allocated_blocks'])"
}

# -------------------------------------------------------------------------
# Test 1: SavingsRatio
# Write 4 identical blocks → dedup should store only 1 unique object.
# -------------------------------------------------------------------------
echo ""
echo "--- Test 1: SavingsRatio ---"

BASELINE=$(get_allocated_blocks)
# Fresh volumes report AllocatedBlocks=-1 (the "untracked" sentinel kept
# for legacy-volume compatibility — see internal/volume/volume.go:110).
# The first write flips tracking on and the counter starts at 0+newBlocks.
# Normalize the baseline so the post-write assertion math works on a fresh
# volume the same way it does on a volume that already has writes.
[ "$BASELINE" -lt 0 ] && BASELINE=0
echo "Baseline allocated_blocks (before writes) = $BASELINE"

# Write same 4KB block to 4 consecutive positions
for i in 0 1 2 3; do
    python3 -c "import sys; sys.stdout.buffer.write(b'\xaa' * 4096)" \
        | dd of="$NBD_DEV" bs=4096 seek="$i" count=1 2>/dev/null
done
sync
sleep 1  # let server persist

ALLOC=$(get_allocated_blocks)
echo "allocated_blocks after 4 identical writes = $ALLOC"
[ "$ALLOC" -eq $(( BASELINE + 1 )) ] || {
    echo "FAIL: SavingsRatio: expected allocated_blocks=$(( BASELINE + 1 )), got $ALLOC"
    exit 1
}
echo "OK: SavingsRatio PASS (4 identical blocks → 1 unique object)"

# -------------------------------------------------------------------------
# Test 2: ReadConsistency
# Write 4 unique blocks → read each back → verify content matches.
# -------------------------------------------------------------------------
echo ""
echo "--- Test 2: ReadConsistency ---"

for i in 0 1 2 3; do
    val=$(( i + 0x10 ))
    python3 -c "import sys; sys.stdout.buffer.write(bytes([$val]) * 4096)" \
        | dd of="$NBD_DEV" bs=4096 seek="$i" count=1 2>/dev/null
done
sync

for i in 0 1 2 3; do
    val=$(( i + 0x10 ))
    result=$(dd if="$NBD_DEV" bs=4096 skip="$i" count=1 2>/dev/null \
        | python3 -c "
import sys
data = sys.stdin.buffer.read()
val = $val
assert len(data) == 4096, f'block $i: expected 4096 bytes, got {len(data)}'
assert data == bytes([val]) * 4096, f'block $i: expected 0x{val:02x} pattern, got {data[:4].hex()}'
print('OK: block $i (0x{:02x}) verified'.format(val))
")
    echo "$result"
done
echo "OK: ReadConsistency PASS"

# -------------------------------------------------------------------------
# Test 3: OverwriteRefcount
# Block A at pos 0 + pos 1 → alloc=1.
# Overwrite pos 0 with block B → alloc=2 (A×1, B×1).
# Overwrite pos 1 with block B → alloc=1 (B×2, A freed).
# -------------------------------------------------------------------------
echo ""
echo "--- Test 3: OverwriteRefcount ---"

# Write block A (0xCC) to positions 4 and 5
for pos in 4 5; do
    python3 -c "import sys; sys.stdout.buffer.write(b'\xcc' * 4096)" \
        | dd of="$NBD_DEV" bs=4096 seek="$pos" count=1 2>/dev/null
done
sync; sleep 1

ALLOC_A=$(get_allocated_blocks)
echo "After A×2 writes: allocated_blocks=$ALLOC_A"
# Previous 4 unique blocks from Test 2 + 1 shared A block = 5
# But exact count depends on what dedup sees — check relative change.

# Overwrite pos 4 with block B (0xDD)
python3 -c "import sys; sys.stdout.buffer.write(b'\xdd' * 4096)" \
    | dd of="$NBD_DEV" bs=4096 seek=4 count=1 2>/dev/null
sync; sleep 1

ALLOC_AB=$(get_allocated_blocks)
echo "After A at pos5 + B at pos4: allocated_blocks=$ALLOC_AB"
[ "$ALLOC_AB" -eq $(( ALLOC_A + 1 )) ] || {
    echo "FAIL: OverwriteRefcount: expected alloc to increase by 1 when adding unique block B, got $ALLOC_A → $ALLOC_AB"
    exit 1
}

# Overwrite pos 5 with block B (0xDD) → A is now unreferenced, freed
python3 -c "import sys; sys.stdout.buffer.write(b'\xdd' * 4096)" \
    | dd of="$NBD_DEV" bs=4096 seek=5 count=1 2>/dev/null
sync; sleep 1

ALLOC_B=$(get_allocated_blocks)
echo "After B×2 (A freed): allocated_blocks=$ALLOC_B"
[ "$ALLOC_B" -eq "$ALLOC_A" ] || {
    echo "FAIL: OverwriteRefcount: expected alloc to return to $ALLOC_A after A freed, got $ALLOC_B"
    exit 1
}
echo "OK: OverwriteRefcount PASS"

echo ""
echo "=== Docker NBD Dedup Test: PASS ==="
