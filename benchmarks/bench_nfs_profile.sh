#!/usr/bin/env bash
# NFSv4 GrainFS single-node benchmark with pprof profiling
# Mounts NFS inside Colima VM, runs fio mixed workload, collects pprof profiles.
# Usage: ./benchmarks/bench_nfs_profile.sh [binary]
#
# Prerequisites:
#   - colima running (colima start)
#   - fio installed in Colima VM (sudo apt-get install -y fio)
#   - grainfs binary built (make build)

set -euo pipefail

BINARY="${1:-./bin/grainfs}"
if [[ ! -x "$BINARY" ]]; then
  echo "binary not found: $BINARY  (run: make build)" >&2
  exit 1
fi

if ! colima status >/dev/null 2>&1; then
  echo "colima not running — start with: colima start" >&2
  exit 1
fi

free_port() {
  python3 -c "import socket; s=socket.socket(); s.bind(('',0)); p=s.getsockname()[1]; s.close(); print(p)"
}

HTTP_PORT=$(free_port)
NFS4_PORT=$(free_port)
PPROF_PORT=$(free_port)
HOST_IP="192.168.5.2"   # macOS host IP as seen from Colima VM
MNT="/mnt/grainfs-bench-nfs"

PROFILE_DIR="benchmarks/profiles/nfs-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$PROFILE_DIR"

DATA_DIR=$(mktemp -d "grainfs-nfs-bench-XXXX" -p /tmp)

cleanup() {
  echo "=== cleanup ==="
  colima ssh -- sudo umount -l "$MNT" 2>/dev/null || true
  colima ssh -- sudo rmdir "$MNT" 2>/dev/null || true
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

echo "=== starting grainfs (HTTP=:$HTTP_PORT, NFS4=:$NFS4_PORT, pprof=:$PPROF_PORT) ==="
"$BINARY" serve \
  --data "$DATA_DIR" \
  --port "$HTTP_PORT" \
  --nfs4-port "$NFS4_PORT" \
  --nbd-port 0 \
  --no-encryption \
  --pprof-port "$PPROF_PORT" \
  --log-level warn \
  > /tmp/grainfs-nfs-bench.log 2>&1 &
SERVER_PID=$!

echo "  server PID: $SERVER_PID"
echo "  waiting for HTTP health (up to 10s)..."
for _ in $(seq 1 50); do
  if curl -sf "http://127.0.0.1:$HTTP_PORT/" >/dev/null 2>&1; then
    echo "  server ready"
    break
  fi
  sleep 0.2
done

if ! curl -sf "http://127.0.0.1:$HTTP_PORT/" >/dev/null 2>&1; then
  echo "server did not become healthy:" >&2
  tail -20 /tmp/grainfs-nfs-bench.log >&2
  exit 1
fi

echo ""
echo "=== mounting NFS inside Colima (host=$HOST_IP port=$NFS4_PORT) ==="
colima ssh -- sudo mkdir -p "$MNT"
colima ssh -- sudo mount -t nfs4 \
  -o "vers=4.0,port=$NFS4_PORT,rsize=131072,wsize=131072,hard,intr" \
  "${HOST_IP}:/" "$MNT"

echo "  mount OK — checking df:"
colima ssh -- df -h "$MNT" || true

echo ""
echo "=== collecting heap baseline (pre-benchmark) ==="
curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/heap" \
  -o "$PROFILE_DIR/heap_pre.pb.gz" && echo "  heap_pre.pb.gz saved"

echo ""
echo "=== running fio + concurrent CPU profile (30s) ==="

# CPU profile: collected concurrently with fio workload (5s warmup)
(
  sleep 5
  echo "  [pprof] starting 30s CPU profile..."
  curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/profile?seconds=30" \
    -o "$PROFILE_DIR/cpu.pb.gz" && \
    echo "  [pprof] cpu.pb.gz saved" || echo "  [pprof] CPU profile failed"
) &
PPROF_PID=$!

# fio: mixed sequential write + read, 4 threads, 1GB data, 128K blocks
# Targets the NFSv4 mount in Colima VM
FIO_RESULT="$PROFILE_DIR/fio_output.json"
FIO_LOG="$PROFILE_DIR/fio_output.txt"

colima ssh -- bash -s <<SCRIPT 2>&1 | tee "$FIO_LOG"
set -e
BENCH_DIR="${MNT}/fio-bench-\$(date +%s)"
sudo mkdir -p "\$BENCH_DIR"

echo "--- sequential write (4 threads, 128K blocks) ---"
sudo fio \
  --name=seq_write \
  --directory="\$BENCH_DIR" \
  --rw=write \
  --bs=128k \
  --size=256m \
  --numjobs=4 \
  --runtime=15 \
  --time_based \
  --group_reporting \
  --output-format=normal \
  --ioengine=sync

echo ""
echo "--- sequential read (4 threads, 128K blocks) ---"
sudo fio \
  --name=seq_read \
  --directory="\$BENCH_DIR" \
  --rw=read \
  --bs=128k \
  --size=256m \
  --numjobs=4 \
  --runtime=15 \
  --time_based \
  --group_reporting \
  --output-format=normal \
  --ioengine=sync

echo ""
echo "--- random read/write mix (4K blocks, 75% read) ---"
sudo fio \
  --name=rand_mix \
  --directory="\$BENCH_DIR" \
  --rw=randrw \
  --rwmixread=75 \
  --bs=4k \
  --size=64m \
  --numjobs=4 \
  --runtime=15 \
  --time_based \
  --group_reporting \
  --output-format=normal \
  --ioengine=sync

sudo rm -rf "\$BENCH_DIR"
SCRIPT

wait "$PPROF_PID" 2>/dev/null || true

echo ""
echo "=== collecting post-benchmark profiles ==="
curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/heap" \
  -o "$PROFILE_DIR/heap_post.pb.gz" && echo "  heap_post.pb.gz saved"
curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/goroutine?debug=1" \
  -o "$PROFILE_DIR/goroutine.txt" && echo "  goroutine.txt saved"
curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/mutex" \
  -o "$PROFILE_DIR/mutex.pb.gz" && echo "  mutex.pb.gz saved"
curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/block" \
  -o "$PROFILE_DIR/block.pb.gz" && echo "  block.pb.gz saved"
curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/allocs" \
  -o "$PROFILE_DIR/allocs.pb.gz" && echo "  allocs.pb.gz saved"

echo ""
echo "=== profile files saved to $PROFILE_DIR ==="
ls -lh "$PROFILE_DIR/"

echo ""
echo "=== CPU top-15 (go tool pprof) ==="
go tool pprof -top -nodecount=15 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  pprof analysis failed"

echo ""
echo "=== alloc top-10 (hottest allocators) ==="
go tool pprof -top -nodecount=10 "$PROFILE_DIR/allocs.pb.gz" 2>/dev/null || echo "  allocs analysis failed"

echo ""
echo "=== heap growth top-5 ==="
go tool pprof -top -nodecount=5 "$PROFILE_DIR/heap_post.pb.gz" 2>/dev/null || echo "  heap analysis failed"

echo ""
echo "=== fio summary ==="
grep -E "READ:|WRITE:|lat \(|iops" "$FIO_LOG" 2>/dev/null || true

echo ""
echo "PROFILE_DIR=$PROFILE_DIR"
echo ""
echo "=== flame graph commands ==="
echo "  go tool pprof -http=:9090 $PROFILE_DIR/cpu.pb.gz"
echo "  go tool pprof -http=:9091 $PROFILE_DIR/allocs.pb.gz"
