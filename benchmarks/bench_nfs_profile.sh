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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${1:-./bin/grainfs}"
bench_require_binary "$BINARY"
bench_require_colima

HTTP_PORT=$(bench_free_port)
NFS4_PORT=$(bench_free_port)
PPROF_PORT=$(bench_free_port)
HOST_IP="192.168.5.2"   # macOS host IP as seen from Colima VM
MNT="/mnt/grainfs-bench-nfs"
NFS_VERS="${NFS_VERS:-4.0}"
NFS_SERVER_WARMUP_SLEEP="${NFS_SERVER_WARMUP_SLEEP:-3}"

PROFILE_DIR="benchmarks/profiles/nfs-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$PROFILE_DIR"

DATA_DIR=$(mktemp -d "grainfs-nfs-bench-XXXX" -p /tmp)

cleanup() {
  echo "=== cleanup ==="
  bench_colima_ssh sudo umount -l "$MNT" 2>/dev/null || true
  bench_colima_ssh sudo rmdir "$MNT" 2>/dev/null || true
  [[ -n "${PPROF_PID:-}" ]] && kill "$PPROF_PID" 2>/dev/null || true
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    # Wait up to 3s for graceful shutdown, then SIGKILL
    for _ in $(seq 1 15); do
      kill -0 "$SERVER_PID" 2>/dev/null || break
      sleep 0.2
    done
    kill -9 "$SERVER_PID" 2>/dev/null || true
  fi
  [[ -f /tmp/grainfs-nfs-bench.log ]] && cp /tmp/grainfs-nfs-bench.log "$PROFILE_DIR/server.log" 2>/dev/null || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

echo "=== starting grainfs (HTTP=:$HTTP_PORT, NFS4=:$NFS4_PORT, pprof=:$PPROF_PORT) ==="
"$BINARY" serve \
  --data "$DATA_DIR" \
  --port "$HTTP_PORT" \
  --nfs4-port "$NFS4_PORT" \
  --nbd-port 0 \
  $(bench_encryption_args) \
  --pprof-port "$PPROF_PORT" \
  --rate-limit-ip-rps 0 \
  --rate-limit-user-rps 0 \
  --log-level warn \
  > /tmp/grainfs-nfs-bench.log 2>&1 &
SERVER_PID=$!

echo "  server PID: $SERVER_PID"
if ! bench_wait_http_ready "http://127.0.0.1:$HTTP_PORT/" "server" 50 0.2; then
  echo "server did not become healthy:" >&2
  tail -20 /tmp/grainfs-nfs-bench.log >&2
  exit 1
fi
echo "  waiting ${NFS_SERVER_WARMUP_SLEEP}s for raft group leadership..."
sleep "$NFS_SERVER_WARMUP_SLEEP"

echo ""
echo "=== mounting NFS inside Colima (vers=$NFS_VERS host=$HOST_IP port=$NFS4_PORT) ==="
bench_colima_ssh sudo mkdir -p "$MNT"
bench_colima_ssh sudo mount -t nfs4 \
  -o "vers=$NFS_VERS,port=$NFS4_PORT,rsize=131072,wsize=131072,hard,intr" \
  "${HOST_IP}:/" "$MNT"

echo "  mount OK — checking df:"
bench_colima_ssh df -h "$MNT" || true

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

bench_colima_ssh bash -s <<SCRIPT 2>&1 | tee "$FIO_LOG"
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
bench_collect_pprof "$PPROF_PORT" "$PROFILE_DIR" heap goroutine mutex block allocs

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
