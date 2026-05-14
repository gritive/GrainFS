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

if [[ "${NO_ENCRYPTION:-0}" == "1" ]]; then
  echo "[error] encryption is mandatory for this benchmark; do not set NO_ENCRYPTION=1" >&2
  exit 1
fi

WORKLOAD="${WORKLOAD:-streaming}"
FIO_RUNTIME="${FIO_RUNTIME:-15}"
FIO_STREAM_SIZE="${FIO_STREAM_SIZE:-16m}"
FIO_STREAM_JOBS="${FIO_STREAM_JOBS:-4}"
FIO_RAND_SIZE="${FIO_RAND_SIZE:-1m}"
FIO_RAND_JOBS="${FIO_RAND_JOBS:-4}"
case "$WORKLOAD" in
  streaming|metadata|append) ;;
  *)
    echo "[error] unknown WORKLOAD=$WORKLOAD (expected: streaming|metadata|append)" >&2
    exit 1
    ;;
esac

HTTP_PORT=$(bench_free_port)
NFS4_PORT=$(bench_free_port)
PPROF_PORT=$(bench_free_port)
HOST_IP="${HOST_IP:-192.168.5.2}"   # macOS host IP as seen from Colima VM
BUCKET="${BUCKET:-benchnfs}"
MNT="/mnt/grainfs-bench-nfs"
NFS_VERS="${NFS_VERS:-4.0}"
NFS_SERVER_WARMUP_SLEEP="${NFS_SERVER_WARMUP_SLEEP:-3}"

PROFILE_DIR="benchmarks/profiles/nfs-${WORKLOAD}-$(date +%Y%m%d-%H%M%S)"
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
  --cluster-key "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" \
  $(bench_encryption_args) \
  --pprof-port "$PPROF_PORT" \
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
echo "=== preparing bucket export ($BUCKET) ==="
bench_bootstrap_iam_credentials "$BINARY" "$DATA_DIR"
bench_create_bucket_admin_retry "$BINARY" "$DATA_DIR" "$BUCKET"
"$BINARY" nfs export add "$BUCKET" --endpoint "$DATA_DIR/admin.sock" || true

echo ""
echo "=== mounting NFS inside Colima (vers=$NFS_VERS host=$HOST_IP port=$NFS4_PORT) ==="
bench_colima_ssh sudo mkdir -p "$MNT"
bench_colima_ssh sudo mount -t nfs4 \
  -o "vers=$NFS_VERS,port=$NFS4_PORT,rsize=131072,wsize=131072,hard,intr" \
  "${HOST_IP}:/${BUCKET}" "$MNT"

echo "  mount OK — checking df:"
bench_colima_ssh df -h "$MNT" || true

echo ""
echo "=== collecting heap baseline (pre-benchmark) ==="
curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/heap" \
  -o "$PROFILE_DIR/heap_pre.pb.gz" && echo "  heap_pre.pb.gz saved"

FIO_LOG="$PROFILE_DIR/fio_output.txt"

case "$WORKLOAD" in
  streaming)
    echo ""
    echo "=== WORKLOAD=streaming: 3 fio scenarios + concurrent CPU profile ==="
    CPU_PROFILE_SECONDS="${CPU_PROFILE_SECONDS:-30}"
    ;;
  metadata)
    echo ""
    echo "=== WORKLOAD=metadata: 10K small-file create storm + concurrent 60s CPU profile ==="
    CPU_PROFILE_SECONDS="${CPU_PROFILE_SECONDS:-60}"
    ;;
  append)
    echo ""
    echo "=== WORKLOAD=append: 1GB single-thread 4K append + concurrent 60s CPU profile ==="
    CPU_PROFILE_SECONDS="${CPU_PROFILE_SECONDS:-60}"
    ;;
esac

(
  sleep 5
  echo "  [pprof] starting ${CPU_PROFILE_SECONDS}s CPU profile..."
  curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/profile?seconds=$CPU_PROFILE_SECONDS" \
    -o "$PROFILE_DIR/cpu.pb.gz" && \
    echo "  [pprof] cpu.pb.gz saved" || echo "  [pprof] CPU profile failed"
) &
PPROF_PID=$!

case "$WORKLOAD" in
  streaming)
    bench_colima_ssh bash -s <<SCRIPT 2>&1 | tee "$FIO_LOG"
set -e
BENCH_DIR="${MNT}/fio-bench-\$(date +%s)"
sudo mkdir -p "\$BENCH_DIR"

echo "--- sequential write (${FIO_STREAM_JOBS} threads, 128K blocks) ---"
sudo fio --name=seq_write --directory="\$BENCH_DIR" --rw=write --bs=128k \
  --fallocate=none \
  --size="$FIO_STREAM_SIZE" --numjobs="$FIO_STREAM_JOBS" --runtime="$FIO_RUNTIME" --time_based --group_reporting \
  --output-format=normal --ioengine=sync

echo ""
echo "--- sequential read (${FIO_STREAM_JOBS} threads, 128K blocks) ---"
sudo fio --name=seq_read --directory="\$BENCH_DIR" --rw=read --bs=128k \
  --fallocate=none \
  --size="$FIO_STREAM_SIZE" --numjobs="$FIO_STREAM_JOBS" --runtime="$FIO_RUNTIME" --time_based --group_reporting \
  --output-format=normal --ioengine=sync

echo ""
echo "--- random read/write mix (${FIO_RAND_JOBS} threads, 4K blocks, 75% read) ---"
sudo fio --name=rand_mix --directory="\$BENCH_DIR" --rw=randrw --rwmixread=75 \
  --fallocate=none \
  --bs=4k --size="$FIO_RAND_SIZE" --numjobs="$FIO_RAND_JOBS" --runtime="$FIO_RUNTIME" --time_based --group_reporting \
  --output-format=normal --ioengine=sync

sudo rm -rf "\$BENCH_DIR"
SCRIPT
    ;;

  metadata)
    bench_colima_ssh bash -s <<SCRIPT 2>&1 | tee "$FIO_LOG"
set -e
BENCH_DIR="${MNT}/storm-\$(date +%s)"
sudo mkdir -p "\$BENCH_DIR"

if ! command -v parallel >/dev/null 2>&1; then
  echo "[bench] installing GNU parallel..."
  sudo apt-get install -y parallel >/dev/null 2>&1 || {
    echo "[error] parallel install failed" >&2
    exit 1
  }
fi

# 10000 files * 4KB content via parallel dd, 8-way parallelism.
# Each invocation = NFS4 OPEN(CREATE) + WRITE + CLOSE.
echo "--- small-file create storm (10000 files * 4KB, parallel=8) ---"
START=\$(date +%s.%N)
seq 1 10000 | sudo parallel -j 8 \
  "dd if=/dev/zero of=\$BENCH_DIR/f{} bs=4k count=1 status=none"
END=\$(date +%s.%N)

ELAPSED=\$(python3 -c "print(\$END - \$START)")
RATE=\$(python3 -c "print(round(10000 / \$ELAPSED, 1))")
echo ""
echo "metadata: 10000 files in \${ELAPSED}s = \${RATE} files/sec"

sudo rm -rf "\$BENCH_DIR"
SCRIPT
    ;;

  append)
    bench_colima_ssh bash -s <<SCRIPT 2>&1 | tee "$FIO_LOG"
set -e
BENCH_DIR="${MNT}/append-\$(date +%s)"
sudo mkdir -p "\$BENCH_DIR"

# Single-thread 4K writes to a 1GB target file, sync engine.
# Pre-WIP O(n^2) MD5 path makes throughput crater as the file grows.
# fio reports per-second bw via --write_bw_log.
echo "--- single-thread append (4K blocks, 1GB target, sync) ---"
sudo fio \
  --name=append \
  --filename="\$BENCH_DIR/file" \
  --rw=write \
  --fallocate=none \
  --bs=4k \
  --size=1g \
  --numjobs=1 \
  --ioengine=sync \
  --runtime=120 \
  --time_based=0 \
  --output-format=normal \
  --write_bw_log="\$BENCH_DIR/append" \
  --log_avg_msec=1000

# Compute first-window vs last-window throughput ratio.
# bw_log row format: time_msec, bw_KiBps, rw_type, bs, ...
LOG_FILE=\$(ls "\$BENCH_DIR"/append_bw.*.log 2>/dev/null | head -1)
if [[ -n "\$LOG_FILE" ]]; then
  echo ""
  echo "--- throughput ratio (first vs last window) ---"
  awk -F, '
    { rows[NR]=\$2 }
    END {
      if (NR < 5) { printf "[warn] only %d samples — ratio not meaningful\n", NR; exit 0 }
      n = int(NR / 5); if (n < 3) n = 3; if (n > NR) n = NR
      first=0; last=0
      for (i=1; i<=n; i++) first += rows[i]
      for (i=NR-n+1; i<=NR; i++) last += rows[i]
      first /= n; last /= n
      ratio = (first > 0) ? last/first : 0
      printf "samples=%d  first_avg=%.0f KiBps  last_avg=%.0f KiBps  ratio=%.2f\n", NR, first, last, ratio
      if (ratio >= 0.8) print "PASS  (ratio >= 0.8)"
      else print "FAIL  (ratio < 0.8 suggests O(n^2) regression)"
    }
  ' "\$LOG_FILE"
fi

sudo rm -rf "\$BENCH_DIR"
SCRIPT
    ;;
esac

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
