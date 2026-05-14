#!/usr/bin/env bash
# 9P single-node benchmark with pprof profiling.
#
# Runs GrainFS on macOS, mounts the 9P bucket export inside Colima, runs fio,
# and saves CPU/heap/alloc profiles for hotspot analysis.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${1:-./bin/grainfs}"
bench_require_binary "$BINARY"
bench_require_colima

HOST_IP="${HOST_IP:-192.168.5.2}"
HTTP_PORT="${HTTP_PORT:-$(bench_free_port)}"
P9_PORT="${P9_PORT:-$(bench_free_port)}"
PPROF_PORT="${PPROF_PORT:-$(bench_free_port)}"
BUCKET="${BUCKET:-bench9p}"
MNT="${MNT:-/mnt/grainfs-bench-9p}"
FIO_RUNTIME="${FIO_RUNTIME:-15}"
FIO_STREAM_SIZE="${FIO_STREAM_SIZE:-64m}"
FIO_STREAM_JOBS="${FIO_STREAM_JOBS:-1}"
FIO_RAND_SIZE="${FIO_RAND_SIZE:-16m}"
FIO_RAND_JOBS="${FIO_RAND_JOBS:-1}"
CPU_PROFILE_SECONDS="${CPU_PROFILE_SECONDS:-30}"
PROFILE_DIR="benchmarks/profiles/9p-single-$(date +%Y%m%d-%H%M%S)"

DATA_DIR=$(mktemp -d "grainfs-9p-bench-XXXX" -p /tmp)
BENCH_ENCRYPTION_KEY_FILE="${BENCH_ENCRYPTION_KEY_FILE:-$DATA_DIR/encryption.key}"
export BENCH_ENCRYPTION_KEY_FILE
bench_generate_encryption_key_file "$BENCH_ENCRYPTION_KEY_FILE"

SERVER_PID=""
mkdir -p "$PROFILE_DIR"

cleanup() {
  echo "=== cleanup ==="
  bench_colima_ssh sudo umount -l "$MNT" 2>/dev/null || true
  bench_colima_ssh sudo rmdir "$MNT" 2>/dev/null || true
  [[ -n "${PPROF_PID:-}" ]] && wait "$PPROF_PID" 2>/dev/null || true
  if [[ -n "$SERVER_PID" ]]; then
    bench_collect_pprof "$PPROF_PORT" "$PROFILE_DIR" heap allocs goroutine mutex block
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  [[ -f /tmp/grainfs-9p-bench.log ]] && cp /tmp/grainfs-9p-bench.log "$PROFILE_DIR/server.log" 2>/dev/null || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT INT TERM

echo "=== 9P single-node benchmark ==="
echo "binary : $BINARY"
echo "9p     : ${HOST_IP}:${P9_PORT}"
echo "bucket : $BUCKET"
echo "profile: $PROFILE_DIR"

"$BINARY" serve \
  --data "$DATA_DIR" \
  --port "$HTTP_PORT" \
  --nfs4-port 0 \
  --nbd-port 0 \
  --9p-bind 0.0.0.0 \
  --9p-port "$P9_PORT" \
  --cluster-key "bench-9p-single-key" \
  $(bench_encryption_args) \
  --pprof-port "$PPROF_PORT" \
  --scrub-interval 0 \
  --lifecycle-interval 0 \
  --log-level warn \
  >/tmp/grainfs-9p-bench.log 2>&1 &
SERVER_PID=$!

bench_wait_tcp_port "127.0.0.1" "$HTTP_PORT" "HTTP" 180 0.2
bench_wait_tcp_port "127.0.0.1" "$P9_PORT" "9P" 180 0.2
bench_wait_tcp_port "127.0.0.1" "$PPROF_PORT" "pprof" 180 0.2
bench_wait_admin_socket "$DATA_DIR" 100 0.2
"$BINARY" bucket create "$BUCKET" --endpoint "$DATA_DIR/admin.sock" >/dev/null

bench_colima_ssh sudo modprobe 9p 2>/dev/null || true
bench_colima_ssh sudo modprobe 9pnet 2>/dev/null || true
bench_colima_ssh sudo mkdir -p "$MNT"
bench_colima_ssh sudo mount -t 9p \
  -o "rw,access=any,trans=tcp,port=${P9_PORT},version=9p2000.L,msize=262144,aname=/${BUCKET}" \
  "$HOST_IP" "$MNT"

curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/heap" \
  -o "$PROFILE_DIR/heap_pre.pb.gz" && echo "  heap_pre.pb.gz saved" || true

(
  sleep 3
  echo "  [pprof] starting ${CPU_PROFILE_SECONDS}s CPU profile..."
  curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/profile?seconds=$CPU_PROFILE_SECONDS" \
    -o "$PROFILE_DIR/cpu.pb.gz" && echo "  [pprof] cpu.pb.gz saved" || echo "  [pprof] CPU profile failed"
) &
PPROF_PID=$!

FIO_LOG="$PROFILE_DIR/fio_output.txt"
bench_colima_ssh bash -s <<SCRIPT 2>&1 | tee "$FIO_LOG"
set -e
RUN_ID="fio-\$(date +%s)"
BENCH_DIR="${MNT}/\${RUN_ID}"
sudo mkdir -p "\$BENCH_DIR"

echo "--- sequential write (${FIO_STREAM_JOBS} threads, 128K blocks) ---"
sudo fio --name=seq_write --directory="\$BENCH_DIR" --filename_format="seq-write-\$jobnum" --rw=write --bs=128k \
  --fallocate=none --size="$FIO_STREAM_SIZE" --numjobs="$FIO_STREAM_JOBS" \
  --runtime="$FIO_RUNTIME" --time_based --group_reporting --output-format=normal --ioengine=sync

echo ""
echo "--- sequential read (${FIO_STREAM_JOBS} threads, 128K blocks) ---"
sudo fio --name=seq_read --directory="\$BENCH_DIR" --filename_format="seq-write-\$jobnum" --rw=read --bs=128k \
  --fallocate=none --size="$FIO_STREAM_SIZE" --numjobs="$FIO_STREAM_JOBS" \
  --runtime="$FIO_RUNTIME" --time_based --group_reporting --output-format=normal --ioengine=sync

echo ""
echo "--- random read/write mix (${FIO_RAND_JOBS} threads, 4K blocks, 75% read) ---"
sudo fio --name=rand_mix --directory="\$BENCH_DIR" --filename_format="rand-mix-\$jobnum" --rw=randrw --rwmixread=75 --bs=4k \
  --fallocate=none --size="$FIO_RAND_SIZE" --numjobs="$FIO_RAND_JOBS" \
  --runtime="$FIO_RUNTIME" --time_based --group_reporting --output-format=normal --ioengine=sync

sudo rm -rf "\$BENCH_DIR"
SCRIPT

wait "$PPROF_PID" 2>/dev/null || true
PPROF_PID=""

echo ""
echo "=== CPU top-15 ==="
go tool pprof -top -nodecount=15 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  pprof analysis failed"

echo ""
echo "=== fio summary ==="
grep -E "READ:|WRITE:|lat \\(|iops" "$FIO_LOG" 2>/dev/null || true
echo "PROFILE_DIR=$PROFILE_DIR"
