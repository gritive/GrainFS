#!/usr/bin/env bash
# NBD GrainFS benchmark: macOS GrainFS server ← Colima fio client
# Measures block I/O throughput via nbd-client + fio in the Colima VM.
# Collects pprof profiles for hotspot analysis.
#
# Usage: ./benchmarks/bench_nbd_profile.sh [binary]
#
# Prerequisites:
#   - colima running (colima start)
#   - fio + nbd-client installed in Colima VM:
#       colima ssh -- sudo apt-get install -y fio nbd-client
#   - grainfs binary built (make build)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${1:-./bin/grainfs}"
bench_require_binary "$BINARY"
bench_require_colima

HTTP_PORT=$(bench_free_port)
NBD_PORT=$(bench_free_port)
PPROF_PORT=$(bench_free_port)
HOST_IP="192.168.5.2"      # macOS host as seen from Colima
NBD_DEV="/dev/nbd0"
VOL_SIZE=$((128 * 1024 * 1024))  # 128MB volume
FIO_SIZE="64m"                   # fio workload size (must fit in VOL_SIZE)
DATA_DIR=$(mktemp -d)
SERVER_PID=""

cleanup() {
  echo ""
  echo "=== cleanup ==="
  # Disconnect nbd-client in Colima VM.
  bench_colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true

  if [[ -n "${CPU_PROFILE_PID:-}" ]]; then
    echo "Waiting for CPU profile..."
    wait "$CPU_PROFILE_PID" 2>/dev/null \
      && echo "pprof saved: /tmp/grainfs-nbd-cpu.out" \
      || echo "cpu profile collection failed"
  fi

  if [[ -n "$SERVER_PID" ]]; then
    if [[ "${GRAINFS_PPROF:-0}" = "1" ]]; then
      echo "--- Collecting pprof profiles ---"
      BENCH_PPROF_PREFIX=grainfs-nbd bench_collect_pprof "$PPROF_PORT" /tmp mutex allocs heap goroutine
      echo ""
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

# ─── 1. Start GrainFS (NBD server on macOS) ─────────────────────────────────
echo "=== NBD GrainFS Benchmark ==="
echo "binary : $BINARY"
echo "NBD    : 0.0.0.0:$NBD_PORT (→ Colima sees $HOST_IP:$NBD_PORT)"
echo "volume : ${VOL_SIZE} bytes"
echo "fio    : size=$FIO_SIZE, dev=$NBD_DEV"
echo ""

SERVE_ARGS=(
  "$BINARY" serve
  --data   "$DATA_DIR"
  --port   "$HTTP_PORT"
  --nbd-port "$NBD_PORT"
  --nbd-volume-size "$VOL_SIZE"
  --nfs4-port 0
  --dedup=false
  $(bench_encryption_args)
  --rate-limit-ip-rps 0
  --rate-limit-user-rps 0
)
if [[ "${GRAINFS_PPROF:-0}" = "1" ]]; then
  SERVE_ARGS+=(--pprof-port "$PPROF_PORT")
  echo "pprof : enabled on port $PPROF_PORT"
fi

"${SERVE_ARGS[@]}" &
SERVER_PID=$!
echo "GrainFS PID=$SERVER_PID"

echo -n "Waiting for HTTP health..."
for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:${HTTP_PORT}/" >/dev/null 2>&1; then
    echo " ready"
    break
  fi
  if [[ "$i" -eq 30 ]]; then
    echo " TIMEOUT"
    exit 1
  fi
  echo -n "."
  sleep 1
done

# ─── 2. Connect nbd-client in Colima ────────────────────────────────────────
echo ""
echo "--- Connecting nbd-client in Colima ---"
bench_colima_ssh sudo modprobe nbd max_part=0 2>/dev/null || true
bench_colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true  # clean stale
bench_colima_ssh sudo nbd-client "$HOST_IP" "$NBD_PORT" "$NBD_DEV" -b 4096 -N default
echo "OK: nbd-client connected ($NBD_DEV)"

# Start CPU profile *after* NBD handshake so fio workload is captured.
CPU_PROFILE_PID=""
if [[ "${GRAINFS_PPROF:-0}" = "1" ]]; then
  curl -sf "http://127.0.0.1:${PPROF_PORT}/debug/pprof/profile?seconds=60" \
    -o /tmp/grainfs-nbd-cpu.out &
  CPU_PROFILE_PID=$!
  echo "CPU profile started (PID=$CPU_PROFILE_PID, 60s window)"
fi

# ─── 3. fio workloads ────────────────────────────────────────────────────────
run_fio() {
  local label="$1"; shift
  echo ""
  echo "--- fio: $label ---"
  bench_colima_ssh sudo fio \
    --name="$label" \
    --filename="$NBD_DEV" \
    --size="$FIO_SIZE" \
    --runtime=15 \
    --time_based \
    --output-format=normal \
    "$@" 2>&1 | grep -E "READ:|WRITE:|iops|bw=|lat"
}

run_fio "seq-read-4K"   --ioengine=sync --rw=read   --bs=4k
run_fio "seq-write-4K"  --ioengine=sync --rw=write  --bs=4k
run_fio "seq-read-64K"  --ioengine=sync --rw=read   --bs=64k
run_fio "seq-write-64K" --ioengine=sync --rw=write  --bs=64k
run_fio "rand-read-4K"  --ioengine=sync --rw=randread  --bs=4k
run_fio "rand-write-4K" --ioengine=sync --rw=randwrite --bs=4k

echo ""
echo "=== Benchmark complete ==="
