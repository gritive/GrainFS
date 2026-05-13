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
VOL_SIZE="${VOL_SIZE:-128Mi}"    # volume size accepted by `grainfs volume create`
FIO_SIZE="${FIO_SIZE:-64m}"      # fio workload size (must fit in VOL_SIZE)
FIO_RUNTIME="${FIO_RUNTIME:-15}"
FIO_CASES="${FIO_CASES:-seq-read-4K seq-write-4K seq-read-64K seq-write-64K rand-read-4K rand-write-4K}"
FIO_DIRECT="${FIO_DIRECT:-0}"    # set 1 to bypass Linux client page cache
CPU_PROFILE_SECONDS="${CPU_PROFILE_SECONDS:-60}"
PROFILE_DIR="benchmarks/profiles/nbd-single-$(date +%Y%m%d-%H%M%S)"
DATA_DIR=$(mktemp -d)
SERVER_PID=""
mkdir -p "$PROFILE_DIR"

cleanup() {
  echo ""
  echo "=== cleanup ==="
  # Disconnect nbd-client in Colima VM.
  bench_colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true

  if [[ -n "${CPU_PROFILE_PID:-}" ]]; then
    echo "Waiting for CPU profile..."
    wait "$CPU_PROFILE_PID" 2>/dev/null \
      && echo "pprof saved: $PROFILE_DIR/cpu.pb.gz" \
      || echo "cpu profile collection failed"
  fi

  if [[ -n "$SERVER_PID" ]]; then
    if [[ "${GRAINFS_PPROF:-0}" = "1" ]]; then
      echo "--- Collecting pprof profiles ---"
      bench_collect_pprof "$PPROF_PORT" "$PROFILE_DIR" mutex allocs heap goroutine
      echo ""
      echo "Analyse:"
      echo "  go tool pprof -top $PROFILE_DIR/cpu.pb.gz    # CPU hotspots"
      echo "  go tool pprof -top $PROFILE_DIR/mutex.pb.gz  # lock contention"
      echo "  go tool pprof -top $PROFILE_DIR/allocs.pb.gz # alloc hotspots"
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
echo "fio    : size=$FIO_SIZE, runtime=${FIO_RUNTIME}s, direct=$FIO_DIRECT, dev=$NBD_DEV"
echo "profile: $PROFILE_DIR"
echo ""

SERVE_ARGS=(
  "$BINARY" serve
  --data   "$DATA_DIR"
  --port   "$HTTP_PORT"
  --nbd-port "$NBD_PORT"
  --nfs4-port 0
  --cluster-key "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  $(bench_encryption_args)
)
if [[ "${GRAINFS_PPROF:-0}" = "1" ]]; then
  SERVE_ARGS+=(--pprof-port "$PPROF_PORT")
  echo "pprof : enabled on port $PPROF_PORT"
fi

"${SERVE_ARGS[@]}" &
SERVER_PID=$!
echo "GrainFS PID=$SERVER_PID"

if ! bench_wait_http_ready "http://127.0.0.1:${HTTP_PORT}/" "server" 30 1; then
  exit 1
fi
bench_wait_admin_socket "$DATA_DIR" 100 0.2
echo "--- Creating NBD export volume ---"
"$BINARY" volume create default --size "$VOL_SIZE" --endpoint "$DATA_DIR/admin.sock" >/dev/null
bench_wait_tcp_port "127.0.0.1" "$NBD_PORT" "NBD listener" 50 0.2

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
  curl -sf "http://127.0.0.1:${PPROF_PORT}/debug/pprof/profile?seconds=$CPU_PROFILE_SECONDS" \
    -o "$PROFILE_DIR/cpu.pb.gz" &
  CPU_PROFILE_PID=$!
  echo "CPU profile started (PID=$CPU_PROFILE_PID, ${CPU_PROFILE_SECONDS}s window)"
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
    --runtime="$FIO_RUNTIME" \
    --time_based \
    --direct="$FIO_DIRECT" \
    --output-format=normal \
    "$@" 2>&1 | tee -a "$PROFILE_DIR/fio_output.txt" | grep -E "READ:|WRITE:|iops|bw=|lat"
}

for fio_case in $FIO_CASES; do
  case "$fio_case" in
    seq-read-4K) run_fio "$fio_case" --ioengine=sync --rw=read --bs=4k ;;
    seq-write-4K) run_fio "$fio_case" --ioengine=sync --rw=write --bs=4k ;;
    seq-read-64K) run_fio "$fio_case" --ioengine=sync --rw=read --bs=64k ;;
    seq-write-64K) run_fio "$fio_case" --ioengine=sync --rw=write --bs=64k ;;
    rand-read-4K) run_fio "$fio_case" --ioengine=sync --rw=randread --bs=4k ;;
    rand-write-4K) run_fio "$fio_case" --ioengine=sync --rw=randwrite --bs=4k ;;
    *)
      echo "[error] unknown FIO_CASES entry: $fio_case" >&2
      exit 1
      ;;
  esac
done

echo ""
echo "=== Benchmark complete ==="
if [[ "${GRAINFS_PPROF:-0}" = "1" ]]; then
  wait "$CPU_PROFILE_PID" 2>/dev/null || true
  CPU_PROFILE_PID=""
  echo ""
  echo "=== CPU top-10 (go tool pprof) ==="
  go tool pprof -top -nodecount=10 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  pprof analysis failed"
fi
echo "PROFILE_DIR=$PROFILE_DIR"
