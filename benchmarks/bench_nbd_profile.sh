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
NBD_PORT=$(free_port)
PPROF_PORT=$(free_port)
HOST_IP="192.168.5.2"      # macOS host as seen from Colima
NBD_DEV="/dev/nbd0"
VOL_SIZE=$((128 * 1024 * 1024))  # 128MB volume
FIO_SIZE="64m"                   # fio workload size (must fit in VOL_SIZE)
DATA_DIR=$(mktemp -d)
SERVER_PID=""

colima_ssh() { colima ssh -- "$@"; }

cleanup() {
  echo ""
  echo "=== cleanup ==="
  # Disconnect nbd-client in Colima VM.
  colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true

  if [[ -n "${CPU_PROFILE_PID:-}" ]]; then
    echo "Waiting for CPU profile..."
    wait "$CPU_PROFILE_PID" 2>/dev/null \
      && echo "pprof saved: /tmp/grainfs-nbd-cpu.out" \
      || echo "cpu profile collection failed"
  fi

  if [[ -n "$SERVER_PID" ]]; then
    if [[ "${GRAINFS_PPROF:-0}" = "1" ]]; then
      echo "--- Collecting pprof profiles ---"
      for profile in mutex allocs heap goroutine; do
        out="/tmp/grainfs-nbd-${profile}.out"
        curl -sf "http://127.0.0.1:${PPROF_PORT}/debug/pprof/${profile}" -o "$out" \
          && echo "pprof saved: $out" \
          || echo "pprof fetch failed: $profile"
      done
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
colima_ssh sudo modprobe nbd max_part=0 2>/dev/null || true
colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true  # clean stale
colima_ssh sudo nbd-client "$HOST_IP" "$NBD_PORT" "$NBD_DEV" -b 4096 -N default
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
  colima_ssh sudo fio \
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
