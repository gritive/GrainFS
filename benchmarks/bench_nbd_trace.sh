#!/usr/bin/env bash
# NBD write latency trace: 단계별 PutObject/HeadObject latency 측정
# GRAINFS_VOLUME_TRACE=1 로 활성화, zerolog Debug 레벨로 출력.
#
# Usage: ./benchmarks/bench_nbd_trace.sh [binary]
#
# Prerequisites:
#   - colima running (colima start)
#   - fio + nbd-client installed in Colima VM
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
HOST_IP="192.168.5.2"
NBD_DEV="/dev/nbd0"
VOL_SIZE="${VOL_SIZE:-32Mi}"     # small volume for fast trace runs
FIO_SIZE="${FIO_SIZE:-4m}"
FIO_RUNTIME="${FIO_RUNTIME:-5}"
FIO_DIRECT="${FIO_DIRECT:-1}"    # trace the real NBD request path by default
DATA_DIR=$(mktemp -d)
SERVER_PID=""
LOG_FILE=$(mktemp /tmp/grainfs-nbd-trace-XXXXXX)

cleanup() {
  echo ""
  echo "=== cleanup ==="
  bench_colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

echo "=== NBD Write Latency Trace ==="
echo "binary  : $BINARY"
echo "log     : $LOG_FILE"
echo ""

GRAINFS_VOLUME_TRACE=1 "$BINARY" serve \
  --log-level debug \
  --data   "$DATA_DIR" \
  --port   "$HTTP_PORT" \
  --nbd-port "$NBD_PORT" \
  --nfs4-port 0 \
  --cluster-key "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" \
  $(bench_encryption_args) \
  2>&1 | tee "$LOG_FILE" &
SERVER_PID=$!
echo "GrainFS PID=$SERVER_PID"

if ! bench_wait_http_ready "http://127.0.0.1:${HTTP_PORT}/" "server" 30 1; then
  exit 1
fi
bench_wait_admin_socket "$DATA_DIR" 100 0.2

echo ""
echo "--- Creating NBD export volume ---"
"$BINARY" volume create default --size "$VOL_SIZE" --endpoint "$DATA_DIR/admin.sock" >/dev/null
bench_wait_tcp_port "127.0.0.1" "$NBD_PORT" "NBD listener" 50 0.2

echo ""
echo "--- Connecting nbd-client in Colima ---"
bench_colima_ssh sudo modprobe nbd max_part=0 2>/dev/null || true
bench_colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true
bench_colima_ssh sudo nbd-client "$HOST_IP" "$NBD_PORT" "$NBD_DEV" -b 4096 -N default
echo "OK: nbd-client connected"

echo ""
echo "--- fio: seq-write-4K (${FIO_RUNTIME}s, direct=$FIO_DIRECT) ---"
bench_colima_ssh sudo fio \
  --name=trace-write \
  --filename="$NBD_DEV" \
  --size="$FIO_SIZE" \
  --runtime="$FIO_RUNTIME" \
  --time_based \
  --direct="$FIO_DIRECT" \
  --ioengine=sync \
  --rw=write \
  --bs=4k \
  --output-format=normal 2>&1 | grep -E "WRITE:|iops|bw="

echo ""
echo "=== Trace analysis (from $LOG_FILE) ==="

parse_field() {
  local field="$1"
  python3 -c "
import sys, json, re

def parse_dur(v):
    if isinstance(v, (int, float)):
        return float(v)  # zerolog Dur() emits ms directly
    if isinstance(v, str):
        m = re.match(r'([0-9.]+)(ms|µs|us|ns|s)', v)
        if m:
            n, u = float(m.group(1)), m.group(2)
            if u in ('µs', 'us'): return n / 1000
            if u == 'ns': return n / 1e6
            if u == 's': return n * 1000
            return n  # ms
    return None

vals = []
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try:
        obj = json.loads(line)
        v = obj.get('$field')
        if v is not None:
            ms = parse_dur(v)
            if ms is not None:
                vals.append(ms)
    except Exception:
        pass
if vals:
    vals.sort()
    print(f'  count={len(vals)}, avg={sum(vals)/len(vals):.3f}ms, p50={vals[len(vals)//2]:.3f}ms, p99={vals[min(int(len(vals)*0.99),len(vals)-1)]:.3f}ms, max={vals[-1]:.3f}ms')
else:
    print('  (no data)')
"
}

echo ""
echo "--- Volume WriteAtDeferred: total ---"
grep '"Volume WriteAtDeferred trace"' "$LOG_FILE" | parse_field total || true

echo ""
echo "--- Volume WriteAt fallback: total ---"
grep '"Volume WriteAt trace"' "$LOG_FILE" | parse_field total || true

echo ""
echo "--- Dedup WriteBlock: total ---"
grep '"Dedup WriteBlock trace"' "$LOG_FILE" | parse_field total || true

echo ""
echo "--- BlockIO action counts ---"
printf '  direct=%s dedup=%s cow=%s\n' \
  "$(grep -c '"BlockIO action direct"' "$LOG_FILE" || true)" \
  "$(grep -c '"BlockIO action dedup"' "$LOG_FILE" || true)" \
  "$(grep -c '"BlockIO action cow"' "$LOG_FILE" || true)"

echo ""
echo "--- BlockIO direct WriteAt / PutObject ---"
grep '"BlockIO direct WriteAt"' "$LOG_FILE" | parse_field total || true
grep '"BlockIO direct PutObject"' "$LOG_FILE" | parse_field total || true

echo ""
echo "--- BlockIO async WriteAt ---"
grep '"BlockIO async WriteAt"' "$LOG_FILE" | parse_field total || true

echo ""
echo "--- Distributed WriteAt: ensure_dir ---"
grep '"Distributed WriteAt trace"' "$LOG_FILE" | parse_field ensure_dir || true

echo ""
echo "--- Distributed WriteAt: open ---"
grep '"Distributed WriteAt trace"' "$LOG_FILE" | parse_field open || true

echo ""
echo "--- Distributed WriteAt: pwrite ---"
grep '"Distributed WriteAt trace"' "$LOG_FILE" | parse_field pwrite || true

echo ""
echo "--- Distributed WriteAt: size ---"
grep '"Distributed WriteAt trace"' "$LOG_FILE" | parse_field size_lookup || true

echo ""
echo "--- Distributed WriteAt: badger_update / total ---"
grep '"Distributed WriteAt trace"' "$LOG_FILE" | parse_field badger_update || true
grep '"Distributed WriteAt trace"' "$LOG_FILE" | parse_field total || true

echo ""
echo "--- Local WriteAt: mkdir ---"
grep '"WriteAt trace"' "$LOG_FILE" | parse_field mkdir || true

echo ""
echo "--- Local WriteAt: open ---"
grep '"WriteAt trace"' "$LOG_FILE" | parse_field open || true

echo ""
echo "--- Local WriteAt: pwrite ---"
grep '"WriteAt trace"' "$LOG_FILE" | parse_field pwrite || true

echo ""
echo "--- Local WriteAt: stat ---"
grep '"WriteAt trace"' "$LOG_FILE" | parse_field stat || true

echo ""
echo "--- Local WriteAt: etag ---"
grep '"WriteAt trace"' "$LOG_FILE" | parse_field etag || true

echo ""
echo "--- Local WriteAt: badger_update / total ---"
grep '"WriteAt trace"' "$LOG_FILE" | parse_field badger_update || true
grep '"WriteAt trace"' "$LOG_FILE" | parse_field total || true

echo ""
echo "--- HeadObject: badger_view ---"
grep '"HeadObject trace"' "$LOG_FILE" | parse_field badger_view || true

echo ""
echo "--- PutObject (metadata path): badger_update / total ---"
grep '"PutObject trace"' "$LOG_FILE" | parse_field badger_update || true
grep '"PutObject trace"' "$LOG_FILE" | parse_field total || true

echo ""
echo "Full trace log: $LOG_FILE"
