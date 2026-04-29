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
HOST_IP="192.168.5.2"
NBD_DEV="/dev/nbd0"
VOL_SIZE=$((32 * 1024 * 1024))   # 32MB — 작은 볼륨으로 측정 빠르게
DATA_DIR=$(mktemp -d)
SERVER_PID=""
LOG_FILE=$(mktemp /tmp/grainfs-nbd-trace-XXXXXX)

colima_ssh() { colima ssh -- "$@"; }

cleanup() {
  echo ""
  echo "=== cleanup ==="
  colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true
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
  --raft-log-fsync=false \
  --dedup=false \
  --data   "$DATA_DIR" \
  --port   "$HTTP_PORT" \
  --nbd-port "$NBD_PORT" \
  --nbd-volume-size "$VOL_SIZE" \
  --nfs4-port 0 \
  2>&1 | tee "$LOG_FILE" &
SERVER_PID=$!
echo "GrainFS PID=$SERVER_PID"

echo -n "Waiting for HTTP health..."
for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:${HTTP_PORT}/" >/dev/null 2>&1; then
    echo " ready"
    break
  fi
  if [[ "$i" -eq 30 ]]; then echo " TIMEOUT"; exit 1; fi
  echo -n "."
  sleep 1
done

echo ""
echo "--- Connecting nbd-client in Colima ---"
colima_ssh sudo modprobe nbd max_part=0 2>/dev/null || true
colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true
colima_ssh sudo nbd-client "$HOST_IP" "$NBD_PORT" "$NBD_DEV" -b 4096 -N default
echo "OK: nbd-client connected"

echo ""
echo "--- fio: seq-write-4K (5s, 200 blocks) ---"
colima_ssh sudo fio \
  --name=trace-write \
  --filename="$NBD_DEV" \
  --size=4m \
  --runtime=5 \
  --time_based \
  --ioengine=sync \
  --rw=write \
  --bs=4k \
  --end_fsync=1 \
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
echo "--- putObjectNx (SYNC): write_file_atomic (CreateTemp+Write+Close+Rename) ---"
grep '"putObjectNx trace"' "$LOG_FILE" | parse_field write_file_atomic || true

echo ""
echo "--- putObjectNx (SYNC): raft_propose (ProposeWait = Raft log fsync) ---"
grep '"putObjectNx trace"' "$LOG_FILE" | parse_field raft_propose || true

echo ""
echo "--- putObjectNx (SYNC): total ---"
grep '"putObjectNx trace"' "$LOG_FILE" | grep '"total"' | parse_field total || true

echo ""
echo "--- putObjectNxAsync (WRITE-BACK): write_file_atomic ---"
grep '"putObjectNxAsync trace"' "$LOG_FILE" | parse_field write_file_atomic || true

echo ""
echo "--- putObjectNxAsync (WRITE-BACK): commit raft_propose (on flush) ---"
grep '"putObjectNxAsync commit trace"' "$LOG_FILE" | parse_field raft_propose || true

echo ""
echo "--- HeadObject: badger_view ---"
grep '"HeadObject trace"' "$LOG_FILE" | parse_field badger_view || true

echo ""
echo "--- raftFlush: batch_wait (proposal→flush 대기) ---"
grep '"raftFlush trace"' "$LOG_FILE" | parse_field batch_wait || true

echo ""
echo "--- raftFlush: persist_log (BadgerDB AppendEntries = fdatasync) ---"
grep '"raftFlush trace"' "$LOG_FILE" | parse_field persist_log || true

echo ""
echo "--- raftFlush: flush_total ---"
grep '"raftFlush trace"' "$LOG_FILE" | parse_field flush_total || true

echo ""
echo "Full trace log: $LOG_FILE"
