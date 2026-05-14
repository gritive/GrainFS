#!/usr/bin/env bash
# 9P cluster benchmark with pprof profiling.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${1:-./bin/grainfs}"
bench_require_binary "$BINARY"
bench_require_colima

HOST_IP="${HOST_IP:-192.168.5.2}"
NODE_COUNT="${NODE_COUNT:-3}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-9p-cluster-bench}"
BUCKET="${BUCKET:-bench9p}"
MNT="${MNT:-/mnt/grainfs-bench-9p-cluster}"
FIO_RUNTIME="${FIO_RUNTIME:-15}"
FIO_STREAM_SIZE="${FIO_STREAM_SIZE:-64m}"
FIO_STREAM_JOBS="${FIO_STREAM_JOBS:-1}"
FIO_RAND_SIZE="${FIO_RAND_SIZE:-16m}"
FIO_RAND_JOBS="${FIO_RAND_JOBS:-1}"
CPU_PROFILE_SECONDS="${CPU_PROFILE_SECONDS:-30}"

if [[ "$NODE_COUNT" -lt 2 ]]; then
  echo "[error] NODE_COUNT must be >= 2 for clustered 9P profile" >&2
  exit 1
fi

HTTP_PORTS=()
RAFT_PORTS=()
P9_PORTS=()
PPROF_PORTS=()
for _ in $(seq 1 "$NODE_COUNT"); do
  HTTP_PORTS+=("$(bench_free_port)")
  RAFT_PORTS+=("$(bench_free_port)")
  P9_PORTS+=("$(bench_free_port)")
  PPROF_PORTS+=("$(bench_free_port)")
done

PIDS=()
rm -rf "$BENCH_DIR"
for idx in $(seq 0 $((NODE_COUNT - 1))); do
  mkdir -p "$BENCH_DIR/n$idx"
done
BENCH_ENCRYPTION_KEY_FILE="${BENCH_ENCRYPTION_KEY_FILE:-$BENCH_DIR/encryption.key}"
export BENCH_ENCRYPTION_KEY_FILE
bench_generate_encryption_key_file "$BENCH_ENCRYPTION_KEY_FILE"

PROFILE_DIR="benchmarks/profiles/9p-${NODE_COUNT}-node-cluster-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$PROFILE_DIR"

cleanup() {
  echo "=== cleanup ==="
  bench_colima_ssh sudo umount -l "$MNT" 2>/dev/null || true
  bench_colima_ssh sudo rmdir "$MNT" 2>/dev/null || true
  [[ -n "${PPROF_PID:-}" ]] && wait "$PPROF_PID" 2>/dev/null || true
  if [[ -n "${LEADER_PPROF_PORT:-}" ]]; then
    bench_collect_pprof "$LEADER_PPROF_PORT" "$PROFILE_DIR" heap allocs goroutine mutex block
  fi
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  bench_copy_node_logs "$BENCH_DIR" "$PROFILE_DIR"
  rm -rf "$BENCH_DIR"
}
trap cleanup EXIT INT TERM

raft_addr() {
  echo "127.0.0.1:${RAFT_PORTS[$1]}"
}

start_node() {
  local i="$1"
  "$BINARY" serve \
    --data "$BENCH_DIR/n$i" \
    --port "${HTTP_PORTS[$i]}" \
    --node-id "bench-9p-node-$i" \
    --raft-addr "$(raft_addr "$i")" \
    --cluster-key "bench-9p-cluster-key" \
    $(bench_encryption_args) \
    --nfs4-port 0 \
    --nbd-port 0 \
    --9p-bind 0.0.0.0 \
    --9p-port "${P9_PORTS[$i]}" \
    --pprof-port "${PPROF_PORTS[$i]}" \
    --scrub-interval 0 \
    --lifecycle-interval 0 \
    --log-level warn \
    >"$BENCH_DIR/n$i.log" 2>&1 &
  PIDS+=($!)
  echo "  node-$i: HTTP=:${HTTP_PORTS[$i]} 9P=:${P9_PORTS[$i]} Raft=$(raft_addr "$i")"
}

echo "=== 9P ${NODE_COUNT}-node cluster benchmark ==="
echo "profile: $PROFILE_DIR"

start_node 0
bench_wait_tcp_port "127.0.0.1" "${HTTP_PORTS[0]}" "node-0 HTTP" 180 0.2
bench_wait_tcp_port "127.0.0.1" "${PPROF_PORTS[0]}" "node-0 pprof" 180 0.2
bench_wait_admin_socket "$BENCH_DIR/n0" 100 0.2
"$BINARY" bucket create "$BUCKET" --endpoint "$BENCH_DIR/n0/admin.sock" >/dev/null

for i in $(seq 1 $((NODE_COUNT - 1))); do
  printf '%s' "$(raft_addr 0)" >"$BENCH_DIR/n$i/.join-pending"
  chmod 600 "$BENCH_DIR/n$i/.join-pending"
  start_node "$i"
  bench_wait_tcp_port "127.0.0.1" "${HTTP_PORTS[$i]}" "node-$i HTTP" 180 0.2
  bench_wait_tcp_port "127.0.0.1" "${PPROF_PORTS[$i]}" "node-$i pprof" 180 0.2
done

LEADER_INDEX=""
for _ in $(seq 1 180); do
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    status=$(curl -sf "http://127.0.0.1:${HTTP_PORTS[$i]}/api/cluster/status" 2>/dev/null || true)
    [[ -z "$status" ]] && continue
    state=$(echo "$status" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("state",""))' 2>/dev/null || true)
    if [[ "$state" == "Leader" ]]; then
      LEADER_INDEX="$i"
      break 2
    fi
  done
  sleep 0.5
done
if [[ -z "$LEADER_INDEX" ]]; then
  echo "no cluster leader found" >&2
  tail -40 "$BENCH_DIR"/n*.log >&2
  exit 1
fi

LEADER_P9_PORT="${P9_PORTS[$LEADER_INDEX]}"
LEADER_PPROF_PORT="${PPROF_PORTS[$LEADER_INDEX]}"
echo "  leader node-$LEADER_INDEX 9P=:$LEADER_P9_PORT pprof=:$LEADER_PPROF_PORT"
sleep "${CLUSTER_WARMUP_SLEEP:-5}"

bench_colima_ssh sudo modprobe 9p 2>/dev/null || true
bench_colima_ssh sudo modprobe 9pnet 2>/dev/null || true
bench_colima_ssh sudo mkdir -p "$MNT"
bench_colima_ssh sudo mount -t 9p \
  -o "rw,access=any,trans=tcp,port=${LEADER_P9_PORT},version=9p2000.L,msize=262144,aname=/${BUCKET}" \
  "$HOST_IP" "$MNT"

curl -sf "http://127.0.0.1:$LEADER_PPROF_PORT/debug/pprof/heap" \
  -o "$PROFILE_DIR/heap_pre.pb.gz" && echo "  heap_pre.pb.gz saved" || true

(
  sleep 3
  echo "  [pprof] starting ${CPU_PROFILE_SECONDS}s CPU profile..."
  curl -sf "http://127.0.0.1:$LEADER_PPROF_PORT/debug/pprof/profile?seconds=$CPU_PROFILE_SECONDS" \
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
