#!/usr/bin/env bash
# NBD GrainFS cluster benchmark with the same fio workload as bench_nbd_profile.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${1:-./bin/grainfs}"
bench_require_binary "$BINARY"
bench_require_colima

HOST_IP="${HOST_IP:-192.168.5.2}"
NODE_COUNT="${NODE_COUNT:-3}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-nbd-cluster-bench}"
NBD_DEV="${NBD_DEV:-/dev/nbd0}"
VOL_SIZE="${VOL_SIZE:-128Mi}"
FIO_SIZE="${FIO_SIZE:-64m}"
FIO_RUNTIME="${FIO_RUNTIME:-15}"
FIO_CASES="${FIO_CASES:-seq-read-4K seq-write-4K seq-read-64K seq-write-64K rand-read-4K rand-write-4K}"
CPU_PROFILE_SECONDS="${CPU_PROFILE_SECONDS:-60}"

if [[ "$NODE_COUNT" -lt 2 ]]; then
  echo "[error] NODE_COUNT must be >= 2 for clustered NBD profile" >&2
  exit 1
fi

HTTP_PORTS=()
RAFT_PORTS=()
NBD_PORTS=()
PPROF_PORTS=()
for idx in $(seq 1 "$NODE_COUNT"); do
  HTTP_PORTS+=("$(bench_free_port)")
  RAFT_PORTS+=("$(bench_free_port)")
  NBD_PORTS+=("$(bench_free_port)")
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

cleanup() {
  echo ""
  echo "=== cleanup ==="
  bench_colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true
  if [[ -n "${CPU_PROFILE_PID:-}" ]]; then
    wait "$CPU_PROFILE_PID" 2>/dev/null || true
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

http_port() {
  echo "${HTTP_PORTS[$1]}"
}

nbd_port() {
  echo "${NBD_PORTS[$1]}"
}

pprof_port() {
  echo "${PPROF_PORTS[$1]}"
}

start_node() {
  local i="$1"
  "$BINARY" serve \
    --data "$BENCH_DIR/n$i" \
    --port "$(http_port "$i")" \
    --node-id "bench-nbd-node-$i" \
    --raft-addr "$(raft_addr "$i")" \
    --cluster-key "bench-nbd-cluster-key" \
    $(bench_encryption_args) \
    --nfs4-port 0 \
    --nbd-port "$(nbd_port "$i")" \
    --pprof-port "$(pprof_port "$i")" \
    --scrub-interval 0 \
    --lifecycle-interval 0 \
    --log-level warn \
    > "$BENCH_DIR/n$i.log" 2>&1 &
  PIDS+=($!)
  echo "  node-$i: HTTP=:$(http_port "$i") NBD=:$(nbd_port "$i") Raft=$(raft_addr "$i")"
}

start_node 0
bench_wait_tcp_port "127.0.0.1" "$(http_port 0)" "node-0 HTTP" 180 0.2
bench_wait_tcp_port "127.0.0.1" "$(pprof_port 0)" "node-0 pprof" 180 0.2
bench_wait_admin_socket "$BENCH_DIR/n0" 100 0.2
"$BINARY" volume create default --size "$VOL_SIZE" --endpoint "$BENCH_DIR/n0/admin.sock" >/dev/null

for i in $(seq 1 $((NODE_COUNT - 1))); do
  printf '%s' "$(raft_addr 0)" >"$BENCH_DIR/n$i/.join-pending"
  chmod 600 "$BENCH_DIR/n$i/.join-pending"
  start_node "$i"
  bench_wait_tcp_port "127.0.0.1" "$(http_port "$i")" "node-$i HTTP" 180 0.2
  bench_wait_tcp_port "127.0.0.1" "$(pprof_port "$i")" "node-$i pprof" 180 0.2
done

echo "=== waiting for cluster leader ==="
LEADER_INDEX=""
for _ in $(seq 1 180); do
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    status=$(curl -sf "http://127.0.0.1:$(http_port "$i")/api/cluster/status" 2>/dev/null || true)
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

LEADER_NBD_PORT="$(nbd_port "$LEADER_INDEX")"
LEADER_PPROF_PORT="$(pprof_port "$LEADER_INDEX")"
echo "  leader node-$LEADER_INDEX NBD=:$LEADER_NBD_PORT pprof=:$LEADER_PPROF_PORT"
sleep "${CLUSTER_WARMUP_SLEEP:-5}"
bench_wait_tcp_port "127.0.0.1" "$LEADER_NBD_PORT" "leader NBD listener" 50 0.2

bench_colima_ssh sudo modprobe nbd max_part=0 2>/dev/null || true
bench_colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true
bench_colima_ssh sudo nbd-client "$HOST_IP" "$LEADER_NBD_PORT" "$NBD_DEV" -b 4096 -N default
echo "OK: nbd-client connected ($NBD_DEV)"

PROFILE_DIR="benchmarks/profiles/nbd-${NODE_COUNT}-node-cluster-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$PROFILE_DIR"
curl -sf "http://127.0.0.1:$LEADER_PPROF_PORT/debug/pprof/profile?seconds=$CPU_PROFILE_SECONDS" \
  -o "$PROFILE_DIR/cpu.pb.gz" &
CPU_PROFILE_PID=$!

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

wait "$CPU_PROFILE_PID" 2>/dev/null || true
bench_collect_pprof "$LEADER_PPROF_PORT" "$PROFILE_DIR" mutex allocs heap goroutine

echo ""
echo "=== CPU top-10 (go tool pprof) ==="
go tool pprof -top -nodecount=10 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  pprof analysis failed"

echo ""
echo "PROFILE_DIR=$PROFILE_DIR"
