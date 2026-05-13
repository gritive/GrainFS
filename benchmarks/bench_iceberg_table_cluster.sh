#!/usr/bin/env bash
# Iceberg REST Catalog table API cluster benchmark.
# Uses the same k6 workload as bench_iceberg_table.sh, changing only topology.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${BINARY:-./bin/grainfs}"
K6="${K6:-k6}"
NODE_COUNT="${NODE_COUNT:-3}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-iceberg-table-cluster-bench}"
PROFILE="${PROFILE:-0}"
VUS="${VUS:-${MAX_VUS:-10}}"
DURATION="${DURATION:-30s}"
RAMP_UP="${RAMP_UP:-10s}"
RAMP_DOWN="${RAMP_DOWN:-5s}"

bench_require_command "$K6" "brew install k6"

if [[ "${NO_BUILD:-0}" != "1" ]]; then
  echo "[bench] building grainfs..."
  make build
fi
bench_require_binary "$BINARY"

if [[ "$NODE_COUNT" -lt 2 ]]; then
  echo "[error] NODE_COUNT must be >= 2 for clustered Iceberg profile" >&2
  exit 1
fi

HTTP_PORTS=()
RAFT_PORTS=()
PPROF_PORTS=()
for idx in $(seq 1 "$NODE_COUNT"); do
  HTTP_PORTS+=("$(bench_free_port)")
  RAFT_PORTS+=("$(bench_free_port)")
  PPROF_PORTS+=("$(bench_free_port)")
done

PIDS=()
rm -rf "$BENCH_DIR"
for idx in $(seq 0 $((NODE_COUNT - 1))); do
  mkdir -p "$BENCH_DIR/n$idx"
done

cleanup() {
  local status=$?
  echo "[bench] stopping Iceberg table API cluster..."
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [[ "$status" == "0" && "${KEEP_BENCH_DIR:-0}" != "1" ]]; then
    rm -rf "$BENCH_DIR"
  else
    echo "[bench] logs preserved in $BENCH_DIR"
  fi
}
trap cleanup EXIT INT TERM

raft_addr() {
  echo "127.0.0.1:${RAFT_PORTS[$1]}"
}

http_port() {
  echo "${HTTP_PORTS[$1]}"
}

pprof_port() {
  echo "${PPROF_PORTS[$1]}"
}

start_node() {
  local i="$1"
  extra=()
  if [[ "$PROFILE" == "1" ]]; then
    extra+=(--pprof-port "$(pprof_port "$i")")
  fi

  "$BINARY" serve \
    --data "$BENCH_DIR/n$i" \
    --port "$(http_port "$i")" \
    --node-id "bench-iceberg-node-$i" \
    --raft-addr "$(raft_addr "$i")" \
    --cluster-key "bench-iceberg-cluster-key" \
    $(bench_encryption_args) \
    --nfs4-port 0 \
    --nbd-port 0 \
    --scrub-interval 0 \
    --lifecycle-interval 0 \
    --log-level warn \
    "${extra[@]}" \
    > "$BENCH_DIR/n$i.log" 2>&1 &
  PIDS+=($!)
  echo "[bench] node-$i HTTP=:$(http_port "$i") Raft=$(raft_addr "$i") pid=${PIDS[-1]}"
}

start_node 0
bench_wait_tcp_port "127.0.0.1" "$(http_port 0)" "node-0 HTTP" 180 0.2
if [[ "$PROFILE" == "1" ]]; then
  bench_wait_tcp_port "127.0.0.1" "$(pprof_port 0)" "node-0 pprof" 180 0.2
fi
bench_bootstrap_iam_credentials "$BINARY" "$BENCH_DIR/n0" "bench-iceberg-cluster"

for i in $(seq 1 $((NODE_COUNT - 1))); do
  printf '%s' "$(raft_addr 0)" >"$BENCH_DIR/n$i/.join-pending"
  chmod 600 "$BENCH_DIR/n$i/.join-pending"
  start_node "$i"
  bench_wait_tcp_port "127.0.0.1" "$(http_port "$i")" "node-$i HTTP" 180 0.2
  if [[ "$PROFILE" == "1" ]]; then
    bench_wait_tcp_port "127.0.0.1" "$(pprof_port "$i")" "node-$i pprof" 180 0.2
  fi
done

echo "[bench] waiting for leader..."
LEADER_PORT=""
LEADER_INDEX=""
for attempt in $(seq 1 180); do
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    port="$(http_port "$i")"
    status=$(curl -sf "http://127.0.0.1:$port/api/cluster/status" 2>/dev/null || true)
    [[ -z "$status" ]] && continue
    state=$(echo "$status" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("state",""))' 2>/dev/null || true)
    if [[ "$state" == "Leader" ]]; then
      LEADER_PORT="$port"
      LEADER_INDEX="$i"
      break 2
    fi
  done
  sleep 0.5
done

if [[ -z "$LEADER_PORT" ]]; then
  echo "[error] no leader found" >&2
  tail -40 "$BENCH_DIR"/n*.log >&2
  exit 1
fi
echo "[bench] leader node-$LEADER_INDEX on :$LEADER_PORT"
TARGET_INDEX="$LEADER_INDEX"
TARGET_PORT="$LEADER_PORT"
echo "[bench] writable target node-$TARGET_INDEX on :$TARGET_PORT"
sleep "${CLUSTER_WARMUP_SLEEP:-5}"

PROFILE_DIR=""
PPROF_BG_PID=""
if [[ "$PROFILE" == "1" ]]; then
  PROFILE_DIR="benchmarks/profiles/iceberg-table-${NODE_COUNT}-node-cluster-$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$PROFILE_DIR"
  target_pprof="$(pprof_port "$TARGET_INDEX")"
  curl -sf "http://127.0.0.1:$target_pprof/debug/pprof/heap" \
    -o "$PROFILE_DIR/heap_pre.pb.gz" && echo "[pprof] heap_pre.pb.gz saved" || true
  (
    sleep 5
    echo "[pprof] collecting CPU profile..."
    curl -sf "http://127.0.0.1:$target_pprof/debug/pprof/profile?seconds=${CPU_PROFILE_SECONDS:-30}" \
      -o "$PROFILE_DIR/cpu.pb.gz" && echo "[pprof] cpu.pb.gz saved" || echo "[pprof] CPU profile failed"
  ) &
  PPROF_BG_PID=$!
fi

"$K6" run "$BENCHMARKS_DIR/iceberg_table_bench.js" \
  --env BASE_URL="http://127.0.0.1:$TARGET_PORT" \
  --env ACCESS_KEY="$ACCESS_KEY" \
  --env SECRET_KEY="$SECRET_KEY" \
  --env MAX_VUS="$VUS" \
  --env DURATION="$DURATION" \
  --env RAMP_UP="$RAMP_UP" \
  --env RAMP_DOWN="$RAMP_DOWN" \
  "$@" || K6_EXIT=$?

[[ -n "$PPROF_BG_PID" ]] && wait "$PPROF_BG_PID" 2>/dev/null || true

if [[ "$PROFILE" == "1" ]]; then
  target_pprof="$(pprof_port "$TARGET_INDEX")"
  bench_collect_pprof "$target_pprof" "$PROFILE_DIR" heap allocs goroutine mutex block
  echo "[pprof] CPU top-10"
  go tool pprof -top -nodecount=10 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  (pprof analysis failed)"
  echo "[pprof] profiles saved to $PROFILE_DIR/"
fi

echo "[bench] done. report: benchmarks/iceberg_table_report.json"
exit "${K6_EXIT:-0}"
