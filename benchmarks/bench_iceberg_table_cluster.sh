#!/usr/bin/env bash
# 3-node Iceberg REST Catalog table API benchmark.
# Uses the same k6 workload as bench_iceberg_table.sh, changing only topology.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${BINARY:-./bin/grainfs}"
K6="${K6:-k6}"
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

HTTP0=$(bench_free_port); HTTP1=$(bench_free_port); HTTP2=$(bench_free_port)
RAFT0=$(bench_free_port); RAFT1=$(bench_free_port); RAFT2=$(bench_free_port)
PPROF0=$(bench_free_port); PPROF1=$(bench_free_port); PPROF2=$(bench_free_port)

PIDS=()
rm -rf "$BENCH_DIR"
mkdir -p "$BENCH_DIR"/{n0,n1,n2}

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

peers_for() {
  case "$1" in
    0) echo "127.0.0.1:$RAFT1,127.0.0.1:$RAFT2" ;;
    1) echo "127.0.0.1:$RAFT0,127.0.0.1:$RAFT2" ;;
    2) echo "127.0.0.1:$RAFT0,127.0.0.1:$RAFT1" ;;
  esac
}

raft_addr() {
  case "$1" in
    0) echo "127.0.0.1:$RAFT0" ;;
    1) echo "127.0.0.1:$RAFT1" ;;
    2) echo "127.0.0.1:$RAFT2" ;;
  esac
}

http_port() {
  case "$1" in
    0) echo "$HTTP0" ;;
    1) echo "$HTTP1" ;;
    2) echo "$HTTP2" ;;
  esac
}

pprof_port() {
  case "$1" in
    0) echo "$PPROF0" ;;
    1) echo "$PPROF1" ;;
    2) echo "$PPROF2" ;;
  esac
}

for i in 0 1 2; do
  extra=()
  if [[ "$PROFILE" == "1" ]]; then
    extra+=(--pprof-port "$(pprof_port "$i")")
  fi

  "$BINARY" serve \
    --data "$BENCH_DIR/n$i" \
    --port "$(http_port "$i")" \
    --node-id "bench-iceberg-node-$i" \
    --raft-addr "$(raft_addr "$i")" \
    --peers "$(peers_for "$i")" \
    --cluster-key "bench-iceberg-cluster-key" \
    --ec-data 2 \
    --ec-parity 1 \
    $(bench_encryption_args) \
    --nfs4-port 0 \
    --nbd-port 0 \
    --snapshot-interval 0 \
    --scrub-interval 0 \
    --lifecycle-interval 0 \
    --rate-limit-ip-rps 0 \
    --rate-limit-user-rps 0 \
    --log-level warn \
    "${extra[@]}" \
    > "$BENCH_DIR/n$i.log" 2>&1 &
  PIDS+=($!)
  echo "[bench] node-$i HTTP=:$(http_port "$i") Raft=$(raft_addr "$i") pid=${PIDS[-1]}"
done

echo "[bench] waiting for leader..."
LEADER_PORT=""
LEADER_INDEX=""
for attempt in $(seq 1 180); do
  for i in 0 1 2; do
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

TARGET_INDEX=""
TARGET_PORT=""
for _ in $(seq 1 60); do
  for i in 0 1 2; do
    port="$(http_port "$i")"
    if BENCH_QUIET=1 bench_create_bucket_retry "http://127.0.0.1:$port" "grainfs-tables" 1 0.1 &&
      BENCH_QUIET=1 bench_put_object_retry "http://127.0.0.1:$port" "grainfs-tables" ".bench-ready" 1 0.1; then
      TARGET_INDEX="$i"
      TARGET_PORT="$port"
      break 2
    fi
  done
  sleep 0.5
done

if [[ -z "$TARGET_PORT" ]]; then
  echo "[error] no writable Iceberg table API endpoint found" >&2
  tail -40 "$BENCH_DIR"/n*.log >&2
  exit 1
fi
echo "[bench] writable target node-$TARGET_INDEX on :$TARGET_PORT"
sleep "${CLUSTER_WARMUP_SLEEP:-5}"

PROFILE_DIR=""
PPROF_BG_PID=""
if [[ "$PROFILE" == "1" ]]; then
  PROFILE_DIR="benchmarks/profiles/iceberg-table-cluster-$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$PROFILE_DIR"
  target_pprof="$(pprof_port "$TARGET_INDEX")"
  curl -sf "http://127.0.0.1:$target_pprof/debug/pprof/heap" \
    -o "$PROFILE_DIR/heap_pre.pb.gz" && echo "[pprof] heap_pre.pb.gz saved" || true
  (
    sleep 5
    echo "[pprof] collecting CPU profile..."
    curl -sf "http://127.0.0.1:$target_pprof/debug/pprof/profile?seconds=30" \
      -o "$PROFILE_DIR/cpu.pb.gz" && echo "[pprof] cpu.pb.gz saved" || echo "[pprof] CPU profile failed"
  ) &
  PPROF_BG_PID=$!
fi

"$K6" run "$BENCHMARKS_DIR/iceberg_table_bench.js" \
  --env BASE_URL="http://127.0.0.1:$TARGET_PORT" \
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
