#!/usr/bin/env bash
# Start one GrainFS server and run S3 PUT/GET mix benchmarks with pprof.
#
# Examples:
#   PROFILE=1 ./benchmarks/bench_single_s3_profile.sh
#   DURATION=15s CONCURRENCY_LIST=1,4 SIZE_KB=4096 ./benchmarks/bench_single_s3_profile.sh
#   SERVER_ARGS="--shard-cache-size 0" DURATION=15s CONCURRENCY_LIST=32 ./benchmarks/bench_single_s3_profile.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${BINARY:-./bin/grainfs}"
K6="${K6:-k6}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-single-s3-bench}"
HTTP_PORT="${HTTP_PORT:-$(bench_free_port)}"
PPROF_PORT="${PPROF_PORT:-$(bench_free_port)}"
DURATION="${DURATION:-30s}"
RAMP_UP="${RAMP_UP:-1s}"
RAMP_DOWN="${RAMP_DOWN:-1s}"
GRACEFUL_RAMP_DOWN="${GRACEFUL_RAMP_DOWN:-2s}"
GRACEFUL_STOP="${GRACEFUL_STOP:-5s}"
CONCURRENCY_LIST="${CONCURRENCY_LIST:-1,4,16,32}"
SIZE_KB="${SIZE_KB:-4096}"
OBJECT_COUNT="${OBJECT_COUNT:-16}"
PROFILE="${PROFILE:-1}"
PROFILE_ROOT="${PROFILE_ROOT:-benchmarks/profiles/single-s3-$(date +%Y%m%d-%H%M%S)}"
BUCKET="${BUCKET:-bench}"
SETUP_TIMEOUT="${SETUP_TIMEOUT:-5m}"
SCRIPT="$BENCHMARKS_DIR/s3_mixed_profile.js"

bench_require_command "$K6" "brew install k6"

if [[ "${NO_BUILD:-0}" != "1" ]]; then
  echo "[bench] building grainfs..."
  make build
fi

if [[ ! -x "$BINARY" ]]; then
  echo "[error] binary not found: $BINARY" >&2
  exit 1
fi

rm -rf "$BENCH_DIR"
mkdir -p "$BENCH_DIR" "$PROFILE_ROOT"

SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    echo "[bench] stopping server..."
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  echo "[bench] stopped"
}
trap cleanup EXIT INT TERM

args=(
  "$BINARY" serve
  --data "$BENCH_DIR/data"
  --port "$HTTP_PORT"
  --nfs4-port 0
  --nbd-port 0
)

while IFS= read -r arg; do
  [[ -n "$arg" ]] && args+=("$arg")
done < <(bench_encryption_args)

if [[ -n "${SERVER_ARGS:-}" ]]; then
  # shellcheck disable=SC2206 # benchmark-only convenience: split args like "--flag value".
  extra_args=($SERVER_ARGS)
  args+=("${extra_args[@]}")
fi

if [[ "$PROFILE" == "1" ]]; then
  args+=(--pprof-port "$PPROF_PORT")
fi

"${args[@]}" >"$BENCH_DIR/server.log" 2>&1 &
SERVER_PID=$!
echo "[bench] server started http=:${HTTP_PORT} pid=${SERVER_PID}"

bench_wait_tcp_port "127.0.0.1" "$HTTP_PORT" "single S3" 150 0.2
if [[ "$PROFILE" == "1" ]]; then
  bench_wait_tcp_port "127.0.0.1" "$PPROF_PORT" "pprof" 150 0.2
fi

bench_create_bucket_retry "http://127.0.0.1:${HTTP_PORT}" "$BUCKET" 120 0.5
bench_put_object_retry "http://127.0.0.1:${HTTP_PORT}" "$BUCKET" ".bench-ready" 120 0.5

echo ""
echo "=================================================================="
echo "  GrainFS single-node S3 profile benchmark"
echo "  target      : http://127.0.0.1:${HTTP_PORT}"
echo "  object      : ${SIZE_KB}KB x ${OBJECT_COUNT} seed objects"
echo "  duration    : ${DURATION}"
  echo "  concurrency : ${CONCURRENCY_LIST}"
  [[ -n "${SERVER_ARGS:-}" ]] && echo "  server args : ${SERVER_ARGS}"
  echo "  output      : ${PROFILE_ROOT}"
[[ "$PROFILE" == "1" ]] && echo "  pprof       : http://127.0.0.1:${PPROF_PORT}/debug/pprof/"
echo "=================================================================="
echo ""

summary_md="$PROFILE_ROOT/summary.md"
{
  echo "# GrainFS single-node S3 profile"
  echo
  echo "- target: http://127.0.0.1:${HTTP_PORT}"
  echo "- object: ${SIZE_KB}KB"
  echo "- duration: ${DURATION}"
  echo "- concurrency: ${CONCURRENCY_LIST}"
  echo
  echo "| scenario | VUs | write % | PUT ops | PUT p99 ms | GET ops | GET p99 ms | failed rate | throughput MB/s |"
  echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
} >"$summary_md"

collect_metrics() {
  local out_dir="$1"
  mkdir -p "$out_dir"
  curl -sf "http://127.0.0.1:${HTTP_PORT}/metrics" -o "$out_dir/metrics.prom" || true
  ps -p "$SERVER_PID" -o pid=,pcpu=,rss=,command= >"$out_dir/ps.txt" 2>/dev/null || true
}

run_scenario() {
  local mix="$1"
  local write_percent="$2"
  local vus="$3"
  local scenario="${mix}-c${vus}"
  local out_dir="$PROFILE_ROOT/$scenario"
  local cpu_sec duration_sec summary_json k6_exit

  mkdir -p "$out_dir"
  summary_json="$out_dir/report.json"
  duration_sec="${DURATION//[^0-9]/}"
  [[ -z "$duration_sec" ]] && duration_sec=30
  cpu_sec=$(( duration_sec > 6 ? duration_sec - 2 : duration_sec ))

  echo "[bench] scenario ${scenario}: write=${write_percent}% vus=${vus}"
  collect_metrics "$out_dir/pre"

  PPROF_BG_PID=""
  if [[ "$PROFILE" == "1" ]]; then
    mkdir -p "$out_dir"
    curl -sf "http://127.0.0.1:${PPROF_PORT}/debug/pprof/heap" -o "$out_dir/heap_pre.pb.gz" || true
    (
      sleep 1
      curl -sf "http://127.0.0.1:${PPROF_PORT}/debug/pprof/profile?seconds=${cpu_sec}" \
        -o "$out_dir/cpu.pb.gz" && echo "[pprof] ${scenario} cpu.pb.gz saved" || echo "[pprof] ${scenario} CPU profile failed"
    ) &
    PPROF_BG_PID=$!
  fi

  k6_exit=0
  "$K6" run "$SCRIPT" \
    --env BASE_URL="http://127.0.0.1:${HTTP_PORT}" \
    --env BUCKET="$BUCKET" \
    --env OBJECT_SIZE_KB="$SIZE_KB" \
    --env OBJECT_COUNT="$OBJECT_COUNT" \
    --env WRITE_PERCENT="$write_percent" \
    --env MIX="$mix" \
    --env SETUP_TIMEOUT="$SETUP_TIMEOUT" \
    --env DURATION="$DURATION" \
    --env RAMP_UP="$RAMP_UP" \
    --env RAMP_DOWN="$RAMP_DOWN" \
    --env GRACEFUL_RAMP_DOWN="$GRACEFUL_RAMP_DOWN" \
    --env GRACEFUL_STOP="$GRACEFUL_STOP" \
    --env MAX_VUS="$vus" \
    --env SUMMARY_JSON="$summary_json" \
    >"$out_dir/k6.out" 2>"$out_dir/k6.err" || k6_exit=$?

  [[ -n "$PPROF_BG_PID" ]] && wait "$PPROF_BG_PID" 2>/dev/null || true
  collect_metrics "$out_dir/post"

  if [[ "$PROFILE" == "1" ]]; then
    bench_collect_pprof "$PPROF_PORT" "$out_dir" heap allocs goroutine mutex block
    go tool pprof -top -nodecount=20 "$out_dir/cpu.pb.gz" >"$out_dir/cpu_top.txt" 2>/dev/null || true
    go tool pprof -top -nodecount=20 "$out_dir/heap.pb.gz" >"$out_dir/heap_top.txt" 2>/dev/null || true
  fi

  if [[ -f "$summary_json" ]]; then
    python3 - "$summary_json" "$summary_md" "$scenario" "$vus" "$write_percent" <<'PY'
import json
import sys

report_path, summary_path, scenario, vus, write_percent = sys.argv[1:]
with open(report_path, "r", encoding="utf-8") as f:
    r = json.load(f)

def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0

put = r.get("put", {})
get = r.get("get", {})
put_bytes = fnum(put.get("bytes"))
get_bytes = fnum(get.get("bytes"))
duration = 0.0
for op in (put, get):
    rate = fnum(op.get("bytes_per_sec"))
    bytes_ = fnum(op.get("bytes"))
    if rate > 0 and bytes_ > 0:
        duration = max(duration, bytes_ / rate)
throughput = ((put_bytes + get_bytes) / duration / 1024 / 1024) if duration > 0 else 0.0

line = (
    f"| {scenario} | {vus} | {write_percent} | "
    f"{int(fnum(put.get('ops')))} | {put.get('p99_ms', '0.00')} | "
    f"{int(fnum(get.get('ops')))} | {get.get('p99_ms', '0.00')} | "
    f"{float(r.get('http_req_failed') or 0):.4f} | {throughput:.1f} |"
)
with open(summary_path, "a", encoding="utf-8") as f:
    f.write(line + "\n")
PY
  else
    echo "| ${scenario} | ${vus} | ${write_percent} | 0 | 0.00 | 0 | 0.00 | 1.0000 | 0.0 |" >>"$summary_md"
  fi

  if [[ "$k6_exit" != "0" ]]; then
    echo "[bench] scenario ${scenario} exited ${k6_exit}; see ${out_dir}/k6.err" >&2
    return "$k6_exit"
  fi
}

FAIL=0
IFS=',' read -ra CONCURRENCY_VALUES <<<"$CONCURRENCY_LIST"
for vus in "${CONCURRENCY_VALUES[@]}"; do
  run_scenario "write-heavy" 90 "$vus" || FAIL=1
  run_scenario "read-heavy" 10 "$vus" || FAIL=1
done

cp "$BENCH_DIR/server.log" "$PROFILE_ROOT/server.log" 2>/dev/null || true

echo ""
echo "=================================================================="
echo "  summary"
echo "=================================================================="
cat "$summary_md"
echo ""
echo "[bench] profiles saved to $PROFILE_ROOT"
exit "$FAIL"
