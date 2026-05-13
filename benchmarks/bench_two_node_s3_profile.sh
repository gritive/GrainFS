#!/usr/bin/env bash
# Start a GrainFS cluster and run S3 PUT/GET mix benchmarks.
#
# Examples:
#   PROFILE=1 ./benchmarks/bench_two_node_s3_profile.sh
#   NODE_COUNT=3 PROFILE=1 ./benchmarks/bench_two_node_s3_profile.sh
#   NODE_COUNT=3 INGRESS_LIST=round-robin MIX_LIST=pure-put,put-heavy,mixed ./benchmarks/bench_two_node_s3_profile.sh
#   SERVER_ARGS="--shard-cache-size 0" DURATION=15s CONCURRENCY_LIST=32 ./benchmarks/bench_two_node_s3_profile.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${BINARY:-./bin/grainfs}"
K6="${K6:-k6}"
NODE_COUNT="${NODE_COUNT:-2}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-${NODE_COUNT}-node-s3-bench}"
DURATION="${DURATION:-30s}"
RAMP_UP="${RAMP_UP:-1s}"
RAMP_DOWN="${RAMP_DOWN:-1s}"
GRACEFUL_RAMP_DOWN="${GRACEFUL_RAMP_DOWN:-2s}"
GRACEFUL_STOP="${GRACEFUL_STOP:-5s}"
CONCURRENCY_LIST="${CONCURRENCY_LIST:-32}"
INGRESS_LIST="${INGRESS_LIST:-single,round-robin}"
MIX_LIST="${MIX_LIST:-put-heavy,read-heavy}"
SIZE_KB="${SIZE_KB:-4096}"
OBJECT_COUNT="${OBJECT_COUNT:-16}"
PROFILE="${PROFILE:-1}"
PROFILE_ROOT="${PROFILE_ROOT:-benchmarks/profiles/${NODE_COUNT}-node-s3-$(date +%Y%m%d-%H%M%S)}"
BUCKET="${BUCKET:-bench}"
SETUP_TIMEOUT="${SETUP_TIMEOUT:-5m}"
SCRIPT="$BENCHMARKS_DIR/s3_mixed_profile.js"

bench_require_command "$K6" "brew install k6"

if [[ "$NODE_COUNT" -lt 2 ]]; then
  echo "[error] NODE_COUNT must be >= 2 for clustered S3 profile" >&2
  exit 1
fi
if [[ "${NO_ENCRYPTION:-0}" == "1" ]]; then
  echo "[error] encryption is mandatory for this benchmark; do not set NO_ENCRYPTION=1" >&2
  exit 1
fi

if [[ "${NO_BUILD:-0}" != "1" ]]; then
  echo "[bench] building grainfs..."
  make build
fi

if [[ ! -x "$BINARY" ]]; then
  echo "[error] binary not found: $BINARY" >&2
  exit 1
fi

rm -rf "$BENCH_DIR"
mkdir -p "$PROFILE_ROOT"
for idx in $(seq 1 "$NODE_COUNT"); do
  mkdir -p "$BENCH_DIR/n${idx}"
done
BENCH_ENCRYPTION_KEY_FILE="${BENCH_ENCRYPTION_KEY_FILE:-$BENCH_DIR/encryption.key}"
export BENCH_ENCRYPTION_KEY_FILE
bench_generate_encryption_key_file "$BENCH_ENCRYPTION_KEY_FILE"

HTTP_PORTS=()
RAFT_PORTS=()
PPROF_PORTS=()
for idx in $(seq 1 "$NODE_COUNT"); do
  http_var="HTTP_PORT_${idx}"
  raft_var="RAFT_PORT_${idx}"
  pprof_var="PPROF_PORT_${idx}"
  HTTP_PORTS+=("${!http_var:-$(bench_free_port)}")
  RAFT_PORTS+=("${!raft_var:-$(bench_free_port)}")
  PPROF_PORTS+=("${!pprof_var:-$(bench_free_port)}")
done
PIDS=()

cleanup() {
  echo "[bench] stopping nodes..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  bench_copy_node_logs "$BENCH_DIR" "$PROFILE_ROOT"
  echo "[bench] stopped"
}
trap cleanup EXIT INT TERM

split_server_args=()
if [[ -n "${SERVER_ARGS:-}" ]]; then
  # shellcheck disable=SC2206 # benchmark-only convenience: split args like "--flag value".
  split_server_args=($SERVER_ARGS)
fi

start_node() {
  local idx="$1"
  local http_port="${HTTP_PORTS[$idx]}"
  local raft_port="${RAFT_PORTS[$idx]}"
  local logfile="$BENCH_DIR/n$((idx + 1)).log"
  local args=(
    "$BINARY" serve
    --data "$BENCH_DIR/n$((idx + 1))"
    --port "$http_port"
    --node-id "n$((idx + 1))"
    --raft-addr "127.0.0.1:${raft_port}"
    --cluster-key "bench-${NODE_COUNT}-node-key"
    --nfs4-port 0
    --nbd-port 0
    --lifecycle-interval 0
  )
  while IFS= read -r arg; do
    [[ -n "$arg" ]] && args+=("$arg")
  done < <(bench_encryption_args)
  args+=("${split_server_args[@]}")
  if [[ "$PROFILE" == "1" ]]; then
    args+=(--pprof-port "${PPROF_PORTS[$idx]}")
  fi

  "${args[@]}" >"$logfile" 2>&1 &
  PIDS+=($!)
  echo "[bench] node$((idx + 1)) started http=:${http_port} raft=:${raft_port} pid=${PIDS[-1]}"
}

start_node 0
bench_wait_tcp_port "127.0.0.1" "${HTTP_PORTS[0]}" "node1 S3" 180 0.2
if [[ "$PROFILE" == "1" ]]; then
  bench_wait_tcp_port "127.0.0.1" "${PPROF_PORTS[0]}" "node1 pprof" 180 0.2
fi

bench_bootstrap_iam_credentials "$BINARY" "$BENCH_DIR/n1" "bench-${NODE_COUNT}-node-s3"

for i in $(seq 1 $((NODE_COUNT - 1))); do
  printf '%s' "127.0.0.1:${RAFT_PORTS[0]}" >"$BENCH_DIR/n$((i + 1))/.join-pending"
  chmod 600 "$BENCH_DIR/n$((i + 1))/.join-pending"
  start_node "$i"
  bench_wait_tcp_port "127.0.0.1" "${HTTP_PORTS[$i]}" "node$((i + 1)) S3" 180 0.2
  if [[ "$PROFILE" == "1" ]]; then
    bench_wait_tcp_port "127.0.0.1" "${PPROF_PORTS[$i]}" "node$((i + 1)) pprof" 180 0.2
  fi
done

BASE_URLS=""
for i in $(seq 0 $((NODE_COUNT - 1))); do
  url="http://127.0.0.1:${HTTP_PORTS[$i]}"
  if [[ -z "$BASE_URLS" ]]; then
    BASE_URLS="$url"
  else
    BASE_URLS="${BASE_URLS},${url}"
  fi
done
BASE_URL_1="http://127.0.0.1:${HTTP_PORTS[0]}"
LEADER_URL=""

for _ in $(seq 1 120); do
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    url="http://127.0.0.1:${HTTP_PORTS[$i]}"
    state=$(curl -sf "$url/api/cluster/status" 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin).get("state",""))' 2>/dev/null || true)
    if [[ "$state" == "Leader" ]]; then
      LEADER_URL="$url"
      break 2
    fi
  done
  sleep 0.5
done
if [[ -z "$LEADER_URL" ]]; then
  echo "[bench] warning: meta leader not discovered; falling back to node1 for single ingress/readiness" >&2
  LEADER_URL="$BASE_URL_1"
fi
BASE_URL_1="$LEADER_URL"

sleep "${CLUSTER_WARMUP_SLEEP:-5}"

summary_md="$PROFILE_ROOT/summary.md"
{
  echo "# GrainFS ${NODE_COUNT}-node S3 profile"
  echo
  echo "- nodes: ${NODE_COUNT}"
  echo "- ec: default profile; check node logs for effective_k/effective_m"
  echo "- urls: ${BASE_URLS}"
  echo "- single ingress url: ${BASE_URL_1}"
  echo "- seed url: ${LEADER_URL}"
  echo "- object: ${SIZE_KB}KB"
  echo "- duration: ${DURATION}"
  echo "- concurrency: ${CONCURRENCY_LIST}"
  echo "- ingress: ${INGRESS_LIST}"
  echo "- mix: ${MIX_LIST}"
  [[ -n "${SERVER_ARGS:-}" ]] && echo "- server args: ${SERVER_ARGS}"
  echo
  printf "| scenario | ingress | VUs | write %% | PUT ops | PUT p99 ms | GET ops | GET p99 ms | failed rate | throughput MB/s |"
  for i in $(seq 1 "$NODE_COUNT"); do
    printf " node%d RSS KB |" "$i"
  done
  for i in $(seq 1 "$NODE_COUNT"); do
    printf " node%d reqs |" "$i"
  done
  printf "\n"
  printf "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
  for _ in $(seq 1 "$NODE_COUNT"); do
    printf " ---: |"
  done
  for _ in $(seq 1 "$NODE_COUNT"); do
    printf " ---: |"
  done
  printf "\n"
} >"$summary_md"

collect_node_metrics() {
  local out_dir="$1"
  mkdir -p "$out_dir"
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    curl -sf "http://127.0.0.1:${HTTP_PORTS[$i]}/metrics" -o "$out_dir/node$((i + 1)).prom" || true
    ps -p "${PIDS[$i]}" -o pid=,pcpu=,rss=,command= >"$out_dir/node$((i + 1)).ps" 2>/dev/null || true
  done
}

run_scenario() {
  local ingress="$1"
  local mix="$2"
  local write_percent="$3"
  local vus="$4"
  local scenario="${ingress}-${mix}-c${vus}"
  local out_dir="$PROFILE_ROOT/$scenario"
  local summary_json="$out_dir/report.json"
  local duration_sec="${DURATION//[^0-9]/}"
  local cpu_sec k6_exit=0
  mkdir -p "$out_dir"
  [[ -z "$duration_sec" ]] && duration_sec=30
  cpu_sec=$(( duration_sec > 6 ? duration_sec - 2 : duration_sec ))

  echo "[bench] scenario ${scenario}: write=${write_percent}% vus=${vus}"
  collect_node_metrics "$out_dir/pre"

  pprof_pids=()
  if [[ "$PROFILE" == "1" ]]; then
    for i in $(seq 0 $((NODE_COUNT - 1))); do
      (
        sleep 1
        curl -sf "http://127.0.0.1:${PPROF_PORTS[$i]}/debug/pprof/profile?seconds=${cpu_sec}" \
          -o "$out_dir/node$((i + 1))-cpu.pb.gz" && echo "[pprof] ${scenario} node$((i + 1)) cpu saved" || true
      ) &
      pprof_pids+=($!)
    done
  fi

  "$K6" run "$SCRIPT" \
    --env BASE_URL="$BASE_URL_1" \
    --env BASE_URLS="$BASE_URLS" \
    --env SEED_URL="$LEADER_URL" \
    --env ACCESS_KEY="$ACCESS_KEY" \
    --env SECRET_KEY="$SECRET_KEY" \
    --env INGRESS="$ingress" \
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

  for pid in "${pprof_pids[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
  collect_node_metrics "$out_dir/post"

  if [[ "$PROFILE" == "1" ]]; then
    for i in $(seq 0 $((NODE_COUNT - 1))); do
      bench_collect_pprof "${PPROF_PORTS[$i]}" "$out_dir/node$((i + 1))" heap allocs goroutine mutex block
      go tool pprof -top -nodecount=20 "$out_dir/node$((i + 1))-cpu.pb.gz" >"$out_dir/node$((i + 1))-cpu_top.txt" 2>/dev/null || true
      go tool pprof -top -nodecount=20 "$out_dir/node$((i + 1))/heap.pb.gz" >"$out_dir/node$((i + 1))-heap_top.txt" 2>/dev/null || true
    done
  fi

  python3 - "$summary_json" "$summary_md" "$scenario" "$ingress" "$vus" "$write_percent" "$out_dir" "$NODE_COUNT" <<'PY'
import json
import re
import sys
from pathlib import Path

report_path, summary_path, scenario, ingress, vus, write_percent, out_dir, node_count = sys.argv[1:]
node_count = int(node_count)

with open(report_path, "r", encoding="utf-8") as f:
    r = json.load(f)

def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0

def rss(node):
    p = Path(out_dir) / "post" / f"node{node}.ps"
    parts = p.read_text().split() if p.exists() else []
    return parts[2] if len(parts) >= 3 else "0"

def http_reqs(node):
    p = Path(out_dir) / "post" / f"node{node}.prom"
    if not p.exists():
        return 0
    total = 0
    for line in p.read_text().splitlines():
        if line.startswith("grainfs_http_requests_total"):
            total += fnum(line.rsplit(" ", 1)[-1])
    return int(total)

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

cells = [
    scenario,
    ingress,
    vus,
    write_percent,
    str(int(fnum(put.get("ops")))),
    str(put.get("p99_ms", "0.00")),
    str(int(fnum(get.get("ops")))),
    str(get.get("p99_ms", "0.00")),
    f"{float(r.get('http_req_failed') or 0):.4f}",
    f"{throughput:.1f}",
]
cells.extend(rss(i) for i in range(1, node_count + 1))
cells.extend(str(http_reqs(i)) for i in range(1, node_count + 1))
line = "| " + " | ".join(cells) + " |"
with open(summary_path, "a", encoding="utf-8") as f:
    f.write(line + "\n")
PY

  if [[ "$k6_exit" != "0" ]]; then
    echo "[bench] scenario ${scenario} exited ${k6_exit}; see ${out_dir}/k6.err" >&2
    return "$k6_exit"
  fi
}

FAIL=0
IFS=',' read -ra CONCURRENCY_VALUES <<<"$CONCURRENCY_LIST"
IFS=',' read -ra INGRESS_VALUES <<<"$INGRESS_LIST"
IFS=',' read -ra MIX_VALUES <<<"$MIX_LIST"
for vus in "${CONCURRENCY_VALUES[@]}"; do
  for ingress in "${INGRESS_VALUES[@]}"; do
    for mix in "${MIX_VALUES[@]}"; do
      case "$mix" in
        pure-put)
          run_scenario "$ingress" "$mix" 100 "$vus" || FAIL=1
          ;;
        put-heavy|write-heavy)
          run_scenario "$ingress" "$mix" 90 "$vus" || FAIL=1
          ;;
        mixed)
          run_scenario "$ingress" "$mix" 50 "$vus" || FAIL=1
          ;;
        read-heavy)
          run_scenario "$ingress" "$mix" 10 "$vus" || FAIL=1
          ;;
        *)
          echo "[error] unsupported mix: $mix" >&2
          FAIL=1
          ;;
      esac
    done
  done
done

"$BINARY" cluster placement "$BUCKET" --endpoint "$BENCH_DIR/n1/admin.sock" \
  >"$PROFILE_ROOT/placement.txt" 2>"$PROFILE_ROOT/placement.err" || true
cp "$BENCH_DIR"/n*.log "$PROFILE_ROOT"/ 2>/dev/null || true

echo ""
echo "=================================================================="
echo "  summary"
echo "=================================================================="
cat "$summary_md"
echo ""
echo "[bench] profiles saved to $PROFILE_ROOT"
exit "$FAIL"
