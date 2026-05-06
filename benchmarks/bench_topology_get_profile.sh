#!/usr/bin/env bash
# Start a real GrainFS topology and run a GET-only S3 benchmark with optional pprof.
#
# Examples:
#   NODE_COUNT=1 PROFILE=1 SIZE_KB=65536 VUS=1 DURATION=30s ./benchmarks/bench_topology_get_profile.sh
#   NODE_COUNT=2 PROFILE=1 SIZE_KB=65536 VUS=1 DURATION=30s ./benchmarks/bench_topology_get_profile.sh
#   NODE_COUNT=3 PROFILE=1 SIZE_KB=65536 VUS=1 DURATION=30s ./benchmarks/bench_topology_get_profile.sh
#   NODE_COUNT=6 PROFILE=1 SIZE_KB=65536 VUS=1 DURATION=30s ./benchmarks/bench_topology_get_profile.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${BINARY:-./bin/grainfs}"
K6="${K6:-k6}"
NODE_COUNT="${NODE_COUNT:-3}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-topology-get-bench}"
BASE_PORT="${BASE_PORT:-9200}"
BASE_RAFT_PORT="${BASE_RAFT_PORT:-19200}"
PPROF_PORT="${PPROF_PORT:-6160}"
DURATION="${DURATION:-30s}"
RAMP_UP="${RAMP_UP:-1s}"
RAMP_DOWN="${RAMP_DOWN:-1s}"
GRACEFUL_RAMP_DOWN="${GRACEFUL_RAMP_DOWN:-2s}"
GRACEFUL_STOP="${GRACEFUL_STOP:-5s}"
VUS="${VUS:-${MAX_VUS:-1}}"
SIZE_KB="${SIZE_KB:-65536}"
OBJECT_COUNT="${OBJECT_COUNT:-4}"
SETUP_TIMEOUT="${SETUP_TIMEOUT:-5m}"
PRELOAD_IN_SHELL="${PRELOAD_IN_SHELL:-1}"
PROFILE="${PROFILE:-1}"
PROFILE_ALL_NODES="${PROFILE_ALL_NODES:-0}"
EC_DATA="${EC_DATA:-4}"
EC_PARITY="${EC_PARITY:-2}"
BUCKET="${BUCKET:-bench}"
SCRIPT="$BENCHMARKS_DIR/s3_get_bench.js"

bench_require_command "$K6" "brew install k6"

if [[ "$NODE_COUNT" -lt 1 ]]; then
  echo "[error] NODE_COUNT must be >= 1" >&2
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
mkdir -p "$BENCH_DIR"
for idx in $(seq 1 "$NODE_COUNT"); do
  mkdir -p "$BENCH_DIR/n${idx}"
done

PROFILE_DIR=""
if [[ "$PROFILE" == "1" ]]; then
  PROFILE_DIR="benchmarks/profiles/topology-get-n${NODE_COUNT}-$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$PROFILE_DIR"
  if [[ "$PROFILE_ALL_NODES" == "1" ]]; then
    for idx in $(seq 1 "$NODE_COUNT"); do
      mkdir -p "$PROFILE_DIR/node${idx}"
    done
  fi
  echo "[bench] pprof profile dir: $PROFILE_DIR"
fi

PIDS=()

port_for_idx() {
  echo $((BASE_PORT + $1 - 1))
}

raft_port_for_idx() {
  echo $((BASE_RAFT_PORT + $1 - 1))
}

pprof_port_for_idx() {
  echo $((PPROF_PORT + $1 - 1))
}

peers_for_idx() {
  local self_idx="$1"
  local peers=()
  for idx in $(seq 1 "$NODE_COUNT"); do
    if [[ "$idx" == "$self_idx" ]]; then
      continue
    fi
    peers+=("127.0.0.1:$(raft_port_for_idx "$idx")")
  done
  local IFS=,
  echo "${peers[*]}"
}

start_node() {
  local idx="$1"
  local s3_port raft_port pprof_port logfile
  s3_port="$(port_for_idx "$idx")"
  raft_port="$(raft_port_for_idx "$idx")"
  pprof_port="$(pprof_port_for_idx "$idx")"
  logfile="$BENCH_DIR/n${idx}.log"

  local args=(
    "$BINARY" serve
    --data "$BENCH_DIR/n${idx}"
    --port "$s3_port"
    --nfs4-port 0
    --nbd-port 0
    --rate-limit-ip-rps 0
    --rate-limit-user-rps 0
  )

  while IFS= read -r arg; do
    [[ -n "$arg" ]] && args+=("$arg")
  done < <(bench_encryption_args)

  if [[ "$NODE_COUNT" -ge 2 ]]; then
    args+=(
      --raft-addr "127.0.0.1:${raft_port}"
      --peers "$(peers_for_idx "$idx")"
      --cluster-key "bench-topology-key"
      --ec-data "$EC_DATA"
      --ec-parity "$EC_PARITY"
    )
  fi

  if [[ "$PROFILE" == "1" ]]; then
    args+=(--pprof-port "$pprof_port")
  fi

  "${args[@]}" >"$logfile" 2>&1 &
  PIDS+=($!)
  echo "[bench] node${idx} started s3=:${s3_port} raft=:${raft_port} pid=${PIDS[-1]}"
}

cleanup() {
  echo "[bench] stopping nodes..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  echo "[bench] stopped"
}
trap cleanup EXIT INT TERM

for idx in $(seq 1 "$NODE_COUNT"); do
  start_node "$idx"
done

echo "[bench] waiting for HTTP ports..."
for idx in $(seq 1 "$NODE_COUNT"); do
  bench_wait_tcp_port "127.0.0.1" "$(port_for_idx "$idx")" "node${idx} S3" 100 0.2
done

LEADER_PORT=""
if [[ "$NODE_COUNT" -ge 2 ]]; then
  echo "[bench] waiting for cluster leader..."
  leader_deadline=$(( $(date +%s) + 60 ))
  while [[ -z "$LEADER_PORT" ]]; do
    if (( $(date +%s) > leader_deadline )); then
      echo "[error] no cluster leader elected within 60s" >&2
      tail -50 "$BENCH_DIR"/n*.log >&2
      exit 1
    fi
    for idx in $(seq 1 "$NODE_COUNT"); do
      port="$(port_for_idx "$idx")"
      status=$(curl -sf "http://127.0.0.1:${port}/api/cluster/status" 2>/dev/null || true)
      [[ -z "$status" ]] && continue
      node_id=$(echo "$status" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("node_id",""))' 2>/dev/null || true)
      leader_id=$(echo "$status" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("leader_id",""))' 2>/dev/null || true)
      if [[ -n "$leader_id" && "$node_id" == "$leader_id" ]]; then
        LEADER_PORT="$port"
        break
      fi
    done
    [[ -z "$LEADER_PORT" ]] && sleep 0.5
  done
  echo "[bench] leader on :${LEADER_PORT}"
else
  LEADER_PORT="$(port_for_idx 1)"
fi

TARGET_PORT=""
TARGET_IDX=""
if [[ "$NODE_COUNT" -ge 2 ]]; then
  TARGET_PORT="$LEADER_PORT"
  TARGET_IDX=$((TARGET_PORT - BASE_PORT + 1))
  if ! bench_create_bucket_retry "http://127.0.0.1:${TARGET_PORT}" "$BUCKET" 120 0.5 ||
    ! bench_put_object_retry "http://127.0.0.1:${TARGET_PORT}" "$BUCKET" ".bench-ready" 120 0.5; then
    TARGET_PORT=""
    TARGET_IDX=""
  fi
else
  TARGET_PORT="$(port_for_idx 1)"
  TARGET_IDX="1"
  if ! bench_create_bucket_retry "http://127.0.0.1:${TARGET_PORT}" "$BUCKET" 120 0.5 ||
    ! bench_put_object_retry "http://127.0.0.1:${TARGET_PORT}" "$BUCKET" ".bench-ready" 120 0.5; then
    TARGET_PORT=""
    TARGET_IDX=""
  fi
fi

if [[ -z "$TARGET_PORT" ]]; then
  echo "[error] no writable S3 endpoint found" >&2
  tail -50 "$BENCH_DIR"/n*.log >&2
  exit 1
fi

TARGET_PPROF_PORT="$(pprof_port_for_idx "$TARGET_IDX")"
sleep "${CLUSTER_WARMUP_SLEEP:-5}"

PRELOADED="0"
if [[ "$PRELOAD_IN_SHELL" == "1" ]]; then
  payload="$BENCH_DIR/payload-${SIZE_KB}kb.bin"
  echo "[bench] preloading ${OBJECT_COUNT} object(s) of ${SIZE_KB}KB via curl..."
  python3 - "$payload" "$SIZE_KB" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
size = int(sys.argv[2]) * 1024
chunk = b"0123456789abcdef" * 4096
with path.open("wb") as f:
    remaining = size
    while remaining > 0:
        n = min(len(chunk), remaining)
        f.write(chunk[:n])
        remaining -= n
PY
  for i in $(seq 0 $((OBJECT_COUNT - 1))); do
    key="get-only-${SIZE_KB}kb-${i}"
    curl -sf -X PUT \
      -H 'Content-Type: application/octet-stream' \
      --data-binary "@${payload}" \
      "http://127.0.0.1:${TARGET_PORT}/${BUCKET}/${key}" >/dev/null
  done
  PRELOADED="1"
fi

STATUS_JSON="$BENCH_DIR/topology.json"
if status=$(curl -sf "http://127.0.0.1:${TARGET_PORT}/api/cluster/status" 2>/dev/null); then
  printf '%s\n' "$status" >"$STATUS_JSON"
  python3 - "$STATUS_JSON" "$BUCKET" <<'PY'
import json
import sys

path, bucket = sys.argv[1], sys.argv[2]
with open(path, "r", encoding="utf-8") as f:
    status = json.load(f)

assignments = status.get("bucket_assignments") or {}
groups = {g.get("id"): g for g in status.get("shard_groups") or []}
group_id = assignments.get(bucket)
if not group_id:
    print(f"[bench] topology bucket={bucket} group=<unassigned> topology_json={path}")
    sys.exit(0)

peers = groups.get(group_id, {}).get("peer_ids") or []
print(
    f"[bench] topology bucket={bucket} group={group_id} "
    f"voters={len(peers)} peer_ids={','.join(peers)} topology_json={path}"
)
PY
else
  echo "[bench] topology status unavailable from :${TARGET_PORT}"
fi

if [[ "$PROFILE" == "1" ]]; then
  curl -sf "http://127.0.0.1:${TARGET_PPROF_PORT}/debug/pprof/heap" \
    -o "$PROFILE_DIR/heap_pre.pb.gz" && echo "[pprof] heap_pre.pb.gz saved" || true
  if [[ "$PROFILE_ALL_NODES" == "1" ]]; then
    cp "$PROFILE_DIR/heap_pre.pb.gz" "$PROFILE_DIR/node${TARGET_IDX}/heap_pre.pb.gz" 2>/dev/null || true
    for idx in $(seq 1 "$NODE_COUNT"); do
      [[ "$idx" == "$TARGET_IDX" ]] && continue
      node_pprof_port="$(pprof_port_for_idx "$idx")"
      curl -sf "http://127.0.0.1:${node_pprof_port}/debug/pprof/heap" \
        -o "$PROFILE_DIR/node${idx}/heap_pre.pb.gz" && echo "[pprof] node${idx}/heap_pre.pb.gz saved" || true
    done
  fi
fi

echo ""
echo "=================================================================="
echo "  GrainFS GET-only topology benchmark"
echo "  nodes  : ${NODE_COUNT}"
echo "  target : http://127.0.0.1:${TARGET_PORT}"
echo "  object : ${SIZE_KB}KB x ${OBJECT_COUNT}"
echo "  vus    : ${VUS}  duration: ${DURATION}"
if [[ "$NODE_COUNT" -ge 3 ]]; then
  echo "  ec     : target ${EC_DATA}+${EC_PARITY} (effective scales by node count)"
else
  echo "  ec     : inactive for <3 nodes"
fi
if [[ "$PROFILE" == "1" ]]; then
  echo "  pprof  : http://127.0.0.1:${TARGET_PPROF_PORT}/debug/pprof/"
  [[ "$PROFILE_ALL_NODES" == "1" ]] && echo "  pprof  : all nodes enabled"
fi
echo "=================================================================="
echo ""

PPROF_BG_PIDS=()
if [[ "$PROFILE" == "1" ]]; then
  duration_sec="${DURATION//[^0-9]/}"
  [[ -z "$duration_sec" ]] && duration_sec=30
  cpu_sec=$(( duration_sec > 10 ? duration_sec - 5 : duration_sec ))
  collect_cpu_profile() {
    local idx="$1"
    local port="$2"
    local out="$3"
    (
      sleep 5
      echo "[pprof] collecting node${idx} ${cpu_sec}s CPU profile..."
      curl -sf "http://127.0.0.1:${port}/debug/pprof/profile?seconds=${cpu_sec}" \
        -o "$out" && echo "[pprof] node${idx} cpu.pb.gz saved" || echo "[pprof] node${idx} CPU profile failed"
    ) &
    PPROF_BG_PIDS+=($!)
  }
  if [[ "$PROFILE_ALL_NODES" == "1" ]]; then
    for idx in $(seq 1 "$NODE_COUNT"); do
      collect_cpu_profile "$idx" "$(pprof_port_for_idx "$idx")" "$PROFILE_DIR/node${idx}/cpu.pb.gz"
    done
  else
    collect_cpu_profile "$TARGET_IDX" "$TARGET_PPROF_PORT" "$PROFILE_DIR/cpu.pb.gz"
  fi
fi

"$K6" run "$SCRIPT" \
  --env BASE_URL="http://127.0.0.1:${TARGET_PORT}" \
  --env BUCKET="$BUCKET" \
  --env OBJECT_SIZE_KB="$SIZE_KB" \
  --env OBJECT_COUNT="$OBJECT_COUNT" \
  --env SETUP_TIMEOUT="$SETUP_TIMEOUT" \
  --env PRELOADED="$PRELOADED" \
  --env DURATION="$DURATION" \
  --env RAMP_UP="$RAMP_UP" \
  --env RAMP_DOWN="$RAMP_DOWN" \
  --env GRACEFUL_RAMP_DOWN="$GRACEFUL_RAMP_DOWN" \
  --env GRACEFUL_STOP="$GRACEFUL_STOP" \
  --env MAX_VUS="$VUS" \
  --env TOPOLOGY="nodes=${NODE_COUNT}" \
  "$@" || K6_EXIT=$?

for pid in "${PPROF_BG_PIDS[@]}"; do
  wait "$pid" 2>/dev/null || true
done
if [[ "$PROFILE" == "1" && "$PROFILE_ALL_NODES" == "1" ]]; then
  cp "$PROFILE_DIR/node${TARGET_IDX}/cpu.pb.gz" "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || true
fi

if [[ "$PROFILE" == "1" ]]; then
  echo ""
  echo "[pprof] collecting post-benchmark profiles..."
  bench_collect_pprof "$TARGET_PPROF_PORT" "$PROFILE_DIR" heap allocs goroutine mutex block
  if [[ "$PROFILE_ALL_NODES" == "1" ]]; then
    cp "$PROFILE_DIR/heap.pb.gz" "$PROFILE_DIR/node${TARGET_IDX}/heap.pb.gz" 2>/dev/null || true
    cp "$PROFILE_DIR/allocs.pb.gz" "$PROFILE_DIR/node${TARGET_IDX}/allocs.pb.gz" 2>/dev/null || true
    cp "$PROFILE_DIR/goroutine.txt" "$PROFILE_DIR/node${TARGET_IDX}/goroutine.txt" 2>/dev/null || true
    cp "$PROFILE_DIR/mutex.pb.gz" "$PROFILE_DIR/node${TARGET_IDX}/mutex.pb.gz" 2>/dev/null || true
    cp "$PROFILE_DIR/block.pb.gz" "$PROFILE_DIR/node${TARGET_IDX}/block.pb.gz" 2>/dev/null || true
    for idx in $(seq 1 "$NODE_COUNT"); do
      [[ "$idx" == "$TARGET_IDX" ]] && continue
      node_pprof_port="$(pprof_port_for_idx "$idx")"
      bench_collect_pprof "$node_pprof_port" "$PROFILE_DIR/node${idx}" heap allocs goroutine mutex block
    done
  fi

  cp benchmarks/get_report.json "$PROFILE_DIR/get_report.json" 2>/dev/null || true

  echo ""
  echo "=================================================================="
  echo "  pprof: CPU top-15"
  echo "=================================================================="
  go tool pprof -top -nodecount=15 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  (pprof analysis failed)"

  echo ""
  echo "=================================================================="
  echo "  pprof: heap top-15"
  echo "=================================================================="
  go tool pprof -top -nodecount=15 "$PROFILE_DIR/heap.pb.gz" 2>/dev/null || echo "  (heap analysis failed)"

  echo ""
  echo "[pprof] saved to $PROFILE_DIR/"
  ls -lh "$PROFILE_DIR/"
fi

echo ""
echo "[bench] done. logs in $BENCH_DIR/"
exit "${K6_EXIT:-0}"
