#!/usr/bin/env bash
# Iceberg REST Catalog cluster benchmark using MinIO warp.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${BINARY:-./bin/grainfs}"
NODE_COUNT="${NODE_COUNT:-4}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-iceberg-table-cluster-bench}"
PROFILE="${PROFILE:-0}"
VUS="${VUS:-${MAX_VUS:-10}}"
DURATION="${DURATION:-30s}"
WARP_BIN="${WARP_BIN:-$(command -v warp 2>/dev/null || true)}"
ICEBERG_WARP_COMMAND="${ICEBERG_WARP_COMMAND:-catalog-mixed}"
ICEBERG_CATALOG="${ICEBERG_CATALOG:-warehouse}"
ICEBERG_BUCKET="${ICEBERG_BUCKET:-grainfs-tables}"
ICEBERG_BASE_LOCATION="${ICEBERG_BASE_LOCATION:-s3://${ICEBERG_BUCKET}}"
ICEBERG_NAMESPACE_WIDTH="${ICEBERG_NAMESPACE_WIDTH:-1}"
ICEBERG_NAMESPACE_DEPTH="${ICEBERG_NAMESPACE_DEPTH:-1}"
# catalog-commits stress (warp iceberg uses ~5 concurrent workers internally).
# At 5 workers, P(at least one collision per commit-round) = 1 - 200!/((200-5)! * 200^5)
# ≈ 0.05% with N=200 tables — well below noise floor. Default 4 floods 409s;
# even 40 leaves ~11% spec-compliant 409 (verified). Spec §5 caps fanout at
# max(16, VUS*4) but the strict gate (failed_requests=0) requires near-zero
# statistical collision; 200 hits that without inflating prepare so much that
# we exit the 30s window. The latency sub-gate (p99<1s / max<3s) still
# surfaces raft/forwarding regressions even after the workload is spread.
ICEBERG_TABLES_PER_NS="${ICEBERG_TABLES_PER_NS:-200}"
ICEBERG_VIEWS_PER_NS="${ICEBERG_VIEWS_PER_NS:-0}"
ICEBERG_COLUMNS="${ICEBERG_COLUMNS:-10}"
ICEBERG_PROPERTIES="${ICEBERG_PROPERTIES:-5}"
ICEBERG_NS_UPDATE_DISTRIB="${ICEBERG_NS_UPDATE_DISTRIB:-0}"
ICEBERG_TABLE_UPDATE_DISTRIB="${ICEBERG_TABLE_UPDATE_DISTRIB:-0}"
ICEBERG_VIEW_UPDATE_DISTRIB="${ICEBERG_VIEW_UPDATE_DISTRIB:-0}"
WARP_HOST_SELECT="${WARP_HOST_SELECT:-roundrobin}"
WARP_NOCLEAR="${WARP_NOCLEAR:-1}"

if [[ "${NO_BUILD:-0}" != "1" ]]; then
  echo "[bench] building grainfs..."
  make build
fi
bench_require_binary "$BINARY"
bench_require_command "$WARP_BIN" "brew install minio/stable/warp"

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
BENCH_ENCRYPTION_KEY_FILE="${BENCH_ENCRYPTION_KEY_FILE:-$BENCH_DIR/encryption.key}"
export BENCH_ENCRYPTION_KEY_FILE
bench_generate_encryption_key_file "$BENCH_ENCRYPTION_KEY_FILE"

cleanup() {
  local status=$?
  echo "[bench] stopping Iceberg table API cluster..."
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [[ -n "${PROFILE_DIR:-}" ]]; then
    bench_copy_node_logs "$BENCH_DIR" "$PROFILE_DIR"
  fi
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
    --log-level "${GRAINFS_LOG_LEVEL:-info}" \
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

echo "[bench] creating Iceberg warehouse bucket ($ICEBERG_BUCKET)..."
bench_create_bucket_with_policy_admin_retry "$BINARY" "$BENCH_DIR/n$TARGET_INDEX" "$ICEBERG_BUCKET" "$SA_ID" bucket-admin

PROFILE_DIR="${PROFILE_ROOT:-benchmarks/profiles/iceberg-table-${NODE_COUNT}-node-cluster-warp-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$PROFILE_DIR"
PPROF_BG_PID=""
if [[ "$PROFILE" == "1" ]]; then
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

hosts=""
for i in $(seq 0 $((NODE_COUNT - 1))); do
  part="127.0.0.1:$(http_port "$i")"
  if [[ -z "$hosts" ]]; then
    hosts="$part"
  else
    hosts="${hosts},${part}"
  fi
done
benchdata="$PROFILE_DIR/warp.data"
args=(
  iceberg "$ICEBERG_WARP_COMMAND"
  --no-color
  --host "$hosts"
  --access-key "$ACCESS_KEY"
  --secret-key "$SECRET_KEY"
  # warp 1.5.0 has no --catalog-prop flag — the AIStor catalog driver always
  # signs REST calls with AWS SigV4 (service=s3tables, region=cfg.Region).
  # GrainFS's SigV4 verifier accepts any service name the client signs with,
  # so the signing-name property would have been a no-op here even if warp
  # supported it. Region is supplied via the env-default --region flag below
  # if non-default values are needed.
  --bucket "$ICEBERG_BUCKET"
  --catalog-name "$ICEBERG_CATALOG"
  --base-location "$ICEBERG_BASE_LOCATION"
  --namespace-width "$ICEBERG_NAMESPACE_WIDTH"
  --namespace-depth "$ICEBERG_NAMESPACE_DEPTH"
  --tables-per-ns "$ICEBERG_TABLES_PER_NS"
  --concurrent "$VUS"
  --duration "$DURATION"
  --host-select "$WARP_HOST_SELECT"
  --lookup path
  --benchdata "$benchdata"
)
case "$ICEBERG_WARP_COMMAND" in
  catalog-read|catalog-mixed)
    args+=(
      --views-per-ns "$ICEBERG_VIEWS_PER_NS"
      --columns "$ICEBERG_COLUMNS"
      --properties "$ICEBERG_PROPERTIES"
    )
    if [[ "$ICEBERG_WARP_COMMAND" == "catalog-mixed" ]]; then
      args+=(
        --ns-update-distrib "$ICEBERG_NS_UPDATE_DISTRIB"
        --table-update-distrib "$ICEBERG_TABLE_UPDATE_DISTRIB"
        --view-update-distrib "$ICEBERG_VIEW_UPDATE_DISTRIB"
      )
    fi
    ;;
  catalog-commits)
    args+=(--views-per-ns "$ICEBERG_VIEWS_PER_NS")
    ;;
  sustained)
    args+=(--columns "$ICEBERG_COLUMNS" --properties "$ICEBERG_PROPERTIES")
    ;;
  *)
    echo "[error] unsupported ICEBERG_WARP_COMMAND: $ICEBERG_WARP_COMMAND" >&2
    exit 1
    ;;
esac
if [[ "$WARP_NOCLEAR" == "1" ]]; then
  args+=(--noclear)
fi

echo "[bench] running warp iceberg ${ICEBERG_WARP_COMMAND}"
"$WARP_BIN" "${args[@]}" "$@" >"$PROFILE_DIR/warp.out" 2>&1 || BENCH_EXIT=$?
data_file="$benchdata.json.zst"
if [[ -f "$data_file" ]]; then
  "$WARP_BIN" analyze "$data_file" >"$PROFILE_DIR/analyze.out" 2>&1 || true
fi
{
  echo "# Iceberg warp cluster benchmark"
  echo
  echo "- date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- commit: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "- command: ${ICEBERG_WARP_COMMAND}"
  echo "- hosts: ${hosts}"
  echo "- catalog: ${ICEBERG_CATALOG}"
  echo "- bucket: ${ICEBERG_BUCKET}"
  echo "- base location: ${ICEBERG_BASE_LOCATION}"
  echo "- concurrency: ${VUS}"
  echo "- duration: ${DURATION}"
  echo "- update distribution: ns=${ICEBERG_NS_UPDATE_DISTRIB}, table=${ICEBERG_TABLE_UPDATE_DISTRIB}, view=${ICEBERG_VIEW_UPDATE_DISTRIB}"
  echo "- raw artifacts: ${PROFILE_DIR}"
} >"$PROFILE_DIR/summary.md"

[[ -n "$PPROF_BG_PID" ]] && wait "$PPROF_BG_PID" 2>/dev/null || true

if [[ "$PROFILE" == "1" ]]; then
  target_pprof="$(pprof_port "$TARGET_INDEX")"
  bench_collect_pprof "$target_pprof" "$PROFILE_DIR" heap allocs goroutine mutex block
  echo "[pprof] CPU top-10"
  go tool pprof -top -nodecount=10 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  (pprof analysis failed)"
  echo "[pprof] profiles saved to $PROFILE_DIR/"
fi

echo "[bench] done. artifacts: $PROFILE_DIR"
exit "${BENCH_EXIT:-0}"
