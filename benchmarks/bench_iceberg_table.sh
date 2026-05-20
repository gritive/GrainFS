#!/usr/bin/env bash
# Single-node Iceberg REST Catalog benchmark using MinIO warp.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${BINARY:-./bin/grainfs}"
BASE_PORT="${BASE_PORT:-$(bench_free_port)}"
PPROF_PORT="${PPROF_PORT:-$(bench_free_port)}"
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
if [[ -z "${ICEBERG_TABLES_PER_NS+x}" ]]; then
  if [[ "$ICEBERG_WARP_COMMAND" == "catalog-commits" ]]; then
    ICEBERG_TABLES_PER_NS="200"
  else
    ICEBERG_TABLES_PER_NS="4"
  fi
fi
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

DATA_DIR=$(mktemp -d "grainfs-iceberg-table-bench-XXXX" -p /tmp)
PROFILE_DIR="${PROFILE_ROOT:-benchmarks/profiles/iceberg-table-warp-$(date +%Y%m%d-%H%M%S)}"
PIDS=()

cleanup() {
  echo "[bench] stopping Iceberg table API benchmark server..."
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT INT TERM

SERVE_ARGS=(
  "$BINARY" serve
  --data "$DATA_DIR"
  --port "$BASE_PORT"
  --nfs4-port 0
  --nbd-port 0
  --cluster-key "bench-iceberg-table-key"
  $(bench_encryption_args)
  --lifecycle-interval 0
  --log-level warn
)

if [[ "$PROFILE" == "1" ]]; then
  SERVE_ARGS+=(--pprof-port "$PPROF_PORT")
  echo "[bench] pprof profile dir: $PROFILE_DIR"
fi
mkdir -p "$PROFILE_DIR"

echo "[bench] starting grainfs Iceberg table API server on :$BASE_PORT"
"${SERVE_ARGS[@]}" >/tmp/grainfs-iceberg-table-bench.log 2>&1 &
PIDS+=($!)

if ! bench_wait_http_ready "http://127.0.0.1:$BASE_PORT/iceberg/v1/config?warehouse=warehouse" "Iceberg REST Catalog" 60 0.2; then
  tail -40 /tmp/grainfs-iceberg-table-bench.log >&2
  exit 1
fi
bench_wait_cluster_leader "http://127.0.0.1:$BASE_PORT" 120 0.5
bench_bootstrap_iam_credentials "$BINARY" "$DATA_DIR" "bench-iceberg-table"
echo "[bench] creating Iceberg warehouse bucket ($ICEBERG_BUCKET)..."
bench_create_bucket_admin_retry "$BINARY" "$DATA_DIR" "$ICEBERG_BUCKET"

PPROF_BG_PID=""
if [[ "$PROFILE" == "1" ]]; then
  curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/heap" \
    -o "$PROFILE_DIR/heap_pre.pb.gz" && echo "[pprof] heap_pre.pb.gz saved" || true
  (
    sleep 5
    echo "[pprof] collecting CPU profile..."
    curl -sf "http://127.0.0.1:$PPROF_PORT/debug/pprof/profile?seconds=30" \
      -o "$PROFILE_DIR/cpu.pb.gz" && echo "[pprof] cpu.pb.gz saved" || echo "[pprof] CPU profile failed"
  ) &
  PPROF_BG_PID=$!
fi

host="127.0.0.1:$BASE_PORT"
benchdata="$PROFILE_DIR/warp.data"
args=(
  iceberg "$ICEBERG_WARP_COMMAND"
  --no-color
  --host "$host"
  --access-key "$ACCESS_KEY"
  --secret-key "$SECRET_KEY"
  # warp 1.5.0 has no --catalog-prop flag. The AIStor catalog driver signs
  # REST calls with SigV4, and GrainFS accepts the service name chosen by the
  # client.
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
  echo "# Iceberg warp benchmark"
  echo
  echo "- date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- commit: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "- command: ${ICEBERG_WARP_COMMAND}"
  echo "- host: ${host}"
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
  echo "[pprof] collecting post-benchmark profiles..."
  bench_collect_pprof "$PPROF_PORT" "$PROFILE_DIR" heap allocs goroutine mutex block
  echo "[pprof] CPU top-10"
  go tool pprof -top -nodecount=10 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  (pprof analysis failed)"
  echo "[pprof] profiles saved to $PROFILE_DIR/"
fi

echo "[bench] done. artifacts: $PROFILE_DIR"
exit "${BENCH_EXIT:-0}"
