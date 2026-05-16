#!/usr/bin/env bash
# Single-node Iceberg REST Catalog table API benchmark.

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
RAMP_UP="${RAMP_UP:-10s}"
RAMP_DOWN="${RAMP_DOWN:-5s}"

if [[ "${NO_BUILD:-0}" != "1" ]]; then
  echo "[bench] building grainfs..."
  make build
fi
bench_require_binary "$BINARY"

DATA_DIR=$(mktemp -d "grainfs-iceberg-table-bench-XXXX" -p /tmp)
PROFILE_DIR=""
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
  PROFILE_DIR="benchmarks/profiles/iceberg-table-$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$PROFILE_DIR"
  SERVE_ARGS+=(--pprof-port "$PPROF_PORT")
  echo "[bench] pprof profile dir: $PROFILE_DIR"
fi

echo "[bench] starting grainfs Iceberg table API server on :$BASE_PORT"
"${SERVE_ARGS[@]}" >/tmp/grainfs-iceberg-table-bench.log 2>&1 &
PIDS+=($!)

if ! bench_wait_http_ready "http://127.0.0.1:$BASE_PORT/iceberg/v1/config?warehouse=warehouse" "Iceberg REST Catalog" 60 0.2; then
  tail -40 /tmp/grainfs-iceberg-table-bench.log >&2
  exit 1
fi
bench_wait_cluster_leader "http://127.0.0.1:$BASE_PORT" 120 0.5
bench_bootstrap_iam_credentials "$BINARY" "$DATA_DIR" "bench-iceberg-table"

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

go run ./benchmarks/iceberg_table_bench \
  -base-url "http://127.0.0.1:$BASE_PORT" \
  -access-key "$ACCESS_KEY" \
  -secret-key "$SECRET_KEY" \
  -concurrency "$VUS" \
  -duration "$DURATION" \
  -namespace-prefix "bench_ns" \
  -report "benchmarks/iceberg_table_report.json" \
  "$@" || BENCH_EXIT=$?

[[ -n "$PPROF_BG_PID" ]] && wait "$PPROF_BG_PID" 2>/dev/null || true

if [[ "$PROFILE" == "1" ]]; then
  echo "[pprof] collecting post-benchmark profiles..."
  bench_collect_pprof "$PPROF_PORT" "$PROFILE_DIR" heap allocs goroutine mutex block
  echo "[pprof] CPU top-10"
  go tool pprof -top -nodecount=10 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  (pprof analysis failed)"
  echo "[pprof] profiles saved to $PROFILE_DIR/"
fi

echo "[bench] done. report: benchmarks/iceberg_table_report.json"
exit "${BENCH_EXIT:-0}"
