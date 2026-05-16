#!/usr/bin/env bash
# Compare GrainFS S3 performance against S3-compatible reference stores.
#
# Defaults run GrainFS single-node, MinIO, and RustFS when native binaries are
# available. External endpoints can be supplied with <TARGET>_URL variables.
#
# Examples:
#   ./benchmarks/bench_s3_compat_compare.sh
#   TARGETS=grainfs-single,minio WARP_DURATION=1m WARP_OBJ_SIZE=20MiB WARP_CONCURRENT=32 ./benchmarks/bench_s3_compat_compare.sh
#   RUSTFS_URL=http://127.0.0.1:9002 RUSTFS_ACCESS_KEY=rustfsadmin RUSTFS_SECRET_KEY=rustfsadmin ./benchmarks/bench_s3_compat_compare.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${BINARY:-./bin/grainfs}"
TARGETS="${TARGETS:-grainfs-single,minio,rustfs}"
PROFILE_ROOT="${PROFILE_ROOT:-benchmarks/profiles/s3-compat-compare-$(date +%Y%m%d-%H%M%S)}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-s3-compat-compare}"
BUCKET="${BUCKET:-bench}"
WARP_BIN="${WARP_BIN:-$(command -v warp 2>/dev/null || true)}"
WARP_DURATION="${WARP_DURATION:-30s}"
WARP_OBJ_SIZE="${WARP_OBJ_SIZE:-64KiB}"
WARP_OBJECTS="${WARP_OBJECTS:-4096}"
WARP_CONCURRENT="${WARP_CONCURRENT:-16}"
WARP_OPS="${WARP_OPS:-put,get}"
WARP_NOCLEAR="${WARP_NOCLEAR:-1}"
WARP_HOST_SELECT="${WARP_HOST_SELECT:-roundrobin}"

MINIO_BIN="${MINIO_BIN:-$(command -v minio 2>/dev/null || true)}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"

RUSTFS_BIN="${RUSTFS_BIN:-$(command -v rustfs 2>/dev/null || true)}"
RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"

if [[ -z "$WARP_BIN" ]]; then
  echo "[error] warp is required for S3-compatible comparison benchmarks. Install minio/warp or set WARP_BIN." >&2
  exit 1
fi

mkdir -p "$PROFILE_ROOT"
rm -rf "$BENCH_DIR"
mkdir -p "$BENCH_DIR"

PIDS=()
START_BASE_URL=""
START_ACCESS_KEY=""
START_SECRET_KEY=""
START_MODE=""

set_start_info() {
  START_BASE_URL="$1"
  START_ACCESS_KEY="$2"
  START_SECRET_KEY="$3"
  START_MODE="$4"
}

cleanup() {
  echo "[bench] stopping comparison backends..."
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [[ "${KEEP_BENCH_DIR:-0}" != "1" ]]; then
    rm -rf "$BENCH_DIR" 2>/dev/null || true
  else
    echo "[bench] bench data dir saved to $BENCH_DIR"
  fi
}
trap cleanup EXIT INT TERM

start_grainfs_single() {
  if [[ "${GRAINFS_SINGLE_URL:-}" != "" ]]; then
    set_start_info "$GRAINFS_SINGLE_URL" "${GRAINFS_ACCESS_KEY:-}" "${GRAINFS_SECRET_KEY:-}" "external"
    return 0
  fi

  if [[ "${NO_BUILD:-0}" != "1" ]]; then
    echo "[bench] building grainfs..." >&2
    make build >&2
  fi
  bench_require_binary "$BINARY"

  local data_dir="$BENCH_DIR/grainfs-single"
  local port
  port="$(bench_free_port)"
  mkdir -p "$data_dir"
  BENCH_ENCRYPTION_KEY_FILE="$data_dir/encryption.key"
  export BENCH_ENCRYPTION_KEY_FILE
  bench_generate_encryption_key_file "$BENCH_ENCRYPTION_KEY_FILE"

  "$BINARY" serve \
    --data "$data_dir" \
    --port "$port" \
    --cluster-key "bench-s3-compat-key" \
    $(bench_encryption_args) \
    --nfs4-port 0 \
    --nbd-port 0 \
    --scrub-interval 0 \
    --lifecycle-interval 0 \
    --log-level warn \
    >"$PROFILE_ROOT/grainfs-single.log" 2>&1 &
  PIDS+=($!)
  bench_wait_tcp_port "127.0.0.1" "$port" "grainfs-single S3" 180 0.2 >&2
  bench_bootstrap_iam_credentials "$BINARY" "$data_dir" "bench-s3-compat" >&2
  set_start_info "http://127.0.0.1:$port" "$ACCESS_KEY" "$SECRET_KEY" "local"
}

start_minio() {
  if [[ "${MINIO_URL:-}" != "" ]]; then
    set_start_info "$MINIO_URL" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" "external"
    return 0
  fi
  if [[ -z "$MINIO_BIN" ]]; then
    return 1
  fi
  if "$MINIO_BIN" --version 2>/dev/null | grep -qi 'AIStor'; then
    echo "minio: skipped; installed binary is MinIO AIStor and may deny S3 operations without a license. Set MINIO_BIN to a benchmarkable native MinIO binary or MINIO_URL to an existing endpoint." >>"$PROFILE_ROOT/skipped.txt"
    return 1
  fi

  local data_dir="$BENCH_DIR/minio"
  local port console_port
  port="$(bench_free_port)"
  console_port="$(bench_free_port)"
  mkdir -p "$data_dir"

  MINIO_ROOT_USER="$MINIO_ACCESS_KEY" \
  MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY" \
  "$MINIO_BIN" server "$data_dir" \
    --address "127.0.0.1:$port" \
    --console-address "127.0.0.1:$console_port" \
    >"$PROFILE_ROOT/minio.log" 2>&1 &
  PIDS+=($!)
  bench_wait_tcp_port "127.0.0.1" "$port" "minio S3" 180 0.2 >&2
  set_start_info "http://127.0.0.1:$port" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" "local"
}

start_rustfs() {
  if [[ "${RUSTFS_URL:-}" != "" ]]; then
    set_start_info "$RUSTFS_URL" "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY" "external"
    return 0
  fi
  if [[ -z "$RUSTFS_BIN" ]]; then
    return 1
  fi

  local data_dir="$BENCH_DIR/rustfs"
  local port console_port
  port="$(bench_free_port)"
  console_port="$(bench_free_port)"
  mkdir -p "$data_dir"

  RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
  RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
  RUSTFS_REGION="us-east-1" \
  "$RUSTFS_BIN" server \
    --address "127.0.0.1:$port" \
    --console-enable \
    --console-address "127.0.0.1:$console_port" \
    "$data_dir" \
    >"$PROFILE_ROOT/rustfs.log" 2>&1 &
  PIDS+=($!)
  bench_wait_tcp_port "127.0.0.1" "$port" "rustfs S3" 180 0.2 >&2
  set_start_info "http://127.0.0.1:$port" "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY" "local"
}

write_summary_header() {
  local summary="$PROFILE_ROOT/summary.md"
  {
    echo "# S3-Compatible Benchmark Comparison"
    echo
    echo "- date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "- commit: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
    echo "- targets: $TARGETS"
    echo "- tool: warp ($("$WARP_BIN" --version 2>/dev/null | head -n 1 || echo "$WARP_BIN"))"
    echo "- operations: ${WARP_OPS}"
    echo "- object: ${WARP_OBJ_SIZE}"
    echo "- objects for GET seed: ${WARP_OBJECTS}"
    echo "- duration: ${WARP_DURATION}"
    echo "- concurrency: ${WARP_CONCURRENT}"
    echo "- noclear: ${WARP_NOCLEAR}"
    echo "- host select: ${WARP_HOST_SELECT}"
    echo "- raw artifacts: ${PROFILE_ROOT}"
    echo
    echo "> Method: all targets use signed S3 requests through warp, identical object size, concurrency, duration, and bucket lookup mode. PUT and GET are reported separately. With WARP_NOCLEAR=1, GET is a warm-read pass over objects written by the preceding PUT pass."
    echo
    echo "> Caveat: GrainFS runs with at-rest encryption. MinIO/RustFS local runs use their default single-node durability unless an external endpoint is supplied."
    echo
    echo "| target | mode | op | MiB/s | obj/s | errors | ratio vs MinIO | ratio vs RustFS | artifacts |"
    echo "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |"
  } >"$summary"
}

host_for_warp() {
  local url="$1"
  local hosts=()
  local part
  IFS=',' read -ra hosts <<<"$url"
  for i in "${!hosts[@]}"; do
    part="${hosts[$i]}"
    part="${part#http://}"
    part="${part#https://}"
    hosts[$i]="${part%%/*}"
  done
  (IFS=','; echo "${hosts[*]}")
}

run_warp_case() {
  local target="$1"
  local base_url="$2"
  local access_key="$3"
  local secret_key="$4"
  local op="$5"
  local out_dir="$PROFILE_ROOT/$target/warp-$op"
  local data_file="$out_dir/warp.data.json.zst"
  local host
  local args
  host="$(host_for_warp "$base_url")"
  mkdir -p "$out_dir"

  echo "[bench] $target warp $op host=$host object=$WARP_OBJ_SIZE concurrent=$WARP_CONCURRENT"
  args=(
    "$op"
    --no-color
    --host "$host" \
    --access-key "$access_key" \
    --secret-key "$secret_key" \
    --bucket "warp-${target}" \
    --duration "$WARP_DURATION" \
    --obj.size "$WARP_OBJ_SIZE" \
    --concurrent "$WARP_CONCURRENT" \
    --lookup path
    --host-select "$WARP_HOST_SELECT"
    --benchdata "$out_dir/warp.data"
  )
  if [[ "$op" == "get" ]]; then
    args+=(--objects "$WARP_OBJECTS")
  fi
  if [[ "$WARP_NOCLEAR" == "1" ]]; then
    args+=(--noclear)
  fi
  if ! "$WARP_BIN" "${args[@]}" >"$out_dir/warp.out" 2>&1; then
    echo "warp-$op: non-zero exit for $target; see $out_dir/warp.out" | tee -a "$PROFILE_ROOT/skipped.txt"
  fi

  if [[ ! -f "$data_file" ]]; then
    echo "warp-$op: missing benchdata for $target; see $out_dir/warp.out" | tee -a "$PROFILE_ROOT/skipped.txt"
    return 0
  fi

  if ! "$WARP_BIN" analyze "$data_file" >"$out_dir/analyze.out" 2>&1; then
    echo "warp-$op: analyze failed for $target; see $out_dir/analyze.out" | tee -a "$PROFILE_ROOT/skipped.txt"
    return 0
  fi

  if ! python3 - "$target" "$START_MODE" "$op" "$out_dir/analyze.out" "$PROFILE_ROOT/warp-results.tsv" "$out_dir" <<'PY'
import re
import sys

target, mode, op, analyze_path, out_path, artifact_dir = sys.argv[1:]
text = open(analyze_path, encoding="utf-8").read()
avg = re.search(r"Average:\s+([0-9.]+)\s+MiB/s,\s+([0-9.]+)\s+obj/s", text)
err = re.search(r"Errors:\s+([0-9]+)", text)
if not avg:
    sys.exit("missing Average line")
row = [
    target,
    mode,
    op,
    f"{float(avg.group(1)):.2f}",
    f"{float(avg.group(2)):.2f}",
    str(int(err.group(1)) if err else 0),
    artifact_dir,
]
with open(out_path, "a", encoding="utf-8") as f:
    f.write("\t".join(row) + "\n")
PY
  then
    echo "warp-$op: missing average throughput for $target; see $out_dir/analyze.out" | tee -a "$PROFILE_ROOT/skipped.txt"
  fi
}

append_summary_rows() {
  local summary="$PROFILE_ROOT/summary.md"
  python3 - "$PROFILE_ROOT/warp-results.tsv" "$summary" <<'PY'
import sys
from collections import defaultdict

results_path, summary_path = sys.argv[1:]
rows = []
try:
    with open(results_path) as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 7:
                continue
            rows.append(parts)
except FileNotFoundError:
    rows = []

by_case = defaultdict(dict)
for row in rows:
    target, _, op = row[:3]
    by_case[op][target] = float(row[3])

def ratio(value, base):
    if not base:
        return ""
    return f"{value / base:.2f}x"

with open(summary_path, "a") as out:
    for row in rows:
        target, mode, op, mib, objs, errors, artifact_dir = row
        throughput = float(mib)
        case = by_case[op]
        minio = case.get("minio", 0)
        rustfs = case.get("rustfs", 0)
        out.write(
            f"| {target} | {mode} | {op.upper()} | {throughput:.2f} | {float(objs):.2f} | "
            f"{int(errors)} | {ratio(throughput, minio)} | {ratio(throughput, rustfs)} | `{artifact_dir}` |\n"
        )
PY
}

target_enabled() {
  local needle="$1"
  case ",$TARGETS," in
    *",$needle,"*) return 0 ;;
    *) return 1 ;;
  esac
}

write_summary_header
: >"$PROFILE_ROOT/warp-results.tsv"
: >"$PROFILE_ROOT/skipped.txt"
IFS=',' read -ra WARP_OP_LIST <<<"$WARP_OPS"

for target in grainfs-single minio rustfs; do
  target_enabled "$target" || continue

  case "$target" in
    grainfs-single)
      start_grainfs_single
      ;;
    minio)
      if ! start_minio; then
        echo "minio: skipped; set MINIO_BIN or MINIO_URL" | tee -a "$PROFILE_ROOT/skipped.txt"
        continue
      fi
      ;;
    rustfs)
      if ! start_rustfs; then
        echo "rustfs: skipped; set RUSTFS_BIN or RUSTFS_URL" | tee -a "$PROFILE_ROOT/skipped.txt"
        continue
      fi
      ;;
  esac

  base_url="$START_BASE_URL"
  access_key="$START_ACCESS_KEY"
  secret_key="$START_SECRET_KEY"
  mode="$START_MODE"
  for op in "${WARP_OP_LIST[@]}"; do
    case "$op" in
      get|put) run_warp_case "$target" "$base_url" "$access_key" "$secret_key" "$op" ;;
      *)
        echo "[error] unknown WARP_OPS entry: $op" >&2
        exit 1
        ;;
    esac
  done
done

append_summary_rows

echo
echo "=================================================================="
echo "  summary"
echo "=================================================================="
cat "$PROFILE_ROOT/summary.md"
if [[ -s "$PROFILE_ROOT/skipped.txt" ]]; then
  echo
  echo "Skipped targets:"
  cat "$PROFILE_ROOT/skipped.txt"
fi
echo
echo "[bench] profiles saved to $PROFILE_ROOT"
