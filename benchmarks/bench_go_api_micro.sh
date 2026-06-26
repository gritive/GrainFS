#!/usr/bin/env bash
# Run Go S3 API microbenchmarks with profiles in a stable artifact directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

PROFILE_ROOT="${PROFILE_ROOT:-benchmarks/profiles/go-api-micro-$(date +%Y%m%d-%H%M%S)}"
BENCHTIME="${BENCHTIME:-5x}"
COUNT="${COUNT:-1}"
SINGLE_BENCH="${SINGLE_BENCH:-BenchmarkS3}"
CLUSTER_BENCH="${CLUSTER_BENCH:-BenchmarkCluster(PutObject|HeadObject|DeleteObject|ListObjectsPage|Copy|Multipart|Append)}"

bench_tmp_base="${TMPDIR:-/tmp}"
if [[ "$(uname -s)" == "Darwin" ]]; then
  bench_tmp_base="/tmp"
fi
RUN_TMP="$(mktemp -d "${bench_tmp_base%/}/grainfs-go-api-micro.XXXXXX")"
CLEANED_UP=0

cleanup() {
  if [[ "$CLEANED_UP" == "1" ]]; then
    return 0
  fi
  CLEANED_UP=1
  rm -rf "$RUN_TMP" 2>/dev/null || true
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

mkdir -p "$PROFILE_ROOT"

run_go_bench() {
  local label="$1"
  local pkg="$2"
  local pattern="$3"
  shift 3
  local out_dir="$PROFILE_ROOT/$label"
  mkdir -p "$out_dir"
  printf '[bench] %s %s\n' "$label" "$pkg" | tee "$out_dir/bench.txt"
  env "$@" go test \
    -run '^$' \
    -bench "$pattern" \
    -benchtime="$BENCHTIME" \
    -count="$COUNT" \
    -benchmem \
    -cpuprofile "$out_dir/cpu.pb.gz" \
    -memprofile "$out_dir/mem.pb.gz" \
    "$pkg" | tee -a "$out_dir/bench.txt"
}

run_go_bench single-1drive ./internal/storage "$SINGLE_BENCH"
run_go_bench single-4drive ./internal/storage "$SINGLE_BENCH" GRAINFS_S3_BENCH_DRIVES=4
run_go_bench cluster ./internal/cluster "$CLUSTER_BENCH"

echo
echo "[bench] profiles written to $PROFILE_ROOT"
