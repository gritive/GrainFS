#!/usr/bin/env bash
# bench_compare.sh — GrainFS vs MinIO 동일 워크로드 비교 벤치마크.
#
# 한 머신에서 grainfs와 minio를 순차적으로 기동하여
# 같은 k6 워크로드(s3_bench.js)를 동일 파라미터로 돌리고,
# 결과 JSON과 비교 마크다운을 results-{timestamp}/ 디렉토리에 저장한다.
#
# 사용법:
#   ./benchmarks/bench_compare.sh [k6 추가 인자]
#
# 환경 변수:
#   SYSTEMS       — 비교할 시스템 (default: "grainfs minio")
#   MODES         — 측정 모드        (default: "single cluster")
#   SIZES_KB      — 오브젝트 크기 KB (default: "4 64 1024")
#   DURATION      — 부하 지속 시간   (default: 30s)
#   VUS           — 최대 동시 VU    (default: 20)
#   NODES         — cluster 노드 수 (default: 4)
#   EC_DATA       — grainfs EC data shards   (default: 2)
#   EC_PARITY     — grainfs EC parity shards (default: 2)
#   PROFILE       — 1이면 grainfs pprof 수집 (default: 0)
#   NO_BUILD      — 1이면 grainfs build 생략 (default: 0)
#   OUT_DIR       — 결과 디렉토리   (default: benchmarks/results-{ts})
#
# Single 모드: grainfs 1-node vs minio 1-node 1-drive
# Cluster 모드: grainfs N-node EC ${EC_DATA}+${EC_PARITY} vs minio N-node distributed (자동 EC)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# minio binary 자동 검출 (lib source 전에 export해야 함)
if [[ -z "${MINIO_BIN:-}" ]]; then
  if [[ -x "$HOME/.local/bin/minio-community" ]]; then
    export MINIO_BIN="$HOME/.local/bin/minio-community"
    echo "[bench] using community minio: $MINIO_BIN"
  elif command -v minio >/dev/null 2>&1; then
    if minio --version 2>&1 | grep -q "AIStor"; then
      echo "[error] system 'minio' is the AIStor enterprise build (license required)." >&2
      echo "        download community edition:" >&2
      echo "        curl -fL https://dl.min.io/server/minio/release/darwin-arm64/archive/minio.RELEASE.2025-09-07T16-13-09Z \\" >&2
      echo "          -o ~/.local/bin/minio-community && chmod +x ~/.local/bin/minio-community" >&2
      exit 1
    fi
    export MINIO_BIN="minio"
  else
    echo "[error] minio binary not found" >&2
    exit 1
  fi
fi

# shellcheck source=lib_grainfs.sh
source "$SCRIPT_DIR/lib_grainfs.sh"
# shellcheck source=lib_minio.sh
source "$SCRIPT_DIR/lib_minio.sh"

SYSTEMS="${SYSTEMS:-grainfs minio}"
MODES="${MODES:-single cluster}"
SIZES_KB="${SIZES_KB:-4 64 1024}"
DURATION="${DURATION:-30s}"
VUS="${VUS:-20}"
NODES="${NODES:-4}"
EC_DATA="${EC_DATA:-2}"
EC_PARITY="${EC_PARITY:-2}"
PROFILE="${PROFILE:-0}"
TS="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-benchmarks/results-$TS}"

K6="${K6:-k6}"
SCRIPT="$SCRIPT_DIR/s3_bench.js"

# 의존성 체크
command -v "$K6" >/dev/null || { echo "[error] k6 not found" >&2; exit 1; }

mkdir -p "$OUT_DIR"
echo "[bench] writing results to $OUT_DIR"

# grainfs 빌드
if [[ "${NO_BUILD:-0}" != "1" ]]; then
  echo "[bench] building grainfs…"
  make build >/dev/null
fi
[[ -x "./bin/grainfs" ]] || { echo "[error] ./bin/grainfs not found" >&2; exit 1; }

# 포트 베이스 (충돌 방지를 위해 grainfs/minio 분리)
GRAINFS_S3_BASE=9100
GRAINFS_RAFT_BASE=19100
GRAINFS_PPROF_BASE=$((PROFILE == 1 ? 6060 : 0))
GRAINFS_SINGLE_PORT=9000
GRAINFS_SINGLE_PPROF=$((PROFILE == 1 ? 6070 : 0))

MINIO_S3_BASE=9200
MINIO_CONSOLE_BASE=9300
MINIO_SINGLE_PORT=9210
MINIO_SINGLE_CONSOLE=9310

DATA_ROOT="/tmp/grainfs-bench-compare"
rm -rf "$DATA_ROOT"; mkdir -p "$DATA_ROOT"

# Cleanup
cleanup() {
  echo "[bench] cleanup..."
  grainfs_stop || true
  minio_stop  || true
}
trap cleanup EXIT INT TERM

# ── helper: 워크로드 1회 실행 ─────────────────────────────────────────────────
# args: <system> <mode> <size_kb> <base_url> <ak> <sk> <bucket>
run_workload() {
  local system="$1" mode="$2" size_kb="$3" url="$4" ak="$5" sk="$6" bucket="$7"
  shift 7
  local report="$OUT_DIR/${system}-${mode}-${size_kb}KB.json"

  echo ""
  echo "──────────────────────────────────────────────────────────────────────"
  echo "  ${system} / ${mode} / ${size_kb}KB"
  echo "  URL: $url"
  echo "──────────────────────────────────────────────────────────────────────"

  "$K6" run "$SCRIPT" \
    --env BASE_URL="$url" \
    --env ACCESS_KEY="$ak" \
    --env SECRET_KEY="$sk" \
    --env BUCKET="$bucket" \
    --env OBJECT_SIZE_KB="$size_kb" \
    --env DURATION="$DURATION" \
    --env MAX_VUS="$VUS" \
    --env REPORT_PATH="$report" \
    --quiet \
    "$@" || true

  if [[ -f "$report" ]]; then
    # 메타데이터 추가
    local tmp; tmp=$(mktemp)
    jq --arg system "$system" --arg mode "$mode" --argjson size "$size_kb" \
       --arg duration "$DURATION" --argjson vus "$VUS" \
       '.system=$system | .mode=$mode | .object_size_kb=$size | .duration=$duration | .vus=$vus' \
       "$report" > "$tmp" && mv "$tmp" "$report"
    echo "[bench] saved: $report"
  else
    echo "[warn] no report produced for $system/$mode/${size_kb}KB"
  fi
}

# ── helper: 기동 + sweep + 종료 ────────────────────────────────────────────────
run_grainfs_single() {
  echo ""
  echo "════════════════════════════════════════════════════════════════════"
  echo "  GRAINFS — single node"
  echo "════════════════════════════════════════════════════════════════════"
  grainfs_start_single "$DATA_ROOT/grainfs-single" "$GRAINFS_SINGLE_PORT" "$GRAINFS_SINGLE_PPROF"
  for size in $SIZES_KB; do
    run_workload "grainfs" "single" "$size" "$GRAINFS_LEADER_URL" \
      "$GRAINFS_ACCESS_KEY" "$GRAINFS_SECRET_KEY" "bench-grainfs"
  done
  grainfs_stop
  sleep 2
}

run_grainfs_cluster() {
  echo ""
  echo "════════════════════════════════════════════════════════════════════"
  echo "  GRAINFS — ${NODES}-node cluster (EC ${EC_DATA}+${EC_PARITY})"
  echo "════════════════════════════════════════════════════════════════════"
  grainfs_start_cluster "$DATA_ROOT/grainfs-cluster" "$NODES" \
    "$GRAINFS_S3_BASE" "$GRAINFS_RAFT_BASE" "$EC_DATA" "$EC_PARITY" "$GRAINFS_PPROF_BASE"

  # PROFILE=1이면 첫 사이즈에서만 pprof 수집
  local profile_done=0
  for size in $SIZES_KB; do
    if [[ "$PROFILE" == "1" && "$profile_done" == "0" && -n "$GRAINFS_LEADER_PPROF" ]]; then
      profile_done=1
      local prof_dir="$OUT_DIR/grainfs-cluster-pprof"
      mkdir -p "$prof_dir"
      curl -sf "http://127.0.0.1:${GRAINFS_LEADER_PPROF}/debug/pprof/heap" \
        -o "$prof_dir/heap_pre.pb.gz" || true
      # 30초 부하 동안 25초 CPU 프로파일을 백그라운드로 수집
      (
        sleep 5
        curl -sf "http://127.0.0.1:${GRAINFS_LEADER_PPROF}/debug/pprof/profile?seconds=25" \
          -o "$prof_dir/cpu.pb.gz" || true
      ) &
      local prof_pid=$!
      run_workload "grainfs" "cluster" "$size" "$GRAINFS_LEADER_URL" \
        "$GRAINFS_ACCESS_KEY" "$GRAINFS_SECRET_KEY" "bench-grainfs"
      wait "$prof_pid" 2>/dev/null || true
      curl -sf "http://127.0.0.1:${GRAINFS_LEADER_PPROF}/debug/pprof/heap" \
        -o "$prof_dir/heap_post.pb.gz" || true
      curl -sf "http://127.0.0.1:${GRAINFS_LEADER_PPROF}/debug/pprof/allocs" \
        -o "$prof_dir/allocs.pb.gz" || true
      curl -sf "http://127.0.0.1:${GRAINFS_LEADER_PPROF}/debug/pprof/mutex" \
        -o "$prof_dir/mutex.pb.gz" || true
      echo "[pprof] saved to $prof_dir/"
    else
      run_workload "grainfs" "cluster" "$size" "$GRAINFS_LEADER_URL" \
        "$GRAINFS_ACCESS_KEY" "$GRAINFS_SECRET_KEY" "bench-grainfs"
    fi
  done
  grainfs_stop
  sleep 2
}

run_minio_single() {
  echo ""
  echo "════════════════════════════════════════════════════════════════════"
  echo "  MINIO — single node"
  echo "════════════════════════════════════════════════════════════════════"
  minio_start_single "$DATA_ROOT/minio-single" "$MINIO_SINGLE_PORT" "$MINIO_SINGLE_CONSOLE"
  for size in $SIZES_KB; do
    run_workload "minio" "single" "$size" "$MINIO_LEADER_URL" \
      "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" "bench-minio"
  done
  minio_stop
  sleep 2
}

run_minio_cluster() {
  echo ""
  echo "════════════════════════════════════════════════════════════════════"
  echo "  MINIO — ${NODES}-node distributed"
  echo "════════════════════════════════════════════════════════════════════"
  minio_start_cluster "$DATA_ROOT/minio-cluster" "$NODES" \
    "$MINIO_S3_BASE" "$MINIO_CONSOLE_BASE"
  for size in $SIZES_KB; do
    run_workload "minio" "cluster" "$size" "$MINIO_LEADER_URL" \
      "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" "bench-minio"
  done
  minio_stop
  sleep 2
}

# ── 실행 ─────────────────────────────────────────────────────────────────────
START_TS=$(date +%s)
for sys in $SYSTEMS; do
  for mode in $MODES; do
    case "$sys-$mode" in
      grainfs-single)  run_grainfs_single  ;;
      grainfs-cluster) run_grainfs_cluster ;;
      minio-single)   run_minio_single   ;;
      minio-cluster)  run_minio_cluster  ;;
    esac
  done
done
END_TS=$(date +%s)

# ── 비교 마크다운 생성 ─────────────────────────────────────────────────────────
echo ""
echo "[bench] generating summary..."

REPORT_MD="$OUT_DIR/SUMMARY.md"

# ratio: g/m. 둘 다 양수일 때만 의미 있음.
compute_ratio() {
  local g="$1" m="$2"
  if [[ -z "$g" || "$g" == "null" || "$g" == "0" || -z "$m" || "$m" == "null" || "$m" == "0" ]]; then
    echo "—"
  else
    awk -v a="$g" -v b="$m" 'BEGIN{ printf "%.2fx", a/b }'
  fi
}

{
  echo "# GrainFS vs MinIO 벤치마크 결과"
  echo ""
  echo "- **실행 시각:** $(date)"
  echo "- **소요 시간:** $((END_TS - START_TS))s"
  echo "- **노드 수:** ${NODES} (cluster mode)"
  echo "- **EC 설정:** grainfs ${EC_DATA}+${EC_PARITY}, minio 자동 (4-drive면 2+2)"
  echo "- **워크로드:** mixed PUT→GET→50% DELETE (s3_bench.js)"
  echo "- **VUs:** ${VUS}, Duration: ${DURATION}"
  echo "- **OS:** $(uname -srm)"
  echo "- **Git:** $(git rev-parse --short HEAD 2>/dev/null || echo unknown) ($(git branch --show-current 2>/dev/null))"
  echo ""

  for mode in $MODES; do
    echo "## ${mode^^} mode"
    echo ""
    for op in put get delete; do
      echo "### ${op^^} latency / throughput"
      echo ""
      echo "| size | system | ops | p50 (ms) | p99 (ms) | avg (ms) | failed |"
      echo "|------|--------|-----|----------|----------|----------|--------|"
      for size in $SIZES_KB; do
        for sys in $SYSTEMS; do
          rfile="$OUT_DIR/${sys}-${mode}-${size}KB.json"
          if [[ -f "$rfile" ]]; then
            ops=$(jq -r ".${op}.ops // 0" "$rfile")
            p50=$(jq -r ".${op}.p50_ms // \"-\"" "$rfile")
            p99=$(jq -r ".${op}.p99_ms // \"-\"" "$rfile")
            avg=$(jq -r ".${op}.avg_ms // \"-\"" "$rfile")
            failed=$(jq -r ".summary.failed_requests // 0" "$rfile")
            echo "| ${size}KB | $sys | $ops | $p50 | $p99 | $avg | $failed |"
          else
            echo "| ${size}KB | $sys | — | — | — | — | — |"
          fi
        done
      done
      echo ""
    done
  done

  echo "## 비교 비율 (grainfs/minio, 1.0보다 작으면 grains가 빠름)"
  echo ""
  for mode in $MODES; do
    echo "### ${mode^^}"
    echo ""
    echo "| size | put p50 | put p99 | get p50 | get p99 | put ops/s | get ops/s |"
    echo "|------|---------|---------|---------|---------|-----------|-----------|"
    for size in $SIZES_KB; do
      g_file="$OUT_DIR/grainfs-${mode}-${size}KB.json"
      m_file="$OUT_DIR/minio-${mode}-${size}KB.json"
      if [[ -f "$g_file" && -f "$m_file" ]]; then
        g_put_p50=$(jq -r '.put.p50_ms // 0' "$g_file"); m_put_p50=$(jq -r '.put.p50_ms // 0' "$m_file")
        g_put_p99=$(jq -r '.put.p99_ms // 0' "$g_file"); m_put_p99=$(jq -r '.put.p99_ms // 0' "$m_file")
        g_get_p50=$(jq -r '.get.p50_ms // 0' "$g_file"); m_get_p50=$(jq -r '.get.p50_ms // 0' "$m_file")
        g_get_p99=$(jq -r '.get.p99_ms // 0' "$g_file"); m_get_p99=$(jq -r '.get.p99_ms // 0' "$m_file")
        g_put_ops=$(jq -r '.put.ops // 0'    "$g_file"); m_put_ops=$(jq -r '.put.ops // 0'    "$m_file")
        g_get_ops=$(jq -r '.get.ops // 0'    "$g_file"); m_get_ops=$(jq -r '.get.ops // 0'    "$m_file")
        # 처리량 비율은 minio/grainfs (큰 게 좋음)이 직관적이지만, 일관성을 위해 모두 grainfs/minio
        # 처리량은 1.0 이상이 grainfs 우위, 레이턴시는 1.0 이하가 grainfs 우위
        echo "| ${size}KB | $(compute_ratio "$g_put_p50" "$m_put_p50") | $(compute_ratio "$g_put_p99" "$m_put_p99") | $(compute_ratio "$g_get_p50" "$m_get_p50") | $(compute_ratio "$g_get_p99" "$m_get_p99") | $(compute_ratio "$g_put_ops" "$m_put_ops") | $(compute_ratio "$g_get_ops" "$m_get_ops") |"
      else
        echo "| ${size}KB | — | — | — | — | — | — |"
      fi
    done
    echo ""
  done

  if [[ "$PROFILE" == "1" && -d "$OUT_DIR/grainfs-cluster-pprof" ]]; then
    echo "## grainfs pprof (cluster, 첫 size)"
    echo ""
    echo "프로파일 위치: \`$OUT_DIR/grainfs-cluster-pprof/\`"
    echo ""
    echo "분석:"
    echo '```'
    echo 'go tool pprof -top -nodecount=15 '"$OUT_DIR"/grainfs-cluster-pprof/cpu.pb.gz
    echo 'go tool pprof -top -nodecount=15 '"$OUT_DIR"/grainfs-cluster-pprof/heap_post.pb.gz
    echo 'go tool pprof -http=:8080 '"$OUT_DIR"/grainfs-cluster-pprof/cpu.pb.gz
    echo '```'
    echo ""
  fi
} > "$REPORT_MD"

echo ""
echo "═══════════════════════════════════════════════════════════════════════"
echo "  Done. Results: $OUT_DIR"
echo "  Summary:      $REPORT_MD"
echo "═══════════════════════════════════════════════════════════════════════"
