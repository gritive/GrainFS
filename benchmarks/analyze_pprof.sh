#!/usr/bin/env bash
# analyze_pprof.sh — bench_compare.sh 결과의 grains pprof 디렉토리에서
# top-N CPU/heap/mutex/allocs 추출 후 마크다운으로 정리.
#
# 사용법:
#   ./benchmarks/analyze_pprof.sh <results-dir>
#
# 예: ./benchmarks/analyze_pprof.sh benchmarks/results-20260428-063130/

set -euo pipefail

RESULTS_DIR="${1:-}"
[[ -z "$RESULTS_DIR" ]] && {
  echo "usage: $0 <results-dir>"
  echo "  e.g. $0 benchmarks/results-20260428-063130/"
  exit 1
}

PPROF_DIR="$RESULTS_DIR/grains-cluster-pprof"
[[ -d "$PPROF_DIR" ]] || {
  echo "[error] pprof dir not found: $PPROF_DIR"
  exit 1
}

OUT="$RESULTS_DIR/PPROF_ANALYSIS.md"
NODECOUNT="${NODECOUNT:-20}"

{
  echo "# grains pprof analysis"
  echo ""
  echo "Profile dir: \`$PPROF_DIR\`"
  echo "Profile target: cluster mode, 첫 size sweep (cf. SUMMARY.md)"
  echo ""

  for prof in cpu heap_post allocs mutex; do
    pb="$PPROF_DIR/${prof}.pb.gz"
    [[ -f "$pb" ]] || continue
    echo "## ${prof}"
    echo ""
    echo '```'
    go tool pprof -top -nodecount="$NODECOUNT" "$pb" 2>&1 | head -50 || echo "(pprof failed)"
    echo '```'
    echo ""
  done

  echo "## 분석 가이드"
  echo ""
  echo "- **CPU 상위에 syscall/scheduler 비중** = I/O bound. SIMD/CPU 최적화 의미 적음"
  echo "- **CPU 상위에 EC/AES/MD5/CRC** = compute bound. SIMD 검토 가치 있음"
  echo "- **Mutex/Block top 항목** = lock contention. lock-free / atomic 후보"
  echo "- **Heap top 항목 / inuse_space** = leak 가능성, sync.Pool 검토 후보"
  echo "- **Allocs top 항목** = GC pressure 원인, allocation reduction 후보"
  echo ""
  echo "## interactive 분석"
  echo ""
  echo '```bash'
  echo "go tool pprof -http=:8080 $PPROF_DIR/cpu.pb.gz"
  echo "go tool pprof -http=:8081 $PPROF_DIR/heap_post.pb.gz"
  echo '```'
} > "$OUT"

echo "wrote: $OUT"
