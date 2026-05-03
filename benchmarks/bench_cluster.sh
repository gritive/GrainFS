#!/usr/bin/env bash
# bench_cluster.sh — 3노드 GrainFS 클러스터를 로컬에서 시작하고 k6 S3 벤치마크를 실행한다.
#
# 사용법:
#   ./benchmarks/bench_cluster.sh [k6 추가 인자]
#
# 환경 변수:
#   BINARY        — grainfs 바이너리 경로 (기본: ./bin/grainfs)
#   K6            — k6 바이너리 경로 (기본: k6)
#   BENCH_DIR     — 노드 데이터 디렉토리 기반 경로 (기본: /tmp/grainfs-bench)
#   DURATION      — 부하 테스트 지속 시간 (기본: 30s)
#   VUS           — 최대 동시 VU 수 (기본: 20)
#   SIZE_KB       — 오브젝트 크기 KB (기본: 64)
#   NO_BUILD      — 1이면 빌드 건너뜀
#   PROFILE       — 1이면 pprof 프로파일 수집 (기본: 0)
#   PPROF_PORT    — pprof HTTP 포트 (기본: 6060, PROFILE=1일 때만 사용)
#
# 3노드 포트 배치:
#   노드1: S3=9100  Raft=19100
#   노드2: S3=9101  Raft=19101
#   노드3: S3=9102  Raft=19102
#
# 리더 포트는 Raft 선출 후 /api/cluster/status로 자동 감지한다.
# PROFILE=1이면 각 노드에 --pprof-port를 열고, 실제 writable target 노드에서
# CPU/heap 프로파일을 수집한 뒤 go tool pprof 요약을 출력한다.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${BINARY:-./bin/grainfs}"
K6="${K6:-k6}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-bench}"
DURATION="${DURATION:-30s}"
RAMP_UP="${RAMP_UP:-10s}"
RAMP_DOWN="${RAMP_DOWN:-5s}"
VUS="${VUS:-${MAX_VUS:-20}}"
SIZE_KB="${SIZE_KB:-64}"
PROFILE="${PROFILE:-0}"
PPROF_PORT="${PPROF_PORT:-6060}"
SCRIPT="$BENCHMARKS_DIR/s3_bench.js"

# ── 의존성 확인 ────────────────────────────────────────────────────────────────
bench_require_command "$K6" "brew install k6"

# ── 빌드 ───────────────────────────────────────────────────────────────────────
if [[ "${NO_BUILD:-0}" != "1" ]]; then
  echo "[bench] building grainfs…"
  make build
fi

if [[ ! -x "$BINARY" ]]; then
  echo "[error] binary not found: $BINARY" >&2
  exit 1
fi

# ── 임시 디렉토리 정리 ────────────────────────────────────────────────────────
rm -rf "$BENCH_DIR"
mkdir -p "$BENCH_DIR"/{n1,n2,n3}

# ── 프로파일 디렉토리 ────────────────────────────────────────────────────────
PROFILE_DIR=""
if [[ "$PROFILE" == "1" ]]; then
  PROFILE_DIR="benchmarks/profiles/$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$PROFILE_DIR"
  echo "[bench] pprof profile dir: $PROFILE_DIR"
fi

# ── 노드 시작 헬퍼 ────────────────────────────────────────────────────────────
PIDS=()
# 리더 상태 출력용 pprof 포트 기록
LEADER_PPROF_PORT=""

start_node() {
  local idx="$1"        # 1 | 2 | 3
  local s3_port="$2"
  local raft_port="$3"
  local peers="$4"      # 쉼표 구분 raft 주소 목록 (자신 제외)
  local extra="${5:-}"  # 추가 플래그 (--pprof-port 등)
  local logfile="$BENCH_DIR/n${idx}.log"

  "$BINARY" serve \
    --data "$BENCH_DIR/n${idx}" \
    --port "$s3_port" \
    --raft-addr "127.0.0.1:${raft_port}" \
    --peers "$peers" \
    --cluster-key "bench-local-key" \
    $(bench_encryption_args) \
    --ec-data 2 \
    --ec-parity 1 \
    --nfs4-port 0 \
    --nbd-port 0 \
    --rate-limit-ip-rps 0 \
    --rate-limit-user-rps 0 \
    $extra \
    >"$logfile" 2>&1 &

  PIDS+=($!)
  echo "[bench] node${idx} started (s3=:${s3_port} raft=:${raft_port} pid=${PIDS[-1]}${extra:+ $extra})"
}

# ── 종료 핸들러 ───────────────────────────────────────────────────────────────
cleanup() {
  echo "[bench] stopping nodes…"
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  echo "[bench] cluster stopped"
}
trap cleanup EXIT INT TERM

# ── 클러스터 시작 (PROFILE=1이면 노드1에 pprof 포트 예비 할당) ──────────────
# 리더가 누가 될지 모르므로 일단 고정 포트로 3노드 모두 시작.
# 리더 감지 후 그 노드에 pprof가 열려 있어야 하므로,
# PROFILE=1이면 세 노드 모두에 서로 다른 pprof 포트를 할당한다.
if [[ "$PROFILE" == "1" ]]; then
  start_node 1 9100 19100 "127.0.0.1:19101,127.0.0.1:19102" "--pprof-port $PPROF_PORT"
  start_node 2 9101 19101 "127.0.0.1:19100,127.0.0.1:19102" "--pprof-port $((PPROF_PORT+1))"
  start_node 3 9102 19102 "127.0.0.1:19100,127.0.0.1:19101" "--pprof-port $((PPROF_PORT+2))"
else
  start_node 1 9100 19100 "127.0.0.1:19101,127.0.0.1:19102"
  start_node 2 9101 19101 "127.0.0.1:19100,127.0.0.1:19102"
  start_node 3 9102 19102 "127.0.0.1:19100,127.0.0.1:19101"
fi

# ── 클러스터 준비 대기 ────────────────────────────────────────────────────────
echo "[bench] waiting for cluster to elect a leader…"
DEADLINE=$(( $(date +%s) + 30 ))

# 모든 노드 TCP 포트가 열릴 때까지 대기
for port in 9100 9101 9102; do
  until nc -z 127.0.0.1 "$port" 2>/dev/null; do
    if (( $(date +%s) > DEADLINE )); then
      echo "[error] node port $port did not open within 30s" >&2
      tail -30 "$BENCH_DIR"/n*.log >&2
      exit 1
    fi
    sleep 0.3
  done
done

# Raft 리더 선출 대기: /api/cluster/status에서 node_id == leader_id 인 노드 탐색
LEADER_PORT=""
LEADER_DEADLINE=$(( $(date +%s) + 30 ))
while [[ -z "$LEADER_PORT" ]]; do
  if (( $(date +%s) > LEADER_DEADLINE )); then
    echo "[error] no Raft leader elected within 30s" >&2
    exit 1
  fi
  for port_idx in 0 1 2; do
    port=$((9100 + port_idx))
    status=$(curl -sf "http://127.0.0.1:${port}/api/cluster/status" 2>/dev/null) || continue
    node_id=$(echo "$status" | grep -o '"node_id":"[^"]*"' | cut -d'"' -f4)
    leader_id=$(echo "$status" | grep -o '"leader_id":"[^"]*"' | cut -d'"' -f4)
    if [[ -n "$leader_id" && "$node_id" == "$leader_id" ]]; then
      LEADER_PORT="$port"
      LEADER_PPROF_PORT=$((PPROF_PORT + port_idx))
      break
    fi
  done
  [[ -z "$LEADER_PORT" ]] && sleep 0.5
done

if [[ "$PROFILE" == "1" ]]; then
  echo "[bench] cluster ready — leader on port ${LEADER_PORT} (pprof=:${LEADER_PPROF_PORT})"
else
  echo "[bench] cluster ready — leader on port ${LEADER_PORT}"
fi

TARGET_PORT=""
TARGET_PPROF_PORT=""
for _ in $(seq 1 60); do
  for port_idx in 0 1 2; do
    port=$((9100 + port_idx))
    if BENCH_QUIET=1 bench_create_bucket_retry "http://127.0.0.1:${port}" "bench" 1 0.1 &&
      BENCH_QUIET=1 bench_put_object_retry "http://127.0.0.1:${port}" "bench" ".bench-ready" 1 0.1; then
      TARGET_PORT="$port"
      TARGET_PPROF_PORT=$((PPROF_PORT + port_idx))
      break 2
    fi
  done
  sleep 0.5
done

if [[ -z "$TARGET_PORT" ]]; then
  echo "[error] no writable S3 endpoint found" >&2
  tail -30 "$BENCH_DIR"/n*.log >&2
  exit 1
fi
echo "[bench] writable target on port ${TARGET_PORT}"
sleep "${CLUSTER_WARMUP_SLEEP:-5}"

# ── 프로파일: 벤치마크 전 heap 수집 ─────────────────────────────────────────
if [[ "$PROFILE" == "1" ]]; then
  echo "[pprof] collecting pre-benchmark heap…"
  curl -sf "http://127.0.0.1:${TARGET_PPROF_PORT}/debug/pprof/heap" \
    -o "$PROFILE_DIR/heap_pre.pb.gz" && echo "[pprof] heap_pre.pb.gz saved" || true
fi

# ── k6 벤치마크 실행 ──────────────────────────────────────────────────────────
echo ""
echo "=================================================================="
echo "  GrainFS 3-node cluster benchmark"
echo "  target : http://127.0.0.1:${TARGET_PORT}"
echo "  vus    : ${VUS}  duration: ${DURATION}  object: ${SIZE_KB}KB"
[[ "$PROFILE" == "1" ]] && echo "  pprof  : http://127.0.0.1:${TARGET_PPROF_PORT}/debug/pprof/"
echo "=================================================================="
echo ""

# PROFILE=1이면 CPU 프로파일을 k6와 동시에 수집 (워밍업 5s 후 시작)
PPROF_BG_PID=""
if [[ "$PROFILE" == "1" ]]; then
  # DURATION에서 숫자 파싱 (예: "30s" → 30)
  DURATION_SEC="${DURATION//[^0-9]/}"
  CPU_SEC=$(( DURATION_SEC > 10 ? DURATION_SEC - 5 : DURATION_SEC ))
  (
    sleep 5
    echo "[pprof] collecting ${CPU_SEC}s CPU profile…"
    curl -sf "http://127.0.0.1:${TARGET_PPROF_PORT}/debug/pprof/profile?seconds=${CPU_SEC}" \
      -o "$PROFILE_DIR/cpu.pb.gz" && echo "[pprof] cpu.pb.gz saved" || echo "[pprof] CPU profile failed"
  ) &
  PPROF_BG_PID=$!
fi

"$K6" run "$SCRIPT" \
  --env BASE_URL="http://127.0.0.1:${TARGET_PORT}" \
  --env BUCKET="bench" \
  --env OBJECT_SIZE_KB="$SIZE_KB" \
  --env DURATION="$DURATION" \
  --env RAMP_UP="$RAMP_UP" \
  --env RAMP_DOWN="$RAMP_DOWN" \
  --env MAX_VUS="$VUS" \
  "$@" || K6_EXIT=$?

# CPU 프로파일 완료 대기
[[ -n "$PPROF_BG_PID" ]] && wait "$PPROF_BG_PID" 2>/dev/null || true

# ── 프로파일: 벤치마크 후 나머지 수집 ───────────────────────────────────────
if [[ "$PROFILE" == "1" ]]; then
  echo ""
  echo "[pprof] collecting post-benchmark profiles…"
  bench_collect_pprof "$TARGET_PPROF_PORT" "$PROFILE_DIR" heap allocs goroutine mutex block

  echo ""
  echo "=================================================================="
  echo "  pprof: CPU top-10"
  echo "=================================================================="
  go tool pprof -top -nodecount=10 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  (pprof analysis failed)"

  echo ""
  echo "=================================================================="
  echo "  pprof: heap (post-benchmark) top-10"
  echo "=================================================================="
  go tool pprof -top -nodecount=10 "$PROFILE_DIR/heap.pb.gz" 2>/dev/null || echo "  (heap analysis failed)"

  cp "$PROFILE_DIR/cpu.pb.gz" /tmp/grainfs-bench-cpu.out 2>/dev/null || true
  echo "  PGO profile: /tmp/grainfs-bench-cpu.out"

  echo ""
  echo "[pprof] all profiles saved to $PROFILE_DIR/"
  ls -lh "$PROFILE_DIR/"
  echo ""
  echo "  interactive: go tool pprof -http=:8080 $PROFILE_DIR/cpu.pb.gz"
fi

echo ""
echo "[bench] done. logs in $BENCH_DIR/"
exit "${K6_EXIT:-0}"
