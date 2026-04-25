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
#
# 3노드 포트 배치:
#   노드1: S3=9100  Raft=19100
#   노드2: S3=9101  Raft=19101
#   노드3: S3=9102  Raft=19102
#
# 부하는 노드1(리더 후보)에만 집중한다. ProposeForward가 있으므로 팔로워에
# 요청을 보내도 동작하지만, 처음 측정은 리더 직접 경로로 기준선을 잡는다.

set -euo pipefail

BINARY="${BINARY:-./bin/grainfs}"
K6="${K6:-k6}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-bench}"
DURATION="${DURATION:-30s}"
VUS="${VUS:-20}"
SIZE_KB="${SIZE_KB:-64}"
SCRIPT="$(dirname "$0")/s3_bench.js"

# ── 의존성 확인 ────────────────────────────────────────────────────────────────
if ! command -v "$K6" &>/dev/null; then
  echo "[error] k6 not found. Install: brew install k6" >&2
  exit 1
fi

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

# ── 노드 시작 헬퍼 ────────────────────────────────────────────────────────────
PIDS=()

start_node() {
  local idx="$1"        # 1 | 2 | 3
  local s3_port="$2"
  local raft_port="$3"
  local peers="$4"      # 쉼표 구분 raft 주소 목록 (자신 제외)
  local logfile="$BENCH_DIR/n${idx}.log"

  "$BINARY" serve \
    --data "$BENCH_DIR/n${idx}" \
    --port "$s3_port" \
    --raft-addr "127.0.0.1:${raft_port}" \
    --peers "$peers" \
    --cluster-key "bench-local-key" \
    --no-encryption \
    --ec-data 2 \
    --ec-parity 1 \
    >"$logfile" 2>&1 &

  PIDS+=($!)
  echo "[bench] node${idx} started (s3=:${s3_port} raft=:${raft_port} pid=${PIDS[-1]})"
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

# ── 클러스터 시작 ─────────────────────────────────────────────────────────────
PEERS_ALL="127.0.0.1:19100,127.0.0.1:19101,127.0.0.1:19102"

start_node 1 9100 19100 "127.0.0.1:19101,127.0.0.1:19102"
start_node 2 9101 19101 "127.0.0.1:19100,127.0.0.1:19102"
start_node 3 9102 19102 "127.0.0.1:19100,127.0.0.1:19101"

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
  for port in 9100 9101 9102; do
    status=$(curl -sf "http://127.0.0.1:${port}/api/cluster/status" 2>/dev/null) || continue
    node_id=$(echo "$status" | grep -o '"node_id":"[^"]*"' | cut -d'"' -f4)
    leader_id=$(echo "$status" | grep -o '"leader_id":"[^"]*"' | cut -d'"' -f4)
    if [[ -n "$leader_id" && "$node_id" == "$leader_id" ]]; then
      LEADER_PORT="$port"
      break
    fi
  done
  [[ -z "$LEADER_PORT" ]] && sleep 0.5
done

echo "[bench] cluster ready — leader on port ${LEADER_PORT}"

# ── 버킷 생성 ────────────────────────────────────────────────────────────────
echo "[bench] creating bucket 'bench' on leader…"
curl -sf -X PUT "http://127.0.0.1:${LEADER_PORT}/bench" >/dev/null 2>&1 || true

# ── k6 벤치마크 실행 ──────────────────────────────────────────────────────────
echo ""
echo "=================================================================="
echo "  GrainFS 3-node cluster benchmark"
echo "  target : http://127.0.0.1:${LEADER_PORT}  (leader)"
echo "  vus    : ${VUS}  duration: ${DURATION}  object: ${SIZE_KB}KB"
echo "=================================================================="
echo ""

"$K6" run "$SCRIPT" \
  --env BASE_URL="http://127.0.0.1:${LEADER_PORT}" \
  --env BUCKET="bench" \
  --env OBJECT_SIZE_KB="$SIZE_KB" \
  --env DURATION="$DURATION" \
  --env MAX_VUS="$VUS" \
  "$@"

echo ""
echo "[bench] done. logs in $BENCH_DIR/"
