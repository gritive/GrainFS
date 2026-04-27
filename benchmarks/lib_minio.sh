#!/usr/bin/env bash
# lib_minio.sh — sourced by bench_compare.sh
# minio single / distributed 클러스터 기동 헬퍼.
#
# Provides:
#   minio_start_single   <data_dir> <port> <console_port>
#   minio_start_cluster  <data_dir> <nodes> <base_port> <base_console_port>
#   minio_stop
#   MINIO_PIDS, MINIO_LEADER_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY  (set after start)
#
# 기본 인증: test / testtest12 (minio 최소 8자 정책)

MINIO_PIDS=()
MINIO_LEADER_URL=""
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-test}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-testtest12}"
MINIO_BIN="${MINIO_BIN:-minio}"

_minio_wait_ready() {
  # Cluster 모드는 /health/ready 통과 후에도 모든 sub-system 초기화 완료까지 ~5s가 추가로 걸린다.
  # 이 사이에 PUT bucket을 호출하면 503 XMinioServerNotInitialized 응답.
  # /health/cluster (200 = cluster healthy)까지 기다려야 안전하다.
  local url="$1"; local deadline=$(( $(date +%s) + 90 ))
  # 1) /health/ready
  while (( $(date +%s) < deadline )); do
    if curl -sf "${url}/minio/health/ready" >/dev/null 2>&1; then
      break
    fi
    sleep 0.5
  done
  # 2) /health/cluster (cluster mode only; single mode also returns 200)
  while (( $(date +%s) < deadline )); do
    if curl -sf "${url}/minio/health/cluster" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "[error] minio cluster not initialized at $url within 90s" >&2
  return 1
}

minio_start_single() {
  local data_dir="$1"; local port="$2"; local console_port="${3:-0}"
  rm -rf "$data_dir"
  mkdir -p "$data_dir/d1"

  MINIO_ROOT_USER="$MINIO_ACCESS_KEY" \
  MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY" \
  MINIO_CI_CD=on \
  "$MINIO_BIN" server "$data_dir/d1" \
    --address ":$port" \
    --console-address ":$console_port" \
    >"$data_dir/minio.log" 2>&1 &
  MINIO_PIDS+=($!)
  MINIO_LEADER_URL="http://127.0.0.1:${port}"
  echo "[minio] single started (port=$port pid=${MINIO_PIDS[-1]})"
  _minio_wait_ready "$MINIO_LEADER_URL"
}

minio_start_cluster() {
  local data_dir="$1"; local nodes="$2"; local base_port="$3"; local base_console_port="$4"
  rm -rf "$data_dir"
  mkdir -p "$data_dir"

  # distributed mode: 모든 노드가 같은 server-url 인자를 받는다.
  # 각 노드는 자신의 단일 drive를 호스팅: $data_dir/n{i}/d1
  local urls=""
  for ((i=1; i<=nodes; i++)); do
    local p=$((base_port + i - 1))
    mkdir -p "$data_dir/n${i}/d1"
    urls+="http://127.0.0.1:${p}${data_dir}/n${i}/d1 "
  done

  for ((i=1; i<=nodes; i++)); do
    local p=$((base_port + i - 1))
    local cp=$((base_console_port + i - 1))
    MINIO_ROOT_USER="$MINIO_ACCESS_KEY" \
    MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY" \
    MINIO_CI_CD=on \
    "$MINIO_BIN" server $urls \
      --address ":$p" \
      --console-address ":$cp" \
      >"$data_dir/minio_n${i}.log" 2>&1 &
    MINIO_PIDS+=($!)
    echo "[minio] node${i} started (port=$p pid=${MINIO_PIDS[-1]})"
  done

  # 첫 노드를 client endpoint로 사용 (모든 노드가 동일하게 처리 가능)
  MINIO_LEADER_URL="http://127.0.0.1:${base_port}"
  _minio_wait_ready "$MINIO_LEADER_URL"
}

minio_stop() {
  for pid in "${MINIO_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait "${MINIO_PIDS[@]}" 2>/dev/null || true
  MINIO_PIDS=()
}
