#!/usr/bin/env bash
# lib_grainfs.sh — sourced by bench_compare.sh
# GrainFS single / cluster 기동 헬퍼.
#
# Provides:
#   grainfs_start_single  <data_dir> <port> [pprof_port]
#   grainfs_start_cluster <data_dir> <nodes> <base_port> <base_raft_port> <ec_data> <ec_parity> [pprof_base]
#   grainfs_stop
#   GRAINFS_PIDS, GRAINFS_LEADER_URL, GRAINFS_LEADER_PPROF, GRAINFS_ACCESS_KEY, GRAINFS_SECRET_KEY

GRAINFS_PIDS=()
GRAINFS_LEADER_URL=""
GRAINFS_LEADER_PPROF=""
GRAINFS_ACCESS_KEY="${GRAINFS_ACCESS_KEY:-test}"
GRAINFS_SECRET_KEY="${GRAINFS_SECRET_KEY:-testtest}"
GRAINFS_BIN="${GRAINFS_BIN:-./bin/grainfs}"

_grainfs_wait_port() {
  local port="$1"; local deadline=$(( $(date +%s) + 30 ))
  while ! nc -z 127.0.0.1 "$port" 2>/dev/null; do
    if (( $(date +%s) > deadline )); then
      echo "[error] grainfs port $port not open within 30s" >&2
      return 1
    fi
    sleep 0.3
  done
}

grainfs_start_single() {
  local data_dir="$1"; local port="$2"; local pprof_port="${3:-0}"
  rm -rf "$data_dir"; mkdir -p "$data_dir"

  local extra=""
  [[ "$pprof_port" != "0" ]] && extra="--pprof-port $pprof_port"

  "$GRAINFS_BIN" serve \
    --data "$data_dir" \
    --port "$port" \
    --access-key "$GRAINFS_ACCESS_KEY" \
    --secret-key "$GRAINFS_SECRET_KEY" \
    --no-encryption \
    --rate-limit-ip-rps 1000000 --rate-limit-ip-burst 1000000 \
    --rate-limit-user-rps 1000000 --rate-limit-user-burst 1000000 \
    $extra \
    >"$data_dir/grainfs.log" 2>&1 &
  GRAINFS_PIDS+=($!)
  GRAINFS_LEADER_URL="http://127.0.0.1:${port}"
  GRAINFS_LEADER_PPROF="$pprof_port"
  echo "[grainfs] single started (port=$port pid=${GRAINFS_PIDS[-1]})"
  _grainfs_wait_port "$port"
  sleep 1
}

grainfs_start_cluster() {
  local data_dir="$1"; local nodes="$2"; local base_port="$3"; local base_raft="$4"
  local ec_data="$5"; local ec_parity="$6"; local pprof_base="${7:-0}"

  rm -rf "$data_dir"; mkdir -p "$data_dir"

  # 모든 노드의 raft addrs 작성
  local raft_addrs=()
  for ((i=1; i<=nodes; i++)); do
    raft_addrs+=("127.0.0.1:$((base_raft + i - 1))")
  done

  for ((i=1; i<=nodes; i++)); do
    mkdir -p "$data_dir/n${i}"
    local p=$((base_port + i - 1))
    local rp=$((base_raft + i - 1))
    # peers = 자기 제외 모든 raft 주소
    local peers=""
    for addr in "${raft_addrs[@]}"; do
      if [[ "$addr" != "127.0.0.1:${rp}" ]]; then
        peers+="${addr},"
      fi
    done
    peers="${peers%,}"

    local extra="--ec-data $ec_data --ec-parity $ec_parity"
    if [[ "$pprof_base" != "0" ]]; then
      extra+=" --pprof-port $((pprof_base + i - 1))"
    fi

    "$GRAINFS_BIN" serve \
      --data "$data_dir/n${i}" \
      --port "$p" \
      --raft-addr "127.0.0.1:${rp}" \
      --peers "$peers" \
      --cluster-key "bench-compare-key" \
      --access-key "$GRAINFS_ACCESS_KEY" \
      --secret-key "$GRAINFS_SECRET_KEY" \
      --no-encryption \
      --rate-limit-ip-rps 1000000 --rate-limit-ip-burst 1000000 \
      --rate-limit-user-rps 1000000 --rate-limit-user-burst 1000000 \
      --raft-log-fsync=false \
      $extra \
      >"$data_dir/grainfs_n${i}.log" 2>&1 &
    GRAINFS_PIDS+=($!)
    echo "[grainfs] node${i} started (s3=:$p raft=:$rp pid=${GRAINFS_PIDS[-1]})"
  done

  # 모든 노드의 S3 포트가 열릴 때까지 대기
  for ((i=1; i<=nodes; i++)); do
    _grainfs_wait_port "$((base_port + i - 1))"
  done

  # Raft 리더 선출 대기
  echo "[grainfs] waiting for raft leader…"
  local deadline=$(( $(date +%s) + 60 ))
  while true; do
    if (( $(date +%s) > deadline )); then
      echo "[error] no Raft leader elected within 60s" >&2
      return 1
    fi
    for ((i=1; i<=nodes; i++)); do
      local p=$((base_port + i - 1))
      local status
      status=$(curl -sf "http://127.0.0.1:${p}/api/cluster/status" 2>/dev/null) || continue
      local node_id leader_id
      node_id=$(echo "$status" | grep -o '"node_id":"[^"]*"' | cut -d'"' -f4)
      leader_id=$(echo "$status" | grep -o '"leader_id":"[^"]*"' | cut -d'"' -f4)
      if [[ -n "$leader_id" && "$node_id" == "$leader_id" ]]; then
        GRAINFS_LEADER_URL="http://127.0.0.1:${p}"
        if [[ "$pprof_base" != "0" ]]; then
          GRAINFS_LEADER_PPROF=$((pprof_base + i - 1))
        fi
        echo "[grainfs] leader on port $p"
        return 0
      fi
    done
    sleep 0.5
  done
}

grainfs_stop() {
  for pid in "${GRAINFS_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait "${GRAINFS_PIDS[@]}" 2>/dev/null || true
  GRAINFS_PIDS=()
}
