#!/usr/bin/env bash
# Compare GrainFS S3 performance against S3-compatible reference stores.
#
# Defaults run GrainFS single-node, MinIO, and RustFS when native binaries are
# available. External endpoints can be supplied with <TARGET>_URL variables.
#
# Examples:
#   ./benchmarks/bench_s3_compat_compare.sh
#   TARGETS=grainfs-single,minio WARP_DURATION=1m WARP_OBJ_SIZE=20MiB WARP_CONCURRENT=32 ./benchmarks/bench_s3_compat_compare.sh
#   TARGETS=grainfs-cluster WARP_OPS=put,get,delete ./benchmarks/bench_s3_compat_compare.sh
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
WARP_DELETE_BATCH="${WARP_DELETE_BATCH:-100}"
GRAINFS_CLUSTER_NODES="${GRAINFS_CLUSTER_NODES:-4}"
GRAINFS_ADMIN_DATA_DIR=""
GRAINFS_SA_ID=""
# Cluster start-up uses bench_wait_cluster_leader per node (replaces the
# legacy fixed CLUSTER_WARMUP_SLEEP). Multipart workloads additionally probe
# the multipart_listing_v1 capability via admin.sock after leader election.
case ",$WARP_OPS," in
  *,multipart,*|*,multipart-put,*)
    BENCH_MULTIPART_PROBE=1
    ;;
  *)
    BENCH_MULTIPART_PROBE=0
    ;;
esac
BENCH_PPROF="${BENCH_PPROF:-0}"
PPROF_BASE_PORT="${PPROF_BASE_PORT:-16060}"
PPROF_CPU_SECONDS="${PPROF_CPU_SECONDS:-30}"
GRAINFS_PPROF_PORTS=()

MINIO_BIN="${MINIO_BIN:-$(command -v minio 2>/dev/null || true)}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"

RUSTFS_BIN="${RUSTFS_BIN:-$(command -v rustfs 2>/dev/null || true)}"
RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_RPC_SECRET="${RUSTFS_RPC_SECRET:-grainfs-bench-rustfs-rpc-secret}"

if [[ -z "$WARP_BIN" ]]; then
  echo "[error] warp is required for S3-compatible comparison benchmarks. Install minio/warp or set WARP_BIN." >&2
  exit 1
fi

mkdir -p "$PROFILE_ROOT"
rm -rf "$BENCH_DIR"
mkdir -p "$BENCH_DIR"

PIDS=()
TARGET_DATA_DIRS=()
START_BASE_URL=""
START_ACCESS_KEY=""
START_SECRET_KEY=""
START_MODE=""
STOP_GRACE_SECONDS="${STOP_GRACE_SECONDS:-5}"
BACKENDS_STARTED=0

set_start_info() {
  START_BASE_URL="$1"
  START_ACCESS_KEY="$2"
  START_SECRET_KEY="$3"
  START_MODE="$4"
}

cleanup() {
  if [[ "$BACKENDS_STARTED" == "1" ]]; then
    echo "[bench] stopping comparison backends..."
  fi
  stop_pids "${PIDS[@]:-}"
  stop_child_backends
  if [[ "${KEEP_BENCH_DIR:-0}" != "1" ]]; then
    rm -rf "$BENCH_DIR" 2>/dev/null || true
  else
    echo "[bench] bench data dir saved to $BENCH_DIR"
  fi
}
trap cleanup EXIT INT TERM

stop_pids() {
  local pids=("$@")
  local live=()
  local pid deadline
  for pid in "${pids[@]:-}"; do
    [[ -n "$pid" ]] || continue
    kill "$pid" 2>/dev/null || true
  done
  deadline=$((SECONDS + STOP_GRACE_SECONDS))
  while (( SECONDS < deadline )); do
    live=()
    for pid in "${pids[@]:-}"; do
      [[ -n "$pid" ]] || continue
      if kill -0 "$pid" 2>/dev/null; then
        live+=("$pid")
      fi
    done
    ((${#live[@]} == 0)) && break
    sleep 0.2
  done
  for pid in "${live[@]:-}"; do
    kill -KILL "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}

stop_child_backends() {
  local children=()
  local pid
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && children+=("$pid")
  done < <(pgrep -P "$$" 2>/dev/null || true)
  ((${#children[@]} == 0)) || stop_pids "${children[@]}"
}

stop_target_backends() {
  local start_idx="$1"
  local i pid
  local target_pids=()
  for ((i=${#PIDS[@]} - 1; i >= start_idx; i--)); do
    pid="${PIDS[$i]}"
    target_pids+=("$pid")
    unset 'PIDS[$i]'
  done
  stop_pids "${target_pids[@]:-}"
  stop_child_backends
  PIDS=("${PIDS[@]:-}")
}

reset_target_resources() {
  TARGET_DATA_DIRS=()
}

register_target_data_dir() {
  TARGET_DATA_DIRS+=("$1")
}

collect_resource_snapshot() {
  local target="$1"
  local op="$2"
  local start_idx="$3"
  local out="$PROFILE_ROOT/resource-results.tsv"
  local target_pids=("${PIDS[@]:$start_idx}")
  local idx pid ps_out rss_kb cpu_pct rss_mib data_dir disk_mib

  ((${#target_pids[@]} > 0)) || return 0

  for idx in "${!target_pids[@]}"; do
    pid="${target_pids[$idx]}"
    [[ -n "$pid" ]] || continue
    ps_out="$(ps -o rss=,pcpu= -p "$pid" 2>/dev/null || true)"
    [[ -n "$ps_out" ]] || continue
    if ! read -r rss_kb cpu_pct <<<"$ps_out"; then
      continue
    fi
    [[ -n "${rss_kb:-}" && -n "${cpu_pct:-}" ]] || continue
    rss_mib="$(awk -v kb="$rss_kb" 'BEGIN { printf "%.2f", kb / 1024 }' || true)"
    data_dir="${TARGET_DATA_DIRS[$idx]:-}"
    disk_mib=""
    if [[ -n "$data_dir" && -d "$data_dir" ]]; then
      disk_mib="$(du -sk "$data_dir" 2>/dev/null | awk '{ printf "%.2f", $1 / 1024 }' || true)"
    fi
    printf '%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n' \
      "$target" "$op" "$((idx + 1))" "$pid" "$rss_mib" "$cpu_pct" "${disk_mib:-0}" "$data_dir" >>"$out"
  done
}

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
  local extra=()
  local extra_flags=()
  port="$(bench_free_port)"
  mkdir -p "$data_dir"
  register_target_data_dir "$data_dir"
  BENCH_ENCRYPTION_KEY_FILE="$data_dir/encryption.key"
  export BENCH_ENCRYPTION_KEY_FILE
  bench_generate_encryption_key_file "$BENCH_ENCRYPTION_KEY_FILE"
  if [[ "$BENCH_PPROF" == "1" ]]; then
    GRAINFS_PPROF_PORTS=("$PPROF_BASE_PORT")
    extra+=(--pprof-port "$PPROF_BASE_PORT")
  fi
  if [[ -n "${EXTRA_GRAINFS_SERVE_FLAGS:-}" ]]; then
    read -r -a extra_flags <<<"$EXTRA_GRAINFS_SERVE_FLAGS"
  fi

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
    "${extra[@]}" \
    "${extra_flags[@]}" \
    >"$PROFILE_ROOT/grainfs-single.log" 2>&1 &
  PIDS+=($!)
  bench_wait_tcp_port "127.0.0.1" "$port" "grainfs-single S3" 180 0.2 >&2
  if [[ "$BENCH_PPROF" == "1" ]]; then
    bench_wait_tcp_port "127.0.0.1" "$PPROF_BASE_PORT" "grainfs-single pprof" 60 0.2 >&2
  fi
  bench_bootstrap_iam_credentials "$BINARY" "$data_dir" "bench-s3-compat" >&2
  GRAINFS_ADMIN_DATA_DIR="$data_dir"
  GRAINFS_SA_ID="$SA_ID"
  set_start_info "http://127.0.0.1:$port" "$ACCESS_KEY" "$SECRET_KEY" "local"
}

start_grainfs_cluster() {
  if [[ "${GRAINFS_CLUSTER_URL:-}" != "" ]]; then
    set_start_info "$GRAINFS_CLUSTER_URL" "${GRAINFS_ACCESS_KEY:-}" "${GRAINFS_SECRET_KEY:-}" "external"
    return 0
  fi

  if [[ "$GRAINFS_CLUSTER_NODES" -lt 2 ]]; then
    echo "[error] GRAINFS_CLUSTER_NODES must be >= 2" >&2
    exit 1
  fi
  if [[ "${NO_BUILD:-0}" != "1" ]]; then
    echo "[bench] building grainfs..." >&2
    make build >&2
  fi
  bench_require_binary "$BINARY"

  local cluster_dir="$BENCH_DIR/gfc"
  local enc_key_file="$cluster_dir/encryption.key"
  local http_ports=()
  local raft_ports=()
  local pprof_ports=()
  local urls=()
  local idx
  mkdir -p "$cluster_dir"
  for idx in $(seq 1 "$GRAINFS_CLUSTER_NODES"); do
    mkdir -p "$cluster_dir/n${idx}"
    register_target_data_dir "$cluster_dir/n${idx}"
    http_ports+=("$(bench_free_port)")
    raft_ports+=("$(bench_free_port)")
    if [[ "$BENCH_PPROF" == "1" ]]; then
      pprof_ports+=("$((PPROF_BASE_PORT + idx - 1))")
    fi
  done
  if [[ "$BENCH_PPROF" == "1" ]]; then
    GRAINFS_PPROF_PORTS=("${pprof_ports[@]}")
  fi
  BENCH_ENCRYPTION_KEY_FILE="$enc_key_file"
  export BENCH_ENCRYPTION_KEY_FILE
  bench_generate_encryption_key_file "$BENCH_ENCRYPTION_KEY_FILE"

  start_grainfs_cluster_node() {
    local node_idx="$1"
    local zero_idx=$((node_idx - 1))
    local extra=()
    if [[ "$BENCH_PPROF" == "1" ]]; then
      extra+=(--pprof-port "${pprof_ports[$zero_idx]}")
    fi
    local extra_flags=()
    if [[ -n "${EXTRA_GRAINFS_SERVE_FLAGS:-}" ]]; then
      read -r -a extra_flags <<<"$EXTRA_GRAINFS_SERVE_FLAGS"
    fi
    "$BINARY" serve \
      --data "$cluster_dir/n${node_idx}" \
      --port "${http_ports[$zero_idx]}" \
      --node-id "bench-node-${node_idx}" \
      --raft-addr "127.0.0.1:${raft_ports[$zero_idx]}" \
      --cluster-key "bench-s3-compat-cluster-key" \
      $(bench_encryption_args) \
      --nfs4-port 0 \
      --nbd-port 0 \
      --scrub-interval 0 \
      --lifecycle-interval 0 \
      --log-level warn \
      "${extra[@]}" \
      "${extra_flags[@]}" \
      >"$PROFILE_ROOT/grainfs-cluster-node-${node_idx}.log" 2>&1 &
    PIDS+=($!)
    bench_wait_tcp_port "127.0.0.1" "${http_ports[$zero_idx]}" "grainfs-cluster node${node_idx} S3" 180 0.2 >&2
    if [[ "$BENCH_PPROF" == "1" ]]; then
      bench_wait_tcp_port "127.0.0.1" "${pprof_ports[$zero_idx]}" "grainfs-cluster node${node_idx} pprof" 60 0.2 >&2
    fi
  }

  start_grainfs_cluster_node 1
  local kek_file="$cluster_dir/n1/kek.key"
  bench_wait_file "$kek_file" "grainfs-cluster node1 KEK" 100 0.2 >&2
  bench_bootstrap_iam_credentials "$BINARY" "$cluster_dir/n1" "bench-s3-compat-cluster" >&2
  for idx in $(seq 2 "$GRAINFS_CLUSTER_NODES"); do
    cp "$kek_file" "$cluster_dir/n${idx}/kek.key"
    chmod 600 "$cluster_dir/n${idx}/kek.key"
    printf '%s' "127.0.0.1:${raft_ports[0]}" >"$cluster_dir/n${idx}/.join-pending"
    chmod 600 "$cluster_dir/n${idx}/.join-pending"
    start_grainfs_cluster_node "$idx"
  done

  for idx in $(seq 1 "$GRAINFS_CLUSTER_NODES"); do
    urls+=("http://127.0.0.1:${http_ports[$((idx - 1))]}")
  done
  # Replace the fixed CLUSTER_WARMUP_SLEEP with an explicit leader probe on
  # node-1 — it bootstraps the meta raft group, so once /api/cluster/status
  # reports state="Leader" the join handshake is far enough along for warp
  # to issue S3 calls (data-group leaders elect on first write). 120 attempts
  # × 0.25s = 30s budget; the legacy fixed 45s sleep is gone. Followers
  # report state="Follower" on the same endpoint, so we don't poll them.
  bench_wait_cluster_leader "${urls[0]}" 120 0.25 >&2
  GRAINFS_ADMIN_DATA_DIR="$cluster_dir/n1"
  GRAINFS_SA_ID="$SA_ID"
  set_start_info "$(IFS=','; echo "${urls[*]}")" "$ACCESS_KEY" "$SECRET_KEY" "local"

  if [[ "${BENCH_MULTIPART_PROBE:-0}" == "1" ]]; then
    local admin_socks=()
    for idx in $(seq 1 "$GRAINFS_CLUSTER_NODES"); do
      admin_socks+=("$cluster_dir/n${idx}/admin.sock")
    done
    bench_wait_capability_ready "$(IFS=':'; echo "${admin_socks[*]}")" "multipart_listing_v1" 120 0.5 >&2 || {
      echo "[warn] multipart capability probe failed; warp may report 503 'rolling upgrade'" >&2
    }
  fi
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
  register_target_data_dir "$data_dir"

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

start_minio_cluster() {
  if [[ "${MINIO_CLUSTER_URL:-}" != "" ]]; then
    set_start_info "$MINIO_CLUSTER_URL" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" "external"
    return 0
  fi
  if [[ -z "$MINIO_BIN" ]]; then
    return 1
  fi
  if "$MINIO_BIN" --version 2>/dev/null | grep -qi 'AIStor'; then
    echo "minio-cluster: skipped; installed binary is MinIO AIStor and may deny S3 operations without a license." >>"$PROFILE_ROOT/skipped.txt"
    return 1
  fi

  local nodes="${MINIO_CLUSTER_NODES:-4}"
  if [[ "$nodes" -lt 4 ]]; then
    echo "minio-cluster: requires >= 4 nodes (MinIO distributed minimum); got $nodes" >&2
    return 1
  fi

  local base_dir="$BENCH_DIR/minio-cluster"
  mkdir -p "$base_dir"
  local ports=()
  local console_ports=()
  local volume_specs=()
  local idx
  for idx in $(seq 1 "$nodes"); do
    mkdir -p "$base_dir/n${idx}"
    register_target_data_dir "$base_dir/n${idx}"
    ports+=("$(bench_free_port)")
    console_ports+=("$(bench_free_port)")
  done
  # MinIO distributed mode requires every node to be addressed via the SAME
  # URL list, including its own. Each node binds to its --address and connects
  # out to the other peers; matching ports against the URL list lets MinIO
  # detect which slot is local.
  for idx in $(seq 1 "$nodes"); do
    volume_specs+=("http://127.0.0.1:${ports[$((idx - 1))]}${base_dir}/n${idx}")
  done

  local urls=()
  for idx in $(seq 1 "$nodes"); do
    local zero_idx=$((idx - 1))
    MINIO_CI_CD="${MINIO_CI_CD:-1}" \
    MINIO_ROOT_USER="$MINIO_ACCESS_KEY" \
    MINIO_ROOT_PASSWORD="$MINIO_SECRET_KEY" \
    "$MINIO_BIN" server \
      --address "127.0.0.1:${ports[$zero_idx]}" \
      --console-address "127.0.0.1:${console_ports[$zero_idx]}" \
      "${volume_specs[@]}" \
      >"$PROFILE_ROOT/minio-cluster-node-${idx}.log" 2>&1 &
    PIDS+=($!)
    urls+=("http://127.0.0.1:${ports[$zero_idx]}")
  done
  # MinIO needs every node to be reachable before completing initialisation —
  # so the per-node TCP wait races against peer dial-back. Wait for every
  # node's port to be open, then poll /minio/health/cluster on node 1 (which
  # only flips to 200 once the cluster quorum has actually formed).
  sleep "${MINIO_CLUSTER_WARMUP_SLEEP:-3}"
  for idx in $(seq 1 "$nodes"); do
    bench_wait_tcp_port "127.0.0.1" "${ports[$((idx - 1))]}" "minio-cluster node${idx} port" 180 0.2 >&2
  done
  local ready_url="http://127.0.0.1:${ports[0]}/minio/health/cluster"
  local ready_attempts="${MINIO_CLUSTER_READY_ATTEMPTS:-600}"
  local ready_sleep="${MINIO_CLUSTER_READY_SLEEP:-0.5}"
  echo "  waiting for minio-cluster S3 cluster-ready..."
  local ready=0
  for _ in $(seq 1 "$ready_attempts"); do
    local code
    code="$(curl -s -m 2 -o /dev/null -w '%{http_code}' "$ready_url" 2>/dev/null || echo 000)"
    if [[ "$code" == "200" ]]; then
      ready=1
      break
    fi
    sleep "$ready_sleep"
  done
  if [[ "$ready" != "1" ]]; then
    echo "  minio-cluster never reported /minio/health/cluster as 200; aborting" >&2
    return 1
  fi
  echo "  minio-cluster S3 cluster-ready"
  echo "  waiting for minio-cluster signed write readiness..."
  bench_wait_s3_signed_write_ready "$(IFS=','; echo "${urls[*]}")" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" "warp-minio-cluster-ready" "${MINIO_CLUSTER_WRITE_READY_ATTEMPTS:-120}" "${MINIO_CLUSTER_WRITE_READY_SLEEP:-0.5}" >&2 || {
    echo "  minio-cluster signed write readiness failed; aborting" >&2
    return 1
  }
  echo "  minio-cluster signed write-ready"
  set_start_info "$(IFS=','; echo "${urls[*]}")" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" "local"
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
  register_target_data_dir "$data_dir"

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

start_rustfs_cluster() {
  if [[ "${RUSTFS_CLUSTER_URL:-}" != "" ]]; then
    set_start_info "$RUSTFS_CLUSTER_URL" "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY" "external"
    return 0
  fi
  if [[ -z "$RUSTFS_BIN" ]]; then
    return 1
  fi

  local nodes="${RUSTFS_CLUSTER_NODES:-4}"
  if [[ "$nodes" -lt 4 ]]; then
    echo "rustfs-cluster: requires >= 4 nodes; got $nodes" >&2
    return 1
  fi

  local base_dir="$BENCH_DIR/rustfs-cluster"
  mkdir -p "$base_dir"
  local ports=()
  local console_ports=()
  local volume_specs=()
  local idx
  for idx in $(seq 1 "$nodes"); do
    mkdir -p "$base_dir/n${idx}"
    register_target_data_dir "$base_dir/n${idx}"
    ports+=("$(bench_free_port)")
    console_ports+=("$(bench_free_port)")
  done
  for idx in $(seq 1 "$nodes"); do
    volume_specs+=("http://127.0.0.1:${ports[$((idx - 1))]}${base_dir}/n${idx}")
  done

  local urls=()
  for idx in $(seq 1 "$nodes"); do
    local zero_idx=$((idx - 1))
    RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
    RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
    RUSTFS_RPC_SECRET="$RUSTFS_RPC_SECRET" \
    RUSTFS_REGION="us-east-1" \
    "$RUSTFS_BIN" server \
      --address "127.0.0.1:${ports[$zero_idx]}" \
      --console-enable \
      --console-address "127.0.0.1:${console_ports[$zero_idx]}" \
      "${volume_specs[@]}" \
      >"$PROFILE_ROOT/rustfs-cluster-node-${idx}.log" 2>&1 &
    PIDS+=($!)
    urls+=("http://127.0.0.1:${ports[$zero_idx]}")
  done

  for idx in $(seq 1 "$nodes"); do
    bench_wait_tcp_port "127.0.0.1" "${ports[$((idx - 1))]}" "rustfs-cluster node${idx} S3" 180 0.2 >&2
  done
  sleep "${RUSTFS_CLUSTER_WARMUP_SLEEP:-5}"
  set_start_info "$(IFS=','; echo "${urls[*]}")" "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY" "local"
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
    echo "- delete batch: ${WARP_DELETE_BATCH}"
    echo "- duration: ${WARP_DURATION}"
    echo "- concurrency: ${WARP_CONCURRENT}"
    echo "- noclear: ${WARP_NOCLEAR}"
    echo "- host select: ${WARP_HOST_SELECT}"
    echo "- raw artifacts: ${PROFILE_ROOT}"
    echo "- host preflight: ${PROFILE_ROOT}/host-preflight.txt"
    echo
    echo "> Method: all targets use signed S3 requests through warp, identical object size, concurrency, duration, and bucket lookup mode. PUT, GET, and DELETE are reported separately. With WARP_NOCLEAR=1, GET measures a warm-read pass over the objects written by the preceding PUT pass; DELETE uses warp's batch delete workload."
    echo
    echo "> Caveat: GrainFS runs with at-rest encryption. Local MinIO/RustFS single-node targets use their default single-node durability; local \`*-cluster\` targets boot 4-node distributed clusters unless overridden."
    bench_print_host_preflight_warnings
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
    --bucket "warp-${target}-${op}" \
    --duration "$WARP_DURATION" \
    --concurrent "$WARP_CONCURRENT" \
    --lookup path
    --host-select "$WARP_HOST_SELECT"
    --benchdata "$out_dir/warp.data"
  )
  case "$op" in
    multipart|multipart-put)
      args+=(--part.size "${WARP_PART_SIZE:-5MiB}")
      ;;
    *)
      args+=(--obj.size "$WARP_OBJ_SIZE")
      ;;
  esac
  local objects="$WARP_OBJECTS"
  if [[ "$op" == "delete" ]]; then
    # warp delete pre-uploads --objects, deletes them in --batch chunks, then
    # analyzes per-segment throughput (default 1s segments). With concurrent=16
    # and batch=100 the previous floor (concurrent * batch * 4 = 6400) finished
    # in roughly 1-2s on local-disk packblob and warp reported "Skipping DELETE
    # too few samples." The 16× floor (≈25600 objects, matching warp's own
    # default) keeps the delete phase busy long enough for analyze to bucket
    # meaningful samples without bloating the pre-upload phase past a few
    # seconds even at 64KiB objects.
    local need=$((WARP_CONCURRENT * WARP_DELETE_BATCH * 16))
    if (( objects < need )); then
      objects="$need"
    fi
  fi
  case "$op" in
    get|delete|list|stat|versioned|retention|mixed)
      args+=(--objects "$objects")
      ;;
  esac
  if [[ "$op" == "delete" ]]; then
    args+=(--batch "$WARP_DELETE_BATCH")
  fi
  if [[ "$WARP_NOCLEAR" == "1" ]]; then
    args+=(--noclear)
  fi
  if ! "$WARP_BIN" "${args[@]}" >"$out_dir/warp.out" 2>&1; then
    RUN_FAILURES=1
    echo "warp-$op: non-zero exit for $target; see $out_dir/warp.out" | tee -a "$PROFILE_ROOT/skipped.txt"
  fi

  if [[ ! -f "$data_file" ]]; then
    RUN_FAILURES=1
    echo "warp-$op: missing benchdata for $target; see $out_dir/warp.out" | tee -a "$PROFILE_ROOT/skipped.txt"
    return 0
  fi

  if ! "$WARP_BIN" analyze "$data_file" >"$out_dir/analyze.out" 2>&1; then
    RUN_FAILURES=1
    echo "warp-$op: analyze failed for $target; see $out_dir/analyze.out" | tee -a "$PROFILE_ROOT/skipped.txt"
    return 0
  fi

  if ! python3 - "$target" "$START_MODE" "$op" "$out_dir/analyze.out" "$PROFILE_ROOT/warp-results.tsv" "$out_dir" "$data_file" <<'PY'
import json
import re
import subprocess
import sys

target, mode, op, analyze_path, out_path, artifact_dir, data_path = sys.argv[1:]
text = open(analyze_path, encoding="utf-8").read()
avg = re.search(r"Average:\s+([0-9.]+)\s+MiB/s,\s+([0-9.]+)\s+obj/s", text)
obj_only = re.search(r"Average:\s+([0-9.]+)\s+obj/s", text)
err = re.search(r"Errors:\s+([0-9]+)", text)
if avg:
    mib = float(avg.group(1))
    obj_s = float(avg.group(2))
    errors = int(err.group(1)) if err else 0
elif obj_only:
    mib = 0.0
    obj_s = float(obj_only.group(1))
    errors = int(err.group(1)) if err else 0
elif op == "delete":
    raw = subprocess.check_output(["zstdcat", data_path], text=True)
    data = json.loads(raw)
    delete = data.get("by_op_type", {}).get("DELETE", {})
    hosts = delete.get("throughput_by_host", {})
    objects = sum(float(v.get("objects", 0)) for v in hosts.values())
    errors = sum(int(v.get("errors", 0)) for v in hosts.values())
    millis = sum(float(v.get("measure_duration_millis", 0)) for v in hosts.values())
    if millis <= 0:
        sys.exit("missing DELETE duration")
    mib = 0.0
    obj_s = objects / (millis / 1000.0)
else:
    sys.exit("missing Average line")
if errors > 0:
    sys.exit(f"non-zero warp errors: {errors}")
row = [target, mode, op, f"{mib:.2f}", f"{obj_s:.2f}", str(errors), artifact_dir]
with open(out_path, "a", encoding="utf-8") as f:
    f.write("\t".join(row) + "\n")
PY
  then
    RUN_FAILURES=1
    echo "warp-$op: analyze result not publishable for $target; see $out_dir/analyze.out" | tee -a "$PROFILE_ROOT/skipped.txt"
  fi
}

prepare_grainfs_warp_bucket() {
  local target="$1"
  local op="$2"

  case "$target" in
    grainfs-single|grainfs-cluster)
      if [[ -n "$GRAINFS_ADMIN_DATA_DIR" ]]; then
        bench_create_bucket_with_policy_admin_retry "$BINARY" "$GRAINFS_ADMIN_DATA_DIR" "warp-${target}-${op}" "$GRAINFS_SA_ID" bucket-admin
      fi
      ;;
  esac
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

def baseline(case, base, target):
    if target.endswith("-cluster"):
        return case.get(f"{base}-cluster", case.get(base, 0))
    return case.get(base, 0)

with open(summary_path, "a") as out:
    for row in rows:
        target, mode, op, mib, objs, errors, artifact_dir = row
        throughput = float(mib)
        case = by_case[op]
        minio = baseline(case, "minio", target)
        rustfs = baseline(case, "rustfs", target)
        out.write(
            f"| {target} | {mode} | {op.upper()} | {throughput:.2f} | {float(objs):.2f} | "
            f"{int(errors)} | {ratio(throughput, minio)} | {ratio(throughput, rustfs)} | `{artifact_dir}` |\n"
        )
PY
}

append_resource_summary() {
  local summary="$PROFILE_ROOT/summary.md"
  python3 - "$PROFILE_ROOT/resource-results.tsv" "$summary" <<'PY'
import statistics
import sys
from collections import defaultdict

results_path, summary_path = sys.argv[1:]
groups = defaultdict(lambda: {"rss": [], "cpu": [], "disk": []})

try:
    with open(results_path, encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 8:
                continue
            target, op, _, _, rss, cpu, disk, _ = parts
            key = (target, op)
            groups[key]["rss"].append(float(rss))
            groups[key]["cpu"].append(float(cpu))
            groups[key]["disk"].append(float(disk))
except FileNotFoundError:
    groups = {}

def avg(values):
    return sum(values) / len(values) if values else 0.0

def skew(values):
    if not values:
        return ""
    med = statistics.median(values)
    if med <= 0:
        return ""
    return f"{max(values) / med:.2f}x"

with open(summary_path, "a", encoding="utf-8") as out:
    out.write("\n## Resource Snapshot\n\n")
    out.write("| target | op | nodes | avg RSS MiB | max RSS MiB | RSS skew | avg CPU % | max CPU % | CPU skew | avg disk MiB | max disk MiB | disk skew |\n")
    out.write("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
    for target, op in sorted(groups):
        data = groups[(target, op)]
        rss = data["rss"]
        cpu = data["cpu"]
        disk = data["disk"]
        out.write(
            f"| {target} | {op.upper()} | {len(rss)} | "
            f"{avg(rss):.2f} | {max(rss) if rss else 0.0:.2f} | {skew(rss)} | "
            f"{avg(cpu):.2f} | {max(cpu) if cpu else 0.0:.2f} | {skew(cpu)} | "
            f"{avg(disk):.2f} | {max(disk) if disk else 0.0:.2f} | {skew(disk)} |\n"
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

: >"$PROFILE_ROOT/warp-results.tsv"
: >"$PROFILE_ROOT/resource-results.tsv"
: >"$PROFILE_ROOT/skipped.txt"
bench_collect_host_preflight "$PROFILE_ROOT"
bench_enforce_strict_host "$PROFILE_ROOT"
write_summary_header
RUN_FAILURES=0
IFS=',' read -ra WARP_OP_LIST <<<"$WARP_OPS"

for target in grainfs-single grainfs-cluster minio minio-cluster rustfs rustfs-cluster; do
  target_enabled "$target" || continue
  target_pid_start="${#PIDS[@]}"
  reset_target_resources

  case "$target" in
    grainfs-single)
      start_grainfs_single
      ;;
    grainfs-cluster)
      start_grainfs_cluster
      ;;
    minio)
      if ! start_minio; then
        echo "minio: skipped; set MINIO_BIN or MINIO_URL" | tee -a "$PROFILE_ROOT/skipped.txt"
        continue
      fi
      ;;
    minio-cluster)
      if ! start_minio_cluster; then
        echo "minio-cluster: skipped; set MINIO_BIN or MINIO_CLUSTER_URL" | tee -a "$PROFILE_ROOT/skipped.txt"
        stop_target_backends "$target_pid_start"
        continue
      fi
      ;;
    rustfs)
      if ! start_rustfs; then
        echo "rustfs: skipped; set RUSTFS_BIN or RUSTFS_URL" | tee -a "$PROFILE_ROOT/skipped.txt"
        continue
      fi
      ;;
    rustfs-cluster)
      if ! start_rustfs_cluster; then
        echo "rustfs-cluster: skipped; set RUSTFS_BIN or RUSTFS_CLUSTER_URL" | tee -a "$PROFILE_ROOT/skipped.txt"
        stop_target_backends "$target_pid_start"
        continue
      fi
      ;;
  esac

  base_url="$START_BASE_URL"
  access_key="$START_ACCESS_KEY"
  secret_key="$START_SECRET_KEY"
  mode="$START_MODE"
  BACKENDS_STARTED=1
  for op in "${WARP_OP_LIST[@]}"; do
    case "$op" in
      get|put|delete|mixed|list|stat|versioned|retention|multipart|multipart-put|append)
        prepare_grainfs_warp_bucket "$target" "$op"
        if [[ "$target" == grainfs-* && -n "$GRAINFS_ADMIN_DATA_DIR" ]]; then
          bench_wait_s3_bucket_auth_ready "$base_url" "$access_key" "$secret_key" "warp-${target}-${op}" "${BENCH_S3_AUTH_READY_ATTEMPTS:-120}" "${BENCH_S3_AUTH_READY_SLEEP:-0.25}"
        fi
        if [[ "$BENCH_PPROF" == "1" && "$target" == grainfs-* && ${#GRAINFS_PPROF_PORTS[@]} -gt 0 ]]; then
          cpu_dir="$PROFILE_ROOT/$target/pprof-cpu-$op"
          mkdir -p "$cpu_dir"
          cpu_pids=()
          for i in "${!GRAINFS_PPROF_PORTS[@]}"; do
            ( curl -sf "http://127.0.0.1:${GRAINFS_PPROF_PORTS[$i]}/debug/pprof/profile?seconds=${PPROF_CPU_SECONDS}" \
                -o "$cpu_dir/cpu-node$((i+1)).pb.gz" \
                || echo "[pprof] CPU capture failed for node$((i+1)) ($op)" >&2 ) &
            cpu_pids+=($!)
          done
          run_warp_case "$target" "$base_url" "$access_key" "$secret_key" "$op"
          wait "${cpu_pids[@]}" 2>/dev/null || true
          echo "[pprof] CPU profiles for $op saved to $cpu_dir"
        else
          run_warp_case "$target" "$base_url" "$access_key" "$secret_key" "$op"
        fi
        collect_resource_snapshot "$target" "$op" "$target_pid_start"
        ;;
      *)
        echo "[error] unknown WARP_OPS entry: $op" >&2
        exit 1
        ;;
    esac
  done

  if [[ "$BENCH_PPROF" == "1" && "$target" == grainfs-* && ${#GRAINFS_PPROF_PORTS[@]} -gt 0 ]]; then
    for i in "${!GRAINFS_PPROF_PORTS[@]}"; do
      snap_dir="$PROFILE_ROOT/$target/pprof-snap/node$((i+1))"
      mkdir -p "$snap_dir"
      echo "[pprof] collecting node$((i+1)) heap/allocs/goroutine/mutex/block snapshots..."
      bench_collect_pprof "${GRAINFS_PPROF_PORTS[$i]}" "$snap_dir" heap allocs goroutine mutex block
    done
  fi

  stop_target_backends "$target_pid_start"
done

append_summary_rows
append_resource_summary

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
if (( RUN_FAILURES != 0 )); then
  exit 1
fi
