#!/usr/bin/env bash

BENCH_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARKS_DIR="$(cd "$BENCH_LIB_DIR/.." && pwd)"
REPO_ROOT="$(cd "$BENCHMARKS_DIR/.." && pwd)"

bench_free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()'
}

bench_require_command() {
  local cmd="$1"
  local install_hint="${2:-}"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    if [[ -n "$install_hint" ]]; then
      echo "[error] $cmd not found. Install: $install_hint" >&2
    else
      echo "[error] $cmd not found" >&2
    fi
    exit 1
  fi
}

bench_require_binary() {
  local binary="$1"
  if [[ ! -x "$binary" ]]; then
    echo "binary not found: $binary  (run: make build)" >&2
    exit 1
  fi
}

bench_require_colima() {
  if ! colima status >/dev/null 2>&1; then
    echo "colima not running - start with: colima start" >&2
    exit 1
  fi
}

bench_colima_ssh() {
  colima ssh -- "$@"
}

bench_wait_http_ready() {
  local url="$1"
  local label="${2:-HTTP health}"
  local attempts="${3:-50}"
  local sleep_seconds="${4:-0.2}"

  # Accept any HTTP response code as "server is up". The S3 root path now
  # returns 403 once IAM is in scope (commit e4cfbb2 / PR #237), so a strict
  # `curl -sf` against `/` would never return success even though the server
  # is fully reachable. The bench scripts only need a TCP-up + HTTP-handler
  # signal here, not an authenticated 200.
  echo "  waiting for $label..."
  for _ in $(seq 1 "$attempts"); do
    local code
    code=$(curl -s -o /dev/null -m 2 -w '%{http_code}' "$url" 2>/dev/null || echo "000")
    if [[ "$code" != "000" ]]; then
      echo "  $label ready (HTTP $code)"
      return 0
    fi
    sleep "$sleep_seconds"
  done

  echo "$label did not become ready: $url" >&2
  return 1
}

bench_wait_tcp_port() {
  local host="$1"
  local port="$2"
  local label="${3:-TCP port}"
  local attempts="${4:-50}"
  local sleep_seconds="${5:-0.2}"

  echo "  waiting for $label..."
  for _ in $(seq 1 "$attempts"); do
    if nc -z "$host" "$port" 2>/dev/null; then
      echo "  $label ready"
      return 0
    fi
    sleep "$sleep_seconds"
  done

  echo "$label did not become ready: $host:$port" >&2
  return 1
}

bench_encryption_args() {
  if [[ "${NO_ENCRYPTION:-0}" == "1" ]]; then
    echo "[error] encryption is mandatory; do not set NO_ENCRYPTION=1" >&2
    return 1
  elif [[ -n "${BENCH_ENCRYPTION_KEY_FILE:-}" ]]; then
    printf '%s\n' "--encryption-key-file"
    printf '%s\n' "$BENCH_ENCRYPTION_KEY_FILE"
  fi
}

bench_generate_encryption_key_file() {
  local key_file="$1"

  if [[ "${NO_ENCRYPTION:-0}" == "1" ]]; then
    echo "[error] encryption is mandatory; do not set NO_ENCRYPTION=1" >&2
    return 1
  fi

  python3 - "$key_file" <<'PY'
import os
import sys

path = sys.argv[1]
parent = os.path.dirname(path)
if parent:
    os.makedirs(parent, exist_ok=True)
with open(path, "wb") as f:
    f.write(os.urandom(32))
PY
  chmod 600 "$key_file"
}

bench_copy_node_logs() {
  local src_dir="$1"
  local dst_dir="$2"

  [[ -d "$src_dir" && -n "$dst_dir" ]] || return 0

  local log_dir="$dst_dir/logs"
  mkdir -p "$log_dir"

  local copied=0
  local f
  shopt -s nullglob
  for f in "$src_dir"/*.log; do
    cp "$f" "$log_dir/"
    copied=1
  done
  shopt -u nullglob

  if [[ "$copied" == "1" ]]; then
    echo "  node logs saved to $log_dir"
  fi
}

bench_wait_admin_socket() {
  local data_dir="$1"
  local attempts="${2:-100}"
  local sleep_seconds="${3:-0.2}"
  local admin_sock="${data_dir}/admin.sock"

  for _ in $(seq 1 "$attempts"); do
    [[ -S "$admin_sock" ]] && return 0
    sleep "$sleep_seconds"
  done

  echo "admin socket did not become ready: $admin_sock" >&2
  return 1
}

bench_bootstrap_iam_credentials() {
  local binary="$1"
  local data_dir="$2"
  local name="${3:-bench}"
  local admin_sock="${data_dir}/admin.sock"
  local bootstrap_json

  echo "  bootstrapping IAM credentials..."
  bench_wait_admin_socket "$data_dir" 100 0.2

  bootstrap_json=$("$binary" iam --json sa create "$name" --endpoint "$admin_sock")
  SA_ID=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["sa_id"])' <<<"$bootstrap_json")
  ACCESS_KEY=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["access_key"])' <<<"$bootstrap_json")
  SECRET_KEY=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["secret_key"])' <<<"$bootstrap_json")
  export SA_ID ACCESS_KEY SECRET_KEY
}

bench_wait_cluster_leader() {
  local base_url="$1"
  local attempts="${2:-120}"
  local sleep_seconds="${3:-0.5}"

  for _ in $(seq 1 "$attempts"); do
    local status state
    status=$(curl -sf "$base_url/api/cluster/status" 2>/dev/null || true)
    if [[ -n "$status" ]]; then
      state=$(echo "$status" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("state",""))' 2>/dev/null || true)
      if [[ "$state" == "Leader" ]]; then
        return 0
      fi
    fi
    sleep "$sleep_seconds"
  done

  echo "cluster leader not ready at $base_url" >&2
  return 1
}

bench_create_bucket_retry() {
  local base_url="$1"
  local bucket="$2"
  local attempts="${3:-60}"
  local sleep_seconds="${4:-0.5}"

  for _ in $(seq 1 "$attempts"); do
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$base_url/$bucket" 2>/dev/null || true)
    if [[ "$code" == "200" || "$code" == "409" ]]; then
      return 0
    fi
    sleep "$sleep_seconds"
  done

  [[ "${BENCH_QUIET:-0}" == "1" ]] || echo "bucket not ready: $bucket at $base_url" >&2
  return 1
}

bench_create_bucket_admin_retry() {
  local binary="$1"
  local data_dir="$2"
  local bucket="$3"
  local attempts="${4:-60}"
  local sleep_seconds="${5:-0.5}"
  local admin_sock="${data_dir}/admin.sock"

  for _ in $(seq 1 "$attempts"); do
    if "$binary" bucket create "$bucket" --endpoint "$admin_sock" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_seconds"
  done

  [[ "${BENCH_QUIET:-0}" == "1" ]] || echo "bucket not ready: $bucket via admin socket $admin_sock" >&2
  return 1
}

bench_create_bucket_with_policy_admin_retry() {
  local binary="$1"
  local data_dir="$2"
  local bucket="$3"
  local sa_id="$4"
  local policy="$5"
  local attempts="${6:-60}"
  local sleep_seconds="${7:-0.5}"
  local admin_sock="${data_dir}/admin.sock"
  local last_error=""
  local output

  for _ in $(seq 1 "$attempts"); do
    if output=$("$binary" bucket create "$bucket" \
      --endpoint "$admin_sock" \
      --attach-sa "$sa_id" \
      --attach-policy "$policy" \
      2>&1 >/dev/null); then
      return 0
    fi
    last_error="$output"
    sleep "$sleep_seconds"
  done

  [[ "${BENCH_QUIET:-0}" == "1" ]] || echo "bucket not ready with policy: $bucket via admin socket $admin_sock" >&2
  [[ "${BENCH_QUIET:-0}" == "1" || -z "$last_error" ]] || echo "last error: $last_error" >&2
  return 1
}

bench_put_object_retry() {
  local base_url="$1"
  local bucket="$2"
  local key="$3"
  local attempts="${4:-60}"
  local sleep_seconds="${5:-0.5}"

  for _ in $(seq 1 "$attempts"); do
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" -X PUT --data "ready" "$base_url/$bucket/$key" 2>/dev/null || true)
    if [[ "$code" == "200" ]]; then
      return 0
    fi
    sleep "$sleep_seconds"
  done

  [[ "${BENCH_QUIET:-0}" == "1" ]] || echo "object write not ready: $bucket/$key at $base_url" >&2
  return 1
}

bench_wait_capability_ready() {
  # Polls /v1/cluster/capabilities on every node's admin UDS until each voter
  # has gossiped the named capability as ready. Replaces a blind sleep that
  # had to budget for the ~30–45s gossip propagation. The admin UDS is
  # localhost+filemode-gated, so curl needs no SigV4 — Authorization headers
  # are not enforced there.
  local admin_socks="$1"        # colon-separated paths (a:b:c)
  local capability="$2"
  local attempts="${3:-120}"    # default 120 × 0.5s = 60s ceiling
  local sleep_seconds="${4:-0.5}"
  local IFS=':'
  read -r -a socks <<<"$admin_socks"
  unset IFS

  if (( ${#socks[@]} == 0 )); then
    echo "bench_wait_capability_ready: no admin sockets supplied" >&2
    return 1
  fi

  local attempt sock body all_ready expected
  expected="${#socks[@]}"
  for attempt in $(seq 1 "$attempts"); do
    all_ready=1
    for sock in "${socks[@]}"; do
      body=$(curl -s --unix-socket "$sock" http://_/v1/cluster/capabilities 2>/dev/null || true)
      if [[ -z "$body" ]]; then
        all_ready=0
        break
      fi
      # The gate reports peer→capability→ready. The capability is ready when
      # every voter peer has reported it true. python3 keeps the parse
      # readable; the bench env already uses it for IAM bootstrap JSON.
      python3 - <<EOF "$body" "$capability" "$expected" || { all_ready=0; break; }
import json, sys
body, cap, expected = sys.argv[1], sys.argv[2], int(sys.argv[3])
peers = (json.loads(body) or {}).get("peers", {}) or {}
ready = sum(1 for caps in peers.values() if caps.get(cap))
sys.exit(0 if ready >= expected else 1)
EOF
    done
    if [[ "$all_ready" == "1" ]]; then
      [[ "${BENCH_QUIET:-0}" == "1" ]] || echo "[bench] capability $capability ready across ${#socks[@]} nodes (attempt $attempt)"
      return 0
    fi
    sleep "$sleep_seconds"
  done

  echo "bench_wait_capability_ready: $capability not ready after $(awk -v a="$attempts" -v s="$sleep_seconds" 'BEGIN{printf "%.1fs", a*s}') across ${#socks[@]} nodes" >&2
  return 1
}

bench_collect_pprof() {
  local port="$1"
  local out_dir="$2"
  shift 2

  mkdir -p "$out_dir"

  local prefix="${BENCH_PPROF_PREFIX:-}"
  local profile endpoint out
  for profile in "$@"; do
    if [[ -n "$prefix" ]]; then
      endpoint="$profile"
      out="$out_dir/${prefix}-${profile}.out"
    else
      case "$profile" in
        goroutine)
          endpoint="goroutine?debug=1"
          out="$out_dir/goroutine.txt"
          ;;
        *)
          endpoint="$profile"
          out="$out_dir/$profile.pb.gz"
          ;;
      esac
    fi

    curl -sf "http://127.0.0.1:${port}/debug/pprof/${endpoint}" -o "$out" \
      && echo "  $out saved" \
      || echo "  pprof fetch failed: $profile"
  done
}
