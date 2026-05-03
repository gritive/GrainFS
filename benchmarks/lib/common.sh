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

  echo "  waiting for $label..."
  for _ in $(seq 1 "$attempts"); do
    if curl -sf "$url" >/dev/null 2>&1; then
      echo "  $label ready"
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
    printf '%s\n' "--no-encryption"
  fi
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

bench_collect_pprof() {
  local port="$1"
  local out_dir="$2"
  shift 2

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
