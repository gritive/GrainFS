#!/usr/bin/env bash
# Compare GrainFS S3 PUT-heavy throughput with server-side direct I/O on/off.
#
# Server: Colima Linux VM, using a cross-built GrainFS Linux binary.
# Client: host k6 over an SSH local-forward into Colima. This avoids VM image
# builds while keeping the shard write path on a Linux filesystem capable
# of O_DIRECT.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

K6="${K6:-k6}"
PROFILE_ROOT="${PROFILE_ROOT:-benchmarks/profiles/directio-s3-$(date +%Y%m%d-%H%M%S)}"
COLIMA_WORKDIR="${COLIMA_WORKDIR:-/tmp/grainfs-directio-s3}"
COLIMA_BINARY="${COLIMA_BINARY:-$COLIMA_WORKDIR/bin/grainfs}"
SERVER_PORT="${SERVER_PORT:-$(bench_free_port)}"

SIZE_KB="${SIZE_KB:-4096}"
CONCURRENCY_LIST="${CONCURRENCY_LIST:-1,4,16,32}"
MIX_LIST="${MIX_LIST:-write-heavy}"
DURATION="${DURATION:-30s}"
RAMP_UP="${RAMP_UP:-1s}"
RAMP_DOWN="${RAMP_DOWN:-1s}"
OBJECT_COUNT="${OBJECT_COUNT:-16}"
PROFILE="${PROFILE:-0}"
PPROF_PORT="${PPROF_PORT:-6060}"
SCRIPT="$BENCHMARKS_DIR/s3_mixed_profile.js"

bench_require_command colima "colima start"
bench_require_command ssh "install OpenSSH"
bench_require_command "$K6" "brew install k6"

colima_ssh() {
  colima ssh -- "$@"
}

colima_sh() {
  colima ssh -- bash -lc "$1"
}

colima_arch() {
  local machine
  machine="$(colima_sh 'uname -m')"
  case "$machine" in
    aarch64|arm64) echo "arm64" ;;
    x86_64|amd64) echo "amd64" ;;
    *)
      echo "[error] unsupported Colima arch: $machine" >&2
      return 1
      ;;
  esac
}

prepare_colima_binary() {
  if ! colima status >/dev/null 2>&1; then
    echo "[error] colima is not running; start with: colima start" >&2
    exit 1
  fi

  local arch local_binary
  arch="$(colima_arch)"
  local_binary="${LOCAL_LINUX_BINARY:-$REPO_ROOT/bin/grainfs-linux-$arch}"

  if [[ "${NO_BUILD:-0}" != "1" ]]; then
    echo "[bench] building Linux GrainFS binary for Colima ($arch)..."
    GOOS=linux GOARCH="$arch" go build -o "$local_binary" ./cmd/grainfs/
  fi
  if [[ ! -x "$local_binary" ]]; then
    echo "[error] Linux binary not found: $local_binary" >&2
    echo "        run without NO_BUILD=1 or set LOCAL_LINUX_BINARY" >&2
    exit 1
  fi

  echo "[bench] copying binary to Colima: $COLIMA_BINARY"
  colima_ssh mkdir -p "$(dirname "$COLIMA_BINARY")"
  cat "$local_binary" | colima_sh "cat > '$COLIMA_BINARY' && chmod +x '$COLIMA_BINARY'"
}

start_colima_forward() {
  local local_port="$1"
  local remote_port="$2"
  local out_dir="$3"
  local ssh_config="$out_dir/colima_ssh.config"
  local pid_file="$out_dir/ssh-forward.pid"
  local log_file="$out_dir/ssh-forward.log"

  colima ssh-config >"$ssh_config"
  ssh -F "$ssh_config" \
    -o ExitOnForwardFailure=yes \
    -N \
    -L "127.0.0.1:${local_port}:127.0.0.1:${remote_port}" \
    colima >"$log_file" 2>&1 &
  echo "$!" >"$pid_file"
}

stop_colima_forward() {
  local out_dir="$1"
  local pid_file="$out_dir/ssh-forward.pid"
  if [[ -f "$pid_file" ]]; then
    kill "$(cat "$pid_file")" >/dev/null 2>&1 || true
    rm -f "$pid_file"
  fi
}

append_summary_row() {
  local report_path="$1"
  local summary_path="$2"
  local scenario="$3"
  local vus="$4"
  local write_percent="$5"

  if [[ ! -f "$report_path" ]]; then
    echo "| ${scenario} | ${vus} | ${write_percent} | 0 | 0.00 | 0 | 0.00 | 1.0000 | 0.0 |" >>"$summary_path"
    return 0
  fi

  python3 - "$report_path" "$summary_path" "$scenario" "$vus" "$write_percent" <<'PY'
import json
import sys

report_path, summary_path, scenario, vus, write_percent = sys.argv[1:]
with open(report_path, "r", encoding="utf-8") as f:
    r = json.load(f)

def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0

put = r.get("put", {})
get = r.get("get", {})
put_bytes = fnum(put.get("bytes"))
get_bytes = fnum(get.get("bytes"))
duration = 0.0
for op in (put, get):
    rate = fnum(op.get("bytes_per_sec"))
    bytes_ = fnum(op.get("bytes"))
    if rate > 0 and bytes_ > 0:
        duration = max(duration, bytes_ / rate)
throughput = ((put_bytes + get_bytes) / duration / 1024 / 1024) if duration > 0 else 0.0

line = (
    f"| {scenario} | {vus} | {write_percent} | "
    f"{int(fnum(put.get('ops')))} | {put.get('p99_ms', '0.00')} | "
    f"{int(fnum(get.get('ops')))} | {get.get('p99_ms', '0.00')} | "
    f"{float(r.get('http_req_failed') or 0):.4f} | {throughput:.1f} |"
)
with open(summary_path, "a", encoding="utf-8") as f:
    f.write(line + "\n")
PY
}

mix_write_percent() {
  case "$1" in
    write-heavy) echo 90 ;;
    read-heavy) echo 10 ;;
    pure-put) echo 100 ;;
    mixed) echo 50 ;;
    *)
      echo "[error] unknown MIX_LIST entry: $1" >&2
      return 1
      ;;
  esac
}

duration_seconds() {
  local raw="$1"
  local n="${raw//[^0-9]/}"
  [[ -n "$n" ]] || n=30
  echo "$n"
}

wait_colima_admin_socket() {
  local socket_path="$1"
  local attempts="${2:-150}"
  local sleep_seconds="${3:-0.2}"

  echo "  waiting for admin socket..."
  for _ in $(seq 1 "$attempts"); do
    if colima_ssh test -S "$socket_path" >/dev/null 2>&1; then
      echo "  admin socket ready"
      return 0
    fi
    sleep "$sleep_seconds"
  done

  echo "admin socket did not become ready: $socket_path" >&2
  return 1
}

wait_colima_pprof() {
  local port="$1"
  local attempts="${2:-150}"
  local sleep_seconds="${3:-0.2}"

  echo "  waiting for pprof..."
  for _ in $(seq 1 "$attempts"); do
    if colima_ssh curl -sf "http://127.0.0.1:${port}/debug/pprof/" >/dev/null 2>&1; then
      echo "  pprof ready"
      return 0
    fi
    sleep "$sleep_seconds"
  done

  echo "pprof did not become ready: $port" >&2
  return 1
}

collect_colima_pprof() {
  local port="$1"
  local out_dir="$2"
  shift 2

  mkdir -p "$out_dir"
  local profile endpoint out
  for profile in "$@"; do
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
    colima_ssh curl -sf "http://127.0.0.1:${port}/debug/pprof/${endpoint}" >"$out" \
      && echo "  $out saved" \
      || echo "  pprof fetch failed: $profile"
  done
}

start_colima_server() {
  local label="$1"
  local direct_io="$2"
  local port="$3"
  local remote_root="$4"
  local remote_data="$5"
  local remote_log="$6"
  local remote_pid="$7"

  local pprof_arg=""
  if [[ "$PROFILE" == "1" ]]; then
    pprof_arg="--pprof-port $PPROF_PORT"
  fi

  colima_sh "if [ -f '$remote_pid' ]; then kill \$(cat '$remote_pid') >/dev/null 2>&1 || true; fi; rm -rf '$remote_root'; mkdir -p '$remote_root'"
  colima_sh "nohup '$COLIMA_BINARY' serve \
    --data '$remote_data' \
    --port '$port' \
    --nfs4-port 0 \
    --nbd-port 0 \
    --cluster-key '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' \
    --direct-io='$direct_io' \
    $pprof_arg >'$remote_log' 2>&1 & echo \$! >'$remote_pid'"
  echo "[bench] ${label}: Colima server pid $(colima_sh "cat '$remote_pid'")"
}

run_case() {
  local label="$1"
  local direct_io="$2"
  local port="$3"
  local remote_root="$COLIMA_WORKDIR/$label"
  local remote_data="$remote_root/data"
  local remote_log="$remote_root/server.log"
  local remote_pid="$remote_root/server.pid"
  local remote_admin_sock="$remote_data/admin.sock"
  local out_dir="$PROFILE_ROOT/$label"
  local summary_md="$out_dir/summary.md"
  local fail=0

  cleanup_case() {
    stop_colima_forward "$out_dir"
    colima_sh "if [ -f '$remote_pid' ]; then kill \$(cat '$remote_pid') >/dev/null 2>&1 || true; fi" >/dev/null 2>&1 || true
    colima_sh "cat '$remote_log' 2>/dev/null || true" >"$out_dir/server.log" 2>&1 || true
    if [[ "${KEEP_DIRECTIO_BENCH_DATA:-0}" != "1" ]]; then
      colima_sh "rm -rf '$remote_root'" >/dev/null 2>&1 || true
    fi
  }

  echo ""
  echo "=================================================================="
  echo "  directio S3 case: $label (--direct-io=$direct_io)"
  echo "=================================================================="

  mkdir -p "$out_dir"
  start_colima_server "$label" "$direct_io" "$port" "$remote_root" "$remote_data" "$remote_log" "$remote_pid"
  start_colima_forward "$port" "$port" "$out_dir"

  if ! bench_wait_tcp_port "$TARGET_HOST" "$port" "GrainFS ${label}" 150 0.2 ||
    ! wait_colima_admin_socket "$remote_admin_sock" 150 0.2; then
    cleanup_case
    return 1
  fi
  if [[ "$PROFILE" == "1" ]] && ! wait_colima_pprof "$PPROF_PORT" 150 0.2; then
    cleanup_case
    return 1
  fi

  if ! colima_ssh "$COLIMA_BINARY" iam --json sa create "bench-${label}" \
    --endpoint "$remote_admin_sock" >"$out_dir/iam.json"; then
    cleanup_case
    return 1
  fi

  local access_key secret_key
  if ! access_key=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["access_key"])' <"$out_dir/iam.json"); then
    cleanup_case
    return 1
  fi
  if ! secret_key=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["secret_key"])' <"$out_dir/iam.json"); then
    cleanup_case
    return 1
  fi

  {
    echo "# GrainFS directio S3 case: ${label}"
    echo
    echo "- direct_io: ${direct_io}"
    echo "- server: Colima Linux VM"
    echo "- target: http://${TARGET_HOST}:${port}"
    echo "- object: ${SIZE_KB}KB"
    echo "- duration: ${DURATION}"
    echo "- concurrency: ${CONCURRENCY_LIST}"
    echo "- mix: ${MIX_LIST}"
    echo
    echo "| scenario | VUs | write % | PUT ops | PUT p99 ms | GET ops | GET p99 ms | failed rate | throughput MB/s |"
    echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
  } >"$summary_md"

  IFS=',' read -ra CONCURRENCY_VALUES <<<"$CONCURRENCY_LIST"
  IFS=',' read -ra MIX_VALUES <<<"$MIX_LIST"
  for vus in "${CONCURRENCY_VALUES[@]}"; do
    for mix in "${MIX_VALUES[@]}"; do
      local write_percent scenario out_scenario summary_json k6_exit pprof_bg_pid
      write_percent="$(mix_write_percent "$mix")" || { fail=1; continue; }
      scenario="${mix}-c${vus}"
      out_scenario="$out_dir/$scenario"
      summary_json="$out_scenario/report.json"
      mkdir -p "$out_scenario"

      echo "[bench] ${label}: scenario ${scenario}: write=${write_percent}% vus=${vus}"
      pprof_bg_pid=""
      if [[ "$PROFILE" == "1" ]]; then
        local duration_sec cpu_sec
        duration_sec="$(duration_seconds "$DURATION")"
        cpu_sec=$(( duration_sec > 6 ? duration_sec - 2 : duration_sec ))
        collect_colima_pprof "$PPROF_PORT" "$out_scenario/pre" heap
        (
          sleep 1
          colima_ssh curl -sf "http://127.0.0.1:${PPROF_PORT}/debug/pprof/profile?seconds=${cpu_sec}" \
            >"$out_scenario/cpu.pb.gz" && echo "[pprof] ${label}/${scenario} cpu.pb.gz saved" || echo "[pprof] ${label}/${scenario} CPU profile failed"
        ) &
        pprof_bg_pid=$!
      fi

      k6_exit=0
      "$K6" run "$SCRIPT" \
        --env BASE_URL="http://${TARGET_HOST}:${port}" \
        --env ACCESS_KEY="$access_key" \
        --env SECRET_KEY="$secret_key" \
        --env BUCKET="directio-${label}" \
        --env OBJECT_SIZE_KB="$SIZE_KB" \
        --env OBJECT_COUNT="$OBJECT_COUNT" \
        --env WRITE_PERCENT="$write_percent" \
        --env MIX="$mix" \
        --env DURATION="$DURATION" \
        --env RAMP_UP="$RAMP_UP" \
        --env RAMP_DOWN="$RAMP_DOWN" \
        --env MAX_VUS="$vus" \
        --env SUMMARY_JSON="$summary_json" \
        >"$out_scenario/k6.out" 2>"$out_scenario/k6.err" || k6_exit=$?

      [[ -n "$pprof_bg_pid" ]] && wait "$pprof_bg_pid" 2>/dev/null || true
      if [[ "$PROFILE" == "1" ]]; then
        collect_colima_pprof "$PPROF_PORT" "$out_scenario" heap allocs goroutine mutex block
        go tool pprof -top -nodecount=20 "$out_scenario/cpu.pb.gz" >"$out_scenario/cpu_top.txt" 2>/dev/null || true
        go tool pprof -top -nodecount=20 "$out_scenario/heap.pb.gz" >"$out_scenario/heap_top.txt" 2>/dev/null || true
        go tool pprof -top -nodecount=20 "$out_scenario/allocs.pb.gz" >"$out_scenario/allocs_top.txt" 2>/dev/null || true
      fi

      append_summary_row "$summary_json" "$summary_md" "$scenario" "$vus" "$write_percent"
      if [[ "$k6_exit" != "0" ]]; then
        echo "[bench] ${label}: scenario ${scenario} exited ${k6_exit}; see ${out_scenario}/k6.err" >&2
        fail=1
      fi
    done
  done

  cleanup_case
  return "$fail"
}

prepare_colima_binary
TARGET_HOST="127.0.0.1"

rm -rf "$PROFILE_ROOT"
mkdir -p "$PROFILE_ROOT"

FAIL=0
run_case "buffered" "false" "$SERVER_PORT" || FAIL=1
run_case "direct" "true" "$SERVER_PORT" || FAIL=1

summary="$PROFILE_ROOT/summary.md"
{
  echo "# GrainFS directio S3 benchmark"
  echo
  echo "- server: Colima Linux VM"
  echo "- client: host k6"
  echo "- target: http://${TARGET_HOST}:${SERVER_PORT}"
  echo "- object: ${SIZE_KB}KB"
  echo "- duration: ${DURATION}"
  echo "- concurrency: ${CONCURRENCY_LIST}"
  echo "- mix: ${MIX_LIST}"
  echo
  echo "## buffered"
  cat "$PROFILE_ROOT/buffered/summary.md"
  echo
  echo "## direct"
  cat "$PROFILE_ROOT/direct/summary.md"
} >"$summary"

echo ""
echo "=================================================================="
echo "  directio comparison summary"
echo "=================================================================="
cat "$summary"
echo ""
echo "[bench] results saved to $PROFILE_ROOT"
exit "$FAIL"
