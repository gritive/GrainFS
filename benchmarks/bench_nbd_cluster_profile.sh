#!/usr/bin/env bash
# NBD GrainFS 3-node cluster benchmark with the same fio workload as bench_nbd_profile.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${1:-./bin/grainfs}"
bench_require_binary "$BINARY"
bench_require_colima

HOST_IP="${HOST_IP:-192.168.5.2}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-nbd-cluster-bench}"
NBD_DEV="${NBD_DEV:-/dev/nbd0}"
VOL_SIZE=$((128 * 1024 * 1024))
FIO_SIZE="64m"

HTTP0=$(bench_free_port); HTTP1=$(bench_free_port); HTTP2=$(bench_free_port)
RAFT0=$(bench_free_port); RAFT1=$(bench_free_port); RAFT2=$(bench_free_port)
NBD0=$(bench_free_port); NBD1=$(bench_free_port); NBD2=$(bench_free_port)
PPROF0=$(bench_free_port); PPROF1=$(bench_free_port); PPROF2=$(bench_free_port)

PIDS=()
rm -rf "$BENCH_DIR"
mkdir -p "$BENCH_DIR"/{n0,n1,n2}

cleanup() {
  echo ""
  echo "=== cleanup ==="
  bench_colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true
  if [[ -n "${CPU_PROFILE_PID:-}" ]]; then
    wait "$CPU_PROFILE_PID" 2>/dev/null || true
  fi
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$BENCH_DIR"
}
trap cleanup EXIT INT TERM

peers_for() {
  case "$1" in
    0) echo "127.0.0.1:$RAFT1,127.0.0.1:$RAFT2" ;;
    1) echo "127.0.0.1:$RAFT0,127.0.0.1:$RAFT2" ;;
    2) echo "127.0.0.1:$RAFT0,127.0.0.1:$RAFT1" ;;
  esac
}

raft_addr() {
  case "$1" in
    0) echo "127.0.0.1:$RAFT0" ;;
    1) echo "127.0.0.1:$RAFT1" ;;
    2) echo "127.0.0.1:$RAFT2" ;;
  esac
}

http_port() {
  case "$1" in
    0) echo "$HTTP0" ;;
    1) echo "$HTTP1" ;;
    2) echo "$HTTP2" ;;
  esac
}

nbd_port() {
  case "$1" in
    0) echo "$NBD0" ;;
    1) echo "$NBD1" ;;
    2) echo "$NBD2" ;;
  esac
}

pprof_port() {
  case "$1" in
    0) echo "$PPROF0" ;;
    1) echo "$PPROF1" ;;
    2) echo "$PPROF2" ;;
  esac
}

for i in 0 1 2; do
  "$BINARY" serve \
    --data "$BENCH_DIR/n$i" \
    --port "$(http_port "$i")" \
    --node-id "bench-nbd-node-$i" \
    --raft-addr "$(raft_addr "$i")" \
    --peers "$(peers_for "$i")" \
    --cluster-key "bench-nbd-cluster-key" \
    --ec-data 2 \
    --ec-parity 1 \
    $(bench_encryption_args) \
    --nfs4-port 0 \
    --nbd-port "$(nbd_port "$i")" \
    --nbd-volume-size "$VOL_SIZE" \
    --pprof-port "$(pprof_port "$i")" \
    --snapshot-interval 0 \
    --scrub-interval 0 \
    --lifecycle-interval 0 \
    --rate-limit-ip-rps 0 \
    --rate-limit-user-rps 0 \
    --dedup=false \
    --log-level warn \
    > "$BENCH_DIR/n$i.log" 2>&1 &
  PIDS+=($!)
  echo "  node-$i: HTTP=:$(http_port "$i") NBD=:$(nbd_port "$i") Raft=$(raft_addr "$i")"
done

echo "=== waiting for cluster leader ==="
LEADER_INDEX=""
for _ in $(seq 1 180); do
  for i in 0 1 2; do
    status=$(curl -sf "http://127.0.0.1:$(http_port "$i")/api/cluster/status" 2>/dev/null || true)
    [[ -z "$status" ]] && continue
    state=$(echo "$status" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("state",""))' 2>/dev/null || true)
    if [[ "$state" == "Leader" ]]; then
      LEADER_INDEX="$i"
      break 2
    fi
  done
  sleep 0.5
done

if [[ -z "$LEADER_INDEX" ]]; then
  echo "no cluster leader found" >&2
  tail -40 "$BENCH_DIR"/n*.log >&2
  exit 1
fi

LEADER_NBD_PORT="$(nbd_port "$LEADER_INDEX")"
LEADER_PPROF_PORT="$(pprof_port "$LEADER_INDEX")"
echo "  leader node-$LEADER_INDEX NBD=:$LEADER_NBD_PORT pprof=:$LEADER_PPROF_PORT"
sleep "${CLUSTER_WARMUP_SLEEP:-5}"

bench_colima_ssh sudo modprobe nbd max_part=0 2>/dev/null || true
bench_colima_ssh sudo nbd-client -d "$NBD_DEV" 2>/dev/null || true
bench_colima_ssh sudo nbd-client "$HOST_IP" "$LEADER_NBD_PORT" "$NBD_DEV" -b 4096 -N default
echo "OK: nbd-client connected ($NBD_DEV)"

PROFILE_DIR="benchmarks/profiles/nbd-cluster-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$PROFILE_DIR"
curl -sf "http://127.0.0.1:$LEADER_PPROF_PORT/debug/pprof/profile?seconds=60" \
  -o "$PROFILE_DIR/cpu.pb.gz" &
CPU_PROFILE_PID=$!

run_fio() {
  local label="$1"; shift
  echo ""
  echo "--- fio: $label ---"
  bench_colima_ssh sudo fio \
    --name="$label" \
    --filename="$NBD_DEV" \
    --size="$FIO_SIZE" \
    --runtime=15 \
    --time_based \
    --output-format=normal \
    "$@" 2>&1 | grep -E "READ:|WRITE:|iops|bw=|lat"
}

run_fio "seq-read-4K"   --ioengine=sync --rw=read   --bs=4k
run_fio "seq-write-4K"  --ioengine=sync --rw=write  --bs=4k
run_fio "seq-read-64K"  --ioengine=sync --rw=read   --bs=64k
run_fio "seq-write-64K" --ioengine=sync --rw=write  --bs=64k
run_fio "rand-read-4K"  --ioengine=sync --rw=randread  --bs=4k
run_fio "rand-write-4K" --ioengine=sync --rw=randwrite --bs=4k

wait "$CPU_PROFILE_PID" 2>/dev/null || true
bench_collect_pprof "$LEADER_PPROF_PORT" "$PROFILE_DIR" mutex allocs heap goroutine

echo ""
echo "=== CPU top-10 (go tool pprof) ==="
go tool pprof -top -nodecount=10 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  pprof analysis failed"

echo ""
echo "PROFILE_DIR=$PROFILE_DIR"
