#!/usr/bin/env bash
# NFSv4 GrainFS 3-node cluster benchmark with the same fio workload as bench_nfs_profile.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${1:-./bin/grainfs}"
bench_require_binary "$BINARY"
bench_require_colima

HOST_IP="${HOST_IP:-192.168.5.2}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-nfs-cluster-bench}"
MNT="/mnt/grainfs-bench-nfs-cluster"
NFS_VERS="${NFS_VERS:-4.0}"

HTTP0=$(bench_free_port); HTTP1=$(bench_free_port); HTTP2=$(bench_free_port)
RAFT0=$(bench_free_port); RAFT1=$(bench_free_port); RAFT2=$(bench_free_port)
NFS0=$(bench_free_port); NFS1=$(bench_free_port); NFS2=$(bench_free_port)
PPROF0=$(bench_free_port); PPROF1=$(bench_free_port); PPROF2=$(bench_free_port)

PROFILE_DIR="benchmarks/profiles/nfs-cluster-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$PROFILE_DIR"

PIDS=()
rm -rf "$BENCH_DIR"
mkdir -p "$BENCH_DIR"/{n0,n1,n2}

cleanup() {
  echo "=== cleanup ==="
  bench_colima_ssh sudo umount -l "$MNT" 2>/dev/null || true
  bench_colima_ssh sudo rmdir "$MNT" 2>/dev/null || true
  [[ -n "${PPROF_PID:-}" ]] && kill "$PPROF_PID" 2>/dev/null || true
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

nfs_port() {
  case "$1" in
    0) echo "$NFS0" ;;
    1) echo "$NFS1" ;;
    2) echo "$NFS2" ;;
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
    --node-id "bench-nfs-node-$i" \
    --raft-addr "$(raft_addr "$i")" \
    --peers "$(peers_for "$i")" \
    --cluster-key "bench-nfs-cluster-key" \
    --ec-data 2 \
    --ec-parity 1 \
    $(bench_encryption_args) \
    --nfs4-port "$(nfs_port "$i")" \
    --nbd-port 0 \
    --pprof-port "$(pprof_port "$i")" \
    --snapshot-interval 0 \
    --scrub-interval 0 \
    --lifecycle-interval 0 \
    --rate-limit-ip-rps 0 \
    --rate-limit-user-rps 0 \
    --log-level warn \
    > "$BENCH_DIR/n$i.log" 2>&1 &
  PIDS+=($!)
  echo "  node-$i: HTTP=:$(http_port "$i") NFS=:$(nfs_port "$i") Raft=$(raft_addr "$i")"
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

LEADER_NFS_PORT="$(nfs_port "$LEADER_INDEX")"
LEADER_PPROF_PORT="$(pprof_port "$LEADER_INDEX")"
echo "  leader node-$LEADER_INDEX NFS=:$LEADER_NFS_PORT pprof=:$LEADER_PPROF_PORT"
sleep "${CLUSTER_WARMUP_SLEEP:-5}"

echo ""
echo "=== mounting NFS inside Colima (vers=$NFS_VERS host=$HOST_IP port=$LEADER_NFS_PORT) ==="
bench_colima_ssh sudo mkdir -p "$MNT"
bench_colima_ssh sudo mount -t nfs4 \
  -o "vers=$NFS_VERS,port=$LEADER_NFS_PORT,rsize=131072,wsize=131072,hard,intr" \
  "${HOST_IP}:/" "$MNT"
bench_colima_ssh df -h "$MNT" || true

curl -sf "http://127.0.0.1:$LEADER_PPROF_PORT/debug/pprof/heap" \
  -o "$PROFILE_DIR/heap_pre.pb.gz" && echo "  heap_pre.pb.gz saved"

(
  sleep 5
  echo "  [pprof] starting 30s CPU profile..."
  curl -sf "http://127.0.0.1:$LEADER_PPROF_PORT/debug/pprof/profile?seconds=30" \
    -o "$PROFILE_DIR/cpu.pb.gz" && echo "  [pprof] cpu.pb.gz saved" || echo "  [pprof] CPU profile failed"
) &
PPROF_PID=$!

FIO_LOG="$PROFILE_DIR/fio_output.txt"
bench_colima_ssh bash -s <<SCRIPT 2>&1 | tee "$FIO_LOG"
set -e
BENCH_DIR="${MNT}/fio-bench-\$(date +%s)"
sudo mkdir -p "\$BENCH_DIR"

echo "--- sequential write (4 threads, 128K blocks) ---"
sudo fio --name=seq_write --directory="\$BENCH_DIR" --rw=write --bs=128k --size=256m --numjobs=4 --runtime=15 --time_based --group_reporting --output-format=normal --ioengine=sync

echo ""
echo "--- sequential read (4 threads, 128K blocks) ---"
sudo fio --name=seq_read --directory="\$BENCH_DIR" --rw=read --bs=128k --size=256m --numjobs=4 --runtime=15 --time_based --group_reporting --output-format=normal --ioengine=sync

echo ""
echo "--- random read/write mix (4K blocks, 75% read) ---"
sudo fio --name=rand_mix --directory="\$BENCH_DIR" --rw=randrw --rwmixread=75 --bs=4k --size=64m --numjobs=4 --runtime=15 --time_based --group_reporting --output-format=normal --ioengine=sync

sudo rm -rf "\$BENCH_DIR"
SCRIPT

wait "$PPROF_PID" 2>/dev/null || true

echo ""
echo "=== collecting post-benchmark profiles ==="
bench_collect_pprof "$LEADER_PPROF_PORT" "$PROFILE_DIR" heap goroutine mutex block allocs

echo ""
echo "=== CPU top-15 (go tool pprof) ==="
go tool pprof -top -nodecount=15 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  pprof analysis failed"

echo ""
echo "=== fio summary ==="
grep -E "READ:|WRITE:|lat \\(|iops" "$FIO_LOG" 2>/dev/null || true

echo ""
echo "PROFILE_DIR=$PROFILE_DIR"
