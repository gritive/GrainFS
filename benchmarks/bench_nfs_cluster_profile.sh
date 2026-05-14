#!/usr/bin/env bash
# NFSv4 GrainFS cluster benchmark with the same fio workload as bench_nfs_profile.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${1:-./bin/grainfs}"
bench_require_binary "$BINARY"
bench_require_colima

HOST_IP="${HOST_IP:-192.168.5.2}"
NODE_COUNT="${NODE_COUNT:-3}"
BENCH_DIR="${BENCH_DIR:-/tmp/grainfs-nfs-cluster-bench}"
BUCKET="${BUCKET:-benchnfs}"
MNT="/mnt/grainfs-bench-nfs-cluster"
NFS_VERS="${NFS_VERS:-4.0}"
FIO_RUNTIME="${FIO_RUNTIME:-15}"
FIO_STREAM_SIZE="${FIO_STREAM_SIZE:-256m}"
FIO_STREAM_JOBS="${FIO_STREAM_JOBS:-4}"
FIO_RAND_SIZE="${FIO_RAND_SIZE:-64m}"
FIO_RAND_JOBS="${FIO_RAND_JOBS:-4}"
CPU_PROFILE_SECONDS="${CPU_PROFILE_SECONDS:-30}"

if [[ "$NODE_COUNT" -lt 2 ]]; then
  echo "[error] NODE_COUNT must be >= 2 for clustered NFS profile" >&2
  exit 1
fi

HTTP_PORTS=()
RAFT_PORTS=()
NFS_PORTS=()
PPROF_PORTS=()
for idx in $(seq 1 "$NODE_COUNT"); do
  HTTP_PORTS+=("$(bench_free_port)")
  RAFT_PORTS+=("$(bench_free_port)")
  NFS_PORTS+=("$(bench_free_port)")
  PPROF_PORTS+=("$(bench_free_port)")
done

PROFILE_DIR="benchmarks/profiles/nfs-${NODE_COUNT}-node-cluster-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$PROFILE_DIR"

PIDS=()
rm -rf "$BENCH_DIR"
for idx in $(seq 0 $((NODE_COUNT - 1))); do
  mkdir -p "$BENCH_DIR/n$idx"
done
BENCH_ENCRYPTION_KEY_FILE="${BENCH_ENCRYPTION_KEY_FILE:-$BENCH_DIR/encryption.key}"
export BENCH_ENCRYPTION_KEY_FILE
bench_generate_encryption_key_file "$BENCH_ENCRYPTION_KEY_FILE"

cleanup() {
  echo "=== cleanup ==="
  bench_colima_ssh sudo umount -l "$MNT" 2>/dev/null || true
  bench_colima_ssh sudo rmdir "$MNT" 2>/dev/null || true
  [[ -n "${PPROF_PID:-}" ]] && kill "$PPROF_PID" 2>/dev/null || true
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  bench_copy_node_logs "$BENCH_DIR" "$PROFILE_DIR"
  rm -rf "$BENCH_DIR"
}
trap cleanup EXIT INT TERM

raft_addr() {
  echo "127.0.0.1:${RAFT_PORTS[$1]}"
}

http_port() {
  echo "${HTTP_PORTS[$1]}"
}

nfs_port() {
  echo "${NFS_PORTS[$1]}"
}

pprof_port() {
  echo "${PPROF_PORTS[$1]}"
}

start_node() {
  local i="$1"
  # This benchmark runs all nodes on localhost. Capability gossip validates the
  # claimed node ID against the sender host, so use the raft address as node ID
  # instead of an opaque benchmark label.
  local node_id
  node_id="$(raft_addr "$i")"
  "$BINARY" serve \
    --data "$BENCH_DIR/n$i" \
    --port "$(http_port "$i")" \
    --node-id "$node_id" \
    --raft-addr "$(raft_addr "$i")" \
    --cluster-key "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" \
    $(bench_encryption_args) \
    --nfs4-port "$(nfs_port "$i")" \
    --nbd-port 0 \
    --pprof-port "$(pprof_port "$i")" \
    --scrub-interval 0 \
    --lifecycle-interval 0 \
    --log-level warn \
    > "$BENCH_DIR/n$i.log" 2>&1 &
  PIDS+=($!)
  echo "  node-$i: HTTP=:$(http_port "$i") NFS=:$(nfs_port "$i") Raft=$(raft_addr "$i")"
}

start_node 0
bench_wait_tcp_port "127.0.0.1" "$(http_port 0)" "node-0 HTTP" 180 0.2
bench_wait_tcp_port "127.0.0.1" "$(pprof_port 0)" "node-0 pprof" 180 0.2
bench_wait_admin_socket "$BENCH_DIR/n0" 100 0.2

for i in $(seq 1 $((NODE_COUNT - 1))); do
  printf '%s' "$(raft_addr 0)" >"$BENCH_DIR/n$i/.join-pending"
  chmod 600 "$BENCH_DIR/n$i/.join-pending"
  start_node "$i"
  bench_wait_tcp_port "127.0.0.1" "$(http_port "$i")" "node-$i HTTP" 180 0.2
  bench_wait_tcp_port "127.0.0.1" "$(pprof_port "$i")" "node-$i pprof" 180 0.2
done

echo "=== waiting for cluster leader ==="
LEADER_INDEX=""
for _ in $(seq 1 180); do
  for i in $(seq 0 $((NODE_COUNT - 1))); do
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
bench_wait_admin_socket "$BENCH_DIR/n$LEADER_INDEX" 100 0.2

echo ""
echo "=== preparing bucket export ($BUCKET) ==="
bench_bootstrap_iam_credentials "$BINARY" "$BENCH_DIR/n$LEADER_INDEX"
bench_create_bucket_admin_retry "$BINARY" "$BENCH_DIR/n$LEADER_INDEX" "$BUCKET"
"$BINARY" nfs export add "$BUCKET" --endpoint "$BENCH_DIR/n$LEADER_INDEX/admin.sock" || true
sleep "${CLUSTER_WARMUP_SLEEP:-5}"

echo ""
echo "=== mounting NFS inside Colima (vers=$NFS_VERS host=$HOST_IP port=$LEADER_NFS_PORT) ==="
bench_colima_ssh sudo mkdir -p "$MNT"
bench_colima_ssh sudo mount -t nfs4 \
  -o "vers=$NFS_VERS,port=$LEADER_NFS_PORT,rsize=131072,wsize=131072,hard,intr" \
  "${HOST_IP}:/${BUCKET}" "$MNT"
bench_colima_ssh df -h "$MNT" || true

curl -sf "http://127.0.0.1:$LEADER_PPROF_PORT/debug/pprof/heap" \
  -o "$PROFILE_DIR/heap_pre.pb.gz" && echo "  heap_pre.pb.gz saved"

(
  sleep 5
  echo "  [pprof] starting ${CPU_PROFILE_SECONDS}s CPU profile..."
  curl -sf "http://127.0.0.1:$LEADER_PPROF_PORT/debug/pprof/profile?seconds=$CPU_PROFILE_SECONDS" \
    -o "$PROFILE_DIR/cpu.pb.gz" && echo "  [pprof] cpu.pb.gz saved" || echo "  [pprof] CPU profile failed"
) &
PPROF_PID=$!

FIO_LOG="$PROFILE_DIR/fio_output.txt"
bench_colima_ssh bash -s <<SCRIPT 2>&1 | tee "$FIO_LOG"
set -e
BENCH_DIR="${MNT}/fio-bench-\$(date +%s)"
sudo mkdir -p "\$BENCH_DIR"

echo "--- sequential write (${FIO_STREAM_JOBS} threads, 128K blocks) ---"
sudo fio --name=seq_write --directory="\$BENCH_DIR" --rw=write --bs=128k --fallocate=none --size="$FIO_STREAM_SIZE" --numjobs="$FIO_STREAM_JOBS" --runtime="$FIO_RUNTIME" --time_based --group_reporting --output-format=normal --ioengine=sync

echo ""
echo "--- sequential read (${FIO_STREAM_JOBS} threads, 128K blocks) ---"
sudo fio --name=seq_read --directory="\$BENCH_DIR" --rw=read --bs=128k --fallocate=none --size="$FIO_STREAM_SIZE" --numjobs="$FIO_STREAM_JOBS" --runtime="$FIO_RUNTIME" --time_based --group_reporting --output-format=normal --ioengine=sync

echo ""
echo "--- random read/write mix (${FIO_RAND_JOBS} threads, 4K blocks, 75% read) ---"
sudo fio --name=rand_mix --directory="\$BENCH_DIR" --rw=randrw --rwmixread=75 --bs=4k --fallocate=none --size="$FIO_RAND_SIZE" --numjobs="$FIO_RAND_JOBS" --runtime="$FIO_RUNTIME" --time_based --group_reporting --output-format=normal --ioengine=sync

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
