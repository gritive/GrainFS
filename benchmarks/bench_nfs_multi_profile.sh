#!/usr/bin/env bash
# NFS multi-bucket export benchmark: N buckets x M fio workers per bucket.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${1:-./bin/grainfs}"
bench_require_binary "$BINARY"
bench_require_colima

HTTP_PORT=$(bench_free_port)
NFS4_PORT=$(bench_free_port)
PPROF_PORT=$(bench_free_port)
HOST_IP="${HOST_IP:-192.168.5.2}"
NUM_BUCKETS="${NUM_BUCKETS:-4}"
FIO_WORKERS_PER_BUCKET="${FIO_WORKERS_PER_BUCKET:-4}"
FIO_RUNTIME="${FIO_RUNTIME:-30}"
FIO_RAMP="${FIO_RAMP:-5}"
FIO_SIZE="${FIO_SIZE:-128m}"
READDIR_SAMPLES="${READDIR_SAMPLES:-100}"
NFS_VERS="${NFS_VERS:-4.1}"
MOUNT_OPTS="vers=${NFS_VERS},proto=tcp,port=${NFS4_PORT},rsize=131072,wsize=131072,hard,intr"

PROFILE_DIR="$REPO_ROOT/benchmarks/profiles/nfs-multi-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$PROFILE_DIR"
SUMMARY="$PROFILE_DIR/summary.txt"
DATA_DIR=$(mktemp -d "grainfs-nfs-multi-bench-XXXX" -p /tmp)

cleanup() {
  local i
  echo "=== cleanup ==="
  for i in $(seq 1 "$NUM_BUCKETS"); do
    bench_colima_ssh sudo umount -l "/mnt/grainfs-nfs-b$i" 2>/dev/null || true
    bench_colima_ssh sudo rmdir "/mnt/grainfs-nfs-b$i" 2>/dev/null || true
  done
  bench_colima_ssh sudo umount -l /mnt/grainfs-nfs-pseudo 2>/dev/null || true
  bench_colima_ssh sudo rmdir /mnt/grainfs-nfs-pseudo 2>/dev/null || true
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  wait "${SERVER_PID:-0}" 2>/dev/null || true
  [[ -f /tmp/grainfs-nfs-multi-bench.log ]] && cp /tmp/grainfs-nfs-multi-bench.log "$PROFILE_DIR/server.log" 2>/dev/null || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

echo "=== starting grainfs (HTTP=:$HTTP_PORT, NFS4=:$NFS4_PORT, pprof=:$PPROF_PORT) ==="
"$BINARY" serve \
  --data "$DATA_DIR" \
  --port "$HTTP_PORT" \
  --nfs4-port "$NFS4_PORT" \
  --nbd-port 0 \
  --cluster-key "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" \
  $(bench_encryption_args) \
  --pprof-port "$PPROF_PORT" \
  --log-level warn \
  > /tmp/grainfs-nfs-multi-bench.log 2>&1 &
SERVER_PID=$!

bench_wait_http_ready "http://127.0.0.1:$HTTP_PORT/" "server" 50 0.2
bench_wait_admin_socket "$DATA_DIR"
bench_bootstrap_iam_credentials "$BINARY" "$DATA_DIR"

echo "=== creating buckets and exports ==="
for i in $(seq 1 "$NUM_BUCKETS"); do
  bucket="bench-$i"
  bench_create_bucket_admin_retry "$BINARY" "$DATA_DIR" "$bucket"
  "$BINARY" nfs export add "$bucket" --endpoint "$DATA_DIR/admin.sock"
done

echo "=== mounting bucket exports ==="
for i in $(seq 1 "$NUM_BUCKETS"); do
  mnt="/mnt/grainfs-nfs-b$i"
  bench_colima_ssh sudo mkdir -p "$mnt"
  bench_colima_ssh sudo mount -t nfs4 -o "$MOUNT_OPTS" "${HOST_IP}:/bench-$i" "$mnt"
done

bench_colima_ssh sudo mkdir -p /mnt/grainfs-nfs-pseudo
bench_colima_ssh sudo mount -t nfs4 -o "$MOUNT_OPTS" "${HOST_IP}:/" /mnt/grainfs-nfs-pseudo

echo "=== NFS multi-export: ${NUM_BUCKETS} buckets x ${FIO_WORKERS_PER_BUCKET} fio workers ===" | tee "$SUMMARY"
start_t=$(date +%s)
PIDS=()
for i in $(seq 1 "$NUM_BUCKETS"); do
  (
    log="$PROFILE_DIR/fio_b$i.log"
    bench_colima_ssh bash -s <<SCRIPT > "$log" 2>&1
set -euo pipefail
BENCH_DIR="/mnt/grainfs-nfs-b$i/fio-\$(date +%s)"
sudo mkdir -p "\$BENCH_DIR"
sudo fio --name=mixed --directory="\$BENCH_DIR" --rw=randrw --rwmixread=70 \
  --bs=64k --size="$FIO_SIZE" --numjobs="$FIO_WORKERS_PER_BUCKET" \
  --ramp_time="$FIO_RAMP" --runtime="$FIO_RUNTIME" --time_based \
  --group_reporting --output-format=json
sudo rm -rf "\$BENCH_DIR"
SCRIPT
  ) &
  PIDS+=($!)
done

sleep $((FIO_RAMP + FIO_RUNTIME / 3))
curl -sS "http://127.0.0.1:${PPROF_PORT}/debug/pprof/profile?seconds=10" > "$PROFILE_DIR/cpu.pprof" || true
curl -sS "http://127.0.0.1:${PPROF_PORT}/debug/pprof/heap" > "$PROFILE_DIR/heap.pprof" || true

for pid in "${PIDS[@]}"; do
  wait "$pid"
done
end_t=$(date +%s)
echo "Total elapsed: $((end_t - start_t))s" | tee -a "$SUMMARY"

echo "" | tee -a "$SUMMARY"
echo "=== Per-bucket throughput (KB/s) ===" | tee -a "$SUMMARY"
total_read_kb=0
total_write_kb=0
for i in $(seq 1 "$NUM_BUCKETS"); do
  log="$PROFILE_DIR/fio_b$i.log"
  if command -v jq >/dev/null 2>&1; then
    read_kb=$(jq -r '.jobs[0].read.bw // "?"' "$log" 2>/dev/null || echo "?")
    write_kb=$(jq -r '.jobs[0].write.bw // "?"' "$log" 2>/dev/null || echo "?")
    if [[ "$read_kb" =~ ^[0-9]+$ ]]; then
      total_read_kb=$((total_read_kb + read_kb))
    fi
    if [[ "$write_kb" =~ ^[0-9]+$ ]]; then
      total_write_kb=$((total_write_kb + write_kb))
    fi
  else
    read_kb="? (jq missing)"
    write_kb="?"
  fi
  echo "  bench-$i: read=${read_kb}KB/s write=${write_kb}KB/s" | tee -a "$SUMMARY"
done
avg_read_kb=$((total_read_kb / NUM_BUCKETS))
avg_write_kb=$((total_write_kb / NUM_BUCKETS))
echo "  total: read=${total_read_kb}KB/s write=${total_write_kb}KB/s" | tee -a "$SUMMARY"
echo "  average_per_bucket: read=${avg_read_kb}KB/s write=${avg_write_kb}KB/s" | tee -a "$SUMMARY"

echo "" | tee -a "$SUMMARY"
echo "=== Pseudo-root READDIR latency (${READDIR_SAMPLES} samples, ms) ===" | tee -a "$SUMMARY"
LAT_FILE="$PROFILE_DIR/pseudo_root_lat_ms.txt"
: > "$LAT_FILE"
for _ in $(seq 1 "$READDIR_SAMPLES"); do
  t_s=$(bench_colima_ssh bash -lc "TIMEFORMAT='%R'; { time ls /mnt/grainfs-nfs-pseudo >/dev/null; } 2>&1" | tail -1)
  if [[ ! "$t_s" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "pseudo-root latency sample failed: $t_s" >&2
    exit 1
  fi
  awk "BEGIN { printf \"%.1f\\n\", ${t_s} * 1000 }" >> "$LAT_FILE"
done
p50=$(sort -n "$LAT_FILE" | awk -v n="$READDIR_SAMPLES" 'NR==int(n*0.50){print; exit}')
p99=$(sort -n "$LAT_FILE" | awk -v n="$READDIR_SAMPLES" 'NR==int(n*0.99){print; exit}')
echo "  p50=${p50}ms p99=${p99}ms" | tee -a "$SUMMARY"

echo "Results: $PROFILE_DIR"
echo "Compare contention: go tool pprof -top -focus='LockPath' $PROFILE_DIR/cpu.pprof"
