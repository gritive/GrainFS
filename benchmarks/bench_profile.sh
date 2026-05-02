#!/usr/bin/env bash
# 3-node GrainFS cluster benchmark with pprof profiling
# Usage: ./benchmarks/bench_profile.sh [binary]
#
# node-0 에 pprof 포트를 열어서 CPU/heap/goroutine 프로파일 수집

set -euo pipefail

BINARY="${1:-./bin/grainfs}"
if [[ ! -x "$BINARY" ]]; then
  echo "binary not found: $BINARY  (run: make build)" >&2
  exit 1
fi

PROFILE_DIR="benchmarks/profiles/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$PROFILE_DIR"

free_port() {
  python3 -c "import socket; s=socket.socket(); s.bind(('',0)); p=s.getsockname()[1]; s.close(); print(p)"
}

HTTP0=$(free_port); HTTP1=$(free_port); HTTP2=$(free_port)
RAFT0=$(free_port); RAFT1=$(free_port); RAFT2=$(free_port)
PPROF0=$(free_port)

CLUSTER_KEY="bench-cluster-key"
ACCESS_KEY="benchuser"
SECRET_KEY="benchpassword"

DIRS=()
PIDS=()

cleanup() {
  echo "=== tearing down cluster ==="
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  for d in "${DIRS[@]:-}"; do
    rm -rf "$d"
  done
}
trap cleanup EXIT

peers_for() {
  local idx=$1
  local peers=()
  for i in 0 1 2; do
    [[ $i == "$idx" ]] && continue
    case $i in
      0) peers+=("127.0.0.1:$RAFT0") ;;
      1) peers+=("127.0.0.1:$RAFT1") ;;
      2) peers+=("127.0.0.1:$RAFT2") ;;
    esac
  done
  IFS=','; echo "${peers[*]}"
}

raft_addr() {
  case $1 in
    0) echo "127.0.0.1:$RAFT0" ;;
    1) echo "127.0.0.1:$RAFT1" ;;
    2) echo "127.0.0.1:$RAFT2" ;;
  esac
}

http_port() {
  case $1 in
    0) echo "$HTTP0" ;;
    1) echo "$HTTP1" ;;
    2) echo "$HTTP2" ;;
  esac
}

echo "=== starting 3-node cluster (EC k=2 m=1) with pprof on node-0 :$PPROF0 ==="
for i in 0 1 2; do
  d=$(mktemp -d "grainfs-bench-node${i}-XXXX" -p /tmp)
  DIRS+=("$d")

  EXTRA_FLAGS=""
  if [[ $i == 0 ]]; then
    EXTRA_FLAGS="--pprof-port $PPROF0"
  fi

  "$BINARY" serve \
    --data "$d" \
    --port "$(http_port $i)" \
    --node-id "bench-node-$i" \
    --raft-addr "$(raft_addr $i)" \
    --peers "$(peers_for $i)" \
    --cluster-key "$CLUSTER_KEY" \
    --access-key "$ACCESS_KEY" \
    --secret-key "$SECRET_KEY" \
    --ec-data 2 \
    --ec-parity 1 \
    --no-encryption \
    --nfs4-port 0 \
    --nfs4-port 0 \
    --nbd-port 0 \
    --snapshot-interval 0 \
    --scrub-interval 0 \
    --log-level error \
    $EXTRA_FLAGS \
    > /tmp/grainfs-node$i.log 2>&1 &
  PIDS+=($!)
  if [[ $i == 0 ]]; then
    echo "  node-$i: HTTP=:$(http_port $i)  Raft=$(raft_addr $i)  pprof=:$PPROF0"
  else
    echo "  node-$i: HTTP=:$(http_port $i)  Raft=$(raft_addr $i)"
  fi
done

echo "=== waiting for cluster leader election (up to 90s) ==="
LEADER_PORT=""
for attempt in $(seq 1 180); do
  for port in $HTTP0 $HTTP1 $HTTP2; do
    st=$(curl -sf "http://127.0.0.1:$port/api/cluster/status" 2>/dev/null || true)
    if [ -z "$st" ]; then continue; fi
    node_state=$(echo "$st" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('state',''))" 2>/dev/null || echo "")
    if [[ "$node_state" == "Leader" ]]; then
      LEADER_PORT="$port"
      echo "  leader found at port $LEADER_PORT (attempt $attempt)"
      break 2
    fi
  done
  sleep 0.5
done

if [[ -z "$LEADER_PORT" ]]; then
  echo "  WARNING: no leader found, falling back to node-0 (:$HTTP0)" >&2
  LEADER_PORT="$HTTP0"
fi

echo "=== cluster status ==="
curl -sf "http://127.0.0.1:$LEADER_PORT/api/cluster/status" 2>/dev/null | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  node_id: {d.get(\"node_id\")}, state: {d.get(\"state\")}, term: {d.get(\"term\")}')" 2>/dev/null || true

echo ""
echo "=== collecting heap baseline (pre-benchmark) ==="
curl -sf "http://127.0.0.1:$PPROF0/debug/pprof/heap" -o "$PROFILE_DIR/heap_pre.pb.gz" && \
  echo "  heap_pre.pb.gz saved" || echo "  heap baseline failed"

echo ""
echo "=== running k6 benchmark (+ concurrent CPU profile 30s) ==="

# CPU 프로파일: 벤치마크와 동시에 30s 수집
(
  sleep 5  # 워밍업 이후 시작
  echo "  [pprof] starting 30s CPU profile..."
  curl -sf "http://127.0.0.1:$PPROF0/debug/pprof/profile?seconds=30" \
    -o "$PROFILE_DIR/cpu.pb.gz" && \
    echo "  [pprof] cpu.pb.gz saved" || echo "  [pprof] CPU profile failed"
) &
PPROF_PID=$!

BENCH_BUCKET="bench-profile-$(date +%s)"
echo "  bench bucket: $BENCH_BUCKET"
k6 run benchmarks/s3_bench.js \
  --env BASE_URL="http://127.0.0.1:$LEADER_PORT" \
  --env BUCKET="$BENCH_BUCKET" \
  --env ACCESS_KEY="$ACCESS_KEY" \
  --env SECRET_KEY="$SECRET_KEY" \
  2>&1 | tee "$PROFILE_DIR/k6_output.txt" || true  # continue to collect profiles even if thresholds fail

wait $PPROF_PID 2>/dev/null || true

echo ""
echo "=== collecting post-benchmark profiles ==="
curl -sf "http://127.0.0.1:$PPROF0/debug/pprof/heap" \
  -o "$PROFILE_DIR/heap_post.pb.gz" && echo "  heap_post.pb.gz saved"
curl -sf "http://127.0.0.1:$PPROF0/debug/pprof/goroutine?debug=1" \
  -o "$PROFILE_DIR/goroutine.txt" && echo "  goroutine.txt saved"
curl -sf "http://127.0.0.1:$PPROF0/debug/pprof/mutex" \
  -o "$PROFILE_DIR/mutex.pb.gz" && echo "  mutex.pb.gz saved"
curl -sf "http://127.0.0.1:$PPROF0/debug/pprof/block" \
  -o "$PROFILE_DIR/block.pb.gz" && echo "  block.pb.gz saved"
curl -sf "http://127.0.0.1:$PPROF0/debug/pprof/allocs" \
  -o "$PROFILE_DIR/allocs.pb.gz" && echo "  allocs.pb.gz saved"

echo ""
echo "=== profile files saved to $PROFILE_DIR ==="
ls -lh "$PROFILE_DIR/"

echo ""
echo "=== quick CPU top-10 (go tool pprof) ==="
go tool pprof -top -nodecount=10 "$PROFILE_DIR/cpu.pb.gz" 2>/dev/null || echo "  pprof analysis failed"

echo ""
echo "=== memory growth (heap_post - heap_pre top-5) ==="
go tool pprof -top -nodecount=5 "$PROFILE_DIR/heap_post.pb.gz" 2>/dev/null || echo "  heap analysis failed"

echo ""
echo "PROFILE_DIR=$PROFILE_DIR"
