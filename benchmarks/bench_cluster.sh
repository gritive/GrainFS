#!/usr/bin/env bash
# 3-node GrainFS cluster benchmark
# Usage: ./benchmarks/bench_cluster.sh [binary]
#
# Spins up 3 nodes with Raft + EC k=2,m=1, runs k6 against node-0,
# then tears everything down.

set -euo pipefail

BINARY="${1:-./bin/grainfs}"
if [[ ! -x "$BINARY" ]]; then
  echo "binary not found: $BINARY  (run: make build)" >&2
  exit 1
fi

# Free port helper
free_port() {
  python3 -c "import socket; s=socket.socket(); s.bind(('',0)); p=s.getsockname()[1]; s.close(); print(p)"
}

HTTP0=$(free_port); HTTP1=$(free_port); HTTP2=$(free_port)
RAFT0=$(free_port); RAFT1=$(free_port); RAFT2=$(free_port)

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

echo "=== starting 3-node cluster (EC k=2 m=1) ==="
for i in 0 1 2; do
  d=$(mktemp -d "grainfs-bench-node${i}-XXXX" -p /tmp)
  DIRS+=("$d")

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
    --nfs-port 0 \
    --nfs4-port 0 \
    --nbd-port 0 \
    --snapshot-interval 0 \
    --scrub-interval 0 \
    --log-level error \
    > /tmp/grainfs-node$i.log 2>&1 &
  PIDS+=($!)
  echo "  node-$i: HTTP=:$(http_port $i)  Raft=$(raft_addr $i)"
done

echo "=== waiting for cluster to become available (up to 30s) ==="
for attempt in $(seq 1 60); do
  status=$(curl -sf "http://127.0.0.1:$HTTP0/api/cluster/balancer/status" 2>/dev/null || true)
  nodes=$(echo "$status" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('nodes',[])))" 2>/dev/null || echo 0)
  if [[ "$nodes" -ge 3 ]]; then
    echo "  cluster ready: $nodes nodes visible (attempt $attempt)"
    break
  fi
  sleep 0.5
done

# Verify all 3 nodes visible
echo "=== cluster status ==="
curl -sf "http://127.0.0.1:$HTTP0/api/cluster/balancer/status" 2>/dev/null | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  nodes: {len(d.get(\"nodes\",[]))}, available: {d.get(\"available\")}')" 2>/dev/null || true

echo ""
echo "=== running k6 benchmark against node-0 (:$HTTP0) ==="
k6 run benchmarks/s3_bench.js \
  --env BASE_URL="http://127.0.0.1:$HTTP0" \
  --env BUCKET="bench-cluster" \
  --env AWS_ACCESS_KEY="$ACCESS_KEY" \
  --env AWS_SECRET_KEY="$SECRET_KEY" \
  2>&1
