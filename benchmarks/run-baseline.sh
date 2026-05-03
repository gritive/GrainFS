#!/usr/bin/env bash
# Single-node S3 baseline benchmark.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

BINARY="${BINARY:-./bin/grainfs}"
K6="${K6:-k6}"
BASE_PORT="${BASE_PORT:-$(bench_free_port)}"
ACCESS_KEY="${ACCESS_KEY:-test}"
SECRET_KEY="${SECRET_KEY:-testtest}"
VUS="${VUS:-${MAX_VUS:-10}}"
DURATION="${DURATION:-30s}"
RAMP_UP="${RAMP_UP:-10s}"
RAMP_DOWN="${RAMP_DOWN:-5s}"
OBJECT_SIZE_KB="${OBJECT_SIZE_KB:-${SIZE_KB:-64}}"
BUCKET="${BUCKET:-bench}"

bench_require_command "$K6" "brew install k6"

echo "=== GrainFS Performance Baseline ==="
echo "Date: $(date)"
echo "Git commit: $(git rev-parse --short HEAD)"
echo ""

if [[ "${NO_BUILD:-0}" != "1" ]]; then
  echo "Building GrainFS..."
  make build
fi

# Start GrainFS server in background
DATA_DIR=$(mktemp -d)
echo "Starting GrainFS (data dir: $DATA_DIR)..."

"$BINARY" serve \
  --data "$DATA_DIR" \
  --port "$BASE_PORT" \
  --access-key "$ACCESS_KEY" \
  --secret-key "$SECRET_KEY" \
  --nfs4-port 0 \
  --nbd-port 0 \
  $(bench_encryption_args) \
  --rate-limit-ip-rps 0 \
  --rate-limit-user-rps 0 \
  > /dev/null 2>&1 &

GRAINFS_PID=$!
echo "GrainFS PID: $GRAINFS_PID"

trap 'kill "$GRAINFS_PID" 2>/dev/null || true; rm -rf "$DATA_DIR"' EXIT

# Verify server is actually running.
if ! ps -p $GRAINFS_PID > /dev/null 2>&1; then
    echo "ERROR: GrainFS failed to start (PID $GRAINFS_PID not running)"
    echo "Check logs in: $DATA_DIR"
    exit 1
fi

bench_wait_tcp_port "127.0.0.1" "$BASE_PORT" "GrainFS HTTP" 50 0.2

echo ""
echo "Running k6 benchmark..."
echo ""

# Run benchmark
"$K6" run \
  --env BASE_URL="http://127.0.0.1:$BASE_PORT" \
  --env ACCESS_KEY="$ACCESS_KEY" \
  --env SECRET_KEY="$SECRET_KEY" \
  --env BUCKET="$BUCKET" \
  --env MAX_VUS="$VUS" \
  --env DURATION="$DURATION" \
  --env RAMP_UP="$RAMP_UP" \
  --env RAMP_DOWN="$RAMP_DOWN" \
  --env OBJECT_SIZE_KB="$OBJECT_SIZE_KB" \
  "$BENCHMARKS_DIR/s3_bench.js"

echo ""
echo "=== Baseline Complete ==="
echo "Report saved to: benchmarks/report.json"
echo ""
echo "Next steps:"
echo "1. Save report as benchmarks/baselines/baseline-$(date +%Y%m%d).json if needed"
echo "2. Re-run after changes"
echo "3. Compare P50/P99 latency and operation counts"
