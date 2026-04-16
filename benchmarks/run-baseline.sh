#!/bin/bash
# Performance Baseline Measurement
# Run before implementing cross-protocol cache invalidation
# Results will be used to measure regression percentage

set -e

echo "=== GrainFS Performance Baseline ==="
echo "Date: $(date)"
echo "Git commit: $(git rev-parse --short HEAD)"
echo ""

# Build GrainFS
echo "Building GrainFS..."
go build -o bin/grainfs ./cmd/grainfs

# Start GrainFS server in background
DATA_DIR=$(mktemp -d)
echo "Starting GrainFS (data dir: $DATA_DIR)..."

./bin/grainfs serve \
  --data "$DATA_DIR" \
  --port 9000 \
  --access-key test \
  --secret-key testtest \
  > /dev/null 2>&1 &

GRAINFS_PID=$!
echo "GrainFS PID: $GRAINFS_PID"

# Wait for server to start
sleep 2

# Trap to kill server on exit
trap "kill $GRAINFS_PID 2>/dev/null; rm -rf $DATA_DIR" EXIT

echo ""
echo "Running k6 benchmark..."
echo ""

# Run benchmark
k6 run \
  --env BASE_URL=http://localhost:9000 \
  benchmarks/s3_bench.js

echo ""
echo "=== Baseline Complete ==="
echo "Report saved to: benchmarks/report.json"
echo ""
echo "Next steps:"
echo "1. Save report as baseline-single-protocol-$(date +%Y%m%d).json"
echo "2. Implement cache coherency"
echo "3. Re-run benchmark"
echo "4. Calculate regression: (cross_protocol - single_protocol) / single_protocol"
