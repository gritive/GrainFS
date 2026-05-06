#!/usr/bin/env bash
# Run GET-only topology benchmarks for multiple node counts and range sizes.
#
# Defaults intentionally cover the behavior boundaries:
#   1 node  = solo/local
#   2 nodes = cluster, EC inactive
#   3 nodes = cluster, EC active with effective scaled config
#   6 nodes = cluster, target 4+2 EC can run at full width

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

NODES_LIST="${NODES_LIST:-1 2 3 6}"
RANGE_BYTES_LIST="${RANGE_BYTES_LIST:-0 4096 65536 1048576 8388608}"
RANGE_MODE="${RANGE_MODE:-sequential}"
VUS_LIST="${VUS_LIST:-${VUS:-1}}"
BASE_PORT_START="${BASE_PORT_START:-9400}"
BASE_RAFT_PORT_START="${BASE_RAFT_PORT_START:-19400}"
PPROF_PORT_START="${PPROF_PORT_START:-6400}"
REPORT_DIR="${REPORT_DIR:-benchmarks/topology-get-reports/$(date +%Y%m%d-%H%M%S)}"

mkdir -p "$REPORT_DIR"

echo "[matrix] nodes: $NODES_LIST"
echo "[matrix] ranges: $RANGE_BYTES_LIST (0=full, mode=${RANGE_MODE})"
echo "[matrix] vus: $VUS_LIST"
echo "[matrix] reports: $REPORT_DIR"

run_idx=0
for nodes in $NODES_LIST; do
  for range_bytes in $RANGE_BYTES_LIST; do
    for vus in $VUS_LIST; do
      base_port=$((BASE_PORT_START + run_idx * 100))
      base_raft_port=$((BASE_RAFT_PORT_START + run_idx * 100))
      pprof_port=$((PPROF_PORT_START + run_idx * 100))
      bench_dir="/tmp/grainfs-topology-get-n${nodes}-r${range_bytes}-v${vus}"

      echo ""
      echo "=================================================================="
      echo "  topology GET benchmark: ${nodes} node(s), range=${range_bytes}B, vus=${vus}"
      echo "=================================================================="

      NODE_COUNT="$nodes" \
        RANGE_BYTES="$range_bytes" \
        RANGE_MODE="$RANGE_MODE" \
        VUS="$vus" \
        BASE_PORT="$base_port" \
        BASE_RAFT_PORT="$base_raft_port" \
        PPROF_PORT="$pprof_port" \
        BENCH_DIR="$bench_dir" \
        "$SCRIPT_DIR/bench_topology_get_profile.sh"

      if [[ -f benchmarks/get_report.json ]]; then
        cp benchmarks/get_report.json "$REPORT_DIR/n${nodes}-r${range_bytes}-v${vus}.json"
      fi

      run_idx=$((run_idx + 1))
    done
  done
done

echo ""
echo "=================================================================="
echo "  topology GET matrix summary"
echo "=================================================================="
python3 - "$REPORT_DIR" <<'PY'
import json
import pathlib
import sys

def sort_key(path):
    parts = path.stem.split("-")
    values = {}
    for part in parts:
        if len(part) < 2:
            continue
        values[part[0]] = int(part[1:])
    return (values.get("n", 0), values.get("r", 0), values.get("v", 0))

report_dir = pathlib.Path(sys.argv[1])
print("nodes\trange_bytes\tvus\tops\tbytes_per_sec\tsuccess\tp50_ms\tp95_ms\tp99_ms\tavg_ms")
for path in sorted(report_dir.glob("n*.json"), key=sort_key):
    data = json.loads(path.read_text())
    parts = path.stem.split("-")
    nodes = parts[0][1:]
    range_bytes = data.get("range_bytes", 0)
    vus = data.get("vus", 0)
    get = data.get("get", {})
    print(
        f"{nodes}\t{range_bytes}\t{vus}\t{get.get('ops', 0)}\t"
        f"{get.get('bytes_per_sec', '0.00')}\t{get.get('success_rate', 0):.6f}\t"
        f"{get.get('p50_ms', '0.00')}\t{get.get('p95_ms', '0.00')}\t"
        f"{get.get('p99_ms', '0.00')}\t{get.get('avg_ms', '0.00')}"
    )
PY

echo ""
echo "[matrix] done: $REPORT_DIR"
