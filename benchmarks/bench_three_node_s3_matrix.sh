#!/usr/bin/env bash
# Run the 3-node encrypted EC S3 throughput matrix used for bottleneck hunting.
#
# Defaults match the next-bottleneck plan:
#   workloads: pure PUT, PUT-heavy, mixed
#   object sizes: 256KiB, 1MiB, 4MiB, 16MiB
#   concurrency: 8, 16, 32, 64, 96
#
# Examples:
#   ./benchmarks/bench_three_node_s3_matrix.sh
#   PROFILE=1 SIZE_LIST_KB=4096 CONCURRENCY_LIST=64 MIX_LIST=pure-put ./benchmarks/bench_three_node_s3_matrix.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

if [[ "${NO_ENCRYPTION:-0}" == "1" ]]; then
  echo "[error] encryption is mandatory for this benchmark; do not set NO_ENCRYPTION=1" >&2
  exit 1
fi

SIZE_LIST_KB="${SIZE_LIST_KB:-256,1024,4096,16384}"
CONCURRENCY_LIST="${CONCURRENCY_LIST:-8,16,32,64,96}"
MIX_LIST="${MIX_LIST:-pure-put,put-heavy,mixed}"
INGRESS_LIST="${INGRESS_LIST:-round-robin}"
PROFILE="${PROFILE:-0}"
NODE_COUNT="${NODE_COUNT:-3}"
PROFILE_ROOT="${PROFILE_ROOT:-benchmarks/profiles/3-node-s3-matrix-$(date +%Y%m%d-%H%M%S)}"
BENCH_BASE_DIR="${BENCH_BASE_DIR:-/tmp/grainfs-3-node-s3-matrix-$(date +%Y%m%d-%H%M%S)}"

if [[ "$NODE_COUNT" != "3" ]]; then
  echo "[error] this matrix is intentionally fixed to NODE_COUNT=3; got ${NODE_COUNT}" >&2
  exit 1
fi

cleanup_matrix() {
  if [[ "${KEEP_BENCH_ARTIFACTS:-0}" != "1" ]]; then
    rm -rf "$BENCH_BASE_DIR"
  else
    echo "[matrix] bench data dir saved to $BENCH_BASE_DIR"
  fi
}
trap cleanup_matrix EXIT

mkdir -p "$PROFILE_ROOT"

matrix_summary="$PROFILE_ROOT/matrix-summary.md"
{
  echo "# GrainFS 3-node S3 bottleneck matrix"
  echo
  echo "- nodes: ${NODE_COUNT}"
  echo "- profile: ${PROFILE}"
  echo "- sizes KB: ${SIZE_LIST_KB}"
  echo "- concurrency: ${CONCURRENCY_LIST}"
  echo "- ingress: ${INGRESS_LIST}"
  echo "- mix: ${MIX_LIST}"
  echo "- encryption: mandatory"
  echo
} >"$matrix_summary"

IFS=',' read -ra SIZES <<<"$SIZE_LIST_KB"
FAIL=0
for size_kb in "${SIZES[@]}"; do
  size_root="$PROFILE_ROOT/size-${size_kb}kb"
  size_bench_dir="$BENCH_BASE_DIR/size-${size_kb}kb"
  echo "[matrix] size=${size_kb}KB profile=${PROFILE} out=${size_root}"
  if ! NODE_COUNT=3 \
    PROFILE="$PROFILE" \
    PROFILE_ROOT="$size_root" \
    BENCH_DIR="$size_bench_dir" \
    SIZE_KB="$size_kb" \
    CONCURRENCY_LIST="$CONCURRENCY_LIST" \
    INGRESS_LIST="$INGRESS_LIST" \
    MIX_LIST="$MIX_LIST" \
    "$SCRIPT_DIR/bench_two_node_s3_profile.sh"; then
    FAIL=1
  fi
  {
    echo
    echo "## ${size_kb}KB"
    echo
    if [[ -f "$size_root/summary.md" ]]; then
      sed -n '/| scenario |/,$p' "$size_root/summary.md"
    else
      echo "_summary missing_"
    fi
  } >>"$matrix_summary"
done

python3 - "$PROFILE_ROOT" "$matrix_summary" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
summary = Path(sys.argv[2])
rows = []
for report in root.glob("size-*kb/*/report.json"):
    try:
        r = json.loads(report.read_text())
    except Exception:
        continue
    put = r.get("put", {})
    get = r.get("get", {})
    put_bps = float(put.get("bytes_per_sec") or 0)
    get_bps = float(get.get("bytes_per_sec") or 0)
    rows.append({
        "size_kb": int(r.get("object_size_kb") or 0),
        "mix": r.get("mix", ""),
        "vus": int(r.get("vus") or 0),
        "ingress": r.get("ingress", ""),
        "put_ops": int(float(put.get("ops") or 0)),
        "put_p99_ms": float(put.get("p99_ms") or 0),
        "get_ops": int(float(get.get("ops") or 0)),
        "get_p99_ms": float(get.get("p99_ms") or 0),
        "failed_rate": float(r.get("http_req_failed") or 0),
        "throughput_mbps": (put_bps + get_bps) / 1024 / 1024,
        "path": str(report.parent.relative_to(root)),
    })

rows.sort(key=lambda x: (x["size_kb"], x["mix"], x["vus"]))
with summary.open("a", encoding="utf-8") as f:
    f.write("\n## Flattened Results\n\n")
    f.write("| size KB | mix | VUs | throughput MB/s | PUT ops | PUT p99 ms | GET ops | GET p99 ms | failed rate | path |\n")
    f.write("| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n")
    for r in rows:
        f.write(
            f"| {r['size_kb']} | {r['mix']} | {r['vus']} | {r['throughput_mbps']:.1f} | "
            f"{r['put_ops']} | {r['put_p99_ms']:.2f} | {r['get_ops']} | {r['get_p99_ms']:.2f} | "
            f"{r['failed_rate']:.4f} | `{r['path']}` |\n"
        )

    f.write("\n## Plateau Candidates\n\n")
    by_key = {}
    for r in rows:
        by_key.setdefault((r["size_kb"], r["mix"]), []).append(r)
    wrote = False
    for (size, mix), group in sorted(by_key.items()):
        group.sort(key=lambda x: x["vus"])
        last = None
        weak = 0
        notes = []
        for r in group:
            if last and last["throughput_mbps"] > 0:
                growth = (r["throughput_mbps"] - last["throughput_mbps"]) / last["throughput_mbps"]
                if growth < 0.10:
                    weak += 1
                    notes.append(f"{last['vus']}->{r['vus']}: {growth * 100:.1f}%")
                else:
                    weak = 0
            last = r
        if weak >= 1 and notes:
            wrote = True
            f.write(f"- {size}KB {mix}: weak scaling at {', '.join(notes[-2:])}\n")
    if not wrote:
        f.write("- none detected\n")
PY

echo ""
echo "=================================================================="
echo "  matrix summary"
echo "=================================================================="
cat "$matrix_summary"
echo ""
echo "[matrix] saved to $PROFILE_ROOT"
exit "$FAIL"
