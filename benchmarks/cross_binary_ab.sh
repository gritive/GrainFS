#!/usr/bin/env bash
# Phase 5 cross-binary A/B — the merge go/no-go decision gate.
#
# Compares the NEW GrainFS binary (devel: data_raft + meta-index removed = "신규
# 전체") against the OLD binary (master: both consensus rounds still present =
# "옛 전체") with the rigor ROADMAP Phase 5 names:
#   - same host, back-to-back   (NEW and OLD arms run on the same machine, paired)
#   - within-run ratio          (new/old computed per run, then aggregated)
#   - multi-run                 (RUNS arms, median taken to suppress per-run noise)
#   - external S3 anchor        (optional minio-cluster, so each binary's drift vs a
#                                fixed reference is visible, not just new/old)
#   - scope PUT + GET + HEAD     (warp put,get,stat — stat == S3 HEAD)
#
# This is a thin orchestrator over benchmarks/bench_s3_compat_compare.sh, which
# already boots a local 4-node GrainFS cluster, runs warp through signed S3
# requests, and (optionally) boots a local minio cluster anchor. We reuse that
# machinery verbatim and only add: build-two-refs, pair the arms, and apply the
# pre-registered decision rule. See benchmarks/cross_binary_ab/README.md for the
# decision rule and how to run the multi-node (GCP) version.
#
# Examples:
#   # local smoke (NOT the Phase 5 verdict — dev bench != parity):
#   RUNS=1 WARP_DURATION=10s ANCHOR=0 ./benchmarks/cross_binary_ab.sh
#
#   # representative cluster A/B with anchor:
#   RUNS=3 WARP_OPS=put,get,stat WARP_OBJ_SIZE=10MiB WARP_CONCURRENT=32 \
#     WARP_DURATION=1m ANCHOR=1 ./benchmarks/cross_binary_ab.sh
#
#   # pre-built binaries (e.g. on a GCP VM where you built both refs once):
#   NEW_BIN=/tmp/grainfs-devel OLD_BIN=/tmp/grainfs-master \
#     ./benchmarks/cross_binary_ab.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
cd "$REPO_ROOT"

NEW_REF="${NEW_REF:-devel}"        # "新 전체" branch; "WORKTREE" = build current tree
OLD_REF="${OLD_REF:-master}"       # "옛 전체" baseline
NEW_BIN="${NEW_BIN:-}"             # pre-built binary; skips build when set
OLD_BIN="${OLD_BIN:-}"
RUNS="${RUNS:-3}"
ANCHOR="${ANCHOR:-1}"              # 1 = include minio-cluster external anchor
WARP_OPS="${WARP_OPS:-put,get,stat}"
STAMP="${STAMP:-$(date +%Y%m%d-%H%M%S)}"
CROSS_ROOT="${CROSS_ROOT:-benchmarks/profiles/cross-binary-ab-$STAMP}"

# Pre-registered decision rule (ROADMAP Phase 5 merge-blocker, both required):
#   ① PUT win:        new/old PUT throughput >= PUT_WIN_MIN  (win beyond noise band)
#   ② GET/HEAD no-reg: new/old GET and HEAD throughput >= NOREG_MIN
PUT_WIN_MIN="${PUT_WIN_MIN:-1.05}"
NOREG_MIN="${NOREG_MIN:-0.95}"

mkdir -p "$CROSS_ROOT"

# ---- build the two binaries (transient worktree per ref; cleaned up) ----------
build_ref() { # ref outpath
  local ref="$1" out="$2"
  out="$(cd "$(dirname "$out")" && pwd)/$(basename "$out")" # absolutize before cd
  if [[ "$ref" == "WORKTREE" ]]; then
    echo "[build] $ref -> $out (current working tree)" >&2
    go build -ldflags "-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
      -o "$out" ./cmd/grainfs/ >&2
    return
  fi
  local wt
  wt="$(mktemp -d "${TMPDIR:-/tmp}/grainfs-xbin-${ref//\//_}.XXXXXX")"
  echo "[build] $ref -> $out (transient worktree $wt)" >&2
  git worktree add --detach "$wt" "$ref" >&2
  ( cd "$wt" && go build \
      -ldflags "-s -w -X main.version=$(git describe --tags --always 2>/dev/null || echo dev)" \
      -o "$out" ./cmd/grainfs/ ) >&2
  git worktree remove --force "$wt" >&2
}

if [[ -z "$NEW_BIN" ]]; then
  NEW_BIN="$CROSS_ROOT/grainfs-new"
  build_ref "$NEW_REF" "$NEW_BIN"
fi
if [[ -z "$OLD_BIN" ]]; then
  OLD_BIN="$CROSS_ROOT/grainfs-old"
  build_ref "$OLD_REF" "$OLD_BIN"
fi
NEW_BIN="$(cd "$(dirname "$NEW_BIN")" && pwd)/$(basename "$NEW_BIN")"
OLD_BIN="$(cd "$(dirname "$OLD_BIN")" && pwd)/$(basename "$OLD_BIN")"
echo "[xbin] NEW=$NEW_BIN ($NEW_REF)  OLD=$OLD_BIN ($OLD_REF)" >&2

targets="grainfs-cluster"
[[ "$ANCHOR" == "1" ]] && targets="grainfs-cluster,minio-cluster"

# ---- run paired arms ----------------------------------------------------------
# One arm = one full bench_s3_compat_compare invocation against a single binary.
# NEW and OLD arms run back-to-back within each run on the same host.
run_arm() { # arm-label binary out-profile
  local label="$1" bin="$2" prof="$3"
  echo "### $(date +%H:%M:%S) arm=$label binary=$bin profile=$prof" >&2
  BINARY="$bin" TARGETS="$targets" WARP_OPS="$WARP_OPS" \
    PROFILE_ROOT="$prof" \
    "$SCRIPT_DIR/bench_s3_compat_compare.sh" >&2
}

for run in $(seq 1 "$RUNS"); do
  run_arm "new-run$run" "$NEW_BIN" "$CROSS_ROOT/run$run/new"
  run_arm "old-run$run" "$OLD_BIN" "$CROSS_ROOT/run$run/old"
done

# ---- aggregate + apply decision rule -----------------------------------------
echo "[xbin] aggregating $RUNS run(s) -> $CROSS_ROOT/verdict.md" >&2
python3 - "$CROSS_ROOT" "$RUNS" "$PUT_WIN_MIN" "$NOREG_MIN" "$NEW_REF" "$OLD_REF" "$ANCHOR" <<'PY'
import os, sys, statistics

root, runs, put_win_min, noreg_min, new_ref, old_ref, anchor = sys.argv[1:]
runs = int(runs); put_win_min = float(put_win_min); noreg_min = float(noreg_min)
anchor = anchor == "1"

# warp-results.tsv rows: target, mode, op, mib, objs, errors, artifact_dir
def load(prof):
    out = {}  # op -> {target: (mib, objs, errors)}
    p = os.path.join(prof, "warp-results.tsv")
    if not os.path.exists(p):
        return out
    with open(p) as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 7:
                continue
            target, _mode, op, mib, objs, errors, _art = parts
            out.setdefault(op.lower(), {})[target] = (float(mib), float(objs), int(errors))
    return out

OPS = ["put", "get", "stat"]
HEAD = {"put": "PUT", "get": "GET", "stat": "HEAD"}
# per op: list of within-run new/old throughput(MiB/s) ratios + anchor ratios + errors
ratios = {op: [] for op in OPS}
new_vs_anchor = {op: [] for op in OPS}
old_vs_anchor = {op: [] for op in OPS}
errs = {op: 0 for op in OPS}
missing = []

for run in range(1, runs + 1):
    newd = load(os.path.join(root, f"run{run}", "new"))
    oldd = load(os.path.join(root, f"run{run}", "old"))
    for op in OPS:
        ng = newd.get(op, {}).get("grainfs-cluster")
        og = oldd.get(op, {}).get("grainfs-cluster")
        if not ng or not og:
            missing.append(f"run{run}:{op}")
            continue
        errs[op] += ng[2] + og[2]
        if og[0] > 0:
            ratios[op].append(ng[0] / og[0])
        if anchor:
            na = newd.get(op, {}).get("minio-cluster")
            oa = oldd.get(op, {}).get("minio-cluster")
            if na and na[0] > 0:
                new_vs_anchor[op].append(ng[0] / na[0])
            if oa and oa[0] > 0:
                old_vs_anchor[op].append(og[0] / oa[0])

def med(xs):
    return statistics.median(xs) if xs else None

lines = []
lines.append(f"# Phase 5 cross-binary A/B verdict\n")
lines.append(f"- NEW (신규 전체) = `{new_ref}`  vs  OLD (옛 전체) = `{old_ref}`\n")
lines.append(f"- runs: {runs}  |  anchor: {'minio-cluster' if anchor else 'none'}\n")
lines.append(f"- decision rule: ① PUT new/old >= {put_win_min:.2f}x (win)  AND  "
             f"② GET & HEAD new/old >= {noreg_min:.2f}x (no-regress)\n\n")
lines.append("| op | new/old (median) | new/anchor | old/anchor | errors | per-run new/old |\n")
lines.append("|----|------------------|-----------|-----------|--------|-----------------|\n")
verdict_put = None
verdict_noreg = {}
for op in OPS:
    m = med(ratios[op])
    na = med(new_vs_anchor[op]); oa = med(old_vs_anchor[op])
    per = ", ".join(f"{r:.2f}" for r in ratios[op]) or "-"
    lines.append(
        f"| {HEAD[op]} | {('%.3fx'%m) if m is not None else 'n/a'} | "
        f"{('%.2fx'%na) if na is not None else '-'} | "
        f"{('%.2fx'%oa) if oa is not None else '-'} | {errs[op]} | {per} |\n"
    )
    if op == "put":
        verdict_put = m
    else:
        verdict_noreg[op] = m

lines.append("\n")
put_ok = verdict_put is not None and verdict_put >= put_win_min
noreg_ok = all(v is not None and v >= noreg_min for v in verdict_noreg.values())
total_errs = sum(errs.values())

lines.append("## Gate\n")
lines.append(f"- ① PUT win: {'PASS' if put_ok else 'FAIL'} "
             f"(PUT {('%.3fx'%verdict_put) if verdict_put is not None else 'n/a'} "
             f">= {put_win_min:.2f}x)\n")
for op in ("get", "stat"):
    v = verdict_noreg[op]
    lines.append(f"- ② {HEAD[op]} no-regress: "
                 f"{'PASS' if (v is not None and v >= noreg_min) else 'FAIL'} "
                 f"({HEAD[op]} {('%.3fx'%v) if v is not None else 'n/a'} >= {noreg_min:.2f}x)\n")
if total_errs:
    lines.append(f"- ⚠️ warp errors observed ({total_errs}): verdict is suspect, investigate before deciding\n")
if missing:
    lines.append(f"- ⚠️ missing samples: {', '.join(missing)}\n")

go = put_ok and noreg_ok and total_errs == 0 and not missing
lines.append(f"\n## VERDICT: {'GO (merge devel→master)' if go else 'NO-GO (discard branch, master 무손상)'}\n")
lines.append("\n> harness output, not authoritative until run multi-node with the "
             "Phase 5 rigor (GCP back-to-back, representative obj size/concurrency). "
             "dev bench != parity.\n")

with open(os.path.join(root, "verdict.md"), "w") as f:
    f.writelines(lines)
sys.stdout.write("".join(lines))
PY

echo "### $(date +%H:%M:%S) DONE -> $CROSS_ROOT/verdict.md" >&2
