#!/usr/bin/env python3
"""Phase 0 quorum-meta shadow — put_trace analysis & kill/pass verdict.

ROADMAP v2 Phase 0 is a *kill-only* perf spike: it answers ONE question —
is the leaderless quorum-meta-write tail *egregiously* slower than the
raft-commit tail at conc32? If yes, the epic is dead-on-arrival → STOP before
building Phases 1-4. If no, PASS — but NOT green-light (Phase 5 cross-binary
bench is the real gate).

This reads put_trace JSONL files emitted with GRAINFS_PUT_TRACE_FILE while
GRAINFS_QUORUM_META_SHADOW=1, and compares the within-run p99 of two stages:

  quorum_meta_write      (the shadow quorum write — candidate)
  data_raft_propose_meta (the existing raft commit — incumbent / denominator)

Pre-registered decision rule (set BEFORE measuring — do not eyeball):

  STOP  iff  p99(quorum_meta_write) >= STOP_RATIO * p99(data_raft_propose_meta)
  with STOP_RATIO = 10  (order-of-magnitude / egregious only).

Why so lenient — every uncertainty axis biases the ratio UP (toward STOP), and
a false-STOP kills a good epic (expensive) while a false-PASS is caught cheaply
by Phase 5. The residual over-count axes (all one-directional):

  1. double-durability: the shadow does its own fsync; Phase 3's real design
     co-locates meta with the shard (rides the shard WAL flush ~free).
  2. cross-PUT raft load: the shadow is measured at conc32 *with raft still
     running*; the real design has no object-data-plane raft at all.

  (self=local write closed the loopback axis; sync placement closed the
   intra-PUT raft<->shadow contention axis.)

  => 2-4x is the EXPECTED artifact band, proceed. Only ~10x means STOP.

Both stages are filtered to Error=="" events only — a handful of transient
timeouts must not blow up p99 and trigger a false-STOP. The ratio is a
within-run comparison (both stages under identical load), so it is valid
regardless of absolute load level.

Usage:
  analyze_put_trace.py trace-node1.jsonl trace-node2.jsonl ...
  analyze_put_trace.py 'traces/*.jsonl'        # shell-glob also accepted
"""

import glob
import json
import sys

STOP_RATIO = 10.0
NUM = "quorum_meta_write"
DEN = "data_raft_propose_meta"


def percentile(values, q):
    if not values:
        return None
    s = sorted(values)
    # nearest-rank
    rank = max(0, min(len(s) - 1, int(round((q / 100.0) * (len(s) - 1)))))
    return s[rank]


def load(paths):
    durs = {NUM: [], DEN: []}
    errs = {NUM: 0, DEN: 0}
    for pat in paths:
        for path in glob.glob(pat) or [pat]:
            try:
                fh = open(path)
            except OSError as e:
                print(f"warn: cannot open {path}: {e}", file=sys.stderr)
                continue
            with fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ev = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    stage = ev.get("stage")
                    if stage not in durs:
                        continue
                    if ev.get("error"):  # Error != "" → exclude
                        errs[stage] += 1
                        continue
                    durs[stage].append(ev.get("duration_micros", 0))
    return durs, errs


def fmt_us(v):
    return "n/a" if v is None else f"{v/1000.0:.2f} ms"


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2
    durs, errs = load(argv[1:])

    print("Phase 0 quorum-meta shadow — put_trace analysis")
    print("=" * 60)
    for stage in (DEN, NUM):
        n = len(durs[stage])
        print(f"\n{stage}: n={n} (excluded {errs[stage]} error events)")
        for q in (50, 90, 99):
            print(f"  p{q}: {fmt_us(percentile(durs[stage], q))}")

    den99 = percentile(durs[DEN], 99)
    num99 = percentile(durs[NUM], 99)
    print("\n" + "=" * 60)
    if not durs[NUM] or not durs[DEN]:
        print("INCONCLUSIVE: missing events for one or both stages.")
        print("  Check GRAINFS_QUORUM_META_SHADOW=1, GRAINFS_PUT_TRACE_FILE set,")
        print("  and that the bench drove EC PUTs (4+2) through this cluster.")
        return 3

    ratio = num99 / den99 if den99 else float("inf")
    print(f"p99 ratio  quorum_meta_write / data_raft_propose_meta = {ratio:.2f}x")
    print(f"pre-registered STOP threshold = {STOP_RATIO:.0f}x (egregious only)")
    if ratio >= STOP_RATIO:
        print("\nVERDICT: STOP — quorum write tail is egregiously slower.")
        print("  Re-evaluate the epic before building Phases 1-4.")
        return 1
    print("\nVERDICT: PASS (proceed to Phase 1).")
    print("  NOTE: PASS is necessary-not-sufficient — NOT a green light.")
    print("  2-4x is the expected over-count artifact. The real go/no-go is")
    print("  the Phase 5 cross-binary bench (PUT win AND GET/HEAD no-regress).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
