#!/usr/bin/env python3
"""Aggregate GrainFS cross-binary benchmark runs into a verdict report."""

from __future__ import annotations

import os
import statistics
import sys


OPS = ["put", "get", "stat"]
HEAD = {"put": "PUT", "get": "GET", "stat": "HEAD"}


def load(profile_dir: str) -> dict[str, dict[str, tuple[float, float, int]]]:
    out: dict[str, dict[str, tuple[float, float, int]]] = {}
    path = os.path.join(profile_dir, "warp-results.tsv")
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 7:
                continue
            target, _mode, op, mib, objs, errors, _artifact_dir = parts
            out.setdefault(op.lower(), {})[target] = (float(mib), float(objs), int(errors))
    return out


def median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def fmt_ratio(value: float | None, precision: int = 3) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{precision}f}x"


def main(argv: list[str]) -> int:
    if len(argv) != 8:
        print(
            "usage: ab_verdict.py <root> <runs> <put_win_min> <noreg_min> "
            "<new_ref> <old_ref> <anchor:0|1>",
            file=sys.stderr,
        )
        return 2

    root, runs_s, put_win_min_s, noreg_min_s, new_ref, old_ref, anchor_s = argv[1:]
    runs = int(runs_s)
    put_win_min = float(put_win_min_s)
    noreg_min = float(noreg_min_s)
    anchor = anchor_s == "1"

    ratios = {op: [] for op in OPS}
    new_vs_anchor = {op: [] for op in OPS}
    old_vs_anchor = {op: [] for op in OPS}
    errors = {op: 0 for op in OPS}
    missing: list[str] = []

    for run in range(1, runs + 1):
        new_data = load(os.path.join(root, f"run{run}", "new"))
        old_data = load(os.path.join(root, f"run{run}", "old"))
        for op in OPS:
            new_grainfs = new_data.get(op, {}).get("grainfs-cluster")
            old_grainfs = old_data.get(op, {}).get("grainfs-cluster")
            if not new_grainfs or not old_grainfs:
                missing.append(f"run{run}:{op}")
                continue
            errors[op] += new_grainfs[2] + old_grainfs[2]
            if old_grainfs[0] > 0:
                ratios[op].append(new_grainfs[0] / old_grainfs[0])
            if anchor:
                new_anchor = new_data.get(op, {}).get("minio-cluster")
                old_anchor = old_data.get(op, {}).get("minio-cluster")
                if new_anchor and new_anchor[0] > 0:
                    new_vs_anchor[op].append(new_grainfs[0] / new_anchor[0])
                if old_anchor and old_anchor[0] > 0:
                    old_vs_anchor[op].append(old_grainfs[0] / old_anchor[0])

    lines: list[str] = []
    lines.append("# Cross-binary A/B verdict\n")
    lines.append(f"- NEW = `{new_ref}`  vs  OLD = `{old_ref}`\n")
    lines.append(f"- runs: {runs}  |  anchor: {'minio-cluster' if anchor else 'none'}\n")
    lines.append(
        f"- decision rule: PUT new/old >= {put_win_min:.2f}x (win) AND "
        f"GET & HEAD new/old >= {noreg_min:.2f}x (no-regress)\n\n"
    )
    lines.append("| op | new/old (median) | new/anchor | old/anchor | errors | per-run new/old |\n")
    lines.append("|----|------------------|------------|------------|--------|-----------------|\n")

    verdict_put: float | None = None
    verdict_noreg: dict[str, float | None] = {}
    for op in OPS:
        op_median = median(ratios[op])
        new_anchor_median = median(new_vs_anchor[op])
        old_anchor_median = median(old_vs_anchor[op])
        per_run = ", ".join(f"{ratio:.2f}" for ratio in ratios[op]) or "-"
        lines.append(
            f"| {HEAD[op]} | {fmt_ratio(op_median)} | "
            f"{fmt_ratio(new_anchor_median, 2) if new_anchor_median is not None else '-'} | "
            f"{fmt_ratio(old_anchor_median, 2) if old_anchor_median is not None else '-'} | "
            f"{errors[op]} | {per_run} |\n"
        )
        if op == "put":
            verdict_put = op_median
        else:
            verdict_noreg[op] = op_median

    put_ok = verdict_put is not None and verdict_put >= put_win_min
    noreg_ok = all(value is not None and value >= noreg_min for value in verdict_noreg.values())
    total_errors = sum(errors.values())

    lines.append("\n## Gate\n")
    lines.append(
        f"- PUT win: {'PASS' if put_ok else 'FAIL'} "
        f"(PUT {fmt_ratio(verdict_put)} >= {put_win_min:.2f}x)\n"
    )
    for op in ("get", "stat"):
        value = verdict_noreg[op]
        lines.append(
            f"- {HEAD[op]} no-regress: "
            f"{'PASS' if (value is not None and value >= noreg_min) else 'FAIL'} "
            f"({HEAD[op]} {fmt_ratio(value)} >= {noreg_min:.2f}x)\n"
        )
    if total_errors:
        lines.append(f"- warp errors observed ({total_errors}): verdict is suspect; investigate before deciding\n")
    if missing:
        lines.append(f"- missing samples: {', '.join(missing)}\n")

    passed = put_ok and noreg_ok and total_errors == 0 and not missing
    lines.append(f"\n## VERDICT: {'PASS' if passed else 'FAIL'}\n")
    lines.append(
        "\n> Harness output only. Use same-zone, back-to-back runs with representative "
        "object size, concurrency, and duration before making a release decision.\n"
    )

    os.makedirs(root, exist_ok=True)
    with open(os.path.join(root, "verdict.md"), "w", encoding="utf-8") as f:
        f.writelines(lines)
    sys.stdout.write("".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
