# Phase 5 — cross-binary A/B (merge go/no-go)

The ROADMAP Phase 5 decision gate. Compares the **new** GrainFS binary (the
`devel` branch, where both consensus rounds were removed — `data_raft` in
Phase 3, the meta-index in Phase 4 = "신규 전체") against the **old** binary
(`master`, both consensus rounds present = "옛 전체") and decides whether
`devel` may merge into `master`.

Driver: [`../cross_binary_ab.sh`](../cross_binary_ab.sh). It is a thin
orchestrator over [`../bench_s3_compat_compare.sh`](../bench_s3_compat_compare.sh)
(cluster boot + warp + optional minio anchor) — it builds two refs, runs the
NEW and OLD arms back-to-back on the same host, and applies the pre-registered
decision rule below.

## Scope

`PUT + GET + HEAD` (warp `put,get,stat`; `stat` == S3 HEAD). GET/HEAD are
in scope because the read path changed from a single raft-read to a multi
quorum-read — regression risk must be measured, not assumed.

## Pre-registered decision rule (merge-blocker, both required)

The two conditions are **equal-weight**; failing either discards the branch.

1. **① PUT win** — `new/old` PUT throughput (MiB/s) median ≥ `PUT_WIN_MIN`
   (default `1.05x`, i.e. a win beyond the per-run noise band).
2. **② GET/HEAD no-regress** — `new/old` GET throughput **and** `new/old`
   HEAD throughput median ≥ `NOREG_MIN` (default `0.95x`).

`VERDICT = GO` iff ① AND ② AND zero warp errors AND no missing samples.
Otherwise `NO-GO` → discard the branch; `master` is untouched (reversibility =
git, per ROADMAP: the branch only merges *after* this gate passes).

The thresholds are intentionally exposed as env vars so the rule is reviewable
and the noise band can be set from the observed per-run spread before the run
that decides — do not retune them *after* seeing the verdict.

## Rigor (ROADMAP原칙)

- **same host, back-to-back** — NEW and OLD arms run paired on one machine.
- **within-run ratio** — `new/old` is computed per run, then median across runs;
  never compare a NEW arm against an OLD arm from a different run/host.
- **multi-run** — `RUNS` (default 3) to suppress per-run noise.
- **external S3 anchor** — `ANCHOR=1` (default) also runs a minio cluster, so
  each binary's drift vs a fixed reference is visible (`new/anchor`,
  `old/anchor`), not only `new/old`. This catches a run where *both* GrainFS
  arms were contaminated by host state.

## Running it

### Multi-node (this is the actual Phase 5 verdict)

Phase 5's verdict must come from a **multi-node** run (the single-node confounds
in prior epics do not transfer to cluster behavior). Provision N=4 VMs
(e.g. GCP `n2-standard-4`, Spot), build both refs once, then point the harness
at pre-built binaries:

```bash
NEW_BIN=/tmp/grainfs-devel OLD_BIN=/tmp/grainfs-master \
RUNS=3 WARP_OBJ_SIZE=10MiB WARP_CONCURRENT=32 WARP_DURATION=1m ANCHOR=1 \
  ./benchmarks/cross_binary_ab.sh
```

On a multi-node cluster, supply the external host list to
`bench_s3_compat_compare.sh` via `GRAINFS_CLUSTER_URL` / `MINIO_CLUSTER_URL`
(see [`../README.md`](../README.md)) instead of letting it boot a local cluster.

### Local smoke (NOT the verdict)

A local run only proves the harness executes end-to-end. It is **not** the
Phase 5 verdict — `dev bench != parity`, and a 4-node *local* cluster shares one
host's disk/CPU so the consensus-removal signal is swamped:

```bash
RUNS=1 WARP_DURATION=10s ANCHOR=0 ./benchmarks/cross_binary_ab.sh
```

## Output

`benchmarks/profiles/cross-binary-ab-<stamp>/verdict.md` — the per-op
`new/old` ratio table, the gate evaluation (① / ②), and the `GO`/`NO-GO`
verdict. Raw per-arm artifacts live under `run<N>/{new,old}/` (each is a full
`bench_s3_compat_compare` profile with `warp-results.tsv`, `summary.md`, and
warp/analyze output).

## Honesty note

The branch carries more than consensus removal (deterministic placement,
strip-down). Per ROADMAP this confound is accepted: the gate measures "신규
전체 vs 옛 전체", not "consensus alone". Prior single-node / sharded-index
findings ("0.41x is structural", "overlapped not additive") are *suggestive*
that the PUT win may be small — but they are GrainFS-vs-minio and
sharded-index, not this devel-vs-master cluster A/B. This gate exists to settle
it empirically; do not pre-write the verdict from those.
