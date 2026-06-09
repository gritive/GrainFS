# Phase 0 — Quorum-Meta Shadow Write (kill-only perf spike)

> **Experimental, measurement-only. NOT a user feature.** This is a developer
> spike harness for ROADMAP v2 Phase 0. The `GRAINFS_QUORUM_META_SHADOW` env knob
> exists solely to measure a design hypothesis; it is gated **OFF by default** and
> has no production use. Do not enable it on a production cluster.

## What it measures

PUT is 0.41x vs external S3 — structurally, because every object PUT pays two raft
consensus rounds. The epic hypothesis: replace the object-data-plane raft commit
with a **leaderless per-node quorum metadata write**. Phase 0 answers the single
*kill-only* question before any expensive build:

> Is the quorum-meta-write tail **egregiously** slower than the raft-commit tail at
> conc32? If yes → STOP (epic is dead-on-arrival). If no → PASS (proceed to Phase 1).

A SHADOW quorum write runs alongside the real `data_raft` propose (env-gated). It is
**not load-bearing**: GET/LIST/DELETE never read it, failures never fail the PUT, the
data is discarded. It only emits a `quorum_meta_write` put_trace stage, co-measured
within the same run as `data_raft_propose_meta`.

**PASS is necessary-not-sufficient.** It only clears an obvious dealbreaker. The real
go/no-go is the Phase 5 cross-binary bench (PUT win **AND** GET/HEAD no-regress).

## Fidelity & the pre-registered decision rule

The shadow is a **conservative upper bound** on the real Phase 3 cost. Every
uncertainty axis biases the measured ratio **up** (toward STOP), and a false-STOP
kills a good epic (expensive) while a false-PASS is caught cheaply by Phase 5. So
the STOP line is deliberately lenient:

```
STOP  iff  p99(quorum_meta_write) >= 10 * p99(data_raft_propose_meta)
```

Residual one-directional over-count axes (all inflate the ratio):

1. **double-durability** — the shadow does its own fsync; Phase 3's real design
   co-locates meta with the shard (rides the shard WAL flush, ~free).
2. **cross-PUT raft load** — measured at conc32 *with raft still running*; the real
   design has no object-data-plane raft at all.

(self=local write closed the loopback axis; synchronous placement closed the
intra-PUT raft↔shadow contention axis.)

→ **2-4x is the expected artifact band — proceed. Only ~10x means STOP.**

The ratio is computed on `Error==""` events only (transient timeouts must not blow up
p99) and is a within-run comparison (both stages under identical load), so it is valid
regardless of absolute load level.

## Procedure (user-run; needs a real multi-node cluster — e.g. GCP)

1. **Build** baseline binary from this branch.
2. **Bring up a 4-node cluster, EC 4+2** (K=4 = majority — avoids the K=2
   degenerate-geometry trap and aligns quorum size with raft majority).
3. **On every node**, set before `serve`:
   ```
   export GRAINFS_QUORUM_META_SHADOW=1
   export GRAINFS_PUT_TRACE_FILE=/var/tmp/put-trace-$(hostname).jsonl
   ```
4. **Drive conc32 PUT load** (warp or the put-trace bench harness) long enough for a
   stable p99 (≥ a few thousand PUTs/node).
5. **Collect** every node's `GRAINFS_PUT_TRACE_FILE`.
6. **Analyze**:
   ```
   ./analyze_put_trace.py /path/to/put-trace-*.jsonl
   ```

## Report template

Record this verbatim — and separate the two prongs (do not let the mechanism's
green tests imply a measurement result):

```
Phase 0 quorum-meta shadow — RESULT
  cluster:        4-node, EC 4+2, conc32, object size = <...>
  binary:         <commit>
  PUTs sampled:   <n per node>
  data_raft_propose_meta p99: <ms>
  quorum_meta_write     p99: <ms>
  ratio (num/den):           <x>
  pre-registered STOP line:  10x
  VERDICT:                   PASS | STOP
  caveat:                    PASS ≠ green-light; Phase 5 cross-binary bench decides.
```

**Overclaim guard:** the in-proc unit tests prove only the *mechanism* (gated on →
fans out / off → inert). They are NOT a filter result. Only this GCP run decides
kill/pass. Never write "Phase 0 passed" without the GCP ratio above.
