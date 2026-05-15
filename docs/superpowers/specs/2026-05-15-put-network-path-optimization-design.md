# PUT Network Path Optimization Design

## Goal

Measure and, only where justified, optimize `GrainFS` PUT network flow without changing the current durability model:

1. Object bytes are written to shard owners over QUIC shard streams.
2. Data-group Raft commits object metadata (`CmdPutObjectMeta`).
3. Meta-Raft commits the global object index (`ObjectIndexEntry`).

The design is intentionally phased. Phase A attributes latency. Phase B applies low-risk optimizations based on those measurements. Phase C remains a measured follow-up for direct shard write.

## Current Flow

Local leader path:

```text
client
  -> coordinator / local data-group leader
  -> shard owners
  -> data-group raft CmdPutObjectMeta
  -> metaRaft ObjectIndex
```

Forwarded path:

```text
client
  -> coordinator non-leader
  -> group forward stream
  -> data-group leader
  -> shard owners
  -> data-group raft CmdPutObjectMeta
  -> metaRaft ObjectIndex
```

The existing shape is sound: Raft and Meta-Raft carry control and metadata, not object bytes. The optimization question is whether the forwarded path's extra body hop or stream contention is material enough to justify changes.

## Phase A: Measurement

Extend the existing PUT stage instrumentation, reusing `observePutStage` where possible. Add only missing attribution points.

Coordinator measurements:

- Route decision latency.
- Local vs forwarded write path.
- `ForwardSender.Send` vs `ForwardSender.SendStream`.
- Forward body bytes.
- Group ID and whether leader hint ordering was used.

Forward receiver measurements:

- Forward body receive and dispatch latency.
- `dg.Backend().PutObject` duration when invoked from a forwarded request.

Leader backend measurements:

- Existing stages: `head_bucket`, `quarantine_check`, `spool_object`, `placement`, `write_shards`, `propose_meta`.
- Ensure large-object streamed paths and small-object memory paths expose comparable stage names.

Shard service measurements:

- Local shard write duration.
- Remote shard stream write duration.
- Slowest shard target per PUT, without adding high-cardinality peer labels to default metrics.

Meta index measurements:

- `ClusterCoordinator.commitObjectIndex` duration.
- `MetaRaft.ProposeObjectIndex` latency as seen by the caller.

Success criteria:

- Benchmark output can identify the dominant p95/p99 stage for small objects and large objects.
- The report distinguishes local leader path from forwarded path.
- Phase B changes are not started until this attribution exists.

## Phase B: Conservative Optimizations

Apply only changes justified by Phase A.

If forward hop dominates:

- Prefer fresh data-group leader hints when ordering forward targets.
- Add metrics for stale leader hint attempts and not-leader retries.
- Keep fallback to all voters so stale hints do not break writes.

If stream contention dominates:

- Tune `ForwardSender` stream slot limits with a benchmark matrix.
- Keep separate limits for write body streams and read body streams.
- Preserve existing QUIC half-close behavior and regression tests for stream credit exhaustion.
- Keep small objects on single-frame forwarding and large objects on streamed forwarding.

If shard fan-out dominates:

- Bound per-object fan-out concurrency to the EC width.
- Surface slow shard write stage timing.
- Preserve write-all semantics: an unavailable target fails the PUT instead of silently shrinking durability.

If Raft commit dominates:

- Do not restructure Raft in this design.
- Record the result against the existing two-propose ceiling follow-up.
- Keep data-group metadata commit and metaRaft object index commit synchronous for user-visible PUT success.

## Phase C: Direct Shard Write Follow-Up

Direct shard write is not in the implementation scope of this spec.

Open a separate design only if all triggers are true:

- Forwarded path p95/p99 is dominated by `coordinator -> data-group leader` body forwarding.
- Phase B leader hint and stream tuning do not materially improve it.
- Large-object or cross-node PUT traffic violates a real SLO.

Candidate flow:

```text
client
  -> coordinator non-leader
  -> shard owners directly
  -> data-group leader metadata commit
  -> metaRaft ObjectIndex
```

Required prerequisites:

- Orphan shard cleanup/reconcile.
- Meta object-index stale/missing reconcile.
- A data-group leader validation path for shard placement written by a non-leader.
- Failure injection tests for:
  - shard write success followed by data-group Raft failure,
  - data-group Raft success followed by metaRaft failure,
  - partial shard success,
  - leader change mid-write.

## Testing And Verification

Phase A:

- Unit tests for any new metric helper or stage classifier.
- Existing cluster coordinator and forward sender tests continue to pass.
- A benchmark/report path that prints local vs forwarded stage attribution.

Phase B:

- Unit tests for leader hint ordering and stale-hint fallback.
- Forward stream backpressure tests for write and read pools.
- E2E or benchmark run comparing before/after under local and forwarded PUT paths.

Phase C:

- No code in this spec. Future work must include failure injection and reconcile tests before direct write is considered safe.

## Non-Goals

- No Raft log format change.
- No Meta-Raft semantics change.
- No asynchronous object index commit.
- No direct shard write implementation in this scope.
- No silent durability downgrade when placement targets are unavailable.
