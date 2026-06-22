# TODO

## Follow-ups

### Bucket-delete config cascade follow-ups (PR: fix-bucket-delete-config-cascade)

- **[P3] Mixed `--lifecycle-interval` clusters.** The lifecycle-delete cascade is gated per-node on
  `LifecycleInterval > 0` (matches the lifecycle-store-wiring condition; default `1h`). A deliberately
  mixed cluster (some nodes `0`, some `>0`) could route an admin delete to a `0` node and skip the
  lifecycle-delete. Uniform deployments unaffected; mixed-interval is operator misconfiguration.
- **[P3] Best-effort crash residual.** If the data-Raft bucket delete commits but the process dies
  before the config cascade, the config leaks and is reconciled only on an operator retry (the
  `ErrBucketNotFound` path re-runs the idempotent cascade). Same residual class as the NFS-export
  cascade; acceptable.

### Tests / docs / spec polish

- **[P3][test] No multi-node integration test for the forwarded-propose apply-wait + MPU phantom-winner
  guard.** Covered by single-node tests + reasoning + the full suite, but a true 2/3-node
  follower-forwards-during-phantom-commit test isn't feasible in the current solo-leader unit harness.
  Add one if a multi-node raft test harness is introduced.

## Superseded / historical (do not resurrect the analysis)

The blob-authoritative pivot (#821–#825: greenfield raft-free data plane + soleauth-machinery removal)
obsoleted the following. Their detailed roadmaps/architecture notes were removed because carrying false
analysis forward is a trap:
- **[EPIC] Per-version quorum-meta foundation** (S4b PR-B / S4c cutover / S5) — the end-state shipped via
  the greenfield route; the staged cutover + verifier + soleauth flip it was gated on were removed.
- **[EPIC] Retroactive EC-redundancy upgrade for NON-LATEST versions** — premise dead: per-version
  metadata is now a K-of-N replicated blob, not a generation-sharded FSM record, so the cross-group
  record migration it described no longer exists. Any residual non-redundant-genesis-DATA gap, if real,
  needs a fresh spec.
- **DeleteObjectVersion stale-latest quorum-meta pointer** — DONE-IN-EFFECT: hard delete now writes a
  durable `IsHardDeleted` tombstone (LWW `MetaSeq+1`) and versioned reads derive strictly from
  per-version blobs excluding tombstones. Worth one confirmatory e2e of the old repro before formal
  closure.
- The entire **S4c-a / a2 / a3 soleauth-precondition** block (epoch fence, boot-window, flip-gating,
  delete forward-wire epoch, etc.) — the soleauth machinery was removed in #824.
- **`-race` test-harness race** (`per_version_backfill_walker_test.go` vs `RunApplyLoop`) — DONE-IN-EFFECT:
  the backfill walker + its test were deleted in #822.
- **Versioned tag/ACL RMW holds the meta-RMW lock across a raft propose** — moot: versioned
  `writeQuorumMeta` is raft-free for versioning-enabled buckets.
