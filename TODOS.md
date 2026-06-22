# TODO

## Follow-ups

### Bucket-delete config cascade follow-ups (PR: fix-bucket-delete-config-cascade)

- **[P3] Mixed `--lifecycle-interval` clusters.** The lifecycle-delete cascade is gated per-node on
  `LifecycleInterval > 0` (matches the lifecycle-store-wiring condition; default `1h`). A deliberately
  mixed cluster (some nodes `0`, some `>0`) could route an admin delete to a `0` node and skip the
  lifecycle-delete. Uniform deployments unaffected; mixed-interval is operator misconfiguration.
- **[P3] Latent typed-nil on `BucketWithPolicyProp`.** `boot_phases_admin.go` boxes the typed
  `*iam.MetaProposer` directly; if `state.iamProposer` were nil this would defeat the `!= nil` guard at
  handlers_bucket.go:43. Unreachable today (boot fails hard when IAMStore is absent), but worth the same
  nil-guard helper used for the new cascade fields, for consistency.
- **[P3] Best-effort crash residual.** If the data-Raft bucket delete commits but the process dies
  before the config cascade, the config leaks and is reconciled only on an operator retry (the
  `ErrBucketNotFound` path re-runs the idempotent cascade). Same residual class as the NFS-export
  cascade; acceptable.

### Tests / docs / spec polish

- **[P3][test] No multi-node integration test for the forwarded-propose apply-wait + MPU phantom-winner
  guard.** Covered by single-node tests + reasoning + the full suite, but a true 2/3-node
  follower-forwards-during-phantom-commit test isn't feasible in the current solo-leader unit harness.
  Add one if a multi-node raft test harness is introduced.
- **[P3][test polish] Strengthen S4c-0 PR1 tests:** ACL concurrency test asserts only `MetaSeq==n` (add
  a final-ACL-value coherence check); latest-writer overwrite-on-tie intent lives only in a code comment
  (add a tie-case test); `TestWriteQuorumMetaVersionLocal_OverwritesWhenCandidateWins` uses `got, _ :=`
  (discards ReadFile error). All non-blocking.
- **[P3][doc] mpudone 24h retention bounds the idempotency window.** The GC sweep expires markers after
  24h (boot_phases_scrubber.go:211). A retry-after-success arriving >24h post-completion is no longer
  idempotency-protected (returns ErrUploadNotFound). 24h conservatively outlives realistic client
  retries + raft replay; document in the operator runbook.
- **[P2][spec] Amend v8 §MPU: VersionID is minted-at-complete, not pinned-at-create.** The S4c v8 design
  (docs/superpowers/specs/2026-06-18-s4c-cutover-design-v8.md §MPU) prescribed "pin VersionID at
  CreateMultipartUpload"; the correct mechanism (shipped) is VID minted at completion + the `mpudone:`
  marker for idempotency. Low urgency — the spec is historical (its surrounding S4c cutover is itself
  superseded; see below).

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
