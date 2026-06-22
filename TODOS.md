# TODO

## Follow-ups

### Bucket-delete config cascade follow-ups (PR: fix-bucket-delete-config-cascade)

- **[P3][won't-fix] Mixed `--lifecycle-interval` clusters are unsupported.** The lifecycle-delete
  cascade is gated per-node on `LifecycleInterval > 0`, which is **load-bearing**, not cosmetic: the
  lifecycle store itself is only wired when `interval > 0` (`boot_phases_srvopts.go`), and
  `applyBucketLifecycleDelete` errors if the store is nil (`meta_fsm_lifecycle.go`). So a deliberately
  mixed cluster (some nodes `0`, some `>0`) is operator misconfiguration, not a latent bug. Uniform
  deployments unaffected.

### Bucket config off group-0 (control-plane read linearization)

- **[DONE] Follower-stale bucket-versioning read at the mutating S3 edge.** A multipart-complete / PUT /
  Copy on a group-0 follower read its lagging local replica and silently wrote the object
  non-versioned (a just-joined follower observed Unversioned for ~90s after another node enabled
  versioning). Fixed: the mutating edge resolves versioning via a linearizing read
  (`GetBucketVersioningLinearized` = ReadIndex+WaitApplied, reusing the object-read primitive),
  **degrading to a local read during a group-0 leaderless window** so writes aren't coupled to
  control-plane leadership; reads/scrub keep the plain local read. Versioning stays on group-0 raft
  (consensus preserved — a mutable RMW cell needs total order + atomic CAS, which a quorum LWW blob
  can't provide; see `docs/superpowers/specs/2026-06-23-bucket-config-off-group0-quorum-design.md`).
- **[premise-WRONG, do not implement] "Linearize `GetBucketPolicy` the same way".** Investigated and
  refuted: the S3 mutating-edge authz decision does NOT call `GetBucketPolicy`. It goes
  `mustAuthorize` → `authz_decision.go` `s.authz.Decide` → `RequestAuthorizer.Decide` → the in-memory
  `*policy.CompiledPolicyStore`. `GetBucketPolicy` (`loadBucketPolicy`) is only the GET ?policy /
  admin **display** read. Linearizing it changes no authorization and only adds group-0 coupling to a
  display read. Superseded by the real gap below.
- **[P2][security, deferred — design chosen, not yet built] Follower's bucket-policy authz cache is
  never populated from committed Raft state.** In cluster mode the authoritative per-bucket S3
  bucket-policy gate is the server-layer `*policy.CompiledPolicyStore` (`CompiledPolicyStore.Allow`
  default-ALLOWs on a miss; `compiled.go:271`). It is `Set` only from `operations_policy.go:13` (the
  request-processing node's write path) and `:38` (lazy-load on a GET ?policy display). It is NOT
  populated by the data-Raft FSM apply: `notifyOnApply` (`backend.go:816`) handles only
  `CmdPutObjectMeta`/`CmdDeleteObject`/`CmdCompleteMultipart`; `CmdSetBucketPolicy` falls to `default`.
  And `FSM.Restore` (`apply.go:1059`) bulk-loads the snapshot into Badger without firing apply hooks.
  → A follower that never served the PutBucketPolicy AND never served a GET ?policy for a bucket
  has an empty compiled entry → every PUT/DELETE/multipart hits `Allow` → `true` (default allow),
  so a committed **Deny** bucket-policy is silently unenforced on that follower. PERMANENT gap (not a
  bounded apply-lag window), partially masked by Layer-1 IAM grants (an SA with no IAM allow is still
  denied at L1; the gap is the bucket-policy Deny layer). Severity below #839 (authz gap, reversible,
  not silent data loss), but a real correctness/security defect. The IAM meta-FSM bucket-policy path
  (`MetaCmdTypeBucketPolicyPut`, `bucketpolicy.InMemoryStore`, `applyBucketPolicyPut/Delete`) is dead
  in production (no proposer) — wire-or-remove it to kill the dual-store ambiguity.
  **Chosen design (deferred 2026-06-23): hybrid = apply-hook + pull-on-miss.** (1) Add
  `CmdSetBucketPolicy`/`CmdDeleteBucketPolicy` cases to `notifyOnApply` → a policy callback →
  `CompiledPolicyStore.Set/Delete` (propagates changes, esp. deletes/tightening, on every node). (2)
  On a `CompiledPolicyStore` miss, load from the local committed replica (the backend's
  `GetBucketPolicy` Badger read + lazy-compile, with a negative-cache for "no policy") instead of
  default-allow — inherently complete across boot / runtime-snapshot-install / apply-lag, self-healing,
  no group-0 coupling, one local read per bucket per node. Needs a deterministic follower
  false-allow e2e (RED) + careful authz-path review (security-critical).
- **[DONE] AppendObject versioned-bucket feature-gate** read the PLAIN versioning state
  (`object_append.go`), so a stale group-0 follower could read Unversioned for an Enabled bucket and
  let an append bypass the 501 gate. Fixed: the gate now resolves versioning via the linearized read
  (`GetBucketVersioningLinearized`, #839), matching the PUT/Copy/CompleteMultipart mutating-edge
  contract. Discriminating unit test (`TestAppendObjectGateUsesLinearizedRead`, call-count).
- **[P3][follow-up] AppendObject 501-gate fails OPEN on a genuine versioning-read fault.** The gate is
  `vErr == nil && state == "Enabled"`, so any non-`UnsupportedOperationError` resolve fault is treated
  as not-Enabled and the append proceeds — diverging from `ctxWithBucketVersioningStrict`, which
  fail-closes on a genuine fault. Pre-existing (predicate byte-identical to pre-#839). Practical risk
  is near-nil: it requires an Enabled bucket AND the linearized read's barrier to fail AND the
  degraded local read to itself error (a real BadgerDB fault), a state where the node can't read its
  own metadata anyway. Fail-closing would also reduce append availability on the common non-versioned
  bucket when versioning-read transiently faults. Decide strict-parity vs availability if it ever
  matters; not worth a behavior change now.
- **[P3][epic, separate] group-0 control-plane demotion** — consolidate bucket existence/assignment
  onto the true meta-raft (`bucketAssignments`) so group-0 becomes a plain data group. Larger; touches
  bucket-lifecycle atomicity + the #838 delete cascade. Not needed for the read-linearization fix.

### Tests / docs / spec polish

- **[P3][test] Deterministic multi-node reproduction of the forwarded-propose apply-wait + MPU
  phantom-winner guard.** The follower-stale-versioning blocker is now fixed (above), so a follower
  CAN do a versioned write (`tests/e2e/cluster_versioned_write_follower_test.go`, a happy-path
  integration spec). A *deterministic* reproduction of the phantom-winner race still needs a group-0 /
  data-group apply-delay seam (a long-enough delay collides with the 30s propose deadline — the trap
  that blocked the earlier attempt). The guard logic is unit-covered (`readDoneMarkerFn` seam);
  add the deterministic e2e if an apply-timing seam is introduced.

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
