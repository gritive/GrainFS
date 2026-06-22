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
- **[DONE] Follower's bucket-policy authz cache is never populated from committed Raft state.**
  In cluster mode a follower (or a single-node node after restart) whose in-memory
  `*policy.CompiledPolicyStore` was never populated for a bucket default-ALLOWed (`Allow` returned
  `true` on a `cp == nil` miss), so a committed **Deny** bucket-policy was silently unenforced.
  Fixed (hybrid, per the chosen design): (1) **pull-on-miss** — `Allow` now loads the policy from
  the local committed replica via an injected loader (`storage.NewOperations` wires
  `loadCommittedBucketPolicy` → `PolicyBackend.GetBucketPolicy`), compiles, and caches it positive
  **or** negative; a successfully-read committed Deny is always honored on the first request. (2)
  **apply-hook invalidate** — `notifyOnApply` fires `CompiledPolicyStore.Invalidate(bucket)` on
  committed `CmdSetBucketPolicy`/`CmdDeleteBucketPolicy` (wired via `DistributedBackend.SetOnBucketPolicyApply`
  in boot) so deletes/tightening on any node drop the cached entry → next `Allow` re-pulls. (3)
  snapshot install flushes the whole policy cache (`restore` → `Invalidate("")`). A global generation
  stamp drops an in-flight pull's result if a concurrent mutation raced it. Fault/structural-read
  errors fail **open** (legacy default-allow, uncached, self-healing — no spurious-deny regression);
  an unparseable committed policy fails **closed** (deny, cached). `internal/policy` stays free of
  `internal/storage` (loader injected as a plain func). Tests: policy unit (pull/negative/fault/
  malformed/tighten/loosen/flush/concurrency, race-clean), storage cold-cache, cluster apply-hook,
  in-process server cold-cache proof.
- **[P3][follow-up, security-consistency] Admin GET ?policy on a malformed stored policy leaves the
  negative cache stale.** `operations_policy.go:38` lazily `Set`s on the display read and ignores
  compile errors; if a bucket was negative-cached and an admin GETs a malformed committed policy, the
  `Set` fails and the negative entry survives → authz `Allow` returns allow, whereas the authz
  pull-on-miss path correctly fail-closes a malformed policy to deny. Only affects already-malformed
  stored policies (PutBucketPolicy rejects new ones); not a bypass of any valid Deny. Make the
  GET-path `Set`-failure clear the negative entry (or call `Invalidate`) for parity.
- **[P3][cleanup] Dead IAM meta-FSM bucket-policy path** (`MetaCmdTypeBucketPolicyPut`,
  `bucketpolicy.InMemoryStore`, `applyBucketPolicyPut/Delete`) is dead in production (no proposer).
  Wire-or-remove it to kill the dual-store ambiguity. (Deferred from the authz-cache fix above.)
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
