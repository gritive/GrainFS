# TODO

## Follow-ups

### ProxyTrust removal follow-ups (2026-06-25)

The orphaned ProxyTrust subsystem (`authoritativeClientIP` + `trusted-proxy.cidr` config +
`OnTrustedProxyCIDR` hook + admin status `trusted_proxy` field + CLI example) was fully REMOVED, and
the dead iceberg `MetaStateSnapshot` FBS slots (5/6/9 + the three `Iceberg*` table defs) were
reserved/deleted (the last iceberg identifiers are now gone from the codebase). Residual dead code
surfaced by that removal:

- **[P3] `tls_posture.go` anon-switch no-op shims.** `enforceTLSPosture` / `bootTLSPostureGate` /
  `iamPostureChecker` / `CheckAnonOff` remain as no-op compatibility shims orphaned by the
  *anon-switch* removal (not iceberg). They are wired into the boot-phase list and AdminAPI, so
  removing them is a separate dead-code cleanup touching boot sequencing + AdminAPI wiring.

- **[P3] `config.Store.SetPostRestore` now has zero production callers.** Its only caller was the
  proxy-CIDR snapshot reconcile, removed with ProxyTrust. The method + its registry remain a
  supported config-store capability (with unit tests) but are currently dead on the production path.
  Either re-use it for a future out-of-band snapshot reconcile, or remove it
  (`internal/config/config.go`) + its tests.

### Discovered during the followup-cleanup PR (2026-06-25, both pre-existing, unrelated to that PR)

- **[P2][pre-existing] Anonymous PUT to `s3://default` fails `500 MetaBucketStore not wired`.**
  The Phase-0 quickstart e2e specs (`tests/e2e/phase0_quickstart_test.go` AnonPut/List/Get/Delete,
  both SingleNode and Cluster3Node) fail: an anonymous PUT to the auto-created `default` bucket
  returns `500 InternalError` — `read versioning state for bucket "default": get bucket versioning
  "default": MetaBucketStore not wired`. Reproduced on `devel` (v0.0.661.0) AND `origin/master`
  (v0.0.662.0 `b993ba40`) — NOT introduced by #853 (verified by re-running the focused probe on the
  pre-#853 commit). The default-bucket versioning read reaches a path where `MetaBucketStore` is nil.
  Needs `/investigate`: real single-node serve regression (default-bucket anon writes broken) vs an
  e2e-harness wiring gap. Re-confirm on current master (v0.0.663.0, post iceberg-removal #854) since
  serveruntime boot changed. Surfaced while verifying the HEAD-quarantine e2e.

- **[P2][pre-existing][flaky-test] `internal/server` `eventWorker` teardown race.** Under full-suite
  `make test-unit` load the `internal/server` package panics `send on closed channel`
  (`badger.(*DB).sendToWriteCh` ← `eventstore.(*Store).Append` ← `eventWorker.start.func1`,
  `internal/server/events_worker.go:42`): the eventstore Badger DB is closed before the worker
  goroutine is drained via `eventWorker.stop()`. Flaky (passes 3/3 in isolation; failed 1 of 2
  full-suite runs). Exposed when the iceberg-removal merge landed on master mid-session; unrelated to
  followup-cleanup (which does not touch `internal/server`). Fix: enforce stop-before-close ordering
  on the eventWorker (call `w.stop()` before closing the eventstore `*Store`) in the server lifecycle /
  test teardown so the worker never `Append`s after Close.

### DeleteBucket non-Enabled emptiness follow-ups (2026-06-24)

- **[P3][pre-existing] TOCTOU between the DeleteBucket emptiness scan and the
  `propose(CmdDeleteBucket)` + `os.RemoveAll(bucketDir)`.** A concurrent PUT (needs only
  `HeadBucket`) can commit qmeta in the window after the scan. Negligible at admin-only
  scope; surfaced by the plan-gate codex pass.

- **[P3][pre-existing] Versioned/Suspended force-delete leaves per-version tombstone
  blobs in `.quorum_meta_versions/{bucket}/`.** `purgePerVersionBlobs` deletes versions
  via `DeleteObjectVersion`, which writes an `IsHardDeleted` tombstone (the versioned
  shards then become orphan-eligible and ARE reclaimed by the orphan-shard walker;
  non-versioned shards are hard-removed inline because the walker does not GC them).
  `os.RemoveAll(b.bucketDir)` removes `{root}/data/{bucket}` but NOT the
  `.quorum_meta_versions/{bucket}/` tombstone blobs, so they persist as inert residue
  (dropped from reads via `dropHardDeletedVersions`, so no resurrection). Shared with the
  pre-existing Enabled force-delete path (`forceDeleteBucketBlobAuth`); surfaced by the
  code-gate codex pass. Fix: add an age-gated per-version tombstone-tree GC, or remove
  `.quorum_meta{,_versions}/{bucket}/` on bucket delete.

- **[P3][pre-existing] Admin force-delete does not invalidate a `CachedBackend` read
  cache.** Moot on the current admin path (admin `Operations` wraps the pull-through
  backend, not `state.cachedBackend`), but if an `Operations` is ever built over a
  `CachedBackend`, `ForceDeleteBucket` now forwards to the backend and skips the
  per-key cache invalidation the old generic walk did. Re-evaluate if the cache wrapper
  moves into the force-delete stack. Surfaced by the code-gate codex pass.

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
### group-0 control-plane demotion follow-ups (2026-06-24, epic DONE)

The demotion shipped: bucket existence/policy/versioning consolidated onto meta-raft as a unified
`BucketRecord`; group-0 carries no bucket control-plane state; create/delete atomicity gaps closed;
bucket writes forward to the meta leader; policy invalidation lossless; versioning barrier best-effort.
Deferred items:

- **[P3][test/CI] Run the object-write throughput bench against master.** The per-mutation versioning
  linearizing read retargeted from the group-0 raft to meta-raft. `make bench` (warp + colima/cluster)
  was unavailable in the dev env. In CI, compare PUT/Copy/CompleteMultipart throughput master vs this
  change; expect neutral-to-slight-improvement. If a regression appears, the short-TTL versioning
  edge-cache below becomes required.
- **[P3] Short-TTL bucket-versioning edge-cache (fast-follow).** The shipped design's deferred "C2":
  only needed if the meta-raft versioning barrier regresses object-write throughput (above).
- **[P3] Neutralize group-0 placement special-casing.** The demotion removed group-0's *control-plane*
  role but it remains the placement legacy-fallback (router default / `object_placement` /
  `object_write_placement` / `append`). Removing that makes group-0 a truly plain data group.
- **[P3] Fold lifecycle + IAM-upstream delete into the meta `DeleteBucket` apply** for full delete
  atomicity (today they remain separate gen-CAS'd meta cascade proposes — no cross-raft gap, but a
  coordinator crash between them can orphan lifecycle/IAM entries).
- **[P3][minor] `CreateBucket` now unconditionally requires a non-empty groupID** (the FSM rejects
  `""`). Safe in production (the router is always wired so placement resolves a real group), but it
  tightens behavior for any future router-less `DistributedBackend` wiring. Note only.

### Multipart off-raft (M1-M5) follow-ups (2026-06-23)

- **[P2][deferred] ModTime-primary latest rule — 7-site migration.** The current `deriveLatestVersion`
  rule is max-VID (UUIDv7 lexicographic = create-time order). When a multipart upload is CREATED
  (T1) before a concurrent PutObject (T2 > T1) but COMPLETED after, the PUT remains latest because
  its vid is larger (T2 > T1). The intended long-term rule is ModTime-primary: the LAST COMPLETED
  write is latest. Changing it requires a coordinated migration across ALL 7 sites:
    • `deriveLatestVersion` (`quorum_meta.go`)
    • `listObjectVersionsBlobAuth` maxVID loop (`object_version.go` ~line 551)
    • `listBlobAuthBucketObjectsForGC` maxVID loop (`object_manifest.go` ~line 172)
    • `scanObjectsBlobAuth` latest-collapse (`scrubbable.go` ~line 218)
    • `reconcileVersionIsLatest` / `sortObjectVersions` (`cluster_coordinator.go`)
    • latest-version resolution (`object_delete.go` ~line 78)
    • `listObjectVersions` latestVID pre-scan (non-blob-auth path, `object_version.go` ~line 370)

  Additional caveats before migration:
    • GET (per-version blob) and LIST (version enumeration) must use the SAME latest rule — split
      implementations are a trap (the listed `IsLatest` flag would disagree with HEAD).
    • A concurrent regular PutObject with the same key can land at any ms; without a global
      sequence tie-breaker, "last completed" is ambiguous when a multipart complete and a PutObject
      complete within the same clock tick.
  The regression-lock is `TestCompleteMultipart_VersionedLatestEdge` — it MUST FAIL (then be updated)
  as part of the migration.

- **[P3][known-edge] Create-ordering is ms-granular only.** `deriveMultipartVID` encodes the
  uploadID's 48-bit UUIDv7 ms timestamp into the derived vid. Two uploads created in the SAME
  millisecond get a hash-arbitrary relative ordering (bytes [6:16] are sha256(rawUploadID), which
  differs per upload). Same-ms concurrent uploads are not ordered by wall clock; their relative
  latest is hash-arbitrary. This is documented in `multipart_upload_id.go`. No action required;
  the test `TestCompleteMultipart_VersionedLatestEdge` handles the same-ms case gracefully (logs
  and skips the latest assertion).

- **[DONE] M4 stale comment + dead `MultipartDoneKey` cleanup (final-review batch).** Stale
  cross-reference comments (references to the removed `CmdCompleteMultipart` flow, removed
  `readDoneMarker` / `MultipartDoneKey` usage sites, stale `//nolint:unused` directives) cleaned
  up; `MultipartDoneKey` (zero callers after M4) removed.

- **[P3][follow-up] Non-versioned multipart-complete idempotency fence weakened vs the removed done-marker.**
  The deterministic-vid existence short-circuit is keyed on the latest-only blob's current VID, so for a
  NON-VERSIONED bucket a client retry of an already-succeeded CompleteMultipartUpload that is preceded by
  an intervening same-key PutObject no longer returns an idempotent 200 — it returns InvalidPart (if a
  leaked manifest replica survives) or NoSuchUpload. NOT data loss (parts are deleted on the first
  successful complete at multipart.go:317 BEFORE the best-effort manifest delete, so re-assembly fails
  closed and never overwrites the newer object — codex final-review P0 'stale overwrite' REFUTED on this
  linchpin). Narrow reachability: non-versioned + lost original 200 + concurrent same-key PUT. A proper
  fix re-introduces the uploadID-keyed completion fence the done-marker provided (e.g. a short-lived
  completion sentinel on the blob, or return NoSuchUpload not InvalidPart when parts are gone). Deferred
  — disproportionate to the narrow non-data-loss impact.

### Data-plane raft-free Slice 2 follow-ups (2026-06-24)

- **[DONE] Retire remaining per-object FSM commands (Slice 2).** `CmdSetObjectTags`,
  `CmdSetObjectACL`, `CmdPutObjectMeta` apply, `CmdPutObjectQuarantine`, `CmdDeleteObject`,
  `CmdDeleteObjectVersion` all retired. FSM is pure control-plane. See CHANGELOG Unreleased entry.

- **[P3][design] Normal non-versioned `DeleteObject` leaves a latest-only tombstone blob
  (the `IsHardDeleted` marker in the quorum-meta blob) that persists indefinitely.** EC shards
  are reclaimed by the orphan-shard walker (which sees no live qmeta referencing them). The
  tombstone blob itself is not reclaimed — confirm there is no unbounded growth path in
  long-lived buckets with high churn, or add a tombstone GC sweep (age-gated, similar to the
  per-version hard-delete tombstone GC already planned).

- **[P3][pre-existing] Per-version tags/acl are latest-only.** `SetObjectTags` / `SetObjectACL`
  blob RMW reads/writes the latest-only quorum-meta blob; the `versionID` parameter is accepted
  but ignored. The only versionID-aware path was the retired `CmdSetObjectTags/ACL` raft command.
  To implement version-scoped tag/acl, wire the RMW through `readQuorumMetaVersion` +
  per-version write. Verified pre-existing (the blob path ignored versionID before Slice 2 too);
  not a Slice 2 regression.

- **[DONE] `deleteShardsQuorum` empty-placement guard.** Fixed in the Slice 2 code-gate: both
  `ForceDeleteBucket` non-versioned loops now fail closed with a descriptive error when
  `len(cmd.NodeIDs) == 0` (corrupt/incomplete qmeta blob) instead of silently stranding shards and
  the qmeta blob. Closes the shard-stranding class for force-delete on objects with corrupt placement.

- **[DONE] Stale comments referencing removed functions.** Fixed in the Slice 2 code-gate:
  the three comments (`apply.go`, `object_version.go`, `cluster_coordinator.go`) referencing
  deleted apply helpers were reworded to describe behavior without the removed symbols.

### Append/coalesce off-raft follow-ups (Slice 1, 2026-06-24)

- **[DONE] Slice 2 — retire remaining per-object FSM commands.** See Slice 2 section above.

- **[P2] EC coalesced orphan shard leak.** `orphan_shard_walker.go` skips every `/coalesced/`
  shard directory, so unpublished/unreferenced coalesced EC shards (B3 EC distribute) are
  never reclaimed — a permanent leak. Needs reachability for coalesced EC shards (pre-existing
  skip behavior, surfaced by Slice 1 code gate). Fix: extend the orphan shard walker to walk
  `/coalesced/` directories and apply the same age-gate + two-cycle tombstone logic used for
  segment orphans.

- **[P2] Composite-ETag reconstruction after coalesce.** `AppendCallMD5s` is not persisted
  (always nil); a post-coalesce append recomputes ETag from remaining segments. Pre-existing;
  behavior-neutral in Slice 1. Wire `AppendCallMD5s` into the quorum-meta schema if exact
  post-coalesce ETag continuity is required for S3 compatibility.

- **[P2] off-raft append fencing-lease** *(only if the accepted failover-safety risk must
  later be closed).* A proper single-writer lease (leader-term fencing token +
  quorum-intersecting read) to make off-raft append/coalesce failover-safe across a
  same-generation leader handoff. See "Known limitation" in `docs/operators/runbook.md` for
  the full risk description.

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
