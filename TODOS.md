# TODO

## Follow-ups

### Quorum-meta blob codec decouple follow-ups (2026-06-24)

- **[P3][pre-existing] Dead `CachedBackend` / apply-driven cache-invalidator wiring.**
  `boot_phases_services.go:35-36` constructs `state.cachedBackend = NewCachedBackend(distBackend)`
  and registers an `s3-cache` invalidator (`distBackend.RegisterCacheInvalidator` →
  `notifyOnApply`). But the live S3 server uses `state.backend` (pullthrough→coordinator), NOT
  `state.cachedBackend` (`boot_phases_admin.go:85` `server.New(cfg.Addr, state.backend, …)`), so the
  read cache is never on the live path and the registered invalidator invalidated a cache nobody
  reads. The blob-codec-decouple PR removed the (already-unreachable since data-plane raft-free Slice 2)
  `notifyOnApply` `CmdPutObjectMeta` object-cache branch, making the dead wiring obvious. `Registry`/
  `InvalidateAll`/`SetOnApply` now have no live object-mutation driver. Fix: either wire `cachedBackend`
  into the live read path (and drive invalidation off the off-raft write path) or remove the unused
  `CachedBackend` + registry wiring. Surfaced by the code-gate codex pass; pre-existing, not a
  regression of this PR (behavior-neutral — the cache was never live).

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
  pre-existing Enabled force-delete path (`forceDeleteBucketSoleAuth`); surfaced by the
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
- **[P3][epic, separate] group-0 control-plane demotion** — consolidate bucket existence/assignment
  onto the true meta-raft (`bucketAssignments`) so group-0 becomes a plain data group. Larger; touches
  bucket-lifecycle atomicity + the #838 delete cascade. Not needed for the read-linearization fix.

### Multipart off-raft (M1-M5) follow-ups (2026-06-23)

- **[P2][deferred] ModTime-primary latest rule — 7-site migration.** The current `deriveLatestVersion`
  rule is max-VID (UUIDv7 lexicographic = create-time order). When a multipart upload is CREATED
  (T1) before a concurrent PutObject (T2 > T1) but COMPLETED after, the PUT remains latest because
  its vid is larger (T2 > T1). The intended long-term rule is ModTime-primary: the LAST COMPLETED
  write is latest. Changing it requires a coordinated migration across ALL 7 sites:
    • `deriveLatestVersion` (`quorum_meta.go`)
    • `listObjectVersionsSoleAuth` maxVID loop (`object_version.go` ~line 551)
    • `listSoleAuthBucketObjectsForGC` maxVID loop (`object_manifest.go` ~line 172)
    • `localSoleAuthScrubObjects` latest-collapse (`scrubbable.go` ~line 218)
    • `reconcileVersionIsLatest` / `sortObjectVersions` (`cluster_coordinator.go`)
    • latest-version resolution (`object_delete.go` ~line 78)
    • `listObjectVersions` latestVID pre-scan (non-sole-auth path, `object_version.go` ~line 370)

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

- **[P2][pre-existing] Non-versioned `DeleteBucket` (non-force) emptiness check is FSM-blind
  (`bucket.go` ~line 129–136 scans `obj:` FSM records; greenfield non-versioned objects are
  stored in `.quorum_meta` blobs only, so the check always sees an empty bucket).** A
  non-empty non-versioned bucket may be deleted without error by the non-force path.
  Fix: enumerate `scanQuorumMetaBucketStrict` / `scanQuorumMetaClusterAll` (mirror the
  ForceDeleteBucket non-versioned enumerate) and return `ErrBucketNotEmpty` on the first hit.

- **[P3][pre-existing] `HeadObject` / `HeadObjectVersion` do not check quarantine status.**
  A quarantined object is HEAD-able (200) even though GET returns `ErrObjectQuarantined`.
  Whether this is intentional behavior or a gap depends on S3 compatibility requirements.
  If HEAD should reflect quarantine, add an `isObjectQuarantined` check in the HEAD paths.

- **[P3][design] Normal non-versioned `DeleteObject` leaves a latest-only tombstone blob
  (the `IsHardDeleted` marker in the quorum-meta blob) that persists indefinitely.** EC shards
  are reclaimed by the orphan-shard walker (which sees no live qmeta referencing them). The
  tombstone blob itself is not reclaimed — confirm there is no unbounded growth path in
  long-lived buckets with high churn, or add a tombstone GC sweep (age-gated, similar to the
  per-version hard-delete tombstone GC already planned).

- **[P3][naming] `soleAuthReadOn` / `forceDeleteBucketSoleAuth` are vestigial names from the
  soleauth era** (the soleauth machinery was removed in #821–#824; the concept is now
  "blob-authoritative read"). Rename to `blobAuthReadOn` / `forceDeleteBucketBlobAuth` for
  clarity. No behavior change.

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

- **[P3] `f.coalesceCfg` write-only dead field.** The FSM's copy of the coalesce config
  (`SetCoalesceCfg` + `FSM.coalesceCfg`) is now write-only: the live config is
  `b.coalesceCfg` (the backend `atomic.Pointer`). Remove the dead FSM copy to kill
  the write-only state.

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
