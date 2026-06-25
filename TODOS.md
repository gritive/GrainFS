# TODO

## Follow-ups

### `__grainfs_volumes` / `__grainfs_nfs4` leftover removal follow-ups (2026-06-26)

PR removed the dead `__grainfs_volumes` EC-routing branch (`routeSourceFor`) and the
`__grainfs_nfs4` `IsInternalBucket` carve-out (behavior-neutral; greenfield, nothing creates
either bucket).

- **[P3][pre-existing] Internal buckets get zero scrub coverage.** `routeSourceFor` now maps every
  internal (`__grainfs_*`) bucket to the `"replication"` scrub source, but production registers only
  the `"ec"` source (`boot_phases_scrubber.go`), so internal-bucket scrub requests resolve to a nil
  source = safe logged no-op. Pre-existing (the old volumes branch was the only internal→"ec" path).
  "Should internal buckets be scrubbed at all, and via which source" is a design question, not a
  leftover fix — surfaced by the code-gate review.
- **[P3][pre-existing] `internal/storage/local.go` legacy single-file read path.** Its only named
  producer was `__grainfs_volumes` Volume Device blocks (now gone). Verify whether any non-segment
  object can still reach this path; if none, the path itself may be removable (separate change — the
  comment was fixed, the code kept).
- **[P4][docs] nfs4 framing superseded.** Design doc
  `docs/superpowers/specs/2026-06-23-data-plane-raft-free-completion-design.md` line 88 lists
  `__grainfs_nfs4` as a deliberate KEEP, and `bucket.go`'s old "Phase 0b (D6)" comment framed it as
  managed-as-regular-bucket. Both are now superseded — nfs4 was removed per user direction
  (2026-06-26). Update the design doc note if it is revisited.
- **[P4][hygiene] Incidental `__grainfs_volumes` test fixtures kept (Minimal scope).** `spool_test.go`,
  `pullthrough_test.go`, `store_bench_test.go`, `backend_bucket_management_integration_test.go`, and
  the `bucket_test.go` `{"__grainfs_volumes", true}` case still use the string as an arbitrary
  internal-bucket name. Optional rename to a neutral name; the integration test's placement is
  name-hash-coupled, so a rename must recompute its expected group.

### ShardService decomposition follow-ups (2026-06-25, PR1 LocalShardStore done)

`ShardService` (1,940 LOC god-struct) is being decomposed into a facade over deep local-store
modules. **PR1 (LocalShardStore extraction) is done** — shard-blob I/O + durability + seal +
staging carved out behind the facade, behavior-preserving. Remaining slices (each a separate PR,
facade stays the spine — see design `docs/superpowers/specs/2026-06-25-shard-service-decomposition-design.md`):

- **[P2] PR2 — semantic LocalQuorumMetaStore.** Carve the 30 quorum-meta `*ShardService` methods
  (in `quorum_meta.go`) into a `LocalQuorumMetaStore`. **Semantic, not raw-KV**: the local write
  decodes candidate + existing blobs and runs `decideQuorumMetaWrite` (CAS-reject / LWW-skip /
  idempotent-replay) before the rename. This module becomes the injected adapter for the
  Quorum Meta Store deepening (Card 1). Fields: `dataDirs` (shared) + `quorumMetaTargetLocks`.
- **[P2] decideQuorumMetaWrite single-ownership.** The CAS/LWW conflict-resolution
  (`decideQuorumMetaWrite` / `quorumMetaCmdWins`) is shared between the local write-accept and the
  Quorum Meta Store orchestration merge. Decide where it lives (one place) when doing PR2/Card 1 —
  this is the actual deepening, not the byte I/O.
- **[P3] PR3 — LocalManifestStore.** Carve the 12 manifest-blob `*ShardService` methods
  (`manifest_blob.go`, `.qmeta_mpu/{bucket}/{uploadID}`) into a `LocalManifestStore`.
- **[P3] parent-dir fsync gap (pre-existing, surfaced by the decomposition).** Quorum-meta
  (`quorum_meta.go` writeQuorumMetaLocal) and manifest (`manifest_blob.go`) writes fsync the temp
  file then rename but do **not** fsync the parent directory (no `syncDirChain`, unlike shard
  writes) — a crash can lose the rename even though bytes are durable. Extracting LocalQuorumMetaStore
  makes this visible/testable. Behavior change, so a separate PR; may relate to Quorum Meta Store
  data-loss ordering.

### DeleteBucket non-Enabled emptiness follow-ups (2026-06-24)

- **[P3][pre-existing] TOCTOU between the DeleteBucket emptiness scan and the
  `propose(CmdDeleteBucket)` + `os.RemoveAll(bucketDir)`.** A concurrent PUT (needs only
  `HeadBucket`) can commit qmeta in the window after the scan. Negligible at admin-only
  scope; surfaced by the plan-gate codex pass.

- **[DONE-local][pre-existing] Versioned/Suspended force-delete left per-version tombstone
  blobs in `.quorum_meta_versions/{bucket}/`.** `purgePerVersionBlobs` deletes versions
  via `DeleteObjectVersion`, which writes an `IsHardDeleted` tombstone (the versioned
  shards then become orphan-eligible and ARE reclaimed by the orphan-shard walker;
  non-versioned shards are hard-removed inline because the walker does not GC them).
  `os.RemoveAll(b.bucketDir)` removes `{root}/data/{bucket}` but NOT the
  `.quorum_meta_versions/{bucket}/` tombstone blobs, so they persisted as inert residue
  (dropped from reads via `dropHardDeletedVersions`, so no resurrection). Shared with the
  pre-existing Enabled force-delete path (`forceDeleteBucketBlobAuth`).
  FIXED at the coordinator-local site: `DeleteBucket` now calls
  `ShardService.RemoveBucketMetaTrees` to remove `.quorum_meta{,_versions}/{bucket}` after
  `os.RemoveAll(bucketDir)`. Multi-node peer fan-out remains open (next item).

- **[P3][pre-existing] Bucket-delete physical cleanup is coordinator-local in a cluster.**
  `DistributedBackend.DeleteBucket` removes `bucketDir` AND (new) the
  `.quorum_meta{,_versions}/{bucket}` trees only on the node that runs the call; the
  meta-Raft DeleteBucket apply removes only the bucket record + `onBucketUnassigned`, with
  no per-node physical-cleanup fan-out. So peer nodes leak `bucketDir` + the meta trees
  (inert residue). Fix: a data-plane per-node bucket physical-cleanup fan-out (covers
  bucketDir + meta trees together). Surfaced by the code-gate codex pass.

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
- **[P2][follow-up] AppendObject is O(N²) in segment count (single-node).** A micro-benchmark sweep
  (`BenchmarkS3Append`, allocs/op deterministic) showed per-append cost GROWS with the existing
  segment count: n=4 → 885 allocs, n=8 → 2,835, n=16 → 10,490 (doubling appends ≈ 3.5× allocs).
  Root cause (memprofile): every `appendExisting` → `PutObjectRecordInTxn` (a) re-reads + decodes the
  full N-segment record (`unmarshalObjectInto`), (b) RemoveRef-all-then-AddRef-all the chunkref
  membership (O(N) pure churn; only 1 chunk actually changed), (c) re-marshals the whole object, and
  (d) `CompositeETag` re-hashes all N+1 call-MD5s. So N sequential appends = O(N²) metadata work;
  with `MaxAppendSegments=10000` the worst case is severe, and AppendObject's whole point is repeated
  append. A bounded win (skip the prev-read on the append path — `existing` is already in hand — and
  AddRef only the NEW segment's chunks instead of remove-all/add-all) removes 3 of the O(N) factors
  but leaves marshal+ETag O(N) (still O(N²), smaller constant). True O(1)/append needs incremental
  metadata persistence (append-only segment log + running ETag state) — a storage-format redesign in
  the chunkref/object-meta area (data-loss-sensitive), so it wants its own design pass (office-hours/
  spec) before implementation. Cluster append: #895 measured it (`BenchmarkClusterAppend`, EC 4+2,
  coalesce-off) — same super-linear O(N²) (n=4 → 545 allocs, n=8 → 1,356, n=16 → 3,711), same
  meta-rewrite root cause (`readAppendBase` decode + manifest re-marshal + quorum-meta), softened in
  production by coalesce every 16 segments. The same storage-format redesign applies to both paths.
- **[P3][follow-up] EC multipart-complete READ-side staged-part buffering (`readShardPayload` /
  `readSpoolEncryptedRecord`).** The WRITE side is now DONE: #895 pooled the per-chunk seal + pre-sized
  the shard-encode buffer; #898 streamed sized EC shard writes; the `atomicShardFileWrite` PR removed the
  last write-side buffer — the encrypted shard `[]byte` is no longer materialized. `LocalShardStore`'s
  encode now writes straight to the shard fd via a callback (no bufio: the AEAD encoder's native ~1 MiB
  write granularity keeps syscalls low), so `writeEncryptedShardFile`'s `[]byte` contract — the documented
  blocker here — is gone, and the fsync decision now comes from the ciphertext bytes actually written.
  Same-machine write-path B/op: COPY 16 MiB 56 → 29 MB/op (−48%), multipart Complete 32 MiB 91 → 40 MB/op
  (−56%); wall-time unchanged (crypto-bound). RESIDUAL is now READ-side only and dominates the remaining
  profile: each staged part / shard is still read whole into memory before re-encode/reconstruct
  (`readShardPayload` `io.ReadAll`/`make` ~14%, `readSpoolEncryptedRecord` cum ~34%,
  `reedsolomon.AllocAligned` ~16%) — a separate read path, separate plan. Re-measure via
  `BenchmarkClusterMultipart_Complete` / `BenchmarkClusterCopy`.
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

- **[P3][pre-existing] Non-blob-auth Suspended cross-group `lat:` split can desync HEAD vs LIST.**
  Surfaced while migrating to ModTime-primary latest. For versioning-Suspended (non-blob-auth)
  buckets, HEAD resolves latest from the latest-only quorum-meta blob (`readQuorumMeta`,
  last-write-wins) while LIST resolves it from the FSM `lat:` pointer + `reconcileVersionIsLatest`.
  These are independent mechanisms (the `lat:` path is legacy-migration-only — "NOTHING ELSE writes
  FSM object meta any more"), so a key split across groups with a divergent `lat:` pointer can make
  LIST `IsLatest` disagree with HEAD. Pre-existing (the migration kept reconcile consistent with the
  rest of LIST; it did not introduce this) and out of scope for the latest-rule change. Low impact
  given the legacy-only `lat:` writer. Fix would unify the non-blob-auth HEAD and LIST resolvers.

- **[P3][known-edge] Create-ordering is ms-granular only.** `deriveMultipartVID` encodes the
  uploadID's 48-bit UUIDv7 ms timestamp into the derived vid. Two uploads created in the SAME
  millisecond get a hash-arbitrary relative ordering (bytes [6:16] are sha256(rawUploadID), which
  differs per upload). Same-ms concurrent uploads are not ordered by wall clock; their relative
  latest is hash-arbitrary. This is documented in `multipart_upload_id.go`. No action required.
  Now also the documented residual of the ModTime-primary latest rule: ModTime is second-granular,
  so a multipart and a same-key PutObject that complete within the SAME second tie on ModTime and
  fall back to the VID tiebreak (larger VID wins) — the original create-time edge still applies
  within a one-second window. `TestCompleteMultipart_VersionedLatestEdge` forces a >1s gap to assert
  the cross-second ModTime-primary behavior deterministically.

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

- **[P3] EC-sharded chunked segment orphan reclaim — staging-write redesign (PR1+PR2 DONE; only PR3
  perf left).** The dangerous cluster-wide live-segment ORACLE (3-axis data-loss) was ABANDONED in
  favor of fixing the WRITE path (stage + promote-on-commit), prior-art aligned (MinIO/Ceph-RGW/S3).
  - **PR1 DONE (#889, v0.0.691.0):** chunked-PUT segments write EC shards to `.segstaging/<txn>/<blobID>`
    (AAD = final key) and **promote (atomic rename) to the final path only at commit** (data-before-meta,
    all-or-fail, fsynced) — NARROWS the orphan window (mid-write/crash strands shards under `.segstaging`,
    not the final namespace).
  - **PR2 DONE (#895? — segment staging PR2):** the orphan-shard walker AGES OUT abandoned `.segstaging`
    leaves (crash promote↔commit / failed PUT / LWW loser) — closing the residual disk leak. NOT a trivial
    age-out: a user object key can collide with the `.segstaging` namespace, so the walker decides by
    **delete-time full-object LIVENESS** (mirrors the regular orphan path: known/frozen/live/seen,
    parseFullObjectRel, owningGroupHosted, hasLiveShardRecord certainty-aware; `!okF`/uncertain/non-hosted
    → keep) + a structural `/segments/` exclusion (chunked user objects under `.segstaging` are skipped, so
    the abandoned SEGMENT oracle is never consulted) + a 24h age floor (in-flight protection). A
    write-edge `.segstaging` key RESERVATION was tried and ABANDONED — pull-through cache + path-
    normalizing keys kept bypassing it; delete-time liveness makes all write-path holes moot. ★Safety
    relies on getShardDir/ShardPathUnderDataDir per-bucket containment (parsed bucket == physical owner ==
    liveness lookup target). new metric `grainfs_scrub_segstaging_reclaimed_total`.
  - **PR3 (this item, P3):** gap/perf — promote-fanout RPC count, empty `.segstaging/<txn>` parent-dir
    cleanup after leaf reclaim.
- **[P3][known-tradeoff] Coalesced orphans in a bucket switched to versioning-Enabled are not
  reclaimed.** `hasLiveCoalescedRef` gates on `blobAuthReadOn` and fails closed (keep) for Enabled
  buckets, so coalesced orphans created during a bucket's prior Unversioned/Suspended life leak after
  it is Enabled. Safe (no data loss), bounded residual; documented deliberate tradeoff.

- **[P3] Appendable/coalesced objects do not get EC redundancy upgrade.** The redundancy-upgrade
  relocation (`relocate_object.go`) now SKIPS `IsAppendable`/`Coalesced` objects (they would be
  corrupted by the chunked re-encode — drops IsAppendable/Coalesced/AppendCallMD5s). So an
  appendable object written 1+0 on a single node stays 1+0 after the cluster grows (no parity).
  A proper appendable-aware relocation that preserves the append manifest shape + digest history
  is a separate feature. Surfaced by the 2026-06-25 AppendCallMD5s code-gate.

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

### Code-health audit findings (2026-06-25, via /health)

Composite 8.1/10 (TC 10, Lint 10, Test 9, Dead 4, Shell 4, GBrain 7). `make lint` + `go build ./...`
are clean and there are **no real test failures** — the `make test-unit` non-zero exit was a stale
build-cache artifact: `go vet` referenced the deleted `internal/cluster/quorum_meta_shadow_test.go`
(removed in #857 / v0.0.666.0). `go clean -testcache` (or any rebuild touching `internal/cluster`)
clears it; 67/68 testable packages pass, 0 real failures. Standalone `staticcheck ./...` (broader than
golangci's `make lint` subset, and it analyzes test files that golangci skips via `tests: false`)
surfaced the items below. None block; tracked for cleanup.

- **[P4] 54 unused test-helper symbols (staticcheck U1000) across `_test.go` files** (e2e 25,
  cluster 15, raft 6, scrubber 4, server 2, storage/lifecycle 2). golangci `unused` skips them
  (`tests: false`) so `make lint` stays green. Batch-removable dead test scaffolding; low risk.
  Enumerate with `staticcheck ./... | grep U1000`.

- **[P4][note, not a defect] staticcheck also flags 3 PRODUCTION U1000 that are INTENTIONAL
  `//nolint:unused` scaffolding** — `(*MetaFSM).incDEKRef` / `decDEKRef` (`meta_fsm_rotation.go`, kept
  wired for the S7 DEK-prune-safety predicate) and `metrics.bucketStates` (`operator_state.go`,
  operator-state scaffolding v0.0.388-389). Standalone staticcheck does not honor golangci's `//nolint`
  directive, so it reports them; `make lint` correctly skips them. If standalone staticcheck is ever
  wired into CI, switch these to `//lint:ignore U1000 <reason>` for parity, or accept the noise. No
  code change otherwise — recorded so the next /health run does not misread these as a regression.

- **[P4][optional] ~18 ST1005 error-string style nits** (capitalized / trailing-punctuation error
  strings in `kek_rotation_leader.go`, `rotation_state.go`, `preflight_errors.go`, `dek_keeper_*.go`,
  `encrypt/keystore.go`, `iam/oidc/config.go`) not gated by `make lint`. Enable golangci `stylecheck`
  to gate them going forward, or leave as-is.

### ROADMAP v2 retirement — remaining deferred / unverified items (2026-06-25)

`ROADMAP.md` (the GrainFS Technical Roadmap v2) was deleted on 2026-06-25 because every shipping
deliverable across Phases 0–9 (+ 6.5) is implemented and merged to master (current `v0.0.671.0`); the
data-plane raft-free epic (Slice 0 #846 / Slice 1 #847 / **Slice 2 #849** — its `(NEXT)` marker was
stale) is also complete, so per-object raft propose is 0 (`fsm.go` RETIRED SLOTS). The roadmap was a
completed ledger; its full history lives in git + CHANGELOG. The genuinely **deferred / unverified**
items it carried are preserved below so they are not lost.

- **[P2][future-feature] Topology migration (object physical relocation + dual-read).** Phase 7
  shipped numGroups **expansion** via generation-probe (data-movement-zero: new groups become a new
  placement generation; reads probe newest-gen-first and fall back to older gens; existing objects are
  never remapped). The alternative — physically relocating existing objects to match the new placement
  with a dual-read window — was explicitly scoped out to a follow-up "consolidation phase" (decision:
  S7-1, generation-probe chosen over migration). Needs its own spec + plan-gate. Trigger: when probe
  fan-out cost (∝ #generations) after many expansions becomes a real operational drag.

- **[P3][known-limitation] numGroups reduction (group 감소) is unsupported.** Phase 7 unlocked
  group *addition* on a running cluster (`grainfs cluster expand-placement`); group *removal* was left
  out of scope (nodes-in-group still shrink/heal via EC). A safe reduction needs object drain off the
  retired group + generation retirement semantics.

- **[P2][validation-gap] Multinode concurrent topology expansion under load — NOT validated.** S7-6
  shipped the add-protocol + cross-generation LWW fence machinery but validated only correctness +
  default-byte-identical (single-flip), not multinode concurrent expansion under load or throughput
  parity (the GCP bench was not run — GO was an eyes-open user override). The fence is armed
  **per-node** via the meta-FSM post-commit hook, so during a raft apply-skew window a lagging node can
  still serve a stale read through the un-armed fast path. Validate concurrent expand-under-load on a
  real multi-node cluster before relying on it in production.

- **[P2][validation-gap, eyes-open] GET/HEAD multihost no-regress benchmark — unmeasured.** Phase 5's
  merge-gate ② (GET/HEAD no-regress, since meta read went from one raft-read to multiple quorum-reads)
  was discharged by read-path analysis only; the multihost A/B measurement was blocked by infra
  (122MB-binary IAP-scp corruption + SPOT-reboot `WaitDEKReady` flake) and accepted as a residual risk
  at the Phase 5 GO. S6-2 then made hot-node read-rerank **live** on the GET path (previously inert
  because the hot-set was always empty), so this newly-active rerank path is also unmeasured under
  multihost load. devel-fixed single-arm multihost numbers are known (PUT 343 / GET 676 MiB/s, HEAD
  2080 obj/s, 0 errors). Needs stable infra (fresh/non-SPOT VMs + GCS-relay binary transfer) for the
  cross-binary A/B.

- **[P3][validation-gap, eyes-open] HTTP transport multinode performance — unmeasured.** The TCP→HTTP
  transport flip (Phase 8 S8-5) + control-plane-over-HTTP (S8-3) shipped with macOS functional-only
  validation (QUIC→TCP §6 eyes-open precedent). HTTP/1.1 is one-in-flight-per-conn vs the removed mux
  corrID multiplexing, so high-concurrency multinode behavior (pooled-conn pressure, per-heartbeat
  amplification in the non-coalesced legacy raft path) was never benchmarked. Transport was confirmed
  to not be the PUT ceiling, so this is a simplification-bet risk, not a perf-lever regression.

- **[P3][trigger-gated] Separate-repo extraction of primitives (Phase 9).** The in-repo package
  boundary split is done (`internal/raft` already standalone; `internal/hrw`, `internal/gossip`
  extracted with cluster coupling inverted via interfaces). All four primitives (raft / HRW / bounded /
  gossip) were decided to **stay in-house** (no hashicorp/raft / memberlist adoption — feature loss +
  migration cost outweigh gains). Extracting HRW/gossip to standalone repos is deferred until a second
  external consumer appears (~1 day mechanical work when triggered).

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
