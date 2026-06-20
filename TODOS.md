# TODO

## Follow-ups

- **[EPIC] Per-version quorum-meta foundation — move versioned object metadata off data-raft.**
  Completes the data-raft bypass for versioning: quorum-meta stores immutable PER-VERSION blobs
  (`.quorum_meta_versions/{bucket}/{key}/{vid}`, RDH-placed, K-of-N replicated), so version history lives
  in the no-raft world. **Keystone:** "latest" is derive-by-scan (max live VersionID; generation-walk on
  grown clusters) — no mutable latest pointer, so the LWW-vs-older-ModTime tension that blocked both
  deferred epics below simply does not exist. This foundation **subsumes** the two items that follow:
  versioned hard-delete/LIST consistency becomes S2, and non-latest EC-redundancy durability becomes S5
  (per-version metadata is now replicable → genesis records survive node loss via re-fan-out, not a
  flaky cross-group raft migration). Roadmap (each slice = own spec→plan→PR):
  - **S1 — per-version dual-WRITE — DONE (this PR).** `writeQuorumMeta` also fans out the per-version
    blob (best-effort, versioning-gated, behavior-neutral; reads/scans/LIST untouched).
  - **S2a — HEAD/GET switch + dual-delete — DONE (this PR).** HEAD/GET (latest derive-by-scan +
    specific-version direct) per-version-authoritative (versioning-Enabled gated, all-groups fan-out
    union NOT probeRead, legacy/mixed-era → FSM `ObjectMetaKeyV` fallback); `DeleteObjectVersion`
    fail-closed dual-deletes the per-version blob. Solves Epic A versioned-hard-delete HEAD/GET
    consistency for versioned objects. Known transitional limits (documented in the S2a spec): per-read
    O(#versions) enumeration cost (benchmark gate before S4); genesis-1+0 split-placement partial-union
    stale-latest (removed by S5); HEAD-vs-LIST divergence for hard-deleted-latest until S2b.
  - **S2b split into PR-A + PR-B** (plan gate: the cluster-gate fix is a cross-cutting prerequisite).
    - **S2b PR-A — cluster-wide versioning-context propagation — DONE (this PR).** Stamps the bucket-
      versioning decision at the SERVER edge (mirror PUT; coordinator must NOT read versioning on the
      data path — `TestControlDataPlaneBoundary...` invariant) for all read/list handlers (GET/HEAD/
      GET?vid/HEAD?vid/LIST), adds `ctx` to `storage.VersionedGetter`/`VersionedHeader`, carries a
      `versioning_state` tri-state across the 6 read/list forward args. **Fixes a latent S2a (v0.0.611.0)
      bug: versioned HEAD/GET were inactive (legacy stale fallback) in multi-group/multi-node clusters**
      because the gate read the wrong (local group) store; now Epic A read consistency is active in
      clusters. Copy-source reads now use the SOURCE bucket's decision. (Note: `applyCopyObjectTags`'s
      source `GetObjectTags` is a separate non-versioning-gated path — out of scope, flagged.)
    - **S2b PR-B — ListObjects per-version derive — DONE (this PR).** `ScanQuorumMetaVersionsBucket` walker
      (robust `WalkDir`-all-files + group-by-decoded-`cmd.Key`, since keys contain `/`; O(total versions))
      + `ScanQuorumMetaVersions` RPC + `listObjectsPerVersion` (all-groups fan-out, max-vid, marker-exclude)
      + branch the three object_list.go methods (via `listLatestEntries`) on versioning-Enabled, using
      `objectFromCmd`. **Did NOT touch `scatterGatherList`/`ScanQuorumMetaBucket`** (`ScanObjectMetaEntries`
      + 3 legacy consumers stay latest-only). Closes the S2a→S2b HEAD-vs-LIST window. **Code-gate fix
      (data-loss):** the derive is gated on the STAMPED ctx flag ONLY (`bucketVersioningFromContext`:
      resolved && enabled), NOT `bucketVersioningEnabled`'s local-read fallback — otherwise unstamped
      internal consumers (DeleteBucket empty-check, vfs/nfs4/p9/metrics) would activate the derive, and a
      best-effort per-version write failure could make the derive omit a live object → DeleteBucket deletes
      a non-empty bucket. Stamped-only scopes the derive to the S3 LIST edge (PR-A re-stamps on forward).
      Residuals (tracked): (a) best-effort per-version write → transient HEAD/LIST divergence on partial
      write failure, reconciled by S3 backfill; (b) `deriveLatestVersion` (HEAD) lacks the MetaSeq tiebreak
      the LIST walker uses. Benchmark before S4 (O(total versions) per LIST). ListObjectVersions stays
      FSM-backed (deferrable to S4).
    - **S2b residual — per-version orphan reconciliation scrubber — DONE (this PR).** A background sweep
      (`orphan_quorum_meta_version_walker.go`, analog of the EC orphan-shard scrubber) reclaims dangling
      per-version blobs whose FSM `obj:` record is gone (the residual of a partially-failed S2a hard-delete
      dual-delete), stopping the derive-by-scan LIST/HEAD?vid resurface. Authority = `obj:` record present
      in ANY hosted group (delete-marker counts as present → keep); fail-closed on any read error; reuses
      the shard sweep's gate + all-hosted-caught-up + owning-group-hosted + floored age-gate + two-cycle
      tombstone; node-local delete (each placement node reclaims its own copy — VERIFIED INVARIANT: blobs
      are written only to owning-group placement nodes and never relocated). Metrics:
      `grainfs_scrub_orphan_quorum_meta_versions_{found,deleted}_total` + `..._sweep_capped_total`.
      **Follow-up (tracked):** full S3-level e2e (versioned PUT/DELETE through the server + lingering-blob
      injection) — landed with a deterministic `internal/cluster` integration test
      (`TestPerVersionOrphanReconcile_StopsDeriveByScanResurface`) instead, since a clean blob-injection
      seam through the S3 edge does not yet exist and multi-node e2e is resource-flaky.
  - **S3 — migrate existing data — DONE (this PR).** Background, idempotent per-version blob backfill
    sweep (`per_version_backfill_walker.go` + `scrubber/per_version_backfill.go`, wired into the scrubber
    tick beside the orphan version sweep). Enumerates FSM `obj:{bucket}/{key}/{vid}` records across ALL
    locally-hosted generation-group stores (`hostedGroupBackends()` — a single-store scan would miss
    older-generation versions), yields versions whose per-version blob is ABSENT on disk (direct `os.Stat`
    idempotency, NEVER overwrites an existing blob), age-gated by the VersionID's UUIDv7 timestamp (NOT
    `LastModified`, which is 0 for fresh markers), and replays S1's fan-out (`backfillPerVersionBlob` →
    `writeQuorumMetaVersionLocal`/`WriteQuorumMetaVersion` to `objectMeta.NodeIDs`). Metrics:
    `grainfs_scrub_quorum_meta_versions_backfill_{found,migrated,capped,error}_total`. **Correctness
    boundaries handled (codex code-gate):** (1) `IsDeleteMarker` reconstructed from `ETag==deleteMarkerETag`
    (objectMeta has no such field) so a backfilled marker reads as deleted; (2) field-equivalence not
    byte-identical — `ExpectedETag`/`PreserveLatest`/`IsAppendable`/`Coalesced` are not in `objectMeta`,
    all read-irrelevant for normal versioned objects; (3) appendable/coalesced versions SKIPPED (the spec
    carve-out — they get no per-version blob); (4) unversioned `obj:` records (key may end in a UUID-looking
    segment) rejected via `meta.Key == parsedKey`; (5) **empty-NodeIDs degraded delete markers** (placement
    unreadable at delete time) get RDH-direct placement (`deriveMarkerPlacement` — owning-group peers + EC
    config) — correct because the tombstone metadata blob need only be DISCOVERABLE by the all-groups
    `readQuorumMetaVersions` readback, not byte-match a data write's shard placement. **S4 verification
    note:** S4's "after migration verified" gate must confirm per-version blob completeness before removing
    the FSM fallback (the backfill is best-effort; e.g. a key whose placement is wholly unresolvable, or a
    version whose owning group is hosted on no live node, is not reachable by this per-node sweep).
  - **S4a — cutover-readiness VERIFICATION gate — DONE (this PR).** Non-breaking, read-only. S4 cutover is
    data-loss-risky without proving completeness first (S3 backfill is best-effort), so this gate ships
    BEFORE any removal. `verifyPerVersionCutover` classifies every FSM `obj:` version (across hosted
    generation groups, Enabled+Suspended buckets, non-appendable, non-internal) COMPLETE / GAP /
    STUCK / UNKNOWN / EXCLUDED. **Completeness = decoded STRICT readback matching the EXACT post-cutover
    read** (`readQuorumMetaVersionsStrict` + layout-dispatch parity vs `getObjectVersionCtx`: marker OK /
    Segments / EC-resolvable) — NOT a local `os.Stat`. **Fail-closed everywhere** (decode/panic/readback/
    versioning-read/scan errors → UNKNOWN or a returned error → not-ready): per-node gauges
    `grainfs_per_version_cutover_{complete,gaps,stuck,unknown,excluded,verify_errors}` (verify_errors
    starts at 1, → 0 only after a fully-clean completed sweep, so default-zero/partial/failed sweeps read
    not-ready), background scrubber-tick sweep, and node-local admin CLI `grainfs cluster verify-per-version`
    (JSON over adminapi, exit non-zero on gaps+stuck+unknown). **Cutover-ready = `gaps+stuck+unknown+
    verify_errors == 0` on EVERY node** (sum/per-node aggregation). **SCOPE the breaking S4 slices must
    respect**: this gate covers the versioned (Enabled/Suspended) non-appendable per-version-blob fallback
    ONLY. It does NOT cover the non-versioned latest-only-blob path or appendable/coalesced (FSM
    carve-out); the cutover must NOT remove fallbacks this gate does not check without extending it first.
    The gate verifies cutover does not REGRESS readability (layout dispatch), not deep segment-data
    integrity (identical pre/post cutover — scrubber's job).
  - **S4b — repoint reads to per-version blobs (FSM fallback retained as safety net).** Repoint
    `ListObjectVersions` (still FSM-`lat:`/`obj:`-backed, object_version.go) + `ScanObjects` (`lat:` walk) to
    the per-version-blob source, behavior-neutral with the FSM fallback kept. Separate slice.
    - **S4b PR-A — DONE (versioning-ctx plumbing, behavior-neutral).** Threaded a stamped version-history
      decision through the `ListObjectVersions` path (interface ctx param; `ListObjectVersionsArgs.versioning_state`
      wire field; forward sender/receiver; `ctxWithVersionHistory` edge resolver = `Enabled||Suspended`). Backend
      still does the FSM scan — nothing reads the flag yet.
    - **S4b PR-B — the actual per-version derive (TODO).** Add the missing all-versions-all-keys bucket enumerator
      over `.quorum_meta_versions` (`ScanQuorumMetaVersionsBucket` is max-VID-per-key only), build `storage.ObjectVersion`
      (IsLatest via per-key max-VID + `reconcileVersionIsLatest`; delete markers via `IsDeleteMarker`/`deleteMarkerETag`),
      gate on the stamped flag, FSM fallback. **Two hazards the S4b PR-A code-gate (Claude+codex adversarial) flagged —
      PR-B MUST handle, do not assume PR-A solved them:** (1) **Rolling-upgrade wire-compat:** an old peer omits
      `versioning_state` → receiver reads `versioningStateUnknown` → unstamped → local/FSM fallback. Behavior-neutral
      while the flag is unread, but once PR-B's derive consumes it, old-sender→new-receiver during a rolling upgrade
      could return PARTIAL version history with HTTP 200. PR-B needs a capability gate or explicit fail-closed handling
      before the flag becomes authoritative. (2) **`Suspended` bool dual-meaning:** `ctxWithVersionHistory` stamps the
      shared `bucketVersionedCtxKey` bool TRUE for `Suspended` (history-bearing), but the SAME bool means Enabled-only
      on the ListObjects/commit paths (`object_list.go:49`, `quorum_meta.go:44`). Safe today (path-scoped by convention;
      the versions backend never reads it), but PR-B must use the Enabled-or-Suspended semantic when it reads the flag
      inside the versions derive, and must NOT route this ctx into an Enabled-only consumer.
    - **S4b lifecycle/recovery flag (PR-B):** lifecycle expiration + recovery-gate call `ListObjectVersions` with an
      unstamped `context.Background()` → FSM path. PR-B must decide whether they need the derive (stamp at their entry).
  - **S4c — cutover: per-version sole authority; remove FSM object-meta path** (`CmdPutObjectMeta`/`apply*`/
    `obj:`/`lat:`), repoint scrubber/snapshot. **GATED on S4a reading clean** (`gaps+stuck+unknown+
    verify_errors == 0` cluster-wide, confirmed STABLE). **Appendable/coalesced carve-out:** those objects
    bypass quorum-meta (append.go:158-166; `PutObjectMetaCmd` lacks `IsAppendable`/`Coalesced`) → stay
    FSM-authoritative, so the FSM object path is retained ONLY for them. raft out of PUT/GET for
    non-appendable objects. **Must NOT remove the non-versioned latest-only-blob fallback** (out of S4a's
    verification scope) without extending S4a.
  - **S5 — Epic B:** genesis per-version meta re-fan-out + data re-encode (reuse EC-redundancy sweep).
  Spec/keystone detail: docs/superpowers/specs/2026-06-17-per-version-quorum-meta-foundation-design.md
  (root worktree, git-ignored). Non-versioned buckets are already quorum-meta-only (latest-only blob,
  no FSM propose) — untouched by this epic.

- **[EPIC] Retroactive EC-redundancy upgrade for NON-LATEST versions (versioning-enabled buckets).**
  The shipped sweep (v0.0.608.0) upgrades non-redundant (1+0) genesis objects but is **latest-version
  only**. Old non-latest 1+0 versions in a versioning-enabled cluster that started single-node stay
  non-redundant. A full prototype was built and reverted because it is **runtime-fragile (flaky ~50%
  in e2e)** — it needs robustness work that is its own epic, not a sweep tweak. Everything learned:

  **Architecture (why it is hard).** A per-version metadata record (`obj:{bucket}/{key}/{versionID}`)
  lives in the SINGLE group `groupIDForObject(bucket,key)` selects **at the generation the version was
  written**. On cluster growth a new placement generation is recorded with a larger candidate set, so
  the same key hashes to a different group — a genesis (pre-growth) version's record sits in the
  ORIGINAL (typically single-peer) generation group, while newer versions live in the current owning
  group. The versioned read (`probeRead` → `RouteObjectReadGenerations`, newest-generation-first) walks
  generations and resolves the version from whichever group holds it.

  **Why in-place relocation does NOT work.** The genesis group is single-peer (only the genesis node);
  its raft FSM — and thus the record — dies when that node is lost, no matter how redundant the DATA
  is. So the record must be MIGRATED to a multi-peer group, not rewritten in place.

  **The design that is functionally correct (code-gate SHIP, but flaky):** run on the newest-generation
  owning group's leader; read the version authoritatively (forwards); if no redundant copy exists,
  re-encode + commit a copy to the newest-gen (multi-peer) group with `PreserveLatest` (so the latest
  pointer never moves); then PURGE the stale copy from the older-generation group(s) via a forwarded
  `CmdDeleteObjectVersion`, so the version ends with exactly one record in a node-loss-surviving group.
  Add `(Key,VersionID)` dedup to `fanOutListObjectVersions` (cross-group LIST) for the migration window.
  Add a non-latest-specific relocated counter (`grainfs_ec_redundancy_upgrade_nonlatest_relocated_total`)
  so the e2e can wait on the SPECIFIC migration (the shared total mixes latest + non-latest).

  **Why it was reverted — runtime fragility.** The migration has fragile preconditions that do not hold
  reliably in a freshly-grown multi-group cluster: (1) only the newest-gen owning group's LEADER runs
  it (leadership churns during/after growth); (2) the cross-group version enumeration
  (`coordinator.ListObjectVersions`) is FAIL-CLOSED — one slow/unreachable group fails the whole list,
  so the candidate is invisible that cycle; (3) generation/gen-0 capture timing. Net: latest-path e2e
  is **2/2 deterministic**; non-latest e2e is **~50% flaky** (and worse under host load). Making it
  robust needs: a TOLERANT cross-group enumeration for the sweep (skip a failed group, retry, don't
  fail-closed), leader-agnostic or convergence-guaranteed migration driving, and a deterministic
  per-version readiness signal.

  **Orthogonal pre-existing bug (Q5) — FIXED independently.** `DeleteObjectVersion` routed via
  `RouteObjectRead`/`currentGroupIDs()` = newest-generation ONLY (no generation walk), so a user delete
  of a *pre-migration* old version missed its resident (older-gen) record and was a no-op. Fixed by a
  deduped generation fan-out (`routeReadGenerations`) in `ClusterCoordinator.DeleteObjectVersion` —
  `applyDeleteObjectVersion` is idempotent, so non-resident generation groups no-op and the resident
  older-gen group deletes the record. This is shipped separately from the EPIC; old non-latest version
  deletes now work without waiting on the migration.

  Scope: this is genuinely a multi-group per-version-record consistency subsystem, not a sweep tweak.
  Common deployments (versioning disabled, or form-then-write / `--bootstrap-expect-nodes`) are
  unaffected — this gap is "single-node start + versioning enabled + old non-latest 1+0 versions" only.

- **DeleteObjectVersion of the *latest* version leaves a stale quorum-meta latest pointer.**
  `DeleteObjectVersion` (internal/cluster/object_version.go) deletes only the data-raft FSM per-version
  record (`obj:bucket/key/versionID`); it never updates quorum-meta, whose `bucket/key` blob holds the
  latest pointer. So `DELETE ?versionId=<latest>` removes the version record but leaves the quorum-meta
  latest pointer dangling, and a subsequent `HEAD`/`GET` (which reads quorum-meta first,
  object_version.go:33-44) can still return the deleted latest version. This is **pre-existing** (long
  predates the Q5 generation-walk fix) and orthogonal to generation routing — Q5 deliberately did not
  touch delete-side quorum-meta semantics. Surfaced by the Q5 plan gate.

  **[EPIC — deferred] This is NOT a one-line "clear the stale pointer" fix.** Deep investigation (2026-06-17)
  found both obvious fixes are flawed, because `LIST` (`scatterGatherList`, quorum_meta.go:872) scans
  quorum-meta ONLY and never falls back to the FSM:
  - **delete-only** (drop the stale blob): HEAD/GET become correct via the FSM fallback
    (`lat:`→`obj:<next>`, object_get.go:301-313) with zero corruption/LWW risk — but the rolled-back
    latest then **vanishes from LIST** until the next write (LIST is quorum-meta-only), so LIST and GET
    disagree.
  - **promote** (delete the stale blob, then re-publish the previous version's blob): makes LIST correct
    too, but (1) there is no inverse seam and `PutObjectMetaCmd` is **lossy** (no `Coalesced`/`IsAppendable`
    fields, which `objectMeta` has) → reconstructing a blob for appendable/coalesced objects corrupts them;
    (2) in a **mixed-type version chain** (e.g. v1 appendable = FSM-only with no quorum blob, v2 a plain PUT)
    promote writes a lossy blob and HEAD then mis-reads v1 as non-appendable — strictly **worse** than
    delete-only, whose FSM fallback is correct; (3) the promoted older version carries an older `ModTime`
    that **loses** `quorumMetaBlobWins` (quorum_meta.go:120, ModTime>VersionID>MetaSeq) against any stale
    blob that survived a best-effort delete on an unreachable node → the deleted version resurfaces
    (the same LWW-vs-older-version tension that got the non-latest EC-redundancy migration reverted);
    (4) a delete-then-write concurrency window; (5) under Q5's per-generation fan-out the backend
    `DeleteObjectVersion` runs N× → promote thrash.

  **Recommended direction for the epic:** instead of promote, make `scatterGatherList` (LIST) **fall back
  to / merge the FSM** for keys absent from quorum-meta, then pair it with delete-only. That sidesteps
  promote's lossy-cmd reconstruction AND the LWW older-ModTime hole entirely, at the cost of a Phase-4
  index-free-LIST architecture change. This is entangled with the same quorum-meta-only-LIST +
  LWW-vs-older-version root tension as the non-latest EC-redundancy epic above, so the two are best
  designed together.

  Cluster-only (single-node uses the erasure backend with no quorum-meta). Repro: `PUT v1` → `PUT v2`
  → `DELETE ?versionId=<v2>` → `HEAD` returns the deleted v2 instead of v1.

## S4c-0 PR1 follow-ups (metadata-RMW lost-update fix)

- **[P3][flaky] Pre-existing `-race` test-harness race in cluster tests.** `go test ./internal/cluster/ -race` intermittently fails with a data race between a test's direct field write (`per_version_backfill_walker_test.go:554`) and the background `RunApplyLoop` (`backend.go:812`), started by `newTestDistributedBackendWithDB` (`backend_test.go:103`). **Confirmed pre-existing on master 6b50c503** (not introduced by the metadata-RMW PR); the non-`-race` suite and `make test-unit` are green. The test pokes a backend field while the apply-loop goroutine reads it without synchronization. Fix: synchronize the test's field mutation with the apply loop (or stop the loop before mutating). Affects `-race` CI reliability only. Found while running the PR's verification gate.
- **[P3] Versioned-bucket tag/ACL RMW holds the per-`(bucket,key)` meta-RMW lock across a Raft propose.** Under `on`-versioning, `SetObjectTags/ACLPropose` → `writeQuorumMeta` calls `b.propose` (raft) while holding `objectMetaRMWLock` (`quorum_meta.go:63-65`). No deadlock (propose never takes that lock; mirrors pre-PR behavior), but it serializes same-object versioned RMW behind a raft round-trip. Revisit if high-concurrency versioned-bucket tagging becomes hot.
- **[P3][test polish] Strengthen S4c-0 PR1 tests:** ACL concurrency test asserts only `MetaSeq==n` (add a final-ACL-value coherence check); latest-writer overwrite-on-tie intent lives only in a code comment (add a tie-case test); `TestWriteQuorumMetaVersionLocal_OverwritesWhenCandidateWins` uses `got, _ :=` (discards ReadFile error). All non-blocking; surfaced in task reviews.
- **[P3] Unbounded per-object lock-map growth.** `objectMetaRMWLocks` (per-`(bucket,key)`) and `quorumMetaTargetLocks` (per blob-target-path) grow one entry per unique object seen and are never evicted. Mirrors the existing accepted pattern (`shardLocks`); a long-running node with millions of objects accumulates lock entries. Consider a bounded/striped lock pool or LRU eviction across all four maps (`shardLocks`/`objectMetaRMWLocks`/`quorumMetaTargetLocks`) if it ever shows up in heap profiles.

## S4c-0 PR2 follow-ups (multipart-complete idempotency)

- **[P2][spec] Amend v8 §MPU: VersionID is minted-at-complete, not pinned-at-create.** The S4c v8 design (docs/superpowers/specs/2026-06-18-s4c-cutover-design-v8.md §MPU) prescribed "pin VersionID at CreateMultipartUpload". The PR2 plan-gate found that unsound: `newVersionID()` is UUIDv7 (k-sortable by time), and the S4c per-version derive relies on "max live VID = latest" — a create-time-pinned VID would stamp an MPU completed after a later PUT with an OLDER VID and break the latest-derive. The correct mechanism (shipped in PR2): VID minted at completion; idempotency provided by the `mpudone:` marker. Update the spec before later S4c slices reference the old wording.
- **[P3] mpudone GC thundering-herd → leader-only proposing.** `SweepStaleMultipartDoneMarkers` runs on every node; in an N-node cluster each node scans its local `mpudone:` and proposes the same stale batch each scrub cycle (N redundant raft entries/cycle). Apply is idempotent so it's correct, but a leader-only gate (or dedup) would cut the redundant proposes. Low priority given 24h-aged, ≤256-batch, O(minutes) cycle.
- **[P3][doc] mpudone 24h retention bounds the idempotency window.** The GC sweep expires markers after 24h (boot_phases_scrubber.go:211). A retry-after-success arriving >24h post-completion is no longer idempotency-protected (returns ErrUploadNotFound). 24h conservatively outlives realistic client retries + raft replay; document in the operator runbook.
- **[P3][test] No multi-node integration test for the forwarded-propose apply-wait + MPU phantom-winner guard.** The fix (forwarded propose waits for local apply; backend.go forwarded path) and the phantom-winner guard are covered by single-node tests + reasoning + the full suite, but a true 2/3-node follower-forwards-during-phantom-commit test isn't feasible in the current solo-leader unit harness. Add one if a multi-node raft test harness is introduced.

## S4c-a follow-ups (inert soleauth foundation flag)

- **[DONE] S4c-a2-A — arm the fence (inter-node path).** Per-bucket monotonic epoch (`soleauthepoch:{b}`, bumped on real transitions in `applySetBucketSoleAuthority`), per-bucket fence `RWMutex` (flip WLock via atomic `fenceLockFn`; leaves RLock), additive `admitted_soleauth_epoch` on the shard wire envelope, and a stale-epoch reject (`localEpoch>0 && wire<local`) at the four quorum-meta leaves with a backend-injected `soleAuthEpochFn` callback. Every legitimate-mutation caller threads the live epoch; dormant at epoch 0. Shipped in the S4c-a2 worktree.

## S4c-a3 follow-ups

- **[DONE] S4c-a3 — backfill soleauth-skip + all-versions enumerator.** (1) The per-version backfill walker fail-CLOSES for a bucket whose soleauth is pending/on (or whose soleauth read errors) — `backfillSkipForSoleAuth`. (2) `ScanQuorumMetaVersionsBucketAll` (local all-version scan, mirror of `ScanQuorumMetaVersionsBucket` minus the max-per-key collapse) + `ScanQuorumMetaVersionsAll` peer RPC (new msgType, fail-closed on un-upgraded-peer `"Error"` reply). Dormant rolling-upgrade primitive — NO production caller yet (cluster fan-in deferred to consumers). Shipped in the s4c-a3 worktree.
- **[DONE] S4c-b — per-node soleauth-on snapshot/restore + verifier ineligibility (Enabled-only scope).** Shipped in the s4c-b worktree. (1) Verifier flags non-Enabled buckets **Ineligible** (definitive per-bucket signal keyed on versioning state, NOT an object count — `forEachHostedObjVersion` silently skips legacy records, so a counts-only verdict false-READ a legacy-only bucket); threaded cluster→scrubber→server→CLI; CLI `gaps+stuck+unknown+ineligible > 0` → not-ready; scrubber publishes `grainfs_per_version_cutover_ineligible` (observability, NOT in the cluster-wide blocking gate). (2) `SnapshotObject` blob-fidelity fields (NodeIDs/ECData/ECParity/StripeBytes/PlacementGroupID/MetaSeq/UserMetadata/Parts) + `scanQuorumMetaVersionsBucketAllStrict` (fail-closed local enumerator — first consumer of a3's all-versions scan). (3) `on`-bucket per-version capture (strict, full fidelity, IsLatest=max-VID) + restore (raw VIDs, force-locked under `bucketSoleAuthLock` quiesce, **absent-blob purge** via strict scan, **exact-version** presence `blobExistsExactVersion` [objectPathV / any EC shard index from the SNAPSHOT's captured placement — no legacy/sibling/live-config vouch], stale-skip). (4) `SetBucketSoleAuthorityCmd.EpochFloor` monotonic floor (additive FB field) so restore preserves soleauth epoch fidelity. (5) Conditional `minReader=2` bump when any bucket `SoleAuthState=="on" || SoleAuthEpoch>0` (old binaries refuse soleauth-on snapshots). All DORMANT (no bucket ever `on`). codex code-gate CLEAN after iterating 6 [P1]s (routed RestoreBuckets/ListAllObjects/RestoreObjects fail-closed guards; exact-version presence vs HeadObject-latest / legacy-plain-file / shard_0-fixed / live-config-drift). Per-node refutation (local-share check; cluster-wide reconstructability is the sweep's job) codex-accepted.
- **[P1][S4c-d HARD precondition] Routed-cluster per-node snapshot for soleauth-on buckets.** S4c-b makes the routed `ClusterCoordinator` snapshot path (`ListAllObjects`/`RestoreObjects`/`RestoreBuckets`) **fail closed** (refuse) for any soleauth-on bucket — cluster-wide capture/restore cannot correctly handle per-node per-version shares. Before S4c-d can flip a bucket `on` in a routed (multi-group) cluster, snapshot capture/restore must run **per-node** (each node captures/restores its own local share; cluster-wide = union; under-replication healed by sweep), wired into `snapshot.Manager`'s routed path. Until then a soleauth-on bucket in a routed cluster has no working snapshot/restore. Surfaced by the S4c-b code-gate; same deferral class as the boot-window precondition.
- **[DONE] S4c-c-read1 — single-object read authority under `soleauth=on` (Enabled-only, dormant).** Shipped in the s4c-c worktree (v0.0.625.0). HEAD/GET latest (`headObjectMeta`), specific-version (`headObjectMetaV`), and `GetObjectTags` each gain an `on` branch: derive from the per-version blob authoritatively; a blob-absent vid-bearing versioned object → 404 (no stale FSM resurrection); specific delete-marker → 405; latest not-live → 404. Carve-out classes (appendable / coalesced / **legacy unversioned bare records** — `obj:{b}/{k}` with no vid, an Enabled bucket can still hold pre-versioning bare objects the backfill/verifier iterator skips at slash<0) stay FSM-authoritative, single-sourced in `fsmCarveoutObject` (bare-vs-versioned resolved from the FSM KEY read, not the call-site versionID). `soleAuthReadOn` fails closed on store error. `off`/`pending` byte-identical. `GetObjectAcl` covered (ACL rides in the returned object). codex code-gate CLEAN after fixing T3 delete-marker handling ([P1] latest-not-live fell to stale carve-out; [P2] specific delete-marker returned tags instead of 405). plan-gate 3 rounds (codex) folded: legacy-bare carve-out, forward-fence deferral, MPU-verifier dependency. **Remaining S4c-c: read2** (all-version enumeration: ListObjectVersions, bucket emptiness/force-delete enumeration, scrubber ScanObjects + cluster-wide all-version fan-in wiring `ScanQuorumMetaVersionsAll`).
- **[P1][S4c-d HARD precondition] Gate every `soleauth=on` activation path (admin flip AND direct snapshot restore) behind verifier-clean + read-fence + boot-window.** The routed `ClusterCoordinator.RestoreBuckets` rejects an `on` snapshot bucket (cluster_coordinator.go:1008), but the direct `DistributedBackend.RestoreBuckets` permits propagating `on` (snapshotable.go:222/285 — intended S4c-b per-node-restore behavior). Today no path produces a *first* `on` (the admin flip is unbuilt; restore only propagates an existing `on`), so no `on` snapshot can exist and the S4c-c-read1 `on` read branch is unreachable. When S4c-d makes `on` first reachable, BOTH the admin flip and the direct restore path must enforce: (1) verifier `gaps+stuck+unknown+verify_errors==0` — note MPU completion mirrors the per-version blob **best-effort/warning-only** (object_put.go:414), so a missing MPU blob shows as gap/stuck and the flip must refuse; (2) the forwarded-read fence below; (3) the boot-window fence (above). Surfaced by the S4c-c-read1 plan-gate (codex Q2/Q4).
- **[DONE] S4c-d precondition — forwarded version/tag/ReadAt reads are now Raft-read-fenced (v0.0.629.0).** `waitForwardReadFence` (ReadIndex+WaitApplied) was added to the SIX forwarded read handlers that resolve through a read1 `soleauth=on` authority branch but lacked it: `handleHeadObjectVersion`, `handleGetObjectVersion` (+ streaming `handleGetObjectVersionRead`), `handleGetObjectTags`, and `handleReadAt` (+ streaming `handleReadAtRead`). They now match the already-fenced latest HEAD/GET (`handleHeadObject`/`handleGetObject`/`handleGetObjectRead`) and `handleListObjectVersions`. **Scope note:** `ReadAt`/`ReadAtRead` were beyond the originally-named version+tags set, but `ReadAt` resolves the LATEST via the same `headObjectMeta` `on` branch as the fenced `handleHeadObject`, so leaving it unfenced was the same gap (codex plan-gate confirmed inclusion). Streaming tails fence BEFORE constructing the lazy `backendReadAtStream` / returning the reader. A forwarded read on a node that cannot obtain a ReadIndex now fails closed (consistent with the fenced siblings); result unchanged when the receiver is current. This is a LIVE linearizability-parity fix (not dormant) motivated by the cutover. plan-gate codex CLEAN (1R) + code-gate codex CLEAN. TDD via a `fenceErrNode` (ReadIndex-erroring node swap) asserting all 6 handlers return Internal before resolving.
- **[DONE] S4c-c-read2a — cluster-wide all-version fan-in + ListObjectVersions authority under `soleauth=on` (Enabled-only, dormant).** Shipped in the s4c-c-read2a worktree (v0.0.626.0). (1) `scanQuorumMetaVersionsClusterAll` — cluster-wide, **end-to-end fail-closed** all-version blob enumerator (self = strict local scan; peers = `ScanQuorumMetaVersionsAll` RPC; ANY local/resolve/RPC/peer-decode error → return error, NOT skip; dedup by (Key,VersionID)+MetaSeq), with the peer handler switched to the strict local scan and the RPC client now erroring on a per-entry decode failure (was a silent skip — codex code-gate [P1]). (2) `ListObjectVersions` under `on`: leaf = cluster-wide blobs (IsLatest=per-key-max-VID, marker-latest still IsLatest, markers included) + THIS-node-LOCAL carve-out FSM records (appendable/coalesced/legacy-bare via the shared `isFsmCarveoutClass` predicate factored out of read1's `fsmCarveoutObject`), dropping plain vid-bearing FSM (no stale resurrection), blob-wins on (Key,VID) collision; coordinator = fan-out maxKeys=0 + keep-one (Key,VID) dedup + sort + maxKeys-once, no reconcile. `off`/`pending` byte-identical. codex plan-gate 4 rounds + code-gate CLEAN (read2a diff). Followed by read2b (below) — **S4c-c READ-derive stage is now COMPLETE.**
- **[DONE] S4c-c-read2b — DeleteBucket emptiness + ForceDeleteBucket + scrubber ScanObjects under `soleauth=on` (Enabled-only, dormant); completes S4c-c READ-derive.** Shipped in the s4c-c-read2b worktree (v0.0.627.0). (1) **`DeleteBucket` emptiness** `on`: leaf runs the blob-authority probe (`listObjectVersionsSoleAuth`, markers included + carve-out) OUTSIDE the metadata read txn (avoids a nested `store.View`); coordinator probes via `ListObjectVersions(...,1)` instead of `ListObjects`. A stale non-carve-out vid-bearing FSM record no longer false-blocks an authoritatively-empty bucket. (2) **`ForceDeleteBucket`** `on`: coordinator enumerates the authoritative set cluster-wide (fail-closed), deletes vid-bearing via the generation-aware blob-purging `c.DeleteObjectVersion`, legacy-bare via a hard delete fanned out to the bucket-assigned group + EVERY shard group (idempotent/convergent — fixes the bucket-era fallback group that the per-generation key hash misses), then a trailing blob-aware `DeleteBucket` as the fail-closed completeness backstop; leaf has the single-node path. Adds one new `on`-gated forward op `ForwardOpHardDeleteObject` (routed legacy-bare hard delete, distinct from the tombstone `DeleteObject`). (3) **`ScanObjects`** `on`: eager STRICT local all-version scan collapsed latest-per-key (markers/non-EC dropped) + local EC carve-out, own-EC-profile preserved, errors surfaced synchronously before the channel; repair-only/local model unchanged. The carve-out walk is single-sourced (`forEachLocalCarveout`) shared by `ListObjectVersions` and the scrubber so classification cannot drift. `off`/`pending` byte-identical. codex plan-gate 3 rounds (confirmed the central invariant: a non-carve-out vid-bearing FSM record is non-authoritative under `on`, so only blobs + carve-outs must be removed) + code-gate CLEAN.
- **[DONE] S4c-d precondition — bucket-name reuse: per-bucket state cleared on delete, soleauth epoch preserved as floor (v0.0.632.0).** `applyDeleteBucket` deleted only `BucketKey`, leaking `policy:{b}`/`bucketver:{b}`/`soleauth:{b}`/`soleauthepoch:{b}` into a recreated same-name bucket. It now clears `bucket:`/`policy:`/`bucketver:`/`soleauth:`(STATE) (tolerating absent keys, mirroring applyDeleteObject) so a recreated bucket starts fresh (no policy, unversioned, not stuck terminal-`on`). **`soleauthepoch:{b}` is DELIBERATELY PRESERVED** (user decision A): it is a monotonic cross-incarnation floor — clearing it would reset a recreated+reflipped bucket to epoch 1 and let a dead incarnation's stale forwarded write pass the fence (`soleAuthEpochStale(1,5)`=false), the exact hazard the cutover prevents. `EpochFloor` (fsm.go:307) is snapshot-restore-only and does NOT cover live delete+recreate. policy/versioning clears = live S3-correctness; soleauth state-clear + epoch-preserve = DORMANT (flip unreachable). plan-gate codex (Q1-Q5 CONFIRMED, no P1) + code-gate codex CLEAN. Tests: `TestDeleteBucket_ClearsSoleAuthState_PreservesEpochFloor` (crux: state cleared, epoch=2 preserved, reflip→3), `ClearsVersioning`, `ClearsPolicy`, `AbsentStateKeys_NoError`. Object `obj:`/`lat:` orphans stay the orphan scrubber's job (DeleteBucket requires empty).
- **[P2][bucket-reuse cross-subsystem follow-up] Clear lifecycle config + IAM bucket-upstream on bucket delete.** Surfaced by the bucket-name-reuse plan-gate (codex [P2]/[P3]). Two MORE per-bucket states leak on delete, in subsystems `applyDeleteBucket` (data-raft FSM keyspace) cannot reach: (1) `lifecycle:{bucket}` lifecycle config (MetaFSM/meta-raft + `internal/lifecycle/store.go`, boot_phases_srvopts.go:276); (2) IAM bucket-upstream (`internal/iam/store.go`, MetaFSM, separate meta command). Admin bucket delete (`internal/server/admin/handlers_bucket.go`) calls only `DeleteBucket`/`ForceDeleteBucket` + NFS cascade, not these. A recreated same-name bucket inherits old lifecycle/upstream config. Closing it needs the bucket-delete flow to fan out meta-raft lifecycle-delete + IAM-upstream-delete commands — a cross-subsystem cascade, NOT S4c-cutover-specific. User chose to ship the FSM-keyspace fix and track this separately.
- **[DONE] S4c-d precondition — read1 single-object `on` reads are now decode-strict (v0.0.628.0).** read1's three `on` branches (object_get.go HEAD/GET-latest, object_version.go specific-version, bucket.go GetObjectTags) read the per-version blobs through a NEW **decode-strict, availability-TOLERANT** reader `readQuorumMetaVersionsDecodeStrict` (+ `readQuorumMetaVersionDecodeStrict`): self via `readQuorumMetaVersionsRawLocal` (raw bytes, read error → fail-closed) + all-groups peer fan-out via a new raw RPC `ReadQuorumMetaVersionsRaw` (handler serves raw blobs WITHOUT decoding; unreachable/un-upgraded/disk-erroring peer → tolerate/continue) + ONE strict-decode loop over all collected raw blobs (ANY decode failure → fail-closed, since a corrupt blob's VID is unknown and might be the authoritative delete-marker-latest) + VID-MetaSeq dedup. The two fall-through-on-error swallows (object_get/object_version `verr == nil && …`) are fixed to return the error (no carve-out/older-live resurrection). **Decision (user, plan-gate [P2]):** decode-strict + availability-tolerant, NOT the verifier's availability-strict reader — read1 is the hot path and an unreachable peer resurrects nothing (only a silently-dropped corrupt blob does). The residual under-replication-AND-unreachable window is gated out by S4c-d's verifier-clean + minReader=2 flip preconditions. The tolerant `readQuorumMetaVersions` (off-path derive + delete blob purge) and the verifier's `readQuorumMetaVersionsStrict` are UNTOUCHED (the verifier's corrupt-present limitation stays SAFE = GAP/STUCK, never false-READY). off/pending byte-identical; DORMANT. plan-gate 2 rounds (Claude general-purpose [codex stalled 30 min] surfaced the availability [P2] → option B; codex CLEAN on option B) + code-gate codex CLEAN.
- **[P2][roadmap] S4c-c flag-on LIST consumes the all-versions enumerator** for the versioned LIST derive under `on`.

## S4c-a2 follow-ups

- **[DONE — single-group] S4c-d precondition — soleauth-fence boot-window closed (v0.0.630.0).** The shard RPC route goes live (`bootShardRoutes`, run.go:193) BEFORE the soleauth-epoch callback is wired (`SetShardService`→backend.go:449), and the wired callback then reads a store that LAGS until the data-raft apply loop replays its boot-committed backlog (run.go:204) — so the 4 quorum-meta leaf fences were bypassed (admitted any epoch) for the whole window. **Fix (chosen = fail-closed until reliable):** `soleAuthEpochFn func→atomic.Pointer` (race fix; nil-safe setter — `Store(nil)` for a nil fn else `(*p)()` panics on a non-nil ptr to a nil func) + NEW `soleAuthEpochReady atomic.Bool` (default false = fail-closed). Shared helper `rejectStaleSoleAuthEpoch`: when NOT reliable (fn unwired OR not-ready) admit `wire==0` (legit boot replication; every bucket today) and fail-closed (retryable `errStaleSoleAuthEpoch`) for `wire>0`; when reliable, the live `soleAuthEpochStale` check. Boot (run.go after `bootSnapshotAndApplyLoop`) marks ready via `ReadIndex`+`WaitApplied` on group-0 — **NOT `CommittedIndex()`** (raft v2 resets it to the snapshot floor on restart → unreliable backlog measure) — RETRYING through transient leader-loss; stays fail-closed if the node never catches up. DORMANT. **plan-gate codex 4 rounds** (R1 apply-loop-lag→full-window readiness gate; R2 multi-group unsound→single-group scope; R3 CommittedIndex unreliable→ReadIndex + setter nil-panic; R4 CLEAN) + **code-gate codex 2 rounds** (P2 boot goroutine gave up on first error→retry; P3 broad test helpers markReady). Test helpers (`newTestShardService`/`newShardServiceTestWithDataDir`/`newSingleNode1Plus0ChunkCapable`/`newTestDistributedBackendWithDB`/`testbackend.go`) mark ready (single-node = caught up). `TestLeaf_BootWindowFailsClosed` + updated `TestLeaf_DormantAcceptsAll`.
- **[DONE] S4c-d precondition — multi-group soleauth epoch source: clobber fixed (v0.0.631.0).** The boot-window code-gate (codex R2) framed this as "bucket-routed epoch + per-group readiness," but investigation REFUTED that framing: the soleauth epoch is **group-0-global**, not per-owning-group — the flip is always applied by group-0's FSM under group-0's keyspace (`SetBucketSoleAuthorityPropose`→`b.propose` on coordinator base=group-0, bucket.go:393; `applySetBucketSoleAuthority` apply.go:702), the coordinator always reads the epoch from group-0 (`ClusterCoordinator.GetBucketSoleAuthEpoch`→c.base, cluster_coordinator.go:860), and group-0 is the "legacy data raft" (`state.node`). The real bug was a **CLOBBER**: the single shared `ShardService.soleAuthEpochFn` is wired by EVERY backend's `SetShardService` (backend.go:449) as a closure over THAT backend's keyspace — group-0 wires it correctly at boot (boot_phases_storage_runtime.go:365), then a runtime data group (`group_backend.go:125`, groupID "group-N") OVERWRITES it with a closure over its own (epoch-less) keyspace → on a node owning group-0 + a data group the fence reads epoch 0 for all buckets → fail-OPEN (latent, DORMANT — every wire epoch is 0 today). **Fix:** `const soleAuthAuthorityGroupID="group-0"` + `isSoleAuthEpochAuthority()` (groupID=="" legacy/test OR "group-0"); guard ONLY the `svc.SetSoleAuthEpochFn(...)` call in `SetShardService` so a routed data group never clobbers the authority's reader (`SetDEKKeeper`/`SetFenceLock` stay unconditional). Readiness UNCHANGED — the single group-0 `soleAuthEpochReady` is the correct cluster-wide signal; NO per-group readiness needed (the TODOS framing was moot). plan-gate (Claude general-purpose; **codex stalled → gtimeout 124 → fallback**) no P1, [P2] corrected my "group-0 spans every node" rationale (false under stable node IDs; safety actually from the readiness gate) + [P3] test-construction; code-gate codex CLEAN. Tests: `TestSetShardService_DataGroupDoesNotClobberEpochSource` (RED without guard), `TestDistributedBackend_isSoleAuthEpochAuthority`, `TestSetShardService_DataGroupStillWiresService`.
- **[P1][NEW S4c-d HARD precondition] Soleauth epoch availability on non-group-0-member leaf nodes (stable node-ID deployments).** The clobber fix above is correct+complete for group-0-MEMBER leaf nodes. But group-0 spans every node ONLY in the address-as-ID deployment: `addJoinedNodeToLegacyDataRaft` (the sole group-0 voter-growth path, boot_phases_forwarders.go:137) is gated by `if nodeID != addr { return nil }` (:279) — a deployment using STABLE node IDs (`--node-id` ≠ raftAddr) does NOT auto-extend group-0, so a data-group-only node is permanently NOT a group-0 voter and has NO local soleauth epoch state. The readiness gate keeps such a node **fail-CLOSED** for wire>0 (its group-0 `WaitApplied` never completes → never ready), which is SAFE (no fail-open / no data-integrity risk) and DORMANT, but a **liveness** gap once the flip is enabled: epoch-bearing writes routed to that leaf would be rejected. Closing it needs the soleauth epoch readable on every potential leaf (extend group-0 membership on stable-ID join, mirror the epoch into the cluster-wide meta-raft, or forward-resolve the epoch). This is the genuine remaining multi-group concern, distinct from + larger than the clobber. Surfaced by the multi-group-epoch plan-gate (Claude fallback [P2]).
- **[P2][S4c-d co-requirement] Classify the soleauth-fence retryable error over the remote shard RPC.** The leaf returns `errStaleSoleAuthEpoch` (retryable); the local/self path preserves it, but the remote shard RPC serializes `err.Error()` into an `"Error"` reply and the client returns a GENERIC error — so a future retry consumer cannot `errors.Is(..., errStaleSoleAuthEpoch)` on a remote reject. PRE-EXISTING + DORMANT (no retry consumer today). Add a wire status code for the stale/not-ready reject + the retry consumer with the S4c-d flip. Surfaced by the boot-window code-gate (codex R1).
- **[DONE] S4c-a2-B — S3-edge soleauth-epoch stamp + forward-wire field (PUT path).** `ContextWithBucketSoleAuthEpoch` mirroring `ContextWithBucketVersioning`; additive `PutObjectArgs.soleauth_epoch` (+1 wire encoding: 0=absent→local fallback, n=epoch n-1); stamp at the S3 PUT edge (`ctxWithSoleAuthEpoch` + `Operations.GetBucketSoleAuthEpoch`), re-stamp on the forward receiver, and `writeQuorumMeta` prefers the context-stamped epoch (`resolveQuorumMetaEpoch`) over a local read. A forwarded PUT now fences against the originating node's epoch. Dormant. Shipped in the S4c-a2b worktree.
- **[P2][roadmap] S4c delete forward-wire epoch.** The PUT path now carries the originating epoch; soft/hard delete do NOT (mirroring versioning, whose forward stamp is PUT-only). `DeleteObjectReturningMarker` drops ctx (`context.Background()` at object_delete.go:20) and `DeleteObjectVersion` is a `storage` interface method (4+ impls); threading the originating epoch needs those signatures to carry it. Forwarded deletes stay fenced by the OWNER's local epoch (a2-A) — same lag window the PUT path just closed. Close it when the delete-write rework lands (natural fit: S4c-d's force-delete rework), or as its own slice.
- **[DONE] S4c-b owns soleauth-epoch snapshot/restore fidelity.** Restore now carries the epoch verbatim via `SetBucketSoleAuthorityCmd.EpochFloor` (monotonic floor: `newEpoch = max(computed, EpochFloor)`), so a restored `pending`/`on` bucket no longer resets to epoch 0 nor mis-derives for buckets that cycled `pending↔off`. Additive FlatBuffers field (default 0 = backward-compat). Shipped in s4c-b.
- **[P2][roadmap] S4c-a3 — backfill skip + all-versions enumerator** (was listed below; still pending).
- **[P2][roadmap] S4c-a3 — backfill skip + all-versions enumerator.** Disable the per-version backfill walker under `soleauth` on/pending, and add a true all-version blob enumerator (`ScanQuorumMetaVersionsBucketAll` / `ScanQuorumMetaVersionsAll`, distinct from the max-per-key `ScanQuorumMetaVersionsBucket`) plus its peer RPC, fail-closed on un-upgraded peers. Required by the snapshot restore-mode absent-blob purge and by flag-on LIST.
- **[P3][test] No cluster/multi-node test for `CmdSetBucketSoleAuthority` replication or snapshot restore-onto-existing across nodes.** S4c-a1 covers the command, one-way guard, codec round-trip, backend end-to-end, and snapshot persist/restore/abort/terminal-conflict with single-node unit tests. A 2/3-node test (command replicates to followers; restore reconciles divergent live soleauth state) isn't feasible in the solo-leader unit harness — add alongside the S4c-a2/a3 wiring when a multi-node harness exists (same gap class as the PR2 forwarded-propose item above).
