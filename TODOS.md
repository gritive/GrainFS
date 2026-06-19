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

## S4c-a2 follow-ups

- **[P2][roadmap] S4c-a2-B — S3-edge soleauth-epoch stamp + forward-wire field.** `ContextWithBucketSoleAuthEpoch` mirroring `ContextWithBucketVersioning` (cluster_coordinator.go:980); a `forward_codec.go` wire field on the forward PUT/delete args; receive-side context threading. So a forwarded PUT carries the ORIGINATING node's epoch instead of the owner re-reading its own (closes the forwarded-write lag window during cutover). a2-A is authoritative only for the direct replication path.
- **[P2][roadmap] S4c-b owns soleauth-epoch snapshot/restore fidelity.** a2-A deliberately does NOT capture/restore the epoch; a restored `pending`/`on` bucket would reset to epoch 0 (unreachable until S4c-d flips a bucket). S4c-b's restore-mode branch must restore the epoch verbatim (transition-replay derivation mismatches for buckets that cycled `pending↔off`).
- **[P2][roadmap] S4c-a3 — backfill skip + all-versions enumerator** (was listed below; still pending).
- **[P2][roadmap] S4c-a3 — backfill skip + all-versions enumerator.** Disable the per-version backfill walker under `soleauth` on/pending, and add a true all-version blob enumerator (`ScanQuorumMetaVersionsBucketAll` / `ScanQuorumMetaVersionsAll`, distinct from the max-per-key `ScanQuorumMetaVersionsBucket`) plus its peer RPC, fail-closed on un-upgraded peers. Required by the snapshot restore-mode absent-blob purge and by flag-on LIST.
- **[P3][test] No cluster/multi-node test for `CmdSetBucketSoleAuthority` replication or snapshot restore-onto-existing across nodes.** S4c-a1 covers the command, one-way guard, codec round-trip, backend end-to-end, and snapshot persist/restore/abort/terminal-conflict with single-node unit tests. A 2/3-node test (command replicates to followers; restore reconciles divergent live soleauth state) isn't feasible in the solo-leader unit harness — add alongside the S4c-a2/a3 wiring when a multi-node harness exists (same gap class as the PR2 forwarded-propose item above).
