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
    - **S2b PR-B — ListObjects per-version derive (NEXT).** `ScanQuorumMetaVersionsBucket` walker
      (robust `WalkDir`-all-files + group-by-decoded-`cmd.Key`, since keys contain `/`; O(total versions))
      + `ScanQuorumMetaVersions` RPC + `listObjectsPerVersion` (all-groups fan-out, max-vid, marker-exclude)
      + branch the three object_list.go methods on versioning-Enabled, using `objectFromCmd`. **Do NOT
      touch `scatterGatherList`/`ScanQuorumMetaBucket`** (`ScanObjectMetaEntries` + 3 legacy consumers stay
      latest-only). Closes the S2a→S2b HEAD-vs-LIST window. Benchmark before S4 (O(total versions) per LIST).
      Also: per-version orphan reconciliation (scrubber removes blobs whose FSM record is gone — separate
      slice). ListObjectVersions stays
    FSM-backed (deferrable to S4).
  - **S3 — migrate existing data** (legacy latest-only blob + FSM per-version records → per-version blobs).
  - **S4 — cutover: per-version sole authority; remove FSM object-meta path** (`CmdPutObjectMeta`/`apply*`/
    `obj:`/`lat:`), repoint scrubber/snapshot. **Appendable/coalesced carve-out:** those objects bypass
    quorum-meta (append.go:158-166; `PutObjectMetaCmd` lacks `IsAppendable`/`Coalesced`) → stay
    FSM-authoritative, so the FSM object path is retained ONLY for them (confirm before S4). raft out of
    PUT/GET for non-appendable objects.
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
