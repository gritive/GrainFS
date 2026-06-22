# TODO

## Follow-ups

### [EPIC] Remove mount protocols (NFS + 9P) + NBD dead sweep — keep S3 + Iceberg only

Decision (2026-06-22): retire every non-S3 access protocol, mirroring the volume/NBD removal epic
(#781–#785). GrainFS continues toward a pure S3 + Iceberg system; NFS and 9P are the last
self-implemented protocol servers and a meaningful complexity/attack-surface sink. `internal/protocred`
survives (S3/Iceberg use it, stripped of mount-protocol enum values); `internal/iam/mountsastore` and
the `MountSA*` meta-Raft commands die once **both** NFS and 9P are gone. NBD is already deleted; only
dead references remain. Three dependency-ordered, independently-shippable slices (one PR each):

- **Slice A — 9P removal — MERGED (PR #827, v0.0.636.0, master).** Deleted `internal/p9server`, 9P serve
  flags + config/`adminapi.P9` status, `protocred.Protocol9P`, `9PAttachOnly`/`grainfs:9PAttach`/`"9p"`
  policy, 9P tests + Makefile/bench. ~6.2k LOC.
- **Slice B — NFS removal — DONE (this PR, branch `remove-nfs`, v0.0.637.0).** Deleted
  `internal/nfs4server`/`nfsadmin`/`nfsexport`, the NFS-export FSM + `NfsExport*` FlatBuffers (make fbs),
  the node-services subsystem (NFS was its last consumer; lifecycle/shutdown boot fns rescued to
  `boot_phases_lifecycle.go`; lifecycle FSM handlers rescued to `meta_fsm_lifecycle.go`), the `grainfs
  nfs` CLI + admin NFS-export API + bucket-delete NFS cascade (#826 config cascade preserved), the
  `/admin` storage-protocols endpoint, `protocred.ProtocolNFS`, serve flags `--nfs4-port`/`--nfs-write-
  buffer-*`, NFS tests/colima/Makefile/bench, NFS docs. ~20.5k LOC removed. **Deferred to Slice C** (kept
  dormant): the `NfsExportCreate` compat capability/operation (used as a generic example capability by
  gossip/meta_raft/iceberg tests — removing it needs test migration). Mount-SA layer + S3/Iceberg untouched.
- **Slice C — DONE (branch `remove-mount-sa-slice-c`, v0.0.638.0, 6 task commits + release, PR pending).**
  Removed `internal/iam/mountsastore`, the `MountSA*` meta-Raft commands + snapshot fields + `handlers_mountsa`
  + `PrincipalTypeMount` (collapsed `PrincipalType` to single S3 value) + `cross_namespace` mountActions +
  `NFSMountOnly`/`grainfs:NFSMount` + the policyattach MountSA layer; the dormant `NfsExportCreate` compat
  capability/operation (migrated the generic gate tests onto the surviving `CapabilityMigrationCutoverV1`,
  same ScopeMetaRaft+SeverityHard — chosen over MultipartListing for semantic match); the NBD dead refs
  (`DomainNBD` AAD constant [no-renumber], `"nbd"`/`"nbd/volume"`/`"volume"`/`"nfs"`/`"nfs/bucket"` protocol-cred
  policy strings, the IAM-admin `mount-sa` resource grammar, `FDCategoryNFSSession` + its `resourceguard`
  consumer, a dead Web-UI NFS section, comments, operator-doc route refs). `make fbs` regen twice (no-renumber).
  Plan gate (codex + completeness-critic, 10 findings incl. 1 ordering BLOCKER) + 6 task reviews + opus
  whole-branch gate (✅ ready-to-merge). The MOUNT-PROTOCOL REMOVAL EPIC IS COMPLETE (A+B+C all shipped/pending).

### [follow-up] Slice C deferred cleanups (low priority, out of slice scope)

- **[P3] Dead `adminapi.VolumeInfo` type** — leftover from the volume-removal epic (#781–#785), referenced
  only by `internal/adminapi/storage_types_test.go`. Not part of the mount-protocol removal; remove in a
  future adminapi cleanup (good companion to the `volumeadmin` rename below).
- **[P3] `internal/protocred/types.go` `validResource` accepts a `volume/` resource prefix** — pre-existing
  latent grammar inconsistency (predates the volume epic, ~PR #579): the protocred validator is more
  permissive than the policy ARN grammar (`validProtocolCredentialResource`, which now only allows
  s3/iceberg). Benign (`validProtocol` only permits s3/iceberg), but worth aligning in a protocred cleanup.
- **[P3] Optional: drop the now-single-valued `policy.PrincipalType` enum entirely** — Slice C collapsed it
  to `PrincipalTypeS3` only; `cacheKey`'s `ptype` param is now vestigial. Removing the type + param ripples
  through `RequestContext`/`Effective` callers for zero behavior change — defer unless a future change needs it.
- **[P3] Optional: dedicated upstream-route authz test** — Slice C T2 deleted MountSA route tests that also
  asserted upstream-route bearer-deny/fail-closed; the middleware mechanism stays covered by surviving IAM
  read/mutation/group tests, but the upstream-route-specific wiring assertion was lost (low value to restore).

### [cleanup] Rename `internal/volumeadmin` → admin-CLI client (stale name from the volume removal epic)

`internal/volumeadmin` is NOT dead — despite the name it is the shared admin HTTP client used by every
`grainfs` admin CLI command (iam/cluster/credential/scrub) plus the live S3 EC bucket-scrub session
client (`grainfs scrub`). The volume removal epic (#781–#785) repurposed it without renaming. Behavior-
neutral rename to e.g. `internal/admincli` (and `adminapi.ScrubVolumeResp`→`ScrubResp`, `ScrubVolumeReq`
etc.). Separate PR — unrelated to the mount-protocol removal. Decided 2026-06-22 (user).

Slice B surface to remove (map precisely before cutting):
- `internal/nfs4server` — the self-implemented NFSv4 server (XDR/RPC), the `:2049` listener.
- `internal/nfsexport` — the export registry store (meta-Raft, per-record `Generation`, fsid allocator).
- Meta-Raft NFS-export commands + apply handlers (`MetaCmdTypeNfsExport*`, `applyNfsExportUpsert`/delete
  in `internal/cluster/meta_fsm_exports.go`; `SetExportStore`/`SetExportFsidMajor`).
- Admin surface: `admin.Deps.NfsExports`, `admin.NfsExportServiceAdapter`, NFS export admin handlers
  (`internal/server/admin/handlers_nfs*.go`), and the **NFS cascade inside `AdminDeleteBucket`**
  (`MarkBucketDeleteCleanup`, `cascadeNfsExportAfterBucketDelete`/`…AfterMissingBucket`,
  `clearNfsExportBucketDeleteCleanupAfterError`, `handlers_bucket_nfs_cleanup.go`). Removing this also
  simplifies the bucket-delete path the config cascade now lives in.
- Boot wiring (`state.nfsExportSvc`, the NFS server start), CLI commands, `tests/nfs4_colima/`, the
  `make test-nfs4-colima` target, any Web UI NFS tab, and the NFSv4 references in `CLAUDE.md`/README.
- Removing NFS also moots the cross-Raft delete→recreate race for the NFS-export cascade (one fewer
  meta-Raft cascade) — see the bucket-delete follow-up below.
- Lesson from volume/NBD removal: split producer (mechanical) vs consumer (delicate read/cascade-plane)
  PRs; run a code-gate for Go-tooling blind spots (dead build-tag flags, dead Web UI tabs); confirm no
  legacy data/exports must be preserved before cutting the read/cascade fallbacks.

### Bucket-delete config cascade follow-ups (PR: fix-bucket-delete-config-cascade)

- **[P2][deferred] delete→recreate concurrency race in the bucket-delete config cascade.** The cascade
  (`cascadeBucketConfigAfterDelete`, internal/server/admin/handlers_bucket_config_cleanup.go) deletes the
  lifecycle config (`lifecycle:{bucket}`, meta-Raft) and IAM bucket-upstream (meta-Raft) keyed by bucket
  NAME, synchronously after the data-Raft bucket delete. A different client that recreates the same-name
  bucket AND writes fresh config inside the sub-millisecond window between the data-Raft delete and the
  cascade propose would have that fresh config wiped. Window is tiny (the recreate needs 2 raft
  round-trips to beat the cascade's single propose), and the same race already exists in the (being
  removed) NFS-export cascade. Strictly better than the prior status quo (unconditional config leak).
  Deferred by decision (2026-06-22) — ship the cascade now, fence later. **Recommended fix when taken:**
  per-record generation + CAS-on-delete (size S, no cross-Raft coupling): each config put stamps
  `gen=prev+1`; the admin handler captures the observed gen before deleting; the cascade-delete carries
  it; apply deletes iff `stored.gen == observed`. A true bucket-incarnation token is L/EPIC (no reusable
  bucket identity exists — the data-Raft bucket record is literally `{}`; data-Raft and meta-Raft cannot
  read each other), so generation-CAS dominates it.
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

### Quick wins (Run 2 candidate)

- **[P3] mpudone GC thundering-herd → leader-only proposing.** `SweepStaleMultipartDoneMarkers`
  (multipart_done_sweep.go) runs on every node; in an N-node cluster each node scans its local `mpudone:`
  and proposes the same stale batch each scrub cycle (N redundant raft entries/cycle). Apply is
  idempotent so it's correct, but a leader-only gate (or dedup) would cut the redundant proposes. Low
  priority given 24h-aged, ≤256-batch, O(minutes) cycle.
- **[P3] Unbounded per-object lock-map growth.** `objectMetaRMWLocks` (per-`(bucket,key)`),
  `quorumMetaTargetLocks` (per blob-target-path), and `shardLocks` grow one entry per unique object seen
  and are never evicted. Mirrors the existing accepted `shardLocks` pattern; a long-running node with
  millions of objects accumulates lock entries. Consider a bounded/striped lock pool or LRU eviction if
  it ever shows up in heap profiles.

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
