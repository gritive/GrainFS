# TODO

## Follow-ups

### Data-group Raft cleanup follow-ups (CmdSetRing retirement, 2026-06-26)


### ShardService/DistributedBackend decomposition follow-ups (2026-06-25, PR1 LocalShardStore + Card1 QuorumMetaStore done)

The `ShardService` (1,940 LOC) and `DistributedBackend` (54-field/300-method) god-structs are being
decomposed into facades over deep modules. **Done:** PR1 = LocalShardStore (MERGED #899; shard-blob
I/O + durability + seal + staging). Card1 = `QuorumMetaStore` (quorum-meta orchestration: fan-out
write, LWW read merge, version resolution, scatter-gather list — 21 methods carved out of
DistributedBackend behind a `qms` facade, injecting `localQuorumMetaStore`/`quorumMetaPeerRPC`/
`ShardGroupSource`/`versioningSource` adapters `*ShardService` satisfies today; conflict-resolution
kept as package-level pure functions; raft-free). Both behavior-preserving. Remaining slices (each a
separate PR, facades stay the spine — see design
`docs/superpowers/specs/2026-06-25-shard-service-decomposition-design.md`):

- **[RESOLVED] decideQuorumMetaWrite single-ownership → stays package-level pure functions.** The
  Card1 grilling + advisor cut-test confirmed `latestWins`/`quorumMetaBlobWins`/`quorumMetaCmdWins`/
  `decideQuorumMetaWrite` are already pure, co-located, zero-dep testable; 23 call sites = leverage,
  not scatter; a hypothetical `LWWResolver` module fails the deletion test. No module — shared by the
  local write-accept and the orchestration merge as free functions.

### DeleteBucket non-Enabled emptiness follow-ups (2026-06-24)


### Bucket-delete config cascade follow-ups (PR: fix-bucket-delete-config-cascade)

- **[P3][won't-fix] Mixed `--lifecycle-interval` clusters are unsupported.** The lifecycle-delete
  cascade is gated per-node on `LifecycleInterval > 0`, which is **load-bearing**, not cosmetic: the
  lifecycle store itself is only wired when `interval > 0` (`boot_phases_srvopts.go`), and
  `applyBucketLifecycleDelete` errors if the store is nil (`meta_fsm_lifecycle.go`). So a deliberately
  mixed cluster (some nodes `0`, some `>0`) is operator misconfiguration, not a latent bug. Uniform
  deployments unaffected.

### group-0 control-plane demotion follow-ups (2026-06-24, epic DONE)

The demotion shipped: bucket existence/policy/versioning consolidated onto meta-raft as a unified
`BucketRecord`; group-0 carries no bucket control-plane state; create/delete atomicity gaps closed;
bucket writes forward to the meta leader; policy invalidation lossless; versioning barrier best-effort.
Deferred items:

- **[P3][minor] `CreateBucket` now unconditionally requires a non-empty groupID** (the FSM rejects
  `""`). Safe in production (the router is always wired so placement resolves a real group), but it
  tightens behavior for any future router-less `DistributedBackend` wiring. Note only.

### Multipart off-raft (M1-M5) follow-ups (2026-06-23)

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


### Data-plane raft-free Slice 2 follow-ups (2026-06-24)

### Append/coalesce off-raft follow-ups (Slice 1, 2026-06-24)


- **[P2][follow-up] LocalBackend `Coalesced` metadata is not persisted in `storagepb.Object`.**
  While tightening append side-summary cap checks, a local post-coalesce append regression test could
  not be made valid because `storage.Object.Coalesced` is not encoded by `internal/storage/codec.go`.
  Cluster quorum-meta has `CoalescedShardRef` coverage; local storage needs a storage FlatBuffer
  format slice (`storagepb.Object` coalesced vector + codec tests) before local post-coalesce
  append/read behavior can be locked down.

- **[P3][known-tradeoff] Coalesced orphans in a bucket switched to versioning-Enabled are not
  reclaimed.** `hasLiveCoalescedRef` gates on `blobAuthReadOn` and fails closed (keep) for Enabled
  buckets, so coalesced orphans created during a bucket's prior Unversioned/Suspended life leak after
  it is Enabled. Safe (no data loss), bounded residual; documented deliberate tradeoff.

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

- **[P4][note, not a defect] staticcheck also flags 3 PRODUCTION U1000 that are INTENTIONAL
  `//nolint:unused` scaffolding** — `(*MetaFSM).incDEKRef` / `decDEKRef` (`meta_fsm_rotation.go`, kept
  wired for the S7 DEK-prune-safety predicate) and `metrics.bucketStates` (`operator_state.go`,
  operator-state scaffolding v0.0.388-389). Standalone staticcheck does not honor golangci's `//nolint`
  directive, so it reports them; `make lint` correctly skips them. If standalone staticcheck is ever
  wired into CI, switch these to `//lint:ignore U1000 <reason>` for parity, or accept the noise. No
  code change otherwise — recorded so the next /health run does not misread these as a regression.

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
- **Versioned tag/ACL RMW holds the meta-RMW lock across a raft propose** — moot: versioned
  `writeQuorumMeta` is raft-free for versioning-enabled buckets.

## Completed

- **[DONE] Reseal machinery retired after proving node-local FSM-value rewrap compatibility.**
  **Completed:** v0.0.754.0 (2026-06-26)
- **[DONE] GC singleton/freshness raft-free replacement complete.**
  **Completed:** v0.0.754.0 (2026-06-26)
- **[DONE] HRW owner routing for append/multipart writes and stale quorum membership migration cleanup.**
  **Completed:** v0.0.754.0 (2026-06-26)
- **[DONE] DeleteBucket per-version blob cleanup.**
  **Completed:** v0.0.754.0 (2026-06-26)
- **[DONE] Follower-stale bucket-versioning and bucket-policy authz cache fixes shipped.**
  **Completed:** v0.0.754.0 (2026-06-26)
- **[DONE] AppendObject versioned-bucket read gate linearization and SetBucketVersioning cache updates.**
  **Completed:** v0.0.754.0 (2026-06-26)
- **[DONE] Throughput TODO and short-TTL edge-cache validation for versioned writes.**
  **Completed:** v0.0.754.0 (2026-06-26)
- **[DONE] Slice 2 and per-object FSM cleanup work.**
  **Completed:** v0.0.754.0 (2026-06-26)
- **[DONE] deleteShardsQuorum empty-placement guard and stale comment cleanup.**
  **Completed:** v0.0.754.0 (2026-06-26)
- **[DONE] Staticcheck U1000 removal in cluster/storage tests.**
  **Completed:** v0.0.754.0 (2026-06-26)
