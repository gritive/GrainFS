# TODOs

## Current Priority Lens: Operator Trust

Design principle: safe defaults, explicit escape hatches, observable operations.

Operators should see recovery, drain, rebalance, and upgrade state without
guessing from logs. Keep measured deferred work in this file. Promote an item to
active work only after reading the code/API surface and writing a specific
engineering plan.

Planning reference: operator trust roadmap note from 2026-05-15.

## Now

Work these in order. Do not run them in parallel.

1. [x] **BadgerDB pre-server recovery journal**
   - Trust risk: metadata recovery becomes invisible if `GrainFS` fails before the
     incident store opens.
   - Signal: durable recovery manifest/journal under `<data>/.recovery/`.
   - Verification: pre-server crash/restart test, fsync/rename atomicity test,
     restart-readable journal contract.

2. [ ] **BadgerDB atomic auto-recovery design**
   - Trust risk: recoverable Badger state still requires manual intervention
     during an outage.
   - Signal: recovery plan, applied action, skipped action, and reason.
   - Verification: corrupted/partial Badger replay plus snapshot restore tests.
   - Boundary: design next; implement after recovery journal decision structs
     stabilize.

3. [ ] **Rolling upgrade remaining safety slices**
   - Trust risk: mixed-version clusters can hide schema, config, or capability
     divergence behind a healthy surface.
   - Signal: compatibility gates, upgrade status, snapshot-config compatibility
     status, warning/safe rejection/degraded state.
   - Verification: `tests/compat/`, mixed-cluster tests, snapshot-config forward
     policy tests.

4. [ ] **Object placement/index orphan and stale reconcile**
   - Trust risk: object-level placement creates a dual-write failure mode between
     data groups and the global object index.
   - Signal: reconcile report, repair/quarantine counters, non-silent failure.
   - Verification: index commit failure injection, orphan data, stale index,
     missing shard, corrupt shard tests.

5. [ ] **Cluster health data-group Raft progress**
   - Trust risk: metaRaft can look healthy while a data group is leaderless or
     lagging.
   - Signal: per-group leader, term, lag, drain state, and transfer state.
   - Verification: induced lag, leader transfer, multi-group cluster health e2e.

6. [ ] **NFSv4 and 9P auth/access-control correctness**
   - Promote only when `GrainFS` must support untrusted networks, multi-tenancy,
     compliance-driven file access audit, or S3 IAM parity for file protocols.
   - Signal: export-level policy, auth mode, denied access events.
   - Verification: cross-protocol auth tests and reserved `__meta/` namespace
     tests.

## Next

- [ ] **NFS `rdattr_error` required gap**: implement READDIR per-entry
  attribute error semantics.
- [ ] **pynfs/nfstest conformance matrix**: publish nightly/basic suite results
  as an operator-readable pass/fail matrix.
- [ ] **`cluster remove-peer` negative liveness signal**: design dead-peer
  detection and display policy.
- [ ] **Hot reload drift detection**: detect disk/runtime config mismatch after
  config reload.
- [ ] **Migration mirror/cutover correctness**: settle mirror, cutover, and
  status semantics before dashboard polish.
- [ ] **Bucket/object-lock/retention governance design**: promote only after the
  semantic mapping is clear for version-level retention records,
  governance/compliance delete and overwrite behavior, legal hold permissions,
  lifecycle expiration interaction, and NFS retention attribute alignment.
- [ ] **Scrub dedup 영구성 정책**: 현재 `scrubber.Director.dedup`은 영구.
  운영 의도 확인 후 done/cancel 시 cleanup 명령 추가 (ADR 또는
  grill-with-docs). 참조: `docs/architecture/scrubber-director-actor.md`
  결정 5.
- [ ] **공통 JobActor 추상화 검토**: `scrubber`/`lifecycle`/`migration`
  worker가 공유 가능한 lifecycle 패턴. design doc의 JobActor 컨셉 참조.
- [ ] **Scrub Register boot phase constructor 옵션화**: `BlockSource`/`Verifier`를
  `NewDirector` 옵션으로 주입해 `Register`/`started.Bool` 가드 제거.
  boot phase 의존 그래프 재배치 동반.

## Deferred Until Triggered

- [ ] **S3 Range GET residual p95/p99**: reopen when product/SLO requires p95
  below 25 ms and p99 below 35 ms, or when FUSE/s3fs/goofys random reads
  reproduce EC read amplification.
- [ ] **S3 Range GET 1 MiB full-width bottleneck**: reopen only with new pprof
  evidence for shard-side encrypted range decrypt or QUIC syscall cost.
- [ ] **NBD direct-write bottleneck**: reopen when direct I/O write SLOs matter
  or trace-off profiles confirm per-block `open/pwrite` as the p95 bottleneck.
- [ ] **NBD qemu/libnbd interop**: promote `OPT_INFO`/`OPT_GO`,
  `NBD_INFO_BLOCK_SIZE`, structured replies, block status, and extended headers
  only after a disposable qemu/libnbd smoke harness runs against a real GrainFS
  NBD port.
- [ ] **Iceberg REST high-concurrency Raft ceiling**: reopen when production
  catalog workloads miss SLOs or the consistency spec for reducing proposals is
  clear.
- [ ] **Iceberg Spark/Trino/PyIceberg client coverage**: promote only after
  real-client REST Catalog smoke tests define which client behaviors are
  supported versus DuckDB-only compatibility.
- [ ] **Volume CLI follow-ups**: export/import, policy, attach/detach, and rename
  need concrete server-side lifecycle requirements.
- [ ] **Scrub scope, EC scrub race, group dir cleanup, PeerHealth threshold**:
  reopen when their telemetry triggers fire.
- [ ] **Incident store scope index / `ScanObjects(bucket, keyPrefix)`**: reopen
  when measured margins fail or a concrete caller needs prefix scope.

## NFSv4 RFC 8881 Follow-Ups

### Required

- [ ] [nfs-audit] bit 11 `rdattr_error` [P0] [Skipped]: READDIR does not emit
  per-entry attribute errors; operation-level errors only. Owner: TBD.

### Recommended

- [ ] [nfs-audit] bit 12 `acl` [P2] [Skipped]: implement NFS ACL payloads if a
  policy requires them. Owner: TBD.
- [ ] [nfs-audit] bits 16/17 case flags [P2] [Skipped]: return
  case-sensitive/case-preserving booleans. Owner: TBD.
- [ ] [nfs-audit] bits 21/22/23 inode-style file counts [P2] [Skipped]: add only
  after defining synthetic capacity policy. Owner: TBD.
- [ ] [nfs-audit] bit 24 `fs_locations` [P2] [Skipped]: referrals and
  `NFS4ERR_MOVED` need a design. Owner: TBD.
- [ ] [nfs-audit] bits 36/37 owner/group [P1] [Partial]: design idmap or
  authenticated user mapping. Owner: TBD.
- [ ] [nfs-audit] bits 38/39/40 quota [P2] [Skipped]: design NFS quota
  accounting before implementation. Owner: TBD.
- [ ] [nfs-audit] bits 42/43/44 filesystem capacity [P2] [Skipped]: select a
  capacity source before surfacing values. Owner: TBD.
- [ ] [nfs-audit] bit 48 `time_access_set` [P1] [Partial]: persist atime in the
  sidecar schema. Owner: TBD.
- [ ] [nfs-audit] bits 56/57 directory notifications [P2] [Skipped]: design
  notification behavior before implementation. Owner: TBD.
- [ ] [nfs-audit] bits 58/59 DACL/SACL [P2] [Skipped]: requires the ACL/audit
  model. Owner: TBD.
- [ ] [nfs-audit] bits 62-68 pNFS [P2] [Skipped]: out of scope unless `GrainFS`
  adds pNFS support. Owner: TBD.
- [ ] [nfs-audit] bits 69-73 retention metadata [P2] [Skipped]: map only after
  object-lock semantics are defined. Owner: TBD.
- [ ] [nfs-audit] bit 74 masked mode SETATTR [P2] [Skipped]: implement if
  client workloads require it. Owner: TBD.
- [ ] [nfs-audit] bit 76 charset capability flags [P2] [Skipped]: add after UTF-8
  policy is explicit. Owner: TBD.

## Conformance Follow-Ups

- [ ] [nfs-conformance] pynfs-nightly [P1]: run pynfs basic suite on a scheduled
  Linux/Colima host and review `results/summary.json`.
- [ ] [nfs-conformance] nfstest-runner [P2]: add nfstest after pynfs stabilizes.

## Storage And Volume Backlog

- [ ] **Thin pool quota**: cross-volume physical capacity pool after Phase A
  volume options.
- [ ] **Multi-tenancy**: account/namespace isolation above IAM.
- [ ] **Quota**: capacity limits per service account or team.
- [ ] **Volume export/import**: backup/restore for volume data and snapshot
  chains.
- [ ] **Volume policy**: per-volume pool quota, dedup, encryption key, and EC
  profile overrides.
- [ ] **Volume attach/detach**: runtime NBD exposure toggles.
- [ ] **Volume rename**: migrate live-map and snapshot key prefixes.
- [ ] **`__grainfs_volumes` EC policy**: revisit forced replication for volume
  blocks after EC scrub and migration strategy are ready.

## Migration Backlog

- [ ] NFS virtual overlay.
- [ ] NBD block proxying.
- [ ] Bucket-level server-side injection:
  - Phase 1 credentials shipped in v0.0.123.0.
  - Phase 2 import hook shipped in v0.0.171.0.
  - Remaining work: mirror mode, cutover verb, progress tracking, upstream
    `List`/`Head`/`CopyObject`, CLI integration, cancellation on leadership loss,
    and counter continuity after leader flip.

## Parked

- [ ] Redis protocol.
- [ ] TSDB.
- [ ] AppendObject API.
- [ ] 9P/NFS shared write-back layer.
- [ ] Blame Mode v2.
- [ ] PagerDuty native webhook mapping.
- [ ] go-billy Direct File I/O / O_DIRECT.
- [ ] Hot/cold auto tiering.
- [ ] I/O-based auto rebalancing.
- [ ] io_uring.
- [ ] SPDK.
- [ ] SIMD.
- [ ] SoA layout.
- [ ] Control-plane/data-plane split.
