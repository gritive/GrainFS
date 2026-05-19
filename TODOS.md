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

- [ ] **Auth redesign DX follow-ups** (from `docs/superpowers/specs/2026-05-19-auth-redesign.md`
  Codex review, medium+cosmetic tier). All single-PR-sized, ship after the main
  redesign lands:
    - [ ] `grainfs iam explain --sa X --s3 put s3://bucket/key` — request-shaped
      simulator that maps a natural sentence onto the underlying
      `(action, resource, condition)` tuple. Wraps `grainfs iam policy simulate`.
    - [ ] `grainfs iceberg secret duckdb --warehouse X --sa Y` and `grainfs
      iceberg catalog spark|trino --warehouse X --sa Y` — emit copy-paste config
      blocks per client. Reduces per-warehouse-token UX tax.
    - [ ] `grainfs iam jwt-key rotate --auto-prune-after-ttl` — scheduler-friendly
      one-shot rotation that runs prune after the TTL window elapses. Optional
      systemd timer / k8s CronJob example in operator docs.
    - [ ] `grainfs doctor auth --warehouse X --sa Y` — health check that walks
      the entire auth path (TLS reachability, socket access, SA exists, policies
      attached, JWT key state, bucket exists, client URL base sane) and prints
      the first broken link.
    - [ ] `docs/users/minio-muscle-memory-map.md` — `mc admin policy info` →
      `grainfs iam policy get`, `mc admin user add` → `grainfs iam sa create`,
      `aws s3api put-bucket-policy` → `grainfs iam bucket policy put` (admin
      UDS), `mc console` → (no equivalent yet). Per-row "why different" column.
    - [ ] Policy template library at `docs/users/iam-policy-templates/`:
      `readonly-bucket.json`, `readwrite-bucket.json`, `write-prefix.json`,
      `read-prefix-from-cidr.json`, `iceberg-readwrite-warehouse.json`. Each
      template is a starting JSON with comments + a sed-friendly bucket name
      placeholder. Wired into `grainfs iam policy create --from-template <name>
      --bucket <b>`.
    - [ ] Adjust spec/docs language: "AWS IAM JSON subset" everywhere instead of
      "AWS-IAM-compatible." Banner block in `docs/users/iam-policy-from-aws.md`
      enumerating the unsupported constructs (`NotAction`, policy variables,
      most condition keys, inline policies, account principals).
    - [ ] Built-in policy attach warning surface (covered in spec, but TODO is
      the docs sentence — explain in `docs/users/iam-policy-from-aws.md` why
      `readwrite` is "global-readwrite" semantically and link to the per-bucket
      template.
    - [ ] `grainfs doctor snapshot` — preflight check that operators run on an
      old binary before installing a new one. Walks the meta-FSM snapshot,
      reports schema version + compatibility with a target binary version.
      Mitigates the "fail-loud cluster won't start" surprise for private
      cluster evaluators.
    - [ ] `docs/operators/containerization.md` — exact chmod/chown/socket-dir
      behavior for admin UDS under rootless containers, systemd units, mounted
      sockets, sidecars, CI users. Failure messages spelled out so operators
      can grep them.
    - [ ] Error-message SA-ID exposure decision — keep current "SA <id> lacks
      ..." or replace external surface with `access_key_suffix` (last 4 chars),
      moving full SA id to audit log only. Needs explicit ADR.
    - [ ] External Iceberg client first-tier-only docs — keep warp + DuckDB as
      tested examples; demote Trino/Spark/PyIceberg/Flink to "configuration
      pattern, not e2e verified" until each gets its own e2e cell. Avoids stale
      docs claiming compatibility we haven't measured.
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
- [ ] **Cluster local pre-check audit (post-SetBucketVersioning fix)**:
  `DistributedBackend`에 `b.HeadBucket`/`b.HeadObject` 후 `b.propose`
  패턴이 15곳 남아 있다 (`SetObjectACL`, multipart paths, etc.).
  follower가 meta-Raft bucket assignment를 받았지만 data-Raft
  `CmdCreateBucket`을 아직 apply하지 않은 짧은 윈도우에서
  같은 false-`NoSuchBucket`을 반환한다.
  `ClusterCoordinator` 단에 cluster-aware pre-check + base에
  `*ProposeOnly` entrypoint 패턴을 일관 적용. 참조 커밋:
  `fix(s3auth/cluster): warp versioned workload passes on a 4-node
  cluster` (`benchmark` branch).
- [ ] **Capability evidence propagation ready probe**: 4-node
  cluster bootstrap 후 multipart capability evidence가 gossip을
  통해 propagate되는 데 약 30-45s가 걸린다 (`CLUSTER_WARMUP_SLEEP`
  default 5s는 부족). 운영/벤치마크 자동화를 위해 (a) gate가
  capability evidence ready인지 polling endpoint 제공, (b) gossip
  interval 조정, (c) capability evidence를 raft commit으로
  immediate propagate. 임시 우회: `CLUSTER_WARMUP_SLEEP=45`.

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
- [ ] **Iceberg `/v1/config` secret over plaintext HTTP**: the endpoint
  publishes the caller's `s3.secret-access-key` in the response JSON. SigV4
  protects request integrity but not response confidentiality, so a
  caller hitting the catalog over HTTP exposes their secret in cleartext
  on the wire. Post-Option-B (caller-identity) the blast radius is the
  caller's own SA, not an org-wide RoleAdmin SA — but a long-lived key
  shared across roles still leaks. Reopen when TLS terminates upstream of
  the catalog and operators want a defense-in-depth gate, OR when a
  caller running HTTP catalog in prod is observed. Options when
  reopened: (A) `if !c.IsTLS() { return empty overrides }` — fails closed
  back to 1832×403 if the operator forgot TLS, (B) docs-only flag in
  `docs/users/audit-iceberg.md`, (C) require an explicit
  `--iceberg-allow-http-creds` opt-in for dev.
- [ ] **Iceberg Spark/Trino/PyIceberg client coverage**: promote only after
  real-client REST Catalog smoke tests define which client behaviors are
  supported versus DuckDB-only compatibility.
- [ ] **Volume CLI follow-ups**: export/import, policy, attach/detach, and rename
  need concrete server-side lifecycle requirements.
- [ ] **Scrub scope, EC scrub race, group dir cleanup, PeerHealth threshold**:
  reopen when their telemetry triggers fire.
- [ ] **Incident store scope index / `ScanObjects(bucket, keyPrefix)`**: reopen
  when measured margins fail or a concrete caller needs prefix scope.
- [ ] **NFSv4 / NBD auth integration with SA model**: reopen after the
  S3+Iceberg IAM redesign (admin-UDS-only bucket lifecycle, 2-tier
  None/Read/Write, OAuth2 bearer for Iceberg) lands. Today NFSv4 relies on
  AUTH_SYS / RPCSEC_GSS and NBD on TLS X.509 / LAN trust. Decide how (or
  whether) mount-time credentials map onto SA + per-bucket Grant, and how
  NFS uid/gid is reconciled with `(SA, Bucket, Role)`. Out of scope for the
  current IAM grilling.

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

## AppendObject Follow-Ups

- [ ] **Forward buffer 512 MiB warp calibration [P1]**: production traffic
  pattern에 default `--cluster-append-forward-buffer-total-bytes` 512 MiB가
  적정한지 검증. `warp append --concurrent 32 --duration 60s --obj.size '1-16MiB'`
  실행, `grainfs_cluster_append_forward_buffer_rejected_total` ratio < 1%
  확인. 충족 못 하면 default 상향 또는 memory-budget tuning을 separate PR로
  분리. PR description에 measurement result 또는 미실행 사유 명시.

- [ ] **EC shard orphan cleanup [P2]**: PR #425 (raw segment side)에서 미해결.
  coalesce 도중 EC 쓰기 후 propose 실패 시 남는 EC shard dir
  (`<shardRoot>/<bucket>/<userKey>/coalesced/<id>/coalesced/<id>/shard_<i>`)
  cleanup. 기존 `OrphanWalkable.WalkOrphanShards` 인터페이스는 plain EC만
  cover — `ScanObjects`가 `lat:` 인덱스로 coalesced shardKey를 yield하지 않아
  known set 구축이 안 됨. Storage layout (shardRoot vs data root) + coalesced
  shard tracking mechanism 조사 → `OrphanWalkable` 확장 또는 별도 sweep
  메커니즘 도입.

- [ ] **Coalesce recoalesce depth audit [P2]**: design open question — 새
  raw segment가 다시 threshold 도달 시 또 coalesce하면 `coalesced[]`에 entry가
  계속 누적된다. `MaxCoalescedEntries=1024` cap 외에 measurement-driven 정책
  (max depth, periodic 통합) 검토. v0.0.253.0 Hardening이 추가한
  `AppendCoalescedEntriesAtCap` counter로 production cap-reach 빈도 baseline
  수집 후 판단.

- [ ] **`AwaitWriteFromNonOwner` harness EC-aware 강화 [P2]**: 현재
  AwaitWriteFromNonOwner는 healthy-cluster path 전용 — EC stripe width ==
  cluster size 환경에서 owner kill 후 호출하면 모든 PUT이 ServiceUnavailable.
  PR #424 T24 OwnerKillSurvives는 직접 `/api/cluster/status` `leader_id`
  폴링으로 우회. EC degraded write을 인식하는 helper 또는 별도 RotationSettled
  helper 분리 검토.

## Pre-existing Test Failures (Phase B3 무관)

- [ ] **`TestBlobStoreAppendNoCompressKeepsAllocationBound` race-mode fail [P2]**:
  baseline에서도 동일하게 fail (allocations=4 vs ≤1 expected). `-race` 빌드에서
  추가 alloc churn. packblob 패키지 별도 작업.

- [ ] **`make test-e2e` 9 unrelated failures [P1]**: backup_restic, multipart
  ("capability multipart_listing_v1 rejected"), cluster_incident,
  quarantine_incident, dynamic_join_services, cluster_scrubber, no_peers,
  encryption_at_rest, iam_scoped_key. AppendObject 경로 무관. multipart는
  capability evidence propagation TODO와 동일 root (cluster bootstrap timing).
  단독 실행 시 PASS인 케이스 vs 환경 의존 케이스 분리 필요.

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
