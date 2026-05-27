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

- [ ] **Static bulk-data encryption key: rotation + nonce-exhaustion lifecycle (design)**
   - Trust risk: ALL bulk data (objects, EC shards, data-WAL, badger values) is
     encrypted with one static `<dataDir>/encryption.key` via `encrypt.Encryptor`
     using RANDOM 96-bit nonces (`encrypt.go` `io.ReadFull(rand.Reader, nonce)`),
     and that key is NEVER rotated, has NO seal counter, NO metric. On a long-lived
     high-volume cluster the per-key seal count accrues toward the NIST SP 800-38D
     random-nonce cap (2^32) with no signal and no remediation — a silent AES-GCM
     nonce-collision cliff (confidentiality + integrity loss).
   - Context: the KEK/DEK envelope subsystem (#36–62, v0.0.356.0) governs ONLY JWT
     signing secrets (`iam/jwt/jwt.go` is the sole `DEKKeeper.Seal`/`DekGen`
     key-selection consumer); it does NOT protect bulk data. `ObjectIndexEntry.DekGen`
     is recorded + ref-counted but is NOT used to select a key for object decryption
     (storage uses the static `b.encryptor`) — a half-built object→DEK bridge.
   - Options to weigh in design: (a) add seal-count + metric + rotation lifecycle to
     the static key; (b) migrate bulk data onto the gen-based DEK and reuse the
     existing KEK-rotation/replication/seal-counter machinery (finish the DekGen
     bridge). Either is a real subsystem, not a patch.
   - Signal: per-key seal count, nonce-budget metric, rotation/re-encrypt status.
   - Verification: high-volume seal-count accounting test; rotation re-encrypt or
     envelope-migration correctness across all storage layers + EC shards.
   - Boundary: design first (brainstorm). Surfaced 2026-05-27 by /review-forever
     killing the mis-targeted auto-DEK-rotation plan (DEK seal volume is JWT-only,
     never reaches the threshold). See [[project-grains-at-rest-two-key-systems]].

- [ ] **BadgerDB atomic auto-recovery design**
   - Trust risk: recoverable Badger state still requires manual intervention
     during an outage.
   - Signal: recovery plan, applied action, skipped action, and reason.
   - Verification: corrupted/partial Badger replay plus snapshot restore tests.
   - Boundary: design next; implement after recovery journal decision structs
     stabilize.

- [ ] **Rolling upgrade remaining safety slices**
   - Trust risk: mixed-version clusters can hide schema, config, or capability
     divergence behind a healthy surface.
   - Signal: compatibility gates, upgrade status, snapshot-config compatibility
     status, warning/safe rejection/degraded state.
   - Verification: `tests/compat/`, mixed-cluster tests, snapshot-config forward
     policy tests.

- [ ] **Object placement/index orphan and stale reconcile**
   - Trust risk: object-level placement creates a dual-write failure mode between
     data groups and the global object index.
   - Signal: reconcile report, repair/quarantine counters, non-silent failure.
   - Verification: index commit failure injection, orphan data, stale index,
     missing shard, corrupt shard tests.

- [ ] **Cluster health data-group Raft progress**
   - Trust risk: metaRaft can look healthy while a data group is leaderless or
     lagging.
   - Signal: per-group leader, term, lag, drain state, and transfer state.
   - Verification: induced lag, leader transfer, multi-group cluster health e2e.

- [ ] **NFSv4 and 9P auth/access-control correctness**
   - Promote only when `GrainFS` must support untrusted networks, multi-tenancy,
     compliance-driven file access audit, or S3 IAM parity for file protocols.
   - Signal: export-level policy, auth mode, denied access events.
   - Verification: cross-protocol auth tests and reserved `__meta/` namespace
     tests.

## Next

- [ ] **Auth redesign §1 Foundation post-ship cleanup** (v0.0.260.0 review-forever
  Pass 1 INFO findings — non-blocking, ship after §2/§3 to keep blast radius small):
    - Maintainability M#2: extract `peelTrailer(data, magic, trailerLen)` helper in
      `meta_fsm.go` (3 structurally identical DKVS/GCFG/IAM peel blocks).
    - Maintainability M#4: rename `encrypt.LoadFromFSM` → `NewDEKKeeperFromVersions`
      (caller-context leaking into callee package name).
    - Maintainability M#6: extract common pattern from `applyConfigPut` /
      `applyConfigDelete` (nil-guard + decode + dispatch).
    - Maintainability M#7: drop the `safe bool` param on `DEKKeeper.Prune` —
      callee should expose `PruneUnchecked` and the FSM guard does its own
      ref-count check before calling.
    - Maintainability M#8: drop the `snapshotMetaFSMToBytes` /
      `restoreMetaFSMFromBytes` one-line wrappers in `config_snapshot_test.go`.
    - Maintainability M#10: register `encryption.rotate-dek` /
      `prune-dek-version` with `OnTrigger: nil` instead of no-op closures.
    - Testing F#11: end-to-end `ProposeConfigPut → Apply → cfgStore.Set → hook`
      test through a real `MetaRaft` (only unit-level apply path covered today).
    - Testing F#12: `applyConfigPut` with a corrupt inner FlatBuffer payload
      (decode-error gate currently untested).
    - Performance P#8: `Snapshot()` acquires 3 separate locks (FSM mu, cfgStore mu,
      DEKKeeper mu) in sequence — document the single-writer guarantee or fold
      into one acquisition if snapshot latency becomes measurable.
    - Tracking: `docs/superpowers/plans/2026-05-19-auth-redesign.md` §10 R-notes
      + §11 decisions log.

- [ ] **§9 Follow-ups** (from F#41/F#41b implementation):
    - L1 anon-allow bucket scope: `internal/s3auth/authorizer.go:83` returns Allow
      for anon on any bucket when `iam.anon-enabled=true`. L2/L3 currently catches
      the leakage, but a future L2/L3 refactor could expose it. The Phase 0 banner
      only promises `/default`. Tighten L1 to bucket==default. Discovered during
      T71/F#41b.
    - **F#42 (Pass-1 MEDIUM-1)**: Phase 0 anon Allow paths emit no audit row
      (`request_authz.go:148` gates recordAllow on `AuthEnabled()`). Phase 2
      anon-to-default IS audited via `AnonAllow` flag. Phase 0 long-lived state
      needs a forensic trail — anon writes to /default during Phase 0 should
      produce at least a single coarse audit row per request. Trade-off: audit
      table volume vs. forensic capability.
    - **F#44 (Pass-1 LOW-2)**: T73 cluster prober runs ~12 samples vs ~11811 on
      single-node (QUIC forward saturates). Parallelize cluster prober (5-10
      goroutines) to lift sample density, OR document the cluster-density gap
      so a future maintainer doesn't assume burst coverage.
      `tests/e2e/phase_transition_test.go:170-201`.
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
- [ ] **Capability evidence warmup wiring**: `/v1/cluster/capabilities` now
  exposes gate evidence on the admin UDS, so the original ready-probe gap is
  closed. Remaining work is to replace fixed benchmark/operator sleeps
  (`CLUSTER_WARMUP_SLEEP=45` in `docs/reference/benchmarks.md`) with polling
  against that endpoint, then re-check whether gossip latency still needs
  interval tuning or raft-committed evidence.

## Deferred Until Triggered

- [ ] **KEK-envelope C-prune-followup: `SegmentRef.dek_gen` done right + with consumer**.
  Deferred from the D-seg-ec-activate slice (v0.0.368.0). Recording the sealing DEK
  generation in segment metadata was cut because the only cheap source
  (`keeper.Active()` at segment-write time) is not guaranteed to equal the actual
  per-shard seal gen (gen-pinning is per-shard-stream; rotation-mid-write / remote-node
  differences), so a recorded value could be silently wrong — a footgun for a prune
  consumer that trusts it to drop DEK generations (wrong-low → prune a still-referenced
  DEK → unreadable data). Authoritative per-shard gen already lives in the GFSENC3 header.
  When reopened: thread the REAL seal gen out of the shard write path through
  `storage.SegmentRef` + `storagepb.SegmentRef` + `clusterpb.SegmentRef` +
  `clusterpb.SegmentMetaEntry` + PutObjectMeta/CompleteMultipart codecs +
  `segmentMetaEntriesToRefs`, AND build the prune consumer that reads it (cross-checking
  the GFSENC3 header gen). Bundle both so the recorded value is correct before anything trusts it.
- [ ] **KEK-envelope: write-path `ErrDEKGenUnknown` → retriable 503**. On the EC-shard
  PUT path, `cpupool.go` → `commit.go` collapses a per-shard `encrypt.ErrDEKGenUnknown`
  (gen not yet local) into a generic "K shards unreachable" error → likely a 500 on S3
  PUT. `WaitDEKReady` (run.go:227) gates serving so a normal write never hits an empty
  keeper, making this unreachable on a serving node today. Reopen as a hardening pass:
  detect `errors.Is(err, encrypt.ErrDEKGenUnknown)` in the commit coordinator and map it
  to a retriable 503 (not 500). The READ side already classifies it as transient (slice C).
- [ ] **KEK-envelope: cluster e2e join + snapshot-restore object reads**. The
  D-seg-ec-activate e2e added rotate-survives + follower-read-no-quarantine (both green
  on a live 3-node cluster). Join-after-bootstrap and snapshot-restore-boot object-read
  specs were skipped because the e2e harness has no dynamic `AddNode` (4th node post-
  bootstrap) and no snapshot-restore-boot helper (it has `KillNode`/`RestartNode` only).
  Reopen: add an `AddNode`/post-bootstrap join helper + a snapshot-restore-boot helper to
  `tests/e2e/cluster_harness_test.go`, then add the two additive specs under the "KEK
  rotation lifecycle" Describe.
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

## Chunking / Large-Object Follow-Ups

- [ ] **Cluster pull-through large-object parity [P1]**: `TestPullthroughE2E/
  Cluster4Node/LargeObject` (5 MiB random payload) returns bytes that differ
  from the upstream payload after the cache-miss GET; the cache-hit GET
  exhibits the same divergence. SingleNode passes the identical case. The
  symptom looks like truncation or a partial 2-pass streaming write to the
  cluster's local cache before the response is served. Test was originally
  single-only; promoting the e2e to TestBucketsE2E dual surfaced this gap as
  a now-failing Cluster4Node subtest — the failing assertion is the
  regression signal that unblocks closing the gap. Fix candidates: trace the
  pull-through 2-pass streaming write on cluster (where in EC distribute the
  body is consumed) and ensure the local cache write completes before the
  HTTP response body is closed.

- [ ] **PITR WAL replay carries segment metadata [P1]**: snapshot/restore now
  knows `Object.Segments`, but `storage/wal.Entry` still only carries scalar
  object metadata (`Bucket`, `Key`, `ETag`, `ContentType`, `Size`, `VersionID`).
  WAL-only PITR replay can therefore recreate a segment-backed object record
  without the segment refs and later report it stale or unreadable. Add a WAL
  format version that encodes segment refs, preserve old replay compatibility,
  and add PITR coverage for a chunked object created after the snapshot point.

- [ ] **Placement monitor: repair a corrupt LOCAL shard from peers instead of quarantining [P2]**:
  `scanRecord` now correctly classifies confirmed corruption (CRC/structural/AEAD) and
  still quarantines the **whole parent object** at object granularity — the on-corruption
  semantics were intentionally left unchanged in the transient-classification PR. But a
  single corrupt local shard is usually recoverable: with 4+2 EC and enough surviving
  peers, reconstruct the bad shard in place (same path the missing-shard repair already
  uses) and only quarantine if reconstruction fails. This downgrades most corruption
  events from object isolation to a silent local repair. (`internal/cluster/shard_placement_monitor.go`.)

- [ ] **Placement monitor: stream scan targets instead of buffering O(objects+segments) [P3]**:
  `Scan` buffers all `ECShardScanTarget`s before processing; ~1.5 GB peak for 1 M chunked
  objects × 10 segments each. Follow up if production scans show RAM pressure; streaming
  would hold the FSM read transaction open during local shard checks (its own tradeoff).

- [ ] **Placement monitor: scan non-latest object versions' segment/coalesced shards [P3]**:
  The placement monitor currently covers only the latest object version
  (`IterObjectMetas` semantics). Segment and coalesced EC shards belonging to
  non-latest versions are not proactively scanned; they rely on read-time EC
  reconstruction. Extend the monitor to iterate non-latest versions' segment refs
  as well, closing the between-boot gap for versioned objects with degraded
  older-version shards.

- [ ] **AppendObject real per-call MD5 chain [P2]**: `AppendObject` still stores
  segment xxhash checksums in `AppendCallMD5s` as a stopgap. Capture each append
  call payload's real MD5 at the API/storage boundary and persist that digest in
  both single-node and cluster apply paths so composite append ETags are S3-wire
  correct while segment checksums remain xxhash3 for internal integrity.

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

- [ ] **Cluster4Node HeadObject가 expired object에 `MethodNotAllowed` 반환 [P1]**:
  `tests/e2e/lifecycle_expiration_test.go:111-118` TagFilter spec에서 expire 후
  HeadObject가 S3 표준 `NotFound` 대신 `MethodNotAllowed`를 반환. 기존 testify
  `assert.Equal` soft-fail이 가렸던 동작 — Ginkgo gomega.Expect는 hard fail이라
  노출됨. 현재 Or(NotFound, MethodNotAllowed) matcher로 마스킹. 원인 후보:
  `internal/cluster/backend.go` `deleteObjectWithMarker` 가 non-versioned bucket
  expiration 후에도 delete-marker semantics 적용. S3-parity 위해 NotFound 일관화
  필요.

- [ ] **Cluster tests create QUIC transports without closing them [P2]**:
  ~60 call sites in `internal/cluster/*_test.go` create
  `transport.MustNewQUICTransport(...)` but never `Close()` it, so quic-go's
  `Transport.listen` / `sendQueue.Run` / `Conn.run` goroutines live until process
  exit. They accumulate across the cluster test binary and are the goroutines that
  `ec_fix_integration_test.go`'s process-global `goleak.VerifyNone` was catching
  (now scoped via `goleak.IgnoreCurrent` baseline). Hygiene cleanup: add a
  `newTestTransport(t)` helper that registers `t.Cleanup(tr.Close)` and migrate the
  call sites. Mechanical but large; not required for correctness.

- [ ] **`TestBlobStoreAppendNoCompressKeepsAllocationBound` race-mode fail [P2]**:
  baseline에서도 동일하게 fail (allocations=4 vs ≤1 expected). `-race` 빌드에서
  추가 alloc churn. packblob 패키지 별도 작업.

- [ ] **Reverify full `make test-e2e` baseline [P1]**: old snapshot listed
  backup_restic, multipart (`capability multipart_listing_v1 rejected`),
  cluster_incident, quarantine_incident, dynamic_join_services,
  cluster_scrubber, no_peers, encryption_at_rest, iam_scoped_key. Since the
  capability evidence endpoint now exists, rerun the baseline and split true
  remaining failures from stale bootstrap/warmup artifacts.

## Conformance Follow-Ups

- [ ] [nfs-conformance] pynfs-nightly [P1]: run pynfs basic suite on a scheduled
  Linux/Colima host and review `results/summary.json`.
- [ ] [nfs-conformance] nfstest-runner [P2]: add nfstest after pynfs stabilizes.
## Storage And Volume Backlog

- [ ] **Volume pool quota operator surface**: `volume.ManagerOptions.PoolQuota`
  and planner/unit coverage exist; expose/configure the cross-volume physical
  capacity pool through the server/CLI path before treating it as an operator
  feature.
- [ ] **Multi-tenancy**: account/namespace isolation above IAM.
- [ ] **Quota**: capacity limits per service account or team.
- [ ] **Volume export/import**: backup/restore for volume data.
- [ ] **Volume policy**: per-volume pool quota, encryption key, and EC
  profile overrides.
- [ ] **Volume attach/detach**: runtime NBD exposure toggles.
- [ ] **Volume rename**: migrate volume block key prefixes.

> Volume dedup, snapshot, clone, rollback, and copy-on-write were removed in
> v0.0.346.0 (plain block device). Items above that previously assumed those
> features are descoped accordingly and depend on the eventual volume redesign.
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

- [ ] **Redesign disaster-recovery surface.** `grainfs recover` and `grainfs doctor` were removed in v0.0.343.0 (shipped partial/misleading). Before reintroducing: (1) define failure domains explicitly (rack/AZ/region); (2) name commands by what they actually do (`recover metadata` vs `recover data`, never bare `recover cluster`); (3) `doctor` must do real integrity checks, not directory-existence stubs; (4) RPO + data-vs-metadata semantics must be in help text. Until then the `recover` verb stays unbound.
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
- [ ] fix(storage/packblob): extend versioning bypass to Suspended state (currently only Enabled bypasses fast path; Suspended buckets on single-node still pack-write under (bucket,key) without versionId="null"). Add e2e cases for Suspended → PUT/DELETE/HEAD by versionId.
- [ ] feat(scrubber): multi-node/multi-group segment GC fan-out. Orphan-segment GC currently (Plan 3.5) activates only on group-0's distBackend AND, in a cluster, runs only on the raft leader (CaughtUp uses node.ReadIndex → followers get ErrNotLeader → fail-closed skip). Result: single-node is complete; in a multi-node cluster, segments on non-leader nodes' local disks and in non-group-0 data-groups are never reclaimed → latent disk growth. Proper design needs leader-coordinated (or per-node-with-freshness-barrier) deletion across all groups — mirror the EC scrub ecResolver fan-out (boot_phases_scrubber.go) and decide who deletes follower-local raw segments. SegmentOrphanLog already namespaces by groupID. Blocked-by: Plan 3.5 (object-segment-gc-activation) land.
