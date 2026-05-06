# TODOS

> **Design principle — Zero Config, Zero Ops.** 기본 설정으로 잘 돌아가고, 잘 운영되며,
> 크리티컬한 문제는 사용자에게 알려서 선제대응하게 만든다.
> 각 Phase 항목에 "— *zero config*" / "— *zero ops*" 표시가 있는 것들이 이 원칙에 해당.

### 기타

- [ ] **Storage operations CopyObject semantics** — `storage.Operations.CopyObject`
  를 S3 CopyObject 의미의 test surface 로 deepen 한다. Handler 는
  `x-amz-copy-source` 를 URL decode + query parse 해서 `ObjectRef{Bucket, Key,
  VersionID}` 로 넘기고, raw HTTP header text 는 facade 로 넘기지 않는다.
  `CopyObjectRequest` 는 metadata directive, source 조건
  (`IfMatch`/`IfNoneMatch`/modified-since 계열), ACL override 를 protocol-neutral
  type 으로 담는다. `Operations.CopyObject` 는 항상 source `HeadObject` /
  `HeadObjectVersion` 을 먼저 수행해 delete-marker, precondition, metadata
  directive, fast-path eligibility 를 body open 전에 결정한다. Optimized
  `storage.Copier` 는 semantics-free entry point 가 아니라 `Operations` 가 검증한
  뒤 쓰는 private acceleration path 로 제한한다. 1차 slice 는 user metadata 전체
  확장 대신 현재 object model 의 `ContentType` 까지만 명확히 처리한다.
  Precondition 실패는 `storage.ErrPreconditionFailed` + typed error 로, explicit
  delete-marker version copy 는 `InvalidCopySourceError` 로 모델링한다. 이번 slice
  에서는 destination previous-object bookkeeping 을 넣지 않고, 위 mutation
  bookkeeping TODO 에서 `CopyObjectWithResult` 포함 여부를 함께 재검토한다.
- [ ] **volumeadmin: 30s http.Client.Timeout 제거** — `internal/volumeadmin/client.go`
  의 `http.Client{Timeout: 30 * time.Second}` 가 `BaseOptions.Timeout > 30s` 를
  override 함. ctx-기반 cancellation 으로 충분하니 client timeout 제거하고 ctx
  하나로 통일. (pre-existing pattern, 이번 라운드는 surface만 옮김)
- [ ] **volumeadmin: ParseSize "1KB" silent fail** — `format.go` 의 suffix
  매칭 순서가 "B" 를 먼저 매치해서 "1KB" → ParseInt("1K") 에러. "1K" / "1KiB"
  는 동작. pre-existing. KB 같은 SI 표기 정식 지원 또는 명시적 reject.
- [ ] **volumeadmin 추가 단위 테스트** — withTimeout deadline 전파 / Do() malformed
  JSON 응답 / FollowScrubSession 비-ctx 네트워크 에러 / AutoDiscoverSocket 실제
  unix-socket 디스커버리 happy path / WriteAt content base64 round-trip.
- [ ] **clusteradmin BaseOptions retro-fit** — `internal/clusteradmin/operations.go` 의
  `RemovePeerOptions / PeersOptions / EventsOptions` 가 `Endpoint/Stdout/Stderr/...`
  공통 필드를 인라인 반복함. `volumeadmin.BaseOptions` 와 같은 임베드 패턴으로
  맞춰 비대칭 해소. 별 PR 권장.
- [ ] **serve.go cmd-thin** — `cmd/grainfs/serve.go` (~2255줄, runCluster 1370줄)
  에서 raft+storage+server+monitors 와이어링을 `internal/serveapp/` (가칭) 같은
  새 패키지로 분리. cobra 진입은 thin wrapper만 남게. 별 grilling 라운드 필요.
- [ ] **resource_monitors.go 패키지 이전** — `cmd/grainfs/resource_monitors.go`
  (~655줄)의 `start*ResourceMonitor` 함수들은 cobra runner가 아니라 runCluster
  goroutine 부트스트랩. `internal/resourceguard/` 같은 server-side 패키지로 이동.
- [ ] **Thin pool quota (cross-volume)** — 여러 볼륨이 공유하는 물리 용량 예산 풀. 볼륨별 `PoolQuota` 옵션(Phase A)보다 정교한 전체 클러스터 수준 quota 관리. Phase A 완료 이후.
- [ ] Memory usage validation
- [ ] multi tenancy
- [ ] quota
- [ ] IAM
- [ ] hot/cold auto tiering
- [ ] io 기반 auto rebalancing
- [ ] iceberg: 카탈로그 상태 점검
- [ ] iceberg: 메타데이터 정합성 검증
- [ ] 정기 복구 리허설

## Phase 17: Scale-Out

- [ ] **BadgerDB atomic auto-recovery** — 이전 Phase 16에서 이연. log-based replay + snapshot restore 자체 구현 (단순 `badger.Open` 내장 복구를 넘어서는 원자적 복구 레이어)
- [ ] **BadgerDB pre-server recovery journal** — *zero ops* — `<data>/.recovery/` 아래에 incident-state DB가 열리기 전 발생한 Badger role decision을 원자적 JSONL/manifest로 남긴다. 현재 Badger role-scoped recovery 첫 slice는 pre-server failure를 stderr-only로 수용한다. 이후 `internal/badgerrole` decision struct가 안정되면 fsync+rename 규칙으로 durable journal을 추가해 startup-blocking failure도 재시작 후 incident/API/UI에서 추적 가능하게 만든다. **Depends on:** Badger role decision structs.
- [ ] **Blame Mode v2 — shard-level 시각적 replay** — Phase 16은 텍스트 타임라인 + JSON download만, v2에서 shard 재생 UI
- [ ] **PagerDuty 네이티브 webhook 매핑** — Phase 16은 Slack-compatible JSON + docs 매핑만

- [ ] **PR-D**: ShardGroupEntry.PeerIDs identity를 nodeID로 통일 (현재 raftAddr 혼재). MetaFSM에 NodeID→Address 매핑 + Resolve API. PR-E 선행. Design: [docs/cluster-dynamic-join.md](docs/cluster-dynamic-join.md).
- [ ] **PR-E**: Cluster dynamic join (1→N sequential bootstrap). `serve --join <leader>` 단일 진입점 + `MetaTransport.SendJoin` admin RPC + `Node.AddVoterCtx` 기반 catch-up 보장 + `MetaRaft.AddShardGroupVoter` add-only voter primitive + balancer가 PendingNodes/ReadyNodes 처리 (voter 보강 + 그룹 동적 확장, N≥3 가드). Design: [docs/cluster-dynamic-join.md](docs/cluster-dynamic-join.md). **Depends on:** PR-D.
- [ ] **PR-X**: e2e 헬퍼 `tryStartMRCluster` 1→N 시퀀셜 동적화 (`serve` seed → 나머지 `serve --join`) + `--fast-bootstrap` 정적 옵트인 + `TestE2E_TwoNodeAvailabilityTrap`/`TestE2E_DynamicGroupSeeding_1to5` 운영 회귀. **Depends on:** PR-E.
- [ ] **PR-F**: §4.3 joint consensus atomic multi-server replacement (Tier 3-1 Sub-project 3에서 다룸). **Depends on:** Voter set lock-free read / `membershipView` quorum snapshot boundary 완료 후 진행 — PR-F는 quorum/election/ReadIndex가 mixed membership state를 보지 않는다는 전제 위에 올라간다.
- [ ] **raft-ehn Tier 2** (raft-ehn 범위 밖, 트리거 조건 도달 시 별도 design):
  - BatchingFSM (FSM apply throughput 한계 도달 시)
  - Snapshot chunking + Concurrent snapshotting (FSM이 QUIC stream max 근접 시)
  - Per-PR Prometheus dashboard 갱신 PR
- [ ] **raft-ehn Tier 3** (별도 브랜치, 각 sub-project는 독립 design + plan + worktree):
  - **Tier 3-3: 클라이언트 dedup** — ClientID + RequestID 기반 dedup table, S3 SDK retry 시 중복 PUT 방지
- [ ] **§2.x 잔여 항목** (필요 시 design doc 재발굴) — 4-30 §2.3 snapshot servers persistence(#103 v0.0.6.11) 동일 series의 다른 violation 항목이 있었는지 다음 brainstorming session에서 확인.
- [ ] Migration: NFS virtual overlay
- [ ] Migration: NBD block proxying
- [ ] nbd over internet for edge computing (powered by wireguard)
- [ ] **Rolling upgrade safety** — *zero ops* — 버전 간 binary 교체로 downtime/데이터 손실 없음 (schema migration 자동, snapshot forward-compat 보장)

## Transport Protocol

### Transport protocol version policy before first external release

**What:** 첫 외부 릴리스 전에 QUIC transport message envelope의 versioning, capability negotiation, rolling-upgrade 정책을 확정한다.

**Why:** 이번 architecture hardening 계획은 pre-release 전제라 old frame 호환성을 의도적으로 제외하지만, 릴리스 이후에는 wire format 변경이 운영 중 cluster split이나 요청 decode 실패로 이어질 수 있다.

**Context:** `docs/superpowers/plans/2026-05-03-storage-layer-architecture-hardening.md`의 Task 3는 `Message.ID`/`Message.Status`를 포함하는 새 envelope로 codec을 교체한다. 지금은 동일 버전 cluster 전제라 단순 교체가 맞지만, 첫 릴리스 전에는 ALPN/capability negotiation, protocol version constant, downgrade 거부 에러, rolling-upgrade 테스트 범위를 정해야 한다.

**Effort:** M
**Priority:** P1
**Depends on:** Storage layer architecture hardening Task 3

## Phase 18: FUSE-over-S3 (외부 도구 호환성 보증)

**방침**: 별도 FUSE 바이너리/서버 사이드 마운트를 만들지 않는다. GrainFS는 표준 S3 API만 제공하고, 클라이언트는 rclone / s3fs / goofys 같은 기존 FUSE-over-S3 도구를 그대로 사용한다. 클라이언트 머신에 grainfs 바이너리 설치 불필요.

**향후 (선택)**:
- [ ] s3fs-fuse, goofys 호환성 추가 검증 (현재 rclone만 검증)
- [ ] FUSE-over-S3 throughput 벤치 (NFSv4 baseline 대비)
- [ ] 엄격한 POSIX 시맨틱(atomic rename, file locking)이 필요하면 NFSv4 권장 — 별도 FUSE 솔루션 도입은 NFSv4 운영성 부족이 입증된 이후

## Phase 19: Performance

- [ ] go-billy: Direct File I/O; O_DIRECT
- [ ] **EC shard cache 사이즈 튜닝** — 본구현 완료 v0.0.4.42 (E2E 85.7% hit). 운영 telemetry(`grainfs_ec_shard_cache_hit_rate`)로 working set 측정 후 default 256 MB 적정성 검증. 큰 객체 백업 워크로드면 GB 단위까지, 작은 객체 위주면 비활성화 권장.
- [ ] io_uring
- [ ] SPDK
- [ ] SoA (Structure of Arrays)
- [ ] SIMD
- [ ] **Predictive resource warnings — measurement re-tuning** — *zero ops* — Goroutine watcher (PR1, v0.0.44.0) + Vlog watcher (PR2, v0.0.45.0) + admin breakdown + e2e + GC metrics (PR3, v0.0.46.0) shipped. PR2 = `*Registry` + Default + DI, VlogProvider (`db.Size()` 합산 + statfs ratio), GC ticker (5min sequential, snapshot-then-unlock + max-iter cap 8 + transition-only fire), startup smoke (60s deferred, mtime stale/live 분류). PR3 = `GET /v1/resource/vlog/breakdown` admin endpoint + GC metric wiring (`grainfs_badger_gc_runs_total`/`failures_total`/`consecutive_failures` 이전 PR2 에서 declare 만 된 채 미증분) + strict mode actually fatal + `--vlog-smoke-defer` flag + 4 e2e (MetricsLive, GCTickerRecovers, StrictFatalOnMissing, NoStarvation). Default ratio 0.4/0.7. **Re-open triggers:** (a) production `grainfs_vlog_used_ratio` p99 분포로 0.4 임계 재조정 / (b) k6 PUT throughput latency dip 측정 후 `gcMaxIterPerDBPerTick` 조정 / (c) per-group breakdown 가 운영 hotspot 으로 입증되면 `Category` 튜플 확장 PR4 / (d) growth-rate detector (sliding window) PR5 / (e) Group dir cleanup TODO (Cluster Replication Reliability 섹션) / (f) ETA cold-start over-eager fire — shipped v0.0.49.2 (`MinETAElapsed` 5min default, level fire 영향 없음) / (g) e2e leak-fire test — shipped v0.0.49.3 (`--badger-value-threshold` hidden flag + `TestE2E_VlogWatcher_FiresOnLeak`).
- [ ] **BadgerDB 인스턴스 통합 (P3 — FSM state DB)** — raft-log 통합(P0b)은 v0.0.13.0 출시됨 (`OpenSharedLogStore` + `--shared-badger` 기본 활성). idle-N8 측정에서 goroutines -16%, heap -19%, RSS -25% 확인. 남은 작업: FSM state DB도 노드당 1개로 통합 (P3). 현재 설계 검토에서 13개 이슈 발견 후 일시 정지 (live snapshot Restore가 FSM 우회, DropPrefix DB-wide stall, 추정 11-14일). docs/architecture/badger-consolidation.md 참고. **상태: PAUSED.** R+H (QUIC stream-reuse) 효과로 idle CPU 70%→3.5% 달성, P3 시급도 낮아짐. 재오픈 조건: (a) FSM state badger가 새 핫스팟으로 떠오르면 (b) 13개 이슈 mitigation 명확해지면. 그 외에는 v0.1.x 시점에 close.
- [ ] **R+H 측정 잔여** — load-N8 / load-N16 mux=on 깨끗한 측정. e2e bucket-replication race + macOS host contention 임계 해결 후. pool size sweep (1/2/4/8)로 RSS +74% 영향 평가 후 default 재조정.
- [ ] **Meta-mux post-deploy 측정** — v0.0.19.0 (#141)로 meta-raft mux 통합 shipped. R+H load-N8 clean baseline 후 meta-mux on/off A/B 측정으로 plan에서 추정한 ~4% 트래픽 감소 실측. 작아서 noise에 묻힐 가능성, host stable 환경 필수. 결과를 `docs/architecture/quic-stream-multiplex.md` §Follow-up에 DELIVERED 헤더로 기록.
- [ ] control plane, data plane 분리

## Phase 20: Protocol Extensions

- [ ] Redis 프로토콜 지원 (RESP, Streaming, Pub/Sub 이벤트)
- [ ] TSDB (Time Series DB) — Metric 저장 및 쿼리 지원

## Phase 20: Operations & Onboarding

운영자 개입 없이도 안정적으로 동작하고, 문제 발생 시 명확하게 알려주는 기본기.

- [ ] **One-command bootstrap** — *zero config* — `grainfs init` 하나로 cluster key, encryption key, 기본 credential, volume 생성 + 필요 파일 권한 설정
- [ ] **Hot reload drift detection** — *zero ops* — config 파일 시스템 도입 후, 런타임 reload 시 디스크 config와 메모리 상태 불일치 감지 + 명확한 에러. config 파일 시스템 자체가 선행 조건.

## Volume CLI follow-ups (Phase B 이후)

`docs/superpowers/specs/2026-05-04-volume-cli-management-design.md` Phase B(라이프사이클 + 변경/진단)가 shipped 된 이후 단계. 각 항목은 server 모듈 부재 — 별도 spec/plan 필요.

- [ ] **`volume export / import`** — 볼륨 + 스냅샷 체인 백업/복구. server 측 export stream(블록 + live_map + meta) 필요. 키 회전, EC 파라미터, 스냅샷 정합성 고려.
- [ ] **`volume policy`** — 볼륨별 PoolQuota / dedup on/off / encryption key id / EC k+m 파라미터 조회·변경. 현재 `ManagerOptions`는 Manager 단위라 per-volume override 모델 선행 필요.
- [ ] **`volume attach / detach`** — NBD 노출 토글. 현재 NBD는 serve 시작 시 정적 바인딩만, 런타임 attach/detach 미지원.
- [ ] **`volume mv / rename`** — 볼륨 이름 변경. live_map/snapshot key prefix 마이그레이션 필요.
- [ ] **incident store scope index** *(deferred — measured 55× margin from re-open threshold)* — `internal/incident/badgerstore/store.go` 의 `List(ctx, 500)` + StatVolume 메모리 필터 패턴. **벤치마크 (`store_bench_test.go`, Apple M3): N=1k/10k/100k 에서 모두 ~1.8 ms/op flat** (List 의 limit=500 cap 때문에 N≥500 부터 fixed cost). TODO 의 re-open threshold (100ms) 대비 55× 마진. Secondary index 추가의 production 이익 measurable 하지 않음. **Re-open triggers:** (a) List limit 이 500 보다 훨씬 커지는 새 caller 가 생기거나, (b) Match rate 가 0.5% 미만으로 떨어져서 newest-500 scan 으로 sufficient match 못 찾는 access pattern 이 나오거나, (c) 운영 telemetry 로 `incident` API/StatVolume p99 가 50ms 초과. (a)/(b)/(c) 중 하나가 측정값으로 trigger 될 때까지 코드 그대로.
- [ ] **`ScanObjects(bucket, keyPrefix)` 로 시그니처 확장** *(deferred — wait for concrete caller)* — `internal/cluster/scrubbable.go` 의 `ScanObjects` 가 `lat:` 인덱스 전체 iterate. 현재 caller 둘 (`scrubber/scrubber.go`, `lifecycle/worker.go`) 모두 bucket 전체 walk 가 필요. prefix-bounded API 를 미리 추가하면 dead surface — YAGNI. **Re-open trigger:** placement key 또는 shard prefix 단위 scan 이 자연스럽게 요구되거나, lifecycle rule 이 prefix-scoped 로 확장될 때 함께 1-line signature 확장 + 기존 callers `""` 적응. 그때 확장이 더 정직 — 실 use case 에 맞춘 prefix shape 결정 가능 (e.g. raw key prefix vs placement key prefix vs ring-bucket prefix).
- [ ] **EC scrub group rebalance race** *(deferred — pre-existing in BackgroundScrubber path)* — In-flight scrub sessions walk a group's BadgerDB. If `BucketAssigner` rebalances bucket→group mid-walk, ScanObjects may emit stale records. EC scrub trigger landing (PR4 v0.0.47.0) shipped via FSM-replicated trigger (`MetaCmdTypeScrubTrigger`) + per-bucket group resolver + `ForwardOpScrubSessionStat` cluster-wide aggregation. The resolver routes bucket → group correctly but does not detect or cancel in-flight scrubs on rebalance. **Re-open trigger:** production reshard event triggers concurrent scrub session, telemetry shows Detected ≥ 1 / repair fail rate spike. Design approach: cancel-on-rebalance hook in `dgMgr.Add/Remove`. Original design doc preserved at `docs/superpowers/specs/2026-05-05-ec-scrub-blocksource-design.md` (D1-D9 of v0.0.43.9 adapter) + `2026-05-05-ec-scrub-trigger-landing-design.md` (D1-D7 of v0.0.47.0 trigger landing).

## Storage Hashing 성능 검토

- [ ] **MD5 hot-path 비용 측정 + faster hash 도입 검토** — *zero ops 무결성 oracle* — v0.0.43 volume scrub PR 에서 `LocalBackend.PutObject` / `cluster/spool.go shouldHashBucket` / `LocalBackend.WriteAt` 의 MD5 skip 정책을 제거 (모든 internal bucket 도 hash 계산). 이유: hash 가 없으면 disk bit-rot, 부분 write, 번역 오류를 영구히 검출 못 함 → scrub oracle 자체가 사라짐. **Concern:** 1MB+ 큰 write 가 NVMe 에 가는 hot path 에서는 MD5 (~2 GB/s 단일 코어) 가 disk IO 보다 비싼 비중을 차지할 수 있다 (예: 1MB / NVMe 250µs vs MD5 ~2ms). **검토 항목:** (a) k6 워크로드로 MD5 비용 실측 (S3 PUT throughput 회귀 측정), (b) BLAKE3 (~6.5 GB/s) / xxhash3 (~30 GB/s, non-cryptographic 이지만 corruption detection 충분) 교체 비교, (c) S3 ETag 호환 (외부 client mc/aws-cli 의 multipart 검증) 영향 평가, (d) hash 계산을 별도 goroutine 으로 옮겨 disk write 와 병렬화 (io.MultiWriter 대체) — 어느 방향이든 PR 별도. **재오픈 트리거:** S3 PUT throughput regression 가 측정값으로 확인되거나, NFS WRITE p99 가 SLO 침범 시.

- [ ] **Hash 정책 분기 가드 — bucket-class 단위로 끄지 말 것** — 과거 `IsInternalBucket(bucket)` 만으로 MD5 를 끈 정책이 volume scrub oracle 을 제거했다 (위 항목 참고). 이유: bucket-class 는 무결성 요구를 표현하지 못함 — `__grainfs_volumes` 와 `__grainfs_nfs4` 둘 다 internal 이지만 둘 다 외부 별도 보호가 없어 hash 가 필수. 만약 미래에 hash 비용이 실제로 hot path 를 압박해서 정책 분기가 필요해지면, **bucket prefix 가 아니라 무결성-요구 등급** (durable replica / EC parity / ephemeral cache) 으로 가르고, 각 등급별로 (a) 별도 protection 이 있는지 (b) detect-only vs detect+repair 정책이 무엇인지 명문화. "internal 이니까 끄자" 같은 한 줄 최적화는 영구 oracle 손실로 이어짐.

- [ ] **`__grainfs_volumes` 의 EC 강제 차단 재검토** — *redundancy 효율 / replication-vs-EC tradeoff* — `internal/cluster/backend.go:821` 가 `IsInternalBucket(bucket) && vfsFixedVersion` 일 때 `useEC=false`. 명시 이유는 "고정 versionID + EC RingVersion-keyed shard placement 충돌 → 링 토폴로지 변경 시 stale shard leak". **하지만 `__grainfs_volumes` 는 고정 versionID 안 씀** (`newVersionID()` 사용). 그런데도 같은 분기에 잡혀 EC 차단됨 → volume 블록은 N×replication 으로 저장. **올바른 분기는** `IsVFSBucket(bucket) && vfsFixedVersion` 또는 `bucket != volumeBucketName && IsInternalBucket && vfsFixedVersion`. 차단을 좁히면 volume 블록이 EC 4+2 로 저장 → 디스크 footprint 1.5x 로 N×replication (3x) 대비 효율. **이건 정합성 문제 해결이 아니라 redundancy 효율 개선** — 정합성 oracle 은 v0.0.43 의 MD5 ETag 복구로 이미 해결됨. **선행 조건:** (a) 기존 replication 으로 저장된 volume 블록 데이터의 EC 마이그레이션 전략 (read-time backfill 또는 명시 마이그레이션 명령), (b) 아래 EC silent corruption audit 로 EC scrub 경로의 oracle 충분성 검증, (c) volume scrub 의 BlockSource/BlockVerifier 가 per-block 으로 EC vs replication 분기하거나 (placement record 보고 EC 면 yield 안 함) EC migration 완료 후 deprecate. → 별도 design 권장.

- [ ] **EC scrub legacy raw shard rewrite/migration** *(deferred — audit fixed, migration remains)* — EC scrub silent-corruption audit found that new EC shards already carry the `eccodec` CRC envelope and are verified by scrub. The remaining no-oracle case is legacy raw shard files written before the CRC envelope. This PR classifies those shards as `Unverified` and maps them to `Skipped` with `reason="legacy_no_crc"` metric, so they no longer count as healthy and do not trigger repair/rewrite. **Deferred work:** optional migration/rewrite to re-save legacy raw shards with the CRC envelope. **Re-open trigger:** production `grainfs_ec_scrub_unverified_shards_total{reason="legacy_no_crc"}` shows non-trivial legacy population, or `__grainfs_volumes` EC enablement needs a clean zero-unverified precondition.

- [ ] **Live-vs-full scrub scope re-introduction** — v0.0.43 Z2 단순화에서 `BackgroundScrubber` 의 `SetVolumeFullInterval` 와 dual-ticker (live/full 분리) 를 제거했음. 이유: `ReplicationObjectSource` 가 generic 이라 ScopeLive 와 ScopeFull 이 동일 walk 로 수렴. 미래에 source 별로 cheap-live (e.g. volume live_map 우선) 와 expensive-full 을 다시 구분하고 싶으면 (a) `BlockSource.Iter` 에서 ScopeLive 가 작은 keyset 만 yield 하도록 source-side filter 추가, (b) `BackgroundScrubber` 에 두 ticker 다시 도입 + serve flag (`--scrub-full-interval`, `--scrub-full-disable`) 노출. plan Task 12 의 의도. 운영에서 live walk vs full walk 의 비용 차이가 측정값으로 분명해지면 재오픈.

- [ ] **Volume scrub cluster-broadcast — MetaFSM ScrubTrigger entries** — v0.0.43 PR 에서 plan 의 Task 8 (MetaFSM `ScrubTriggerEntry` / `ScrubSessionDoneEntry` / `ScrubCancelEntry`) 은 deferred. 이유: 코드베이스의 meta-FSM 은 FlatBuffers schema (`clusterpb.MetaCmd`) 기반이고 plan 은 gob 패턴 가정 — schema 추가 + flatc 재생성이 별도 surface. v0.0.43 은 admin handler 가 single-node 처리 (각 노드에 직접 admin call) 로 cluster-wide trigger 를 우회. 후속에서 (a) `clusterpb.MetaCmdTypeScrubTrigger` enum 추가 (b) `clusterpb.MetaScrubTriggerCmd` flatbuffer 테이블 추가 (c) `applyScrubTrigger` 가 `Director.ApplyFromFSM` 호출하도록 wire 후, 단일 진입점으로 전 노드 동시 트리거 가능. Done/Cancel 도 동일 패턴.

## Cluster Replication Reliability

- [ ] **Group dir cleanup on RemoveGroup** *(deferred — wait for production dataDir telemetry)* — `internal/cluster/group_lifecycle.go` 의 group close path 가 `db.Close()` 만 호출, disk 의 `<data>/groups/<id>/` (raft + vlog files) 잔존. PR2 vlog watcher (`docs/superpowers/specs/2026-05-05-vlog-watcher-pr2-design.md` Arch #4) 의 startup smoke 가 mtime 기반 stale orphan 으로 분류해 incident false positive 는 막음. 그러나 root cause 는 그대로 — 1년 운영 시 group churn 누적분 GB 단위 leak 가능. **Re-open trigger:** production telemetry 에서 dataDir bytes 가 group remove 카운트에 비례 상승 시 별도 PR — `RemoveGroup` (또는 동등 hook) 에 `os.RemoveAll(groupDir)` + 기존 in-flight Open 와 race 방지 (DeregisterDB 후 일정 grace).

- [ ] **PeerHealth one-strike-out — consecutive-failures threshold** *(deferred — wait for production telemetry)* — v0.0.43.4 에서 observability (Gauge + Counter + admin endpoint + transition log) 까지 wired. 현재 동작: 1회 실패 → 10s cooldown → 그 동안 다른 writes 는 IsHealthy=false 로 skip → 실패한 그 block 자체는 retry 안 됨 (RF 가 1 줄어든 채 진행, background scrub 이 추후 보강). N consecutive 로 바꾸면 transient hiccup 에 더 forgiving 하지만 진짜 죽은 peer 검출 지연 (1 write 더 dead path 시도). 어느 쪽이 net win 인지는 워크로드 의존 — high-throughput 핫패스면 N>1 유리, low-throughput / dead-peer 검출 latency 가 critical 하면 N=1 유리. **가설 없이 default 변경 위험 — production 데이터 후 결정.** **Re-open triggers:** (a) `grainfs_peer_unhealthy` Gauge 의 false-positive rate 가 측정값으로 확인됨 (true peer death 가 아닌 transient 로 인한 마킹 비율 ≥ 일정 threshold), (b) 또는 production 에서 cold-start / GC / network blip 으로 인한 1회-실패 quarantine 이 운영 incident 의 root cause 로 식별됨, (c) 또는 ≥ 1주 Prometheus rate 데이터로 N=2 가 dead-peer 검출 지연 cost < transient 보호 이익 임이 정량 입증. 운영 데이터 모이는 동안 코드는 그대로 둠.

- [ ] **`cluster remove-peer` 진짜 liveness signal — metaRaft 노드 헬스 시그널** — `cluster-membership-cli` 라운드에서 `POST /api/cluster/remove-peer` + `cluster peers/events` CLI + joint-consensus 엔진 경로 + e2e (3-node, kill follower, happy-path remove-peer) 까지 ship. **현재 단순화:** `ClusterInfo.LivePeers()` 는 metaRaft voter set을 모두 alive로 보고함 (legacy `DistributedBackend.LiveNodes` 와는 raft 인스턴스가 달라 식별자가 어긋나기 때문). 이 단순화로 (a) pre-flight quorum 수학은 정확 — voter 수 변화로 only-1-voter-left 위험은 잡음, (b) 그러나 dead-peer를 자동으로 **detect하지 않음** → 운영자가 외부 신호 (모니터링/SSH 접속 실패) 로 죽음을 확인한 뒤 명시 호출. 결과: `cluster peers` 출력의 STATE 컬럼은 항상 "alive". **후속 작업:** PR-D 의 nodeID↔Address resolve API 가 들어오면 metaRaft 측에 별도 peerHealth 모니터를 두고 `LivePeers()` 가 그것을 소스로 삼도록 교체. 이때 `cluster peers` STATE 컬럼이 진짜 down/alive 를 반영하고, pre-flight 가 "alive였는데 정족수 깨질" 케이스도 잡게 됨. **Re-open trigger:** PR-D / PR-E 머지되어 nodeID 통일이 끝나면 즉시.

- [ ] **`cluster peers` 출력의 leader 마킹 식별자 mismatch** — `cluster_observe.go` 의 PeersFromStatus가 `s.LeaderID == p` 로 leader 행을 표시하지만, 현 시점 `LeaderID()`는 nodeID ("n1") 를 반환하고 `Peers()` 는 raft-addr ("127.0.0.1:30139") 를 반환해 절대 매치 안 됨 — 결과적으로 모든 voter 가 follower 로 표시. 운영자는 별도로 `cluster status` 의 `leader_id` 행을 봐야 누가 leader 인지 확인 가능. PR-D 의 nodeID 통일 후 매치되도록 자연스레 풀림. **임시 운영 가이드 보완 필요:** README/RUNBOOK 에 "현재 cluster peers 의 ROLE 컬럼은 PR-D 미머지 상태에서 leader 자동 인식이 동작하지 않음 — `cluster status --format text` 로 leader_id 별도 확인" 캡션 추가 검토.

- [ ] **cmd-thin refactor — `serve.go`/`volume.go`/`resource_monitors.go` 등 큰 cmd 파일도 동일 패턴으로 정리** — `cluster-membership-cli` 라운드에서 cluster CLI 3개를 cmd-는-flag-파싱-만 / 비즈니스-로직-은-`internal/clusteradmin` 으로 분리하는 패턴을 확립함 (`refactor(cluster-cli): orchestration into clusteradmin package, cmd thinned` 커밋 참조). 동일 처리가 필요한 큰 cmd 파일들: `serve.go` (2255줄, `runCluster` 단일 함수가 1300줄), `volume.go` (782줄), `resource_monitors.go` (655줄), `cluster_rotate_key.go` (172줄), `recover.go` (182줄) 등. **별도 브랜치 (`cmd-thin-refactor`) 필수** — 회귀 영향이 모든 e2e 에 걸려 있고 패키지 경계 결정 (`internal/runtime` vs `internal/serve` vs domain-별 분리?) 자체가 grilling 한 라운드 사이즈. **선행 작업:** (a) serve.go 에 인라인된 adapter/helper 들이 어떤 도메인에 속하는지 분류, (b) wiring 라이프사이클 (start/stop ordering, ctx cancel propagation) 을 깨지 않는 패키지 경계 결정. **Re-open trigger:** cluster CLI refactor 결과를 본 뒤 이 패턴이 안정 검증되면 다음 라운드로 들어감.
