# TODOS

> **Design principle — Zero Config, Zero Ops.** 기본 설정으로 잘 돌아가고, 잘 운영되며,
> 크리티컬한 문제는 사용자에게 알려서 선제대응하게 만든다.
> 각 Phase 항목에 "— *zero config*" / "— *zero ops*" 표시가 있는 것들이 이 원칙에 해당.

### 기타

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

- [ ] **P0: multi-raft restart recovery e2e timeout** — `/ship` on `feature/fd-predictive-warning` found `tests/e2e.TestE2E_MultiRaftSharding_RestartRecovery` timing out after 10m while waiting for restart-recovery test ports. This branch does not modify `tests/e2e/multiraft_sharding_test.go` or the multiraft startup/recovery path; investigate host contention, leaked e2e servers, or restart port readiness separately.
- [ ] go-billy: Direct File I/O; O_DIRECT
- [ ] **EC shard cache 사이즈 튜닝** — 본구현 완료 v0.0.4.42 (E2E 85.7% hit). 운영 telemetry(`grainfs_ec_shard_cache_hit_rate`)로 working set 측정 후 default 256 MB 적정성 검증. 큰 객체 백업 워크로드면 GB 단위까지, 작은 객체 위주면 비활성화 권장.
- [ ] io_uring
- [ ] SPDK
- [ ] SoA (Structure of Arrays)
- [ ] SIMD
- [ ] **Predictive resource warnings — BadgerDB / goroutine** — *zero ops* — BadgerDB value log 크기와 goroutine 수 추세를 추적하고 임계 도달 전 경고. 디스크/FD 사용률 경고와 동일 패턴(transition-only firing).
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
- [ ] **`volume scrub` 명령 (Phase B 에서 분리됨)** — 별도 spec 필요. 근본 원인: `__grainfs_volumes` 가 internal bucket 이라 `internal/cluster/backend.go:821` 에서 `useEC=false` 강제 → volume 블록은 EC 가 아니라 N×replication 으로 저장 → `lat:` 인덱스 미등록 → 기존 `BackgroundScrubber` 가 스캔 못함. **별도 설계 방향:** Manager 측 `Scrub(name)` 직접 구현 — live_map 순회 + 블록 GetObject + checksum 검증, 손상 시 `cluster.DistributedBackend` NX repair 경로 호출. cluster-wide trigger 는 meta-raft `ScrubTrigger` entry 패턴 (원래 spec A4=C 결정 재사용 가능).
- [ ] **incident store scope index** — `internal/incident/badgerstore/store.go` 의 `List(ctx, limit)` 외에 `ListByScope(ctx, kind, id, limit)` 추가. volume scrub follow-up 에서 status 응답이 v1 에 `List(500) + 메모리 필터` 로 시작. 운영에서 incident 누적이 수만건+ 가 되어 status 응답이 100ms 초과하면 secondary index(`scope:<kind>:<id>:<-ts>:<id>`) 도입.
- [ ] **`ScanObjects(bucket, keyPrefix)` 로 시그니처 확장** — `internal/cluster/scrubbable.go` 의 `ScanObjects` 가 `lat:` 인덱스 전체를 iterate. volume scrub follow-up 에서 BackgroundScrubber 를 사용하는 경로가 생기면 prefix-bounded scan 으로 비용 감소.
- [ ] **`RepairReplica` 단위 테스트** — `internal/cluster/repair_replica.go` 는 v0.0.43 volume scrub PR 에서 통합 테스트 (3-node `scrubber_volume_integration_test.go`) 만 커버. 단위 테스트는 `*ShardService` 가 concrete 라 mock 불가능 — backend 의 shardSvc 를 작은 인터페이스 (`peerReader { ReadShard(...) }`) 로 추출하면 white-box 테스트 가능. 회귀 보호 레벨 향상 + 통합 테스트 시간 절감 효과.
- [ ] **EC scrub 을 `BlockSource` 인터페이스로 마이그레이션** — v0.0.43 volume scrub PR 에서 `BackgroundScrubber` 의 EC `runOnce` 경로는 변경 안 함 — Volume 만 신규 `BlockSource` 인터페이스 사용. EC 도 동일 인터페이스로 마이그레이션하면 (a) source registration API 통일 (b) Director.Trigger 가 EC 도 라우팅 가능 (현재는 volume 만 trigger 가능). 운영 데이터 후 우선순위 조정.

## Storage Hashing 성능 검토

- [ ] **MD5 hot-path 비용 측정 + faster hash 도입 검토** — *zero ops 무결성 oracle* — v0.0.43 volume scrub PR 에서 `LocalBackend.PutObject` / `cluster/spool.go shouldHashBucket` / `LocalBackend.WriteAt` 의 MD5 skip 정책을 제거 (모든 internal bucket 도 hash 계산). 이유: hash 가 없으면 disk bit-rot, 부분 write, 번역 오류를 영구히 검출 못 함 → scrub oracle 자체가 사라짐. **Concern:** 1MB+ 큰 write 가 NVMe 에 가는 hot path 에서는 MD5 (~2 GB/s 단일 코어) 가 disk IO 보다 비싼 비중을 차지할 수 있다 (예: 1MB / NVMe 250µs vs MD5 ~2ms). **검토 항목:** (a) k6 워크로드로 MD5 비용 실측 (S3 PUT throughput 회귀 측정), (b) BLAKE3 (~6.5 GB/s) / xxhash3 (~30 GB/s, non-cryptographic 이지만 corruption detection 충분) 교체 비교, (c) S3 ETag 호환 (외부 client mc/aws-cli 의 multipart 검증) 영향 평가, (d) hash 계산을 별도 goroutine 으로 옮겨 disk write 와 병렬화 (io.MultiWriter 대체) — 어느 방향이든 PR 별도. **재오픈 트리거:** S3 PUT throughput regression 가 측정값으로 확인되거나, NFS WRITE p99 가 SLO 침범 시.

- [ ] **Hash 정책 분기 가드 — bucket-class 단위로 끄지 말 것** — 과거 `IsInternalBucket(bucket)` 만으로 MD5 를 끈 정책이 volume scrub oracle 을 제거했다 (위 항목 참고). 이유: bucket-class 는 무결성 요구를 표현하지 못함 — `__grainfs_volumes` 와 `__grainfs_nfs4` 둘 다 internal 이지만 둘 다 외부 별도 보호가 없어 hash 가 필수. 만약 미래에 hash 비용이 실제로 hot path 를 압박해서 정책 분기가 필요해지면, **bucket prefix 가 아니라 무결성-요구 등급** (durable replica / EC parity / ephemeral cache) 으로 가르고, 각 등급별로 (a) 별도 protection 이 있는지 (b) detect-only vs detect+repair 정책이 무엇인지 명문화. "internal 이니까 끄자" 같은 한 줄 최적화는 영구 oracle 손실로 이어짐.

- [ ] **`__grainfs_volumes` 의 EC 강제 차단 재검토** — *redundancy 효율 / replication-vs-EC tradeoff* — `internal/cluster/backend.go:821` 가 `IsInternalBucket(bucket) && vfsFixedVersion` 일 때 `useEC=false`. 명시 이유는 "고정 versionID + EC RingVersion-keyed shard placement 충돌 → 링 토폴로지 변경 시 stale shard leak". **하지만 `__grainfs_volumes` 는 고정 versionID 안 씀** (`newVersionID()` 사용). 그런데도 같은 분기에 잡혀 EC 차단됨 → volume 블록은 N×replication 으로 저장. **올바른 분기는** `IsVFSBucket(bucket) && vfsFixedVersion` 또는 `bucket != volumeBucketName && IsInternalBucket && vfsFixedVersion`. 차단을 좁히면 volume 블록이 EC 4+2 로 저장 → 디스크 footprint 1.5x 로 N×replication (3x) 대비 효율. **이건 정합성 문제 해결이 아니라 redundancy 효율 개선** — 정합성 oracle 은 v0.0.43 의 MD5 ETag 복구로 이미 해결됨. **선행 조건:** (a) 기존 replication 으로 저장된 volume 블록 데이터의 EC 마이그레이션 전략 (read-time backfill 또는 명시 마이그레이션 명령), (b) 아래 EC silent corruption audit 로 EC scrub 경로의 oracle 충분성 검증, (c) volume scrub 의 BlockSource/BlockVerifier 가 per-block 으로 EC vs replication 분기하거나 (placement record 보고 EC 면 yield 안 함) EC migration 완료 후 deprecate. → 별도 design 권장.

- [ ] **EC scrub 의 silent shard corruption 검출 audit** — *정합성 oracle gap 가능성* — `internal/scrubber/scrubber.go` 의 EC `runOnce` 가 shard 존재 여부만 확인하는지, shard 자체의 checksum 도 검증하는지 audit 필요. Reed-Solomon 은 erasure code 라 shard 가 "있다" 면 그 bytes 를 신뢰함 → shard 안 silent bit-flip 이 reconstruction 시 그대로 객체에 들어감. 만약 EC scrub 이 shard checksum 을 보지 않으면 GrainFS 전체에 oracle gap 존재 (EC 데이터의 silent corruption 이 영구히 묻힘). 점검 항목: (a) shard 저장 시 별도 checksum 이 metadata 에 있는지, (b) EC scrub 이 shard 를 fetch 해서 비교하는지 (cost 큼) 또는 sample-based audit 인지, (c) reconstruction 시 cross-shard consistency 가 검증되는지. 만약 gap 이면 별도 design — shard 단위 ETag 또는 객체 단위 reconstruction-then-MD5 oracle 도입.

- [ ] **Live-vs-full scrub scope re-introduction** — v0.0.43 Z2 단순화에서 `BackgroundScrubber` 의 `SetVolumeFullInterval` 와 dual-ticker (live/full 분리) 를 제거했음. 이유: `ReplicationObjectSource` 가 generic 이라 ScopeLive 와 ScopeFull 이 동일 walk 로 수렴. 미래에 source 별로 cheap-live (e.g. volume live_map 우선) 와 expensive-full 을 다시 구분하고 싶으면 (a) `BlockSource.Iter` 에서 ScopeLive 가 작은 keyset 만 yield 하도록 source-side filter 추가, (b) `BackgroundScrubber` 에 두 ticker 다시 도입 + serve flag (`--scrub-full-interval`, `--scrub-full-disable`) 노출. plan Task 12 의 의도. 운영에서 live walk vs full walk 의 비용 차이가 측정값으로 분명해지면 재오픈.

- [ ] **Volume scrub cluster-broadcast — MetaFSM ScrubTrigger entries** — v0.0.43 PR 에서 plan 의 Task 8 (MetaFSM `ScrubTriggerEntry` / `ScrubSessionDoneEntry` / `ScrubCancelEntry`) 은 deferred. 이유: 코드베이스의 meta-FSM 은 FlatBuffers schema (`clusterpb.MetaCmd`) 기반이고 plan 은 gob 패턴 가정 — schema 추가 + flatc 재생성이 별도 surface. v0.0.43 은 admin handler 가 single-node 처리 (각 노드에 직접 admin call) 로 cluster-wide trigger 를 우회. 후속에서 (a) `clusterpb.MetaCmdTypeScrubTrigger` enum 추가 (b) `clusterpb.MetaScrubTriggerCmd` flatbuffer 테이블 추가 (c) `applyScrubTrigger` 가 `Director.ApplyFromFSM` 호출하도록 wire 후, 단일 진입점으로 전 노드 동시 트리거 가능. Done/Cancel 도 동일 패턴.
