# TODOS

> **Design principle — Zero Config, Zero Ops.** 기본 설정으로 잘 돌아가고, 잘 운영되며,
> 크리티컬한 문제는 사용자에게 알려서 선제대응하게 만든다.
> 각 Phase 항목에 "— *zero config*" / "— *zero ops*" 표시가 있는 것들이 이 원칙에 해당.

### 기타

- [ ] **Thin pool quota (cross-volume)** — 여러 볼륨이 공유하는 물리 용량 예산 풀. 볼륨별 `PoolQuota` 옵션(Phase A)보다 정교한 전체 클러스터 수준 quota 관리. Phase A 완료 이후.
- [ ] Memory usage validation
- [ ] **단일 블롭 손상 격리** — *zero ops* — 손상된 블롭 객체만 격리해 read-only로 표시; 동일 볼륨의 다른 객체는 정상 서비스 유지.

## Phase 17: Scale-Out

- [ ] **BadgerDB atomic auto-recovery** — 이전 Phase 16에서 이연. log-based replay + snapshot restore 자체 구현 (단순 `badger.Open` 내장 복구를 넘어서는 원자적 복구 레이어)
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

- [ ] **P0: auto-snapshot e2e pre-existing failure** — `/ship` on `perf/nfs4-bottleneck` found `tests/e2e.TestAutoSnapshot_CreatesSnapshotAutomatically` failing with `list objects: forward: no reachable peer` and zero snapshots after 1.5s. Reproduced on `origin/master` (`7092bb1`) with the same focused test, so it is not caused by the NFSv4 allocation branch. Fix snapshot startup/routing readiness or adjust the test to wait for data-group leadership before snapshot assertions.
- [ ] go-billy: Direct File I/O; O_DIRECT
- [ ] **EC shard cache 사이즈 튜닝** — 본구현 완료 v0.0.4.42 (E2E 85.7% hit). 운영 telemetry(`grainfs_ec_shard_cache_hit_rate`)로 working set 측정 후 default 256 MB 적정성 검증. 큰 객체 백업 워크로드면 GB 단위까지, 작은 객체 위주면 비활성화 권장.
- [ ] io_uring
- [ ] SPDK
- [ ] SoA (Structure of Arrays)
- [ ] SIMD
- [ ] **Predictive resource warnings — BadgerDB / goroutine / FD** — *zero ops* — BadgerDB value log 크기, goroutine 수, open FD 추세를 추적하고 임계 도달 전 경고. 디스크 사용률 경고와 동일 패턴(transition-only firing).
- [ ] **BadgerDB 인스턴스 통합 (P3 — FSM state DB)** — raft-log 통합(P0b)은 v0.0.13.0 출시됨 (`OpenSharedLogStore` + `--shared-badger` 기본 활성). idle-N8 측정에서 goroutines -16%, heap -19%, RSS -25% 확인. 남은 작업: FSM state DB도 노드당 1개로 통합 (P3). 현재 설계 검토에서 13개 이슈 발견 후 일시 정지 (live snapshot Restore가 FSM 우회, DropPrefix DB-wide stall, 추정 11-14일). docs/architecture/badger-consolidation.md 참고. **상태: PAUSED.** R+H (QUIC stream-reuse) 효과로 idle CPU 70%→3.5% 달성, P3 시급도 낮아짐. 재오픈 조건: (a) FSM state badger가 새 핫스팟으로 떠오르면 (b) 13개 이슈 mitigation 명확해지면. 그 외에는 v0.1.x 시점에 close.
- [ ] **R+H 측정 잔여** — load-N8 / load-N16 mux=on 깨끗한 측정. e2e bucket-replication race + macOS host contention 임계 해결 후. pool size sweep (1/2/4/8)로 RSS +74% 영향 평가 후 default 재조정.
- [ ] **Meta-mux post-deploy 측정** — v0.0.19.0 (#141)로 meta-raft mux 통합 shipped. R+H load-N8 clean baseline 후 meta-mux on/off A/B 측정으로 plan에서 추정한 ~4% 트래픽 감소 실측. 작아서 noise에 묻힐 가능성, host stable 환경 필수. 결과를 `docs/architecture/quic-stream-multiplex.md` §Follow-up에 DELIVERED 헤더로 기록.
- [ ] **e2e bucket-replication race fix (P0 — perf 측정 블로커)** — `tests/e2e/cluster_perf_profile_test.go:232` load-N16/N32에서 "no such bucket" 가끔 발생. R+H 후속 perf 측정 + meta-mux post-deploy 측정 모두 이 race 안정화 후 진행 가능. 재현/근본원인 분석 + 격리 fix 필요.
- [ ] control plane, data plane 분리

## Phase 20: Protocol Extensions

- [ ] Redis 프로토콜 지원 (RESP, Streaming, Pub/Sub 이벤트)
- [ ] TSDB (Time Series DB) — Metric 저장 및 쿼리 지원

## Phase 20: Operations & Onboarding

운영자 개입 없이도 안정적으로 동작하고, 문제 발생 시 명확하게 알려주는 기본기.

- [ ] **One-command bootstrap** — *zero config* — `grainfs init` 하나로 cluster key, encryption key, 기본 credential, volume 생성 + 필요 파일 권한 설정
- [ ] **Hot reload drift detection** — *zero ops* — config 파일 시스템 도입 후, 런타임 reload 시 디스크 config와 메모리 상태 불일치 감지 + 명확한 에러. config 파일 시스템 자체가 선행 조건.
