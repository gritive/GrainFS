# TODOS

> **Design principle — Zero Config, Zero Ops.** 기본 설정으로 잘 돌아가고, 잘 운영되며,
> 크리티컬한 문제는 사용자에게 알려서 선제대응하게 만든다.
> 각 Phase 항목에 "— *zero config*" / "— *zero ops*" 표시가 있는 것들이 이 원칙에 해당.

### 기타

- [ ] **Thin pool quota (cross-volume)** — 여러 볼륨이 공유하는 물리 용량 예산 풀. 볼륨별 `PoolQuota` 옵션(Phase A)보다 정교한 전체 클러스터 수준 quota 관리. Phase A 완료 이후.
- [ ] Memory usage validation
- [ ] Erasure Coding을 활용한 Bit Rot 방지
- [ ] **단일 블롭 손상 격리** — *zero ops* — 손상된 블롭 객체만 격리해 read-only로 표시; 동일 볼륨의 다른 객체는 정상 서비스 유지.

## Phase 17: Scale-Out

- [ ] **BadgerDB atomic auto-recovery** — 이전 Phase 16에서 이연. log-based replay + snapshot restore 자체 구현 (단순 `badger.Open` 내장 복구를 넘어서는 원자적 복구 레이어)
- [ ] **Blame Mode v2 — shard-level 시각적 replay** — Phase 16은 텍스트 타임라인 + JSON download만, v2에서 shard 재생 UI
- [ ] **PagerDuty 네이티브 webhook 매핑** — Phase 16은 Slack-compatible JSON + docs 매핑만
- **Phase 17 — Multi-Raft Data Plane 라우팅 완료 (v0.0.7.0 #117 ~ v0.0.7.1 #123)**
  - PR-A: ShardGroupAssignment + FSM (v0.0.7.0 #117) ✅
  - PR-B: Per-Group Raft + BadgerDB (v0.0.7.0 #118) ✅
  - PR-C: Router + Coordinator scaffold (v0.0.7.0 #119) ✅
  - PR-D 후속: ForwardReceiver + serve.go wiring + e2e tests (v0.0.7.1 #123) ✅
  - PR-D 후속: Test coverage enhancements (integration tests + self-removal retry) ✅

- [ ] **PR-F**: §4.3 joint consensus atomic multi-server replacement (Tier 3-1 Sub-project 3에서 다룸)
- [ ] Raft leader 부하 분산 검토 (follower proxy, read-only query, lease read 등)
- [ ] **raft-ehn Tier 2** (raft-ehn 범위 밖, 트리거 조건 도달 시 별도 design):
  - ReadIndex (현재 `IsLeader()` 보증으로 충분; FSM linearizable read 요구 시)
  - Public `Snapshot()` trigger API (운영 도구)
  - BatchingFSM (FSM apply throughput 한계 도달 시)
  - Snapshot chunking + Concurrent snapshotting (FSM이 QUIC stream max 근접 시)
  - Per-PR Prometheus dashboard 갱신 PR
- [ ] **raft-ehn Tier 3** (별도 브랜치, 각 sub-project는 독립 design + plan + worktree):
  - **Tier 3-1: Joint consensus + Learner** — Multi-Raft atomic move-replica · multi-server replacement 안전성. Sub-project 1 (Learner-first, v0.0.6.13 #105 + chaos #107) + Sub-project 2 (Joint core, v0.0.6.16~v0.0.6.20 #108~#112) 완료. 남은 작업:
    - **Sub-project 3 완료**: PR-K1 (v0.0.6.22 #114) · PR-K2 (v0.0.6.23) · PR-K3 (v0.0.7.1 #117 managed_by_joint persistence) — 모두 master 머지 완료.
    - **후속: Joint phase observation API** — process restart 시 caller lost 위험 (cross-model 발견 Codex #1). Sub-project 3 ChangeMembership 공개 시 idempotent reattach + `Configuration()`/`JointPhase()` query API 노출 검토.
    - **후속: Stuck joint abort 메커니즘** — v0.0.7.2 #118 완료. `JointOpAbort` + `ForceAbortJoint` API 구현; multi-cycle jAborted reset + wg 누수 수정 포함.
    - **후속: Voter set lock-free read** — `n.mu` hold 안 voter set read를 `atomic.Pointer[voterSets]` COW swap으로 분리. raft.go 전반 multi-field invariant 안에서 voter set만 분리하려면 design 필요.
    - **후속: Joint chaos scenarios + E2E snapshot mid-joint** — Sub-project 3 `ChangeMembership` public API 노출 후 chaos scenarios 3개 (LeaderCrashBetweenEnterAndLeave / PartitionDuringJoint / RepeatedLeaderChange) + `TestJoint_E2E_SnapshotMidJoint_AutoCompletes` 작성.
  - **Tier 3-2: RecoverCluster** — 단일 노드 재해 복구 운영 도구
  - **Tier 3-3: 클라이언트 dedup** — ClientID + RequestID 기반 dedup table, S3 SDK retry 시 중복 PUT 방지
  - **Tier 3-4: AE pipelining** — in-flight AppendEntries 1 → N (replication throughput)
- [ ] **§2.x 잔여 항목** (필요 시 design doc 재발굴) — 4-30 §2.3 snapshot servers persistence(#103 v0.0.6.11) 동일 series의 다른 violation 항목이 있었는지 다음 brainstorming session에서 확인.
- [ ] Migration: NFS virtual overlay
- [ ] Migration: NBD block proxying
- [ ] nbd over internet for edge computing (powered by wireguard)
- [ ] **Rolling upgrade safety** — *zero ops* — 버전 간 binary 교체로 downtime/데이터 손실 없음 (schema migration 자동, snapshot forward-compat 보장)

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
- [ ] **Predictive resource warnings — BadgerDB / goroutine / FD** — *zero ops* — BadgerDB value log 크기, goroutine 수, open FD 추세를 추적하고 임계 도달 전 경고. 디스크 사용률 경고와 동일 패턴(transition-only firing).
- [ ] control plane, data plane 분리

## Phase 20: Protocol Extensions

- [ ] Redis 프로토콜 지원 (RESP, Streaming, Pub/Sub 이벤트)
- [ ] TSDB (Time Series DB) — Metric 저장 및 쿼리 지원

## Phase 20: Operations & Onboarding

운영자 개입 없이도 안정적으로 동작하고, 문제 발생 시 명확하게 알려주는 기본기.

- [ ] **One-command bootstrap** — *zero config* — `grainfs init` 하나로 cluster key, encryption key, 기본 credential, volume 생성 + 필요 파일 권한 설정
- [ ] **Hot reload drift detection** — *zero ops* — config 파일 시스템 도입 후, 런타임 reload 시 디스크 config와 메모리 상태 불일치 감지 + 명확한 에러. config 파일 시스템 자체가 선행 조건.
