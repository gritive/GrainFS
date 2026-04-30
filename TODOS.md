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
- [ ] **Multi-Raft (Joint consensus 설계 완료)** — 5 PR 시퀀스:
  - ✅ PR-A: §4.4 one-at-a-time membership change (v0.0.6.11 snapshot membership 까지 완결)
  - ✅ PR-B: meta-Raft scaffold (`MetaRaft`, `MetaFSM`, shard_map, load_snapshot)
  - ✅ PR-C: 데이터 그룹 다중화 + bucket→group Router (`DataGroup.Backend`, `Router.AssignBucket` COW)
  - ✅ PR-D: autonomous rebalance (v0.0.6.6 `DataGroupPlanExecutor` + Full Sharding E2E)
  - [ ] **PR-E: Multi-Raft scale micro-bench** (현재 진행 중 — 설계: `docs/superpowers/specs/2026-04-30-multi-raft-scale-microbench-design.md`)
    - in-process N-group raft 하네스 (5 호스트, 그룹별 3 voter)
    - sweep N=8/32/64/128 (호스트당 raft Node 수: 5/19/38/77 — N=128이 운영 목표 256×10host 와 동일 부하)
    - 측정: heap, CPU idle %, heartbeat msg/sec, election count, goroutines
    - 산출: `internal/cluster/scale_bench_test.go` + design doc 결과 표 + CHANGELOG
    - **out of scope**: k6 throughput, Docker Compose 다중 노드, BadgerDB I/O, 실 EC 부하 (운영 단계로 이연)
  - 설계: `~/.gstack/projects/gritive-GrainFS/whitekid-joint-consensus-design-20260429.md`
  - PR-F (트리거 시): §4.3 joint consensus atomic multi-server replacement
  - PR-D 잔여 must-fix (PR-E 본 작업과 무관, 별도 follow-up):
    - M2: MetaAbortPlanCmd reason:uint8 추가
- [ ] Raft leader 부하 분산 검토 (follower proxy, read-only query, lease read 등)
- [ ] **raft-ehn Tier 2** (raft-ehn 범위 밖, 트리거 조건 도달 시 별도 design):
  - ReadIndex (현재 `IsLeader()` 보증으로 충분; FSM linearizable read 요구 시)
  - Public `Snapshot()` trigger API (운영 도구)
  - BatchingFSM (FSM apply throughput 한계 도달 시)
  - Snapshot chunking + Concurrent snapshotting (FSM이 QUIC stream max 근접 시)
  - Per-PR Prometheus dashboard 갱신 PR
- [ ] **raft-ehn Tier 3** (별도 브랜치, 각 sub-project는 독립 design + plan + worktree):
  - **Tier 3-1: Joint consensus + Learner** — Multi-Raft atomic move-replica · multi-server replacement 안전성. wire format `JointConfChange=2` + `new_config:[string]` / `old_config:[string]` 슬롯 reserved (PR-A에서). 세 sub-project로 분해:
    - **Sub-project 1: Learner-first 정책** — 새 voter 추가 시 자동 learner → catch-up 감지 → promote. 의존성 없음. Joint consensus의 안전망. 4 PR (~880 LOC). **진행 중** (worktree `feat-raft-joint-consensus`). plan-eng-review + cross-model outside voice 통과 (Claude + Gemini, REVISE→commit-time close 채택, RETHINK→PR-E refactor 합류).
      - 후속 검토: `AddVoterDirect` skip mode (recovery/migration 운영 도구로 필요 시); Learner stuck watchdog auto-remove (현재는 caller ctx timeout 채택); 추가 chaos scenarios (slow learner, repeated leader change during catch-up).
    - **Sub-project 2: Joint consensus core** — §4.3 wire activation, 4-state machine (normal → C_old+new → C_new), dual-quorum, ConfChange entry encoding. 큼 (~6-8 PR). Sub-project 1 완료 후.
    - **Sub-project 3: Multi-server replacement API** — `ChangeMembership(add, remove)` public, PR-E `DataGroupPlanExecutor`를 joint으로 전환. 중간 (~2-3 PR). Sub-project 2 의존.
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

**완료**:
- [x] `internal/storage/errors.go` — sentinel errors (`ErrECDegraded`, `ErrNoSpace`, `ErrQuotaExceeded`, `ErrInvalidVersion`)
- [x] `internal/vfs/vfs.go` — `grainFile.ReadAt` 동시성 안전 (`mu sync.Mutex` + `rc`/`pos` 보호) — io.ReaderAt 계약 준수, S3 GetObject 병렬 range read 지원
- [x] `tests/fuse_s3_colima/` — Colima Linux VM에서 rclone mount로 macOS 호스트 GrainFS S3 endpoint 마운트 후 smoke / directories / rename / cross-protocol round-trip 검증 (`make test-fuse-s3-colima`)
- [x] `tests/fuse_s3_colima/bench_test.go` — FUSE mount vs direct S3 처리량 벤치 (`make bench-fuse-s3-colima`). 64 MiB / Apple M3 / loopback / `--vfs-cache-mode off` / 3회 평균: Direct PUT 96.8 MB/s · Direct GET 108.0 MB/s · FUSE Write 106.7 MB/s · FUSE Read 107.3 MB/s — **FUSE 오버헤드 ≈ 0%**
- [x] README "FUSE-over-S3 마운트" 섹션 — 마운트 가이드 + 지원/미지원 연산표 + 처리량 결과

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
