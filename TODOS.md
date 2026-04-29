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
  - PR-A: §4.4 one-at-a-time membership change 정확 구현 (AddVoter/RemoveVoter/AddLearner/PromoteToVoter, FlatBuffers ConfChangeEntry)
  - PR-B: meta-Raft scaffold (클러스터 사실의 소스, shard_map, load_snapshot)
  - PR-C: 데이터 그룹 다중화 + bucket→group Router
  - PR-D: autonomous rebalance (10s tick + meta-Raft leader 평가 + RebalancePlan) — 플랜 리뷰 + 교차검증 완료, must-fix 확인됨:
    - M1: tickOnce resume 분기 (leader restart) + executingPlan atomic.Pointer + 리더십 이탈 시 cancel
    - M2: MetaAbortPlanCmd reason:uint8 (Should-fix — PR-E로 이연 가능)
    - ~~M3: 기각~~ — serve.go에서 이미 seed, broadcastOnce() 수정 불필요
    - M4: CreateVectorOfSortedTables → StartEntriesVector 패턴 교체 (Snapshot() line도 포함)
    - M5: applyAbortPlan() idempotent guard — planID 불일치 시 no-op (에러 반환 금지)
    - T1-T3: restart-with-active-plan, ExecutePlan failure, Stop+goroutine leak 테스트
    - P1: EvalInterval 30s + zero/stale 노드 제외 (~177 MB/day → ~44 MB/day)
    - A1: Snapshot() FlatBuffers 빌드 순서 — lsVec을 Start 이전 완성
    - A2: SetOnRebalancePlan callback doc "must not block" 추가
    - A3: make fbs 선행 후 Entries() 시그니처 확인 후 decode 코드 작성
  - PR-E: EC + Multi-Raft 통합 마무리 + k6 256 group 벤치
  - 설계: `~/.gstack/projects/gritive-GrainFS/whitekid-joint-consensus-design-20260429.md`
- [ ] Raft leader 부하 분산 검토 (follower proxy, read-only query, lease read 등)
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
