# TODOS

> **Design principle — Zero Config, Zero Ops.** 기본 설정으로 잘 돌아가고, 잘 운영되며,
> 크리티컬한 문제는 사용자에게 알려서 선제대응하게 만든다.
> 각 Phase 항목에 "— *zero config*" / "— *zero ops*" 표시가 있는 것들이 이 원칙에 해당.

## Phase 16: Advanced Storage

### Phase 16 Week 5 — Heal Receipt + OTel (설계 완료, 미진행)

설계: `~/.gstack/projects/gritive-grains/whitekid-master-design-20260420-182422.md`

- [ ] **Slice 3: Scrubber wiring + Blame Mode v1 UI** — repair 세션 → receipt emit, dashboard timeline + JSON download
- [ ] **Slice 4: OTel spans** — head-based 1% sampling, `--otel-endpoint`, `--otel-sample-rate`, scrubber phase별 span

### Phase 16 Week 6 — Grafana Bundle + Demo (설계 완료, 미진행)

- [ ] `deploy/grafana/self-healing.json` + `//go:embed` + `GET /dashboard/grafana.json`
- [ ] `docs/grafana-quickstart.md`
- [ ] `scripts/demo/pull-the-disk.sh` 90초 3막 데모 + README GIF embed
- [ ] README top section: "See it heal. See it prove it."
- [ ] CI: `grafana-cli plugin validate-dashboard` 호환성 가드

### 기타

- [ ] Thin provisioning
- [ ] NBD Copy on Write in storage layer (thin provisioning 이후)
- [ ] Reference counting for shared blocks
- [ ] CoW E2E tests
- [ ] Memory usage validation
- [ ] Erasure Coding을 활용한 Bit Rot 방지
- [ ] io.WriteTo 구현 (FlatBuffers zero-copy)
- [ ] nbd server가 굳이 linux만 컴파일 될 필요는 없잖아. 클라이언트만 리눅스 제한이지.
- [ ] **Degraded mode (storage)** — *zero ops* — EC backend 실패 → read-only + critical alert; 단일 블롭 손상 → 해당 객체만 격리

## Phase 17: Scale-Out

- [ ] **Lock-free 아키텍처 전사 검토** — *성능·확장성 개선, Phase 16/17 한정 아님*
  프로젝트 전체에서 `sync.Mutex`/`sync.RWMutex`를 쓰는 핫패스를 전수 조사하고, 각 상황에 맞는 lock-free 기법을 선별 적용.
  **조사 범위**:
  - Phase 16 receipt: `RoutingCache` (read-heavy gossip lookup), `Store.drainMu` (writer serialization), `KeyStore` rotation
  - 기존 스토리지: `internal/scrubber/` 상태 머신, `internal/cluster/NodeStatsStore`, `internal/metadata/` BadgerDB 캐시, `internal/cluster/balancer.go` inflight 슬롯
  - 트랜스포트: QUICTransport connection map, StreamRouter handlers
  **후보 기법 리서치 필요**:
  - **CAS (compare-and-swap)** — 단순 카운터/플래그, writer 1인 + reader N
  - **Actor model** — 단일 goroutine 소유 state + channel 메시지 (예: scrubber 세션 매니저)
  - **LMAX Disruptor** — 고처리량 단일 생산자→단일 소비자 링 버퍼 (예: HealEvent burst)
  - **RCU (Read-Copy-Update)** — read 0 lock, writer는 grace period 후 해제 (리눅스 커널 패턴, Go는 `atomic.Pointer` 기반 구현)
  - **Copy-on-Write + `atomic.Pointer`** — read-heavy config/라우팅 테이블 (Phase 16 RoutingCache 후보)
  - **Wait-free 자료구조** — 절대 block 없음 보장 (SPSC 큐, seqlock)
  **매칭 전략**: 각 자리마다 접근 패턴(1:N, N:1, N:N, 쓰기 빈도) 측정 후 가장 경량인 기법 선택. 일률적 `sync.Map` 치환 금지 — 패턴 불일치 시 오히려 느려진다.
  **트리거 조건**: (a) Phase 17 스케일아웃 전에 벤치마크 수행해 각 lock의 p99 contention 측정, (b) 벤치마크 위에서 bottleneck ≥1ms 확인된 자리부터 순차 치환, (c) 각 치환은 race detector + long-run integration test로 검증.
- [ ] **BadgerDB atomic auto-recovery** — 이전 Phase 16에서 이연. log-based replay + snapshot restore 자체 구현 (단순 `badger.Open` 내장 복구를 넘어서는 원자적 복구 레이어)
- [ ] **Blame Mode v2 — shard-level 시각적 replay** — Phase 16은 텍스트 타임라인 + JSON download만, v2에서 shard 재생 UI
- [ ] **PagerDuty 네이티브 webhook 매핑** — Phase 16은 Slack-compatible JSON + docs 매핑만
- [ ] Sharding, multi raft
- [ ] Raft leader 부하 분산 검토 (follower proxy, read-only query, lease read 등)
- [ ] Migration: NFS virtual overlay
- [ ] Migration: NBD block proxying
- [ ] nbd over internet for edge computing (powered by wireguard)
- [ ] **Rolling upgrade safety** — *zero ops* — 버전 간 binary 교체로 downtime/데이터 손실 없음 (schema migration 자동, snapshot forward-compat 보장)
- [ ] **Raft quorum lost alert** — *zero ops* — critical alert channel로 즉시 경고; 자동 re-election 시도 로직

## Phase 18: Cluster EC

설계: `~/.gstack/projects/gritive-grains/whitekid-master-design-20260421-024627.md` (Office-hours 2026-04-21)

**동기**: 현재 cluster 모드는 N× full-replication (모든 피어에 전체 객체 복제)이며 solo 모드 EC와 스토리지 모델이 비대칭. `ReplicationMonitor`는 dead code, balancer-triggered migration은 runtime 불일치로 실패. "Zero-ops cluster EC" 포지셔닝 회복이 목표.

**Stage 3 진입 선결 과제** (Stage 0-2 완료, CONDITIONAL GO p95 53.5ms PASS):
- [ ] **Placement map 설계 확정** — Consistent Hashing vs Raft FSM map 선택 → Stage 3 WBS Week 1~2 범위 결정. `/plan-eng-review` Phase 18 상세 리뷰 시 해결.

**Stage 3 선결 과제** (Phase 18 풀 구현 시작 전):
- [ ] Raft FSM v1→v2 변환 전략 + 롤백 경로 설계
- [ ] Min-node=6 → `k+m` 파라미터화
- [ ] N×→EC 백그라운드 re-placement의 concurrent PUT/GET 일관성 계약
- [ ] HealReceipt 스키마 변경 범위 분석
- [ ] Write-all vs write-majority tail latency 트레이드오프 재검토

### v0.0.4.0 follow-up

- [ ] **NFS/VFS path 충돌 해결** — `objectPath(bucket, key)` (unversioned file) vs `objectPathV(bucket, key, vid)` (`key/.v/vid` 디렉터리) 경로가 충돌하여 `TestNFS_MountAndWriteReadFile`/`TestNFS_MultipleFiles` 실패. Key/version path 스킴 재설계 (예: `data/bucket/.objects/{key}/{vid}` 플랫 구조) 또는 NFS 레이어에서 별도 bucket namespace 사용.
- [ ] **At-rest encryption 복구** — 이전 ECBackend의 AES-256-GCM 경로가 삭제됨. `ShardService.WriteLocalShard`/`ReadLocalShard`에 encryption wrapper 추가, `--encryption-key-file`/`--no-encryption` 플래그 실제 사용 회로 복구.
- [ ] **S3 ACL Raft 직렬화** — `server.ACLSetter`(SetObjectACL) 구현. 현재 `internal/server/acl_e2e_test.go`가 `t.Skip`. ObjectMeta에 ACL 필드 추가 + `CmdSetObjectACL` FSM 명령.
- [ ] **BucketVersioning Raft 직렬화** — 현재 `bucketver:{bucket}` 키는 로컬 BadgerDB에만 씀. 멀티-노드 클러스터에서 일관성을 위해 `CmdSetBucketVersioning` FSM 명령 추가.
- [ ] **ShardOwner 필터링 재활성화** — `scrubber.ShardOwner` 인터페이스가 선언되었지만 DistributedBackend의 `NodeID`가 raft name 반환 vs allNodes가 주소 저장 → 필터가 no-op이었음. Slice 8에서 self-addr 수정으로 이제 주소끼리 비교 가능. 별도 slice에서 `ShardPlacementMonitor`의 `SetOnMissing` 콜백 연결 포함.
- [ ] **5-node loopback Raft 부트스트랩 안정성** — `TestE2E_ClusterEC_PutGet_5Node`가 CI에서 "no leader found" 로 flaky (130s+ 대기 후 timeout). 3-node 시나리오는 안정적. Election timeout 튜닝 또는 테스트에서 warm-up을 단계적으로 하는 방식 검토.
- [ ] **Per-bucket EC policy 재설계** — ECBackend의 `/admin/buckets/{b}/ec-policy` 토글 API가 사라짐. DistributedBackend는 cluster 전역 `--cluster-ec`로 동작. 필요시 per-bucket `ECConfig`를 FSM에 저장하여 복원.
- [ ] **TestE2E_Versioning_Full 재작성** — 이전 테스트는 `startECServerWithScrub` 헬퍼를 통해 ECBackend 내부에 결합. DistributedBackend 버전 API로 재작성 (`internal/cluster/versioning_test.go`는 unit 커버, e2e 경로 재구축 필요).
- [ ] **TestCrossProtocolS3PutVFSStat 복구** — NFS/VFS flush 경로 수정(NFS 항목 참조) 뒤 자동 복구 가능성 있음. 함께 재검증.

## Phase 19: Performance

- [ ] sendfile syscall (SetBodyStream 완료, syscall 미구현)
- [ ] hertz: Zero-copy Read/Write
- [ ] go-billy: Direct File I/O; O_DIRECT
- [ ] Zero-copy Protocol Bridge (NFS to S3)
- [ ] Unified buffer cache: Centralized Page Cache
- [ ] Reed-Solomon 버퍼 재사용 with sync.Pool
- [ ] io_uring
- [ ] SPDK
- [ ] SoA (Structure of Arrays)
- [ ] SIMD
- [ ] PGO
- [ ] **Predictive resource warnings** — *zero ops* — 디스크 사용률/증가율, BadgerDB value log 크기, goroutine/FD 추세 추적하고 임계 도달 전 경고 (dashboard + log)

## Phase 20: Protocol Extensions

- [ ] Redis 프로토콜 지원 (RESP, Streaming, Pub/Sub 이벤트)
- [ ] TSDB (Time Series DB) — Metric 저장 및 쿼리 지원

## Phase 20: Operations & Onboarding

운영자 개입 없이도 안정적으로 동작하고, 문제 발생 시 명확하게 알려주는 기본기.

- [ ] **Preflight health check** — *zero config* — 부팅 시 data dir 쓰기권한, 디스크 잔여공간, BadgerDB 무결성, port 충돌, TLS 키 권한 자동 검증하고 fail-fast
- [ ] **Safe defaults for every flag** — *zero config* — `grainfs serve` 기본값만으로 production-ready (encryption on, `--no-auth` 명시적 warn, fsync 정책 명확)
- [ ] **Operator-friendly errors** — *zero ops* — 모든 fatal error에 원인 + 복구 방법 + 관련 문서 링크 포함 (e.g., "BadgerDB write failed: disk full at /data, free at least 1GB or set --data-dir")
- [ ] **One-command bootstrap** — *zero config* — `grainfs init` 하나로 cluster key, encryption key, 기본 credential, volume 생성 + 필요 파일 권한 설정
- [ ] **Config drift detection** — *zero ops* — runtime config와 디스크 config 불일치 감지, hot reload 실패 시 명확한 에러
