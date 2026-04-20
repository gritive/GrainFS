# TODOS

> **Design principle — Zero Config, Zero Ops.** 기본 설정으로 잘 돌아가고, 잘 운영되며,
> 크리티컬한 문제는 사용자에게 알려서 선제대응하게 만든다.
> 각 Phase 항목에 "— *zero config*" / "— *zero ops*" 표시가 있는 것들이 이 원칙에 해당.

## Phase 16: Advanced Storage

### Phase 16 Week 5 — Heal Receipt + OTel (설계 완료, 미진행)

설계: `~/.gstack/projects/gritive-grains/whitekid-master-design-20260420-182422.md`

- [ ] **Slice 1: Receipt Core (v0.0.21)** — `internal/receipt/` 신규 패키지, HealReceipt + JCS + HMAC-SHA256 + KeyStore(key_id rotation) + BadgerDB local-only 저장 + batch write (100 or 50ms) + 30일 retention. API/gossip/UI/OTel 제외, scrubber repair wiring 제외.
- [x] **Slice 2: API + Gossip** — `/api/receipts/:id`, `/api/receipts?from=&to=`, gossip 50 receipt IDs rolling window, cluster broadcast fallback **Completed:** v0.0.0.22 (2026-04-21). 단, 아래 통합 잔여 항목은 후속.
- [ ] **Slice 2 후속 — serve.go 통합 wiring** — `receipt.Store` + `RoutingCache` + `ReceiptBroadcaster` + `ReceiptGossipSender`를 `cmd/grainfs/serve.go`의 `runSoloWithNFS`/`runCluster`에 플러밍. PSK 공급 (기존 cluster key 재사용 vs 신규 `--heal-receipt-psk`), retention/interval CLI 플래그, `server.WithReceiptAPI(api)` 호출. 현재는 모든 빌딩 블록이 테스트 완료·opt-in 준비.
- [ ] **Slice 2 후속 — multi-node E2E** — `tests/e2e/heal_receipt_api_test.go`: 3-node 클러스터 부팅 → node A에서 receipt 생성 → gossip 전파 → node B의 `/api/receipts/:id` HMAC 인증 + 조회 성공 + broadcast fallback 경로(rolling window 밖 id). serve.go 통합 wiring 선행 필요.
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

## Phase 18: Performance

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

## Phase 19: Protocol Extensions

- [ ] Redis 프로토콜 지원 (RESP, Streaming, Pub/Sub 이벤트)
- [ ] TSDB (Time Series DB) — Metric 저장 및 쿼리 지원

## Phase 20: Operations & Onboarding

운영자 개입 없이도 안정적으로 동작하고, 문제 발생 시 명확하게 알려주는 기본기.

- [ ] **Preflight health check** — *zero config* — 부팅 시 data dir 쓰기권한, 디스크 잔여공간, BadgerDB 무결성, port 충돌, TLS 키 권한 자동 검증하고 fail-fast
- [ ] **Safe defaults for every flag** — *zero config* — `grainfs serve` 기본값만으로 production-ready (encryption on, `--no-auth` 명시적 warn, fsync 정책 명확)
- [ ] **Operator-friendly errors** — *zero ops* — 모든 fatal error에 원인 + 복구 방법 + 관련 문서 링크 포함 (e.g., "BadgerDB write failed: disk full at /data, free at least 1GB or set --data-dir")
- [ ] **One-command bootstrap** — *zero config* — `grainfs init` 하나로 cluster key, encryption key, 기본 credential, volume 생성 + 필요 파일 권한 설정
- [ ] **Config drift detection** — *zero ops* — runtime config와 디스크 config 불일치 감지, hot reload 실패 시 명확한 에러
