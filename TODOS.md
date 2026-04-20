# TODOS

> **Design principle — Zero Config, Zero Ops.** 기본 설정으로 잘 돌아가고, 잘 운영되며,
> 크리티컬한 문제는 사용자에게 알려서 선제대응하게 만든다.
> 각 Phase 항목에 "— *zero config*" / "— *zero ops*" 표시가 있는 것들이 이 원칙에 해당.

## Phase 16: Advanced Storage

### Phase 16 Week 5 — Heal Receipt + OTel (설계 완료, 미진행)

설계: `~/.gstack/projects/gritive-grains/whitekid-master-design-20260420-182422.md`

- [ ] **Slice 1: Receipt Core (v0.0.21)** — `internal/receipt/` 신규 패키지, HealReceipt + JCS + HMAC-SHA256 + KeyStore(key_id rotation) + BadgerDB local-only 저장 + batch write (100 or 50ms) + 30일 retention. API/gossip/UI/OTel 제외, scrubber repair wiring 제외.
- [ ] **Slice 2: API + Gossip** — `/api/receipts/:id`, `/api/receipts?from=&to=`, gossip 50 receipt IDs rolling window, cluster broadcast fallback
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

- [ ] **Lock-free 아키텍처 전환** — *성능 개선*
  현재 Phase 16 Slice 2 `internal/receipt/RoutingCache`와 `Store.drainMu`가 `sync.RWMutex` / `sync.Mutex`를 사용. gossip 수신 + 대시보드 조회가 매 초 수 KHz에 이르면 writer starvation + read 경합으로 p99 latency 튐.
  **후보 접근**: (1) `atomic.Pointer[map[...]]` 기반 copy-on-write — read 0 lock, write는 전체 맵 복사 후 원자 교체. (2) sharded map (node_id hash % N) — write/read 파편화. (3) `sync.Map` 재평가 (rolling replace 패턴에 부적합할 수도).
  **트리거 조건**: (a) Slice 2 gossip 주기가 1초 이내로 내려가거나, (b) routing cache size가 peer 수 × 수백 IDs를 넘거나, (c) 벤치마크에서 Lookup p99 > 100μs 나올 때. 지금은 MVP 정합성 우선, 측정 후 리팩터링.
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
