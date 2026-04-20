# TODOS

> **Design principle — Zero Config, Zero Ops.** 기본 설정으로 잘 돌아가고, 잘 운영되며,
> 크리티컬한 문제는 사용자에게 알려서 선제대응하게 만든다.
> 각 Phase 항목에 "— *zero config*" / "— *zero ops*" 표시가 있는 것들이 이 원칙에 해당.

## Phase 16: Advanced Storage

- [ ] Thin provisioning
- [ ] NBD Copy on Write in storage layer (thin provisioning 이후)
- [ ] Reference counting for shared blocks
- [ ] CoW E2E tests
- [ ] Memory usage validation
- [ ] Erasure Coding을 활용한 Bit Rot 방지
- [ ] io.WriteTo 구현 (FlatBuffers zero-copy)
- [ ] nbd server가 굳이 linux만 컴파일 될 필요는 없잖아. 클라이언트만 리눅스 제한이지.
- [ ] **Degraded mode (storage)** — *zero ops* — EC backend 실패 → read-only + critical alert; 단일 블롭 손상 → 해당 객체만 격리
- [ ] **alerts.Dispatcher dedup race** — `internal/alerts/webhook.go:124-157` — `shouldSuppress` 체크와 `recordSent` 사이 lock release 구간에서 동일 (Type, Resource) 동시 Send가 둘 다 통과해 webhook 중복 발송 가능. `shouldSuppress` 통과 즉시 lastSent에 placeholder 기록하거나 Send 전체를 하나의 critical section으로 묶기
- [ ] **gaugeTracker.Report race** — `internal/server/alerts_api.go:89-96` — `inner.Report()` 후 `inner.Degraded()` 사이 다른 goroutine의 Report가 끼어들면 `grainfs_degraded` 게이지 값이 실제 상태와 불일치 가능. Prometheus scrape 타이밍 운에 따라 잘못된 값 노출. gauge 업데이트를 DegradedTracker 내부 lock 안에서 수행하거나 gaugeTracker에 자체 mutex 추가
- [ ] **lastSent map monotonic growth** — `internal/alerts/webhook.go:86,176-180` — dedup map에서 엔트리 삭제 경로 없음. 현재 호출자는 low-cardinality resource만 쓰지만 invariant가 코드에 없어 미래 회귀 위험. 주기적 cutoff sweep 또는 "Resource는 low-cardinality여야 함" godoc 명시
- [ ] **startup recovery WalkDir 에러 swallow** — `internal/server/startup_cleanup.go:126-129` — `filepath.WalkDir`의 top-level 에러(root Lstat 실패 등)가 context.Canceled 외엔 nil로 변환되어 손실. `res.Errors`에 기록하거나 return err로 변경

## Phase 17: Scale-Out

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
