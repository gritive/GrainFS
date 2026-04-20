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
- [x] **Self-healing storage** — *zero ops* — 재시작 시 corrupted cache, stale lock, orphan tmp 자동 정리 (Phase 16 Transparent Self-Healing plan에 포함). **Completed:** v0.0.19 (2026-04-20) — orphan .tmp + 24h+ 포기된 multipart upload 자동 삭제, BadgerDB preflight 무결성 체크. flock 기반 lock 파일과 디스크 캐시는 N/A로 문서화 (각각 OS 자동 해제 / 메모리 전용)
- [x] **EC shard auto-repair** — *zero ops* — scrubber가 mismatch 감지 시 자동 rebuild; dashboard에 진행도 노출. **Completed:** v0.0.18 (2026-04-20) — Phase 16 Week 2 Self-Healing Card + HealEvent SSE 스트림
- [ ] **Degraded mode (storage)** — *zero ops* — EC backend 실패 → read-only + critical alert; 단일 블롭 손상 → 해당 객체만 격리

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
- [ ] **Critical alert channel** — *zero ops* — erasure unrecoverable, raft quorum lost, disk full imminent, encryption key missing 같은 운영자 개입 필요 이벤트를 dashboard + webhook + log로 일관되게 노출
- [ ] **Safe defaults for every flag** — *zero config* — `grainfs serve` 기본값만으로 production-ready (encryption on, `--no-auth` 명시적 warn, fsync 정책 명확)
- [ ] **Operator-friendly errors** — *zero ops* — 모든 fatal error에 원인 + 복구 방법 + 관련 문서 링크 포함 (e.g., "BadgerDB write failed: disk full at /data, free at least 1GB or set --data-dir")
- [ ] **One-command bootstrap** — *zero config* — `grainfs init` 하나로 cluster key, encryption key, 기본 credential, volume 생성 + 필요 파일 권한 설정
- [ ] **Config drift detection** — *zero ops* — runtime config와 디스크 config 불일치 감지, hot reload 실패 시 명확한 에러
