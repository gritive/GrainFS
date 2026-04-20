# TODOS

## Reliability: Zero Config, Zero Ops

기본 설정으로 잘 돌아가고, 잘 운영되며, 크리티컬한 문제는 사용자에게 알려서 선제대응하게 만든다.

- [ ] **Self-healing defaults** — 재시작만으로 회복되는 상태 (corrupted cache, stale lock, orphan tmp 등) 자동 정리
- [ ] **Preflight health check** — 부팅 시 data dir 쓰기권한, 디스크 잔여공간, BadgerDB 무결성, port 충돌, TLS 키 권한 등 자동 검증하고 fail-fast
- [ ] **Critical alert channel** — 운영자 개입이 필요한 이벤트(erasure unrecoverable, raft quorum lost, disk full imminent, encryption key missing)를 dashboard + webhook + log로 일관되게 노출
- [ ] **Predictive warnings** — 디스크 사용률/증가율, BadgerDB value log 크기, goroutine/FD 누수 추세를 추적하고 임계 도달 전 경고
- [ ] **Auto-recovery playbook** — Raft leader 실패, BadgerDB 손상, EC shard 불일치 등에 대한 자동 복구 절차 (log-based replay, snapshot restore, shard repair) 내장
- [ ] **Safe defaults for every flag** — `grainfs serve` 기본값만으로 production-ready (encryption on, auth required 또는 명시적 `--no-auth` warn, fsync 정책 명확)
- [ ] **Operator-friendly errors** — 모든 fatal error에 원인 + 복구 방법 + 관련 문서 링크 포함 (e.g., "BadgerDB write failed: disk full at /data, free at least 1GB or set --data-dir")
- [ ] **Degraded mode** — 한 계층 실패 시 안전하게 축소 동작 (e.g., EC backend 실패 → read-only + critical alert; NFS 실패 → S3/NBD는 계속)
- [ ] **Rolling upgrade safety** — 버전 간 binary 교체로 downtime/데이터 손실 없음 (schema migration 자동, snapshot forward-compat 보장)
- [ ] **One-command bootstrap** — `grainfs init` 하나로 cluster key, encryption key, 기본 credential, volume 생성 + 필요 파일 권한 설정

## Phase 16: Advanced Storage

- [ ] Thin provisioning
- [ ] NBD Copy on Write in storage layer (thin provisioning 이후)
- [ ] Reference counting for shared blocks
- [ ] CoW E2E tests
- [ ] Memory usage validation
- [ ] Erasure Coding을 활용한 Bit Rot 방지
- [ ] io.WriteTo 구현 (FlatBuffers zero-copy)
- [ ] nbd server가 굳이 linux만 컴파일 될 필요는 없잖아. 클라이언트만 리눅스 제한이지.

## Phase 17: Scale-Out

- [ ] Sharding, multi raft
- [ ] Raft leader 부하 분산 검토 (follower proxy, read-only query, lease read 등)
- [ ] Migration: NFS virtual overlay
- [ ] Migration: NBD block proxying
- [ ] nbd over internet for edge computing (powered by wireguard)

## Phase 19: Protocol Extensions

- [ ] Redis 프로토콜 지원 (RESP, Streaming, Pub/Sub 이벤트)
- [ ] TSDB (Time Series DB) — Metric 저장 및 쿼리 지원

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
