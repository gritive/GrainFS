# TODOS

## Phase 15: Dashboard (v0.0.14 — 부분 완료)

- [ ] storage & cluster management and monitoring
- [ ] dashboard: snapshot, backup manage
- [ ] dashboard: event logging

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
