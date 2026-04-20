# TODOS

## Bug Fix / Flaky Tests

- [ ] TestNBD_Docker: 대기 루프가 `/metrics`(S3)만 체크 → NBD 포트 10809 리스닝 대기 추가 필요 (`docker/nbd-test.sh`)
- [ ] TestNFS_MountAndWriteReadFile: macOS NFS 클라이언트에서 mount 500초 후 disconnect → timeout/retry 또는 skip 플래그 검토 (`tests/e2e/volume_test.go`)
- [ ] Event Log: `emitEvent` unbounded goroutine — 고부하 S3 트래픽 시 BadgerDB write 지연 시 goroutine leak 가능 (`internal/server/events_api.go:81`). worker pool 또는 buffered channel 검토
- [ ] Event Log: `/api/eventlog`의 `since`/`until` dual-mode 로직 단순화 — `<=3600`이면 상대 오프셋, 초과하면 절대 unix초인 로직이 혼란 유발 (`internal/server/events_api.go:37-47`)
- [ ] Event Log: `handleFormUpload`에서 `emitEvent` 호출 누락 — 다른 PUT 경로는 event emit하는데 form upload만 빠짐 (`internal/server/handlers.go:791`)

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
