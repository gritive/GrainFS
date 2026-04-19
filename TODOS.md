# TODOS

## Phase 14: Scale

- [ ] Raft waiter leak on truncation — flushBatch 등록한 n.waiters 항목이 step-down + log truncation 시 미정리. HandleAppendEntries truncation path에서 affected index 범위의 waiters 삭제 필요 (Opus review finding #1)
- [ ] Thin provisioning
- [ ] sharding, multi raft
- [ ] migration: nfs: virtual overlay
- [ ] migration: nbd: block proxying

## Phase 15: Dashboard

- [ ] storage & cluster management and monitoring
- [ ] dashboard: hot config change (scrub interval 포함), log tailing
- [ ] dashboard: on-demand metric with sse
- [ ] dashboard: snapshot, backup manage
- [ ] dashboard: event logging

## Copy on Write (NBD — thin provisioning 이후)

- [ ] NBD Copy on Write in storage layer
- [ ] Reference counting for shared blocks
- [ ] CoW E2E tests
- [ ] Memory usage validation

## Phase F: FlatBuffers Migration ✅ 완료 (v0.0.12)

- [x] Phase F1: ECObjectMeta, BucketMeta, MultipartMeta FlatBuffers 전환 (dual-format reader)
- [x] Phase F2–F5: 전 계층(erasure/cluster/raft/storage/volume) write path FlatBuffers 전환
- [x] Phase F (완료): proto 완전 제거, FB suffix 정리, Makefile stamp 규칙
- [x] Phase F (fix): 15개 decode 함수 패닉 보호, Makefile clean 스탬프 삭제

## Deferred 12m+ (측정된 병목 확인 후)

- [ ] zero copy, zero allocation, sendfile (현재 SetBodyStream 검증 먼저)
- [ ] PGO
- [ ] io_uring
- [ ] SoA (Structure of Arrays)
- [ ] SIMD
- [ ] Unified buffer cache: Centralized Page Cache
- [ ] Zero-copy Protocol Bridge (NFS to S3)
- [ ] SPDK
- [ ] hertz: Zero-copy Read/Write
- [ ] go-billy: Direct File I/O; O_DIRECT
- [ ] smithy-go: io.WriteTo를 구현하여, FlatBuffers, zero-copy, zero-allocation 활용
- [ ] Erasure Coding을 활용한 Bit Rot 방지
- [ ] sync.Pool의 적극 활용
- [ ] Zstd & Reed-Solomon: 버퍼 재사용 with sync.Pool
- [ ] nbd over internet for edge computing (powered by wireguard)
- [ ] nbd server가 굳이 linux만 컴파일 될 필요는 없잖아. 클라이언트만 리눅스 제한이지.
