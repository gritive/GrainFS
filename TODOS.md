# TODOS

## Phase 13: Operations

### Phase 13 DX 후속 (devex review 2026-04-18)

- [ ] 실패 모드 명세: WriteShard retry/backoff/give-up, pending migration TTL orphan cleanup, DstNode disk-full circuit breaker
- [ ] Structured logging: component="balancer" tag + phase field
- [ ] Rolling upgrade 대응: warm-up timeout 후 available peers로 시작, protobuf unknown field tolerance

## Phase 14: Scale

- [ ] ScrubObjects cursor pagination — ScanObjects BadgerDB 전체 순회 → cursor 기반으로 교체 (10K+ 클러스터 대비)
- [ ] scrubber: orphan shard 탐지 및 정리 (migration Phase-3→4 크래시 gap으로 남는 src 고아 샤드)
- [ ] Thin provisioning
- [ ] sharding, multi raft
- [ ] badgerdb: managed mode (raft)
- [ ] badgerdb: write batch
- [ ] badgerdb: TableLoadingMode
- [ ] badgerdb: LSM Read Amplification, bloom filter
- [ ] Adaptive Raft Batching
- [ ] migration: nfs: virtual overlay
- [ ] migration: nbd: block proxying
- [ ] migration: adaptive throttling, priority queue

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

## Deferred 12m+ (측정된 병목 확인 후)

- [ ] zero copy, zero allocation, sendfile (현재 SetBodyStream 검증 먼저)
- [ ] PGO
- [ ] FlatBuffers
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

