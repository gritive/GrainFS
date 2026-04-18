# TODOS

## Phase 13: Operations

- [ ] dashboard: hot config change (scrub interval 포함), log tailing
- [ ] dashboard: on-demand metric with sse
- [ ] dashboard: snapshot, backup manage
- [ ] dashboard: event logging
- [ ] self healing: data balancing: capacity, hot spot balancing, raft leader balancing
- [ ] IAM, Unified ACL: policy pre-compile
- [ ] ACL embedding
- [ ] LRU token validation
- [ ] zero copy permission check
- [ ] life cycle management

## Phase 14: Scale

- [ ] ScrubObjects cursor pagination — ScanObjects BadgerDB 전체 순회 → cursor 기반으로 교체 (10K+ 클러스터 대비)
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

