# TODOS

## Known Issues (Non-blocking)

- [ ] **ECBackend.PutObject: io.ReadAll OOM 위험** — `erasure/backend.go:260` 동일 패턴. 대용량 PUT 시 OOM. pullthrough fix 경험 재사용하여 streaming encode로 교체. (Phase 14 또는 pullthrough fix 직후)
- [ ] **EC on/off toggle: plain↔EC migration path** — 클러스터 config 변경 시 plain 객체와 EC 객체가 혼재. 스크러버는 DataShards=0 필터링하지만 migration 경로 없음. 데이터 손실 위험은 없으나 EC 전환 후 plain 객체는 복구 불가.
- [ ] **scrub interval hot config** — 현재 `--scrub-interval` CLI flag만. Dashboard에서 런타임 변경 가능하도록 Phase 14 dashboard와 통합.

## Phase 13: Operations

- [ ] dashboard: raft health, badgerdb vlog gc status, erasure coding status
- [ ] dashboard: hot config change, log tailing
- [ ] dashboard: on-demand metric with sse
- [ ] dashboard: snapshot, backup manage
- [ ] dashboard: event logging
- [ ] self healing: data scrubbing, lazy scrubbing, ec repair
- [ ] self healing: data balancing: capacity, hot spot balancing, raft leader balancing
- [ ] IAM, Unified ACL: policy pre-compile
- [ ] ACL embedding
- [ ] LRU token validation
- [ ] zero copy permission check
- [ ] versioning
- [ ] soft-delete
- [ ] life cycle management

## Phase 14: Scale

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

