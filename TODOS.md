# TODOS

## Immediate: Bug Fixes (데이터 안전성)

- [x] BUG: `SaveIndex` 호출 누락 — `Close()`에서 `SaveIndex()` 호출 + 30초 주기 백그라운드 goroutine
- [x] BUG: ListObjects size=0 for packed 객체 — found 객체도 `OriginalSize`로 업데이트하도록 수정
- [x] BUG: Panic recovery 전무 — Hertz: `server.Default()` 내장 recovery 확인, NBD: `handleConn`에 `defer recover()` 추가

## Phase 12: Data Safety (우선순위 순)

1. [x] panic recovery (Hertz middleware + NBD goroutine)
2. [x] http file serving — CDN origin용 (getObject 확장: Cache-Control, ETag, If-None-Match)
3. [x] `grainfs cluster status` CLI 명령 (Raft leader/term/peer lag 출력)
4. [ ] split brain monitoring
5. [x] snapshot (user-facing) — snapshot_api.go 플레이스홀더 구현
6. [x] WAL (data-plane, Raft WAL과 별개) + PITR
7. [ ] automated snapshot and PITR
8. [ ] migration: Pull-through Cache (MinIO→GrainFS)
9. [ ] migration: injector
10. [x] NFS null auth 경고 — serve 시작 시 stdout 출력

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

## Architecture Exploration (결정 완료: 통합 유지 + 데이터 안전성 집중)

- [x] S3-First MVP approach evaluation → **결정: 통합 유지**
- [x] Market need analysis (S3-only vs unified storage) → **결정: unified (NBD/NFS moat 유지)**
- [x] Decision on protocol prioritization → **결정: 데이터 안전성(snapshot/PITR/migration) 우선**
