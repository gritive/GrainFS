# TODOS

## Known Issues (Non-blocking)

- [ ] **pull-through: 메모리 OOM 위험** — `pullthrough/pullthrough.go:63` `io.ReadAll` 크기 제한 없음. 대형 파일 업스트림 fetch 시 OOM 가능. 스트리밍 저장으로 교체 필요 (프로덕션 전)
- [ ] **snapshot retention: 수동 스냅샷도 자동 삭제** — `snapshot/auto.go:pruneOld()` 가 `maxRetain` 초과 시 auto+수동 통합 카운트로 오래된 것부터 삭제. 수동 스냅샷은 보호해야 함 (e.g. `reason="auto"` 태그로 구분)
- [ ] **localhostOnly: IPv4-mapped IPv6 누락** — `::ffff:127.0.0.1` 형식 미처리. 실제 발생 가능성 낮지만 완전한 localhost 검사 필요
- [ ] **snapshot-interval 기본값 변경 (0→1h)** — 업그레이드 시 기존 사용자에게 예상치 못한 자동 스냅샷 시작됨. 릴리스 노트에 명시

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

