# TODOS

## Known Issues (Non-blocking)

- [x] **ECBackend.PutObject: io.ReadAll OOM 위험** — `erasure/backend.go:260` 동일 패턴. 대용량 PUT 시 OOM. pullthrough fix 경험 재사용하여 streaming encode로 교체. (Phase 14 또는 pullthrough fix 직후)
- [ ] **lat: 포인터 비원자적 업데이트** — `obj:` 메타 쓰기와 `lat:` 포인터 갱신이 별도 `db.Update` 트랜잭션으로 분리됨. 중간 크래시 시 lat:가 구버전을 가리키거나 누락될 수 있음. 빈도 낮음(OS 크래시 수준)이나 PITR 복원 시 lat: 재구성 로직으로 보완 가능. 장기적으로는 두 쓰기를 단일 트랜잭션으로 통합 필요.
- [ ] **EC on/off toggle: plain↔EC migration path** — 클러스터 config 변경 시 plain 객체와 EC 객체가 혼재. 스크러버는 DataShards=0 필터링하지만 migration 경로 없음. 데이터 손실 위험은 없으나 EC 전환 후 plain 객체는 복구 불가.
- [ ] **scrub interval hot config** — 현재 `--scrub-interval` CLI flag만. Dashboard에서 런타임 변경 가능하도록 Phase 14 dashboard와 통합.
- [ ] **ScrubObjects cursor pagination** — 현재 ScanObjects가 BadgerDB 전체 순회. 10K objects+ 클러스터에서 Phase 14 cursor 기반 페이지네이션 필요.
- [x] **CRC shard migration** — CRC 없는 기존 shard 는 첫 scrub cycle 에 corrupt 로 감지 후 rewrite. 대규모 클러스터에서 첫 scrub 시 repair storm 가능성은 `maxRepairsPerCycle=100` 제한으로 완화됨. (Migration 분류/메트릭은 legacy 감지 경로가 없어 dead code 였고 0.0.4 post-release 에서 제거됨.)

## Phase 13: Operations

- [x] dashboard: raft health, badgerdb vlog gc status, erasure coding status
- [ ] dashboard: hot config change, log tailing
- [ ] dashboard: on-demand metric with sse
- [ ] dashboard: snapshot, backup manage
- [ ] dashboard: event logging
- [x] self healing: data scrubbing, lazy scrubbing, ec repair
- [ ] self healing: data balancing: capacity, hot spot balancing, raft leader balancing
- [ ] IAM, Unified ACL: policy pre-compile
- [ ] ACL embedding
- [ ] LRU token validation
- [ ] zero copy permission check
- [x] versioning (Enabled/Suspended, PUT/GET/DELETE versioned, ListObjectVersions, RestoreObjects, DELETE ?versionId hard-delete, E2E)
- [x] soft-delete (delete marker, IsDeleteMarker, lat: pointer, CachedBackend cache invalidation)
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

