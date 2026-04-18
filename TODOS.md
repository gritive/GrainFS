# TODOS

## Phase 13: Operations

### Phase 13 DX 후속 (devex review 2026-04-18)

- [ ] balancer 튜닝 플래그 6종: --balancer-enabled, --balancer-gossip-interval, --balancer-imbalance-trigger-pct, --balancer-imbalance-stop-pct, --balancer-migration-rate, --balancer-leader-tenure-min (serve.go)
- [ ] internal/metrics/metrics.go에 balancer metrics 11종 추가 (gossip, migrations, leader transfer, load-aware reads, pending stale)
- [ ] 실패 모드 명세: WriteShard retry/backoff/give-up, pending migration TTL orphan cleanup, DstNode disk-full circuit breaker
- [ ] Structured logging: component="balancer" tag + phase field
- [ ] 신규 노드 join 시 grace period (10분, trigger 50% 완화) — migration storm 방지
- [ ] Rolling upgrade 대응: warm-up timeout 후 available peers로 시작, protobuf unknown field tolerance
- [ ] Health endpoint: GET /cluster/balancer/status (pending migrations, last gossip, imbalance pct)
- [ ] Operator runbook: docs/operations/balancer.md
- [ ] E2E: disk-fill 전략 — 테스트 하니스에서 diskUsedPct 주입 hook

### Phase 13 리뷰 후속 (adversarial review 2026-04-18)

- [ ] **[F1] proposeMigration: 실제 오브젝트 선택** — `balancer.go:144` `proposeMigration`이 현재 SrcNode/DstNode만 전송하고 Bucket/Key/VersionID를 비워서 `applyMigrateShard`에서 모두 폐기됨. 메타데이터에서 실제 오브젝트를 조회해 채워야 함. (Phase 14 배선과 함께 구현)
- [ ] **[F3] 프로덕션 배선** — `SetMigrationHooks`, `NewMigrationExecutor`, `NewGossipSender/Receiver`, `NewBalancerProposer`가 `cmd/`에서 연결되지 않음. Phase 14 배선 단계에서 연결.
- [ ] **[F4] NotifyCommit 조기 도착 시 DeleteShards 스킵** — `migration_executor.go:82` FSM이 Execute() 등록 전에 CmdMigrationDone을 적용하면 src 샤드 삭제가 영구 스킵됨. 조기 도착 경로에서 Phase 4 보장 필요.
- [ ] **[F2] 채널 풀 → 태스크 영구 소실** — `apply.go:237` 채널 풀 시 Raft는 커밋 처리하나 태스크는 소실. 영속 태스크 큐 또는 버퍼 크기 동적 조정 검토.

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

