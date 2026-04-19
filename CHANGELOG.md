# Changelog

## [0.0.13] - 2026-04-20

### Fixed
- **Raft waiter correctness 버그 수정** — `HandleAppendEntries` log truncation 및 `HandleInstallSnapshot` 시 `n.waiters` map이 정리되지 않아 발생하던 false-success 시나리오 제거.
  - `waiters map[uint64]chan struct{}` → `waiters map[uint64]chan error`로 전환. `close(ch)` = nil = 성공, `ch <- ErrProposalFailed` = 실패.
  - `abortWaitersFrom(from uint64)` 헬퍼 추가 — truncation 시 영향받는 index의 goroutine을 즉시 종료.
  - `HandleAppendEntries` 두 truncation 경로 및 `HandleInstallSnapshot` 에 `abortWaitersFrom` 호출 추가.
  - split-brain 상황에서 다른 Leader가 같은 index에 다른 엔트리를 커밋할 때 원래 제안자에게 SUCCESS가 잘못 전달되던 Raft 안전성 불변식 위반 수정.

## [0.0.12] - 2026-04-20

### Changed
- **Phase F: FlatBuffers 완전 전환** — protobuf 의존성 제거. 직렬화 계층 전체(erasure, cluster, raft, storage, volume)를 FlatBuffers로 통일.
  - `internal/erasure`: `ECObjectMeta`, `BucketMeta`, `MultipartUploadMeta` FlatBuffers 전환. `builderPool`(`sync.Pool`)으로 hot-path 할당 제거. `fbRecover` 헬퍼로 모든 decode에 패닉 → 에러 변환.
  - `internal/cluster`: Raft FSM 커맨드(`CreateBucket`…`MigrationDone`), `ObjectMeta`, `MultipartMeta`, snapshot state 모두 FlatBuffers. `fbSafe` 제네릭 헬퍼로 decode 패닉 보호. 고시 수신 경로(`decodeNodeStatsMsg`)도 패닉 격리.
  - `internal/raft`: `LogEntry`, `RaftState`, `SnapshotMeta`, Raft RPC 6종(`RequestVote`, `AppendEntries`, `InstallSnapshot` args/reply) FlatBuffers. decode 함수 전부 defer/recover 추가.
  - `internal/storage`: `Object`, `MultipartMeta` FlatBuffers 전환. unmarshal 패닉 보호.
  - `internal/volume`: `Volume` FlatBuffers. unmarshal 패닉 보호.
  - `internal/cluster/shard_service`: `unmarshalShardRequest` 패닉 보호.
  - Makefile: `%.fbs.stamp` 규칙으로 `flatc --gen-all` 1:N 출력 추적. `make clean` 시 스탬프 파일 삭제.
- **`--raft-flatbuffers` 플래그 제거** — FlatBuffers가 유일한 포맷. 피처 플래그 불필요.

### Fixed
- `make clean` 후 `make`가 `.fbs` 파일을 재생성하지 않던 버그 수정 — `clean` 타겟에 `*.fbs.stamp` 삭제 추가.
- FlatBuffers decode 함수 15곳 패닉 보호 누락 수정 — 손상된 데이터나 이전 포맷 바이트 입력 시 프로세스 크래시 방지.

## [0.0.11] - 2026-04-19

### Added
- **Adaptive Raft Batching** (`batcherLoop` + `flushBatch`) — EWMA 기반 동적 배치로 BadgerDB 커밋 횟수 대폭 감소. 100 동시 PUT → 97% 커밋 감소 (100회 → 3회).
  - `proposal` / `proposalResult` 타입 도입. `ProposeWait(ctx, cmd)` 인터페이스 유지.
  - `proposalCh` (buf=4096): leader 진입 후 batcherLoop가 독립적으로 수집·flush.
  - **EWMA 적응 알고리즘**: alpha=0.3. 저부하(<100 req/s) → 100µs / 4-batch, 중부하(100-500) → 1ms / 32-batch, 고부하(>500) → 5ms / 128-batch. 설정 파일 없이 자동 전환.
  - **즉시 복제 트리거**: `flushBatch` 완료 시 `replicationCh`(buf=1)에 신호 → `runLeader`가 HeartbeatTimeout을 기다리지 않고 즉시 `replicateToAll()` 호출.
  - **Graceful shutdown**: `stopCh` 닫기 → pending 제안 전부 `ErrProposalFailed` 반환 후 종료.
  - `BatchMetrics()` accessor: `EWMARate`, `BatchTimeout`, `MaxBatch` 스냅샷 반환.
- **7개 신규 테스트** (`batcher_test.go`): `TestBatcher_HighLoad`, `TestBatcher_LowLoad`, `TestBatcher_NotLeader`, `TestBatcher_Shutdown`, `TestBatcher_ReplicationTrigger`, `TestAdaptiveMetrics_Transition`, `TestBatcher_PersistPanic`.
- **Raft 로그 GC** (`--badger-managed-mode`) — 클러스터 부트스트랩 이후 누적된 Raft 로그를 자동 정리. 쿼럼 watermark 기준으로 안전하게 삭제하므로 `data/raft/` 가 무한히 커지지 않는다.
  - `--badger-managed-mode` 플래그 (기본 false) — 명시적 opt-in. 활성화 후 플래그 없이 재시작하면 포맷 불일치 오류로 거부해 silent data loss를 방지.
  - `--raft-log-gc-interval` 플래그 (기본 30s, 0=비활성) — GC 실행 주기.
  - GC 고루틴 격리: heartbeat 루프와 별도 고루틴으로 실행 (`atomic.Bool` 가드), 대규모 GC가 heartbeat를 지연시키지 않음.
  - 배치 삭제(1000개/txn): 대용량 로그에서 `ErrTxnTooBig` 방지.
  - 스냅샷 게이트: 스냅샷 없이 GC 시도 시 skip + warn으로 뒤처진 팔로워 복구 경로 보호.
  - `docs/badger-managed-mode-rollback.md` — 활성화 방법, Prometheus 검증 쿼리, 롤백 절차, cut-over 체크리스트.
- **Phase 14a: Orphan Shard Sweep** — migration Phase 3→4 크래시 갭으로 남는 고아 샤드 디렉토리를 자동으로 탐지하고 정리한다. `OrphanWalkable` 인터페이스(선택적 확장)를 구현한 ECBackend에서 활성화된다.
  - **Age gate** — 생성 후 5분 미만인 샤드 디렉토리는 진행 중인 PUT 보호를 위해 건드리지 않는다.
  - **Tombstone delay** — 2 연속 사이클에서 고아로 확인된 디렉토리만 삭제 (오탐 방지).
  - **I/O storm 방지** — 사이클당 최대 50개(`maxOrphansPerCycle`) 삭제 캡.
  - **Zero-config** — CLI 플래그 없이 백엔드가 `OrphanWalkable`을 구현하면 자동 활성.
- **3개 신규 메트릭** — `grainfs_scrub_orphan_shards_found_total`, `grainfs_scrub_orphan_shards_deleted_total`, `grainfs_scrub_orphan_sweep_capped_total`.
- **Phase 14b: Migration Priority Queue + Adaptive Throttle** — `MigrationPriorityQueue` (container/heap 기반 max-heap)로 마이그레이션 소스 노드를 DiskUsedPct 내림차순으로 정렬. 가장 꽉 찬 노드의 객체가 먼저 이동한다.
  - **토큰 버킷** — `MigrationProposalRate` (기본 2.0/s)로 proposal 속도 제한. I/O 폭풍 방지.
  - **Aging factor** — `effectivePriority = diskUsedPct × (1 + ageMin/10)`. 10분 지연된 50% 노드가 갓 등록된 80% 노드보다 우선될 수 있어 기아 방지.
  - **Sticky donor** — `StickyDonorHoldTime` (기본 30s)동안 동일 src 노드 유지. 우선순위 flip으로 인한 thrash 방지.
- **2개 신규 BalancerConfig 필드** — `MigrationProposalRate float64`, `StickyDonorHoldTime time.Duration`.
- **ScanObjects 커서 페이지네이션** — `ECBackend.ScanObjects` / `ScanPlainObjects`가 단일 장기 BadgerDB 읽기 트랜잭션 대신 페이지(기본 256개 키)마다 새 트랜잭션을 열어 MVCC 읽기 락을 조기 해제. 대규모 버킷에서 BadgerDB GC 지연 방지. 기존 API 변경 없음.
- **`WithScanPageSize` ECOption** — 테스트 및 특수 환경에서 페이지 크기를 조정할 수 있는 옵션 추가.
- **`WithBloomFalsePositive` ECOption** — BadgerDB SSTable 블룸 필터 오탐율을 `NewECBackend` 생성 시 지정 가능. 낮은 값은 읽기 증폭 감소, 블룸 필터 메모리 증가.

### Changed
- `NewECBackend` — 옵션을 `badger.Open` 이전에 적용해 DB 수준 설정(`BloomFalsePositive`)을 반영하도록 초기화 순서 변경. 기존 코드 호환.

## [0.0.10] - 2026-04-19

### Added
- **Circuit Breaker** — per-node 2-state (open/closed) disk-full 게이트. `grainfs_balancer_cb_open` (GaugeVec) 메트릭으로 상태 노출. `--balancer-cb-threshold` 플래그 (기본 0.90 = 90%)로 설정.
- **WriteShard 재시도** — 지수 백오프 + ±20% 지터, `ErrPermanent` 즉시 실패 경로. `--balancer-migration-max-retries` 플래그 (기본 3회). `grainfs_balancer_shard_write_retries_total` (CounterVec) 메트릭.
- **Pending Migration TTL** — 좀비 마이그레이션 자동 취소. Phase 2 이후 1회 연장(Option A), 2차 만료 시 취소. `--balancer-migration-pending-ttl` 플래그 (기본 5분). `grainfs_balancer_migration_pending_ttl_expired_total` 메트릭.
- **Structured Logging** — 마이그레이션 Phase 1~4 진행 상황을 `phase=` 필드로 추적. `component=migration` 로그에서 현재 어느 단계가 걸렸는지 즉시 확인 가능.
- **warmupComplete 개선** — 워밍업 완료 판단 기준을 `store.Len()` 비교에서 `NodeStats.UpdatedAt` 최근성 검사로 교체. 노드 재시작 직후 false-positive로 마이그레이션이 즉시 시작되는 문제 해소.
- **4개 신규 메트릭** — `grainfs_balancer_cb_open`, `grainfs_balancer_cb_all_open_total`, `grainfs_balancer_shard_write_retries_total`, `grainfs_balancer_migration_pending_ttl_expired_total`.

### Fixed
- **MigrationExecutor 레이스 컨디션** — `NotifyCommit`이 `pending[id]`를 조기 삭제해 동시 goroutine이 Phase 1을 재실행하던 버그 수정. 이제 `Execute`가 `markDone` 직후 `mu` 홀딩 상태에서 삭제.
- **`tickOnce`/`proposeMigration` 미사용 ctx 파라미터** — 시그니처에서 제거, 연관 테스트 정리.
- **중복 `component=balancer` 로그 필드** — `selectDstNode` Warn 로그에서 이미 logger에 설정된 필드 중복 제거.
- **TTL sweep dead code** — `Execute()`에 `registerPending` 호출 누락으로 TTL sweep이 동작하지 않던 버그 수정. 이제 `pendingTTL > 0` 시 derived context + cancel이 TTL sweep에 연결됨.
- **음수 pendingTTL 패닉** — `Start()` 가드를 `== 0`에서 `<= 0`으로 수정해 음수 Duration 입력 시 패닉 방지.
- **CBThreshold 입력 검증 누락** — `startBalancer()`에서 `--balancer-cb-threshold` 플래그 값이 [0, 1] 범위인지 검증 추가.

## [0.0.9] - 2026-04-19

### Added
- **DiskCollector** — 로컬 디스크 사용률을 주기적으로 읽어 `NodeStatsStore`에 반영하는 goroutine 추가. 이제 balancer가 실제 디스크 사용률에 반응한다.
- **`grainfs_disk_used_pct` 메트릭** — node_id 레이블을 가진 Prometheus GaugeVec. DiskCollector tick마다 갱신된다.
- **`--balancer-warmup-timeout` 플래그** — 노드 시작 후 마이그레이션 제안을 유예하는 시간 설정. 조인/복구 중 오탐 방지.
- **`GRAINFS_TEST_DISK_PCT` 환경 변수** — 실제 디스크 사용률 대신 고정값을 주입. 통합 테스트 및 운영 시뮬레이션용. 유효하지 않은 값([0,100] 범위 초과 포함)이면 서버 시작 시 즉시 실패.
- **Operator Runbook Testing 섹션** — `docs/operations/balancer.md`에 `GRAINFS_TEST_DISK_PCT` 사용 예제 및 Prometheus 쿼리 추가.
- **`disk_stat_stub.go`** — `//go:build !unix` 스텁 추가. Windows/Plan9 빌드에서 `sysDiskStat`가 (0,0)을 반환해 `collect()`가 조용히 스킵.

### Fixed
- **DiskUsedPct 항상 0 문제** — GossipSender가 DiskUsedPct를 브로드캐스트하지만 실제로 syscall.Statfs를 호출하는 goroutine이 없어 항상 0으로 전송되던 근본 버그를 수정.
- **doctor.go DRY 위반** — `checkDiskSpace()`의 Statfs 로직을 `sysDiskStat()`로 추출. DiskCollector와 동일한 코드 경로 공유.
- **`disk_stat_unix.go` 빌드 태그 누락** — `//go:build unix` 추가 + `_unix` 파일명 suffix만으로는 Go 빌드 제약으로 인식되지 않는 문제 수정.
- **Prometheus 클램프 누락** — `DiskCollector.collect()`에서 `metrics.DiskUsedPct.Set()`에 전달 전 [0,100] 클램프 적용.
- **`sysDiskStat` (0,0) 오탐** — `collect()`에서 (0,0) 반환 시 `UpdateDiskStats` 호출을 스킵하고 WARN 로그 출력. 유효하지 않은 디스크 통계로 NodeStatsStore를 오염시키는 문제 수정.
- **`GRAINFS_TEST_DISK_PCT` 범위 검증 누락** — 0~100 범위 검증 추가. 범위 초과 시 서버 시작 시 즉시 오류 반환.
- **`DiskCollector.logger` 데드 필드** — 사용되지 않는 `logger` 필드 제거.

## [0.0.8] - 2026-04-18

### Added
- **Balancer Prometheus 메트릭 11종** — `grainfs_balancer_gossip_total`, `grainfs_balancer_migrations_proposed_total`, `grainfs_balancer_migrations_done_total`, `grainfs_balancer_migrations_failed_total`, `grainfs_balancer_imbalance_pct`, `grainfs_balancer_pending_tasks`, `grainfs_balancer_leader_transfers_total`, `grainfs_balancer_shard_write_errors_total`, `grainfs_balancer_shard_copy_duration_seconds`, `grainfs_balancer_grace_period_active_ticks_total` 추가.
- **Grace Period 이중 트리거 완화** — 새 노드 join 후 `GracePeriod` 동안 불균형 트리거 임계값 1.5× 완화. `BalancerGracePeriodActiveTicks` 메트릭으로 가시화.
- **Balancer HTTP 헬스 엔드포인트** — `GET /api/cluster/balancer/status` 추가. 현재 활성 상태, 불균형 %, 노드별 stats 반환.
- **Operator Runbook** — `docs/operations/balancer.md` 추가. 알람 임계값, 트러블슈팅 가이드, 설정 레퍼런스 포함.

### Fixed
- **Migration early-commit race** — `MigrationExecutor.Execute()`가 earlyCommit 경로에서도 sentinel `commitCh`를 mutex 하에 등록. 동시 goroutine이 Phase 1(shard copy)을 재진입하는 race 방지.
- **`cleanupPending` 채널 누수** — 오류 경로에서 pending 채널 제거 시 `close(ch)` 추가. 대기 goroutine이 영원히 블록되는 문제 수정.
- **`BalancerProposer` inflight 키 불일치** — `proposeMigration`의 inflight 키를 `NotifyMigrationDone` 키와 동일하게 통일 (`bucket/key/versionID`).
- **리뷰 발견 4종 수정** — `closer.Close()` 에러 로깅, `WalkDir` I/O 에러 로깅, `LocalObjectPicker` 테스트 커버리지 추가, `BalancerGracePeriodActiveTicks` 리네임.
- **E2E 포트 충돌 수정** — `TestNetworkPartitionSuite`에서 toxiproxy 포트와 프록시 리스너 포트를 동적 할당으로 전환. 전체 suite 실행 시 포트 8474/9000 고정값과의 충돌 방지.
- **E2E PITR stale blob 오탐 수정** — `TestPITR_WALReplayAddsObjects`의 stale blob 검증을 현재 테스트 버킷으로 한정. 선행 테스트들의 cleanup으로 발생하는 외부 stale blob을 오류로 인식하던 문제 수정.
- **E2E NBD 테스트 안정화** — `docker/nbd-test.sh`에서 stale `/dev/nbd0` 연결 해제 추가 (이전 컨테이너 SIGKILL 잔류 문제). `mkfs.ext4` 제거 후 `dd` 패턴 검증으로 교체 (테스트 시간 330s → ~2s). `--nbd-volume-size` CLI 플래그 추가.
- **ObjectPicker skipIDs FSM 연결** — `NotifyMigrationDone` 3-arg 시그니처로 FSM goroutine에서 inflight 정확히 클리어. picker가 skipIDs를 무시하는 경우를 대비한 double-check guard 추가. `RecoverPending` context 전파로 ctx 취소 시 복구 루프 안전 종료.
- **`BalancerProposer` inflight map data race** — `NotifyMigrationDone`(FSM goroutine)과 `proposeMigration`(balancer goroutine)이 동시에 `inflight` map에 접근해 발생하는 race. `sync.Mutex`로 `inflight`, `active` 필드 보호.

## [0.0.7] - 2026-04-18

### Added
- **클러스터 Auto-Balancing (Phase 13)** — Gossip 프로토콜로 노드 디스크 사용률 공유, Raft 기반 샤드 마이그레이션, 리더 주도 발란싱 루프 구현. `GossipSender`/`GossipReceiver`, `MigrationExecutor`, `BalancerProposer` 추가.

### Changed
- **QUIC 스트림 라우팅** — `StreamRouter`가 스트림 타입별 독립 채널로 분배. Gossip 수신자 채널이 더 이상 데드락되지 않음.

### Fixed
- **Gossip cold-start 브로드캐스트** — 로컬 stats 미준비 시 DiskUsedPct=0 브로드캐스트를 스킵해 새 노드가 즉시 마이그레이션 폭풍의 대상이 되는 문제 수정.
- **리더 tenure 타이머** — `BalancerProposer.Run()` 진입 시 타이머 재설정. 기존에는 생성 시점 기준이라 팔로워 기간도 LeaderTenureMin에 포함됐음.
- **빈 Bucket/Key 가드** — `applyMigrateShard`에서 `Bucket="", Key=""` 제안을 조용히 폐기해 `//"` 키 패스 오류 방지.
- **FSM Migration 채널 논블로킹** — `onMigrateShard`를 콜백에서 버퍼 채널로 교체. 채널 풀 시 경고 로그 후 드롭.
- **NodeId 스푸핑 방지** — Gossip 수신 시 `conn.RemoteAddr()`와 `NodeId` 불일치 메시지 드롭. hostname nodeID + IP from 조합 처리.
- **Gossip 수신값 범위 검증** — `DiskUsedPct`를 [0,100], `RequestsPerSec`를 [0,∞)로 클램프해 오작동 발란서 방지.
- **Migration idempotency 맵 OOM** — `done` 맵이 10,000건 초과 시 리셋해 고유 마이그레이션 폭풍에 의한 메모리 소진 방지.
- **Connect() TOCTOU 커넥션 누수** — Write lock 재확인으로 동시 dial 시 중복 커넥션 닫기.
- **zstd 풀 테스트 임계값** — race detector 오버헤드(~8×)를 반영해 alloc 임계값 상향 조정.

## [0.0.6] - 2026-04-18

### Added
- **S3 Lifecycle Management** — `PutBucketLifecycleConfiguration` / `GetBucketLifecycleConfiguration` / `DeleteBucketLifecycleConfiguration` API 구현. XML 직렬화/역직렬화, `lifecycle.Store` (BadgerDB 영속화), `lifecycle.Worker` (주기적 만료 스캔). Rate limiter (100 deletes/sec)로 삭제 속도 제한.
- **Expiration 자동 삭제** — 룰별 `Days` 기준으로 오브젝트 만료 삭제. Prefix 필터 지원. delete marker 오브젝트는 건너뜀.
- **NoncurrentVersionExpiration** — `NoncurrentDays` + `NewerNoncurrentVersions` AND 조합으로 비최신 버전 정리. S3 스펙 준수: 두 필드 모두 설정 시 두 조건 모두 충족해야 삭제.

### Fixed
- **`Stats()` 데이터 레이스** — `w.stats.LastRun` 읽기 시 mutex 누락으로 race condition 발생. `Stats()` 내 mutex 락 추가.
- **고루틴 누수 방지** — ctx 취소 시 `ScanObjects` 프로듀서 고루틴이 블록되던 문제. `go func() { for range objs {} }()`로 드레인.
- **`limiter.Wait` 오류 묵살** — `_ = w.limiter.Wait(ctx)` → 오류 반환 시 즉시 return 처리.
- **delete marker 무한 증가** — `DeleteObject` 버전 버킷에서 매 사이클마다 delete marker를 새로 생성하던 버그. `!obj.IsDeleteMarker` 조건 추가.
- **`ListObjectVersions` prefix 오탐** — `ECBackend.ListObjectVersions`가 prefix 매칭을 수행해 다른 키의 버전을 삭제할 수 있었음. 어댑터에서 `v.Key == key` 정확 매칭으로 필터링.
- **lifecycle 설정 전 버킷 존재 확인** — `PutBucketLifecycle`에서 `HeadBucket` 검증 추가. 존재하지 않는 버킷에 lifecycle이 선 설정되던 문제 방지.
- **lifecycle XML 바디 크기 제한** — 64 KiB 초과 시 `EntityTooLarge` 반환.
- **Expiration Days=0 유효성** — `Days <= 0` 으로 조건 강화. S3 스펙: Days는 1 이상이어야 함.

## [0.0.5] - 2026-04-18

### Security
- **SigV4 서명 캐시 보안 강화** — `CachingVerifier`가 캐시 히트 시에도 `VerifyWithSigningKey`로 HMAC을 재검증하도록 수정. 기존 구현은 첫 검증 성공 후 이후 요청에서 서명을 검사하지 않아 토큰 재사용 공격에 노출됐음. 서명 키(32바이트, 하루 단위 안정)만 캐시하여 키 도출 비용(HMAC 4회)은 절감하면서 매 요청 서명 검증을 유지.
- **익명 fast-path S3 서브리소스 차단** — `RawQuery == ""`를 추가해 `?acl`, `?versions`, `?uploads`, `?tagging` 등 S3 서브리소스 요청이 인증 없이 통과되지 않도록 수정.

### Fixed
- **s3:GetObject 정책이 HEAD 요청 포함** — AWS S3 호환: `s3:GetObject` 버킷 정책이 GET뿐 아니라 HEAD도 허용하도록 `actionAliases` 컴파일 타임 매핑 추가. 기존에는 HEAD가 `HeadObject` 액션으로만 평가되어 `GetObject`만 허용한 정책에서 HEAD가 거부됐음.
- **PutObject ACL 원자성** — PUT + ACL 설정이 두 단계로 분리돼 크래시 시 ACL 손실 위험이 있었음. `storage.AtomicACLPutter` 인터페이스 + `ECBackend.PutObjectWithACL` 추가로 단일 BadgerDB 트랜잭션에서 오브젝트와 ACL을 함께 저장. 핸들러는 `AtomicACLPutter` 지원 시 atomic 경로, 미지원 시 기존 2단계 경로로 폴백.
- **PITR 복원 ACL 손실** — `SnapshotObject`에 `ACL` 필드가 없어 스냅샷/복원 시 모든 오브젝트 ACL이 private으로 리셋됐음. `storage.SnapshotObject`에 `acl` 필드 추가, `ListAllObjects`/`RestoreObjects`에서 직렬화/역직렬화.

### Added
- **IAM/정책 컴파일러** — 버킷 정책을 Set() 시점에 컴파일해 액션별 deny/allow 룰 배열로 인덱싱. 요청 평가는 O(1) 룩업 + deny-first AWS 호환 로직. `CompiledPolicyStore` 구현.
- **ACL 통합 (`s3auth.ACLGrant`)** — 오브젝트별 ACL bitmask를 ECBackend 메타데이터에 저장. `SetObjectACL`, `PutObjectWithACL` 인터페이스로 핸들러에서 접근. `GetObject`/`HeadObject`에서 ACL 기반 접근 제어 적용.
- **SigV4 캐싱 검증자** — `CachingVerifier`가 서명 키를 LRU 캐시에 저장하고 `VerifyWithSigningKey`로 재검증. Cold 대비 Hot 경로 ~4× 성능 향상(17µs → 4µs).

## [0.0.4] - 2026-04-18

### Removed (post-release review)
- **CRC Migration 분류 코드 제거** — `ErrCRCMissing`, `ErrLegacyShard`, `ShardStatus.Migration`, `ScrubStats.MigrationRewrites`, `grainfs_scrub_migration_rewrites_total` 메트릭 제거. `stripVerifyCRC` 의 "too short" 케이스도 `ErrCRCMismatch` 로 통합. 실제 legacy shard 감지 경로가 존재하지 않아 dead code 상태였음.

### Fixed (post-release review)
- **PITR 스냅샷에 버킷 메타 포함** — 기존 Snapshot 포맷은 `bucket:` prefix(버전 상태, EC 플래그)를 담지 않아 PITR 복원 후 버킷이 기본값(`Unversioned`, `ECEnabled=true`)으로 리셋됐음. `storage.SnapshotBucket` / `BucketSnapshotable` 인터페이스 + `Snapshot.BucketMeta` 필드 추가, `ECBackend.ListAllBuckets/RestoreBuckets` 구현. 구형 스냅샷(BucketMeta=nil)은 Restore에서 no-op 처리(하위 호환).
- **GetObject/GetObjectVersion delete-marker 405 응답** — 특정 버전이 delete marker 일 때 `readAndDecode` 가 쓰레기 데이터를 반환하던 버그. `storage.ErrMethodNotAllowed` sentinel 추가. S3 스펙대로 `405 MethodNotAllowed` + `x-amz-delete-marker: true` + `x-amz-version-id` 헤더 반환.
- **HEAD ?versionId 지원** — `headObject` 가 versionId 쿼리 파라미터를 무시하던 문제 수정. `VersionedHeader` 인터페이스 + `ECBackend.HeadObjectVersion` 추가. delete marker 에 대한 HEAD 도 405 로 응답.
- **PUT ?versioning Status=Unversioned 거부** — S3 스펙상 `Status` 는 `Enabled`/`Suspended` 만 유효. `Unversioned` 를 400 `InvalidArgument` 로 거부.
- **ListVersions XML 선언 prepend** — `GET /<bucket>?versions` 응답에 `<?xml version="1.0" encoding="UTF-8"?>` 헤더 추가 (일부 S3 클라이언트의 파서 호환성). Owner/StorageClass 필드는 IAM/ACL 통합 이후 TODO.

### Added
- **ListObjectVersions API (4e)** — `GET /<bucket>?versions` → `ListVersionsResult` XML (Version/DeleteMarker 분리). `ObjectVersionLister` 인터페이스로 ECBackend에서 lat: 포인터 기반 latest 판별. LocalBackend → 501.
- **Versioning-aware Scrubber + Snapshot (4f)** — `ScanObjects`에서 delete marker 건너뛰기 + versioned key UUID 파싱. `ShardPaths(bucket, key, versionID, total)` 시그니처로 versioned shard 정확한 경로 조회. `SnapshotObject`에 VersionID/IsDeleteMarker 추가, `ListAllObjects`에서 versioned key 올바른 파싱 + delete marker 제외.
- **Versioning-aware DeleteObject (4d)** — Enabled 버킷에서 DELETE 시 delete marker(UUID4, IsDeleteMarker=true) 생성 및 lat: 포인터 업데이트. getObjectMeta가 delete marker 감지 시 ErrObjectNotFound 반환.
- **GET /<bucket>/<key>?versionId=<id> (4c)** — `VersionedGetter` 인터페이스로 특정 버전 직접 조회. PUT 응답에 X-Amz-Version-Id 헤더 설정.
- **Bucket Versioning API (4a)** — `PUT /<bucket>?versioning`으로 버전 상태 설정(Enabled/Suspended), `GET /<bucket>?versioning`으로 현재 상태 조회. ECBackend에서 protobuf `BucketMeta.versioning_state` 필드로 영속화. 미지원 백엔드는 501.
- **Dashboard health 엔드포인트** — `GET /admin/health/badger` (BadgerDB LSM/vlog 크기), `GET /admin/health/raft` (Raft node 상태, commit/applied index), `GET /admin/buckets/ec` (bucket별 EC 활성 여부). 모두 `localhostOnly()` 적용.

### Fixed
- **ListObjects 버전 버킷 중복 반환 수정** — 버전 활성 버킷에서 `ListObjects`가 동일 키를 버전 수만큼 중복 반환하던 버그 수정. 최신 비-delete-marker 버전만 반환하도록 lat: 사전 로드 후 필터링.
- **DeleteObjectVersion 최신 버전 선택 오류 수정** — 최신 버전 하드삭제 시 남은 버전 중 `lat:` 포인터를 UUID 알파벳 순(UUIDv4는 랜덤)이 아닌 `CreatedNano` 기준 최고값으로 선택. `ECObjectMeta.created_nano` (proto field 11) 추가 — 기존 레코드는 `last_modified × 1e9` 폴백.
- **isLocalhostAddr 주소 패턴 버그 수정** — `strings.HasPrefix("127.0.0.10:9000", "127.0.0.1")` 가 true 로 잘못 평가되던 버그를 `net.SplitHostPort` + 정확한 호스트 문자열 비교로 수정.
- **DeleteObjectVersion 샤드 삭제 오류 무시 수정** — `os.RemoveAll` 실패 시 에러를 묵살하던 코드를 `slog.Warn` 로깅으로 수정 (메타데이터는 이미 커밋됨).
- **putObjectData 데드 코드 제거** — streaming 전환 후 호출처 없는 함수 삭제.
- **RestoreObjects 멀티버전 lat: 정확성 수정** — `SnapshotObject.IsLatest` 필드 추가. `ListAllObjects`에서 `lat:` 포인터를 읽어 IsLatest 마킹, `RestoreObjects` 포스트패스에서 IsLatest 기준으로 lat: 복원. 동일 초 내 3회 PUT 시 UUID 정렬 순서로 lat:가 잘못 설정되던 버그 수정.
- **RestoreObjects plain 객체 복원 수정** — EC 샤드 디렉터리뿐 아니라 `.plain/` 플랫 파일도 존재 확인하도록 stale 판별 로직 확장. DataShards=0(소형 객체) 복원 시 stale 오분류 버그 수정.
- **RestoreObjects 고아 lat: 포인터 정리** — 삭제 패스에서 스냅샷에 없는 versioned key의 `lat:` 포인터도 함께 삭제. DB 팽창 방지.
- **RestoreObjects 구형 스냅샷 하위 호환성** — `IsLatest` 필드 없는 구형 스냅샷 복원 시 max-Modified 폴백으로 lat: 포인터 복원. 필드 추가 전 생성된 스냅샷으로 PITR해도 GetObject 동작 보장.
- **Versioning 버그 4종 수정 (Advisor review)** — `RestoreObjects` versioned key 지원(lat: 포인터 복원 포함), `ListObjectVersions` nested key UUID 휴리스틱 적용(unversioned 버킷 오탐 방지), `DeleteObjectVersion` 하드삭제 구현, `CachedBackend.DeleteObjectReturningMarker` 캐시 무효화 추가.
- **DELETE ?versionId=<id>** — 특정 버전 하드삭제 HTTP 엔드포인트. shard 제거 + lat: 포인터 갱신. `ObjectVersionDeleter` 인터페이스로 ECBackend 연결.
- **DELETE soft-delete marker ID 반환** — `VersionedSoftDeleter` 인터페이스, `x-amz-version-id` / `x-amz-delete-marker` 헤더 응답으로 S3 호환성 확보.
- **ECBackend.PutObject OOM 제거** — `io.ReadAll(r)` → 2-pass spool-to-disk 스트리밍. body → 단일 tempfile(ETag 동시 계산) → StreamEncoder.Split/Encode → 샤드 tempfile 직렬 처리. 비암호화 경로 peak ~32KB(`streamWriteShardCRC`), 암호화 경로 peak ~shardSize×2(AES-GCM 블록 연산 특성상 불가피).
- **CompleteMultipartUpload OOM 제거** — part bytes.Buffer 조립 → io.MultiReader+동일 스풀 경로 통합.

## [0.0.3] - 2026-04-18

### Added
- **Self-healing MVP** — background EC shard scrubber (`--scrub-interval`, 기본 24h). 누락·손상 shard 자동 감지 후 Reed-Solomon으로 복구. `GET /admin/health/scrub`으로 상태 확인.
- **CRC32 shard footer** — 모든 EC shard에 4바이트 CRC32-IEEE footer 기록/검증. bit-rot 감지.
- **Crash-safe WriteShard** — tmp+fsync+rename+dir-fsync 패턴으로 전원 손실 시 partial shard 방지.
- **RWMutex per-key locking** — 스크러버 Verify는 RLock (클라이언트 GET 동시 허용), Repair는 Lock (exclusive).
- **Scrub metrics** — `grainfs_scrub_shard_errors_total`, `grainfs_scrub_repaired_total`, `grainfs_ec_degraded_total`, `grainfs_scrub_objects_checked_total`, `grainfs_scrub_skipped_over_cap_total` Prometheus 지표 추가.

### Changed
- **`--scrub-interval` CLI flag** — `grainfs serve --scrub-interval=24h` (기본값). `0` 으로 비활성화.
- **EC shard format** — CRC32 footer 추가로 기존 shard(CRC 없음)는 scrubber에서 corrupt로 감지됨. 첫 scrub cycle에 자동 rewrite.

## [0.0.2] - 2026-04-18

### Added
- **Pull-through caching** - `--upstream` 플래그로 S3 호환 업스트림 지정, 로컬 캐시 미스 시 자동 fetch·캐시 저장
- **Migration injector** - `grainfs migrate inject` 명령으로 S3→GrainFS 대량 이관 지원 (`--skip-existing`)
- **PITR API** - `POST /admin/pitr` 엔드포인트로 특정 시점 복원 지원 (RFC3339 타임스탬프)
- **Snapshot 자동화** - `--snapshot-interval` 기본값 0 → 1h로 변경. **업그레이드 주의**: 기존
  사용자는 업그레이드 후 자동 스냅샷이 시작됩니다. 비활성화하려면 `--snapshot-interval=0`.
- **Snapshot retention fix** - 수동 스냅샷(`reason != "auto"`)이 `maxRetain` 초과 시 자동 삭제되던 버그 수정.
  이제 `auto` + 빈 reason(legacy)만 정리 대상.
- **Pull-through streaming** - 대형 업스트림 객체를 io.ReadAll 대신 2-pass 스트리밍으로 캐싱하여 OOM 위험 제거.
- **localhostOnly IPv6-mapped** - `[::ffff:127.0.0.1]` 형식을 localhost로 인식하도록 `isLocalhostAddr` 함수 도입.
- **Admin 보안 강화** - 모든 `/admin/*` 엔드포인트에 `localhostOnly()` 미들웨어 적용

### Fixed
- **S3 익명 자격증명** - 빈 access-key/secret-key 전달 시 AWS SDK가 거부하던 문제 수정 (`aws.AnonymousCredentials{}` 사용)
- **Snapshot 싱글톤** - `snapshot.Manager`를 요청마다 생성하던 문제 수정 (Server 초기화 시 1회 생성, seq 충돌 방지)
- **Cache-Control 헤더** - 인증 설정 시 `private, no-store`, 미설정 시 `public, max-age=3600` 조건부 응답
- **Snapshot manager 초기화 실패 로깅** - 초기화 실패 시 `slog.Warn` 출력

## [0.0.1] - 2026-04-17

### Added
- **NFSv4 buffer optimization** - 적응형 버퍼 풀(32KB/256KB/1MB)을 도입하여 대용량 파일 처리 성능 2-3x 개선
  - sync.Pool 기반 버퍼 재사용으로 메모리 사용량 감소 및 GC 압박 완화
  - io.ReadAll 대신 adaptive buffered streaming으로 대용량 파일 처리 최적화

### Changed
- **NFSv4 READ operations** - 100MB 파일 기준 4,109 MB/s 처리량 달성 (목표 100MB/s의 41배)
- **NFSv4 WRITE operations** - 1MB 단일 쓰기 크기 제한 검증 추가 (NFSv4 RFC 7530 준수)
- **Resource management** - Seek/non-seeker fallback 경로 개선으로 storage 호환성 강화

### Testing
- **E2E performance tests** - 10MB/50MB/100MB 파일 읽기/쓰기 throughput 검증
- **Unit tests** - 버퍼 풀 concurrent access safety 검증
- **Benchmarks** - buffered copy 성능 베치마크 추가

### Observability
- **Prometheus metrics** - NFSv4BufferPoolGets, NFSv4BufferPoolMisses, NFSv4BufferSizeInUse 메트릭 추가
