# Changelog

## [0.0.172.0] - 2026-05-13 — refactor: remove `--badger-managed-mode` flag

### Removed

- `serve --badger-managed-mode` flag and `Config.BadgerManagedMode`. Raft log GC
  via quorum watermark is now the only mode. GrainFS is pre-1.0 with no
  deployments using the non-managed (no-GC) layout, so the toggle and the
  mismatch guard were dead weight.
- `recover cluster --badger-managed-mode` flag. Recovery always writes a managed
  store.
- `cluster.RecoverClusterOptions.BadgerManagedMode` field and the
  `managed-mode mismatch` check in `BuildRecoverClusterPlan`.
- `raft.InspectManagedModeReadOnly` — no callers remain.

### Changed

- `--raft-log-gc-interval` (default 30s) is kept as an operational tuning knob.
- `docs/badger-managed-mode-rollback.md` updated to reflect always-on behaviour.

## [0.0.171.0] - 2026-05-13 — feat(migration): deep JobStore + leader-only Worker + FSM apply seam

### Added
- `migration.JobStore`: BadgerDB 기반 job 상태 (`StatusRunning/Complete/Failed`) 및 cursor 저장소. `jobPrefix`/`cursorPrefix` 별도 keyspace로 분리.
- `migration.Worker`: trigger 채널 기반 leader-only job executor. 시작 시 running 잡 재개 + `Trigger()` 호출 또는 ticker로 pending 잡 처리.
- `migration.Service`: `LeadershipSignal` 구독으로 leader/follower 전환 시 Worker 자동 start/stop. `SubmitJob` → Raft proposal → Worker trigger 흐름.
- `migration.Source.ListObjectsPage`: cursor 기반 페이지네이션 API. 기존 `ListObjects` 대체 (cursor="": 처음부터, next="": 마지막 페이지).
- FSM payload encode/decode (`payload.go`): `JobStart`/`JobDone`/`JobFailed` big-endian 바이너리 인코딩.
- `MetaCmdType` 상수 추가: `MetaCmdTypeMigrationJobStart` (37), `MetaCmdTypeMigrationJobDone` (38), `MetaCmdTypeMigrationJobFailed` (39).
- `MetaFSM.applyMigrationJobStart/Done/Failed`: FSM apply 메서드. `time.Now()` 미사용 — payload에서 디코딩한 타임스탬프만 사용 (결정론적 FSM 불변조건 준수).
- `MigrationProposer`: `cluster` 패키지의 Proposer 어댑터. proposal 시점에 `time.Now().UnixNano()` 기록 (FSM 외부, determinism-safe).
- `serveruntime` 부트 배선: `JobStore`, `MigrationProposer`, `RaftLeadership` 연결; `go state.migrationSvc.Run(ctx)`.
- `MigrationInterval` config 필드 추가.

### Known Limitations
- 실행 중인 버킷에 `SubmitJob` 재호출 시 `Copied`/`StartedAt` 리셋 (cursor 보존). "잡 재시작" 의미론으로 의도된 동작.
- `ProposeJobDone` 직전 크래시 시 커서 처음부터 재시작 (S3 PUT 멱등성으로 데이터 무결성 유지).

## [0.0.170.0] - 2026-05-13 — refactor: remove --nbd-volume-size flag and NBD auto-volume creation

### Removed
- `--nbd-volume-size` CLI flag: NBD 서버 시작 시 "default" 볼륨을 자동으로 생성하던 동작을 제거. 오퍼레이터가 NBD 서버 기동 전 볼륨을 명시적으로 생성해야 함. 볼륨 없이 클라이언트 연결 시 오류를 명확히 반환.
- `EnsureDefaultNBDVolume` 함수 및 `Config.NBDVolumeSize` 필드 제거.

## [0.0.169.0] - 2026-05-13 — feat: always-cluster mode + solo data guard for grainfs join

### Changed
- `clusterMode` 항상 true: 단일 노드 포함 모든 `grainfs serve` 실행에서 클러스터 모드 활성화. `--cluster-key`가 모든 모드에서 필수 (기존에는 멀티노드 모드에서만 필수). **Breaking**: cluster key 없이 solo 기동 불가.

### Added
- `grainfs join --force`: solo 노드에 사용자 데이터가 있을 때 데이터 가드를 우회하여 join 강제 실행. `--force` 없이 데이터가 있으면 409 `data_present` 반환.
- `JoinHandler` 데이터 가드: solo 노드에 버킷 등 사용자 데이터가 있고 `force=false`이면 409 `data_present` 응답으로 join 거부. 데이터 손실 방지용 안전 장치.
- `TestE2E_Bootstrap_DataPresent_BlocksJoin`: solo 노드에 버킷 생성 후 force 없이 join 시도 시 409를 반환하는지 검증하는 e2e 테스트.

### Fixed
- `grainfs join` 409 응답을 친화적 CLI 출력으로 디코딩: `status`, `message` 필드를 파싱해 사용자에게 `--force=true` 힌트 포함 메시지 출력.
- e2e 스냅샷 테스트 interval 500ms → 1s: `cluster-config` 검증 로직이 1s 미만 값을 거부하므로 수정.

## [0.0.168.0] - 2026-05-13 — refactor(lifecycle): deepen Store.put seam + Apply→Worker round-trip test

### Changed
- `Store.Put()` → unexported `Store.put()`: FSM apply 경로(`PutRaw`)를 우회하는 public write 경로 제거. 운영 코드에서는 반드시 Raft proposal(`ProposeLifecyclePut` → FSM → `PutRaw`)을 통해서만 쓰기 가능.
- `ListBuckets()` 주석 정리: "pre-FSM-era leftover" 문구 제거.
- `reconcile()` 리더십 획득 로그 메시지 단순화.

### Added
- `fakeProposerWithStore` 테스트 헬퍼: `PutRaw`를 직접 호출해 동기 FSM apply를 시뮬레이션, 실제 Raft 노드 없이 Apply→Worker 라운드트립 검증 가능.
- `TestService_Apply_ThenWorkerProcesses`: Apply() 후 Worker가 실제로 만료 객체를 삭제하는 end-to-end 모듈 불변 조건 테스트. race detector 통과 확인.

## [0.0.167.0] - 2026-05-13 — feat(cluster): runtime UDS join + .join-pending boot simplification

클러스터 join 워크플로우 단순화. 모든 노드가 동일한 `grainfs serve` 명령으로 기동.
새 노드 추가는 런타임 UDS admin API(`grainfs join <peer>`)로 단일 명령 처리.

### Added
- `grainfs join <peer> [--endpoint <sock>]`: 실행 중인 노드에 join을 요청하는 새 CLI 명령.
  - UDS admin socket으로 `POST /v1/cluster/join`을 호출.
  - 이미 멀티노드 클러스터 멤버 → `already_member` no-op.
  - peer가 자기 자신 → `already_member` no-op.
  - solo 상태 → `.join-pending` 파일 기록 후 graceful restart 트리거.
- `POST /v1/cluster/join` admin UDS 엔드포인트 (`JoinHandler`).
- `clusterNodes` 인터페이스: `JoinHandler`가 클러스터 멤버십을 조회하는 minimal 인터페이스 (production: `*cluster.MetaRaft`, test: `fakeClusterNodes`).
- `.join-pending` 파일 기반 부팅 시 join 감지: `grainfs serve` 시작 시 데이터 디렉터리에 파일이 있으면 자동으로 join 모드로 진입.
- `wipeSoloRaftState()`: join 전 solo Raft 상태를 `.pre-join-backup`으로 이동 (롤백 가능).
- e2e 테스트: `TestE2E_Bootstrap_JoinUDS_AlreadyMember`, `TestE2E_Bootstrap_JoinCLI_Idempotent`.

### Removed
- `grainfs serve --peers <addr,...>`: 다중 노드 정적 피어 목록 플래그 제거.
- `grainfs serve --join <addr>`: serve 시 즉시 join 플래그 제거.
- `grainfs cluster join`: 별도 프로세스로 join하던 서브커맨드 제거. `grainfs join`으로 대체.

### Changed
- `grainfs serve`: 모든 노드가 동일한 명령으로 기동. 시드 노드와 팔로워 노드 구분 없음.
  - 기존 Raft 상태 존재 → 재연결 모드.
  - Raft 상태 없음 + `.join-pending` 없음 → solo bootstrap.
  - `.join-pending` 존재 → join 모드 (한 번만 처리 후 파일 삭제).

### Deferred (follow-up PR)
- Phase 1.3: `clusterMode` 항상 `true` 고정 (solo 부팅 시에도 `--cluster-key` 필수화). 단위 테스트 ~18개 + e2e 인프라 수정 범위.
- Phase 3.2: solo 노드에 사용자 데이터 있을 때 `--force` 없이 join 방지 가드.

## [0.0.166.0] - 2026-05-13 — feat(cluster): forward operation atomicity — leader handles index commit

follower가 storage write를 leader로 포워딩한 후 index commit을 follower에서 별도로 수행하던 방식을 제거.
이제 ForwardReceiver가 leader 측에서 storage write + index commit을 원자적으로 처리하므로,
두 단계 사이 크래시로 인한 고아 shard 생성 가능성이 없어짐.

### Changed
- `ForwardReceiver`: `PutObject`, `PutObjectStream`, `CompleteMultipartUpload`, `DeleteObject` (delete-marker), `DeleteObjectVersion` 핸들러에서 리더 측 `ProposeObjectIndex`/`ProposeDeleteObjectIndex` 호출 추가.
- `ClusterCoordinator`: 포워딩 후 중복으로 수행하던 index commit 5곳 제거.
- `bootWALAndForwarders`: `indexProposer`를 공유 로컬 변수로 추출하여 `ForwardingObjectIndexProposer`와 `ForwardReceiver` 양쪽에 전달.

### Added
- `internal/cluster/forward_receiver_test.go`: 5개 포워딩 경로의 index-propose 검증 테스트 10개.
- `internal/cluster/backend_test.go`, `cluster_coordinator_test.go`: 판별 테스트 각 1각.

## [0.0.165.0] - 2026-05-13 — feat(reshard): separate ring-reshard-interval from reshard-interval

ring reshard(EC 오브젝트 ring topology 마이그레이션)를 EC reshard(N×→EC 변환/프로파일 업그레이드)에서 분리.
`--reshard-interval=0` 설정 시 correctness-critical ring reshard까지 꺼지던 버그 해결.
`--scrub-interval`, `--reshard-interval`, `--ring-reshard-interval` 세 인터벌 모두 always-on;
0으로 설정 시 default 복원 + warn 로그.

### Added
- `NewRingReshardManager()`: `ecReshardEnabled=false` 플래그로 ring-only 모드 동작.
- `runRingOnly()`: EC 오브젝트 중 `RingVersion` 불일치 항목만 `ReshardToRing()` 호출.
- `ValidateRequiredIntervals()`: scrub/reshard/ring-reshard 인터벌이 0이면 default 복원 + warn 로그.
- `--ring-reshard-interval` flag (default `1h`): ring reshard 전용 주기.
- `--datagroup-refresh-interval` flag (default `1m`): DataGroup 갱신 주기.

### Changed
- boot phase: 기존 `if cfg.ReshardInterval > 0` guard 제거. ring/EC manager 항상 시작.
- ring manager(`ReshardManagerRegistry`)와 EC manager를 독립 인스턴스로 분리.
- `runRingOnly` 루프 안에 leader check 추가 (EC path와 동일한 leadership transfer 처리).

### Fixed
- **race condition** (`raft.Node`): `SetNoOpCommand`가 `runLeader` 고루틴과 `noOpCmd` 필드를 경쟁하던 data race 수정. `n.mu`로 write/read 모두 보호.

## [0.0.164.0] - 2026-05-13 — refactor(cluster): extract EC object reader

`DistributedBackend`에 흩어져 있던 EC 읽기 경로(~450줄)를 `ecObjectReader` 모듈로 추출.
`ecObjectWriter` 패턴을 대칭적으로 따르며, 단위 테스트 8개 추가.

### Changed
- `ecObjectReader` struct 신설(`internal/cluster/ec_object_reader.go`):
  - 공개 메서드: `ReadObject`, `OpenObject`, `ReadAt`
  - 의존성 인터페이스: `ecObjectShardFetcher`, `ecObjectShardCache`
  - compile-time interface assertion: `var _ ecObjectShardFetcher = (*ShardService)(nil)`
- `DistributedBackend`에 `newECObjectReader()` 헬퍼 추가 — typed-nil 캐시 가드 포함
- `DistributedBackend`의 EC 읽기 메서드들이 `newECObjectReader()` 위임 호출로 단순화

### Tests
- `ec_object_reader_test.go` 신설: AllLocal, FallsBackToParityShard, MarksUnhealthyPeerOnFetchError, ErrorsWhenNotEnoughShards, UsesCache, ReadAt_ReturnsCorrectRange, ReadAt_PastEOFReturnsEOF, NilShardService_ReturnsError 총 8개

## [0.0.163.0] - 2026-05-13 — refactor(volume): split blockIOEngine.write into planner + executor

`blockIOEngine.write()` 의 라우팅 결정 로직과 I/O 실행 로직을 분리.
`blockIOPlanner`(read-only, HeadObject/dedup.ReadBlock으로 `[]BlockAction` 결정)와
`blockIOExecutor`(write-only, `[]BlockAction` 소비)로 분리. 테스트 커버리지 대폭 확대.
partial block write 시 HeadObject + GetObject(2 round-trip) 대신 GetObject만 호출하도록
성능 회귀도 함께 수정.

### Changed
- `blockIOEngine.write()` → `blockIOPlanner.planWrite()` + `blockIOExecutor.executeWrite()` 로 분리.
  라우팅 결정(ActionDirect/ActionDedup/ActionCow)이 뮤테이션 없는 단일 진입점에 집중됨.
- `blockIOExecutor.executeWrite()` 시그니처에서 `off` 파라미터 제거; `WriteAt` 분기는 `PreferWriteAt()` guard로 이동.

### Fixed
- **성능 회귀** (partial block HeadObject): `poolQuota=0` 일 때 partial block write가 HeadObject(존재 확인)를 불필요하게 호출하던 문제 수정. 이제 `isFullBlk || poolQuota > 0` 조건에서만 HeadObject 호출.
- `executeDirectAsync` partial-block 경로에서 `io.ReadFull` / `rc.Close` 에러를 무시하던 버그 수정 (silent zero-pad 데이터 손상 가능성).
- CoW 신규 블록의 OldKey를 불필요하게 캐시 무효화하던 spurious invalidation 제거.

### Tests
- `block_io_planner_test.go`: DirectNewBlock, DirectExistingBlock, AsyncEligible, CowMode, DedupMode, QuotaExceeded/Ok, MultipleBlocks, PartialBlockAtOffset, CowExistingBlock, DedupExistingBlock, PartialDirectNoHeadObject, PartialCowNoHeadObject, DedupReadBlockError 총 13개 케이스.
- `block_io_executor_test.go`: DirectNew/Existing, InvalidatesCache, AsyncCollectsCommitFn, DedupNew/Duplicate/ToDelete, CowWritesToCowKey/NewBlockSkipsRead, DirectWriteAtPath, DirectPartialBlock, AsyncDirectPartialBlock, PartialDirectNewBlockCountsAllocation, InvalidateAllNilCache 총 12개 케이스.

## [0.0.162.0] - 2026-05-12 — feat(cluster-config): snapshot-interval / snapshot-retain → cluster config

자동 스냅샷 정책을 노드 실행 flag에서 cluster config(Raft-replicated + admin API hot-patchable)로 이주. 운영 중 RPO 조정 시 노드 재시작 불필요.

### Breaking changes
- `--snapshot-interval` / `--snapshot-retain` 플래그 제거. 이제 cluster config 키로 관리:
  ```
  curl --unix-socket <data>/admin.sock -X PATCH http://./v1/cluster/config \
       -H "Content-Type: application/json" \
       -d '{"snapshot-interval":"30m","snapshot-retain":12}'
  ```
  기본값 동일 (interval=1h, retain=24). `snapshot-interval=0`이면 auto-snapshot 비활성. 비활성 ↔ 활성 토글은 같은 PATCH 경로로 런타임 변경 가능 (재시작 불필요).

### Added
- `snapshot-interval` / `snapshot-retain` cluster config 키 (FBS schema + defaults + getter + admin GET/PATCH + Raft replicate + validate). `SourceForKey()`도 두 키 지원.
- `internal/snapshot/auto.SnapshotPolicy` 인터페이스. `AutoSnapshotter`는 매 tick에 `policy.SnapshotInterval()` / `policy.SnapshotRetain()` 재조회 → PATCH 적용 latency 최대 5s (idle bound).
- forward-compat decoder 테스트 (`TestClusterConfigCodec_SnapshotForwardCompatEmptyPayload`): 새 필드 없는 buffer 디코드 시 default fallback.
- e2e harness helper `patchSnapshotInterval(t, dataDir, dur)` — 5 주요 진입점에서 부팅 직후 `"0s"`로 자동 비활성.

### Changed
- `internal/snapshot/AutoSnapshotter` 시그니처: `(interval, retain)` 스칼라 → `(policy SnapshotPolicy, idleWhenDisabled time.Duration)`. `time.NewTicker` → `time.After + idle 단위 chunked wait` 패턴 (PATCH→effect latency를 interval에 무관하게 idle로 제한).
- `internal/serveruntime.StartAutoSnapshotterWhenReady` 시그니처: `(interval, retain)` 제거, `cfg *cluster.ClusterConfig` 추가.
- `serveruntime.Config.SnapInterval` / `SnapRetain` 필드 제거.

### Notes
- Mixed-version rolling upgrade 시 구버전 노드는 새 키 무시 → 정책 노드별 차이 가능. 업그레이드 완료 후 cluster config로 재설정 권장. 후속 작업은 `TODOS.md > Cluster Day-2 Operations > snapshot-config v1.1 rolling-upgrade gap`에서 추적.
- `meta_fsm_cluster_config.go` audit log dict는 snapshot 키 patch를 아직 emit하지 않음 — `TODOS.md`의 같은 항목에서 후속 보완.

## [0.0.161.0] - 2026-05-12 — fix(raft/v2): preserve learner state across snapshots

### Fixed
- Raft v2 snapshots now preserve learner membership when snapshots are created, transmitted, installed, and used to reconstruct configuration after restart.
- Followers can now install a fresher snapshot after already compacting to a prior snapshot boundary.
- Malformed AppendEntries batches with an internal index gap are rejected before any partial log mutation.

### Added
- Regression coverage for learner-preserving snapshot creation/install, adapter suffrage translation, repeated Badger snapshot boundary installs, and atomic AppendEntries batch rejection.

## [0.0.160.0] - 2026-05-12 — cluster-config v1.1 hardening (peerUID + webhook loud-fail + CB hot-reload + deferred tests)

Slice 1 (PR #305, v0.0.155.0)의 cluster-wide config가 출하한 4가지 deferred 항목을 모두 해결. ACL/eager rewrap 등 spec out-of-scope 항목은 별도 슬라이스로 분리.

### Changed
- **Admin UDS audit log에 실제 peer uid 기록.** `peerCredListener`가 admin UDS conn을 wrap해서 `SO_PEERCRED` (linux) / `LOCAL_PEERCRED` (darwin)로 ucred를 읽고, Hertz 미들웨어가 `*peerCredAddr` typed addr를 통해 `WithPeerCred(ctx, ...)`로 context에 전파. `cluster_config_patch_received` 감사 로그가 이제 `actor_uid` + `actor_uid_resolved` 필드를 emit. FreeBSD/Windows 빌드는 `actor_uid_resolved:false`로 영구 표시 + boot 시 `peercred_unsupported` warn.
- **Webhook signature decrypt 실패 가시화.** `Dispatcher.resolveLive`가 `DecryptWithAAD` 실패 시 `grainfs_webhook_signature_decrypt_failure_total{alert_kind,err_class}` 카운터 증가 + 1분당 1회로 rate-limit된 warn 로그 emit. Alert 전송은 unsigned로 계속 진행 (DoS-by-rotation 방지). `classifyDecryptErr`가 실제 encrypt 패키지의 AEAD 태그 실패 메시지 (`"message authentication failed"`)를 `aad_mismatch`로 분류.
- **BalancerCBThreshold 핫리로드가 기존 breaker에 도달.** `circuitBreaker`에서 threshold 필드 제거 (state minimization) — `update(ns, thresholdPct)`가 호출당 threshold를 인자로 받음. `syncCB`는 매 gossip tick에서 `clusterCfg.BalancerCBThreshold() * 100`을 다시 읽음. Slice 1이 유일하게 핫리로드 no-op이었던 키가 이제 동작.
- **`BalancerProposer.active`/`tickCount`를 atomic으로 변환** — `atomic.Bool`/`atomic.Int64`로 race-clean. 테스트 fake (`fakeBalancerCfg`)의 hot-reload 가능한 필드도 `atomic.Value`로 wrap. `go test -race ./internal/cluster/...` clean pass.

### Added
- **Audit integration test** (`internal/clusteradmin/cluster_config_handler_audit_test.go`, linux/darwin only): admin UDS dial → PATCH → 감사 로그가 `os.Getuid()` + `actor_uid_resolved:true` 캐리하는지 검증.
- **Webhook decrypt-failure tests** (`internal/alerts/webhook_decrypt_failure_test.go`): 단일 실패 → metric+log+unsigned delivery 검증, 100-burst → metric=100/log=1 rate-limit 검증, 실제 `*encrypt.Encryptor` key-rotation classification 검증, bounded-enum cardinality 보호.
- **CB threshold hot-reload tests**: `circuitBreaker` 단위 (`circuit_breaker_test.go::TestCircuitBreaker_ThresholdHotReloadAcrossUpdates`) + `BalancerProposer.syncCB` 통합 (`balancer_cb_threshold_test.go`).
- **BalancerImbalanceTriggerPct 핫리로드 테스트** (`balancer_hot_reload_test.go`): trigger를 낮추면 같은 imbalance에서도 `Active()`가 `true`로 전환되는지 검증.
- **AlertWebhook URL+secret 핫리로드 테스트** (`internal/server/alerts_api_hot_reload_test.go`): real `*cluster.MetaFSM` + real `*encrypt.Encryptor` + 두 `httptest.Server` receiver로 회전 후 새 secret HMAC 서명 일치 검증. 계획상 e2e 형태에서 integration 형태로 다운스코프 (cluster boot 비용 회피, 동일 코드 경로 커버).

### Notes
- `DiskWarnThreshold` 핫리로드 검증은 Slice 1의 `TestDiskCollector_Threshold_HotReload` (PR #305에 동봉)이 이미 커버 — 추가 코드 불필요.
- `cluster rotate-key` eager rewrap (spec의 (e) 항목)은 별도 슬라이스로 분리 — secret 종류별 rewrap 정책, 백그라운드 워커, 진행률 보고, 실패 복구 등 설계 공간이 별개.
- ACL/uid 화이트리스트는 이 슬라이스 범위 밖 — peerUID는 audit-only이며 authorization은 admin UDS group ownership (0660 + chown :admin) 유지.

## [0.0.159.0] - 2026-05-12 — refactor(cluster): EC object writer owns the full data path

### Changed
- EC object writes now route single-local writes, in-memory shard materialization, spooled shard materialization, and legacy data-slice writes through the private `EC Object Writer` module.
- `DistributedBackend` now keeps placement selection and Raft metadata commits while delegating EC data-plane side effects, cleanup, retry, and peer-health marking to the writer.
- `CONTEXT.md` now describes `EC Object Writer` as the owner of shard materialization and single-local fast-path writes, not just shard-reader fan-out.

### Removed
- Removed the old `DistributedBackend` EC shard streaming helpers that duplicated writer retry and cleanup behavior.

### Added
- Added writer-level coverage for single-local shard header/hash encoding, spooled shard materialization, and direct data-shard object facts.

## [0.0.158.0] - 2026-05-12 — refactor(cluster): EC object shard writes move behind private writer

### Changed
- EC memory-shard object writes now run local/remote shard fan-out through a private `EC Object Writer` module. The metadata commit still stays in `DistributedBackend`, so the Raft mutation ordering remains unchanged while shard-write side effects become easier to test.
- The new writer owns cleanup of shards written before a write failure and peer-health marking from remote shard outcomes. This concentrates EC write-all failure behavior behind one internal test surface.
- `CONTEXT.md` now records the `EC Object Writer` domain term and its current slice: shard-reader write execution first, with shard materialization and single-local fast path still left in `DistributedBackend`.

### Added
- Unit coverage for the partial-failure path where a remote shard write fails after a local shard succeeded. The test verifies the original write error is preserved and the already-written local shard is cleaned up.

## [0.0.157.0] - 2026-05-12 — refactor(server): clusterStatus emits typed adminapi.Status

### Changed
- `server.clusterStatus` 핸들러가 `map[string]any` 빌드 대신 `adminapi.Status` struct로 직접 emit. 서버와 클라이언트가 동일한 typed schema 공유 — wire-shape drift 위험 제거.
- `adminapi.Status`에 `Degraded bool` (always-emit), `ObjectIndexSummary *ObjectIndexSummary` (closed-schema mirror) 필드 추가. `Mode`/`Degraded`/`DownNodes`는 zero value에도 의미가 있어 omitempty 제거.
- `adminapi.ObjectIndexSummary` 새 타입 (`cluster.ObjectIndexSummary` mirror): adminapi 패키지가 cluster를 import 하지 않는 closed-schema 원칙 유지.
- `internal/server/golden_fixture_test.go` `assertGoldenJSON` 헬퍼가 응답·골든 양쪽을 unmarshal→sort-key marshal 후 비교. struct 필드 선언 순서 vs map 알파벳 순서 차이에 robust.

### Removed (BREAKING — wire schema)
- `/v1/cluster/status` 응답에서 `split_brain_suspected` 필드 제거. 항상 하드코딩 `false`였고 내부 소비처 0건 — `grainfs_split_brain_suspected` Prometheus 메트릭은 그대로 유지되어 모니터링 경로 무영향.

## [0.0.156.0] - 2026-05-12 — refactor(adminapi): Details → json.RawMessage + typed structs

### Changed
- `adminapi.Error.Details` 타입이 `map[string]any` → `json.RawMessage`로 전환. 캘러는 endpoint별 typed struct에 `json.Unmarshal(e.Details, &d)` 한 줄로 디코드 — typed-everything 일관성 완성, `map[string]any` 타입어설션 보일러 제거.
- `clusteradmin.RemovePeerError`/`TransferLeaderError`가 새 `RemovePeerErrorDetails`/`TransferLeaderErrorDetails` typed struct를 embed. `e.LeaderID`/`.VotersAfter`/`.Retry` 외부 API는 Go field promotion으로 그대로 동작 — caller 무변경.
- `clusteradmin.intField` helper 폐기. JSON number → int 변환은 `encoding/json`이 typed struct 필드 타입에 따라 자동 처리.
- `volumeadmin.AsDeleteConflict`/`AsResizeUnsupported`가 `remapJSON`(map→bytes→typed) 우회 단계 제거하고 `json.Unmarshal` 직접 호출.
- `adminapi.Transport.parseErrorBody` legacy flat-shape 경로: 원본 body bytes를 그대로 `Details`에 할당 (zero-alloc; typed struct가 `code`/`error` 키 자연 무시).
- `server/admin.NewConflict`/`NewUnsupported` 헬퍼가 입력 map[string]any를 내부에서 marshal 후 RawMessage로 채움 — 호출 시그니처는 보존, 호출부 무변경.
- wire bytes 무변경 — JSON 응답 모양 동일. 모든 변경은 Go 측 표현형 (map → bytes + typed struct).

## [0.0.155.0] - 2026-05-12 — BREAKING: cluster-wide policy moved to admin API

Cluster-wide policy values are now stored in MetaFSM (Raft-replicated) and
configured exclusively through the admin UDS (`/v1/cluster/config` GET/PATCH).
Per-node flag overrides for these values are no longer possible — divergence
across nodes is now structurally impossible — and changes take effect at the
next consumer tick without restart.

### Removed flags

    --balancer-enabled
    --balancer-imbalance-trigger-pct, --balancer-imbalance-stop-pct
    --balancer-migration-rate, --balancer-migration-max-retries,
      --balancer-migration-pending-ttl
    --balancer-leader-tenure-min, --balancer-warmup-timeout
    --balancer-cb-threshold, --balancer-gossip-interval
    --alert-webhook, --alert-webhook-secret
    --disk-warn-threshold, --disk-critical-threshold

### New: `grainfs cluster config`

    grainfs cluster config show
    grainfs cluster config get <key>
    grainfs cluster config set <key>=<value> [<key>=<value> ...]
    grainfs cluster config reset <key> [<key> ...]
    grainfs cluster config diff

All commands talk to the admin UDS (default `<data>/admin.sock`).

### Migration

1. Remove the removed flags from systemd units / ansible playbooks. The cluster
   boots with the documented defaults (unchanged from v0.150).
2. If you previously overrode any value, after the cluster is up:

    grainfs cluster config set \
      balancer-imbalance-trigger-pct=15 \
      alert-webhook=https://hooks.slack.com/... \
      alert-webhook-secret=<secret>

### Key rotation note

The cluster-wide `alert-webhook-secret` is wrapped at `cluster config set`
time using the current cluster key. After `cluster rotate-key` completes
and the `previous.key` grace window ends, a wrapped value that was set
before the rotation will fail to decrypt — alerts will stop firing.
Re-issue `grainfs cluster config set alert-webhook-secret=<secret>` after
rotation (any time before the grace window closes) to refresh the wrap.

### Default reference

| Key                                  | Default |
| ------------------------------------ | ------- |
| balancer-enabled                     | true    |
| balancer-imbalance-trigger-pct       | 20.0    |
| balancer-imbalance-stop-pct          | 5.0     |
| balancer-migration-rate              | 1       |
| balancer-leader-tenure-min           | 5m      |
| balancer-warmup-timeout              | 60s     |
| balancer-cb-threshold                | 0.90    |
| balancer-migration-max-retries       | 3       |
| balancer-migration-pending-ttl       | 5m      |
| balancer-gossip-interval             | 30s    |
| alert-webhook                        | (none)  |
| alert-webhook-secret                 | (none)  |
| disk-warn-threshold                  | 0.80    |
| disk-critical-threshold              | 0.90    |

## [0.0.154.0] - 2026-05-12 — refactor(clusteradmin): extract BaseOptions to match volumeadmin

### Changed
- `internal/clusteradmin/operations.go`가 `BaseOptions{Endpoint, Timeout, Stdout, Stderr, Stdin}` struct를 추출. `RemovePeerOptions`/`PeersOptions`/`EventsOptions`가 이를 embed하면서 5 공통 필드를 인라인 반복하던 비대칭을 제거 — `volumeadmin.BaseOptions` 패턴과 정렬.
- 동작 무변경: strict validation(`Stdout==nil`/`Timeout<=0` → 즉시 에러)은 그대로 유지. helper(`clientFor`/`withTimeout` 등)는 도입 안 함 — volumeadmin의 관대한 fallback과 의도적으로 다름.
- wire/CLI flag 무변경. cmd/grainfs 호출부 3곳 + 테스트 17곳이 `BaseOptions: BaseOptions{...}` 명시 nested 형태로 갱신.

## [0.0.153.0] - 2026-05-12 — refactor: unify admin client transport via adminapi

### Changed
- `internal/adminapi`가 admin HTTP API의 단일 출처로 확장됨. wire types(`Status`, `Health`, `PlacementReport`, `BalancerStatus`, `Event`, `PeerLivenessRow`, ...), generic `Transport` (UDS/HTTP dispatch + `Do/Get/Post/Delete/GetRaw`), generic `Error` envelope(`Status`/`cause`/`Unwrap()`) 모두 한 패키지에 모음.
- `volumeadmin.Client`와 `clusteradmin.Client`가 `*adminapi.Transport`를 embed해서 약 250 LOC의 중복 transport 코드 제거. CLI 호출 인터페이스(`NewClient`, endpoint method 시그니처)는 변경 없음.
- `server`가 balancer 응답을 `adminapi.BalancerStatus`로 직접 emit. `cluster.PeerLivenessRowsToWire()`가 typed enum → plain string 변환을 단일 지점에서 처리.
- CONTEXT.md "Admin API Wire Schema" 단락이 transport + error envelope 책임까지 포괄하도록 갱신.

### Added
- 4개 admin endpoint(`cluster/status`, `cluster/health`, `cluster/placement`, `cluster/balancer/status`) wire byte를 캡처하는 golden file characterization 테스트. 향후 admin wire 변경 시 자동 회귀 검출.
- `adminapi.Error`에 `IsCode(err, code)` 헬퍼 + `WithCause(err)` 빌더. `errors.Is(err, context.Canceled)`이 typed envelope을 통과해 transport-level cause를 보게 됨.
- `clusteradmin.RemovePeerError`/`TransferLeaderError`가 `*adminapi.Error`를 embed 또는 wrap하는 typed sub-error wrapper로 재구성. `parseRemovePeerError`/`parseTransferLeaderError` 파서로 `Details` map → typed 도메인 필드 lift.

### Fixed
- balancer 응답의 zero-time `JoinedAt`/`UpdatedAt`이 이전엔 `"0001-01-01T00:00:00Z"`로 emit되던 것을 `omitempty`로 누락 처리. `TestBalancerWire_ZeroTime_OmitsField` 회귀 테스트로 고정. 의도된 wire 변화 — missing field == empty time이라 모든 합리적 JSON 소비자에 호환.

## [0.0.152.0] - 2026-05-12 — feat(raft/v2): M6.0 — AddLearner + PromoteToVoter (Path B)

### Added
- `raft/v2` learner support. `AddLearner(id, addr)` registers a non-voting observer via a single-phase ConfChange — quorum math is byte-for-byte unchanged. `PromoteToVoter(id)` runs the two-entry Path B sequence: drop-from-learners → joint AddVoter. Catchup-gated by `cfg.LearnerCatchupThreshold`; returns `ErrLearnerNotCaughtUp` when the learner is lagging.
- New v2 sentinels exported: `ErrLearnerNotCaughtUp`, `ErrNotALearner`, `ErrAlreadyLearner`. The cluster adapter maps the catchup sentinel to a new mirrored `raft.ErrLearnerNotCaughtUp` (the v1 package retains it for caller compatibility).
- New `effectiveConfig.learners map[string]string` (id → address). Voter slices (`voters`, `oldVoters`) are NEVER mutated by learner operations, so the existing joint quorum math (`quorumOK`, `commitOK`, `quorumOKByRound`) and joint encoder shape are unchanged.

### Internal
- Path B: learners ride out-of-band of joint encoder. New `replicaSet()` (voters ∪ learners − self) drives broadcastHeartbeat + the post-propose dispatch. Quorum-shaped sites (RequestVote, TransferLeadership target, becomeLeader peer setup) keep using the voter-only `peerSet()`.
- ConfChange single-phase wire format bumped to version 0x02; carries an `Op` tag + (optional) learner target + the full resulting voters / learners snapshot. The joint encoder (version 0x01, kind 0x01) is UNCHANGED.
- v2 BadgerLogStore stamps a schema version on first open. Opening any pre-M6.0 store with entries or compaction meta but no stamp fails fast with a migration hint.
- v2 BadgerSnapshotStore encoding bumped to 0x02 with a learners section after the voter list.

### BREAKING — on-disk schema bump

Pre-M6.0 v2 deployments cannot upgrade in place. Operators must:

1. Drain or accept service interruption (best on a quiesced cluster).
2. Stop every grainfs node.
3. Wipe each node's `<dataDir>/raft-v2/` directory (the log + stable + snapshot keyspaces under that prefix). The FSM-state DB (`<dataDir>/shared-fsm/`) is untouched.
4. Re-bootstrap the cluster (`grainfs cluster bootstrap` or equivalent) and rejoin nodes.

v2 has been the production raft since M5 PR 28b (2026-05-12), so the soak window is short. Single-node and small-cluster operators can recover via re-bootstrap; multi-node production with retained FSM state needs the same wipe applied per node before they rejoin.

### Operational notes for M6.0

- A leader that crashes between the PromoteToVoter stage-1 commit and stage-2 dispatch leaves the target ID in neither voters nor learners (orphan). `pendingPromote` is leader-local state and does NOT survive failover. Recovery is operator-driven: re-issue `AddLearner` + `PromoteToVoter` for the orphaned ID after the new leader stabilises. A future revision may synthesize a fix-up entry at `becomeLeader` time; for now the workaround is documented here.
- `LearnerCatchupThreshold` defaults to 0 (gate disabled in v2 — promote at proposal time without lag check). Set it to a non-zero entry count to enforce the catchup gate.

## [0.0.151.0] - 2026-05-12 — chore: trim serve flags (hide no-encryption/direct-io, remove quic-mux)

### Changed
- `--no-encryption` and `--direct-io` are now hidden flags. Both still work and behave exactly as before — they're escape hatches operators normally shouldn't touch (encryption-off for benchmarks/recovery; `--direct-io=false` for filesystems that reject O_DIRECT). Hiding them keeps `serve --help` focused without removing the capability.

### Removed
- `--quic-mux` CLI flag. Multiplexed QUIC raft RPCs are now unconditionally on (78% drop in CPU samples / 17x fewer recvmsg syscalls vs the legacy per-message path; the per-peer ALPN fallback to the legacy path for older peers is retained). `--quic-mux-pool` and `--quic-mux-flush` remain as tuning knobs for the always-on mux path.

## [0.0.150.0] - 2026-05-12 — chore: remove dead --raft-log-fsync flag

### Removed
- `--raft-log-fsync` CLI flag. It was a no-op: nothing ever read it, and the "auto" behavior its help text described (cluster=false / single=true) was never implemented. Raft-log durability is unchanged — the v1 BadgerLogStore still opens with `SyncWrites=true`, and the v2 LogStore (the default) opens with `SyncWrites=false` plus explicit `Sync()` at consensus-critical points, which is already the smarter version of what the flag promised.

## [0.0.149.0] - 2026-05-12 — feat(cluster): per-node shared FSM-state DB (C2 P3)

### Changed
- Per-group `<dataDir>/groups/<id>/badger/` FSM-state BadgerDBs consolidated into one per-node `<dataDir>/shared-fsm/` DB. Each raft group's keys carry a 4-byte-big-endian-length || groupID prefix via the new (package-private) `cluster.stateKeyspace` (byte-identical encoding to `raft.OpenSharedLogStore`'s P0b prefix). `GroupLifecycleConfig.FSMStore` is now a required field (the per-group `OpenStateDB` injection seam is gone — mirrors how #290 made `LogStore` required). `state.distBackend` uses the shared DB with a `"group-0"` keyspace via `cluster.NewDistributedBackendForGroup`. A legacy `groups/*/badger/` dir is IGNORED on startup (pre-1.0, no migration).
- `FSM.Snapshot` / `FSM.Restore` are now group-prefix-scoped: a snapshot's keys are group-relative; restore drops only the calling group's prefix (or whole-DB for empty keyspace) and rewrites with the group prefix. Restore validates before mutating (refuses wrong `FormatVersion`, refuses already-prefixed keys) so a rejected/corrupt snapshot leaves existing state intact. A new `FormatVersion uint8` field on the raft snapshot-meta record (last field of the FlatBuffers `SnapshotMeta`, so old records decode with `0`) carries the version; this binary writes `2`.
- `grainfs recover-cluster` now requires the source data dir's last FSM snapshot to be `format_version == 2`; pre-P3 dirs are rejected with a clear error. See `docs/recover-cluster.md`.
- Bucket lifecycle policy keys (`lifecycle:{bucket}`) now live in the shared FSM DB unprefixed (`state.distBackend.FSMDB()` switched DBs as part of the wiring) — process-global namespace, no collision with group-prefixed keys (the 4-byte length prefix guarantees this). Pre-1.0, no existing lifecycle data.

### Added
- `make lint-keyspace`: lint gate that rejects raw `[]byte("bucket:"|"obj:"|"lat:"|"mpu:"|"placement:"|"policy:"|"bucketver:"|"pending-migration:"|"quarantine:")` literals reaching Badger ops in `internal/cluster/` — forces FSM-state keys through `stateKeyspace`. Hooked into `make lint`.
- `resourcewatch.DBCategorySharedFSM` + `badgerrole.RoleSharedFSM` for the new per-node shared FSM-state DB.
- Invariant test suite: prefix-isolation across all FSM/backend paths, pathological group IDs (length ≥ 256, NUL bytes, prefix-of-prefix), group-close-doesn't-close-shared-DB, snapshot containment, restore replaces only own group, restore rejects wrong `FormatVersion` / already-prefixed keys / corrupt bytes (decode-before-drop), empty-keyspace whole-DB replace, restart persistence, restore-crash-mid-DropPrefix self-heals on reboot via the durable snapshot (kill-point hook).

### Performance
- Per-node BadgerDB instance count goes from 2N+1 to 3 (shared raft-log from P0b + shared FSM-state from this PR + the process-level meta DB). The expected wins (boot ceiling at high N, idle goroutines / RSS) are documented in `docs/architecture/badger-consolidation.md` §"Status: FINALIZED"; the full idle-N8/N16/load-N8/N16/N32 matrix sweep is deferred to a quiet-host run (host contention invalidates the numbers).

### Removed
- `GroupLifecycleConfig.OpenStateDB` field + `OpenGroupStateDBFunc` type (the per-group state-DB injection seam is gone).
- Per-group `<dataDir>/groups/<id>/badger/` directories (the shared FSM DB is the only layout).

## [0.0.148.0] - 2026-05-12 — feat(raft/v2): M5 PR 29 remove GRAINFS_RAFT_V2 flag + raftv2adapter cleanup

### BREAKING — operator-facing

- The `GRAINFS_RAFT_V2` environment variable is no longer read. Setting
  `GRAINFS_RAFT_V2=off` (the former escape hatch back to raft v1) has NO
  effect — raft v2 is now the only path for both `serveruntime` and
  `cluster`. Operators on v2 since PR 28b stay on v2; there is no longer
  a way to revert to v1 short of rolling back the binary. PR 30 deletes
  `internal/raft/` (the v1 package) outright.

### Removed

- `internal/cluster/raftflag.go` — entire file deleted. `IsV2Enabled`,
  `ParseRaftV2Flag`, `resetRaftV2FlagForTest`, `raftV2DefaultOnPkgs`,
  `raftV2FlagOff`, `raftV2FlagEnv` are gone.
- `cluster.RaftV2Snapshotter` interface — folded into `cluster.RaftNode`.
  `DistributedBackend.TriggerRaftSnapshot` and `RaftSnapshotStatus`
  dispatch through `b.node.CreateSnapshot` / `b.node.SnapshotStatus`
  directly. The `raftSnapshotRequest.v2Snap` field is gone; there is no
  longer a v1-vs-v2 branch in `completeRaftSnapshotRequest`.
- `internal/cluster/backend.go::triggerRaftV2SnapshotInApplyLoop` (the v2
  counterpart of the v1 SnapshotManager dispatcher) is gone — its body is
  inlined into `triggerRaftSnapshotInApplyLoop`, which is now the only
  apply-loop snapshot path.

### Changed

- `internal/cluster/raftfactory.go::newRaftNode` always constructs the v2
  adapter via `newRaftNodeV2`. The `logStore` parameter is accepted for
  source-compat with PR 28b callers and ignored; PR 30 removes the
  parameter when v1's `LogStore` type disappears.
- `internal/serveruntime/run.go` — drops the v1/v2 if/else for raft node
  construction. Always builds the v2 node via
  `cluster.NewRaftV2NodeForServeruntime` + `cluster.NewRaftQUICRPCTransport`.
- `internal/serveruntime/boot_phases_services.go::bootSnapshotAndApplyLoop`
  — drops the `raft.SnapshotManager` wiring entirely. raftv2 owns snapshot
  lifecycle internally (SnapshotStore + CreateSnapshot + InstallSnapshot)
  so the v1 manager is no longer needed.
- `internal/serveruntime/boot_phases_storage_runtime.go` — the per-group
  mux registration is a single `state.groupRaftMux.Register(entry.ID,
  gb.Node())` call; the v1/v2 dispatch branch is gone.
- `internal/raft/group_transport_quic.go` — collapsed
  `Register(groupID, *Node)` + `RegisterV2(groupID, RaftV2Handler)` into a
  single `Register(groupID, RaftV2Handler)` entry point with a nil-handler
  guard. `*Node` (v1) still satisfies `RaftV2Handler` so internal v1
  tests inside `internal/raft/` compile.
- Renamed `cluster.RaftV2QUICRPCTransport` →
  `cluster.RaftQUICRPCTransport` (and the file
  `raftv2_quic_rpc.go` → `raft_quic_rpc.go`). v2 is the only path; the
  `V2` disambiguator is noise.
- `internal/cluster/raftnode.go` — `RaftNode` interface gains
  `CreateSnapshot` and `SnapshotStatus`. v1's `*raft.Node` still satisfies
  the interface via panicking stubs in `internal/raft/v2compat.go` so
  v1-specific test files (in `internal/cluster/backend_test.go`,
  `cluster_coordinator_test.go`, `degraded_monitor_test.go`,
  `group_backend_test.go`, etc.) still compile. PR 30 deletes the v1
  package and these stubs together.

### Added

- `internal/raft/v2compat.go` — temporary compile-only shim that adds
  `*raft.Node.CreateSnapshot` and `*raft.Node.SnapshotStatus` panicking
  stubs so v1 `*raft.Node` continues to satisfy the cluster.RaftNode
  interface. Deleted by PR 30.

### Test churn

- `internal/cluster/raft_v2_smoke_test.go` — `TestRaftV2Smoke_*` tests
  that asserted flag-gated behavior (`OffEscapeHatchDisablesV2`,
  `ClusterFlagSelectsV2`, `OtherPkgFlagDoesNotAffectCluster`,
  `ParseFlag`, `DefaultsAreV2`) are gone. Two tests remain:
  `TestRaftV2Smoke_DefaultClusterIsV2` (newRaftNode returns the v2
  adapter) and `TestRaftV2Smoke_BootstrapProposeRoundtrip` (single-node
  v2 round-trip).
- `internal/cluster/group_lifecycle_test.go::TestInstantiateLocalGroup_UsesGroupIDAsElectionPriorityKey`
  retired — assertion shape required v1's `*raft.Node.ElectionPriorityKey()`
  accessor and there is no equivalent on cluster.RaftNode.
- `internal/cluster/backend_test.go::TestDistributedBackend_SnapshotTriggersAfterThreshold`,
  `TestDistributedBackend_TriggerRaftSnapshotLeader`,
  `TestDistributedBackend_TriggerRaftSnapshotSerializesWithApplyLoop`,
  `TestDistributedBackend_TriggerRaftSnapshotRejectsFollower` — all
  `t.Skip`'d. They exercise v1 `raft.SnapshotManager` semantics; PR 30
  deletes the v1 package and these tests together.
- All remaining test files: `t.Setenv("GRAINFS_RAFT_V2", …)` +
  `resetRaftV2FlagForTest` calls stripped (the flag is gone, v2 is the
  default).
- `internal/cluster/testbackend.go::NewSingletonBackendForTest` migrated
  to the v2 path (`newRaftNode` instead of `raft.NewNode`).
- `internal/serveruntime/boot_phases_services_test.go::TestBootSnapshotAndApplyLoop_PopulatesState`
  — assertion flipped: `state.snapMgr` stays nil because the v1
  SnapshotManager is no longer wired.

## [0.0.147.0] - 2026-05-12 — feat(raft/v2): M5 PR 28b cluster default-on + per-group QUIC mux v2 bridge

### Changed

- `internal/cluster/raftflag.go` — `cluster` joins `serveruntime` in
  `raftV2DefaultOnPkgs`. With `GRAINFS_RAFT_V2` unset, both packages now
  default to raft v2. The phased flip is complete; PR 29 removes the flag
  entirely. `GRAINFS_RAFT_V2=off` remains the operator escape hatch.
- `internal/raft/group_transport_quic.go` — `GroupRaftQUICMux.nodes`
  sync.Map is retyped from `*Node` to a new local `RaftV2Handler` interface
  (`HandleRequestVote` / `HandleAppendEntries`). Both v1 `*Node` and the
  cluster-layer v2 adapter satisfy this shape, so inbound dispatch is
  engine-agnostic. The `Register` method keeps its `*Node` signature for v1
  callers; v2 callers use the new `RegisterV2(groupID, RaftV2Handler)` entry
  point. Defined locally inside `internal/raft` to avoid an import cycle on
  `internal/cluster`.
- `internal/raft/group_transport_mux.go` — `lookupNode` returns the
  `RaftV2Handler` interface so all three dispatch sites (legacy `handleRPC`,
  mux-mode `handleMuxRequest`, and heartbeat-coalescer `dispatchToLocalGroup`)
  route v2 inbound RPCs through the adapter. The `metaNode atomic.Pointer`
  is unchanged — meta-raft v2 goes through the cluster-layer `StreamControl`
  bridge (PR 27) and bypasses this mux entirely.
- `internal/serveruntime/boot_phases_storage_runtime.go` — the per-group mux
  registration now selects between `Register` (v1, `gb.RaftNode()` non-nil)
  and `RegisterV2` (v2, falls back to `gb.Node()` interface) at the call
  site, fixing the typed-nil-deref that would otherwise crash v2 group raft
  the first time a peer delivered a RequestVote.

### Added

- `internal/cluster/raftv2_group_mux_test.go` — three-node real-QUIC
  coverage for per-group v2 raft elections and replication
  (`TestV2GroupMuxCluster_ThreeNode_ElectsLeader`,
  `TestV2GroupMuxCluster_ThreeNode_Propose_Replicate`,
  `TestV2GroupMuxCluster_ThreeNode_MuxMode_Propose_Replicate`). The
  mux-mode variant exercises the heartbeat-coalescer dispatch site so the
  third dispatch path is covered too.

### Removed / Replaced

- `TestRaftV2Smoke_DefaultClusterIsV1` and `TestRaftV2Smoke_DefaultServeruntimeIsV2`
  were renamed to `TestRaftV2Smoke_DefaultClusterIsV2` and
  `TestRaftV2Smoke_DefaultsAreV2`, with assertions flipped to match the
  PR 28b default. The `ParseFlag` table's empty-env row now expects
  `cluster=true`.

## [0.0.146.0] - 2026-05-12 — feat(raft/v2): M5 PR 28 default GRAINFS_RAFT_V2=serveruntime on (off escape hatch retained)

### Changed

- `internal/cluster/raftflag.go` — phased per-package default-on flip. With
  `GRAINFS_RAFT_V2` unset, `serveruntime` now selects raft v2 by default; the
  `cluster` package still selects v1 until PR 28b wires the group-raft mux
  through the v2 RPC bridge (`raftV2DefaultOnPkgs` tracks the per-package
  rollout — PR 29 removes the flag entirely once v2 is validated).
- `internal/cluster/lifecycle_adapters.go` — `RaftLeadership.Subscribe` no
  longer subscribes to the v1 observer pattern (which the v2 adapter no-ops);
  it now polls `State()` every 500ms. Latency for lifecycle reconcile is
  bounded by the poll interval and works for both v1 and v2. The
  `raftNodeAccess` interface drops `RegisterObserver`/`DeregisterObserver`.
- `internal/cluster/backend.go` — adds `(*DistributedBackend).Node() RaftNode`
  so production callers that need leadership/state queries can use a
  v1/v2-agnostic surface (the existing `RaftNode() *raft.Node` v1-only
  accessor stays for true v1-internal paths like the snapshot manager).
- `internal/serveruntime/boot_phases_srvopts.go` — `RaftLeadership` is now
  wired against `state.distBackend.Node()` (interface) rather than
  `RaftNode()` (v1 concrete), fixing the nil-deref panic that surfaced when
  serveruntime=v2 boots the lifecycle service.

### Added

- `GRAINFS_RAFT_V2=off` operator escape hatch. Disables raft v2 for every
  package (reverts serveruntime back to v1) — the production bail-out if v2
  exhibits a regression. PR 29 removes the flag entirely; until then this is
  the documented rollback.

### Notes

- Backward compat preserved: `GRAINFS_RAFT_V2=cluster`,
  `GRAINFS_RAFT_V2=serveruntime`, `GRAINFS_RAFT_V2=all`, and unknown-token
  warnings all behave exactly as in PR 26/27.
- The 3-node QUIC v2 cluster smoke
  (`TestV2QUICCluster_ThreeNode_ElectsLeader`,
  `TestV2QUICCluster_ThreeNode_Propose_Replicate`) still passes.

## [0.0.145.0] - 2026-05-11 — refactor(serve): remove `--shared-badger` flag

### Removed

- `serve --shared-badger` flag and `Config.SharedBadgerEnabled`. The C2 P0b
  shared raft-log layout (one `<dataDir>/shared-raft-log/` BadgerDB viewed by
  every data group's Raft log via a 4-byte length-prefixed key namespace) has
  been the default since v0.0.13.0 and is now the only layout. GrainFS is
  pre-1.0 with no deployments on the legacy per-group `groups/*/raft/` layout,
  so the toggle and its legacy-detection startup guard were dead weight.
- `bootOpenSharedRaftLogDB`'s `groups/*/raft/` legacy-layout refusal — there is
  no longer any way to produce that layout.
- `GroupLifecycleConfig.OpenLogStore` / `OpenGroupLogStoreFunc` — unused
  indirection.
- The per-group raft-log layout itself: `instantiateLocalGroup` no longer
  auto-creates a `<dataDir>/groups/*/raft/` BadgerDB when `LogStore` is unset —
  `GroupLifecycleConfig.LogStore` is now **required** (production passes a
  shared-log view via `raft.OpenSharedLogStore`; `internal/cluster` tests pass
  `raft.NewBadgerLogStore` explicitly). Nothing creates `groups/*/raft/` anymore.
- `GRAINFS_PERF_SHARED_BADGER` forwarding in `cluster_perf_profile_test.go`.

### Unchanged

- The meta-Raft log store at `<dataDir>/raft/` (`RoleMetaRaftLog`) — never
  governed by this flag.
- Per-group FSM-state BadgerDB at `<dataDir>/groups/*/badger/` — C2 P3, still
  paused (see `docs/architecture/badger-consolidation.md` / `TODOS.md`).

## [0.0.144.0] - 2026-05-11 — Lifecycle / Cluster follow-ups

### Fixed

- raft: a node no longer adds its own address to `config.Peers` when applying
  (or replaying from the log) its own `AddVoter`/`Promote` ConfChange. The
  phantom self-entry inflated `currentVoters` (`n.id ++ config.Peers`), so a
  3-node `ClusterModeDynamicJoin` cluster required a 3-of-4 majority and two
  survivors could not re-elect after a leader died.
  (`internal/raft/membership.go`: `applyConfChangeLocked`, `rebuildConfigFromLog`.)
- `clusterpb`: `cluster.fbs` now declares `MetaCmdType.BucketLifecyclePut` (34) /
  `BucketLifecycleDelete` (35). PR #284 hand-edited the generated `MetaCmdType.go`
  but left the `.fbs` source untouched, so `make fbs` (run by `make build`)
  clobbered the hand-edit and the build failed.
- `cluster transfer-leader`: `RaftClusterInfo` exposes `IsLeader`/`TransferLeadership`,
  fixing the `503 "cluster adapter does not support transfer-leader"` regression
  from the OpRouter extraction (#277). NOTE: the endpoint now returns 200, but the
  meta-Raft `TransferLeadership` handoff itself is still incomplete (no
  `SendTimeoutNow` plumbing) — tracked in `TODOS.md`; `TestE2E_ClusterTransferLeader`
  is skipped pending that work.

### Added

- `internal/lifecycle.Service.Status()` surfaces executor worker stats (running
  flag, `LastRun`, `ObjectsChecked`, `Expired`, `VersionsPruned`) plus the list
  of buckets with a locally-persisted lifecycle config (ADR 0011 deferred surface);
  exposed via `GET /api/cluster/lifecycle/status` (mirrors `/api/cluster/balancer/status`).
- `internal/lifecycle.Store.ListBuckets()` iterates the `lifecycle:` prefix.
- On leadership acquire, the lifecycle service logs the buckets that have a config
  in the local store, so operators can spot pre-FSM-era leftover `lifecycle:{bucket}`
  keys (remediation stays manual — re-apply the policy — per ADR 0011).
- E2E: `TestE2E_DynamicJoinTwoSurvivorReelect` (3-node dynamic-join, SIGKILL the
  leader, two survivors must re-elect — regression for the quorum-inflation fix).

## [0.0.143.0] - 2026-05-11 — fix(cluster): GetObjectVersion reconstructs EC-stored versions

- fix(cluster): `DistributedBackend.GetObjectVersion` now reconstructs erasure-coded objects from their shards before falling back to a plain data file — mirroring `GetObject`. Previously it only tried `os.Open(objectPathV)` then the legacy unversioned path, so any versioned read of an EC-stored object (S3 `GET ?versionId=`, `CopyObject` from a versioned source, object-index reconcile against an EC bucket) failed with `open versioned object: ... no such file or directory` — the bytes live as shards, not a plain file. Adds `placementMetaForVersion` (reads the per-version meta, extracts the same `RingVersion`/`ECData`/`ECParity`/`NodeIDs` `PlacementMeta` that `headObjectMeta` builds) and wires the same `shardSvc → ResolvePlacement → getObjectECReaderAtShardKey` block. Non-EC and legacy objects fall through unchanged (`ResolvePlacement` returns `ErrNotEC`); a meta-read error here propagates rather than masking the real problem behind the plain-file `no such file`.

## [0.0.142.0] - 2026-05-11 — fix(cluster): ListAllObjects tolerates unreadable blobs (PITR snapshot resilience)

- fix(cluster): `ClusterCoordinator.ListAllObjects` no longer aborts the whole listing when a single object's data file can't be opened. It opens each blob only to enrich ETag/Size/ContentType; on failure it now logs a warning and falls back to the version-listing metadata instead of returning an error. Previously one unreadable blob (e.g. `__grainfs_volumes/__vol/default/meta` mid-boot, or an EC-stored object with no plain-file fallback in `GetObjectVersion`) made `Manager.Create()` fail on every PITR auto-snapshot tick after the first — surfacing as a flaky `TestAutoSnapshot_CreatesSnapshotAutomatically` under load. A metadata snapshot must succeed even on a partially-degraded cluster.

## [0.0.141.0] - 2026-05-11 — feat(lifecycle): Bucket Lifecycle Policy deep module (ADR 0011)

### Added

- `internal/lifecycle.Service` — the deep module for the Bucket Lifecycle Policy
  domain: validates incoming policy, replicates it through the meta-Raft FSM,
  persists it, and runs the leader-only expiration executor. Single seam for
  server handlers (`Apply`/`Get`/`GetRaw`/`Delete`/`Enabled`/`Status`/`Run`).
- `MetaCmdType` `BucketLifecyclePut` (34) / `BucketLifecycleDelete` (35) carrying
  the per-bucket policy as opaque S3 wire XML bytes; `lifecycle.Store.PutRaw`/
  `GetRaw` for byte-for-byte operator round-trip.
- `cluster.LifecycleProposer` + `cluster.RaftLeadership` adapters satisfying the
  `lifecycle.Proposer` / `lifecycle.LeadershipSignal` seams.
- `MetaRaft.SetForwarder` — follower→leader propose forwarding via the meta-Raft
  forward path so an S3 `PUT ?lifecycle` on any node replicates correctly.
- E2E: `TestLifecycle_FollowerPutLeaderGet`, `TestLifecycle_LeaderChangePreservesConfig`.

### Changed

- `server.WithLifecycleStore` → `server.WithLifecycleService`. S3 lifecycle
  handlers call the service; they no longer reach the raw BadgerDB store.
- Lifecycle config changes are now replicated via meta-Raft (ADR 0011) instead
  of a direct local BadgerDB write — fixes silent loss on follower PUT and on
  leader change.
- `cluster.LifecycleManager` removed; its leader-only reconcile loop is now
  `lifecycle.Service.Run`.

### Fixed

- `MetaRaft` now wires a `MetaCmdTypeNoOp` via `node.SetNoOpCommand` (Raft
  §5.4.2): a freshly elected meta-Raft leader commits an entry in its own term,
  unlocking backlogged previous-term entries (e.g. lifecycle config written by
  the prior leader).

## [0.0.140.0] - 2026-05-11 — fix(storage/pullthrough): forward Snapshotable through the pull-through decorator

- fix(storage/pullthrough): `pullthrough.Backend` now implements `storage.Snapshotable`/`storage.BucketSnapshotable` (and `Unwrap()`) by delegating to the wrapped backend. It only embedded `storage.Backend`, which does not promote the snapshot interfaces — so on the serve path (`pullthrough(wal(ClusterCoordinator))`) the backend chain stopped satisfying `Snapshotable`. Effect: `GET /admin/snapshots` returned 500 ("backend does not support snapshots") and the PITR auto-snapshotter was silently skipped on single-node serve. Regression since the pull-through layer entered the boot chain. Fixes `TestAutoSnapshot_CreatesSnapshotAutomatically`.

## [0.0.139.0] - 2026-05-11 — cli/test: --dedup flag deprecated; e2e workaround removed (PR-C of dedup+snapshot series)

- cli(serve): `--dedup` flag is now hidden and deprecated (`MarkHidden` + `MarkDeprecated`) — dedup is always enabled by default. The flag value is still honored (passing `--dedup=false` keeps the S3-backed `s3SnapshotStore` path) and will be removed entirely in v0.1.0.
- test(e2e): removed the `--dedup=false` workaround from the shared e2e server and 6 test files, plus the `GRAINFS_DEDUP` env gate. The e2e suite now runs with dedup on by default (matching production). `volume_scrub_test.go` retains explicit `--dedup=false`/`--dedup=true` calls — those are deliberate dedup-off-vs-on comparisons.

## [0.0.138.0] - 2026-05-11 — feat(volume): dedup-aware snapshot (PR-B of dedup+snapshot series)

- feat(volume): dedup+snapshot integration — `badgerSnapshotStore` implements the `SnapshotStore` interface for dedup-enabled volumes. Snapshot maps live in BadgerDB (`vd:s:` keyspace) with refcount-shared canonicals; lifecycle (Begin/AppendChunk/Commit/Abort/Delete/Rollback/Clone) uses chunked Badger transactions + state markers + MVCC views for crash safety.
- feat(volume): `Manager.RecoverOnBoot` reconciles in-progress snapshots, stuck rollbacks, and stuck clones on startup (idempotent re-apply).
- fix(volume): remove the `dedup + snapshots not supported in Phase A` hard error — the CLI default (`--dedup=true` + `--snapshot-interval=1h`) no longer breaks from the second snapshot onward.

## [0.0.137.0] - 2026-05-11 — raft/v2 M3 milestone COMPLETE — test harness + property suite + chaos

### M3 PRs 17-21 (Test Harness Phase)

**Phase summary:** raft v2 now has a comprehensive test harness covering property-based,
chaos, and v1-corpus-derived test coverage. The package's correctness story is now:
6 Raft invariants asserted under random op sequences + sustained chaos (kill/restart,
drop, reorder, partition) + categorized v1 corpus coverage.

### PR 17 — property test framework + 2 invariants

- `pgregory.net/rapid` v1.3.0 added (state-machine property tests).
- Election Safety + Leader Append-Only invariants.
- 100k random sequences clean (nightly: -rapid.checks=100000).

### PR 18 — Log Matching + Leader Completeness + State Machine Safety + Liveness

- 4 additional invariants (3 safety + 1 liveness).
- Liveness "Eventual Commit Under Stable Leadership" — checks that a stable 50-action
  suffix with leader present commits every successful Propose.

### PR 19 — v1 21-test corpus port

- Outcome: 10 GREEN (8 EXISTING + 2 PORTED) / 11 SKIPPED / 0 V2-BUG.
- Plan target ≥17 green not met because v2 architecturally simplified away v1 components
  (Batcher, HeartbeatCoalescer, PeerReplicator, GroupRaftQUICMux, RaftConn,
  membershipView, FlatBuffers wire codec) — those 11 SKIPPED files have legit
  "no v2 equivalent by design" rationale.
- Ported: persistence post-Stop RPC safety + Stop idempotency; concurrent
  Configuration() reads + Stop goroutine cleanup.
- See `internal/raft/v2/PORT_MATRIX.md` for the file-by-file matrix.

### PR 20 — chaos suite

- partitionNet extended with probabilistic drop (0-100%) + reorder delay.
- `chaosCluster` with KillNode/StartNode (BadgerDB recovery).
- 8-action `TestChaos_Sustained` with all 6 invariants asserted throughout.
- Make target `test-raft-v2-chaos`; nightly cadence via `RAFT_CHAOS_DURATION=30m`.

### PR 21 — perf snapshot + M3 close

- v2 benchmark harness: single-node Propose (no-fsync + Badger-default),
  3-voter in-process commit, 1 GiB InstallSnapshot.
- v1 vs v2 comparison deferred — redesign goal was safety/maintainability
  per plan §Driver, not throughput. Absolute v2 numbers below.

### v2 benchmark results (2026-05-11, Apple M3)

Methodology: `go test -bench=. -benchmem -benchtime=2s -count=3 -run '^$' ./internal/raft/v2`.
Snapshot bench: `-benchtime=1x -count=1` (setup dominates).

| Benchmark | ns/op (med) | B/op | allocs/op | Notes |
|---|---|---|---|---|
| ProposeWait_SingleNode_NoFsync | ~935 ns | ~660 B | 5 | memLogStore + memStableStore; pure actor overhead |
| ProposeWait_SingleNode_BadgerDefault | ~28 µs | ~2.4 KB | 57 | BadgerDB + explicit db.Sync() per Append+SaveHardState |
| ProposeAndCommit_3Voter | ~4.2 µs | ~2.3 KB | 24 | 3 in-process nodes via memNetwork; no real network |
| InstallSnapshot_1GiB | ~350 ms | 1 GiB | ~120 | in-memory copy; 1 GiB FSM blob; leader→follower install |

### M3 status: COMPLETE (5/5)

Q7 success criteria measured at M3:

- Race detector clean: yes (all v2 tests + chaos sustained 30s smoke clean)
- Dual-impl harness equivalence: yes (existing M1 PR 2-3 harness; PR 18 extended invariant coverage)
- Property tests 5 invariants: yes — 6 invariants implemented (5 safety + 1 liveness); 100k seqs clean nightly
- Throughput regression vs v1: SKIPPED (per plan §Driver — redesign goal is safety, not throughput)
- v1 21-test corpus port: yes — 21/21 categorized; 10 green + 11 SKIPPED with rationale
- Cyclomatic complexity: deferred to M5 close per PR 7 note
- Staging soak / mutex reduction: M4/M2 measured separately

Next: M4 (staging soak, feature flag, long-tail bug fix). M3 → M4 handoff blocking on user direction.

---

## [0.0.136.0] - 2026-05-11 — refactor(volume): SnapshotStore extraction (PR-A)

- refactor(volume): introduce SnapshotStore interface; move S3-backed snapshot ops to s3SnapshotStore (no behavior change)

## [0.0.135.0] - 2026-05-11 — refactor(cluster): extract OpRouter + LocalExecution from ClusterCoordinator

### Added

- `internal/cluster/op_routing.go` (new, 171 LoC): `OpRouter` — ctx-free routing
  module that resolves S3-level operations to placement-group targets.
  `RouteTarget` value type with exported fields (`GroupID`, `Peers`, `SelfIsLeader`,
  `SelfIsVoter`, `SelfIsOnlyVoter`) and `CanReadLocal()` predicate. Three methods:
  `RouteBucket(bucket)`, `RouteObjectRead(bucket, key, versionID)`,
  `RouteObjectWrite(bucket, key)`. Owns object-index lookup, internal-bucket
  bypass, write target selection via `SelectObjectPlacementGroup`, and peer
  address resolution through the address book.
- `internal/cluster/exec_policy.go` (new, 112 LoC): `LocalExecution` — ctx-aware
  sibling module that takes a `RouteTarget` plus intent and returns either a
  local `*GroupBackend` (use it) or `nil` (forward signal). Owns follower-read
  `ReadIndex+WaitApplied` deadline and self-only-voter leader-wait.
- `internal/cluster/op_routing_test.go` + `internal/cluster/exec_policy_test.go`
  (new, 346 LoC combined): unit tests for both modules including
  `TestLocalExecution_ResolveWrite_LeadershipFlipMidCall` regression guard.

### Changed

- `internal/cluster/cluster_coordinator.go` (1623 → 1446 LoC, -177 net): every
  bucket-scoped public method now routes through `c.opRouter.RouteXxx` and
  `c.localExec.ResolveXxx`. The legacy `(gb, ok, err)` 3-tuple from
  `localXxxBackend` collapses to `(gb, err)` where `gb != nil` means use it.
  19 methods migrated: `RestoreObjects`, `ListObjects`, `ListObjectVersions`,
  `WalkObjects`, `WriteAt`, `Truncate`, `PreferWriteAt`, `GetObject`,
  `GetObjectVersion`, `HeadObject`, `ReadAt`, `DeleteObjectReturningMarker`,
  `DeleteObjectVersion`, `CreateMultipartUpload`, `CompleteMultipartUpload`,
  `PutObject`, `UploadPart`, `AbortMultipartUpload`, `ListParts`.
  `internal/cluster/object_reconcile.go` also migrated
  (`ReconcileObjectIndexLatest`, `headObjectVersionAt`).
- `internal/cluster/group_backend.go`: added unexported `raftLeaderProbe`
  interface + `testLeaderProbe` field + `leaderProbe()` method as a minimal
  test seam for the F3 regression test. Production `RaftNode()` accessor
  unchanged.
- `CONTEXT.md`: two new domain sections — "Storage Op Routing" and
  "Local Execution Decision".

### Fixed

- **Route→execute leadership flip race (F3)**: `LocalExecution.ResolveWrite`
  re-checks `RaftNode().IsLeader()` at entry instead of trusting the
  potentially stale `target.SelfIsLeader` flag captured at route time. The
  legacy `localWriteBackend` could let a no-longer-leader node propose a
  write between OpRouter resolution and execution. Closed by 1 atomic load
  per write call; verified by `TestLocalExecution_ResolveWrite_LeadershipFlipMidCall`.

### Removed

- Legacy private helpers from `cluster_coordinator.go`: `routeBucket`,
  `routeGroup`, `routeDataGroupSnapshot`, `routeObjectLatest`,
  `routeObjectVersion`, `routeObjectWrite`, `localReadBackend`,
  `localWriteBackend`, `peersForForward`, `canReadLocal`, and the
  unexported `routeTarget` struct. Constants `defaultFollowerReadWait` and
  `defaultSelfOnlyLeaderWait` superseded by `localExecFollowerReadDeadline`
  and `localExecSelfOnlyLeaderWait` in `exec_policy.go`.
## [0.0.134.0] - 2026-05-11 — raft/v2 PR 16: Joint Consensus — atomic multi-server membership change (M2 7 of 7, M2 COMPLETE)

### Added

- `internal/raft/v2/confchange.go` (new, 394 LoC): wire format + state machine
  for joint consensus. `effectiveConfig{joint bool, voters, oldVoters []string}`
  represents single (`joint=false`) and joint (`joint=true`) states. Helpers:
  `commitOK(idx, matchIndex, selfID, selfMatch)` — joint-aware commit quorum
  check; `quorumOK(granted map[string]bool)` — vote-counting quorum;
  `quorumOKByRound(peerLastRound, selfID, minRound)` — ReadIndex round
  confirmation. `containsVoter(id)` — Cnew-only (used for self-removed leader
  stepdown); `containsAnyVoter(id)` — Cold ∪ Cnew (used for election eligibility
  per §4.3 "any server from either configuration may serve as leader"). Binary
  versioned encoding for ConfChange and JointConfChange entry payloads
  (length-prefixed BE). `reconstructConfig(snap, logStore, selfID, peers)`
  bootstrap helper walks snapshot voters + log entries to recover effective
  config across restarts.
- `internal/raft/v2/membership.go` (new, 394 LoC): leader-driven §4.3 flow.
  `handleConfChange` appends `LogEntryJointConfChange` (Cold ∪ Cnew),
  `advanceConfChangePhase` appends `LogEntryConfChange` (Cnew alone) after the
  joint entry commits. `truncateAndRevertConfig` reverts effective config on
  AE truncation (Diego thesis §4.3 — server uses config in latest APPENDED
  entry, not latest committed; reverts to last surviving entry on truncate).
  `recoverInFlightJoint` (called from `becomeLeader`) resurrects a
  `pendingConfChange` from a joint entry left by the previous leader so a
  leadership change mid-joint does not stall the cluster.
  `maybeStepDownAfterRemoval` gates self-removed leader stepdown on
  `appendedConfigIndex <= commitIndex` so the leader does not abandon the
  final entry's commit responsibility.
- `Node.AddVoter` / `Node.AddVoterCtx` / `Node.RemoveVoter` — replace
  `ErrNotImplemented` stubs at `node.go:436-448`. Route through the cmdCh
  with bounded reply channels and ctx/stopCh select. Reject concurrent changes
  with `ErrConfChangeInFlight` when `pendingConfChange != nil`, the previous
  config entry is uncommitted (`appendedConfigIndex > commitIndex`), or
  `currentConfig.joint` (defense-in-depth — `recoverInFlightJoint` covers
  the leader-churn case).
- New tests: `confchange_test.go` (228 LoC) — wire format roundtrip + helper
  semantics; `membership_test.go` (~396 LoC) — joint quorum requires both
  majorities, AddVoter/RemoveVoter happy paths, leader-churn joint recovery,
  AE truncation reverts effective config, InstallSnapshot resets effective
  config, self-removed-leader stepdown gated on commit, Cold-only voter can
  elect during joint state (regression test for §4.3 liveness).

### Changed

- `effectiveConfig` replaces `cfg.Peers` as the runtime source of truth for
  the voter set. `cfg.Peers` is now read only at bootstrap (in `NewNode`
  when no snapshot exists) — the v2 actor never reads it after start.
- Election/replication/commit/ReadIndex paths route through the new joint-aware
  helpers (`commitOK`, `quorumOK`, `quorumOKByRound`, `peerSet`). Joint state
  enforces "majority of Cold AND majority of Cnew separately" for every quorum
  decision.
- `handleInstallSnapshot` resets `currentConfig` from the snapshot's voters
  and clears `configHistory` + `appendedConfigIndex` — fixes a missed reset
  that left a stale joint config in memory after a follower's log was replaced.
- Election timeout's voter-membership guard at `onElectionTimeout` switched
  from `containsVoter` (Cnew-only) to `containsAnyVoter` (Cold ∪ Cnew). Under
  the old code, a server in Cold but not in Cnew would never call an election
  during the joint period — a liveness violation when the Cnew-only candidates
  are unreachable. `containsVoter` is retained for `maybeStepDownAfterRemoval`
  where Cnew-only is the correct check.
- Snapshot voters reflect committed effective config (joint state refused
  with `ErrConfChangeInProgress` — joint cannot be flattened into a single
  voter list).
- `actorState` peer-set caching: `peerSet()` returns a cached slice reused
  across calls (invalidated at every `currentConfig` mutation site). Removes
  per-tick `[]string` allocation from `broadcastHeartbeat` (50 ms cadence) and
  per-write allocation from `handlePropose`. `commitOK` + `quorumOKByRound`
  refactored to take maps/scalars directly instead of materializing fresh
  per-call maps. Restores the v1 zero-alloc baseline on the hottest leader
  paths.

### Internal contract notes

- Effective config rule (Diego thesis §4.3): a server uses the configuration
  from the latest log entry it has APPENDED (not committed). Truncation reverts.
- Joint quorum: every quorum decision requires majority from Cold AND Cnew
  separately during joint state. Single state: majority of voters.
- Self-removed leader: stays Leader until Cnew commits (so it can finish
  driving the change), then steps down. Gated on `appendedConfigIndex <= commitIndex`.
- Leader churn mid-joint: new leader rebuilds `pendingConfChange` from the
  joint log entry; the previous-leader caller's `AddVoter`/`RemoveVoter` has
  already returned `ErrProposalFailed` (drained on stepdown), but the joint
  state itself is durable in the log and recovery completes it.
- `AddLearner` / `PromoteToVoter` / `TransferLeadership` remain
  `ErrNotImplemented` (out of scope for M2 — joint consensus is sufficient
  for atomic voter changes).

### Status

- M2 (Persistence + ReadIndex + Snapshots + Joint Consensus): **7/7 COMPLETE**.
- M1: 7/7 complete.
- Next: M3 (test harness + chaos), M4 (staging soak), M5 (production swap).

### Known follow-ups (not blocking)

- `matchIndex`/`nextIndex`/`peerInFlight`/`peerLastRound` map entries for
  removed voters are not pruned at config-change time; they age out at the
  next `becomeLeader`. Benign because quorum paths consult `currentConfig`,
  not the maps, but flagged for future cleanup.

## v0.0.133.0 — 2026-05-09

### Breaking
- **Moved** `grainfs iam bucket-upstream` → `grainfs bucket upstream`. Verb `set` → `put`.
  Migration mapping:

  | v0.0.123–v0.0.132 | v0.0.133+ |
  |---|---|
  | `grainfs iam bucket-upstream set <bucket> ...` | `grainfs bucket upstream put <bucket> ...` |
  | `grainfs iam bucket-upstream get/list/delete ...` | `grainfs bucket upstream get/list/delete ...` |
- **Moved** admin UDS endpoints `/v1/iam/bucket-upstream*` → `/v1/buckets/[:bucket/]upstream`.
  HTTP method for upsert: POST → PUT. Legacy `/v1/iam/bucket-upstream*` removed (no alias).

### Changed (non-breaking)
- ADR 0010 records the surface relocation; ADR 0009's storage decisions
  (IAM Store, AAD `"bucket-upstream:"+bucket`, MetaCmdType IDs 32/33,
  IAM v3 snapshot trailer) are preserved → v0.0.122 ↔ v0.0.133 raft
  compatibility unchanged.

## [0.0.132.0] - 2026-05-11 — raft/v2 PR 15: Snapshots — log compaction + InstallSnapshot RPC (M2 6 of 7)

### Added

- `internal/raft/v2/snapshot.go` (new): `Snapshot` data type and
  `SnapshotStore` interface plus an in-memory `memSnapshotStore` impl.
  Snapshot captures `LastIncludedIndex`, `LastIncludedTerm`, the cluster
  configuration at snapshot time (flat voter list — joint-config encoding
  is deferred to PR 16), and FSM state bytes opaque to Node.
- `internal/raft/v2/snapshot_badger.go` (new): `badgerSnapshotStore` with
  versioned binary encoding (`[ver:1][LastIdx:8BE][LastTerm:8BE][NumVoters:4BE]
  ([VoterIDLen:4BE][VoterID]...)[DataLen:8BE][Data]`); single key
  `"snap/latest"` replaced atomically; `db.Sync()` after every Save for
  power-loss durability.
- `internal/raft/v2/snapshot_actor.go` (new): actor handlers
  `cmdCreateSnapshot` + `cmdInstallSnapshot`, plus the leader-side
  `dispatchOne(peer)` helper that picks AE vs InstallSnapshot based on
  `nextIndex[peer]` vs `LogStore.FirstIndex()`.
- `LogStore.CompactBefore(boundary)` interface method — advances FirstIndex
  past the compacted prefix; both `memLogStore` and `badgerLogStore`
  track `firstIndex` + `prevTerm` so `TermAt(boundary)` returns the
  snapshot's `LastIncludedTerm` post-compaction (load-bearing for AE
  PrevLogIndex/PrevLogTerm consistency at the snapshot boundary).
- Non-public `snapshotInstaller` interface with `InstallSnapshotBoundary`
  for the InstallSnapshot follower path that needs to seed
  firstIndex+prevTerm beyond LastIndex (when boundary > LastIndex). Both
  in-tree LogStore impls satisfy it; type-asserted at the single call
  site in `handleInstallSnapshot`.
- New `LogEntryType = LogEntrySnapshot (4)`. FSM consumers MUST recognise
  it on `applyCh`, reset their state, and reload from the entry's
  `Command` bytes (which carry the snapshot Data). Same applyCh /
  applyInCh / applyLoop pipeline preserves FIFO with subsequent
  committed entries.
- `Node.CreateSnapshot(lastIncludedIndex, data)` — caller-driven (FSM
  decides snapshot cadence). Validates `lastIncludedIndex <= commitIndex`
  AND `>= FirstIndex`; rejects otherwise. Rejects when joint config is
  active (defer to PR 16). Saves snapshot durably, then compacts the log.
- `Node.HandleInstallSnapshot(args)` — follower-side RPC handler.
  Truncates the log, installs the snapshot boundary, sets `commitIndex
  = LastIncludedIndex`, and delivers a `LogEntrySnapshot` signal via
  `applyInCh` (FIFO with committed entries).
- `Node.LatestSnapshot()` — FSM consumer reads the latest snapshot
  after Start to restore its state.
- `InstallSnapshotArgs` / `InstallSnapshotReply` RPC types in `types.go`.
- `Transport` interface gains `SendInstallSnapshot`. `memTransport`
  routes it through the in-process network for tests.
- `Config.SnapshotStore` field. NewNode defaults to memSnapshotStore;
  pairing persistent LogStore with in-memory SnapshotStore is documented
  as unsafe (mirrors StableStore pairing rule).
- 10 new tests in `snapshot_test.go` and `logstore_test.go`:
  TestSnapshot_{CreateAndCompact, RejectUncommitted, RejectAlreadyCompacted,
  RecoveryFromBadger}, TestInstallSnapshot_{FollowerInstalls,
  StaleTermRejected, LeaderSendsWhenFollowerBehind, SecondInstallOverPriorSnapshot,
  StaleSnapshotIgnored, SkipWhenAlreadyCaughtUp},
  TestLogStore_CompactBefore{,_Idempotent,_BeyondLast} (table-driven over
  mem + badger).

### Recovery

- `NewNode` loads `LatestSnapshot` after HardState + log; if a snapshot
  exists, `commitIndex` is seeded to `snap.LastIncludedIndex`. The
  log's persisted `firstIndex` must be `>= snap.LastIncludedIndex+1`
  (mid-CreateSnapshot crash recovery is out of scope; panic with a
  clear "manual recovery required" message guards the invariant).

### Fixed (review findings)

- `actor.go applyConflictHint` binary search and case-2 conflict walkback
  now respect `FirstIndex` floor — pre-PR these would panic on
  `mustTermAt` for compacted indices once snapshot/CompactBefore made
  `FirstIndex > 1` reachable. Search range shifts to
  `[FirstIndex, LastIndex]`; walkback stops at `floor = FirstIndex`.
  When `ConflictTerm` lives entirely in the compacted prefix, fall
  through to `hbConflictIndex`; `dispatchOne` then picks
  InstallSnapshot.
- `handleInstallSnapshot` Rule 4b (skip-when-already-caught-up) now
  nudges `commitIndex` to `LastIncludedIndex` when the leader's known
  commit is ahead of ours (Figure 13 step 6 wording).

### Known follow-ups

- `handleInstallSnapshot` truncate + `InstallSnapshotBoundary` are two
  separate fsynced txns. Recovery is non-fatal (NewNode rebuilds
  consistent state from independent firstIdx+lastIdx reads), but the
  asymmetry with `CreateSnapshot`'s atomic chunked-txn approach is
  ugly. Optional fold-into-final-txn cleanup deferred.
- `TestHandleAppendEntries_HeartbeatStepDown` flakes ~1/12 under race
  in unrelated timing race between actor's reply send and deferred
  publish. Pre-existing; orthogonal to this PR.

### Cumulative status (raft/v2 actor-pattern rewrite)

- M1 Foundation: 7/7 (#251)
- M2 Persistence + ReadIndex + Snapshots: 6/7 (PR 8 #252, PR 9 #256,
  PR 10 #259, PR 11 #261, PR 14 #272, PR 15 this).
- M2 final piece — joint consensus (§4.3 atomic membership change) —
  scheduled for PR 16. Implementer subagent quota line that runs
  the JC implementer is rate-limited until 2026-05-12; resume after
  reset. Snapshot encoding's `Configuration []string` will need
  joint-config extension at that time.
- Then M3 (test harness + chaos), M4 (staging soak), M5 (production
  swap). All raft/v2 work continues in `.worktrees/raft-actor` on
  dedicated PR branches per the user-locked workflow.

## [0.0.131.0] - 2026-05-09 — serveruntime boot decomposition: services + shutdown phases (final, milestone complete)

### Refactored

- `internal/serveruntime/run.go` collapses from **938 lines** (pre-PR 1) → **136
  lines** for the final PR. The `Run` function body is now a flat sequence
  of phase calls:
  - PR 2: `bootValidateConfig`, `bootAutoMigrate`, `bootOpenMetaDB`,
    `bootValidateTimings`, `bootOpenRaftLogStore`, `bootOpenSharedRaftLogDB`.
  - PR 3: `bootQUICTransport`, `bootPeerConnections`, `bootGroupRaftMux`.
  - inline: data-plane `raft.Node` construction (consumed by PR 4 + PR 5).
  - PR 4: `bootMetaRaftWiring`, `bootDataGroupRouter`,
    `bootRotationAndAdminAPI`, `bootMetaRaftStart`.
  - PR 5: `bootShardService`, `bootStreamRouter`, `bootOwnedGroupsAndEC`.
  - PR 6: `bootSnapshotAndApplyLoop`.
  - **New (this PR)**: `bootBalancerAndGossip`, `bootWALAndForwarders`,
    `bootBackendWrap`, `bootSrvOptsAndReceipt`, `bootHTTPServerAndAdmin`,
    `bootRecoveryAndScrubber`, `bootResharderAndDegraded`,
    `bootNodeServices`, `bootShutdownDrain`.

  Mirror-locals are gone — phase functions read `state.*` directly. Behavior
  is preserved exactly: same constructor calls, same callback registration
  order, same cleanup LIFO order, same shutdown orchestration. The only
  change is structural.

### Added (new phase files + tests)

- `internal/serveruntime/boot_phases_balancer.go`: `bootBalancerAndGossip`
  — starts the StartBalancer (cluster mode) and ensures a single
  GossipReceiver drains `transport.Receive()` whenever heal-receipt is on.
  Populates `state.balancerProposer`, `state.gossipReceiver`.

- `internal/serveruntime/boot_phases_forwarders.go`: `bootWALAndForwarders`
  — opens the WAL, computes `seedGroups`, builds `ForwardSender`/Receiver,
  `MetaProposeForwardSender`/Receiver, `MetaCatalogReadSender`,
  `MetaJoinReceiver`, and the `ClusterCoordinator`. Performs join-mode
  meta-join + initial Router sync. Cleanup: `wal.Close` registered via
  `state.AddCleanup`.

- `internal/serveruntime/boot_phases_backend.go`: `bootBackendWrap` —
  composes `wal.Backend → pullthrough.Backend → optional RecoveryWriteGate`,
  reduces startup probe decisions, honours the recover-cluster marker,
  constructs the `DiskCollector`, creates the default bucket on singleton
  startup, kicks off the auto-snapshotter, emits the startup config snapshot.

- `internal/serveruntime/boot_phases_srvopts.go`: `bootSrvOptsAndReceipt`
  — assembles the `[]server.Option` slice (cluster info / membership /
  event store / alerts / iceberg / auth / balancer / receipt / volume mgr /
  block cache / shard cache / read-indexer / raft snapshotter /
  mutation gate), wires the disk-threshold callbacks, opens the receipt DB,
  the incident DB, and the resource guards. **Critical correctness work**:
  every `defer X.Close()` and `defer resourcewatch.DeregisterDB(...)` from
  the original `Run` body is converted to `state.AddCleanup(...)` so it
  runs at `Run()` exit (LIFO) — not at phase-function exit.

- `internal/serveruntime/boot_phases_admin.go`: `bootHTTPServerAndAdmin`
  — `server.New`, IAM proposer wiring, dashboard token + middleware,
  admin UI route registration, admin Unix Domain Socket Start. Spec A5
  invariant: admin routes register **before** the data-plane Hertz starts
  serving. Best-effort `adminSrv.Stop` registered via `state.AddCleanup`.

- `internal/serveruntime/boot_phases_scrubber.go`: `bootRecoveryAndScrubber`
  — startup recovery, scrubber Director (replication + EC sources),
  ShardPlacementMonitor wiring (with receipt signing). The PR 4-style
  "callbacks before Start" invariant is preserved:
  `metaRaft.FSM().SetOnScrubTrigger(...)` and
  `forwardReceiver.WithScrubSessionLookup(director)` register before
  `director.Start(ctx)`. The receipt-tracking emitter `Close()` is moved to
  `state.AddCleanup` so it tears down at `Run()` exit.

- `internal/serveruntime/boot_phases_node_services.go`: three phases —
  `bootResharderAndDegraded` (per-group ReshardManager loop, lifecycle
  worker, degraded monitor, data-plane `srv.Run` goroutine),
  `bootNodeServices` (universal NFS/NFSv4/NBD via `StartNodeServices` +
  `nfs4` cache invalidator registration), and `bootShutdownDrain` (the
  `<-ctx.Done()` block + ordered drain: admin first → HTTP second → raft
  leadership transfer → close `stopApply`). Shutdown is **not** registered
  through `AddCleanup` because its order is the inverse of generic LIFO —
  admin must stop FIRST.

- `internal/serveruntime/boot_phases_services_extra_test.go`: witness +
  happy-path tests for the lightweight subset of the new phases:
  - `TestBootBalancerAndGossip_NoFlagsSkips` — no-op when both flags off.
  - `TestBootWALAndForwarders_PopulatesForwarders` — every sender/receiver
    populated; `seedGroups ≥ 8`.
  - `TestBootServicesExtraPhases_OrderingInvariant` — walks balancer +
    WAL+forwarders, asserts each `state.*` field nil before its phase and
    populated after. Mirrors `TestBootRaftPhases_OrderingInvariant`. The
    heavier phases (backend wrap, srvOpts, admin, scrubber, node services,
    shutdown) are covered by the smoke E2E
    (`TestSmoke_DeploymentVerification`) — they require IAMStore, a real
    admin UDS plumbing, and a fully-resolved data-plane raft leader, which
    is what E2E provides.

- `internal/serveruntime/boot_state.go`: typed fields added for the new
  phases — `balancerProposer`, `gossipReceiver`, `wal`, `walDir`,
  `forwardSender`, `forwardReceiver`, `metaForwardSender`, `metaReadSender`,
  `clusterCoord`, `seedGroups`, `backend`, `recoveryReadOnly`,
  `diskCollector`, `srvOpts`, `clusterAlerts`, `receiptWiring`,
  `incidentRecorder`, `lifecycleMgr`, `mutationGate`, `volMgr`, `srv`,
  `tokenStore`, `adminDeps`, `adminSrv`, `scrubDirector`, `activeEmitter`.
  Each annotated with its owning phase.

### Boundary rationale

The original 6-phase plan listed `bootClusterCoordAndDomain` as a single
phase covering ~330 lines (lines 232-566 of the pre-PR-final `Run`). That
absorbed too much for one reviewable unit. We split along natural
construction boundaries:

1. WAL + forwarders + ClusterCoordinator + meta-join (everything that wires
   the routing fabric).
2. Backend wrap + recovery gates + disk collector + auto-snap + log (the
   storage-chain-and-startup-services boundary).
3. srvOpts + receipt + incident + lifecycle + volume mgr + mutation gate
   (everything that mutates `[]server.Option` on its way to `server.New`).
4. HTTP server + admin UDS (the data-plane construction).
5. Recovery + scrubber director + placement monitors (the heal/scrub block).
6. Resharder + degraded + lifecycle + `srv.Run` goroutine (background loops
   that only need a wired-up server).
7. Node services (NFS/NFSv4/NBD) + nfs4 invalidator.
8. Shutdown drain (the ordered teardown sequence).

Each phase has a single, narrow responsibility, a clear input/output
contract on `bootState`, and a godoc explaining its phase ordering invariants.

### Behavior preservation

- All `defer X.Close()` / `defer resourcewatch.DeregisterDB(...)` lines
  in the original `Run` body have been converted to `state.AddCleanup(...)`
  to preserve LIFO-at-`Run()`-exit semantics. (Direct lift-and-move would
  have changed behavior — those defers would have fired at phase-function
  exit, closing DBs while `Run` was still using them.)
- The shutdown drain remains an explicit ordered sequence (admin → HTTP →
  raft leadership transfer → `close(stopApply)`) rather than `AddCleanup`
  registrations, because the order is the inverse of generic cleanup LIFO.
- The fallback `state.AddCleanup(adminSrv.Stop)` for early-return paths is
  preserved; on the happy path the explicit shutdown still runs first
  because `admin.Stop` is idempotent.

### Verification

- `make build`: clean.
- `make test`: all packages green (cluster ~84s, volume ~54s, serveruntime ~20s).
- `go test ./tests/e2e/ -run TestSmoke_DeploymentVerification`: passes (~6s).
- `go test ./internal/serveruntime/ -run TestBoot -count=1`: 11s, all green
  including the new ordering witness.

### Milestone closeout

This PR completes the **boot decomposition milestone**:

| PR  | Subject                                        | Lines collapsed |
| --- | ---------------------------------------------- | --------------- |
| 1   | bootState scaffold + Cleanup LIFO              | scaffold        |
| 2   | config + storage open phases                   | ~60             |
| 3   | transport phases (QUIC + peers + mux)          | ~50             |
| 4   | raft phases (witness ordering test)            | ~80             |
| 5   | storage runtime phases                         | ~140            |
| 6   | snapshot + apply-loop                          | ~37             |
| 7   | services + shutdown (this PR)                  | ~600            |

`Run()` body: **938 → 136 lines** (~85% reduction). Every inline construction
is now isolated, named, godoc'd, and replaceable.

## [0.0.130.0] - 2026-05-09 — serveruntime boot decomposition PR 6: snapshot + apply-loop phase (1 of ~6 services)

### Refactored

- `internal/serveruntime/boot_phases_services.go` (new): first services phase
  in the post-storage-runtime block of `Run`:
  - `bootSnapshotAndApplyLoop(state)` — wires the meta-FSM + SnapshotManager
    onto `distBackend`, calls `Restore()` to replay any prior snapshot,
    builds the `packblob → cachedBackend` wrap chain (packblob is gated on
    `cfg.PackThreshold > 0`), registers the `s3-cache` cache invalidator
    on `distBackend`, and fires `go distBackend.RunApplyLoop(state.stopApply)`.

- `internal/serveruntime/boot_state.go`: typed fields added — `fsm
  *cluster.FSM`, `snapMgr *raft.SnapshotManager`, `cachedBackend
  *storage.CachedBackend`. Each is owned by `bootSnapshotAndApplyLoop`.

- `internal/serveruntime/run.go`: ~37 inline lines (snapshot manager + Restore
  + wrap chain + invalidator + go RunApplyLoop) collapse into a single phase
  call plus a `fsm := state.fsm` mirror. The "invalidator must register
  before apply-loop fires" invariant — previously a comment proximity in
  run.go — is now enforced inside the phase function. The `packblob` import
  moves to the phase file.

### Added

- `internal/serveruntime/boot_phases_services_test.go`: witness test on real
  components (real BadgerDB, real QUIC transport, real meta-raft, real
  data-plane raft, real distBackend):
  - `TestBootSnapshotAndApplyLoop_PopulatesState` — asserts fsm / snapMgr /
    cachedBackend are nil before the phase, populated after.

### Why

PR 6 of the 6-PR boot decomposition milestone (see
`docs/superpowers/specs/2026-05-08-serveruntime-boot-decomposition.md`).

Scope deviation from the original plan: the milestone spec sketched PR 6
as "Services + Shutdown (4 phases)" covering the entire ~780-line
post-storage block. After reviewing the territory in detail, the four
named phases (`bootReceiptAndBalancer`, `bootAdminUDS`,
`bootNodeServicesAndHertz`, `bootShutdownDrain`) do not align with the
real data flow — the wrap chain, balancer, gossipReceiver, WAL,
forwardSender, clusterCoord, snapshotter, IcebergCatalog selection,
incident recorder, lifecycle, volume manager, mutationGate, srv.New,
dashboard token, admin UDS, scrubber, reshard manager, NodeServices, and
shutdown drain are heavily interleaved across that block.

This PR ships a smaller, surgical first slice (the snapshot + apply-loop
block) with a clear input/output contract, leaving the remaining
services territory for follow-up PRs that can land on a typed-state
foundation without forcing a 15-field bootState extension in one shot.

Behavior preserved: phase order matches the prior inline order exactly,
snapshot Restore still precedes the apply-loop go statement, the
s3-cache invalidator still registers before the apply loop fires.

## [0.0.129.0] - 2026-05-09 — serveruntime boot decomposition PR 5: storage runtime phases (3 of ~20)

### Refactored

- `internal/serveruntime/boot_phases_storage_runtime.go` (new): three storage
  data-plane phases that split the largest remaining inline block of
  `Run` (lines ~155–443 pre-PR 5, ~280 lines) into typed phase functions:
  - `bootShardService(ctx, state)` — completes cluster topology bootstrap
    in non-join mode (wait meta-raft leader, propose `MetaNodeEntry`,
    seed initial shard groups, sync the router and flip
    `SetRequireExplicitAssignments(true)`), then constructs
    `cluster.ShardService` with cfg-driven options (DirectIO, MeasureReadAmp,
    AddressBook). Captures `state.effectiveEC` so downstream phases reuse
    the same auto-resolved profile without re-deriving from cluster size.
  - `bootStreamRouter(state)` — wires the QUIC stream multiplexer
    (`StreamControl` → raft RPC, `StreamData` → shard RPC), registers
    the body handlers (`StreamShardWriteBody`, `StreamShardReadBody`)
    that consume body streams directly off `QUICTransport.HandleBody/HandleRead`,
    and fires `node.Start()` on the data-plane raft. The body handler
    registration is the regression fence for the 2024 fix where
    `StreamShardWriteBody` fell through the catch-all router → stream
    closed without response → `decode response: read header: EOF` →
    N×replication produced only the leader's local copy.
  - `bootOwnedGroupsAndEC(ctx, state, recordStartupDecision)` — the heavy
    phase: constructs `DistributedBackend`, wires the group-0 wrapper for
    legacy single-backend semantics, builds the EC `shardCache`, sets the
    cluster `Router` + `ShardGroupSource` on the backend, constructs the
    `Rebalancer` + `DataGroupPlanExecutor` and registers
    `SetOnRebalancePlan`, runs synchronous cold-start instantiation for
    every owned shard group (recording badgerrole decisions for opens
    that fail), registers `SetOnShardGroupAdded` for runtime apply,
    spins up the `LoadReporter`, sets the EC config on the backend, and
    pushes the LIFO shutdown hook that drains owned groups in parallel
    on cleanup. `recordStartupDecision` is plumbed via parameter so the
    phase signature is honest about what it mutates outside of `state`.

- `internal/serveruntime/boot_state.go`: typed fields added — `node
  *raft.Node`, `rpcTransport *raft.QUICRPCTransport`, `streamRouter
  *transport.StreamRouter`, `shardSvc *cluster.ShardService`, `distBackend
  *cluster.DistributedBackend`, `shardCache *shardcache.Cache`,
  `effectiveEC cluster.ECConfig`, `stopApply chan struct{}`, `rebalancer
  *cluster.Rebalancer`, `loadReporter *cluster.LoadReporter`,
  `loadReporterStor *cluster.NodeStatsStore`. Each field is populated by
  exactly one phase; the phase ownership is annotated alongside the
  declaration.

- `internal/serveruntime/run.go`: ~258 lines of inline storage-runtime
  setup collapse into three phase calls plus a small mirror block. The
  "body handlers MUST register before node.Start" and "OnRebalancePlan
  callback MUST register before rebalancer.Run" invariants — previously
  protected only by source-line proximity — are now enforced by phase
  ordering. `node` and `rpcTransport` are now also captured on
  `bootState` immediately after construction so the storage phases can
  read them from typed state. Run is down to 965 lines (from 1223).

### Added

- `internal/serveruntime/boot_phases_storage_runtime_test.go`: 4 unit
  tests on real components (real BadgerDB, real QUIC transport, real
  meta-raft) covering each storage phase plus the witness ordering
  invariant:
  - `TestBootShardService_ComputesEffectiveEC` — phase resolves the
    auto EC profile (1+0 for a single-node cluster), constructs the
    `ShardService`.
  - `TestBootStreamRouter_RegistersHandlersAndStartsNode` — phase
    populates `state.streamRouter` and pushes the data-plane node Stop
    cleanup onto the LIFO stack.
  - `TestBootOwnedGroupsAndEC_WiresDistBackend` — heavy integration:
    phase wires `distBackend`, `shardCache`, `rebalancer`,
    `loadReporter`, `loadReporterStor`, and `stopApply`.
  - `TestBootStoragePhases_OrderingInvariant` — the witness test:
    `shardSvc`/`streamRouter`/`distBackend` are nil before each phase,
    populated after, and `effectiveEC.NumShards()` jumps from 0 to >0
    only after `bootShardService`. A future re-order that runs
    OwnedGroupsAndEC before StreamRouter (or skips ShardService) fails
    this assertion chain.

### Why

PR 5 of the 6-PR milestone (see
`docs/superpowers/specs/2026-05-08-serveruntime-boot-decomposition.md`).
The storage runtime was the second-largest inline block in `Run` after
the services + shutdown territory (PR 6). Splitting it into three
phases makes the "ShardService → StreamRouter → distBackend" pipeline
explicit and testable in isolation, and clears the way for PR 6 to
collapse the remaining ~520 lines (snapshot+receipt, balancer, admin
UDS, Hertz data plane, NFS/NBD startup, drain) on top of typed state
that already has every storage handle named.

Behavior preserved: phase order matches the prior inline order
exactly, no error wrapping changed, no callback timing changed, no
goroutine timing changed. `make test` (full unit suite) and
`go test ./tests/e2e/ -run TestSmoke_DeploymentVerification` both
green; the smoke test is the integration cover.

## [0.0.128.0] - 2026-05-09 — serveruntime boot decomposition PR 4: raft phases (4 of ~20)

### Refactored

- `internal/serveruntime/boot_phases_raft.go` (new): four raft phases that
  split the meta-raft control-plane setup into "register everything, THEN
  Start" — making the central PR 4 invariant explicit at the phase
  boundary instead of source-line proximity:
  - `bootMetaRaftWiring(state)` — `cluster.NewMetaRaft`,
    `NewMetaTransportQUICMux` (auto-registers `__meta__` group on the
    mux), `SetTransport`, `SetIAM`. NO callbacks, NO Start. Pure wiring.
  - `bootDataGroupRouter(state)` — constructs `DataGroupManager` +
    `Router`, calls `SetDefault("group-0")`, registers
    `SetOnBucketAssigned` on the meta-FSM. Callback-only phase.
  - `bootRotationAndAdminAPI(state)` — constructs `RotationKeystore` +
    `RotationWorker`, registers `SetOnRotationApplied`, seeds rotation
    steady state via `DeriveClusterIdentity`. When IAM is configured,
    also constructs `iamProposer` + `iamAdminAPI` (admin UDS POST
    `/v1/iam/sa` boots the first SA). Callback-only phase.
  - `bootMetaRaftStart(ctx, state, startRotationSocket)` — fires the
    apply loop. `Bootstrap` (when not in join mode), `Start`,
    `StartPreviousKeyCleanup`, `StartRotationSocket` (plumbed via
    parameter so tests can pass nil), `state.AddCleanup(metaRaft.Close)`,
    and `Router.Sync(BucketAssignments)` to seed the router with any
    buckets that landed during the synchronous replay window.

- `internal/serveruntime/boot_state.go`: typed fields added — `metaRaft
  *cluster.MetaRaft`, `metaTransport *cluster.MetaTransportQUIC`,
  `dgMgr *cluster.DataGroupManager`, `clusterRouter *cluster.Router`,
  `rotationKeystore *transport.Keystore`, `rotationWorker
  *cluster.RotationWorker`, `iamAdminAPI *iam.AdminAPI`, `iamProposer
  *iam.MetaProposer`. Each field is populated by exactly one phase; the
  phase ownership is annotated alongside the declaration.

- `internal/serveruntime/run.go`: ~75 lines of inline meta-raft + router +
  rotation + admin-API setup collapse into four phase calls plus a tiny
  mirror block. The "callbacks before Start" invariant — previously a
  comment at run.go:160-161 protected only by source ordering — is now
  enforced by phase ordering: `bootDataGroupRouter` and
  `bootRotationAndAdminAPI` complete before `bootMetaRaftStart` even
  enters. A future refactor that re-orders Start ahead of either
  callback phase will fail the witness ordering test.

### Added

- `internal/serveruntime/boot_phases_raft_test.go`: 5 unit tests on real
  meta-raft (real BadgerDB, real QUIC transport) covering all four
  raft phases plus the central ordering invariant:
  - `TestBootMetaRaftWiring_PopulatesState` — phase populates `metaRaft`
    and `metaTransport`; `dgMgr`, `clusterRouter`, and `rotationWorker`
    remain nil (downstream phases own those).
  - `TestBootDataGroupRouter_RegistersCallbackBeforeStart` — phase wires
    `dgMgr` + `Router` + `SetDefault("group-0")` + `SetOnBucketAssigned`
    callback. A brand-new bucket has no explicit assignment (proves
    the callback hasn't fired yet — Start is gated by a later phase).
  - `TestBootRotationAndAdminAPI_NoIAMSkipsAdmin` — rotation worker is
    constructed unconditionally; `iamAdminAPI` and `iamProposer` stay
    nil when IAM dependencies are absent (`--no-encryption` parity).
  - `TestBootMetaRaftStart_FiresApplyLoop` — phase fires `Start`; the
    underlying raft `Node` is alive afterwards. `metaRaft.Close` is
    registered on the cleanup stack so `t.Cleanup(state.Cleanup)` tears
    down the apply loop deterministically.
  - `TestBootRaftPhases_OrderingInvariant` — the witness test:
    `state.metaRaft` is nil before Wiring, populated after Wiring and
    nil-callback-fields stay nil; `clusterRouter` is nil before
    DataGroupRouter and populated after; `rotationWorker` is nil before
    RotationAndAdminAPI and populated after; `metaRaft` is finally
    Started only after all callback phases ran. A future re-order that
    Starts before a callback phase will fail this assertion chain.

### Why

PR 4 of the 6-PR milestone (see
`docs/superpowers/specs/2026-05-08-serveruntime-boot-decomposition.md`).
The hardest PR in the milestone because the raft section concentrates
the "must register before Start" invariant: any callback registered
after the apply loop fires races the FSM mutex — `SetOnBucketAssigned`,
`SetOnRotationApplied`, `SetRotationSteady` all take `f.mu.Lock()`,
which the apply goroutine also takes.

Pre-PR 4 the invariant was a 2-line comment ("must register before
Start") protected by source-line ordering. After PR 4 the invariant is
captured by phase boundaries: `bootDataGroupRouter` and
`bootRotationAndAdminAPI` populate `state` BEFORE `bootMetaRaftStart`
even runs. Re-ordering callbacks past Start now requires re-ordering
phase calls, which the witness test
`TestBootRaftPhases_OrderingInvariant` will catch.

Behavior preserved: same `MetaRaft` constructor wiring, same transport
mux registration, same IAM applier plumbing, same callback set, same
Bootstrap/Start sequence, same `previous.key` cleanup goroutine, same
rotation socket boot, same `metaRaft.Close` cleanup, same post-replay
`Router.Sync`. The only visible structural change is `StartRotationSocket`
is plumbed through the phase signature so tests can substitute a no-op.

PR 5 (storage runtime phases — `bootShardService`, `bootStreamRouter`,
`bootOwnedGroups+EC`) is next.

## [0.0.125.0] - 2026-05-09 — raft/v2 PR 14: ReadIndex (linearizable reads) + apply pipeline decoupling (M2 main + last M1 follow-up)

### Added

- `internal/raft/v2/readindex.go` (new) + `internal/raft/v2/readindex_test.go` (new):
  Raft §6.4 leader-side ReadIndex implementation.
  - `Node.ReadIndex(ctx) (uint64, error)` returns a barrier index after a
    fresh heartbeat round confirms majority leadership at the current term.
    Caller is responsible for waiting until its FSM `lastApplied >= barrier`
    before serving the linearizable read.
  - Single-voter shortcut: returns `commitIndex` inline (self-quorum at
    every commit implies leadership).
  - Multi-voter protocol: bumps `leaderRound`, captures `barrier =
    commitIndex`, queues request with `minPeerRound = leaderRound`,
    triggers `broadcastHeartbeat`. `tryFlushReadIndex` reaps the queue
    once a majority of peers reply (gated on `state==Leader && term match`)
    with `peerLastRound[peer] >= minPeerRound`.
  - Step-down drains queued requests with `ErrProposalFailed`. `becomeLeader`
    resets `leaderRound = 0` and re-allocates `peerLastRound` so re-leadership
    at a higher term cannot pre-satisfy a fresh ReadIndex with stale evidence
    from a prior term.
  - SLA: idle-peer-majority case bounded by RTT; in-flight-peer-majority
    case bounded by `heartbeatTimeout + RTT` (next tick re-dispatches the
    bumped round). Tightening to one-shot redispatch on `peerInFlight`
    clear is deferred to a follow-up PR.

- `internal/raft/v2/actor.go` `applyLoop` goroutine + `applyInCh` channel
  (apply pipeline decoupling — addresses TODO raft/v2 cmdCh backpressure
  cascade, the last structural M1 follow-up):
  - Actor's `applyCommitted` now enqueues to `applyInCh` (buffered 256)
    instead of sending directly on `applyCh`. `applyLoop` drains
    `applyInCh` aggressively into an unbounded in-goroutine buffer, then
    forwards to `applyCh` in FIFO order.
  - Decouples actor liveness from FSM consumer speed: a slow FSM no longer
    wedges the actor's send → `applyCommitted` → AE/heartbeat handling →
    cascade to election storms.
  - Shutdown ordering: actor closes `applyInCh` on exit; `applyLoop`
    drains its buffer to `applyCh`, then closes `applyCh` and signals
    `applyDoneCh`. `Node.Stop` awaits both `doneCh` and `applyDoneCh` so
    callers can rely on `applyCh` being closed by the time `Stop` returns.
  - GC: `applyLoop` zeroes the popped slot before `buf = buf[1:]` so
    delivered entries' `Command []byte` is reclaimable before the buffer
    fully drains.

### Changed

- `internal/raft/v2/actor.go`:
  - `dispatchAppendEntries(peer, args, roundID)` signature gains roundID
    captured at dispatch time. `broadcastHeartbeat` and `handlePropose`
    pass `n.st.leaderRound` as the round identifier.
  - `handleHeartbeatReply` advances `peerLastRound[peer]` on success
    (gated on existing `state==Leader && term match` check) and calls
    `tryFlushReadIndex` to reap queued requests.
  - `command` struct gains `hbRoundID` (captured by dispatch closure for
    reply correlation) and `riReply` (ReadIndex reply chan).
  - New `cmdReadIndex` cmdKind + `handleReadIndex` + `tryFlushReadIndex`.
  - `becomeLeader` resets `leaderRound = 0`, allocates fresh
    `peerLastRound`, clears `readIndexQueue`.
  - `stepDownToFollower` drains `readIndexQueue` with `ErrProposalFailed`
    (blocking sends — `riReply` is cap-1 buffered + at-most-one send per
    req — and matches the inline-reply paths for consistency) and nils
    `peerLastRound` + zeroes `leaderRound`.

- `internal/raft/v2/state.go` `actorState`: new fields `leaderRound`,
  `peerLastRound`, `readIndexQueue`. Comments document the invariants
  (gate conditions, lifecycle).

- `internal/raft/v2/replication_test.go`
  `TestApplyCommitted_StopRaceLeavesCommitIndexConsistent`: retargeted
  from `applyCh` to `applyInCh` to reflect the new actor boundary. Stop
  semantics are equivalent (last fully-enqueued vs last fully-delivered);
  actual FSM-side delivery is now `applyLoop`'s contract.

### Tests

- `TestReadIndex_NotLeader` — Follower rejects with `ErrNotLeader`.
- `TestReadIndex_SingleVoter` — single-voter returns `commitIndex` inline.
- `TestReadIndex_MultiVoter_LeaderConfirmation` — multi-voter blocks until
  heartbeat round confirms majority.
- `TestReadIndex_MultiVoter_StepDownDrains` — partitioned leader's queued
  ReadIndex resolves with `ErrProposalFailed` on step-down.
- `TestReadIndex_StaleRoundReplyIgnored` — direct state-machine test:
  hand-built Leader at term 5 receives a synthetic term-4 reply with
  `hbRoundID=999`; `peerLastRound` must NOT advance (term gate). Companion
  same-term reply asserts the gate is term-specific, not blanket-rejecting.
- `TestReadIndex_PeerLastRoundResetOnLeaderRegain` — leader steps down,
  cluster re-elects; new leader's fresh ReadIndex must succeed (proves
  `becomeLeader` resets `peerLastRound`/`leaderRound`).
- `TestApplyPipeline_DecoupledFromActor` — slow FSM consumer (5ms/entry)
  with 200 ProposeWaits + a fresh ReadIndex during the slow drain;
  proves cmdCh + actor remain live (pre-PR direct-applyCh path would
  wedge after applyCh fills).
- `TestApplyPipeline_OrderingUnderSlowFSM` — 200 entries with slow
  consumer; verify FIFO order preserved.

### Cumulative status (raft/v2 actor-pattern rewrite)

- M1 Foundation: 7/7 PRs (#251)
- M2 Persistence + ReadIndex: 5/7 (PR 8 #252, PR 9 #256, PR 10 #259, PR 11
  #261, PR 14 this).
- M1 follow-ups: 7/7 (PR 12 #263, PR 13 #266, PR 14 closes the last —
  cmdCh backpressure cascade — bundled here).

Remaining M2: snapshots (log compaction + InstallSnapshot RPC), joint
consensus (membership changes §4.3). Then M3 (test harness + chaos), M4
(staging soak), M5 (production swap). Until production swap, all
raft/v2 work continues in `.worktrees/raft-actor` on dedicated PR
branches per the user-locked workflow.

## [0.0.124.0] - 2026-05-09 — serveruntime boot decomposition PR 3: transport phases (3 of ~20)

### Refactored

- `internal/serveruntime/boot_phases_transport.go` (new): three phases for
  the transport entry of `Run`:
  - `bootQUICTransport(ctx, state)` — `ResolveClusterKey` (disk > flag >
    ephemeral fallback in solo mode), `transport.NewQUICTransport`,
    `SetTrafficLimits` for forwarded S3 PUT bulk fan-outs, `Listen`. On
    success populates `state.quicTransport`, updates `state.raftAddr` to
    the kernel-picked port (when operator passed `127.0.0.1:0`), and
    queues `Close` on the cleanup stack.
  - `bootPeerConnections(ctx, state)` — Connects to each peer; logs but
    swallows individual connection failures so the transport can retry
    lazily on the first send. No-op for solo mode.
  - `bootGroupRaftMux(state)` — Constructs `raft.NewGroupRaftQUICMux` and
    enables the mux mode when `cfg.QUICMuxEnabled`. Now runs **before**
    raft node creation (was inline after node pre-PR 3) so the
    "groupRaftMux exists before NewMetaTransportQUICMux" invariant is
    enforced by phase ordering, not by source-line proximity.

- `internal/serveruntime/boot_state.go`: typed fields added —
  `transportPSK string`, `quicTransport *transport.QUICTransport`,
  `groupRaftMux *raft.GroupRaftQUICMux`. `state.raftAddr` is mutated
  in-place by `bootQUICTransport` to the resolved port.

- `internal/serveruntime/run.go`: ~50 lines of QUIC + peer + mux setup
  collapse into three phase calls plus a tiny mirror block. The
  `groupRaftMux` block previously between raft node creation and
  metaRaft construction is gone — `bootGroupRaftMux` runs immediately
  after `bootPeerConnections`. The structural invariant is captured in
  a 3-line comment instead of being implicit in line ordering.

### Added

- `internal/serveruntime/boot_phases_transport_test.go`: 6 unit tests
  covering all three transport phases on real QUIC sockets:
  - `TestBootQUICTransport_BindsLoopbackPort0` — Listens on `:0`,
    `state.raftAddr` resolves to the kernel-picked port, matches
    `LocalAddr()`.
  - `TestBootQUICTransport_GeneratesEphemeralKeyInSoloMode` — empty
    ClusterKey + no peers → ephemeral PSK (≥ 32 chars) in
    `state.transportPSK`.
  - `TestBootPeerConnections_EmptyPeersIsNoOp` — solo mode is a clean
    no-op; no panic on nil peer list.
  - `TestBootGroupRaftMux_CreatesMux` — phase populates
    `state.groupRaftMux`; `QUICMuxEnabled=false` does not call
    `EnableMux`.
  - `TestBootGroupRaftMux_EnabledHonorsConfig` — `QUICMuxEnabled=true`
    with operator-supplied pool size + flush window wires the prototype
    mux mode.
  - `TestBootTransportPhases_OrderingPreservesMuxBeforeMetaTransportInvariant`
    — witnesses the central invariant: `state.groupRaftMux` is nil after
    `bootQUICTransport` and `bootPeerConnections`, populated after
    `bootGroupRaftMux`. A future `bootMetaRaft` phase would run after
    the mux exists.

### Why

PR 3 of the 6-PR milestone (see
`docs/superpowers/specs/2026-05-08-serveruntime-boot-decomposition.md`).
Three transport phases now have explicit boundaries. The most important
win: the "groupRaftMux must exist before metaTransport" invariant
(previously a comment at line ~167 of run.go protected only by source
ordering) is now enforced by phase ordering — moving the mux phase
explicitly says: "this construction sequence is required for
correctness, not coincidence."

Behavior preserved: same cluster-key resolution order, same
`SetTrafficLimits(Bulk: 64)`, same kernel-picked-port resolution, same
`EnableMux` plumbing, same cleanup ordering. The only structural change
is that `groupRaftMux` is now constructed after `bootPeerConnections`
instead of after raft node creation; nothing in between reads the mux,
so the move is behavior-neutral.

PR 4 (raft phases — meta-raft wiring + register/start split, the
invariant-critical PR) is next. The split between
`bootMetaRaftWiring` (register only, callbacks set) and
`bootMetaRaftStart` (Start fires the apply loop) becomes the testability
win for the "OnBucketAssigned registered before Start" race-free
guarantee at `run.go:343-348`.
## [0.0.123.0] — 2026-05-09 — bucket-scoped upstream credentials (drop --upstream* flags)

### **Breaking**

- **Removed** `--upstream`, `--upstream-access-key`, `--upstream-secret-key` cmdline flags from `grainfs serve`. Pull-through credentials now live in the IAM Store and are managed via `grainfs iam bucket-upstream set/get/list/delete` over admin UDS. Migration: register one record per bucket with the new CLI; one-shot, no per-restart redeclaration. Closes the cmdline plaintext-secret leak that PR #258 missed.

### Added

- New IAM record type `BucketUpstream` (per-bucket upstream config: bucket, endpoint, access_key, encrypted secret_key, created_at, created_by). Encryption AAD prefix `"bucket-upstream:"+bucket` makes the AAD space provably disjoint from the `sa_id` AAD used by SA secrets.
- `MetaCmdType` IDs 32 (`IAMBucketUpstreamPut`) and 33 (`IAMBucketUpstreamDelete`).
- Admin UDS endpoints under `/v1/iam/bucket-upstream` (POST/GET/GET-list/DELETE). Wire JSON key for the upstream URL is `upstream_url`.
- `grainfs iam bucket-upstream` CLI subcommand group (set/get/list/delete). Secret-key input via `--secret-key-stdin` or `--secret-key-file=<path>` only — never `--secret-key=<value>`.
- `pullthrough.Resolver` interface + `IAMResolver` impl with per-bucket cache and `sync.RWMutex` fast path. Lazy cache invalidation on access-key rotation.
- ADR 0009: Bucket-Scoped Upstream Credentials. References `TODOS.md:63` broader migration design — this PR is Phase 1 (credentials).
- e2e regression test `TestServe_RejectsRemovedUpstreamFlags` locks the breaking flag removal so future code can't silently re-add the flags.

### Changed

- `pullthrough.Backend` constructor signature: `NewBackend(local, upstream Upstream)` → `NewBackend(local, resolver Resolver)`.
- `pullthrough.Backend.GetObject`: cache-miss with no resolver hit returns the original local 404 (no upstream attempt).
- `serveruntime.Config`: removed `UpstreamEndpoint`, `UpstreamAccessKey`, `UpstreamSecretKey` fields. Pull-through wrapping is now unconditional; with zero `BucketUpstream` records, the resolver returns `(nil, false)` and the backend behaves identically to a bare local backend.
- Snapshot format: bucket-upstream records appended as TRAILER bytes to the v3 IAM snapshot (no version bump). Pre-v0.0.123 readers ignore trailing bytes via existing EOF tolerance — bidirectional rolling upgrade preserved.

## [0.0.122.0] - 2026-05-08 — raft v2: per-peer single-flight + Stop-race refactor (PR 13)

### Changed

- `internal/raft/v2/state.go`: added `peerInFlight map[string]bool` to
  `actorState`. Tracks whether an AppendEntries goroutine is currently in
  flight for each peer. Leader-only state: allocated in `becomeLeader`,
  cleared in `stepDownToFollower`.
- `broadcastHeartbeat` and the multi-voter dispatch loop in `handlePropose`
  now skip peers with `peerInFlight[peer] == true` and set the flag to `true`
  before spawning `go dispatchAppendEntries`. Without this gate, every
  heartbeat tick spawns a new goroutine per peer while old ones block on a
  hung/partitioned transport — O(ticks × peers) goroutines accumulate until
  OOM or scheduler thrash.
- `handleHeartbeatReply` clears `peerInFlight[peer]`, gated on
  `cmd.hbDispatchTerm == n.st.currentTerm` so a goroutine from a prior
  leader term (step-down → re-election → stale return) cannot corrupt the
  fresh term's gate.
- New `command.hbDispatchTerm uint64` captures the term at dispatch time.
- `dispatchAppendEntries` sends a synthetic error reply when
  `loadTransport()` returns nil so the gate clears even on the
  transport-misconfigured path. Without this, peerInFlight stays true
  forever and the leader silently stops dispatching.
- `applyCommitted` refactored: `commitIndex` is now advanced per-entry
  inside the loop (after each successful `applyCh` send) instead of via
  end-of-loop set + Stop-race rollback. The post-condition is identical
  to the previous rollback path; the per-entry pattern just makes the
  invariant ("commitIndex tracks delivered entries exactly") obvious in
  the code.

### Notes

- Two new tests in `replication_test.go`:
  - `TestSingleFlight_PartitionedPeerNoGoroutineLeak`: 3-voter cluster
    with a blocking transport on one peer; counter-based assertion that
    max-concurrent dispatches per peer ≤ 1.
  - `TestApplyCommitted_StopRaceLeavesCommitIndexConsistent`: two
    subtests — `ZeroDeliveries` (pre-closed stopCh wins first send) and
    `PartialDelivery` (consumer goroutine reads K=3 then closes stopCh,
    asserts `commitIndex == K`).
- All 60+ tests pass under `-race -count=10` (~40s; 2× consecutive runs
  verified stable).
- 1 of 7 raft v2 follow-up TODOs from M1 adversarial review remains:
  cmdCh backpressure cascade (#3) — structural, separate apply
  goroutine, deferred to dedicated PR.

## [0.0.121.0] - 2026-05-08 — serveruntime boot decomposition PR 2: config + storage phases (6 of ~20)

### Refactored

- `internal/serveruntime/boot_phases.go` (new): six explicit phase functions
  carved out of the top of `Run()`. Each takes `*bootState`, returns `error`,
  and registers its teardown via `state.AddCleanup`:
  - `bootValidateConfig` — validates flag combinations, resolves nodeID via
    `GenerateNodeID` (the only side effect), sets clusterMode, defaults
    `raftAddr` for solo mode, stages `metaDir`/`raftDir` paths, initializes
    `roleRegistry` and `startupDecisions`.
  - `bootAutoMigrate` — runs `cluster.MigrateLegacyMetaToCluster` only when
    `raftDir` is absent and `metaDir` holds a populated legacy meta DB.
    Otherwise a no-op. Must run before any FS or lock side effects.
  - `bootOpenMetaDB` — `MkdirAll(metaDir)` + `badger.Open` + `RegisterDB` +
    writability preflight. Registers two cleanups (close DB, deregister
    resourcewatch). Returns the structured `PreflightBadger` error on
    rejection.
  - `bootValidateTimings` — cross-validates raft/QUIC mux timing flags
    (election ≥ 3× heartbeat; QUIC mux flush ≪ heartbeat; flush ≪ meta
    heartbeat).
  - `bootOpenRaftLogStore` — `raft.NewBadgerLogStore` for the meta raft log,
    propagates `BadgerManagedMode` via `state.storeOpts` so per-data-group
    shared-log opens later in `Run` reuse the same option set without
    re-deriving from cfg.
  - `bootOpenSharedRaftLogDB` — optional C2 P0b shared raft-log DB. No-op
    when `cfg.SharedBadgerEnabled` is false. Refuses to silently abandon
    legacy per-group raft logs (data-loss guard) with a clear migration
    message.

- `internal/serveruntime/boot_state.go`: typed fields added —
  `nodeID`, `raftAddr`, `peers`, `clusterMode`, `metaDir`, `raftDir`,
  `roleRegistry`, `startupDecisions`, `db`, `logStore`, `sharedRaftLogDB`,
  `storeOpts`. Phase functions populate these; the rest of `Run` reads
  through local-variable mirrors (PRs 3-6 will migrate downstream readers
  to read state directly and drop the mirrors).

- `internal/serveruntime/run.go`: top of `Run` shrinks from ~190 lines of
  imperative wiring to six `if err := bootX(state); err != nil { return err }`
  calls plus a small mirror-copy block that reads phase outputs into the
  local variables that PRs 3-6's downstream wiring still uses. Net file
  size: 1470 → 1322 lines.

### Added

- `internal/serveruntime/boot_phases_test.go`: 13 unit tests covering all
  six phases with real I/O on `t.TempDir()`:
  - Config validation: nodeID auto-gen, `--join` without `--raft-addr`,
    `--join` + `--peers` conflict, `--peers` without `--raft-addr`.
  - Auto-migrate: no-op on fresh dir, no-op when raftDir already exists
    (sentinel proves migrate did not touch metaDir), no-op when metaDir
    is empty.
  - Meta DB: creates and opens, preflight decision recorded; cleanup is
    idempotent.
  - Timing validation: rejects too-fast election; rejects flush not <<
    heartbeat; accepts valid (200ms hb / 1s election / 2ms flush).
  - Raft log store: opens, propagates `BadgerManagedMode` via storeOpts.
  - Shared raft-log DB: disabled is a no-op (no extra cleanups), legacy
    per-group raft dirs trigger the data-loss guard, happy path opens at
    the expected path with the expected role.

### Why

PR 2 of the 6-PR milestone (see
`docs/superpowers/specs/2026-05-08-serveruntime-boot-decomposition.md`).
Six phases with real-component integration tests now cover the
config + storage entry of `Run`. Test discoverable invariants that
were previously protected only by code comments — e.g., auto-migrate
ordering relative to MkdirAll/badger.Open, shared-badger refusing to
overwrite legacy per-group state.

PR 3 (transport phases — QUIC, peer connections, raft RPC mux) is next.

## [0.0.120.0] - 2026-05-08 — serveruntime boot decomposition PR 1: bootState + cleanup stack

### Refactored

- `internal/serveruntime/boot_state.go` (new): `bootState` struct with an
  explicit cleanup stack. Methods:
  - `newBootState(cfg Config) *bootState`
  - `(*bootState).AddCleanup(fn func())` — push (mirrors a former `defer`
    registration in `Run`).
  - `(*bootState).Cleanup()` — LIFO drain. Idempotent (slice reset to nil
    after first drain); panic-safe (each fn wrapped in `recover` so one
    panicking cleanup does not skip the rest).
- `internal/serveruntime/run.go`: 11 boot-related `defer`s replaced with
  `state.AddCleanup(...)` registrations, exactly where the original defer
  lived. The function exit registers a single `defer state.Cleanup()` near
  the top so all teardown runs LIFO. The 12th `defer shutdownCancel()`
  (line 1448) stays as a regular defer — it is the standard
  `context.WithTimeout` cancel idiom, not boot teardown.

### Added

- `internal/serveruntime/boot_state_test.go`: 6 unit tests covering LIFO
  ordering, drain idempotency, panic safety in one fn does not skip the
  rest, nil-fn skip, nil-receiver safety, and config binding.

### Why

First step of a 6-PR milestone to decompose the 1463-line `Run` orchestrator
into ~20 explicit boot phases. Motivation is testability: per-phase wiring
invariants (e.g., "`OnBucketAssigned` registered before `metaRaft.Start`",
`run.go:343-348`) are protected only by code comments today. The cleanup
stack is the foundation; PRs 2-6 will move local variables into typed
`bootState` fields and extract phase functions, enabling tests like:

```go
state := newTestBootState(t, testCfg(t))
t.Cleanup(state.Cleanup)
require.NoError(t, bootMetaDB(ctx, state))
require.NoError(t, bootQUICTransport(ctx, state))
require.NoError(t, bootMetaRaftWiring(ctx, state))  // register only
// assert callbacks registered before Start runs in next phase
require.NoError(t, bootMetaRaftStart(ctx, state))
```

Behavior is unchanged in this PR. The cleanup ordering is bit-identical to
the original defers (LIFO push order = LIFO drain order). Full unit suite
passes; `TestSmoke_DeploymentVerification` boots and tears down cleanly.

### Milestone tracker

See `docs/superpowers/specs/2026-05-08-serveruntime-boot-decomposition.md`
(gitignored) for the full 6-PR plan, locked decisions, and per-phase test
contracts. PR 1 = infra. PRs 2-6 = phase extraction in batches (config,
transport, raft, storage, services).

## [0.0.119.0] - 2026-05-08 — raft v2 M1 follow-ups: 4 quick wins (PR 12)

### Changed

- `handleAppendEntries` and `handleRequestVote` now `defer n.publish()` after
  the stale-term early return, instead of calling `n.publish()` per branch.
  Eliminates per-AE/RV `*readState` allocation duplication on the heartbeat
  hot path. (TODO #4)
- New `stepDownToFollower(term)` helper performs the term-advance state
  mutation (currentTerm, vote, leader, leader-only state cleanup) and
  persists HardState without publishing or rearming the election timer —
  caller is expected to do those at function exit. `becomeFollower` is now a
  thin wrapper that adds publish + resetElectionTimer.
- `Node.transport` is now `atomic.Pointer[Transport]`. `SetTransport` calls
  Store; outbound dispatch goroutines (sendRequestVote, dispatchAppendEntries)
  read via Load through `loadTransport()`. A late SetTransport after Start
  is now data-race-free, though still discouraged. (TODO #5)
- `Bootstrap()` doc tightened to call out that it is INFORMATIONAL ONLY today
  — actor.run() auto-promotes single-voter regardless of whether Bootstrap
  was called. Callers MUST NOT depend on "Start without Bootstrap stays
  Follower"; that contract is reserved for the M5 caller migration. (TODO #6)
- Single-voter auto-promote in `actor.run()` now routes through
  `becomeLeader()` for state-transition consistency with multi-voter. The
  becomeLeader path is uniform across single/multi-voter; `LogEntryNoOp` is
  appended only when `len(cfg.Peers) > 0` (§5.4.2 motivation does not apply
  to single-voter — self-quorum commits at append, no prior-term uncommitted
  entries possible). Heartbeat ticker is also skipped for single-voter
  (no peers to broadcast to). (TODO #7)
- `handlePropose` single-voter shortcut now routes through `applyCommitted`
  instead of inlining the apply logic — preserves the same behaviour but
  consolidates the apply+waiter resolution path through one helper.

### Notes

- Pure correctness/perf bundle. No new public API surface. All 55+ tests
  pass under `-race -count=10` (~33s).
- 3 of the 7 raft v2 follow-up TODOs from the M1 adversarial review remain:
  goroutine explosion on partitioned peer (#1), applyCommitted commitIndex
  regression on Stop (#2), cmdCh backpressure cascade (#3). Larger
  structural changes; deferred to dedicated PRs.

## [0.0.118.0] - 2026-05-08 — ClusterInfo Snapshot (single seam for cluster topology fan-out)

### Refactored

- `internal/server/server.go`: 7 type-asserted mini-interfaces deleted —
  `clusterPeerAddrs`, `clusterPeerStates`, `clusterPeerSnapshot`,
  `clusterBucketAssignments`, `clusterShardGroups`,
  `clusterObjectIndexSummary`, `clusterPlacementReporter`. Their methods
  fold into `ClusterInfo` proper as one `Snapshot() cluster.ClusterStatus`
  call (peer/topology data) plus `ObjectIndexSummary(bucket)` and
  `PlacementReport(bucket, key, maxRows)` (parameterized reports).
- `internal/cluster/cluster_status.go` (new): `ClusterStatus` struct with
  nullable fields `PeerSnapshot`, `PeerAddrs`, `PeerStates`,
  `BucketAssignments`, `ShardGroups`. Single-shot snapshot returned by
  `ClusterInfo.Snapshot()`; nil/empty fields signal an unsurfaced
  capability.
- `internal/serveruntime/adapters.go`: `RaftClusterInfo.Snapshot()` added —
  composes the existing `PeerSnapshot()` / `PeerAddrs()` / `PeerStates()` /
  `BucketAssignments()` / `ShardGroups()` methods; production behavior
  unchanged.
- 9 type-assertion call sites collapse into direct method/field access:
  - `handlers.go::clusterStatus` (6 of the 7 fan-outs lived here): one
    `s.cluster.Snapshot()` plus `snap.PeerAddrs`/`snap.ShardGroups`/...
    field reads.
  - `handlers.go::clusterPlacement`: `s.cluster.PlacementReport(...)`
    direct call (the `clusterPlacementReporter` ok-check disappears
    because `PlacementReport` is mandatory on the interface; nil-cluster
    branch above the call still short-circuits with the empty report).
  - `handlers.go::evaluateRemovePeerPreflight`: peer snapshot read via
    `s.cluster.Snapshot().PeerSnapshot`.
  - `cluster_health.go`: peer snapshot read via the same path.
- Test fakes updated: `fakeClusterInfo`, `fakeTopologyClusterInfo`,
  `fakeClusterInfoWithoutSnapshot` (`cluster_remove_peer_test.go`) drop
  the now-orphan `PeerAddrs/PeerStates/PeerSnapshot/BucketAssignments/
  ShardGroups` methods, gain `Snapshot()` (composing their fields) and
  zero-value `ObjectIndexSummary` / `PlacementReport` stubs.
  `fakeTransferLeader` (`cluster_admin_uds_test.go`) inherits the new
  methods via its `*fakeClusterInfo` embedding — no edit needed.

Behavior preserved on production paths: `RaftClusterInfo` already
implemented every former mini-interface, so dashboard JSON output is
identical. The dashboard's `object_index_summary` field is now always
present in cluster mode (previously absent when the impl didn't satisfy
the optional interface — a meaningless distinction in practice because
the only impl satisfied it). Net delta: −44 lines in server.go,
−59 lines in test fakes (orphan method removals), +47 lines for the
new struct + Snapshot composer + helper stubs.

### Why

Continuation of the deepening that produced #254 (MutationBroker) and
#260 (authz PostLoad helper). `s.cluster.(...)` type assertions modeled
"optional capability" at the type system level when in production every
capability was always present — pure ceremony. Single-seam Snapshot
collapses the fan-out, makes the dashboard's intent ("read a coherent
view of cluster state") explicit, and prepares the ground for the
DistributedBackend god-object decomposition by establishing what the
cluster status surface actually is.

## [0.0.117.0] - 2026-05-08 — authz PostLoad helper (single seam for handler ACL re-checks)

### Refactored

- `internal/server/authz.go` gains two helper methods on `*Server`:
  - `mustAuthorize(ctx, c, bucket, key, action) (denied bool)` — PreLoad
    authz for cross-bucket reads (CopyObject source). Builds the
    `PermCheckInput`, calls `s.authz.Decide` at `PhasePreLoad`, writes a
    403 `AccessDenied` XML response on deny, and returns `denied`.
  - `mustAuthorizePostLoad(ctx, c, bucket, key, action, aclByte) (denied bool)`
    — PostLoad authz once an object's metadata is loaded. Same shape but
    converts `obj.ACL` (`uint8`) to `s3auth.ACLGrant` internally and
    evaluates `PhasePostLoad`.
- 5 mutation-adjacent call sites in `internal/server/handlers.go` migrate
  from inline `s.authz.Decide(...) + writeXMLError(...) + return` (8 lines
  each) to the helper's 3-line `if s.mustAuthorizePostLoad(...) { return }`
  pattern:
  - `handleGetObject` (PostLoad, GetObject + obj.ACL)
  - `handleGetObjectRange` (PostLoad, GetObject + obj.ACL, returns `true`
    to caller)
  - `handleHeadObject` (PostLoad, HeadObject + obj.ACL)
  - `handleCopyObject` source PreLoad (no ACL yet)
  - `handleCopyObject` source PostLoad (after `srcObj.ACL` loaded)

Behavior preserved: same `Decide` call, same 403 response, same XML body
(`AccessDenied` / "Access Denied"). Net delta: −37 lines in handlers.go,
+38 lines (helpers + tests) in authz.go / authz_helper_test.go.

### Why

Continuation of the deepening that produced #254 (MutationBroker). Same
single-seam pattern: scattered `Decide + writeXMLError + return` triplets
collapse into one method per phase. The interface stays the same — only
the call site shrinks — and `s3auth.ACLGrant(obj.ACL)` boilerplate moves
inside the helper. Future PostLoad sites get the helper for free.

## [0.0.116.0] - 2026-05-08 — raft v2 M2 PR 11: crash recovery wiring

### Added

- `StableStore` interface and `memStableStore` in `internal/raft/v2/stablestore.go`:
  persists `HardState` (currentTerm + votedFor) across restarts to satisfy Raft
  §5.4.1 safety invariant.
- `badgerStableStore` in `internal/raft/v2/stablestore_badger.go`: durable
  `StableStore` backed by BadgerDB. Encoding is binary (8B term + 4B votedForLen +
  votedFor bytes), not JSON. Single key per Raft group under a caller-supplied prefix.
- `Config.LogStore` and `Config.StableStore` fields: opt-in persistent stores.
  Both default to in-memory; supplying both enables full crash recovery. Pairing a
  persistent `LogStore` with an in-memory `StableStore` violates §5.4.1 — documented,
  not enforced.
- `Node.stable` field wired through `NewNode`.
- `persistHardState()` helper in `actor.go`: called synchronously before any reply
  that exposes the new term or vote to peers. Covers `becomeFollower`, `becomeCandidate`,
  vote-grant in `handleRequestVote`, and term-step-down in `handleRequestVote` and
  `handleAppendEntries` (both the Leader and non-Leader inline paths).
- `stablestore_test.go`: factory-pattern tests for `memStableStore` and
  `badgerStableStore` (first-open-returns-zero, round-trip, overwrite, persist-across-reopen).
- `recovery_test.go`: four end-to-end recovery scenarios — fresh start, log replay from
  badger, HardState persisted across election vote (§5.4.1), restarted node is Follower.

### Changed

- `NewNode` signature changed from `*Node` to `(*Node, error)` to surface I/O errors
  when loading `HardState` from a persistent `StableStore` on restart.
- Single-voter bootstrap in `actor.run()` only advances `currentTerm` to 1 when it is
  currently 0 (fresh start); on restart the persisted term is preserved unchanged.
- All `NewNode` call sites updated across test files.

## [0.0.115.0] - 2026-05-08 — raft v2 M2 PR 10: BadgerDB LogStore implementation

### Added

- `badgerLogStore` in `internal/raft/v2/logstore_badger.go`: durable LogStore
  backed by BadgerDB. Key format is `prefix || be64(idx)` (big-endian so
  lexicographic order matches numeric order). Value encoding is a compact 21-byte
  fixed header (Term 8B + Index 8B + Type 1B + CommandLen 4B) followed by the
  command payload — binary, not JSON.
- `newBadgerLogStore(db *badger.DB, prefix []byte)` constructor. A prefix-based
  design allows multiple Raft groups to coexist in a single Badger DB. On open,
  a reverse iterator O(1) scan populates the cached `lastIdx` atomic and the
  highest entry's value is decoded as a sanity check (rejects prefix collision).
- Chunked TruncateAfter (1024 keys/txn) so partition-heal-scale truncations
  don't hit Badger's `ErrTxnTooBig`. CmdLen bounds-check at encode boundary
  (rejects Commands ≥ 4 GiB rather than silently truncating).
- `logstore_badger_test.go`: 4 persistence-specific tests —
  `TestBadgerLogStore_EmptyDirectoryFirstOpen`,
  `TestBadgerLogStore_PersistsAcrossReopen`,
  `TestBadgerLogStore_TruncateAfterPersistsToDisk`,
  `TestBadgerLogStore_TruncateAfter_LargeChunkedDeletes`,
  `TestBadgerLogStore_RejectsCorruptHighestEntryOnOpen` — that close and reopen
  the DB to verify durability at the store layer (crash-recovery wiring of
  `NewNode` is PR 11).

### Changed

- `logstore_test.go` refactored to Option A factory pattern: `allStores` table
  drives the 8 original LogStore scenario tests across both `mem` and `badger`
  implementations. Test names updated from `TestMemLogStore_*` to
  `TestLogStore_*`. Total test count: 35 → 44 (count=1 run).
- LogStore interface doc: added canonical-Command convention (`len==0` ↔ `nil`);
  FSMs must length-check rather than nil-check.

### Notes

- `badgerLogStore` is not yet wired into `NewNode`; `memLogStore` remains the
  default until PR 11 adds the config field and crash-recovery replay. Two
  implementations now satisfy the `LogStore` interface: in-memory (default) and
  persistent (opt-in via PR 11).
## [0.0.114.0] - 2026-05-08 — raft v2 M2 PR 9: LogStore interface (pure refactor)

### Added

- `LogStore` interface in `internal/raft/v2/logstore.go`: defines the contract
  for Raft log access (`FirstIndex`, `LastIndex`, `Entry`, `TermAt`, `Append`,
  `TruncateAfter`, `EntriesFrom`). M2 persistence foundation — BadgerDB backing
  lands in PR 10; crash-recovery wiring in PR 11.
- `memLogStore` in-memory implementation (same file): actor-goroutine-owned,
  no locks. `EntriesFrom` returns a deep copy (slice header + each entry's
  `Command []byte`) so callers may retain the slice across subsequent store
  mutations and freely mutate the returned bytes.
- `ErrLogIndexOutOfRange` sentinel error; actor panics on out-of-range access
  (programmer bug, not a recoverable runtime condition).
- `mustEntry` / `mustTermAt` helpers in `actor.go`: wrap the panic-on-error
  path to keep call sites readable.
- `logstore_test.go`: 8 table-driven tests covering empty-store invariants,
  append/read, truncation, range reads, cap enforcement, deep-copy semantics
  (including `Command []byte` aliasing), `Append` non-contiguous panic with
  exact message match, and the truncate-then-append round-trip the actor
  exercises during conflict resolution.

### Changed

- `actorState.log` field type: `[]LogEntry` → `LogStore` (pure refactor, no
  behavior change). All 15 mutation/read sites in `actor.go` and 3 helper
  methods in `state.go` updated to use the LogStore interface.
- `NewNode` initializes `actorState.log` with `newMemLogStore()`.
- `lastLogTermAt` removed from `state.go`; sole call site (`buildAppendEntriesArgs`)
  now uses `mustTermAt` directly. One panic-wrapper instead of two.
- Test seeding sites in `replication_test.go` updated to use `seedLogEntries`
  helper (replaces direct slice assignment). Post-Stop assertions updated to
  use `LogStore.Entry` / `LastIndex` (replaces direct slice indexing).

### Notes

- No behavior change. All 33 prior tests continue to pass under `-race
  -count=10`; +2 new LogStore tests pass alongside (35 total). This PR is the
  persistence interface layer only — `memLogStore` is the sole implementation
  until PR 10 adds BadgerDB backing.

## [0.0.113.0] - 2026-05-08 — Complete IAM-only cleanup (PR #255 follow-up)

### Fixed

- 12 e2e test files that PR #255 left half-migrated: the legacy
  `"--access-key", "test", "--secret-key", "test"` argument pair was
  deleted from `cmd/serve` invocations in
  `auto_snapshot_test.go`, `backup_test.go`, `cluster_test.go`,
  `dashboard_healing_card_test.go`, `encryption_test.go`,
  `erasure_test.go`, `jepsen_impl_test.go`, `migration_injector_test.go`,
  `pullthrough_test.go`, `restart_recovery_test.go`, and `smoke_test.go`.
- `benchmarks/run-baseline.sh` and `tests/fuse_s3_colima/fuse_s3_colima_test.go`
  had the same orphaned argument pair on their `grainfs serve`
  invocations — caught by adversarial review. Both are off the
  default test path (one is a benchmark script, the other lives
  behind the `colima` build tag) so the unit + e2e suites didn't
  surface them.
  In master 5748d2e0 (v0.0.112.0), `bootstrapAdminViaUDS()` calls had
  been **added** but the legacy lines had **not been removed** —
  cobra rejected the unknown flags with `Error: unknown flag: --access-key`
  on every invocation, so all 12 e2e tests hit `waitForPort` timeout.
- `cmd/grainfs/serve_config.go`: removed `"secret-key"` from the
  redaction switch (the flag itself is gone in v0.0.112.0; the case was
  dead) and added `"upstream-secret-key"` to the redaction set —
  caught by adversarial review as a pre-existing leak: the flag was
  declared on `serveCmd` but never redacted, so its raw value landed
  in the structured startup log and the on-disk flags snapshot.
  `cluster-key`, `alert-webhook-secret`, `heal-receipt-psk`
  redaction unchanged.

### Changed

- `docs/RUNBOOK.md`: production deployment examples no longer pass
  `--access-key $GRAINFS_ACCESS_KEY` / `--secret-key $GRAINFS_SECRET_KEY`
  to `grainfs serve` (Step 4 local + cluster modes; Docker `-e`; K8s
  ConfigMap + Deployment env). Added a Step 4 preamble explaining that
  `$GRAINFS_ACCESS_KEY`/`$GRAINFS_SECRET_KEY` now refer to the admin SA
  credentials obtained via the bootstrap flow and are used only by S3
  client-side examples (`aws --endpoint-url`).

## [0.0.112.0] - 2026-05-08 — IAM-only auth, drop --access-key flag

### Removed (BREAKING)

- `--access-key` / `--secret-key` flags from `grainfs serve`. Bootstrap now goes
  exclusively through the admin UDS:
  `grainfs iam sa create admin --endpoint <data>/admin.sock`.
- Sticky `auth_enabled` bit (`Store.AuthEnabled()`, `ApplyAuthEnable`,
  `ProposeAuthEnable`, `MetaCmdTypeIAMAuthEnable` dispatcher case). The
  authz middleware always evaluates the IAM layer; no anonymous-mode
  escape hatch.
- Snapshot v1 + v2 readers — v3 only. `auth_enabled` byte gone from the
  snapshot header.
- E2E `--access-key`/`--secret-key` flag pattern across 14+ test files;
  replaced by the new `bootstrapAdminViaUDS()` helper.
- `TestIAM_E2E_ET5_StickyAuth_ClusterRestart` — the sticky bit it exercised
  no longer exists.

### Added

- `IAMInitFirstSA` MetaCmdType (31) — composite payload commits
  `SA + AccessKey + WildcardGrant` atomically on the first admin-UDS
  bootstrap. Race guard via fixed `DefaultSAID` + completeness predicate
  `isFirstSACommitted` (4-point: SA + active key + wildcard); a partial
  Apply re-fires on retry instead of leaving the cluster permanently
  half-bootstrapped. Concurrent admin-UDS callers receive `409 Conflict`
  if their access key didn't land.
- `bootstrapAdminViaUDS(t, dataDir)` test helper plus the
  `bootstrapAdminViaUDSAny(...)` cluster variant + shared encryption-key
  helper used by multi-node e2e suites.
- e2e bootstrap acceptance tests F1–F4 (`tests/e2e/iam_bootstrap_test.go`):
  empty-store wildcard issuance, second-SA no-grant, pre-bootstrap 401,
  post-bootstrap ListBuckets/PutObject/GetObject 200.
- ADR 0008 `docs/adr/0008-drop-access-key-flag.md` capturing the design
  decisions surfaced in `/grill-me` + `/plan-eng-review`.
- RUNBOOK section "Admin UDS — Bootstrap & Permissions" documenting
  socket perms (chmod 0660, `--admin-group` chown), the bootstrap CLI
  flow, and the 409 race outcome.

### Changed

- `HandleSACreate` dispatches by store state: empty → `ProposeInitFirstSA`
  composite + grants response; non-empty → existing `SACreate` + `KeyCreate`
  with no auto-grant.
- `ApplyInitFirstSA` decodes the composite via the FlatBuffers zero-copy
  `*BlobBytes()` accessors (drops the per-blob `readByteVector` alloc churn).
- README Quick Start, CONTEXT.md, and CLAUDE.md Persona Test now describe
  the admin-UDS bootstrap flow instead of the removed flags.

### Migration

Pre-1.0 product, no migration support. For existing clusters:

- Bootstrapped previously via `--access-key`: state is already in IAM;
  upgrade is effectively a no-op (sticky bit was true, "always on" now
  means the same thing).
- Running in anonymous mode (no SA, sticky=false): all S3 traffic 401
  after upgrade. Run
  `grainfs iam sa create admin --endpoint <data>/admin.sock` to
  bootstrap. See `docs/adr/0008-drop-access-key-flag.md` for the full
  rationale.

### Note on master drift

This PR was rebased twice over master while in flight: PR #250
(`v0.0.107.0` RequestAuthorizer), PR #251 (`v0.0.108.0` raft v2 M1),
PR #252 (raft v2 M2 prep), PR #253 (`v0.0.110.0` standalone E2E
test flag fixes), and PR #254 (`v0.0.111.0` MutationBroker) all
landed first. PR #253 is **superseded by this PR**: its `--access-key`
re-additions are removed here in favor of the new `bootstrapAdminViaUDS()`
helper. PR #250's `RequestAuthorizer.Decide` architecture is preserved;
the old sticky-bit gates are replaced by a no-op `Store.AuthEnabled() bool { return true }`
compat shim.

## [0.0.111.0] - 2026-05-08 — MutationBroker + multipart/copy emit fixes

### Added

- `internal/server.MutationBroker`: single seam dispatching S3 mutation observations to metrics + event observers, replacing ad-hoc fan-out across 7 handler sites. Adding a new mutation kind becomes a compile-time error in every observer.
- `metricsObserver` (sync) and `eventObserver` (async via injected emit closure) implement `MutationObserver`. `eventObserver` mirrors the closure-injection pattern at `heal_emitter.go:30` and preserves the existing bounded-channel drop-on-full semantics.
- `Server.mutations *MutationBroker` field, wired in `NewWithServerStorage` after `buildAuthorizer()` so the observer captures the final `s.emitEvent` closure.

### Fixed

- `handleCompleteMultipartUpload` now emits `EventActionPut` to the event store at multipart finalize. Previously updated metrics only, leaving multipart-uploaded objects invisible to event subscribers (audit, dashboards, eventstore queries).
- `handleCopyObject` now emits `EventActionPut` at the destination to the event store. Previously updated metrics only, leaving copies invisible to event subscribers. Source bucket/key are intentionally not encoded — `eventstore.Event` has no source field; copy surfaces as Put@destination, matching how legacy PutObject handlers emit.

### Changed

- 7 mutation call sites in `internal/server/handlers.go` migrated from ad-hoc `recordObjectWriteMetrics + s.emitEvent` pairs to single `s.mutations.OnObjectWrite/Delete/Copy/BucketCreate/BucketDelete` calls. Locality moved from 7 scattered sites to 1 broker; new observers (audit attribution, retention, lifecycle hooks) can now be added in one place instead of 7.

### Notes

- Read events (`EventActionGet` at handlers.go:550, 740) and system events (`EventTypeSystem` at handlers.go:1606, 1687) intentionally remain on the legacy `s.emitEvent` path — different conceptual seam.
- No external `RegisterMutationObserver` API yet (YAGNI per locked decision R1). Add when an external component (plugin, audit shim) actually wants to register.

## [0.0.110.0] - 2026-05-08 — fix standalone E2E tests broken since v0.0.98.0

### Fixed

- 11 E2E tests (`tests/e2e/{auto_snapshot,backup,cluster,dashboard_healing_card,encryption,erasure,jepsen_impl,migration_injector,pullthrough,restart_recovery,smoke}_test.go`) now pass `--access-key test --secret-key test` to their standalone `serve` invocations. These tests roll their own `exec.Command(binary, "serve", …)` rather than going through `helpers_test.go:Start()` (which already passes the flags), and were silently rejected with `AccessDenied: unknown access key: test` since v0.0.98.0 (#237 IAM Foundation) made the SigV4 verifier IAM-aware. The README still advertises an "익명 모드 (개발용)" mode for omitting the flags, but the code path went away with #237 — the README claim is currently a lie and is left for a separate cleanup PR.
- `tests/e2e/migration_injector_test.go`: pass `--src-access-key/--src-secret-key/--dst-access-key/--dst-secret-key` to the `migrate inject` subcommand so it can sign requests against both source and destination servers.
- `tests/e2e/pullthrough_test.go`: pass `--upstream-access-key/--upstream-secret-key` to the local server so its pull-through fetches authenticate against the upstream.

### Notes

- Two pre-existing baseline failures surfaced during this audit and are NOT addressed here (each fails on origin/master without these test fixes, so they are independent issues): `TestRestartRecovery_SweepsOrphanArtifacts` and `TestAutoSnapshot_CreatesSnapshotAutomatically` show timing/cleanup issues unrelated to authn. Tracked separately.

## [0.0.109.0] - 2026-05-08 — raft v2 M2 prep: 4 correctness fixes

### Added

- `LogEntryNoOp` (type=3) in `internal/raft/v2/types.go`: dedicated log-entry
  type for leader no-op entries. FSM consumers must skip entries of this type.
- `becomeLeader` now appends a `LogEntryNoOp` at the new term immediately on
  election (Raft §5.4.2). Prior-term uncommitted entries become commitable as a
  side effect of the no-op's commit. FSM caller note: in a 3-voter cluster the
  first applied entry is always the no-op; user `ProposeWait` returns index 2.
- `buildAppendEntriesArgs` now caps the batch at `cfg.MaxEntriesPerAE` (default
  512, matching v1). Prevents shipping the entire log to a fresh follower in one
  RPC. Added `defaultMaxEntriesPerAE = 512` const in `actor.go`.
- `handleAppendEntries` now validates `e.Index == target` for each entry in the
  incoming batch. On mismatch, rejects the AE without mutating the log and sends
  `ConflictIndex=target, ConflictTerm=0` to force leader resync (Log Matching
  §5.3 correctness fix for future joint consensus / dueling leaders scope).
- `applyConflictHint` replaces the O(N) backward scan with `sort.Search` binary
  search. Raft terms are monotonically non-decreasing in the leader's log;
  binary search finds the rightmost entry at ConflictTerm in O(log N).
- 4 new tests in `replication_test.go`:
  `TestBecomeLeader_AppendsNoOp`, `TestBuildAE_RespectsMaxEntriesPerAE`,
  `TestAE_RejectsMismatchedEntryIndex`,
  `TestApplyConflictHint_BoundedScanOnLargeLog` (2 sub-tests).

### Notes

- All 4 fixes are in `internal/raft/v2/actor.go`; no callers exist yet (v2 not
  imported in production). Correctness prerequisites for M2 LogStore + crash
  recovery: once persistence lands, silent corruption from latent bugs around
  `currentTerm`/`commitIndex`/log mutation becomes a real data-loss risk.
- Existing multi-voter tests updated to account for the no-op at index 1.
  Equivalence tests note deliberate v1↔v2 index divergence (v1 requires
  explicit `SetNoOpCommand`; v2 always emits no-op on election).

## [0.0.108.0] - 2026-05-08 — raft v2 actor pattern foundation (M1 milestone)

### Added

- New `internal/raft/v2/` package: actor + channel concurrency model replacing
  `internal/raft`'s mutex-protected design. Single goroutine owns mutable state;
  hot-path readers (`State`, `Term`, `IsLeader`, `LeaderID`, `CommittedIndex`)
  use `atomic.Pointer[readState]` for ~ns reads. Mirrors etcd-io/raft's
  SoftState publication. v2 has zero callers; production wiring lands at M5.
- Single-voter Propose round-trip with auto-leader bootstrap.
- Multi-voter election state machine: randomized election timer, Candidate
  state, vote sending via Transport, Leader transition on majority, heartbeat
  ticker. Election outcome verified equivalent to v1 in 3-voter test.
- Inbound RPC handling: full Raft §5.4 RequestVote vote-granting logic with
  term step-down, votedFor state, log up-to-date check.
- Log replication via AppendEntries: leader-side matchIndex/nextIndex
  tracking, majority commit advancement under §5.4.2 term gate, follower
  log validation + apply on leaderCommit.
- AppendEntries conflict handling: ConflictTerm/ConflictIndex hint on
  rejection, follower log truncation on accept, leader fast nextIndex
  backoff (skip entire conflict term in one round-trip).
- v1↔v2 equivalence harness (`equivalence_test.go`): drives both
  implementations through identical scenarios, compares log + observable
  state. Four scenarios shipped (single propose, multi propose, 3-voter
  election, divergent log convergence).
- Public API mirrors of v1 for M5 swap-time parity: `Bootstrap`,
  `Configuration`, `AddVoter`/`AddVoterCtx`, `RemoveVoter`, `AddLearner`,
  `PromoteToVoter`, `TransferLeadership`. Membership-change methods stub
  with `ErrNotImplemented` (real impl lands in M2 with joint consensus).
- Test coverage: 27 tests pass `go test -race -count=10` clean (~22s).

### Notes

- This PR ships v2 as new code only; nothing imports it yet. v1 raft remains
  canonical. M2 (persistence + snapshots), M3 (property tests + chaos),
  M4 (staging soak), M5 (per-package import flip) follow per
  `docs/superpowers/plans/2026-05-08-raft-actor-redesign.md`.
- See `internal/raft/v2/README.md` for design rationale and capability
  matrix.

## [0.0.107.0] - 2026-05-08 — S3 Request Authorization Decision (deepened authz seam)

### Added

- `internal/s3auth.RequestAuthorizer` composes IAM grant, bucket policy, and
  object ACL into a single `Decision{Allow, Layer, Reason}` per request. Two
  phases: `PhasePreLoad` (Layer 1+2, called from middleware before object load)
  and `PhasePostLoad` (Layer 1+2+3, called from handlers after object load).
  See `CONTEXT.md` § "S3 Request Authorization Decision" for the full contract.
- `internal/server/authorizer_wiring_test.go` verifies the server builds a
  non-nil authorizer.
- `internal/server/handlers_copy_acl_test.go` covers anonymous CopyObject
  source ACL gating.

### Changed

- `internal/server` authz middleware delegates Layers 1-3 (IAM grant + bucket
  policy + post-load ACL) to `RequestAuthorizer`. Layer 0 (AccessKey bucket
  scope, introduced in v0.0.106.0) stays in middleware because it depends on
  iam-specific request context. Object handlers (`handleGet`, the standard-path
  GET helper, `handleHead`) delegate post-load ACL decisions through the same
  authorizer seam. Audit ownership unified — every allow and deny is recorded
  by the authorizer rather than by middleware and handlers separately.
- Object ACL evaluation now runs unconditionally at the post-load gate. In
  no-auth-flag clusters (no `--access-key/--secret-key` and no IAM), private
  ACL objects are no longer anonymously readable — they were previously
  readable because the ACL probe was guarded by `s.verifier != nil`. Existing
  tests that PUT objects without an explicit ACL and read them anonymously
  were updated to use `public-read`. Iceberg-managed metadata files are now
  written with `ACLPublicRead` so they remain anonymously readable in no-auth
  mode.

### Fixed

- CopyObject now enforces source `GetObject` ACL. Previously a caller with
  destination write permission could copy a private source object regardless
  of source ACL, because the source object was loaded by the storage facade
  for existence/precondition checks but no ACL gate ran on it.

## [0.0.106.0] - 2026-05-08 — IAM bucket-scoped access keys

### Added

- IAM bucket-scoped access keys: `AccessKey.BucketScope []string` 차원 추가. POST
  `/v1/iam/sa/{id}/key`에 `buckets:[]` 필드 (CLI: `grainfs iam key create --bucket
  <name>`, 반복 가능). 발급 시 strict 검증 (`scope ⊆ SA grants`, sentinel
  `*`/`__system__` reject → 400). Authz Layer 0에서 scope 위반 시 403 + audit
  `reason=key_scope_mismatch`. Immutable at issue (변경 시 rotation).
- ListBuckets (`GET /`) 응답이 scoped key의 BucketScope로 필터링됨 — scope에 없는
  bucket은 응답에 미포함되어 enumeration leak 차단.

### Changed

- 새 raft `MetaCmdType 30 IAMKeyCreateScoped` (scope 비어있으면 기존 type 23 사용).
  Mixed-version 클러스터에서 v0.0.105.0 follower는 type 30을 unknown으로 graceful
  no-op 처리하고 warn 로그를 남긴다 — 의도적 propagation 결손이므로 운영자는
  rolling upgrade 완료 후에만 scoped key를 발급해야 한다.
- IAM snapshot binary `version=2` (v1 호환 read path 보존, v3+ 거부).
- `iam.ResolveSA`가 `(*AccessKey, string, bool)` 반환으로 시그니처 확장 — auth
  middleware의 redundant `LookupKey` 호출 제거 (TOCTOU window 단축 + hot path
  atomic load 1회로 통합).

### Migration

- v0.0.105.0 이하 키는 `BucketScope=nil`로 자동 unrestricted (backward compat).
- Rolling upgrade: 모든 노드를 v0.0.106.0+로 올린 뒤에 scoped key 발급.

## [0.0.105.0] - 2026-05-08 — LifecycleManager subscribes to raft leader events

### Changed

- `LifecycleManager` no longer polls `raft.Node.State()` every 250ms to
  decide whether to run the worker. It registers a `raft.Event` channel
  via `RegisterObserver` and reacts to `EventLeaderChange` events as they
  arrive. The eager `reconcile` at startup is preserved so a node that is
  already leader picks up the worker without waiting for the next event.
  Net effect: leader-flip latency drops from up to 250ms to next-event
  delivery (~heartbeat interval), and idle followers no longer wake every
  250ms.
- The internal `leadershipSource` interface gains
  `RegisterObserver(chan<- raft.Event)` and `DeregisterObserver(chan<-
  raft.Event)`. `*raft.Node` already implements these from
  `internal/raft/observer.go`. The `pollEvery` field on the manager is
  removed.
- Test fake `fakeLeadership` was updated to mirror real `*raft.Node`
  semantics: `set(state)` records the state and emits an
  `EventLeaderChange` event to every registered observer, so the existing
  Follower→Leader→Follower test driving still works without polling.
- New tests verify observer registration/deregistration on Run lifecycle
  and that non-leader-change events (e.g. `EventFailedHeartbeat`) do not
  trigger reconcile.

## [0.0.104.0] - 2026-05-08 — wire NFSv4 cache invalidator into cluster registry

### Fixed

- Cross-protocol cache coherency for NFSv4 in cluster mode: an S3 PUT/DELETE
  replicated via Raft from another node now invalidates this node's NFS
  metadata caches (`StateManager.fileMeta`, parent-directory mtime). Before
  this change `cluster.Registry.InvalidateAll` only fanned out to the S3
  CachedBackend; NFSv4 readers could see stale stat data and stale READDIR
  output until the next backend round-trip or natural cache expiry.

### Added

- `nfs4server.StateManager.InvalidateKey(key)` — drops the per-key
  `fileMeta` entry and refreshes the parent directory's stored mtime so the
  next READDIR is treated as a new generation. Filehandles are
  intentionally preserved because NFS clients hold open handles across the
  invalidation; stale `fhToPath` mappings surface as backend NotFound on
  next access rather than producing wrong data.
- `nfs4server.Server.Invalidate(bucket, key)` — duck-typed
  `cluster.CacheInvalidator` entry point. Buckets other than `nfs4Bucket`
  are no-ops.
- `serveruntime.NodeServices.NFS4()` getter so the runtime can register
  the NFS server's invalidator with `distBackend.RegisterCacheInvalidator`
  after `StartNodeServices` returns. Registration is conditional on NFS4
  being enabled (port > 0).

## [0.0.103.0] - 2026-05-08 — remove dead PolicyStore (uncompiled)

### Removed

- `policy.PolicyStore` (the uncompiled bucket-policy cache) and its
  `internal/server/policy_store.go` alias. `policy.CompiledPolicyStore` is
  the production cache (zero-alloc hot path, wired into
  `storage.Operations` via `WithPolicyStore`); `PolicyStore` had no
  production callers and was only kept alive by its own tests and the
  side-by-side benchmark fixture. `CompiledPolicyStore` already provides
  Set / GetRaw / Delete / Allow with full superset semantics.
- `TestPolicyStore_*` (6 tests) and the `BenchmarkOld_*` fixtures that
  compared the dead path to the compiled path. `TestCompiledPolicyStore_*`
  and the remaining `BenchmarkNew_*` continue to cover Allow/Deny rules,
  cache update/delete, and zero-alloc evaluation perf.

## [0.0.102.0] - 2026-05-08 — S3 ListMultipartUploads + ListParts handlers

### Added

- `GET /:bucket?uploads` is wired to `Operations.ListMultipartUploads`. The
  handler renders the S3 `ListMultipartUploadsResult` XML payload with
  `?prefix=` filtering and `?max-uploads=` capping (default 1000).
- `GET /:bucket/:key?uploadId=<id>` is wired to `Operations.ListParts`,
  rendered as `ListPartsResult` XML with `?max-parts=` cap (default 1000).
  Returns `404 NoSuchUpload` when the uploadID does not match an active
  upload, sorted by part number ascending.
- `Backend` interface gains `ListMultipartUploads(ctx, bucket, prefix,
  maxUploads)` and `ListParts(ctx, bucket, key, uploadID, maxParts)`.
  `LocalBackend` scans the BadgerDB `mpu:` prefix for in-progress uploads
  and walks the per-uploadID partDir for parts (re-hashing each part file
  for the ETag — acceptable for first slice since list is not a hot path).
  `SwappableBackend`, `PackedBackend`, and the test fakes get the matching
  pass-throughs; `RecoveryWriteGate` and `pullthrough.Backend` use embedded
  Backend so the read methods auto-pass.

### Notes

- Cluster-mode limitations (first slice, follow-up planned):
  `DistributedBackend.ListMultipartUploads` returns an empty list because
  the FSM-replicated multipart record (`clusterMultipartMeta`) only stores
  ContentType + PlacementGroupID — bucket and key are not yet replicated.
  `DistributedBackend.ListParts` reads only the local node's part directory
  for the upload, so parts uploaded against another node's routed group are
  not visible from this node. Single-node deployments (LocalBackend) return
  the full list in both cases.

## [0.0.101.0] - 2026-05-08 — S3 AbortMultipartUpload handler

### Added

- `DELETE /:bucket/:key?uploadId=<id>` is now wired to
  `Operations.AbortMultipartUpload`. Previously the request fell through to
  `DeleteObject` because the handler had no `?uploadId=` branch, so S3
  clients calling `AbortMultipartUpload` saw the wrong semantics (and
  multipart staging directories survived for the 24h grace window). The
  uploadId branch is checked before `?versionId=` because S3 routes the
  request to AbortMultipartUpload whenever `uploadId` is present, and
  returns `204 No Content` on success or `404 NoSuchUpload` on a missing
  upload (the existing `mapError` already maps `storage.ErrUploadNotFound`
  to that response shape).

## [0.0.100.0] - 2026-05-08 — multipart orphan sweep moves into storage capability plan

### Changed

- Multipart orphan sweep moves from `internal/server/startup_cleanup.go` into a
  storage-decorator capability (`storage.OrphanMultipartSweeper`). The
  filesystem layout `<root>/parts/<uploadID>/` now lives only on
  `LocalBackend` (alongside `partDir`/`partPath`); server startup recovery
  no longer hardcodes the same path. `Operations.SweepOrphanMultiparts(ctx,
  before)` walks the decorator stack via `unwrapOperationBackend` to find a
  reachable sweeper, blocking on `RecoveryWriteGate` so gated runtimes
  refuse mutation even on cleanup paths.
- `RunStartupRecovery` signature gains a `*storage.Operations` parameter.
  `*server.Server` exposes `Operations()` so `serveruntime.Run` can pass
  the facade through. nil ops disables only the multipart sweep — the tmp
  cleanup half is unaffected and tests-only callers can still pass nil.

### Fixed

- Per-removal HealEvent emission and `metrics.HealEventsTotal{outcome=success}`
  no longer double-count multipart-sweep failures. The previous flow emitted
  the success event before `os.RemoveAll`, then emitted the failure event on
  error, so a single failed removal produced both. The new sweep result
  carries `RemovedPaths` and emits exactly one event per confirmed removal.

## [0.0.99.0] - 2026-05-08 — volume health replica fetcher + handler wiring

### Added

- `internal/serveruntime/VolumePlacementAdapter` implements the new
  `admin.VolumePlacementSource` interface over the cluster meta-Raft FSM.
  One pass over `ObjectIndexLatestEntries(__grainfs_volumes, "", 0)` plus
  a single `ShardGroups()` snapshot is fed through a pure
  `aggregateVolumeReplicaLayout` helper that calls `cluster.ClassifyObjectLayout`
  per entry and tallies counts per volume.
- `volume.NameFromBlockKey(key)` extracts the volume name from a block-storage
  key (`__vol/{name}/blk_{N}`). Unit-tested with 8 cases including malformed
  keys and meta-only keys.
- `admin.Deps.VolumePlacement` (optional, nil OK). `serveruntime.Run` wires
  `NewVolumePlacementAdapter(metaRaft)` so cluster runtimes get replica
  signals; standalone runtimes pass nil and the composer falls back to
  incident-only health.

### Changed

- `fetchAndAnnotateHealth` (and `StatVolume`) now ask `VolumePlacement` for
  per-volume replica summaries and pass them to `annotateVolumeHealth`.
  VolumePlacement errors are silent — the composer keeps incident-only output
  rather than stamping a noisy `replica_lookup_failed` while clusters
  reconfigure.

## [0.0.98.0] - 2026-05-08 — IAM Foundation (ServiceAccount + AccessKey + Grant)

### Added

- IAM model with cluster-wide ServiceAccount, AccessKey, and Grant primitives
  replacing the single `--access-key/--secret-key` flag. State lives in
  `internal/iam.Store` behind `atomic.Pointer[iamState]` for lock-free reads;
  writes serialize through the cluster meta-Raft FSM via `MetaCmdType` 21-29.
- Three-tier role model (Read / Write / Admin) with explicit-overrides-wildcard
  semantics. Bucket grants gate every S3 op; bucket policies are evaluated as
  a serial second layer.
- AES-256-GCM secret_key wrapping with AAD bound to the owning sa_id. Plaintext
  secret_key never lands on disk; replay across SAs fails the AAD check.
- Bootstrap shim: when `--access-key/--secret-key` are set on first start, the
  leader proposes a default SA (`sa-default`) + wildcard Admin grant + sticky
  `auth_enabled` bit. Idempotent on completeness check (SA + key + wildcard +
  AuthEnable all present); a partial failure on any node retries the missing
  step on the next bootstrap pass.
- Sticky `auth_enabled` bit. Once any SA is registered the bit stays on
  permanently; deleting all SAs does not regress to anonymous mode.
- Admin HTTP API on the admin Unix socket at `/v1/iam/*`: SA CRUD, AccessKey
  rotate/revoke, Grant put/delete/list with `?sa=`/`?bucket=` filters.
  Wildcard-grant deletion routes through a dedicated FSM command and refuses
  to remove the last grant from `sa-default` (lockout guard enforced at the
  apply layer for race-freeness).
- `grainfs iam {sa,key,grant}` CLI subcommands following the existing
  `grainfs cluster --endpoint <data-dir>/admin.sock` pattern.
- `CreateBucket` auto-issues an explicit Admin grant to the creating SA so
  per-bucket access survives wildcard grant removal.
- Audit logger with pluggable emitter; default backend is zerolog
  `event=iam_audit` with allow/deny + reason fields.
- IAM state persisted in MetaFSM raft snapshots as a length-prefixed trailer
  with magic bytes after the FlatBuffer root. Multi-team clusters survive
  raft log compaction without losing SAs/keys/grants/sticky bit; legacy
  snapshots without the trailer restore unchanged.

### Changed

- S3 SigV4 verifier consults `SecretLookup` after the static credentials
  map, so admin-API-issued SA keys actually authenticate. Pre-fix only the
  bootstrap default SA worked because it was double-injected into static
  creds.
- `?policy` bucket-policy CRUD requests now flow through IAM authz with
  three new S3Action values (GetBucketPolicy / PutBucketPolicy /
  DeleteBucketPolicy). PUT/DELETE require Admin on the bucket; GET requires
  Read or higher. Pre-fix any signed SA could read, modify, or delete any
  bucket's policy.
- Bucket-policy Layer 2 carve-out for the new BucketPolicy actions: a
  deny-all policy must remain removable by an IAM-Admin so IAM is the
  authoritative gate.
- `HandleSACreate` surfaces a `Warning` field in the response when
  `ProposeAuthEnable` fails after the SA is created, instead of silently
  swallowing the error and returning a misleading "success" while the
  sticky bit stays off.

### Performance

- IAM hot-path measured at p50=167ns / p99=542ns for `ResolveSA + CheckAccess`
  (target was p99 < 1ms — 1800× headroom on Apple M3).
- IAM apply-path measured at p50=126µs / p99=529µs for `ApplySACreate` at
  1k-SA scale (target was p99 < 5ms — 9× headroom).
- Performance budgets are enforced as bench-time assertions in
  `internal/iam/bench_test.go` so regressions land as test failures.

### Documentation

- New ADR `docs/adr/0007-iam-foundation.md`.
- README and CONTEXT.md gain IAM sections covering the three auth modes,
  CLI usage, and the sticky `auth_enabled` invariant.

### Out of scope (deferred)

- Multipart-upload cascade on SA delete (Tasks 26-28 + ET4). Lifecycle
  reaper handles abandoned parts; matches standard S3 semantics.
- Typed `internal/iamadmin/` CLI client. The inline `iamRequest` helper
  in `cmd/grainfs/iam.go` covers the single consumer; extract when a
  second consumer (web admin UI, automation) appears.


## [0.0.97.0] - 2026-05-08 — volume health replica/EC layout signals

### Added

- Volume health composer accepts replica/EC actual layout signals via
  `ReplicaLayoutFact` (per-volume aggregate of `LayoutState` counts from
  `internal/cluster/topology_policy.go`). Repair-needed objects raise the
  volume to `critical` with reason `replica_repair_needed`; pending-upgrade
  objects raise to `degraded` with reason `replica_missing`; unknown layout
  raises to `warning` with reason `replica_layout_unknown`. Downgrade-skipped
  and current counts contribute nothing.
- `worseVolumeHealth` rank gains a `degraded` slot between `warning` and
  `critical`. Relative order of pre-existing labels is preserved, so
  incident-only callers see no change.

### Changed

- Existing handler call sites (`fetchAndAnnotateHealth`) still pass `nil`
  for replicas, so production behavior is unchanged in this slice. A
  follow-up adapter will populate `ReplicaLayoutFact` from the object index
  for each volume's block prefix.

## [0.0.96.0] - 2026-05-08 — NFSv4 server bottleneck tuning (single-node)

### Changed

- **NFSv4 server processes COMPOUND ops concurrently per TCP connection.**
  `handleConn` previously serialized read → dispatch → write per-connection.
  Linux NFSv4 clients use a single connection per mount, so multi-threaded
  fio workloads were funneled through one in-flight RPC at a time. Each RPC
  frame now dispatches to a worker goroutine (cap 64 in-flight per
  connection) with a write mutex serializing TCP frame writes; out-of-order
  replies are matched by XID per RFC 7530.
- **NFSv4 server recycles RPC frame buffers** through a `frameBufPool`
  (`sync.Pool` of `[]byte`). The concurrency change above made each read
  allocate a fresh frame; without the pool, fresh
  `resizeFrameBuffer` calls dominated heap traffic (2.37 GB / 2.71 GB total
  in 30 s of streaming) and regressed read throughput. With the pool,
  streaming read jumps from 256 → 348 MiB/s (+37 % over the pre-change
  baseline) on a single-node Colima loopback mount.
- **NFSv4 `StateManager` filehandle maps switch from CoW to RWMutex.** The
  previous design cloned the full `pathToFH`/`fhToPath` maps on every
  insert under `writeMu` and atomically swapped an `atomic.Pointer`. Reads
  were lock-free, but writes were O(N) per insert — catastrophic on
  small-file create storms. A 10,000 × 4 KB workload allocated 6.24 GB
  inside `maps.Copy` alone (93 % of the run's allocations). Replacing
  with plain maps under `RWMutex` brings allocations down to 95.85 MB and
  raises files/sec from 428 → 515 (+20 %). Read concurrency is preserved
  via `RLock` on the hot path; the lock cost is in the noise vs NFS RPC
  RTT.
- **`DistributedBackend.WriteAt` no longer rehashes the full file on
  partial writes.** The previous `writeAtETag` re-read the whole object
  into MD5 on every offset > 0 pwrite, making NFS append O(n²) — a 1 GiB
  4 KB sequential append never completed in fio's runtime cap. Partial
  writes now publish an empty ETag and rely on the scrubber's
  `ReplicationVerifier` to skip empty-ETag blocks (existing
  `TestReplicationVerifier_LegacyETagSkipped` regression-guards the
  contract). EC parity still detects shard-level corruption on read.
  Append regression-guard workload now finishes 1 GiB in 47 s.
- **`DistributedBackend.Truncate` materializes parent dir + file** via
  `ensureInternalObjectDir` + `OpenFile(O_CREATE|O_RDWR)` + `Truncate(size)`.
  Previously called `os.Truncate` directly, which failed for paths whose
  parent directory had not been materialized by an earlier PUT.
- **NFSv4 `OPEN(CREATE)` uses `Truncate(0)` for new file materialization**
  instead of a zero-byte `PutObject`, when the backend implements
  `storage.Truncatable`. Skips the object-index Propose round-trip on
  every fresh OPEN — measurable on small-file create storms.
- **`ClusterCoordinator.routeObjectLatest` short-circuits the
  object-index lookup for internal buckets.** Internal buckets (NFS4,
  VFS, NBD) are pinned to a single placement group via
  `Router.AssignBucket`; any index entry would always agree with the
  bucket route. Skipping the lookup avoids one
  `indexWriter.Propose` round-trip per first-time NFS pwrite.
  `TestClusterCoordinator_InternalReadAtFallsBackWhenObjectIndexMissing`
  locks the invariant.
- **`ClusterCoordinator.PreferWriteAt` exposes the routed single-leader
  fast path** so consumers can detect when WriteAt avoids the
  object-index path without reaching into backend internals.

### Added

- **NFS profile bench script gains a `WORKLOAD` selector**
  (`benchmarks/bench_nfs_profile.sh`). Three workloads:
  - `streaming` (default — preserves prior 3-fio behavior).
  - `metadata` — 10,000 small-file create storm via parallel `dd`
    (4 KB files, parallel=8). Targets NFSv4 OPEN+WRITE+CLOSE per-op
    latency and per-create allocation behavior.
  - `append` — single-thread 4 KB writes to a 1 GiB target. fio
    bandwidth log is post-processed with awk to compute first-window vs
    last-window throughput ratio; ratio < 0.8 fires the `O(n²)`
    regression guard.

  Encryption is mandatory (`NO_ENCRYPTION=1` is rejected) — mirrors
  `bench_three_node_s3_matrix.sh`.

### Notes

- **Streaming write throughput** moves from 24.8 → 31.4 MiB/s (+27 %)
  but does not reach the plan's 200 MiB/s primary target. With server
  CPU at 17 % and 80 %+ idle, the remaining ceiling is the loopback NFS
  RPC RTT — ~4 ms per RPC × 4 fio threads × \\~100 ops/sec/thread ≈ the
  observed throughput. Further write headroom requires removing the
  network-layer floor, not the server. Plan terminated on the
  "external hotspot" condition.
- **Cluster phase deferred to a follow-up PR.** This PR is the
  single-node measurement substrate; cluster-phase bottleneck shape
  differs (meta-raft propose dominates rather than RPC RTT). NBD WIP
  is also held back for a separate per-protocol PR per the project's
  one-protocol-per-PR convention.
- **Pre-existing data race** in
  `DistributedBackend.SetSnapshotManager` vs `RunApplyLoop` exists on
  master and is not introduced here. `go test -race ./internal/cluster`
  surfaces it on both branches; non-race `go test` passes clean.


## [0.0.95.0] - 2026-05-08 — volume health composer extraction

### Changed

- Volume health synthesis (`Healthy`/`Degraded`/`Failed` labels and reasons
  on admin volume responses) moves out of `internal/server/admin/handlers_volume.go`
  into a pure composer in `internal/server/admin/health.go`. Handlers now
  fetch incident state and delegate composition; the composer performs no
  I/O and is directly unit-tested with literal inputs, mirroring the
  Cluster Peer Liveness Snapshot pattern in `CONTEXT.md`.
- `StatVolume` reuses the shared `incidentMatchesVolume` matcher when
  collecting `RecentIncidents`, so volume-scoped incident filtering is
  defined in one place instead of inlined twice.

### Added

- Table-driven unit tests for volume health composition cover incident
  scope matching, severity-to-label rules, resolved-state suppression,
  label rank merging, and reason deduplication. Multi-volume independence
  and the matcher itself are also tested.
- A `ReplicaLayoutFact` placeholder input on the composer reserves a
  per-volume seam for replica/EC actual layout signals (ADR 0007). The
  first slice is incident-only; a follow-up adapter will populate it.

## [0.0.94.0] - 2026-05-08 — topology durability hardening

### Added

- Object writes fall back to bucket and router-resolved data groups when
  EC placement selection finds no candidate, so internal-bucket and
  bootstrap clusters keep accepting writes without an EC-capable group.
- ObjectIndex entries record only the actual k+m shard targets, and
  `ClassifyObjectLayout` flags entries whose NodeIDs length disagrees
  with the recorded EC profile so the scrubber can re-place them.

### Changed

- Forwarded PUT receivers now run with the placement group entry in
  context, so the receiving voter applies the same topology-derived EC
  profile as the origin.
- New writes fail with S3 503 while a configured placement target is
  unavailable instead of silently downshifting to a narrower EC profile;
  e2e expectations updated to match this contract.

### Fixed

- ReadIndex retries the remaining voters on transport errors instead of
  short-circuiting on the first failure, so killed-peer scenarios
  converge instead of bubbling out before the leader cycle.
- Remove-peer resolves canonical node IDs back to raft addresses before
  invoking joint consensus, so dead-follower removal works whether the
  meta-Raft engine remembers the peer by node ID or address.

## [0.0.93.0] - 2026-05-08 — encrypted EC bottleneck tuning

### Added

- Added a 3-node encrypted S3 benchmark matrix for one-at-a-time bottleneck
  sweeps across object size, concurrency, ingress, and PUT/read mix.
- Extended the S3 profile benchmark with `pure-put`, `put-heavy`, and `mixed`
  workload mixes, while keeping encryption mandatory for this benchmark path.

### Changed

- Small encrypted EC PUTs now split shards in memory for objects up to 16 MiB,
  avoiding the extra EC shard spool while preserving bounded memory use.
- Small redundant EC GETs now use the buffered k-of-n reader path, so one dead
  data-shard peer does not hold the read before parity can be used.
- e2e artifacts are cleaned by default, with `GRAINFS_E2E_KEEP_ARTIFACTS=1` as
  the opt-in path for retaining failing logs and data directories.

### Fixed

- The 3-node benchmark matrix removes its temporary data directory by default,
  with `KEEP_BENCH_ARTIFACTS=1` available for profiling/debug sessions.
- The 5-node EC e2e recovery check now probes surviving endpoints after a node
  kill instead of pinning reads to a client that may still forward ReadIndex to
  the failed node.

## [0.0.92.0] - 2026-05-08 — zero-config cluster topology

### Changed

- `grainfs serve` now derives object EC profiles from cluster size instead of
  requiring operators to pick shard counts. The automatic table is: 1 node
  `1+0`, 2 nodes `1+1`, 3 nodes `2+1`, 4 nodes `2+2`, 5 nodes `3+2`,
  6 nodes `4+2`, 7 nodes `5+2`, and 8+ nodes `6+2`.
- Placement group seeding is now automatic from cluster size using
  `max(8, clusterSize * 4)`, so bootstrap topology no longer depends on a
  manual group-count flag.
- Object index metadata now records the EC profile from the selected placement
  group voter count, so reads and repairs use the profile that was actually
  written.
- Cluster e2e tests and benchmark scripts now express EC and placement behavior
  through node count instead of manual topology flags.

### Removed

- Removed public `grainfs serve --ec-data`, `--ec-parity`, and `--seed-groups`
  options. Use node count to select the zero-config EC and placement-group
  policies.

### Fixed

- Stabilized the 3-node EC e2e startup path by using the shared static-peers
  harness and probing writable endpoints before asserting the `2+1` profile and
  single-node failure recovery.

## [0.0.91.0] - 2026-05-08 — Cluster Day-2 Operations CLI (Phase 1)

### Added

- **`grainfs cluster transfer-leader`** — Trigger immediate leadership transfer
  to another voter (raft picks the best peer by matchIndex). Use before
  stopping the leader for graceful node maintenance. `--wait` polls status
  until the new leader is confirmed.
- **`grainfs cluster drain <id>`** — Composite: transfer-leader (if target is
  the leader) + remove-peer. Progressive feedback on partial failures (A2-a):
  transfer fail leaves the voter set unchanged; transfer-ok-but-remove-fail
  prints next-step guidance. Self-drain is supported (admin socket may close
  after removal — explicit `--yes` required for leader drain).
- **`grainfs cluster health`** — Aggregated health view: quorum verdict,
  per-peer state, and a server-side derived `issues[]` list (single source
  of truth — same rules the dashboard renders).
- **`grainfs cluster placement [bucket]`** — Human-readable shard groups +
  bucket assignments. Reuses existing `cluster status` data; no new server
  route. Optional bucket arg filters to a single row.
- **`grainfs cluster balancer status`** — Per-node disk/load stats from the
  balancer. Mirror of the existing dashboard `/api/cluster/balancer/status`
  on the admin UDS, exposed under `cluster balancer` subgroup.
- `internal/server/cluster_health.go` — `Health/QuorumInfo/PeerHealthRow`
  types + `deriveHealth/deriveIssues` (5 rules, table-driven).
- `internal/clusteradmin.{TransferLeader,Health,BalancerStatus}` methods +
  `TransferLeaderError` typed error following `RemovePeerError` pattern.

### Notes

- **Scope: metaRaft only.** Per-data-group leader transfer/drain (multi-raft
  scope), target-aware transfer, persistent drain state, and balancer trigger
  CLI are deferred to a follow-up spec — see `TODOS.md` "Cluster Day-2
  Operations".
- No breaking changes. v0.0.89.0's command surface is preserved.

## [0.0.90.0] - 2026-05-08 — distributed S3 write path benchmarked and tuned

### Added

- Added single-node and multi-node S3 mixed workload benchmark scripts with
  round-robin ingress, per-node request/RSS summaries, pprof capture, and
  fixed-stage PUT latency metrics for EC shard writes.
- Added PUT-stage Prometheus histograms for distributed object writes,
  encrypted shard writes, shard-stream server work, and QUIC body streaming.

### Changed

- Improved distributed EC PUT throughput by streaming remote shard writes for
  benchmark-sized shards, increasing QUIC receive windows, and pooling copy
  buffers in spool, encrypted shard, and QUIC stream paths.
- Kept benchmark encryption mandatory: the S3 cluster benchmark rejects
  `NO_ENCRYPTION=1`, and node logs confirm at-rest encryption plus active EC.
- Reduced EC spool pressure by sizing Reed-Solomon stream blocks to the object
  shard size instead of retaining the larger library default.

### Fixed

- Fixed forwarded data-group writes when Raft leader hints are node IDs instead
  of dialable QUIC addresses.
- Fixed forwarded read streams sharing write-forward backpressure slots, which
  could make reads block behind concurrent streamed PUT forwards.
- Fixed follower object-index proposals by forwarding meta-Raft object index
  mutations through the meta leader.
- Fixed pprof profile collection for benchmark runs by creating the output
  directory before fetching heap, allocs, goroutine, mutex, and block profiles.
- Fixed an e2e vlog watcher setup that wrote EC objects without seeding a normal
  EC-capable placement group.

### Removed

- Removed the built-in S3 request rate limiter and its `serve --rate-limit-*`
  flags so storage benchmarks and production request handling no longer pass
  through that middleware.

## [0.0.89.0] - 2026-05-07 — cluster CLI uses admin UDS, output unified

### BREAKING CHANGES

- **`grainfs cluster {status,peers,events,remove-peer}` now requires
  `--endpoint <data-dir>/admin.sock`** (UDS). Previous default
  `http://127.0.0.1:9000` removed. HTTP `/api/cluster/*` and `/api/eventlog`
  remain available for the dashboard (web UI). Migration:
  ```
  Old: grainfs cluster status --endpoint http://127.0.0.1:9000
  New: grainfs cluster --endpoint <data-dir>/admin.sock status
  ```
- **`grainfs cluster join` moved to `grainfs join`**. Aligns with bootstrap
  convention (`serve`, `migrate`, `doctor`, `recover` all live at root).
  Migration:
  ```
  Old: grainfs cluster join <peer-addr> --data-dir <data> --raft-addr <addr>
  New: grainfs join <peer-addr> --data-dir <data> --raft-addr <addr>
  ```
- **`grainfs cluster status` default format changed from `json` to `text`.**
  Pipe consumers must add `--format json` explicitly. Format flag is now
  unified as a persistent flag on `clusterCmd`, `volumeCmd`, and `dashboardCmd`.
- **Output flags unified on `--format text|json`.** Dropped `--json`,
  `--raw` (alias of `--bytes`) on `volume`, `dashboard`, `doctor`. `--bytes`
  is retained on `volume` for raw byte rendering inside text output.
  Migration:
  ```
  Old: grainfs volume list --json
  New: grainfs volume list --format json
  ```

### Removed

- **`grainfs cluster plan-show`** and **`grainfs cluster rebalance`** —
  stub commands introduced in PR-D, deferred to multi-raft work that moved
  to other milestones. `loadFSMFromStore` was a no-op. Will be reintroduced
  when real local FSM read lands.

### Added

- `RegisterClusterAdminUDS` mounts `/v1/cluster/{status,remove-peer,eventlog}`
  on the admin Unix socket. Single handler source: same `*server.Server`
  methods serve both HTTP `/api/cluster/*` (dashboard) and UDS
  `/v1/cluster/*` (CLI).
- `clusteradmin.NewClient` now dispatches on prefix: bare path / `unix:` →
  UDS dialer, `http(s)://` → direct HTTP. Matches `volumeadmin` pattern.
- `clusteradmin.StatusRaw` returns the raw response body for forward-compat
  JSON output.
- `clusteradmin` gains `RotateKey{Status,Begin,Abort}` methods. The
  `rotate.sock` listener was migrated from line-delimited JSON to Hertz
  HTTP (`/v1/rotate-key/{status,begin,abort}`); socket file mode stays
  `0600` so PSK material remains owner-only and out of admin-group reach.
- `--endpoint` shows actionable errors on dial failure
  (ENOENT / ECONNREFUSED / EACCES) and on `http://` input ("admin endpoint
  must be a UDS socket path").
- ADR 0006: CLI uses admin UDS, dashboard uses HTTP.

### Changed

- Translated remaining Korean Example blocks in `cluster remove-peer`,
  `volume`, and `dashboard` cmd help to English for consistency.

## [0.0.88.0] - 2026-05-07 — volume block I/O policy private module

### Changed

- Volume byte-range reads, writes, deferred writes, and discards now delegate
  block-level policy to a private `internal/volume` module. `Manager` keeps the
  public lock, metadata, and live-map orchestration, while block merge rules,
  cache behavior, quota checks, dedup/live-map mutation, and allocation deltas
  live behind one focused implementation.
- `Volume Block I/O` is now recorded in `CONTEXT.md` and ADR 0005 so future
  architecture work has a stable name for this seam.

### Tests

- Added private block I/O tests for cache-hit read metering, direct full-block
  write allocation results, and live-map discard side effects.
- Existing volume, dedup, quota, discard, and block-cache behavior tests continue
  to cover the public `Manager` semantics.

## [0.0.87.0] - 2026-05-07 — hot bucket object placement

### Added

- Hot buckets can now spread new object writes across normal EC-capable data
  groups using object-level placement instead of pinning all object traffic to
  the bucket assignment group.
- Meta-Raft now stores a global object index with placement group, version, EC
  profile, node list, and delete-marker metadata so object reads, deletes,
  version reads, multipart completion, copy-through-write, Range `ReadAt`,
  `ListObjects`, `ListObjectVersions`, and `WalkObjects` can route from the
  object index.
- Cluster status and the topology GET benchmark now expose object placement
  distribution so operators can see whether a hot bucket is spreading across
  placement groups.
- ADR 0004 records the object-level placement decision and the dual-write
  reconcile model.

### Changed

- Cluster object writes now require the EC pipeline when running through
  placement groups, and explicit EC profiles fail fast when the selected group
  cannot fit `k+m` shards.
- Data-group Raft elections now use a group-specific priority key so seeded
  placement groups prefer different initial leaders instead of letting startup
  timing concentrate group leadership on one node.
- Zero-config EC now scales from active cluster size, including the single-node
  `1+0` profile and progressively wider profiles for larger clusters.
- VFS/internal bucket fixed-version special-casing is removed from the cluster
  object write path as part of the EC-only placement model.

### Fixed

- LIST-style reads now enumerate from the object index, so objects written into
  different placement groups within the same bucket are visible together.
- Hard `DeleteObjectVersion` now removes the corresponding meta object-index
  row and recomputes the latest pointer.
- Object-index orphan detection scans group-local metadata directly, so it can
  still find data-group objects that are missing from the global index.

## [0.0.86.0] - 2026-05-07 — cluster remove-peer/peer-liveness 안전 강화 (#215에서 분리)

### Fixed

- **`cluster remove-peer`는 peer liveness snapshot이 없으면 거절.** 기존
  legacy `LivePeers()` quorum math fallback은 `configured`/`live`/unresolved
  legacy identity 상태를 구분하지 못해 안전하지 않았음. snapshot interface
  부재 시 membership mutation은 무조건 차단. CLI preflight도 같은 snapshot
  모델로 일원화돼 `configured` peer가 alive로 카운트되지 않음.
- **`--force`는 quorum break override로만 동작.** target이 cluster
  membership view에 없거나 unresolved legacy identity인 경우는 force가
  bypass하지 않음. 운영자 실수로 잘못된 target에 force가 통하던 경로 차단.
- **`cluster peers` render에서 configured peer는 `unknown_configured`로
  표시.** 과거에는 healthy처럼 보였음. 운영자 인지 정확도 개선.
- **self identity 부재 시 peer liveness snapshot은 nil 반환.** meta-Raft가
  자기 노드 identity를 아직 모르면 snapshot 자체를 만들지 않아 부정확한
  membership 결정을 사전에 차단. meta-Raft liveness freshness window는
  `cluster` 패키지에서 명시적으로 명명되고 `serveruntime` Adapter에서 사용.

### 출처

원래 #215(v0.0.77.0 refactor)의 일부로 묶여 있던 두 커밋
(`0f6e4a9`, `ccdf411`)을 cherry-pick. #215의 runtime assembly refactor
부분은 이번 세션의 #224/#225/#226로 인해 base가 크게 어긋나 별건으로 미룸.

## [0.0.85.0] - 2026-05-07 — `dashboard` warning 메시지에서 grainfs.toml orphan 안내 제거

### Fixed

- `grainfs dashboard` 가 `--public-url` 미설정 시 출력하던 warning에 "또는
  `grainfs.toml`의 `public_url`에 설정"이라는 안내가 있었으나, repo에 TOML
  리더가 없어 동작하지 않는 silent-failure trap이었음. 안내 1줄 삭제 +
  앞 줄의 `지정하거나,` → `지정.`로 정리. v0.0.82.0/v0.0.84.0의 "explicit
  flag-only" 결정과 정합성 회복.

## [0.0.84.0] - 2026-05-07 — `cluster rotate-key`도 `--endpoint`로 통합 + env fallback 제거

### Changed (BREAKING)

- **`cluster rotate-key`의 `--data` 제거, `--endpoint` 도입.** 기존
  `grainfs cluster rotate-key {begin,status,abort} --data ./tmp` 형태는 더 이상
  동작하지 않으며 `--endpoint ./tmp/rotate.sock` 으로 socket 경로를 직접
  넘겨야 합니다. CLI가 `<data>/rotate.sock` 내부 구조에 의존하던 layering leak
  제거.
- **endpoint env fallback 제거.** admin/rotate 양쪽 모두 `--endpoint` flag만
  지원하고 `$GRAINFS_ENDPOINT` fallback은 삭제됐습니다. rotate-key는 일회성/사고
  대응용이라 env 편의 가치가 작고, 두 socket이 다른 파일이므로 단일 env가
  의미 없음. admin은 일관성 차원으로 같이 정리. 명령줄에 socket path가
  항상 보이게 되어 사고 시 추적성도 개선.

### Changed

- `serve` 시작 로그에서 rotate socket 출력 shape을 admin과 통일.
  `rotation socket listening` 한 줄 → `rotate endpoint path=... hint="--endpoint <path>"`
  형태로 변경되어 두 endpoint를 동일한 grep 패턴으로 추적 가능.
- `volumeadmin.ResolveEndpoint`는 flag → fail-fast로 단순화. 기존 env 분기 삭제,
  hint 메시지에서 `GRAINFS_ENDPOINT` 언급 제거.

### Migration

```sh
# Before
grainfs cluster rotate-key status --data ./tmp

# After
grainfs cluster rotate-key status --endpoint ./tmp/rotate.sock
```

admin CLI도 동일하게 `export GRAINFS_ENDPOINT=...` 형태가 더 이상 통하지
않으므로 매번 `--endpoint <path>`로 명시. `grainfs serve`가 시작 시 두 endpoint
경로를 모두 출력하므로 그대로 복사하면 됩니다.

## [0.0.82.0] - 2026-05-07 — admin CLI: `--data` 제거하고 `--endpoint`로 통합

### Changed (BREAKING)

- **admin CLI에서 `--data` 플래그 제거.** `grainfs volume *`, `grainfs dashboard`,
  `grainfs scrub` 은 이제 `--endpoint <admin-sock-path>` 만 받습니다. admin CLI가
  data 디렉토리의 내부 구조(`<data>/admin.sock`)를 가정하던 결합을 끊어, 미래에
  소켓 위치·이름이 바뀌어도 CLI가 깨지지 않습니다.
- **endpoint 해석 우선순위 단순화:** `--endpoint` flag → `$GRAINFS_ENDPOINT` env →
  fail-fast. 기존 `$GRAINFS_DATA` env와 `grainfs.toml`의 `data_dir` 자동탐색은
  모두 삭제됐습니다(실제로 grainfs.toml을 읽는 production 코드 경로는 없었음).
- `volumeadmin.NewClient(endpoint, dataFlag)` → `NewClient(endpoint)`.
  `BaseOptions.DataDir` 필드 삭제.

### Added

- `volumeadmin.ResolveEndpoint(flagVal)` 단일 진입점 + 단위 테스트 4건.
- `cmd/grainfs/admin_helpers.go::registerAdminEndpointFlag` 공유 helper로
  `volume`/`dashboard`/`scrub` 세 그룹이 같은 방식으로 `--endpoint`를 등록.
  `scrub` 은 기존에 플래그 등록이 누락돼 있던 것을 함께 수정.
- `serve` 시작 시 admin endpoint 안내 로그
  (`admin endpoint path=... hint="export GRAINFS_ENDPOINT=..."`). 경로에 공백이
  있어도 셸에 그대로 복사 가능하도록 `%q`로 인용.

### Migration

기존 `grainfs volume list --data ./tmp` 형태의 사용자는 다음 중 하나로 전환:

```sh
# 매번 명시
grainfs volume list --endpoint ./tmp/admin.sock

# 또는 한 번 export
export GRAINFS_ENDPOINT=./tmp/admin.sock
grainfs volume list
```

`grainfs serve`가 시작 시 정확한 경로를 stderr에 출력하므로 그대로 복사하면 됩니다.

## [0.0.81.0] - 2026-05-07 — data-group forwarding dispatch policy

### Changed

- Bucket-scoped forwarding operations now have one internal metadata table for
  transport shape: frame-only, body-streamed, or read-streamed. Node-scoped
  scrub session status stays outside bucket forwarding.
- Forward receivers reject body/read stream kind mismatches before group lookup,
  while the frame handler keeps the existing legacy-compatible path.

### Tests

- Added metadata coverage for every bucket forwarding op and explicit exclusion
  of scrub session status.
- Added receiver stream gating tests for invalid body/read dispatch and valid
  stream paths reaching group lookup.

## [0.0.80.0] - 2026-05-07 — admin CLI: 30s `http.Client.Timeout` 제거 + `--timeout` 플래그

### Fixed

- `volumeadmin.NewClient`/`NewClientForURL`의 `http.Client.Timeout: 30s` cap이
  `BaseOptions.Timeout > 30s` 설정을 silent하게 truncate하던 버그 해소.
  client에는 timeout cap이 없고 ctx로만 통제됩니다.

### Added

- 모든 admin CLI 명령(`grainfs volume *`, `grainfs dashboard`, `grainfs scrub`)에
  `--timeout` flag 추가. unset이면 30s default(이전 hardcoded 값 보존),
  `--timeout 5m`로 연장, `--timeout 0`으로 uncapped 가능. cobra `Duration` 타입.
- `cmd/grainfs/admin_helpers.go`: `DefaultAdminTimeout` const,
  `registerAdminTimeoutFlag`, `adminTimeoutFromCmd`, `applyAdminTimeout` 헬퍼 추가.
  비-volume admin runner(dashboard, bucket scrub)도 동일 timeout 정책 적용.

### Changed

- **Timeout semantic shift (per-request → per-command).** 이전에는
  `http.Client.Timeout: 30s`가 매 요청마다 적용돼 `volume scrub`/`scrub` follow
  loop는 무한 실행됐습니다. 새 default(30s)는 ctx 기반이라 명령 전체에 적용됩니다.
  분~시간 단위 follow가 필요하면 `--timeout 5m` 또는 `--timeout 0`(uncapped)을
  명시하세요. `--detach`로 trigger만 하고 빠지는 사용 패턴은 영향 없음.

### Tests

- `TestNewClientForURL_NoHTTPClientTimeoutCap` /
  `TestNewClient_NoHTTPClientTimeoutCap_HTTPEndpoint` —
  client에 Timeout cap이 없음을 deterministic 검증.

## [0.0.79.0] - 2026-05-07 — volumeadmin: gap 단위 테스트 4건 추가

### Tests

- `withTimeout`이 `BaseOptions.Timeout > 0`이면 ctx에 deadline을 붙이고,
  `Timeout == 0`이면 parent ctx를 그대로 통과시키는지 deterministic 검증.
- `Client.Do`가 2xx 응답에 깨진 JSON body를 받으면 `decode response: ...`
  에러를 반환하는지 검증.
- `FollowScrubSession`이 비-ctx 네트워크 에러(서버 500)를 graceful-stop으로
  swallow하지 않고 그대로 bubble up하는지 검증.
- `AutoDiscoverSocket`이 `dataFlag` 인자로 받은 디렉터리에서 실제 unix
  socket을 찾아 `unix:<path>`로 반환하는 happy path 검증 (신규
  `discover_test.go`).
- TODO에 적힌 "WriteAt content base64 round-trip"은 기존
  `TestClient_WriteAt_ReadAt`가 이미 round-trip을 검증하고, 와이어 base64
  인코딩은 stdlib `encoding/json`의 `[]byte` 동작이라 우리 코드 회귀
  가능성이 0이라 제외했습니다.

## [0.0.78.0] - 2026-05-07 — volumeadmin: ParseSize Kubernetes 단위 컨벤션

### Breaking

- `grainfs volume create/resize --size`가 bare `K`/`M`/`G`/`T`/`P` 단독 표기를
  ambiguous로 거부합니다. binary는 `Ki`/`Mi`/`Gi`/`Ti`/`Pi` (또는 `KiB` 등 = 1024^n),
  decimal은 `KB`/`MB`/`GB`/`TB`/`PB` (= 1000^n)로 명시해야 합니다. 기존
  `--size 1G` 자동화는 `--size 1Gi` 또는 `--size 1GB`로 갱신이 필요합니다.

### Fixed

- `--size 1KB`가 silent fail (`ParseInt("1K") error`) 하던 버그 해소. SI decimal
  표기 (KB/MB/GB/TB/PB)가 정식 지원됩니다.

### Changed

- `volume create/resize` 명령 도움말과 예시가 새 단위 컨벤션을 반영합니다
  (`1Gi` binary, `1GB` decimal).

## [0.0.77.0] - 2026-05-07 — Cluster runtime relocated to serveruntime.Run (cmd-thin PR2b)

### Changed

- The 1366-line `runCluster` body is moved verbatim from `cmd/grainfs/serve.go`
  into `internal/serveruntime/run.go` as `Run(ctx, Config)`. PR2a had already
  hoisted every `cmd.Flags().Get*` into `clusterConfig`, so the body itself
  was cobra-free and the move is mechanical.
- `clusterConfig` becomes the exported `serveruntime.Config`; a new
  `Version string` field replaces the body's prior dependency on
  `cmd/grainfs`'s package-level `version` var.
- `logStartupConfigSnapshotFromMap` + `diffSnapshots` move to
  `internal/serveruntime/config.go` as `LogStartupConfigSnapshot` (exported)
  + unexported `diffSnapshots`.
- `cmd/grainfs/serve.go` shrinks 1636 → 238 lines. `runServe` now ends with
  `return serveruntime.Run(ctx, cfg)`. `buildClusterConfig` and
  `collectFlagsSnapshot` stay in cmd/grainfs because they read cobra.
- `serve_cluster_key_test.go` updated to call `serveruntime.Run` — the
  cluster-key guard moved with the body, so the test still validates the
  same code path.

### Tests

- cmd/grainfs + serveruntime unit tests pass.
- `make test-e2e` shows zero new failures vs master. `TestE2E_ClusterRemovePeer_DeadFollower`
  was failing on master(20b7c72) before this PR with the same symptom and
  remains failing — pre-existing, unrelated.

## [0.0.76.0] - 2026-05-07

### Added

- Topology GET profiling now reports the assigned shard-group voter count and
  can fail fast when a benchmark expected a full-width group but the bucket
  hashed to a smaller group, making load-distribution measurements explicit.

### Changed

- Encrypted shard range reads now decrypt only the requested encrypted chunk
  instead of discarding plaintext from earlier chunks, improving random and
  shard-local Range GET behavior.
- S3 Range GET streaming now reuses its 1 MiB backend `ReadAt` buffer, reducing
  allocation pressure on large Range GET workloads.
- Remote shard range single-frame replies are capped at 64 KiB so larger ranges
  stay on the streaming path instead of building oversized in-memory replies.

### Fixed

- Documented the measured hot-bucket placement issue where bucket-level routing
  can concentrate 6-node traffic onto a 3-voter shard group, preserving the
  next load-distribution fix target.

## [0.0.75.0] - 2026-05-07

### Added

- Topology GET benchmarks can now measure full-object and byte-range GETs
  across multiple range sizes, range modes, VU counts, and explicit target
  nodes, making shard-local and remote-heavy EC read paths measurable.
- Added regression coverage for S3 Range GET `ReadAt` usage, EC user-bucket
  partial reads, group-forward `ReadAt`, remote shard range reads, and QUIC
  RPC stream-credit reuse.

### Changed

- S3 Range GET now prefers backend `ReadAt` before opening full object bodies,
  so EC user buckets read only the data shard segments that overlap the
  requested range.
- EC range reads now use direct local shard `ReadAt`, bounded remote shard
  range RPCs, and bounded group-forward `ReadAt` responses for small ranges,
  avoiding full object reconstruction on common random-read workloads.
- QUIC request/response calls now half-close the request write side after
  sending the frame, so repeated range reads release bidirectional stream
  credit instead of stalling at the connection stream limit.

### Fixed

- Fixed the 6-node VUS=8 Range GET stall where the workload stopped around
  1356 completed operations after exhausting QUIC stream credit. The same
  benchmark now completes with 5836 successful operations, zero failures, and
  zero interrupted iterations.

## [0.0.74.0] - 2026-05-07

### Added

- `cluster status` / `cluster peers` can now mark remote metaRaft voters as
  `live` when the leader has fresh successful AppendEntries evidence for that
  peer, giving remove-peer preflight a real positive liveness source.
- Added regression coverage for heartbeat and entries-bearing AppendEntries
  success evidence, leader-only evidence exposure, leader epoch reset behavior,
  freshness filtering, and raft-address to node-ID evidence normalization.

### Changed

- Remove-peer membership safety now treats fresh leader-side metaRaft
  replication evidence as the positive liveness signal while keeping stale,
  missing, and follower-side evidence conservative as `configured`.
- Operator docs now distinguish positive metaRaft liveness evidence from the
  still-deferred negative dead-peer detection policy.

### Fixed

- New leader epochs now clear prior-term replication evidence so stale
  successful AppendEntries timestamps cannot briefly mark remote voters live
  after a leadership change.

## [0.0.73.0] - 2026-05-07

### Changed

- `cmd/grainfs/serve.go` now hoists every cobra flag read into a flat
  `clusterConfig` struct via `buildClusterConfig` before calling `runCluster`.
  The body itself reads only `cfg.X`, no `cmd.Flags().Get*`. This keeps the
  cobra surface exclusively in `runServe` and prepares the body for relocation
  to `internal/serveruntime.Run` in a follow-up PR.
- Resource-guard wiring (FD/goroutine/vlog watchers) is now triggered inline
  with pre-built `resourceguard.{FD,Goroutine,Vlog}Options`; the cobra-coupled
  `startResourceGuards` helper has been removed.
- Startup config snapshot is built once via `cmd.Flags().VisitAll` (with the
  same secret redaction set) and rendered through a cobra-free
  `logStartupConfigSnapshotFromMap`; `serve_observability.go` is folded into
  `serve_config.go`.

## [0.0.72.0] - 2026-05-06

### Added

- `cluster remove-peer` now evaluates membership safety from the peer liveness
  snapshot when that Interface is available.
- Added focused remove-peer preflight coverage for configured peers, live peers,
  explicit down states, unresolved legacy voters, missing targets, force
  overrides, and snapshot rows outside the current voter set.

### Changed

- Remove-peer preflight now treats `configured` remote voters as unknown rather
  than alive, while still counting `self` and explicitly live resolved voters.
- The server keeps the legacy `LivePeers()` preflight only as a compatibility
  fallback when a cluster Adapter has no peer snapshot.

### Fixed

- Remove-peer preflight no longer lets unrelated live snapshot rows inflate the
  post-removal quorum calculation.

## [0.0.71.0] - 2026-05-06

### Added

- Added a real multi-node topology GET benchmark that starts GrainFS servers,
  preloads large S3 objects, records bucket-to-shard-group topology, and can
  collect pprof profiles from every node.
- Added a GET-only k6 workload and matrix runner for comparing 1, 2, 3, and 6
  node behavior under the same large-object read workload.

### Changed

- EC GET now streams remote shard bodies over QUIC instead of loading complete
  remote shards into memory before reconstruction.
- Oversized EC GETs now bypass the shard cache when the per-shard payload cannot
  fit in the configured cache budget, preserving streaming behavior for large
  objects.
- EC read reconstruction now avoids data-shard prefetch on nodes that do not own
  a local data shard, while retaining the faster local prefetch path where it
  helps.
- Encrypted shard reads now reuse chunk decode buffers, reducing repeated
  allocation pressure during large EC GETs.

### Fixed

- EC GET streaming now applies to small shard groups as well as full-width EC
  groups, improving the benchmarked 3-voter topology path.
- EC missing-data-shard reconstruction now stays windowed instead of reading
  all available shard bodies into memory during parity recovery.

### Tests

- Added EC streaming, remote shard stream, encrypted shard stream, shard cache
  sizing, topology status, and allocation-bound regression coverage for the new
  GET paths.

## [0.0.70.0] — 2026-05-06 — Serveruntime adapters + cobra-coupled helpers (cmd-thin PR1b)

### Changed

- 7 어댑터 (peerHealth, scrubProposer, scrubAggregator, vlogBreakdown,
  balancerInfo, raftClusterInfo, raftMembership) + replicaRepairerFunc 를
  `internal/serveruntime/adapters.go` 로 이주. 공개 ctor 노출.
- cobra-coupled 헬퍼 4개 — `setupClusterReceipt`, `buildVolumeManager`,
  `startBalancer`, `startNodeServices` — Options struct 어댑터 패턴으로
  `internal/serveruntime/{receipt,volume_manager,balancer,node_services}.go`
  로 이주. cmd 측은 cobra 플래그를 읽어 Options 빌더 → serveruntime 호출.
- `HealReceiptWiring` 의 store/keyStore 를 공개 메서드 (`Store()`,
  `KeyStore()`) 로 노출.
- `serve.go` 2064 → 1669 줄 (-395). cmd-thin PR2 (runCluster 본체) 의
  의존 그래프가 어댑터·옵션 전무 상태로 단축됨.

### Tests

- 기존 cmd 단위 테스트를 패키지 호출 + Options 빌드로 갱신.
- `receipt_wiring_test.go` → `serveruntime/receipt_test.go` 으로 이동
  (private receiptDBOptions / openReceiptDB 접근).

## [0.0.69.0] - 2026-05-06

### Added

- Cluster status responses now include `peer_snapshot`, an operator-facing
  view that separates peer identity state from liveness state.
- Cluster peer liveness policy now lives in `internal/cluster` as a pure
  snapshot composer with predicates for display-down and membership-mutation
  decisions.

### Changed

- `/api/cluster/status` now derives `peers`, `peer_addrs`, `peer_states`, and
  `down_nodes` from the peer snapshot when the cluster adapter exposes it.
- MetaRaft cluster status now includes the local node as `self`, resolved
  voters as node IDs with raft addresses, and unresolved legacy raft-address
  voters as explicit compatibility rows.

### Fixed

- Conflicting same-peer probe results now preserve the snapshot contract:
  probe success wins over probe failure, independent of input order.

### Tests

- Added coverage for snapshot identity/liveness composition, policy
  predicates, probe-priority conflicts, cluster status legacy-field derivation,
  and the `grainfs serve` cluster status adapter.

## [0.0.68.0] - 2026-05-06

### Changed

- Storage ACL writes now use an internal decorator capability plan so optional
  ACL adapters are discovered without bypassing outer storage wrapper side
  effects such as cache invalidation and WAL ordering.
- Documented the storage decorator capability plan as the owner for ACL
  capability probing, fallback ordering, and rollback selection across
  decorated backends.

### Tests

- Added regression coverage proving `PutObjectWithACL` falls back through the
  outer `PutObject` wrapper when an inner decorated backend also exposes an
  atomic ACL adapter.

## [0.0.67.0] - 2026-05-06

### Changed

- Cluster peer identity now presents node IDs as the canonical shard-group
  runtime view, while legacy raft addresses are resolved through the MetaFSM
  address book when possible.
- `grainfs cluster peers` now shows node ID first, raft address as supporting
  detail, and `configured` / `unresolved_legacy` / `down` state instead of
  presenting configured peers as fake live peers.
- Cluster status responses include optional `peer_addrs` and `peer_states`
  maps so admin clients can show identity and legacy-resolution state without
  guessing from raft addresses.
- Data-group replica moves now block when the group still contains an
  unresolved legacy raft-address peer, preventing membership mutation until the
  node mapping is restored.
- PR-D identity decisions are recorded in `CONTEXT.md` and ADR 0003 so dynamic
  join, remove-peer, and later peer-health work share the same nodeID model.

### Tests

- Added coverage for reverse address-book lookup, MetaFSM legacy peer
  normalization, shard-group peer resolution, unresolved legacy mutation
  blocking, cluster status `peer_addrs` / `peer_states`, and CLI peer table
  rendering.


## [0.0.66.0] - 2026-05-06

### Added

- EC GET can now reconstruct from local shard streams instead of first loading
  full shard bodies into memory.
- Local shard reads now expose a plaintext stream API for chunked encrypted
  shards and CRC-protected shards.

### Changed

- EC GET reconstruction now works in bounded 1 MiB windows, lowering 64 MiB
  local EC GET allocation from roughly 67 MiB/op to about 4.2 MiB/op.
- CRC-protected shard reads now verify the footer while streaming, so EC GET
  keeps the integrity check without re-buffering the whole shard.

### Fixed

- CRC footer corruption is now detected even when a caller reads exactly the
  payload length from a sized shard stream.

### Tests

- Added stream reconstruction coverage for missing data shards, encrypted shard
  reader round-trips, CRC reader footer mismatch detection, and ShardService
  plaintext stream behavior.

## [0.0.65.0] — 2026-05-06 — Serveruntime helpers expansion (cmd-thin PR1a)

### Changed

- 9개 cobra-free 헬퍼 파일을 `cmd/grainfs/` 에서 `internal/serveruntime/`
  로 이주: `serve_bootstrap.go`, `nodeid.go`, `meta_join.go`,
  `iceberg_migration.go`, `optional_badger_role.go`,
  `incident_recorder_wiring.go`, `placement_monitor_registry.go`,
  `reshard_manager_registry.go`, `rotation_socket.go`.
- 새로 export된 심볼: `FilterEmpty`, `ShouldCreateDefaultBucketOnStartup`,
  `MetaProposalTargets`, `CreateDefaultBucketWithRetry`, `GenerateNodeID`,
  `PerformMetaJoin`, `MigrateLegacySingletonIcebergCatalog`,
  `EnsureIcebergMetadataObject`, `OptionalRoleDisabled`,
  `LogOptionalRoleDisabled`, `IncidentRecorderInterfaces`,
  `PlacementMonitorRegistry`, `ReshardManagerRegistry`,
  `StartRotationSocket`, `RotationSocketRequest`,
  `RotationSocketResponse`, `RotationSocketName`.
- cmd-side 호출 사이트 (`serve.go`, `cluster_join.go`,
  `cluster_rotate_key.go`, `serve_storage.go`, `receipt_wiring.go`) 와
  관련 테스트 파일을 `serveruntime.X` 호출로 갱신.
- 9개 cmd 파일 삭제 (총 -593 라인). cmd-thin PR1 본체(`runCluster`
  본체 이주)의 의존 그래프가 단축됨.

### Tests

- 기존 cmd 단위 테스트를 패키지 호출로 갱신 (동등 커버리지 유지).
  serveruntime 패키지의 단위 테스트 모두 통과.

## [0.0.64.0] — 2026-05-06 — Serveruntime helpers extraction (cmd-thin PR0)

### Changed

- 자체 의존성이 적은 cluster bootstrap 헬퍼 12개를 `cmd/grainfs/serve.go`
  에서 신규 `internal/serveruntime/` 패키지로 분리:
  - `ResolveClusterKey`, `GenerateEphemeralClusterKey` (cluster-key D10 해소)
  - `SeedInitialShardGroups`, `SeedShardGroupVoters` (group seeding)
  - `WaitForMetaRaftLeader`, `WaitForShardGroupCount` (bootstrap polling)
  - `StartAutoSnapshotterWhenReady` (+ private `waitForSnapshotBackendReady`)
  - `EnsureShardGroupPlaceholder`, `ECShardCounterFor`,
    `HandleRuntimeGroupInstantiationError`
- 새 패키지는 cobra 의존 0; runCluster 본체 이주(PR1)의 토대.
- `serve.go` 2250 → 2002 줄로 248줄 슬림.

### Tests

- `internal/serveruntime/{clusterkey,seed,helpers,snapshot}_test.go`
  에 13개 단위 테스트 신설. cmd/grainfs 의 중복 테스트
  (`serve_keystore_bootstrap_test.go`, `serve_snapshot_test.go`) 삭제;
  badger role 회복 테스트의 instantiation-error 케이스도 동일.

## [0.0.63.0] — 2026-05-06 — Resource monitors thin-cmd refactor

### Changed

- FD / goroutine / vlog 리소스 모니터의 watcher 부트스트랩, incident 기록,
  cluster alert 송출, prometheus metrics 갱신 로직을
  `internal/resourceguard/` 패키지로 이주. `cmd/grainfs/resource_monitors.go`
  (655줄) 가 사라지고, cmd 쪽에는 cobra flag → Options struct 어댑터와
  단일 진입 함수 `startResourceGuards(...)` 만 남음
  (`cmd/grainfs/resource_guard_helpers.go`).
- `serve.go` runCluster 의 watcher 시작 3줄이 단일 호출 한 줄로 축소. 새
  패키지는 `internal/server` 를 import 하지 않으며 alerts/incident 의존을
  슬림 인터페이스(`AlertsSender`, `IncidentRecorder`)로 노출해 테스트 시
  fake 주입이 직관적.

### Tests

- `internal/resourceguard/{fd,goroutine,vlog}_test.go` 에 16개 비즈니스 로직
  테스트 이주(decision warn/critical/recovery, metrics 게이지, badger GC
  failed incident, smoke under-populated incident, vlog top-3 breakdown).
  cmd 쪽에는 cobra flag-wiring 검증 4개만 잔류 (`*WatchFlagDefault`,
  `*WatchEnabledHelper`).

## [0.0.62.0] - 2026-05-06

### Added

- EC benchmark coverage now measures split, PUT, and GET behavior across 64 KiB,
  1 MiB, 16 MiB, and 64 MiB objects, making object-size memory regressions
  visible before ship.
- Encrypted EC shards now use a chunked AEAD envelope for new writes, so large
  encrypted shard writes can stream to disk instead of buffering plaintext.

### Changed

- EC GET now avoids local parity reads when all data shards are local, reducing
  read amplification on the common healthy path while still falling back to
  parity when a data shard is missing.
- EC GET reconstruction now writes through a reader pipeline instead of
  allocating the full reconstructed object before returning it.
- EC stream encoding now uses smaller Reed-Solomon stream blocks to reduce
  retained heap after large-object PUT workloads.
- Shard documentation now describes the generic integrity envelope instead of
  assuming every EC shard uses the legacy CRC-only format.

### Fixed

- Chunked encrypted shard decoding now rejects oversized chunk-size headers
  before allocation, so corrupted shard metadata cannot force an unexpectedly
  large read allocation.

### Tests

- Added encrypted shard round-trip, wrong-AAD, tamper, truncation, and oversized
  header coverage.
- Added EC reconstruction streaming and missing-data-shard parity fallback
  coverage.

## [0.0.61.0] - 2026-05-06

### Changed

- CopyObject now always returns destination previous-object facts from the
  storage operations facade, so overwrite accounting no longer depends on a
  separate result-specific method.
- CopyObject requests now use typed source, destination, and precondition
  fields throughout the facade and handler adapter instead of legacy bucket/key
  compatibility fields.
- Optimized copy support is now documented and wired as a private acceleration
  path behind the facade, keeping S3 CopyObject semantics in
  `storage.Operations`.

### Removed

- Removed the completed CopyObject semantics TODO after the facade now owns
  source validation, preconditions, metadata directive handling, and result
  facts.

### Docs

- Added an ADR and context vocabulary for CopyObject semantics ownership and
  updated the storage operations architecture notes to match the private
  acceleration path.

## [0.0.59.0] - 2026-05-06

### Changed

- Object write and delete metrics now use storage operation results for PUT,
  CopyObject, browser form upload, multipart complete, and delete, avoiding
  overwrite and repeated-delete drift.
- Browser form upload and multipart complete responses now include
  `X-Amz-Version-Id` headers when the storage mutation returns a version ID.
- Mutation result APIs now return normalized object facts and reject invalid
  mutation metadata before handlers emit metrics or responses.

### Tests

- Added storage operation coverage for multipart/delete mutation results and
  invalid mutation metadata handling.
- Added pure server metric delta tests for write overwrite and delete cases.

## [0.0.58.1] — 2026-05-06 — EC stream memory tuning

### Changed

- Cluster EC PUT now sizes Reed-Solomon stream blocks from the object size
  instead of retaining the library's default 4 MiB block buffers for small
  objects, sharply reducing post-benchmark heap use on small-object workloads.

### Tests

- Added EC stream block-size coverage for minimum, proportional, and capped
  sizing behavior.

## [0.0.58.0] — 2026-05-06 — Admin API wire type SOT

### Changed

- Admin HTTP JSON schema types now live in `internal/adminapi`, with
  `internal/server/admin` and `internal/volumeadmin` using aliases to the same
  wire definitions instead of maintaining parallel structs.
- Volume snapshot list responses now use the same explicit `created_at` and
  `block_count` JSON shape consumed by the volume admin client.

### Tests

- Added `internal/adminapi` JSON shape and round-trip coverage for scrub,
  volume, and snapshot wire payloads.

## [0.0.57.0] — 2026-05-06 — Storage mutation result bookkeeping

### Changed

- S3 PUT and CopyObject handlers now receive mutation results from
  `storage.Operations`, so previous-object accounting stays inside the storage
  facade instead of being pre-read by HTTP handlers.
- CopyObject overwrite metrics now use destination previous-object facts from
  the facade while preserving the legacy `CopyObject` method's existing read
  behavior for callers that do not request mutation bookkeeping.

### Tests

- Added facade-level coverage for previous-object summaries, missing
  destinations, and previous-read failures before mutation.
- Added HTTP metrics coverage for CopyObject overwrites so object count and byte
  totals do not double-count overwritten destinations.

## [0.0.56.0] — 2026-05-06 — CopyObject semantics

### Changed

- S3 CopyObject now honors encoded copy sources, source version IDs, metadata
  replacement for content type, and copy-source conditional headers before
  opening the source body.
- Storage copy operations now validate delete-marker sources, unsupported user
  metadata, unsupported ETag selectors, and same-object no-op copies with
  explicit storage errors that map cleanly to S3 responses.
- Packed object copies can now use a metadata-only adapter path while preserving
  content type metadata and avoiding wrapper bypasses around cache, WAL, and
  recovery write gates.

### Tests

- Added focused server, storage operation, and packed-backend coverage for
  CopyObject condition evaluation, versioned restore behavior, metadata
  replacement, delete-marker rejection, wrapper ordering, and packed copy
  readability after source deletion.

## [0.0.55.0] — 2026-05-06 — Volume CLI thin-cmd refactor

### Changed

- `grainfs volume` 명령군의 비즈니스 로직(HTTP, 에러 detail 포맷팅, 출력
  렌더링, 폴링 루프)을 `internal/volumeadmin/` 패키지로 이주. cmd/grainfs/
  쪽은 cobra 정의 + flag 파싱 + 옵션 빌드 + Run* 호출만 남는 thin wrapper로
  단순화. 테스트가 가능한 표면이 cmd 외부로 이동.
- `cmd/grainfs/admin_client.go`, `cmd/grainfs/format.go`, 그리고
  `cmd/grainfs/format_test.go` 가 사라지고 동등한 코드는
  `internal/volumeadmin/` 의 Client / format.go / discover.go 로 이주.
- `dashboard`, `scrub` (bucket) 명령은 새 `volumeadmin.Client` 의 공개
  HTTP primitives(`Get`/`Post`/`Delete`)와 `FollowScrubSession` 을 통해
  같은 admin 연결 인프라를 공유. 기능 변화 없음.

### Fixed

- `grainfs volume scrub status` 가 서버의 partial-peer-failure 정보(
  `OwnedHere`/`Partial`/`PeerFailures` 필드)를 조용히 떨어뜨리던 문제 수정.
  partial 인 세션은 `Partial: yes (peer failures: ...)` 줄로 표시.

### Tests

- `internal/volumeadmin/` 에 단위 테스트 신설: errors_test.go (typed detail
  helpers + 포맷터), format_test.go (parseSize/formatBytes 이주), client_test.go
  (httptest 기반 14 케이스), volume_ops_test.go (18 케이스),
  snapshot_ops_test.go (4 케이스), scrub_ops_test.go (8 케이스, follow loop
  의 정상 종료·ctx 취소 graceful exit 포함). 총 ~60 단위 테스트.

## [0.0.54.0] — 2026-05-06 — Storage operations facade hardening

### Changed

- Server bucket, object, multipart, lifecycle, and Iceberg metadata storage paths
  now route through `storage.Operations`, keeping optional backend behavior,
  fallback ordering, cache invalidation, and recovery write gating behind one
  facade.
- `storage.Operations` now satisfies the full `storage.Backend` interface,
  including bucket, object, multipart, delete, and walk operations, so upper
  layers can depend on the facade without retaining a separate raw-backend path.
- Server construction now carries the canonical backend through `ServerStorage`,
  keeping handler backend selection aligned with the provided operations facade.

### Fixed

- Cache invalidation now uses registered apply-loop invalidators, so committed
  object mutations can notify S3 cache consumers without depending on the legacy
  single `SetOnApply` hook.
- ACL writes through cached, swappable, and WAL-wrapped backends now stay within
  the correct wrapper ordering without locks, preserving cache invalidation and
  WAL recording while avoiding mixed-backend fallback writes during swaps.

### Tests

- Added focused coverage for cache invalidator registration, storage operation
  delegation, swappable plan refresh, ACL fallback rollback, lifecycle mutation
  routing, and server storage construction.

## [0.0.53.0] — 2026-05-06 — Cluster membership CLI

### Added

- **`grainfs cluster remove-peer <id>`** — operator can now evict a meta-Raft
  voter via §4.3 joint consensus. Pre-flight refuses the request when removal
  would drop live voters below the post-removal quorum; `--force` bypasses the
  pre-flight when ChangeMembership can still commit (e.g., evicting a
  permanently-dead peer). `--yes` skips the confirmation prompt for CI use.
  Endpoint is admin-restricted to localhost — run on the leader node.
- **`grainfs cluster peers`** — tab-aligned voter list (id, role, alive/down)
  with `--format=text|json`. Reuses `/api/cluster/status`, no new endpoint.
- **`grainfs cluster events`** — recent cluster audit events from
  `/api/eventlog` with `--since`, `--limit`, and client-side `--type` filter
  (e.g. `--type cluster-remove-peer,cluster-join`).
- New `cluster-remove-peer` audit event surfaces every membership change in
  `cluster events` and the `/api/eventlog` JSON.

### Changed

- `cluster.Peers()` / `cluster.LivePeers()` now read from the meta-Raft node
  rather than the legacy per-process raft, so `cluster status` and
  `cluster peers` reflect the current voter set after dynamic-join updates.
- `cluster.LivePeers()` reports all meta-Raft voters as alive until PR-D
  unifies node-id/raft-addr identifiers; pre-flight quorum math still blocks
  the only-1-voter-left case via voter count.
- `/api/cluster/remove-peer` joins `/api/cluster/status` and `/api/eventlog`
  on the auth-bypass list (gated by `localhostOnly` + `mutationGate`).

### Fixed

- `removePeerHandler` remaps `raft.ErrNotLeader` (engine returns it when
  leadership changed mid-call) to `409 + leader_id`, matching the up-front
  check's shape so the CLI sees one consistent error contract.
- `clusteradmin.RemovePeer/Peers/Events` reject `Timeout <= 0` with a clear
  error rather than producing a confusing `context deadline exceeded`.

### Tests

- `make test`
- `go test ./tests/e2e/ -run TestE2E_ClusterRemovePeer_DeadFollower` (3-node
  dynamic-join cluster, kill follower, happy-path remove-peer, audit event
  verified)
- `go test ./tests/e2e/ -run "TestClusterStatusCLI|TestE2E_JoinedNode|TestE2E_AllServicesAvailable"` (no regressions)

## [0.0.52.1] — 2026-05-06 — E2E data directory cleanup

### Fixed

- Fixed the shared e2e `TestMain` server data directory cleanup path. The
  previous `defer os.RemoveAll(...)` never ran because `TestMain` exits through
  `os.Exit`, so `grainfs-e2e-go-*` directories accumulated after each test
  process.

### Tests

- `make test-e2e`
- `GRAINFS_BINARY="$(pwd)/bin/grainfs" go test ./tests/e2e -run '^TestBuckets_Create$' -count=1 -timeout 60s`

## [0.0.52.0] — 2026-05-06 — Shard group peer identity

### Changed

- **Shard group peer identity** — new shard group seeding now resolves known
  peer addresses to stable node IDs before writing membership, while preserving
  dialable address aliases for static peers whose node IDs are not known during
  bootstrap.
- **Local peer matching** — centralized shard group local-peer matching and
  forward ordering behind `ShardGroupPeerSet`, so coordinator routing and group
  lifecycle wiring share the same node ID / legacy address compatibility rules.

### Tests

- Added focused coverage for node ID preference, legacy address aliases,
  forwarding order, and static peer fallback during shard group seeding.

## [0.0.51.0] — 2026-05-06 — Storage operations facade

### Added

- **Storage operations facade** — added `storage.Operations` as the server
  request-handler entry point for optional storage capabilities, with typed
  unsupported-operation errors and operation families for ACL, CopyObject,
  bucket policy, and object versioning.
- **Server storage composition** — added explicit `ServerStorage` wiring so
  handlers use the facade while snapshot, volume, DB, and authorization
  dependencies stay visible at construction boundaries.
- **Dedicated bucket policy package** — moved bucket policy parsing, compiled
  authorization, tests, and benchmarks into `internal/policy`, leaving server
  aliases as compatibility shims during the migration.

### Fixed

- **CopyObject through cache decorators** — kept the optimized copy path behind
  the facade's outer-first capability lookup so cache invalidation and recovery
  gates remain in the request path.
- **ACL rollback reporting** — failed ACL writes now surface a rollback failure
  when the newly written object does not report a version ID, instead of
  implying cleanup happened.

### Tests

- Added focused `internal/storage` facade contract coverage for unsupported
  operations, ACL rollback, CopyObject fallback streaming, bucket policy cache
  synchronization, versioned operations, and decorator ordering.
- Added `internal/server` storage wiring coverage and moved policy unit tests
  and benchmarks to `internal/policy`.

## [0.0.50.0] — 2026-05-06 — Volume health CLI

### Added

- **Volume health in admin responses and CLI output** — `grainfs volume list`
  now includes a `HEALTH` column, and `volume info` / `volume stat` show the
  same health summary. JSON output now includes stable `health` and
  `health_reasons` fields so scripts can distinguish `ok`, `warning`,
  `critical`, and `unknown` volumes without parsing human text.
- **Incident-backed volume health reasons** — volume health now derives from
  recent volume-scoped incidents, reports unresolved critical incidents as
  `critical`, and reports incident-store lookup failures as `unknown` with
  `incident_lookup_failed`.
- **Storage operations facade design docs** — added the architecture proposal
  and implementation plan for moving optional storage capability probing behind
  a `storage.Operations` facade.

### Fixed

- **Consistent health across `list`, `info`, and `stat`** — single-volume
  metadata now uses the same incident-backed health calculation as list/stat,
  so a damaged volume cannot appear healthy in `volume info`.

## [0.0.49.3] — 2026-05-06 — Predictive vlog watcher e2e leak-fire coverage

### Added

- `--badger-value-threshold` (hidden serve flag, test-only) installs a
  process-wide `WithValueThreshold` override so even small metadata writes
  spill into the value log. Defaults to 0 (Badger's 1 MiB default).
- `internal/badgerutil.SetValueThresholdOverride` exposes the same hook to
  test harnesses that open Badger via `SmallOptions` / `RaftLogOptions`.

### Tests

- New `TestE2E_VlogWatcher_FiresOnLeak` boots a single-node cluster with the
  flag and a near-zero `--vlog-warn-ratio`, exercises a small S3 PUT workload,
  and waits for the predictive watcher to record a `vlog_pressure` incident.
  This closes the (g) follow-up from the predictive resource watcher TODO —
  end-to-end "fires on leak" was previously only proven by unit tests because
  the default 1 MiB value threshold kept e2e vlog file sizes at zero.
- The existing `TestE2E_VlogWatcher_MetricsLive` comment was updated to point
  at the new test for the leak-fire assertion.

## [0.0.49.2] — 2026-05-06 — Predictive watcher cold-start suppression

### Fixed

- **Resource watcher (FD/goroutine/vlog) cold-start over-eager fire** —
  `Detector.projectedETA` computed slope from the very first observed
  sample to the current one. The startup transient (0 → steady-state)
  inflated the slope and drove the predicted ETA below the ETAWindow
  within seconds, so all three watchers fired a `WARN`-level
  `ResourceUsagePredicted` on the second poll even when steady state
  was nowhere near the warn ratio. The new `DetectorConfig.MinETAElapsed`
  (default 5min) gates predictive fire until the oldest retained sample
  is old enough for the slope estimate to be meaningful. Level-based
  fires (ratio ≥ warn/critical) are unaffected — real threshold
  breaches still fire immediately.

## [0.0.49.1] — 2026-05-06 — full e2e stabilization after master rebase

### Fixed

- Stabilized full e2e startup after rebasing on `origin/master` by spreading
  port allocation across test processes and retrying cluster startup with fresh
  ports on bind/start races.
- Updated CoW and volume e2e coverage to use the admin-socket CLI volume
  surface after the data-plane `/volumes/*` API removal.
- Updated Docker NBD, NBD CoW, and NBD dedup scripts to manage volumes through
  `grainfs volume ... --data`, matching the current control-plane contract.
- Hardened data-group self-removal migration so leadership transfer waits for a
  caught-up voter and caller retry handles short post-transfer leadership churn.
- Made QUIC raft integration setup retry transient full-mesh connection races
  instead of failing on one idle-timeout dial.

### Tests

- `make test`
- `make test-e2e` completed once across NFS/NBD/Iceberg/EC coverage; NFSv4
  smoke was skipped on macOS as Linux-only. A final post-fix rerun was started
  and intentionally stopped at the user's request to ship the current work.

## [0.0.49.0] — 2026-05-06 — EC scrub legacy shard safety

### Added

- **EC scrub now distinguishes verified shards from legacy raw shards** — scrub
  can tell when a readable EC shard has no CRC envelope, records it as
  unverified, and keeps it out of the healthy path.
- **Unverified legacy shard telemetry** — operators can track
  `grainfs_ec_scrub_unverified_shards_total{reason="legacy_no_crc"}` to decide
  whether a legacy shard rewrite or migration is worth running.

### Fixed

- **Legacy raw EC shards no longer trigger unsafe repair or rewrite** —
  unverified-only cases are skipped, and mixed missing/corrupt plus unverified
  cases do not reconstruct from bytes that lack an integrity oracle.
- **EC scrub detect events now report skipped legacy shards** with
  `err_code="legacy_no_crc"` so dashboards can separate CRC-backed corruption
  from pre-envelope legacy data.

### Changed

- **`ReadShard` remains backward-compatible for legacy raw bytes** while the
  scrub-only `ReadShardIntegrity` path exposes whether the bytes were actually
  CRC verified.
- **Legacy raw shard rewrite stays as explicit follow-up work** so production
  telemetry can drive whether migration is needed.

## [0.0.48.2] — 2026-05-06 — Volume CLI flag dedup

### Changed

- **`grainfs volume` 공통 flag 통합** — 18개 서브커맨드(list/create/info/stat/
  delete/resize/recalculate/clone/rollback/write-at/read-at/snapshot {create,
  list,delete}/scrub {trigger,status,list,cancel})에 반복 등록되던
  `--endpoint`/`--data`/`--json`/`--bytes`/`--raw` 5개 flag를
  `volumeCmd.PersistentFlags()`로 일괄 이동. Cobra가 자식 명령에 자동
  상속하므로 사용자 입력은 그대로 동작하고, `volume {sub} --help` 출력에서
  공통 flag가 Global Flags 섹션에 묶여 가독성이 향상된다.

## [0.0.48.1] — 2026-05-06 — Multi-raft restart e2e timeout fix

### Fixed

- **TestE2E_MultiRaftSharding_RestartRecovery / PerGroupPersistence** — The
  tests passed `0` as `seedGroups` when restarting nodes, which made the
  serve binary auto-derive `0 → max(8, (1+peers)*4) = 12` for a 3-node
  cluster. Each restart re-proposed 10 unseeded shard groups and waited
  for them via `waitForShardGroupCount(12, 30s)`, pushing cumulative
  test time past the 600s budget under macOS host contention. The
  test now caches the original `seedGroups` on `mrCluster` and reuses
  it on restart so WAL replay alone satisfies the wait. With the fix
  both tests run in ~32s instead of timing out.

### Changed

- **`E2E_TEST_TIMEOUT` default 600s → 900s** — gives 300s headroom for
  long-running multi-raft and predictive-warning tests under host load.

## [0.0.48.0] — 2026-05-06 — Badger role-scoped startup recovery

### Added

- **Badger role registry and startup reducer** — metadata, raft-log,
  group-state, receipt, dedup, volume-catalog, and incident-state DBs now
  have explicit role criticality, paths, feature flags, and deterministic
  startup decisions.
- **Role probes and quarantine transaction helpers** — Badger role opens now
  return structured decisions, writable probes classify read-only admission,
  and quarantine moves role directories through explicit staging instead of
  ad hoc path handling.
- **Recovery read-only gates** — restart-time group Badger failures can admit
  the server in read-only mode; storage writes and non-storage mutating APIs
  return recovery errors while read paths remain available.
- **Optional role disablement** — receipt, dedup, and incident-state DB open
  failures now disable only their feature path instead of aborting startup.

### Fixed

- **Failed corruption isolation incidents now stay visible** — failed object
  isolation transitions to a human-actionable state instead of looking like a
  normal completed repair.
- **Optional incident-state disable no longer passes typed nil recorders** —
  scrub and repair wiring now receive real nil interfaces when the incident
  API is disabled, avoiding a panic on later incident emission.
- **Default bucket creation skips recovery read-only startup** so a degraded
  server does not immediately perform a write during recovery admission.

### Changed

- **Cluster startup now reduces Badger decisions before accepting traffic** —
  cold-start group instantiation runs synchronously enough to classify startup
  health, while runtime group failures log without killing the process.
- **Badger preflight uses the shared role probe path** so cluster and local
  startup diagnostics follow the same control-loop vocabulary.
- **Dependencies refreshed** — fsnotify, grpc, and genproto are updated to the
  latest branch baseline used by this release.

### Tests

- Added unit coverage for role registry resolution, open failure actions,
  startup reducer modes, probe classification, quarantine transactions,
  mutation gates, incident reducer Badger causes, and group lifecycle injection.
- Added integration and e2e coverage for Badger role startup recovery,
  recovery read-only default-bucket behavior, optional role disablement, typed
  nil incident recorder wiring, and cluster missing-shard repair with receipts.
## [0.0.47.0] — 2026-05-05 — EC scrub trigger — admin/CLI + cluster-wide aggregation (PR4)

### Added

- **Admin endpoint `POST /v1/scrub`** — body `{bucket, key_prefix?, scope?, dry_run?}`,
  publishes a cluster-wide trigger via meta-raft (`MetaCmdTypeScrubTrigger`).
  Each node's `Director.ApplyFromFSM` creates a session for the same
  `SessionID`; the EC source resolver routes per-bucket to the right
  group's BadgerDB or returns an empty channel on peer-owned groups.
- **`GET /v1/scrub/jobs/<id>` cluster-wide aggregation** — fans out
  `ForwardOpScrubSessionStat` to every peer in parallel with 5s per-peer
  timeout. Aggregated `ScrubJobInfo` gains `OwnedHere bool`,
  `Partial bool`, `PeerFailures []string` so operators can distinguish
  "everyone agrees done" from "we got most peers, one timed out".
- **CLI `grainfs scrub <bucket>`** with `--prefix / --scope / --dry-run /
  --detach`. Without `--detach` follows the session via
  `followScrubSession` (shared with `grainfs volume scrub`).
- **`Director.LookupDedup`** — exported helper used by admin
  `ScrubProposer` to short-circuit duplicate triggers before raft propose.
  `ApplyFromFSM` now populates the dedup map so the leader-side check
  works for raft-applied entries.
- **`MetaScrubTriggerCmd`** with 1h TTL filter on apply — fresh nodes
  joining via raft log replay don't re-run ancient scrubs.

### Architecture

- `ECScrubSource` constructor takes
  `GroupResolver func(bucket) (Scrubbable, bool)` instead of a single
  backend. `SingleBackendResolver` shim preserves single-node / test
  ergonomics. scrubber package stays cluster-clean (no cluster import).
- `admin.ScrubProposer` and `admin.ScrubAggregator` interfaces follow
  the `PeerHealthAPI` / `VlogBreakdownAPI` patterns; `serve.go` wires
  `scrubProposerAdapter` (over `MetaRaft.ProposeScrubTrigger`) and
  `scrubAggregatorAdapter` (over `ClusterCoordinator.ScrubSessionStat`).
- `ForwardReceiver.WithScrubSessionLookup` exposes the local Director's
  `GetSession` to peer RPCs without pulling the full Director surface
  into the cluster package. `ForwardOpScrubSessionStat` is the first
  cluster-wide forward op that bypasses the per-group leader gate
  (read-only + node-scoped).

### Tests

- Unit: 11 existing `ec_source_test.go` tests adapted via
  `SingleBackendResolver`; +2 resolver-specific (false → empty channel,
  per-bucket routing). +2 `Director.LookupDedup` (hit/miss). +4
  `MetaFSM.applyScrubTrigger` (recent / stale / no-callback / empty
  payload). +6 admin `TriggerScrub` + `GetScrubJob` aggregation.
- E2E: `ec_scrub_trigger_e2e_test.go` covers `FlowsThroughCluster`
  (3-node EC 2+1, trigger reaches done with bucket round-trip via
  aggregator) and `DedupHit_ReturnsExistingSession` (identical body
  twice → same `SessionID`, `created=false`).

### Deferred (re-open triggers in TODOS.md)

- **Group rebalance race during in-flight scrub** — pre-existing in
  `BackgroundScrubber` path; resolver routes correctly but does not
  detect/cancel mid-walk. Telemetry-driven re-open.

## [0.0.46.0] — 2026-05-05 — vlog admin breakdown + GC metric wiring + e2e (PR3)

### Added

- **`GET /v1/resource/vlog/breakdown`** — per-category vlog bytes
  (sorted desc), consecutive GC failure counters, on-demand registry
  smoke report, computed warn/critical level. Operators answer "which
  BadgerDB category dominates vlog right now?" without reading raw
  metrics.
- **`--vlog-smoke-defer` flag** (default 60s) so e2e tests can shrink
  the smoke window without baking process exits into a 70+s wait.
- **4 vlog watcher e2e tests** — `MetricsLive`, `GCTickerRecovers`,
  `StrictFatalOnMissing`, `NoStarvation`. Cluster harness gains a
  generic `ExtraArgs` knob for per-test flag tweaks.

### Fixed

- `grainfs_badger_gc_runs_total` / `grainfs_badger_gc_failures_total` /
  `grainfs_badger_gc_consecutive_failures` were declared in PR2
  (v0.0.45.0) but never incremented. PR3 wires `GCMetricsRecorder`
  interface on `GCTickerConfig` (keeps resourcewatch metrics-free)
  bridged to Prometheus from `cmd/grainfs`.
- `--strict-vlog-registry` was dead — PR2 swallowed the strict-mode
  error to a `log.Warn`. Now `log.Fatal` so smoke failures actually
  exit the process as the help text claims.

## [0.0.45.0] — 2026-05-05 — BadgerDB vlog watcher (PR2)

### Added

- **vlog resource watcher** (`internal/resourcewatch/`) — generic
  `*Registry` + `Default` singleton + DI; `VlogProvider` sums
  `db.Size()` across categories + statfs-derived ratio; GC ticker
  (5min sequential, snapshot-then-unlock + max-iter cap 8 +
  transition-only fire); startup smoke (60s deferred, mtime stale/live
  classification).
- **Categories registered**: meta, shared-raft-log, group-raft,
  incident, receipts, dedup, storage. Default warn ratio 0.4,
  critical 0.7.
- **8 metrics**: `grainfs_vlog_bytes`,
  `grainfs_vlog_bytes_by_category`, `grainfs_vlog_limit_bytes`,
  `grainfs_vlog_used_ratio`, `grainfs_vlog_eta_seconds`,
  `grainfs_badger_gc_runs_total`, `grainfs_badger_gc_failures_total`,
  `grainfs_badger_gc_consecutive_failures` (last three start firing
  in PR3).
- **3 incident causes**: `vlog_pressure`, `badger_gc_failed`,
  `registry_under_populated`.

## [0.0.44.0] — 2026-05-05 — goroutine predictive warnings + generic refactor (PR1)

### Added

- **goroutine resource watcher** — fires warn/critical on goroutine
  count crossing thresholds (default 5000 / 20000 from baseline ~200).
  Generic resource watcher framework
  (`Sample / Provider / Level / Category / DetectorConfig / Decision`)
  used by this PR and by PR2 vlog watcher.
- **Generic `Detector` with `ResourceLabel`** — fixes the latent bug
  where vlog watcher's decision message would say
  "FD usage 18.1% (...)" with vlog categories because the label was
  hardcoded "FD".

## [0.0.43.4] — 2026-05-05 — Peer health observability

### Added

- **Prometheus metrics**:
  - `grainfs_peer_unhealthy{peer}` — Gauge, `1` while a peer is in
    `PeerHealth` cooldown, `0` otherwise. Alert on `> 0`.
  - `grainfs_replication_skipped_total{peer,bucket}` — Counter, increments
    every time `putObjectNxSpooled` skips a peer because it is unhealthy.
    Rate `> 0` means the cluster is operating with reduced replication
    factor.
- **Admin endpoint** `GET /v1/cluster/peers` — returns `{ peers: [{id,
  healthy, last_failure, cooldown_remaining_ms}, ...] }`. Available on the
  Unix-socket admin server and `/ui/api/cluster/peers`. Empty list (200
  OK) when peer health is not wired (single-node).
- **Transition logs** at `warn` level in `putObjectNxSpooled` /
  `putObjectNxSpooledAsync` when a peer transitions healthy→unhealthy and
  `info` level on the recovery transition. One log per transition, not per
  skip — counter carries the rate signal.
- `PeerHealth.MarkUnhealthy` / `MarkHealthy` now return `bool` indicating
  whether the call caused a state transition. `PeerHealth.Snapshot()`
  returns the current view of every tracked peer.

### Notes

- Behavior unchanged: cooldown is still 10 s, single-strike still moves
  the peer to "skip" mode. This release closes the visibility gap so
  operators see silent under-replication immediately. The next follow-up
  ("PeerHealth one-strike-out — consecutive-failures threshold") is a
  separate policy decision tracked in `TODOS.md`.

## [0.0.43.3] — 2026-05-05 — N×replication actually replicates

### Fixed

- **Critical correctness fix.** `cmd/grainfs/serve.go:747` registered the
  `StreamShardWriteBody` body handler on a local `StreamRouter` instead of
  the `*QUICTransport` instance's internal router. The catch-all
  `SetStreamHandler(router.Dispatch)` only proxies non-body messages, so
  every `StreamShardWriteBody` arriving at a peer fell through to the
  inbox-fallback path: server closed the stream without responding, sender
  saw `decode response: read header: EOF`, peerHealth marked the peer
  unhealthy, and `putObjectNxSpooled` silently skipped replication on
  every subsequent write (debug-level log only).
- **Net effect before this fix:** every `__grainfs_volumes` (and other
  N×replication) write produced a single replica on the bucket's group
  leader. A single disk failure on the leader was unrecoverable. Operators
  saw at most one "data replication failed" warning per peer per cooldown
  window — subsequent skips were invisible.
- One-line fix: replace `router.HandleBody(...)` with
  `quicTransport.HandleBody(...)` so the handler lives on
  `t.router.bodyHandlers`, where `handleStream` actually looks. Other
  body-stream registrations (`StreamGroupForwardBody`,
  `StreamGroupForwardRead`) already used this path correctly via
  `shardSvc.RegisterBodyHandler`.

### Added

- `TestE2E_VolumeScrub_MultiNodeRepair` — 3-node cluster, write a volume
  block, scan all nodes' on-disk replicas (data/.obj path for the leader,
  shards/ path for peers), truncate one replica, trigger scrub, expect
  `Detected=1 Repaired=1` via peer-pull. Pre-fix: only the leader has a
  copy → `holders=1` assertion fails. Post-fix: 3 holders, repair works.

### Notes

- The new test inserts a 12s sleep after `volume create` so peerHealth's
  10s cooldown clears before the block write. Tracked as follow-up
  ("PeerHealth one-strike-out") in TODOS § Cluster Replication Reliability;
  not a problem this PR introduces, just newly visible without the
  shard-stream EOF masking it.

## [0.0.43.2] — 2026-05-05 — Scrub admin trigger works at --scrub-interval=0

### Fixed

- `cmd/grainfs/serve.go` — Director wiring (admin-trigger `volume scrub`)
  was incorrectly gated on `--scrub-interval > 0`. Operators who disabled
  periodic scrub lost the on-demand CLI as a side-effect ("scrub director
  not configured" error). Director + shared replication source/verifier
  are now wired unconditionally; only the periodic ticker + placement
  monitor still require a positive interval.

### Added

- E2E regression `TestE2E_VolumeScrub_AdminTriggerWorksAtZeroInterval`
  pins the fix: spin up a server with `--scrub-interval=0`, trigger
  `volume scrub`, expect success rather than the "not configured" error.

## [0.0.43.1] — 2026-05-05 — Dedup-mode scrub regression coverage

### Notes

- `__grainfs_volumes` scrub already worked under `--dedup=true` in v0.0.43.0;
  the original "dedup-mode incompatibility" follow-up was a misdiagnosis
  carried over from an intermediate group-routing bug (since fixed). No code
  change needed — test additions confirm the existing behavior.

### Added

- E2E variants `*_Dedup` for `HealthyNoop`, `DryRunDetectsCorruption`, and
  `SingleNodeRepairUnrepairable`. Pin source walk + verifier MD5 + repair
  attempt against canonical `_v<UUID>` keys so a future dedup-mode regression
  is caught by CI.

### Removed

- TODOS entry **"Volume scrub — dedup 모드 호환"** — closed by the regression
  tests above.

## [0.0.43.0] — 2026-05-04 — Object-layer replication scrub

### Added

- **`grainfs volume scrub <name>` CLI** — operator entry point for non-EC
  volume bit-rot detection and peer-pull repair. Flags: `--scope=full|live`,
  `--dry-run`, `--detach`. Subcommands: `scrub status <id>`,
  `scrub list`, `scrub cancel <id>`.
- **Admin scrub endpoints** (Unix socket + `/ui/api`):
  `POST /v1/volumes/:name/scrub`, `GET /v1/scrub/jobs`,
  `GET /v1/scrub/jobs/:id`, `DELETE /v1/scrub/jobs/:id`.
- **`scrubber.Director`** owns scrub session lifecycle (Trigger dedup +
  `ApplyFromFSM` non-blocking enqueue + per-block incident.Fact emission).
  `incidentRecorder` is wired in cluster scrub block of `serve.go` so
  detection/repair flows into the existing incident store.
- **`scrubber.ReplicationObjectSource` / `ReplicationVerifier`** — generic,
  bucket-prefix-driven replication scrub primitives. `BlockSource` /
  `BlockVerifier` interfaces are bucket-agnostic; volume blocks are the first
  wiring, future internal-bucket replication objects (NFS, future iceberg
  metadata, etc.) plug in the same way.
- **`cluster.DistributedBackend.RepairReplica(bucket, key)`** — pulls a
  healthy peer copy and atomically rewrites the local replica when scrub
  detects corruption or a missing replica.
- **`OpenLocalReplica(bucket, key)`** on both `LocalBackend` and
  `DistributedBackend`. Used by the verifier to MD5 the local-only file
  without falling back to peers (peer recovery is the repair path's job).
- **Admin volume `write-at` / `read-at`** helpers (used by e2e tests as the
  block ingress / readback path; also useful for forensic block inspection).

### Changed

- **MD5 oracle restored across all internal buckets.** Three sites previously
  skipped MD5 for `IsInternalBucket(bucket)` as a hot-path shortcut, leaving
  every internal bucket (volume blocks, NFS objects, future packed blobs)
  without a corruption-detection oracle. Now hashed everywhere:
  - `LocalBackend.PutObject` / `LocalBackend.WriteAt`
  - `cluster.spool.shouldHashBucket`
  - `cluster.DistributedBackend.WriteAt`
  - `storage/packblob/PackedBackend.PutObject`
- **`storage/bucket.go` doc** rewritten — internal classification is a
  routing concern, never a hash-skip shortcut. Future BLAKE3 / xxhash3 swap
  is queued in `TODOS.md` "Storage Hashing 성능 검토".
- **Volume Manager no longer owns scrub.** The previous incarnation lived in
  `internal/volume/scrubber.go`; layered architecture says the object
  storage layer must guarantee integrity so upper layers (NFS, NBD, S3,
  Iceberg, Volume) can trust it. The implementation moved to
  `internal/scrubber/replication.go` and `volume.Manager` no longer carries
  scrub responsibility.
- **`BackgroundScrubber`** simplified to a single replication-source ticker
  (the previous live/full dual-ticker had no functional difference because
  the generic source treats both scopes identically). Re-introduction
  conditions are documented in `TODOS.md`.

### Deferred (TODOS.md)

- Volume scrub cluster-broadcast via `MetaFSM ScrubTrigger` entries — the
  meta-FSM uses FlatBuffers schemas (not gob), so the schema work is split
  out. Operators trigger one node at a time until that lands.
- Volume scrub for dedup-mode volumes — dedup keys are `blk_<NNN>_v<UUID>`
  and need an index reverse-lookup. v0.0.43 e2e tests run with `--dedup=false`.
- Multi-node corruption-repair E2E — single-node E2E is shipped here;
  3-node repair test follows.
- `RepairReplica` unit test — `*ShardService` must be extracted to a
  `peerReader` interface first.
- EC silent shard-corruption audit — current EC scrub may be missing a shard
  checksum oracle. Independent gap that affects all EC data, not specific
  to this PR.
- `__grainfs_volumes` EC migration — replication is forced today (a coarse
  internal-bucket guard); future PR can narrow the guard to VFS-only and
  migrate existing replication blocks.
- Per-source Prometheus metrics (`grainfs_scrub_*` counters/gauges).

## [0.0.42.0] — 2026-05-04 — Volume CLI Phase B (admin socket + dashboard token)

### Added

- **Volume management CLI** (operator-facing) — `grainfs volume {list, create, info,
  stat, delete, resize, recalculate, clone, rollback}` plus
  `grainfs volume snapshot {create, list, delete}`. All commands run against the
  admin Unix socket and route through `volume.Manager` directly. Examples are
  embedded in `--help` output.
- **`grainfs volume resize`** (grow only) — `Manager.Resize` 추가. Shrink 거부 +
  clone hint, equal size no-op.
- **`grainfs volume delete --force`** — atomic cascade through new
  `Manager.DeleteWithSnapshots`. Without `--force`, conflict error includes the
  three most recent snapshots and the cascade command.
- **`grainfs dashboard`** — issues a single shared auth token URL for the web
  dashboard. Token persisted at `<data>/dashboard.token` (mode 0600). Use
  `--rotate` to invalidate. Fragment-based handoff so the token never reaches
  server logs.
- **Admin Unix socket** (`<data>/admin.sock`, mode 0660) — local operator entry
  point. File permission = authentication. Optional `--admin-group <name>` for
  shared access. `--public-url` controls the dashboard URL prefix.
- **`/ui/api/*` data-plane routes** — same admin handlers, gated by the
  dashboard token middleware on `/ui/*` and `/ui/api/*`.

### Changed

- **Data-plane `/volumes/*` routes removed.** Volume management lives on the
  admin Unix socket (CLI) and `/ui/api/volumes/*` (web UI). Pre-release scope —
  no shim. Web UI fetches updated to the new path with `Authorization: Bearer`
  header.

### Deferred

- **`volume scrub` 명령** — Phase B 범위에서 분리. 근본 원인: `__grainfs_volumes`
  가 internal bucket 으로 분류되어 `internal/cluster/backend.go` 가 EC 인코딩을
  비활성화 — volume 블록은 `lat:` 인덱스에 등록되지 않아 기존
  `BackgroundScrubber` 가 스캔하지 못한다. 별도 spec 필요 (Manager 측 verify +
  NX-replica repair, meta-raft cluster-wide trigger). follow-up 은 `TODOS.md`
  의 "Volume CLI follow-ups" 참고.

## [0.0.41.0] — 2026-05-04 — online cluster-key rotation

### Added

- **online cluster-key rotation** (`grainfs cluster rotate-key {begin,status,abort}`):
  S3/NFS/NBD downtime 없이 PSK 교체. Localhost-only Unix socket
  (`$DATA/rotate.sock`, mode 0600) 으로만 명령 수신 — 네트워크 노출 없음.
  Operator-distributed `keys.d/next.key` SPKI 를 raft 합의 명령과 대조 검증
  후 transport identity swap. Leader-driven 4-phase 자동 진행
  (steady→begun→switched→steady-on-NEW), 5초 phase grace, 30분 global
  timeout 자동 abort. Phase-2 abort 는 OLD 롤백, phase-3 abort 는 D18 spec
  대로 NEW forward-roll.
  > **다중 노드 사용 시 필독**: 운영자는 회전 시작 전에 새 PSK 파일을
  > **모든 peer 의 `<DATA>/keys.d/next.key` 에 직접 배포**해야 한다. CLI 는
  > leader 디스크에만 쓰며, follower 워커는 자기 디스크의 `next.key` 를
  > 읽는다. 미배포 시 cluster network split. 자세한 절차는
  > `docs/RUNBOOK.md#online-rotation-권장`.
- **transport keystore** (`internal/transport/keystore.go`): `keys.d/{current,next,previous}.key`
  슬롯 매니저. Atomic write + fsync, O_NOFOLLOW, mode 0700/0600.
- **multi-SPKI accept set**: `transport.IdentitySnapshot` + `atomic.Pointer`
  로 lock-free identity reload. 회전 중 OLD/NEW 모두 accept, present 만 전환.
- **PSK bootstrap conflict resolution** (D10): `keys.d/current.key` 가
  `--cluster-key` 플래그와 다르면 디스크가 이김 (warn 출력). 플래그 단독이면
  최초 부트시 디스크에 미러링.
- **cluster-key 회전 RUNBOOK**: `docs/RUNBOOK.md#cluster-key-rotation` —
  online 절차, abort 동작, offline fallback.

### Changed

- **MetaFSM**: 4 신규 명령 (RotateKeyBegin/Switch/Drop/Abort) 디스패치.
  결정론적 RotationFSM (D16 분리) + onRotationApplied 콜백으로 side-effects 트리거.
- **MetaRaft.Start**: leader-only auto-progress goroutine 추가.

### Tests

- 단위: RotationFSM (9), RotationWorker (6), rotation codec round-trip (3),
  MetaFSM end-to-end apply (1), receipt rotation audit (1).
- E2E: `tests/e2e/cluster_rotate_key_test.go` — solo status smoke + full
  4-phase happy-path with auto-progress 검증 (`keys.d/current.key` = NEW,
  `keys.d/previous.key` = OLD).

## [0.0.40.0] — 2026-05-04 — stabilize e2e startup routing

### Changed

- **snapshot startup**: PITR auto-snapshot now waits for routed backend enumeration to become ready before starting the snapshot loop.
- **static shard seeding**: seed groups now use the same peer identity form as group raft, and the coordinator treats both node ID and raft address as local self aliases.
- **EC perf writes**: small spooled EC shards now use framed shard RPCs, while larger shards retain streaming writes with longer deadlines and retry.
- **QUIC bulk streams**: body-stream overload now cancels unread streams instead of risking sender-side body-copy stalls, and default bulk capacity now covers forwarded PUT plus EC fan-out.

### Fixed

- **remote shard routing**: non-local shard groups are registered as placeholders so bucket routing can forward to remote voters instead of failing with missing local groups.
- **TODO cleanup**: removed completed e2e TODO entries, including the EC-on `load-N16` perf follow-up after stabilizing it.

### Tests

- Added readiness, remote placeholder, seed voter identity, and self-alias coverage.
- Verified `go test ./cmd/grainfs ./internal/cluster ./internal/snapshot`.
- Verified focused auto-snapshot e2e with `go test ./tests/e2e -run TestAutoSnapshot_CreatesSnapshotAutomatically -count=1 -v`.
- Verified EC-on perf e2e with `GRAINFS_PERF=1 GRAINFS_PERF_SCENARIO=load-N16 go test ./tests/e2e -run TestE2E_ClusterPerf_All -count=1 -v`.

## [0.0.39.0] — 2026-05-04 — harden validation and stream object memory paths

### Added

- **repository lint baseline**: added `.golangci.yml` and wired `make lint` to run `go vet`, `gofmt`, and the configured `golangci-lint` pass.
- **PUT spooling and streaming**: added cluster object spooling, streaming local shard writes, streamed peer shard replication, and streamed EC shard generation.
- **memory regressions**: added allocation coverage for Range GET and cluster PUT, including FD cleanup coverage for streamed shard fan-out.

### Changed

- **Range GET**: large byte ranges now stream through bounded readers instead of allocating the whole response body.
- **cluster writes**: N× replication and EC writes now preserve ETag, content type, versioning, and shard metadata while avoiding full-body buffering in the hot fan-out path.
- **lint hygiene**: removed dead receipt/shard helpers, classified test-only helpers, and fixed production unchecked-error/static-analysis findings.

### Fixed

- **rebalance timeout context leak**: join-mode startup no longer creates an unused timeout context.
- **auto-snapshot e2e readiness**: the focused auto-snapshot test now waits deterministically for routing and snapshot readiness before asserting.
- **heartbeat coalescer test race**: fake sender assertions now read lock-protected snapshots.
- **empty EC objects**: streamed EC shard spooling and reconstruction now handle 0-byte objects.

### Tests

- Verified `make lint`.
- Verified changed packages with `go test ./cmd/grainfs ./internal/server ./internal/cluster ./internal/transport ./internal/storage/eccodec ./internal/nbd ./internal/raft ./internal/raft/chaos -count=1`.
- Verified focused auto-snapshot e2e with `go test ./tests/e2e -run TestAutoSnapshot_CreatesSnapshotAutomatically -count=1`.

## [0.0.38.0] — 2026-05-04 — predict open FD exhaustion before outage

### Added

- **open FD predictive warnings**: GrainFS now samples process file descriptor usage, projects warning/critical threshold arrival, and records transition-only `fd_exhaustion_risk` incidents with operator next actions.
- **FD metrics**: added Prometheus gauges for open FD count, FD soft limit, used ratio, ETA by threshold, and open FD category breakdown.
- **FD watcher**: added a Unix FD provider, trend detector, polling watcher, and serve-time flags for thresholds, ETA window, recovery window, interval, and classification cap.

### Changed

- **zero-ops incidents**: resource warnings now use `ProofNotRequired` and resolve through `FactResolved` instead of repair receipt semantics.
- **dashboard**: incident rendering now shows readable cause labels and decision text for resource warnings without implying repair proof.
- **TODO tracking**: completed open-FD predictive warning work was removed from `TODOS.md`, leaving BadgerDB/goroutine warning follow-up open.

### Fixed

- **FD watcher resilience**: transient provider, detector, or incident sink errors no longer permanently stop FD monitoring; failed decisions are retried on later polls.
- **FD category metrics**: category gauges now clear missing categories between samples so stale labels do not keep showing old contributors.

### Tests

- Added reducer, metrics, detector, provider, watcher, serve wiring, dashboard static, and runbook coverage for the FD warning loop.
- Verified branch-relevant packages and dashboard e2e; full `go test ./...` still exposes pre-existing e2e failures tracked in `TODOS.md`.

## [0.0.37.0] — 2026-05-04 — add zero-ops incident control loop

### Added

- **zero-ops incidents**: missing shard repairs and corruption isolation now produce durable incidents with state, severity, scope, proof status, and next action.
- **incident API**: added `/api/incidents` and `/api/incidents/:id` for dashboard and operator tooling.
- **dashboard**: added a self-healing incident table that surfaces fixed, isolated, blocked, and proof-unavailable states.
- **incident store**: added a Badger-backed current-state store plus reducer, recorder, and reconciler components.

### Changed

- **cluster repair**: missing-shard repairs now record incident transitions and only attach signed proof after the HealReceipt is signed and persisted.
- **corruption handling**: corrupt shards now quarantine the affected object version instead of attempting unsafe repair.
- **placement monitor**: CRC mismatch detection now routes corrupt local shards into quarantine incidents instead of only logging the read error.

### Fixed

- **receipt API security**: `/api/receipts` remains behind S3 SigV4 auth when credentials are configured.
- **quarantine scope**: object quarantine keys include `versionID`, so isolating one bad version does not block newer versions or unrelated objects.
- **e2e receipt verification**: missing-shard incident tests now fetch the signed proof by receipt ID, matching receipt routing semantics.

### Tests

- Added reducer, recorder, reconciler, Badger store, incident API, incident repair, quarantine, placement-monitor corruption, dashboard, and multi-node e2e coverage.
- Verified signed receipt access, unauthenticated receipt rejection, missing-shard repair proof, corruption quarantine, and dashboard incident rendering.

## [0.0.36.0] — 2026-05-03 — cluster trust boundary + NFS input validation

Closes 2026-05-03 CSO security audit findings #1, #2, #3.

### Breaking

- **`--cluster-key` is now required** in cluster mode (`--peers` 또는 `--join` 지정 시).
  Solo mode는 영향 없음. 빈 문자열로 cluster mode 시작 시 fail-fast.
- **Cluster TLS authentication moved from ALPN-baked PSK hash to certificate
  SPKI pinning.** `--cluster-key`로부터 HKDF + ECDSA P-256으로 결정론적 클러스터
  identity cert를 도출하고, ALPN protocol 문자열은 정적(`grainfs`,
  `grainfs-mux-v1`) — TLS ClientHello에 PSK 정보 노출 안 됨. **혼합 버전 클러스터는
  인증 실패** — 업그레이드 시 모든 노드 동시 재시작.
- **NFSv4 OPEN/CREATE/REMOVE/RENAME** 이제 빈 문자열, `.`, `..` component name 거부
  (`NFS4ERR_INVAL`, RFC 7530 §6 준수). 이런 이름으로 probe하던 클라이언트는 이전에
  undefined behavior 또는 `NFS4ERR_NOENT`를 받았으나 이제 일관되게 `NFS4ERR_INVAL` 받음.
  Linux kernel client / macOS / busybox 등 주요 NFS 클라이언트로 사전 검증 권장.

### Security

- **Closes HIGH severity finding**: cluster mode가 `--cluster-key` 비어있을 때
  unauthenticated peer를 더 이상 받지 않음.
- **Closes MEDIUM severity finding**: cluster TLS 인증이 관찰 가능한 ALPN 채널
  (truncated 64-bit PSK hash)에서 SPKI pinning(개인키 보유 증명)으로 이동. ALPN은
  더 이상 PSK material을 carry하지 않음.
- **Closes MEDIUM severity finding**: NFSv4 component-name 검증이 모든 관련 op에서
  공유 helper(`validateComponentName`)로 일관되게 적용됨.

### Migration

1. 모든 cluster 노드 정지.
2. `--cluster-key`가 비어있거나 64 hex chars보다 짧다면:
   ```
   openssl rand -hex 32   # 32 random bytes = 64 hex chars (256-bit)
   ```
   기존 secret 채널로 모든 노드에 배포.
3. 모든 노드를 동일한 `--cluster-key`로 재시작.

### Trust Model (변경 없음, 명시적 문서화)

클러스터 identity는 `--cluster-key`로부터 결정론적으로 도출된 단일 공유 TLS
인증서이다. 모든 노드가 모든 노드를 동일하게 신뢰한다. per-node identity나
revocation은 없다. `--cluster-key` 누출 시 회전: 새 키 생성 + 모든 노드 재시작
(rolling rotation은 미지원, RUNBOOK 참조).

## [0.0.35.0] — 2026-05-03 — optimize NBD volume IO paths

### Added

- **cluster/NBD**: added cluster-mode NBD microbenchmarks for 4K and 64K writes, batched flushes, and 4K reads through the distributed backend.

### Changed

- **NBD**: 4K and 64K request buffers now reuse size-specific pools, reducing 64K read allocation pressure on the server hot path.
- **volume**: internal volume reads can use backend-approved `ReadAt` directly into caller buffers, avoiding full-object reads for local and cluster internal buckets.
- **storage**: cache and WAL wrappers now forward backend read/write IO preferences so fast paths survive wrapper composition.

### Fixed

- **NBD/local volume writes**: full-block internal writes now use the local partial-IO path instead of temp-file `PutObject` replacement, removing the per-block create/rename bottleneck.
- **cluster safety**: full-block write fast paths are now opt-in, so distributed backends keep using the replicated Raft/commit path instead of bypassing cluster durability semantics.

### Benchmarks

- **local NBD**: 4K writes improved from ~3.50 ms/op to ~38.8 us/op, and 64K writes from ~10.32 ms/op to ~705 us/op in the final standalone benchmark run.
- **cluster NBD**: added a repeatable baseline for cluster-mode NBD; the final standalone run measured 4K reads at ~24.7 us/op and 64K writes at ~2.69 ms/op.

## [0.0.34.0] — 2026-05-03 — make storage operations context-aware

### Added

- **cluster/storage**: added a P0 durability architecture plan for context-first storage boundaries before the partial IO, EC overwrite, durable MPU, and behavioral cancellation follow-up slices.
- **storage**: added `context.Context` to the core `storage.Backend` contract and introduced a context-aware `PartialIO` optional interface for efficient `WriteAt`, `ReadAt`, and `Truncate` paths.
- **tests**: added wrapper context pass-through coverage and compile-time interface assertions for the main storage and cluster backends.

### Changed

- **storage wrappers**: cache, WAL, swappable, recovery gate, packblob, and pull-through backends now forward caller contexts through the storage boundary.
- **cluster**: distributed backends, coordinators, forward receivers, scrubber surfaces, lifecycle workers, and migration paths now use context-aware storage calls.
- **protocols**: S3, NFSv4, VFS, volume, Iceberg, and migration call sites now pass request, service, command, or explicit operation-root contexts to storage.

### Fixed

- **NFSv4**: `READ` and `WRITE` fast paths now detect the new context-aware `storage.PartialIO` interface instead of falling back to full-object read/modify/write.
- **server**: multipart upload, copy object, browser form upload, lifecycle, and Iceberg metadata helpers now preserve the request context instead of creating fresh background contexts.
- **migration**: `migrate inject` now propagates command cancellation to destination storage operations.
- **cache**: cached backends now forward `Truncate` through the partial IO path and invalidate cached object state.

### Tests

- **storage/cluster/server/NFSv4**: verified context boundary compilation, wrapper forwarding, partial IO fast paths, server helper context propagation, and migration command context propagation.
- **e2e smoke**: verified the built `grainfs` binary with deployment and default-bucket startup smoke tests.

## [0.0.33.0] — 2026-05-03 — modernize NBD fixed newstyle protocol

### Added

- **NBD**: fixed newstyle negotiation now supports `OPT_INFO`, `OPT_GO`, `NBD_INFO_BLOCK_SIZE`, structured read replies, `base:allocation` block status, and `WRITE_ZEROES`.
- **NBD**: extended request headers now have a guarded parser while negotiation remains disabled until qemu/libnbd interop is proven.
- **tests**: added modern NBD protocol regressions, allocation-aware NBD benchmarks, and an optional qemu/libnbd interop smoke target.

### Changed

- **NBD**: request parsing, reply writing, and handshake state are split into focused protocol helpers while simple replies remain the default for existing Linux `nbd-client` paths.
- **NBD**: flush handling now orders deferred writes, trims, and zeroing mutations through a shared pending-mutation path.

### Fixed

- **NBD**: rejects unknown client flags, invalid metadata context negotiation, unsupported `FAST_ZERO`, and oversized request or option payloads before unsafe allocation.
- **NBD**: oversized writes now close the connection after the error reply so unread payload bytes cannot desynchronize the request stream.

### Tests

- **NBD**: covered `NO_ZEROES`, export validation, block-size info, structured replies, block status, zeroing read-back, disabled extended headers, metadata context replies, oversize reads/writes/options, and interop-tool availability.

## [0.0.32.0] — 2026-05-03 — close protocol contract operational gaps

### Added

- **cluster**: follower-routed object reads now stream large `GET` and `GET ?versionId=...` responses from the bucket owner instead of buffering the full body into a capped forwarded reply.
- **snapshot/PITR**: versioned bucket restore now preserves historical object versions and delete markers, so operators can recover the same version graph exposed by S3 versioning.
- **distribution**: root Docker builds now make the documented container build path usable.

### Changed

- **serve**: split storage, bootstrap, and observability helpers out of `cmd/grainfs/serve.go` so serve wiring is easier to review without changing runtime behavior.
- **docs**: README and runbook now document local Docker builds and the S3-only non-root default container profile.

### Fixed

- **WAL/PITR**: delete operations now retain delete marker version IDs when available, and PITR replay deletes only the targeted object version for `DeleteObjectVersion`.
- **docker**: multi-arch container builds now use Docker target platform arguments, so local buildx runs produce the target OS/arch binary.

### Tests

- **transport/cluster/snapshot**: added regressions for streamed read metadata/body handoff, caller-context detachment after metadata, forwarded large reads, version-aware snapshot restore, delete marker restore, and PITR version replay.

## [0.0.31.0] — 2026-05-03 — reduce NFSv4 allocation hot paths

### Changed

- **NFSv4**: RPC frame reads, XDR response writes, auth-body parsing, SEQUENCE parsing, GETATTR args, READ args, WRITE payload handling, and READ response encoding now reuse buffers or slice existing request data instead of copying on hot paths.
- **NFSv4**: file metadata sidecars, NFSv4.2 ALLOCATE truncation, and connection-local response buffers now avoid repeat object IO and transient allocations during common file operations.
- **cluster**: internal NFS object writes now cache object paths, metadata keys, directories, and sizes, and local shard leaders bypass peer resolution and shard-group slice copies.
- **metrics**: read amplification trackers now initialize lazily so disabled readamp telemetry does not allocate startup state.
- **benchmarks**: NFS profile scripts now expose protocol-version selection so NFSv4.0, v4.1, and v4.2 bottlenecks can be measured separately.
- **receipts**: receipt DB wiring now uses small Badger arena defaults to reduce idle memory pressure.

### Fixed

- **cluster**: routed `ReadAt` now rejects negative offsets before slicing fallback buffers, preventing malformed callers from crashing the coordinator path.

### Tests

- **NFSv4/cluster**: added allocation, buffer reuse, metadata cache, RPC auth skip, frame reuse, local routing, WAL `WriteAt`/`ReadAt`, truncation, and negative-offset regressions for the new hot paths.

## [0.0.30.0] — 2026-05-03 — harden protocol contract gaps

### Added

- **cluster**: forwarded operations now cover object version reads, deletes, and version listing so S3 versioning keeps the same semantics when requests route to remote bucket owners.
- **iceberg**: singleton-mode startup now migrates legacy Iceberg catalog rows into object-backed metadata and meta-Raft pointers before serving the REST catalog.
- **cluster**: `grainfs cluster join` now performs the real meta-Raft join path instead of printing a placeholder message.

### Fixed

- **compatibility**: new FlatBuffers forward-reply fields are append-only, preserving existing reply field slots for rolling upgrades.
- **iceberg**: legacy catalog migration treats both storage missing-bucket sentinels as an empty legacy catalog and remains idempotent after metadata objects already exist.

### Tests

- **cluster/iceberg**: added regressions for remote versioned forwarding, join CLI dispatch, legacy Iceberg export and migration, idempotent migration, and missing-bucket migration startup.

## [0.0.29.0] — 2026-05-03 — harden protocol layering over distributed object storage

### Added

- **architecture**: documented the protocol layering contract that keeps S3, NFSv4, NBD, and Iceberg REST Catalog above the shared distributed object storage backend.
- **tests**: added NBD and Iceberg cross-protocol e2e coverage proving protocol adapters route through the cluster coordinator, alongside existing S3/NFS paths.

### Changed

- **cluster**: bucket routing now switches to explicit assignment mode after shard-group bootstrap, so protocol traffic only reaches buckets with durable meta-Raft placement.
- **transport**: QUIC messages now carry request IDs and response status, and bulk forwarded body streams have their own traffic budget so object writes cannot starve meta/control calls.
- **iceberg**: docs now describe the table surface as Iceberg REST Catalog compatible, with metadata JSON stored as ordinary objects and catalog pointers kept in meta-Raft.

### Fixed

- **cluster**: dynamic join nodes now join meta-Raft before strict routing waits on shard-group visibility, preserving joined-node S3/Iceberg/NFS/NBD service startup.
- **cluster**: follower-routed bucket creation now waits for local bucket assignment visibility with a bounded timeout before returning, preventing immediate routed writes from seeing a missing bucket or hanging forever.

## [0.0.28.0] — 2026-05-03 — remove clustered S3 QUIC timeout cascade

### Changed

- **transport**: QUIC clients and listeners now use a shared config with a 4096 RPC stream cap, so concurrent cluster fan-out can open burst streams instead of stalling behind the old default limit.

### Fixed

- **cluster**: forwarded S3 operations now pass the per-call request deadline into QUIC calls instead of using the server lifetime context, so slow peers fail fast and the caller can retry normally.
- **transport**: caller-side `OpenStreamSync` deadline and cancel errors no longer evict healthy shared QUIC connections, preventing one timed-out request from cascading into reconnect churn for the whole node.

### Tests

- **cluster/transport**: added regressions for forwarded single-message and streamed-body timeout propagation, healthy connection reuse after caller deadlines, and burst RPC stream capacity.
- **benchmarks**: validated the fix with the 5-node S3 workload; the run improved from 84 requests with 22 failures to 298 requests with 0 failures, and PUT p99 dropped from roughly 20s to 28.67ms.

## [0.0.27.0] — 2026-05-03 — organize benchmark suite coverage

### Added

- **benchmarks**: single-node and multi-node targets now cover S3 object, Iceberg REST Catalog table API, NFS, and NBD workloads with matching action sequences for comparable runs.
- **benchmarks**: shared benchmark helpers now handle free ports, readiness waits, Colima checks, bucket/object readiness retries, pprof collection, and default-on encryption behavior.
- **make**: added direct benchmark targets for cluster S3, Iceberg table API, NFS, and NBD runs.

### Changed

- **benchmarks**: k6 wrappers now expose ramp-up and ramp-down controls, disable request rate limits during local measurements, and keep encryption enabled unless `NO_ENCRYPTION=1` is explicitly set.
- **benchmarks**: profile wrappers now collect pprof data from the writable target endpoint in clustered S3 and Iceberg runs.

### Fixed

- **serve**: `--rate-limit-*-rps 0` now truly disables per-IP and per-user request limiters, so benchmark scripts can measure storage behavior instead of limiter behavior.

### Tests

- **server**: added regression coverage for zero-RPS rate limit disabling at both limiter and server option levels.
- **benchmarks**: validated shell syntax for benchmark wrappers and smoke-tested S3 plus Iceberg single-node and cluster benchmark paths.

## [0.0.26.0] — 2026-05-03 — stabilize full cluster e2e suite

### Fixed

- **cluster**: routed object writes now reject forwarded replies whose metadata size does not match the transferred body, preventing empty objects from being accepted during leader/bootstrap races.
- **serve**: EC shard cache wiring now reaches every per-group backend, so repeated large-object reads hit the real shard cache instead of only reporting cache status on group-0.
- **server**: non-streaming PUT requests now reject mismatched `Content-Length` values instead of committing a truncated body.

### Tests

- **e2e**: full e2e now avoids ephemeral listener ports, waits for bucket write readiness, shuts down spawned processes cleanly, and disables background workers in isolated harnesses.
- **e2e**: MultiRaft persistence, EC shard-cache active, EC spike, backup, snapshot/PITR, pull-through, migration, erasure, and Jepsen paths now use deterministic readiness checks.
- **cluster**: added regressions for forwarded body-size mismatches and small-body single-message forwarding.

## [0.0.25.0] — 2026-05-03 — reduce migration coordination contention

### Changed

- **cluster**: migration coordination state now runs through a single actor, removing the nested mutex state around pending commits, early commits, done tracking, and TTL cancellation while keeping shard payload I/O outside the actor path.
- **receipt**: gossip routing lookups now use an immutable atomic snapshot with a receipt-to-node index, making dashboard receipt routing lookups O(1), lock-free, and allocation-free.

### Tests

- **cluster**: expanded migration executor coverage for actor lifecycle cleanup, early commit handling, duplicate execute suppression, TTL sweep behavior, and structured logging.
- **receipt**: added immutable snapshot and allocation regression coverage; `BenchmarkRoutingCache_Lookup` reports `0 B/op` and `0 allocs/op`.

## [0.0.24.0] — 2026-05-03 — reduce multi-node Badger heap overhead

### Changed

- **cluster**: small metadata BadgerDB instances now use a reduced memtable and block-cache budget, cutting the per-node Go heap reserved by 6-node idle clusters by roughly 75-84%.
- **raft**: raft log stores keep sync-write durability and single-version storage while sharing the smaller Badger options used for metadata-heavy stores.
- **serve**: metadata, shared raft-log, per-group state, and dedup databases now avoid Badger's large default arena sizing for tiny stores.

### Tests

- **badgerutil**: added option-level coverage proving the smaller Badger profile is applied and raft log durability settings are preserved.
- **profiling**: validated the fix with a real 6-node cluster profile; `skl.newArena` heap dropped from 1.16-1.91 GiB per node to 218-281 MiB.

## [0.0.23.0] — 2026-05-03 — remove raft idle batcher polling

### Fixed

- **raft**: idle batcher loops now block until the first proposal before arming a timer, removing the 100µs idle polling cycle that showed up as CPU overhead in quiet multi-node clusters.
- **raft**: batch collection now reuses a single timer and a fixed stack buffer, keeping the hot path allocation-free after the first proposal while preserving the existing lock-free proposal channel flow.

### Changed

- **profiling**: e2e profile reports now label `heap (MB)` as Go `runtime.MemStats.HeapAlloc`, not RSS and not the same view as pprof `inuse_space`.

### Tests

- **raft**: added a regression test proving an idle batcher does not keep allocating timers when no proposals arrive.
- **cluster**: validated the fix with a real 6-node idle profile run; CPU samples were 2.55% to 4.35% per node over 20s, and batcher timer allocation no longer appeared in CPU or allocation profiles.

## [0.0.22.0] — 2026-05-03 — dynamic cluster join and all-node service coverage

### Added

- **cluster**: `grainfs serve --join <raft-addr>` lets new nodes join an existing meta-Raft cluster without a static `--peers` list.
- **cluster**: node ID peer entries now resolve through meta-Raft membership, so dynamic join groups can route shard, S3, and Iceberg traffic to dialable node addresses.
- **transport**: reserved `StreamMetaJoin` for the dynamic join admin RPC.

### Changed

- **cluster**: default bucket startup creation is now singleton-only; clustered deployments treat bucket creation as shared metadata instead of having every node race to create it.
- **e2e**: dynamic join is now the default shared multi-node test harness, while static `--peers` tests are explicitly marked as legacy static topology coverage.
- **docs**: README and runbook now document `--join` as the normal way to add cluster nodes.

### Fixed

- **cluster**: joined nodes can forward bucket assignment, S3 object operations, and Iceberg catalog writes to the correct meta/data leader before they own local data groups.
- **cluster**: concurrent join requests for the same node ID are serialized so a second request cannot slip between voter addition and metadata registration with a different address.

### Tests

- **cluster**: added unit coverage for meta join forwarding, address book resolution, forwarding bucket assignment, node-address shard routing, and singleton default bucket startup policy.
- **e2e**: added dynamic join coverage for S3, Iceberg REST, NFSv4, and NBD across 2-node and 3-node clusters, plus default bucket and joined-node service checks.

## [0.0.21.0] — 2026-05-02 — harden cluster EC forwarding and shard integrity

### Added

- **cluster**: non-leader `PutObject` and `UploadPart` now stream request bodies to the group leader over QUIC, so large routed writes no longer hit the legacy 5 MiB single-message cap.
- **cluster**: `--reshard-interval` starts background EC reshard managers for local DataGroups by default, with `0` preserving the opt-out path.
- **storage**: new cluster EC shard writes use a versioned CRC envelope while legacy raw shards remain readable during rolling upgrades.

### Fixed

- **cluster**: streamed forward rejects now drain request bodies before replying, preventing sender-side QUIC stalls when leadership changes or a node is not a voter.
- **transport**: streamed body calls cancel QUIC read/write sides when their context expires, so timeout cleanup releases stream state promptly.
- **docs**: README and ROADMAP now describe the current cluster EC behavior instead of the old N-times replication/future-work wording.

### Tests

- **cluster/transport**: added coverage for forwarded large write streaming, forwarded large read replies, leader preflight, stream backpressure, rejected-body draining, CRC shard encoding, legacy raw fallback, corrupt CRC classification, and reshard manager startup defaults.

## [0.0.20.0] — 2026-05-02 — cluster-any-node Iceberg table API

### Fixed

- **iceberg**: DuckDB can now create, insert, read, and drop Iceberg tables through different cluster nodes in the same GrainFS cluster.
- **cluster**: Iceberg namespace and table reads on followers now route to the meta-Raft leader, so REST catalog clients see fresh metadata after writes.
- **cluster**: follower-routed object deletes now return the generated delete-marker version ID, fixing DuckDB commit cleanup when a request lands on a non-owner node.
- **server**: Iceberg transaction commits now reuse the table state committed earlier in the same transaction request, avoiding stale self-conflict checks.
- **server**: Iceberg snapshot requirement checks now tolerate DuckDB's rounded large JSON snapshot IDs.

### Tests

- **e2e**: added a DuckDB cluster test that creates the warehouse bucket, writes through node 0, appends through node 1, reads and drops through node 2.
- **server/cluster**: added focused regression coverage for Iceberg auth routing, transaction state reuse, rounded snapshot IDs, and forwarded delete markers.

## [0.0.19.0] — 2026-05-02 — meta-raft mux integration

### Added

- **raft**: meta-raft RPCs now ride the same persistent-stream mux as per-group raft. Sender prefixes payloads with the magic groupID `__meta__`; receiver dispatches via `handleMuxRequest` (direct calls) and `dispatchToLocalGroup` (coalesced heartbeats). Wire-compatible with v0.0.16+ (no frame-format change). Snapshots stay on the legacy StreamMetaRaft path — large-payload + 60s timeout don't fit the per-frame model.
- **raft**: `NewMetaRaftQUICTransportMux(tr, node, groupMux)` — mux-aware constructor that auto-registers `node` under `__meta__` so receiver-side dispatch is wired before `EnableMux` installs the mux accept handler. Closes the startup race (`MetaRaftQUICTransport` previously had to be created strictly after `EnableMux`).
- **raft**: `ValidateGroupID` rejects empty IDs, `__meta__`, and the entire `__` prefix. Called at four boundaries: `MetaFSM.applyPutShardGroup` (warn + skip on replay so old logs don't crash startup), `MetaRaft.ProposeShardGroup` (hard error to proposer), `instantiateLocalGroup` (fatal — voter-status fail), `GroupRaftQUICMux.Register` (panic — programming bug if upstream validation skipped).

### Changed

- **raft**: `MetaRaftQUICTransport` mux path uses a 200ms attempt budget; on dial / send / "unknown group" remote / ctx-deadline failure it falls back to legacy `tr.Call(StreamMetaRaft)` with a fresh 500ms ctx. The fresh ctx prevents the original mux-attempt deadline from immediately firing the legacy call too. Mixed-version clusters (v0.0.18.x receivers without `__meta__` registered) take the unknown-group fallback automatically.
- **serve**: startup order rearranged — `groupRaftMux` is now built and `EnableMux`-d before `NewMetaTransportQUICMux`, replacing the previous late-bind. The meta-raft constructor receives the mux directly and auto-registers.

### Why

`/plan-eng-review` of meta-raft mux integration surfaced 7 codex findings: missing `dispatchToLocalGroup` branch (P0 — silent meta heartbeat drop), `__meta__` ID not reserved (P0 — user config could collide with mux dispatch), startup-order race (P1), legacy-fallback context budget could be exhausted by a half-open mux call (P1), `HeartbeatCoalescer` reply race in R+H itself (P1, shipped separately as v0.0.18.1 / #140), mixed-version peers never recovering from "unknown group" responses (P1), meta RPC constants accidentally aliased with group constants (P2). All seven are addressed in this PR.

### Tests

- **raft**: added `group_id_test.go` (validation rules), `group_transport_mux_meta_test.go` (lookupNode meta path, RegisterMetaNode idempotency, Register panic on reserved IDs, isMuxFallbackErr decision matrix, NewMetaRaftQUICTransportMux auto-register).
- **cluster**: added `group_id_validation_test.go` (`MetaFSM` warn-and-skip, `ProposeShardGroup` hard error, `instantiateLocalGroup` fatal). Added `meta_raft_mux_e2e_test.go` — three-node meta-raft on the shared mux, leader election + state replication via heartbeat-coalesced AE.
- All existing raft + cluster + transport tests continue to pass.

## [0.0.18.1] — 2026-05-02 — close HeartbeatCoalescer reply race

### Fixed

- **raft**: `HeartbeatCoalescer.flush()` now registers the inflight batch in `inFlight` BEFORE the frame is sent. The previous order (send → store) let a fast receiver reply before the corrID was looked-up-able, so `DispatchReplyBatch` silently dropped the reply and the caller hung until ctx timeout. Group raft 200ms hb / 1s election timing masked it; surfaced as a P1 finding during `/plan-eng-review` for meta-raft mux integration (which would have run on 150/750 timing where this is election-fatal).

### Changed

- **raft**: `CoalescerSender` interface split — `NextHeartbeatCorrID()` and `SendHeartbeatBatchWithCorrID(corrID, payload)` replace `SendHeartbeatBatch(payload) (corrID, err)`. Lets callers reserve the corrID, register inflight, then send. Internal interface only — no external API impact.

### Tests

- **raft**: added `TestCoalescer_FastReplyBeforeRegister`. The `fakeSender` `onSend` hook synchronously dispatches a reply during the wire-side send. With the fix the reply lands on a registered entry; without it the caller would time out, providing a regression guard if the order is ever inverted again.

### Why

The race was found by codex outside-voice during `/plan-eng-review` for the meta-raft mux follow-up (see `docs/architecture/quic-stream-multiplex.md` §Follow-up). Splitting it out as a precursor PR isolates the perf delta of the upcoming meta-mux work and lands the fix on group raft now (where it is a latent bug that may already contribute to e2e flake under host contention).

## [0.0.18.0] — 2026-05-02 — cluster-safe Iceberg REST catalog

### Added

- **iceberg**: `/iceberg/*` now works in clustered mode with namespace and table metadata pointers replicated through meta-Raft, so clients can use the table API against cluster nodes instead of relying on node-local catalog state.
- **cluster**: added meta-Raft Iceberg catalog commands, typed apply results, snapshot/restore support, and follower-to-leader proposal forwarding for catalog writes.
- **tests**: added coverage for MetaCatalog leader operations, follower forwarding, pointer-only snapshots, typed conflict propagation, commit CAS, delete paths, and server catalog injection.

### Changed

- **server**: Iceberg handlers now depend on a catalog interface, preserving the local Badger catalog for standalone mode while wiring the replicated MetaCatalog in cluster mode.
- **storage**: table metadata JSON stays in object storage; meta-Raft stores only namespace/table pointers and properties.

## [0.0.17.0] — 2026-05-02 — flip --quic-mux default to true

### Changed

- **serve**: `--quic-mux` default flipped from `false` to `true`. Per-group raft RPCs now route through the persistent stream + heartbeat coalescer path by default. ALPN-based fallback to the legacy per-message path remains in place for cross-version cluster bring-up.

### Why

idle-N8 e2e measurement (5 nodes × 8 raft groups, shared raft-log BadgerDB):

| Metric | mux=off | mux=on | Δ |
|--------|--------:|-------:|---:|
| CPU samples (30s pprof) | 25.86s | 5.60s | **−78%** |
| `syscall.rawsyscall` (recvmsg) | 35% | 2% | **−17x** |
| `runtime.kevent` absolute time | 5.04s | 0.45s | **−91%** |
| Wall-clock CPU% | 26.0 | 22.9 | −12% |
| RSS | 278 MB | 484 MB | +74% (frame buffers, handler pool) |
| Goroutines | 241 | 329 | +37% (pool=4 readers per peer × 4 peers, mostly parked) |

The dominant cost on the legacy path was per-message `OpenStreamSync` + `Close` driving kqueue wake-ups on every raft heartbeat (8 groups × 50ms heartbeat × 4 peers ≈ 640 stream open/close per second). Persistent stream + per-peer heartbeat coalescing collapses that to a small constant.

### Rollout & rollback

- Cluster bring-up: an older binary (no mux ALPN) co-existing with this binary keeps working — newer node falls back to legacy `Call` for that peer until the older node is upgraded. No flag-day required.
- Roll-back: `--quic-mux=false` on every node restores the legacy path. Wire-format compatible (different ALPN; legacy ALPN still advertised).
- Tunables: `--quic-mux-pool` (default 4) sizes the per-peer stream pool. `--quic-mux-flush` (default 2ms) is the heartbeat coalescing window.

### Notes

- Meta-raft transport still uses the legacy per-message path; ~4% of cluster RPC traffic. Mux integration deferred until measurement justifies the additional refactor (needs a meta-vs-group discriminator inside the mux frame).
- Snapshot install paths (`internal/raft/quic_rpc.go`, `internal/raft/meta_transport_quic.go`) intentionally bypass mux — large payloads continue on dedicated per-message streams.
- load-N8 / load-N16 mux=on validation deferred until host contention + e2e bucket-replication race in `tests/e2e/cluster_perf_profile_test.go:232` are addressed (independent of this change).

## [0.0.16.1] — 2026-05-02 — fix mux-enabled nil panic on early conn close

### Fixed

- **raft**: `GroupRaftQUICMux.muxConnFor` panicked with nil pointer dereference inside `HeartbeatCoalescer.FailAll` when `OpenOutboundStreams` failed. The OnBroken closure dereferenced `ps.hc`, but the coalescer was attached only AFTER stream open succeeded — so `rc.Close()` on the failure path saw `ps.hc == nil`. Caught running load-N8 e2e with `--quic-mux=true` (host under contention, RaftConn open path tripped). Fix: attach the coalescer before `OpenOutboundStreams` and nil-guard the `OnBroken` / `HBReplyHandler` closures. Regression test added.

### Notes

- Mux mode (`--quic-mux=true`) only triggered this on early conn-close failure; default-off deployments are unaffected.
- Validation measurement (idle-N8) confirms mux delivers ~78% reduction in CPU samples and 17x drop in `syscall.rawsyscall` (recvmsg) versus default-off baseline. Default flip pending clean load-N8 measurement.

## [0.0.16.0] — 2026-05-02 — QUIC stream-reuse R+H prototype + raft heartbeat tuning

### Added

- **raft**: `RaftConn` (`internal/raft/raft_conn.go`) — persistent QUIC stream pool per peer for raft RPCs, replacing per-message `OpenStreamSync`/`Close`. Frame `[length|OP|EC|corrID|payload]`, bounded handler pool, conn-level corrID pending map, `opStreamInit` visibility handshake.
- **raft**: `HeartbeatCoalescer` (`internal/raft/heartbeat_coalescer.go`) — per-peer batching of entries-empty `AppendEntries` into `opHeartbeatBatch`; replies fan back to synchronous callers via per-group reply channels. Entries-bearing AE bypasses coalescer.
- **transport**: ALPN routing — listener advertises both `grainfs-<pskhash>` (legacy) and `grainfs-mux-v1-<pskhash>` (mux). Inbound conns dispatch by negotiated ALPN. PSK preserved.
- **transport**: `GetOrConnectMux` / `EvictMux` / `SetMuxConnHandler` for mux conn lifecycle.
- **raft**: `GroupRaftQUICMux.EnableMux(pool, flushWindow)` — opt-in mux mode for per-group raft. Falls back to legacy if mux dial fails.
- **serve**: `--quic-mux` (default off), `--quic-mux-pool` (default 4), `--quic-mux-flush` (default 2ms), `--raft-heartbeat-interval` (default 200ms), `--raft-election-timeout` (default 1s) flags.
- **tests**: 23 new unit tests covering RaftConn paths, ALPN routing, HeartbeatCoalescer, and group-mux integration.

### Changed

- **raft**: per-group raft default heartbeat 50ms→200ms, election 150ms→1s. Meta-raft timeouts unchanged.

### Notes

- Mux mode is a measurement prototype, default off until idle-N8 / load-N8 CPU data validates the predicted drop.
- Meta-raft transport stays on legacy path this PR (~4% of cluster RPC traffic; needs meta-vs-group discriminator inside the mux frame). Follow-up after measurement.
- Snapshot install paths bypass mux — large payloads continue to use dedicated per-message streams.

## [0.0.15.0] — 2026-05-02 — DuckDB Iceberg Table API completion

### Added

- **server**: DuckDB can now commit table updates through the table-scoped Iceberg REST endpoint and drop tables/namespaces through the REST catalog path.
- **tests/e2e**: added an embedded DuckDB e2e target that creates, writes, restarts, reads, and drops an Iceberg table against GrainFS.
- **docs**: documented `make test-e2e-iceberg` so the DuckDB catalog smoke can be rerun without a separate CLI setup.

### Fixed

- **icebergcatalog**: namespace deletion now returns a typed conflict when tables still exist, then succeeds after the table is dropped.
- **todos**: clarified that multi-peer leader/follower Iceberg API support still requires meta-Raft replicated catalog state, not node-local Badger state.

## [0.0.14.0] — 2026-05-02 — DuckDB Iceberg REST Catalog

### Added

- **server**: `/iceberg/v1` now exposes a DuckDB-compatible Iceberg REST Catalog for local single-node GrainFS, including config, namespace, table, and transaction commit endpoints.
- **icebergcatalog**: added a Badger-backed namespace/table catalog with metadata pointer compare-and-swap so stale table commits fail instead of overwriting current state.
- **serve**: local `grainfs serve` wires the Iceberg catalog onto the existing meta BadgerDB while multi-peer cluster mode keeps `/iceberg/*` disabled with typed JSON `NotImplementedException` responses.
- **docs**: added DuckDB attach and smoke-test instructions plus a captured DuckDB 1.5.2 request trace for catalog create flows.

### Fixed

- **server**: `/iceberg/*` now stays on an Iceberg JSON error boundary instead of falling through into S3 XML/object route behavior for unsupported catalog operations.
- **server**: Iceberg transaction commits now validate snapshot requirements and write metadata JSON versions into the warehouse bucket as ordinary GrainFS objects.

## [0.0.13.0] — 2026-05-02 — BadgerDB compactor reduction + shared raft-log prototype

### Added

- **raft**: `OpenSharedLogStore` enables a single per-node BadgerDB to host raft logs for all groups via 4-byte big-endian length-prefixed keys (`group_id` namespace), reducing per-node BadgerDB instances when many raft groups are active.
- **serve**: `--shared-badger` flag (default `true`) opts into the shared raft-log layout; refuses to start when legacy per-group `groups/*/raft/` directories are present so deployments don't silently lose state.
- **tests**: prefix-isolation, validation, pathological-groupID, concurrent-open, and restart-persistence unit tests for `OpenSharedLogStore`.
- **perf**: `tests/e2e/cluster_perf_profile_test.go` harness for 5-node cluster CPU/heap/goroutine measurement across N={8,16,32,64} groups, with idle/load scenarios and per-node stdout capture.
- **scripts**: `scripts/p0b-auto-measure.sh` — host-aware periodic driver that runs the P0b sweep only when load and foreign processes are below thresholds.
- **docs**: `docs/architecture/badger-consolidation.md` design doc covering C1 (compactor pool) + C2 (state and raft-log DB consolidation) phases.

### Changed

- **cluster**: per-group state BadgerDB now opens with `WithNumCompactors(2)` (down from default 4) to reduce idle CPU and goroutines from BadgerDB housekeeping.

## [0.0.12.0] — 2026-05-02 — Raft AppendEntries pipelining

### Added

- **raft**: leaders now keep peer-owned AppendEntries replicators for voters and learners, allowing bounded per-peer replication pipelines instead of one in-flight AppendEntries at a time.
- **raft**: added `MaxAppendEntriesInflight` and `MaxAppendEntriesInflightBytes` controls so operators can bound replication window count and payload memory per peer.
- **metrics**: added counters for stale AppendEntries replies, conflict resets, full pipeline windows, and snapshot-exclusive replication paths.
- **benchmarks**: added in-process and QUIC replication window benchmarks for comparing AppendEntries pipeline sizes.

### Fixed

- **raft**: out-of-order AppendEntries replies now advance `matchIndex` only through contiguous acknowledged ranges, preserving leader commit safety while pipelining is enabled.
- **raft**: conflict and heartbeat failure replies now reset the peer pipeline and backtrack `nextIndex`, keeping learner catch-up and slow-follower recovery moving.
- **raft**: snapshot install remains exclusive per peer, and snapshot errors clear the exclusive state so normal replication can retry.
- **tests**: added regression coverage for out-of-order acknowledgements, stale replies, byte budgets, membership reconciliation, heartbeat conflict backtracking, higher-term stepdown, and oversized single-entry byte-budget progress.

## [0.0.11.1] — 2026-05-02 — E2E cluster readiness hardening

### Fixed

- **tests/e2e**: degraded-mode coverage now starts all peers with the full cluster view, waits for every port, and probes a writable endpoint before killing quorum members.
- **tests/e2e**: network partition coverage now forces IPv4 loopback through Toxiproxy, creates a valid timeout toxic, and asserts partitioned writes actually fail.
- **tests/e2e**: multi-node cluster tests now use Raft addresses as node IDs, matching the stricter gossip sender validation introduced on master.
- **tests/e2e**: EC put/get and topology checks now retry bounded startup data-path readiness races instead of failing on transient unknown-group errors.
- **tests/e2e**: topology-change coverage now bootstraps an initial quorum before adding the remaining peers, reducing cold-start election races.
- **tests/e2e**: multi-Raft sharding coverage now bootstraps an initial quorum before adding the remaining peers, matching the stabilized EC topology startup path.
- **tests/e2e**: multi-Raft write checks now retry transient degraded-mode startup windows before asserting object persistence.
- **cluster**: zero-valued EC shard settings now keep EC inactive, allowing non-EC cluster tests and operators to disable EC without tripping degraded-mode write suspension.
- **cmd/grainfs**: NBD startup now retries transient default-volume creation failures while shard groups are still coming online.
- **tests/e2e**: Docker NBD CoW cleanup now ignores already-removed temporary files after the rollback scenario has passed.
- **tests/e2e**: volume listing now asserts the volumes it creates instead of depending on asynchronous default NBD volume creation.
- **tests/e2e**: package-level e2e server startup now waits longer before failing, matching cold-start latency under serialized Docker-heavy runs.

## [0.0.11.0] — 2026-05-02 — Raft membership view completion

### Changed

- **raft**: membership view snapshots now use explicit voter and learner field names, making single-mode and joint-consensus quorum reads easier to audit.
- **raft**: config-change application now publishes a new membership view only when membership actually changes, so no-op entries do not disrupt pending ReadIndex work.
- **raft**: ReadIndex waiters now fail fast when a real membership change publishes, forcing callers to retry against the new quorum view instead of completing on stale membership.

### Fixed

- **raft**: snapshot install, joint-state restore, self-removal, quorum watermark, and read-index paths now share the same coherent membership-view boundary.
- **tests**: added regression coverage for no-op membership changes, ReadIndex waiter draining, captured ReadIndex membership, snapshot install publication, live joint transitions, and membership-view hot-path benchmarks.

## [0.0.10.0] — 2026-05-02 — Raft membership view hardening

### Changed

- **raft**: quorum, election, ReadIndex, and membership observation paths now read from a single immutable membership snapshot, preventing mixed old/new voter views during joint consensus transitions.
- **raft**: membership mutations now publish a fresh read view after config changes, joint changes, snapshot restore/install, log rebuild, and commit-time self-removal updates.

### Removed

- **raft**: retired the older mutex-bound quorum helper path so all quorum decisions share one membership-view implementation.

### Fixed

- **tests**: added regression coverage for immutable membership snapshots, no mixed joint state, JointSnapshotState consistency, self-removal republish, captured election membership, CheckQuorum, JointLeave commit advancement, and hot read allocation behavior.

## [0.0.9.0] — 2026-05-02 — Offline RecoverCluster drill

### Added

- **recover**: `grainfs recover cluster plan`, `execute`, and `verify --mark-writable` commands for promoting an offline Raft snapshot into a fresh single-node cluster.
- **cluster**: RecoverCluster planning and execution flow that inspects source Raft state read-only, refuses multi-Raft group recovery in v1, rewrites snapshot membership to one recovered voter, and writes a recovery marker with writes disabled by default.
- **storage**: recovery write gate that keeps recovered clusters read-only until verification while preserving read/list/head and policy read paths.
- **docs**: operator drill guide for RecoverCluster, including source/target safety rules and the writable verification step.
- **tests**: coverage for recovery planning, marker handling, joint-state refusal, managed-mode mismatch, write-gate mutation blocking, and stopped Raft RPCs after close.

### Changed

- **recover**: legacy `recover --auto` now fails closed instead of attempting unsafe automatic snapshot recovery, and doctor/SLO guidance now points operators to dry-run and RecoverCluster planning.

### Fixed

- **raft**: stopped nodes now reject late RequestVote, AppendEntries, and InstallSnapshot RPCs before touching persistent storage, preventing closed-store panics during shutdown races.

## [0.0.8.1] — 2026-05-02 — E2E cluster startup hardening

### Fixed

- **tests/e2e**: `make test-e2e` now runs e2e tests one at a time with `-p 1` and `-parallel 1`, reducing cross-test cluster and port interference.
- **tests/e2e**: EC, topology, scale-bench, NFSv4, and NBD test nodes now use explicit free-port pools instead of disabling protocols or racing ad hoc port allocation.
- **cluster**: bucket deletion now checks routed data groups before deleting base bucket metadata, preserving S3 `BucketNotEmpty` semantics for multi-raft routed objects.
- **cmd/grainfs**: cluster-mode default bucket creation no longer blocks HTTP readiness during simultaneous peer startup.
- **raft**: Meta-Raft QUIC timing and election defaults were relaxed for multi-node cold starts on loaded e2e hosts.

## [0.0.8.0] — 2026-05-02 — Raft snapshot admin trigger

### Added

- **raft**: manual snapshot trigger primitive with status reporting for the latest snapshot index, term, and size.
- **server**: localhost-only `/admin/raft/snapshot` GET/POST API for checking snapshot status and forcing a leader-side snapshot.
- **metrics**: snapshot trigger counters and last snapshot index/size gauges for operator visibility.
- **tests**: unit and e2e coverage for the admin trigger, status response, metrics, follower rejection, unavailable snapshotter path, and apply-loop serialization.

### Fixed

- **cluster**: manual snapshot triggers now run through the Raft apply loop, so snapshot creation reads a consistent applied index/term without racing normal apply progress.

## [0.0.7.8] — 2026-05-02 — Shared EC placement resolver + shard repair wiring

### Added

- **cluster**: shared placement resolver that resolves EC shard placement in priority order: ring metadata, object metadata `NodeIDs`, then legacy placement records.
- **cluster**: shard placement monitor metadata scan path, so metadata-only EC objects are included in missing-shard detection and repair.
- **cmd/grainfs**: placement monitor registry for per-data-group monitors, including backend replacement and removal lifecycle handling.
- **tests**: regression coverage for metadata-only placement, legacy bare-key fallback, reshard classification, monitor repair detection, and data-group monitor replacement.

### Fixed

- **cluster**: `GetObject` and `ReshardToRing` now read EC shards using the resolver's physical shard key, preserving legacy bare-key fallback for versioned metadata.
- **cluster**: `RepairShard`, scrubber ownership, reshard conversion, and monitor scans now share one placement resolution path instead of drifting fallback logic.
- **cmd/grainfs**: cluster scrubber repair now starts placement monitors for routed data-group backends, enabling auto-repair in live multi-raft sharding mode.

## [0.0.7.7] — 2026-05-01 — Raft joint chaos hardening + e2e port isolation

### Added

- **raft**: chaos coverage for joint-consensus partition/abort behavior, including managed learner cleanup and C_old voter preservation.
- **cluster**: `ClusterCoordinator` snapshot/PITR coverage for listing and restoring bucket-routed group objects.

### Fixed

- **raft**: joint operation result channels are drained on stop or leadership loss, preventing blocked membership-change callers.
- **raft**: stale old-term `JointLeave` entries and aborted in-flight leaves no longer confuse joint abort/leave detection.
- **cluster**: object snapshots now enumerate data-group objects through `ClusterCoordinator` routing, and restores remove extra group objects instead of delegating to empty base metadata.
- **cmd/grainfs**: cluster mode now wraps the `ClusterCoordinator` with WAL, so routed object mutations are recorded for PITR replay.
- **tests/e2e**: all S3, NFSv4, NBD, and pprof ports used by e2e processes are allocated via `freePort()` to avoid cross-test port collisions.

## [0.0.7.6] — 2026-05-01 — Linearizable ReadIndex reads for S3 and NBD

### Added

- **raft**: `ReadIndex` and `WaitApplied` read fences for serving linearizable reads after the local FSM has applied the leader-confirmed commit index.
- **cluster**: follower read forwarding over the new `StreamReadIndex (0x0A)` QUIC RPC, so followers can ask the leader for a safe read index instead of rejecting read traffic.
- **server**: S3 `GET` and `HEAD` object handlers now wait behind the ReadIndex fence in clustered mode before returning object data or metadata.
- **nbd**: NBD reads now use the same ReadIndex fence in clustered mode, keeping block reads aligned with committed Raft state.
- **tests**: coverage for ReadIndex quorum behavior, single-node ReadIndex fast path, WaitApplied wakeups, S3 read-fence wiring, and NBD read-fence success and failure paths.

### Fixed

- **raft**: single-node leaders now return their current commit index immediately from `ReadIndex` instead of waiting for peer acknowledgements that can never arrive.

## [0.0.7.5] — 2026-05-01 — AbortPlanReason enum + ConfChangeEntry cleanup (PR-D must-fix + PR-A)

### Added

- **cluster**: `AbortPlanReason` enum (`Unknown`, `Timeout`, `ExecutionFailed`, `Completed`) added to `MetaAbortPlanCmd` FlatBuffers message. `ProposeAbortPlan` now carries the reason so operators can distinguish plan abort causes in logs and future metrics.

### Changed

- **cluster**: `MetaRaft.ProposeAbortPlan`, `MetaRaftClient` interface, and all `Rebalancer` callers updated to pass `AbortPlanReason` — timeout aborts use `Timeout`, `MoveReplica` failures use `ExecutionFailed`, successful plan completion uses `Completed`.

### Removed

- **raft**: `ConfChangeEntry.new_config` and `ConfChangeEntry.old_config` FlatBuffers fields removed. These were deprecated in PR-J1 (superseded by `JointConfChangeEntry`) and are now safe to drop after the mixed-version compatibility window.

## [0.0.7.4] — 2026-05-01 — Live Multi-Raft Routing E2E (PR-D 완료)

### Added

- **cluster**: `ForwardReceiver` — server-side `StreamProposeGroupForward (0x08)` handler. 10 operation dispatchers (PutObject, GetObject, HeadObject, DeleteObject, ListObjects, WalkObjects, CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload) to `GroupBackend` with leader check + NotLeader hint. Errors mapped to `ForwardStatus` codes.
- **cluster**: `forward_receiver_test.go` — unit tests for ForwardReceiver (unknown group → NotVoter, non-leader voter → NotLeader with hint).
- **raft**: `GroupRaftQUICMux` — per-group Raft QUIC multiplexer over `StreamGroupRaft (0x09)`. Wire prefix: `[4B groupIDLen][groupID][Raft RPC FlatBuffers payload]`. Registered once in serve.go; each group uses `ForGroup(groupID)` as its Raft transport.
- **cmd/grainfs**: serve.go wiring — ClusterCoordinator wired into S3 server startup with ForwardSender dialer + ForwardReceiver registration on ShardService.
- **tests/e2e**: multiraft_sharding_test.go — t.Skip removed from BucketAssignment + RestartRecovery (data-plane routing now enables auto-redirect). 4 new tests: PerGroupPersistence, CrossNodeDispatch, GroupLeaderFailover, NFSv4Smoke (Linux-only).
- **internal/cluster**: wire_coexistence_test.go — REGRESSION test verifying legacy `StreamProposeForward (0x06)` and new `StreamProposeGroupForward (0x08)` handlers can coexist without interference (4 subtests).

### Fixed

- **cluster**: `GroupBackend.selfAddr` was initialized from `PeerIDs[0]` (alphabetically smallest peer) rather than the local node ID. `instantiateLocalGroup` now reorders PeerIDs so self is first; `NewGroupBackend` explicitly sets `selfAddr = cfg.Node.ID()`. Fixes WriteShard self-skip and ReadShard fallback after leader failover.
- **cluster**: `MoveReplica` self-removal guard — when `fromNode == localNodeID`, calls `TransferLeadership` and returns `ErrLeadershipTransferred` so the new leader retries the migration.
- **cluster**: `CreateBucket` falls back to `router.RouteKey` when `HashAssign` returns empty.

### Changed

- **cluster**: `ClusterCoordinator.WithForwardSender(forwardSender)` setter for deferred ForwardSender injection (serve.go startup pattern).

### Technical Notes

- Rolling upgrade safe: legacy 0x06 wire unchanged; new 0x08 uses independent stream type.

## [0.0.7.3] — 2026-05-01 — Raft joint P2 correctness fixes + LearnerIDs API

### Added

- **raft**: `ErrLeadershipLost` — new sentinel error returned to `ChangeMembership` callers when a natural Leader→Follower transition occurs while a joint operation is in progress.
- **raft**: `LearnerIDs() []string` — public API on `*Node` returning a snapshot of current learner node IDs, safe for concurrent access.

### Fixed

- **raft**: `JointOpAbort` apply now removes managed learner IDs from `learnerIDs` before clearing `jointManagedLearners`. Without this fix, `checkLearnerCatchup` would treat managed learners as ordinary learners after an abort and attempt spurious auto-promotion into the wrong voter set.
- **raft**: `runFollower()` now drains `jointResultCh` with `ErrLeadershipLost` at entry, unblocking any `ChangeMembership` goroutine blocked in `proposeJointConfChangeWait` during a natural Leader→Follower transition. Prevents goroutine leak when leadership is lost without `StopNode`.

## [0.0.7.1] — 2026-05-01 — Raft managed_by_joint persistence (PR-K3)

### Added

- **raft**: `jointManagedLearners` state now survives process restart. Learners added by `ChangeMembership` are tagged `ManagedByJoint=true` in the ConfChange log entry; on restart the leader replays the log (or loads the snapshot) and rebuilds the guard set that blocks premature auto-promotion of these learners during the joint-consensus window (Guard 2 in `checkLearnerCatchup`).
- **raft**: Snapshot carries `JointManagedLearners []string` field (FlatBuffers `SnapshotMeta.joint_managed_learners`). Nodes restored from a snapshot have Guard 2 active without needing full log replay.
- **raft**: `restoreFromStore` now replays ConfChange entries from the full log even when no snapshot has been taken yet (fresh cluster), closing the gap between first `ChangeMembership` call and first snapshot.

### Fixed

- **raft**: `marshalLogEntry` was not persisting the `entry_type` field. All log entries loaded from BadgerDB were deserialized as `LogEntryCommand` (type 0), causing `rebuildConfigFromLog` to silently skip ConfChange and JointConfChange entries on restart. The field is now written correctly; FlatBuffers' zero-default maintains backward read compatibility with existing entries.
- **raft**: `restoreFromStore` no-snapshot replay branch now correctly guards against `LoadSnapshot` errors with `snapErr == nil`.

## [0.0.7.0] — 2026-04-30 — Live Multi-Raft Sharding (PR-C 데이터 plane 라우팅)

### Added

- **cluster**: `ClusterCoordinator` — `storage.Backend` implementation routing bucket-scoped ops to per-group Raft leaders. 4 cluster-wide ops (CreateBucket, HeadBucket, DeleteBucket, ListBuckets) delegate to base `DistributedBackend`; 10 bucket-scoped ops (GetObject, HeadObject, DeleteObject, ListObjects, WalkObjects, CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, PutObject) route via `Router.RouteKey` → `ForwardSender.Send` over QUIC stream `0x08`. spec: `docs/superpowers/specs/2026-04-30-live-multi-raft-sharding-design.md`.
- **cluster**: `ForwardSender` — client-side single-shot Send with try-each-peer reliability (peers attempted in order, self last) and NotLeader hint redirect (1× round-trip). Returns `ErrNoReachablePeer` after all peers exhausted.
- **cluster**: `forward_codec.go` — FlatBuffers args builders (10 ops) + reply parsers (`objectFromReply`, `objectsFromReply`, `uploadFromReply`, `partFromReply`, `parseReplyStatus`). Body-bearing ops (PutObject, UploadPart) embed bytes ≤5MB inside FBS args — single-message wire model, no chunked streaming.
- **cluster**: `ClusterCoordinator.PutObject` / `UploadPart` — 5MB hard cap enforcement via `io.ReadAll(io.LimitReader(r, maxBody+1))`. Returns `storage.ErrEntityTooLarge` on overflow.
- **cluster**: `routeTarget` — self-leader shortcut optimization. When self is a voter AND local GroupBackend's `RaftNode().IsLeader()` returns true, ops call backend directly bypassing QUIC wire.
- **raft/raftpb**: `forward_cmd.fbs` + generated bindings — FlatBuffers schema for 10 op-arg structs + `ForwardOp` enum + `ForwardReply` envelope with `ForwardObjectMeta`, `ForwardMultipartUploadMeta`, `ForwardPartMeta`, `objects` vector, `read_body` bytes.

### Changed

- **cluster**: `DataGroup.GroupForBucket(bucket, router)` helper — bucket → DataGroup lookup via router. Returns (nil, false) if router nil or no assignment.
- **cluster**: `lookupForwardTarget` → `PeersForForward(entry, selfID)` — returns attempt-ordered peer list (voters first, leader first, self last) for try-each-peer pattern. Used by `ClusterCoordinator.routeBucket`.

### Technical Notes

- ForwardReceiver implementation (server-side 0x08 handler) deferred to v0.0.7.1 PR-D.
- serve.go wiring (ClusterCoordinator + ForwardSender + ForwardReceiver registration) deferred to v0.0.7.1.
- e2e tests, wire coexistence REGRESSION test, perf benchmarks deferred to v0.0.7.1.

## [0.0.6.23] — 2026-04-30 — MoveReplica → ChangeMembership (Sub-project 3 PR-K2)

### Changed

- **cluster**: `DataGroupPlanExecutor.MoveReplica` now uses a single §4.3 atomic `ChangeMembership` call instead of the 5-step §4.4 sequence (AddLearner → catch-up → PromoteToVoter → RemoveVoter). Self-removal handled by joint commit-time step-down hook in `raft.Node` (jointPromoteCh closes BEFORE state=Follower), so caller wakes up nil even when removing self.
- **cluster**: `dataRaftNode` interface gains `ChangeMembership(ctx, adds, removes)` — wired to `*raft.Node.ChangeMembership` via test fakes (`fakeRaftNode`, `raftNodeAdapter`, `autoRebalDataNode`, `fullShardDataNode`).
- **cluster**: `Rebalancer.ExecutePlan` no longer special-cases `ErrLeadershipTransferred` — single error log + AbortPlan path.

### Removed

- **cluster**: `ErrLeadershipTransferred` sentinel + caller-retry contract. With joint consensus, self-removal is in-place (no leadership round-trip required).

### Tests

- **test**: `TestVoterMigration_SelfRemoval_E2E` — leader self-removal via real raft.Node + chaos transport. New leader emerges from C_new; group voter set excludes old leader.
- **test**: `TestMoveReplica_SelfRemoval_UsesChangeMembership` — unit-level self-removal asserts ChangeMembership called with correct adds/removes, returns nil.
- **test**: `TestVoterMigration_ViaDataGroupPlanExecutor` — fixed pre-existing leader-stepdown race by picking `fromNode = non-leader voter` deterministically.

## [0.0.6.22] — 2026-04-30 — ChangeMembership Public API (Sub-project 3 PR-K1)

### Added

- **raft**: `Node.ChangeMembership(ctx, adds, removes)` — public §4.3 joint API with §4.4 learner-first hybrid catch-up. Internal flow: register adds in `jointManagedLearners` → AddLearner each → wait catch-up → atomic JointEnter (promote+remove) → auto JointLeave on commit → caller wakeup nil. Self-removal supported via commit-time step-down hook. spec: `docs/superpowers/specs/2026-04-30-raft-changemembership-design.md`.
- **raft**: `Node.SetChangeMembershipDefaults(opts)` — runtime config of CatchUpTimeout (default 30s) + SkipLearnerPhase opt-out.
- **raft**: `Node.JointPhase()` — observation API returning (phase, oldVoters, newVoters, enterIndex). `JointPhase` type alias exported.
- **raft**: `Node.Configuration()` — extended to return union of C_old ∪ C_new during JointEntering.
- **raft**: `jointManagedLearners` set + dual guard in `checkLearnerCatchup` (jointPhase != None OR managed-by-joint) preventing auto-promote race during ChangeMembership.
- **raft**: `removedFromCluster` flag — orphan election guard. Set in commit-time JointLeave hook when self ∉ C_new; runFollower skips election if true. Reset when self rejoins. Persists across restart via (a) `rebuildConfigFromLog` replay path, (b) snapshot derivation from Servers list (`currentConfigServers` omits self when removed; restore + InstallSnapshot derive flag from `self ∉ Servers`).
- **raft**: `ErrLearnerCatchUpTimeout` sentinel; defer cleanup attempts best-effort RemoveVoter on added learners.
- **test**: `TestJoint_E2E_RemoveSelf` — leader self-removal verified end-to-end (50/50 stable).
- **test**: `TestChangeMembership_*` unit + E2E coverage; `TestCheckLearnerCatchup_SkipsDuringJoint` + `TestCheckLearnerCatchup_SkipsJointManaged` regression guards.

### Fixed

- **raft**: `applyJointConfChangeLocked` JointLeave now removes promoted learners from `learnerIDs` (state-machine torn invariant). Mirror fix in `rebuildConfigFromLog` replay path. Previously dormant — activated by ChangeMembership's joint promote.

## [0.0.6.21] — 2026-04-30 — Live Multi-Raft Sharding (PR-G+H 인프라)

### Added

- **cluster (PR-G+H 인프라)**: per-group raft.Node + BadgerDB lifecycle. 각 노드가 자기가 voter인 그룹들에 대해 별도 BadgerDB + raft.Node 인스턴스를 띄움. spec: `docs/superpowers/specs/2026-04-30-live-multi-raft-sharding-design.md`.
- **cluster**: `pickVoters(groupID, allNodes, RF)` — rendezvous hashing(HRW) 기반 deterministic voter 선택. SHA-256 ranking + sorted output. RF가 cluster size 초과 시 자동 클램프.
- **cluster**: `instantiateLocalGroup` / `shutdownLocalGroup` — BadgerDB+raft.Node 부팅/종료. 5s timeout 후 ungraceful close (WAL 보존). idempotent recovery.
- **cluster**: `GroupBackend` 신규 타입 — *DistributedBackend embedding으로 데이터 plane 메서드 자동 promote. `WrapDistributedBackend(id, b)` 헬퍼로 group-0 (legacy 공유 backend) 등록.
- **cluster**: `lookupForwardTarget(src, groupID)` — PeerIDs[0] 픽 (단순). raft NotLeader redirect 패턴 사용 (leader 캐시 없음 — cold path 1 RTT 손실 OK).
- **cluster**: `MetaFSM.OnShardGroupAdded` callback — apply path가 PutShardGroup commit 시 점화. **반드시 비동기로 dispatch** (heavy work이라 apply loop 블록 위험).
- **cluster**: `MetaFSM.ShardGroup(id)` — 단일 entry 조회 (defensive copy).
- **cluster**: `DistributedBackend.SetShardGroupSource` — CreateBucket의 hash assignment 경로용.
- **cluster**: `Router.HashAssign(bucket, groups)` — FNV-32 % len(groups). CreateBucket이 explicit assignment 없으면 hash로 결정.
- **cluster**: `DataGroupManager.Remove(id)` — graceful detach 시 사용.
- **transport**: `StreamProposeGroupForward = 0x08` — per-group ProposeForward용 신규 stream type. payload: `[4B groupIDLen][groupID][cmdData]`. legacy `StreamProposeForward = 0x06` wire 그대로 (REGRESSION 테스트 가드).
- **cmd/grainfs**: serve.go가 cold-start에 `metaFSM.ShardGroups()` 순회 + `OnShardGroupAdded` callback으로 owned 그룹 자동 instantiate. 5s timeout shutdown loop. `clusterPeers = [raftAddr, peers...]`로 cluster-wide identity 통일 (이전: nodeID label 혼재).

### Tests

- `voter_picker_test.go` — 결정론, 균등 분포, RF 클램프, 단일 노드, 빈 입력, sorted output (7건).
- `forward_target_test.go` — 첫 peer 반환, unknown group, empty peers (3건).
- `forward_wire_test.go` — encode/decode 라운드트립, empty groupID, malformed, **REGRESSION**: legacy wire 호환 (5건).
- `group_backend_test.go` — ID, PUT/GET round-trip, ListBuckets, Close 멱등성 (4건).
- `group_lifecycle_test.go` — 성공, idempotent recovery, BadgerDB open 실패, empty group/node ID, 5s timeout ungraceful sim (6건).
- `router_hash_test.go` — 결정론, spread, empty/single group (4건).
- `meta_fsm_test.go` — `OnShardGroupAdded_FiresOnApply` (defensive copy), `OnShardGroupAdded_AsyncCallbackDoesNotBlockApply` (REGRESSION: apply path는 callback 작업 합산보다 훨씬 짧아야 함).
- `rebalancer_test.go` — `NewNodeUnderUtilization_TriggersMigration` (advisor 우려 검증: findImbalance가 이미 lightest 노드를 ToNode로 처리).
- `tests/e2e/multiraft_sharding_test.go` — `TestE2E_MultiRaftSharding_Boot` (5-proc, 8 그룹, 노드별 그룹 디렉토리 검증).

### Deferred to v0.0.7.x

- **데이터 plane 라우팅**: PUT/GET이 그룹별 backend로 dispatch되지 않음. 모든 트래픽 여전히 group-0 (legacy 공유 distBackend)로 흐름. Per-group BadgerDB는 부팅만 되고 비어있는 상태. 별도 `ClusterCoordinator` 타입 도입 + S3 server wiring 필요.
- **Cross-node forward**: 비-voter 노드가 PUT 받았을 때 voter로 forward. `StreamProposeGroupForward (0x08)` 인프라 + encoder는 들어왔으나 호출 path 미배선.
- **e2e BucketAssignment / RestartRecovery / PerGroupPersistence / CrossNodeDispatch / GroupLeaderFailover**: macOS 멀티프로세스에서 meta-Raft leader change race로 AWS SDK retry 시간 초과. 데이터 plane 라우팅 path 도입 후 stable.

### Notes

- 단일 PR로 묶었던 design은 plan-eng-review에서 결정 (Issue 2-C / TODO 1-C / 2-C / 3-C 모두 "이번 PR 포함"). 실제 구현 중 데이터 plane 라우팅이 spec scope 1.5x 이상으로 확대됨이 명백해져 사용자와 합의 후 인프라만 v0.0.6.21로, 라우팅 v0.0.7.x로 분리.
- `OnShardGroupAdded` 동기 콜백이 apply loop 블록 → meta-Raft 복제 stall → "no leader" — async dispatch로 해결. 이 트랩은 `OnBucketAssigned` 패턴(이미 존재) 따라가는 게 안전. 향후 비슷한 callback 추가 시 동일 가이드.

## [0.0.6.20] — 2026-04-30

### Added

- **raft (Sub-project 2 PR-J5 — 마지막)**: §4.3 snapshot persistence. `Snapshot` struct에 `JointPhase` / `JointOldVoters` / `JointNewVoters` / `JointEnterIndex` 4 필드. `BadgerLogStore.SaveSnapshot` / `LoadSnapshot` 직렬화/복원. Legacy snapshot은 zero values로 자연스럽게 호환.
- **raft**: `SnapshotManager`에 `SetJointStateProvider` / `SetJointStateRestorer` callback. `MaybeTrigger`는 자동 트리거 시점에 provider로부터 joint state 캡처, `Restore`는 restorer로 Node에 적용. Provider/restorer가 nil이면 zero values (legacy 호환).
- **raft**: `Node.JointSnapshotState() (int8, []string, []string, uint64)` + `Node.RestoreJointStateFromSnapshot(int8, []string, []string, uint64)` 헬퍼. int8 phase로 jointPhase unexported 유지.
- **cluster**: `DistributedBackend.SetSnapshotManager` 및 `meta_raft.go` 초기화 시점에 joint state callback 등록. 운영에서 자동 snapshot이 joint phase 진행 중 발생해도 restart 후 phase 자동 재개.

### Tests

- `TestSnapshot_RoundtripPreservesJointState` — full struct roundtrip (Index, Term, Data, Servers, joint 4 필드).
- `TestSnapshot_LegacyHasZeroJointState` — joint 필드 미설정 snapshot이 JointNone + 빈 vectors로 복원.
- `TestRestoreJointStateFromSnapshot_ResetsLeaveProposed` — restart 후 `jointLeaveProposed` flag 리셋해서 leader watcher가 재평가.
- `TestApply_JointLeave_SelfRemoval_StepsDown` — append-time 적용은 leader state 유지 (commit-time hook이 step-down 처리).

### Notes

- Mixed-version reject은 PR-J4에서 이미 `mixedVersion` flag로 통합 (`ErrMixedVersionNoMembershipChange`). 별도 `ErrMixedVersionNoJointChange` 에러는 over-engineering이라 미도입.
- Chaos scenarios (LeaderCrashBetweenEnterAndLeave / PartitionDuringJoint / RepeatedLeaderChange) + E2E `TestJoint_E2E_SnapshotMidJoint_AutoCompletes`는 internal-only `proposeJointConfChangeWait`라 chaos pkg(별 package)에서 호출 불가하여 skip. Sub-project 3 `ChangeMembership` public API 노출 후 follow-up (TODOS.md 등록).
- Sub-project 2 완결. 5 PR 시퀀스: PR-J1 FBS schema, PR-J2 dual-quorum, PR-J3 apply/auto-progression/truncation revert, PR-J4 caller API + commit-time close, PR-J5 snapshot persistence + self-removal step-down 검증.

## [0.0.6.19] — 2026-04-30

### Added

- **raft (Sub-project 2 PR-J4)**: §4.3 caller API. `proposeJointConfChangeWait(ctx, adds, removes) error` — adds/removes로부터 C_old/C_new 자동 산출 → JointEnter propose → leader heartbeat watcher가 자동 JointLeave → commit-time apply path가 jointPromoteCh close → caller wakeup. Internal-only (Sub-project 3에서 public 노출).
- **raft**: `equalServerSets` 헬퍼 (id-keyed, order-independent voter set 비교). C_old == C_new no-op detection.
- **raft**: `peersExcludingSelf` (`internal/raft/membership.go`) — joint voter set (self 포함)을 기존 `config.Peers` 컨벤션 (self 제외)으로 변환. JointLeave apply + truncation revert에서 사용.

### Tests

- 단위 6개: RejectsNotLeader / RejectsConcurrentJoint / RejectsConcurrentSingleServer / RejectsMixedVersion / NoOpEqualSets / EqualServerSets.
- 통합 1개: `TestJoint_E2E_RemoveOne` — 4-node in-memory cluster, leader가 non-self peer 제거. Full joint cycle (JointEnter → auto JointLeave → commit-time wakeup) 검증. config.Peers는 self 제외 컨벤션 유지.

### Notes

- Edge cases (CtxTimeout 시 state 보존, partition 시 leader change, mid-failure recovery)는 PR-J5 chaos scenarios에서 검증.

## [0.0.6.18] — 2026-04-30

### Added

- **raft (Sub-project 2 PR-J3)**: §4.3 apply path + auto-progression + truncation revert.
  - `applyConfigChangeLocked` JointConfChange 분기 (`internal/raft/membership.go`). JointEnter는 append-time에 jointPhase=Entering + voter sets 설정, leader는 새 voter (newServers \ oldServers)에 한해 nextIndex/matchIndex bootstrap. JointLeave는 append-time에 jointPhase=None + config.Peers=newServers (§4.4 invariant 유지).
  - Apply loop (commit-time, `internal/raft/raft.go`): JointLeave 커밋 시 `jointPromoteCh` close + 자기 제거된 leader는 step-down. **Truncation safety**: append-time이 아닌 commit-time gating으로 새 leader가 JointLeave를 truncate해도 caller가 잘못 wake up하지 않음.
  - `rebuildConfigFromLog`이 JointConfChange entry도 replay (truncation revert). 로그에 JointEnter는 있지만 JointLeave가 잘린 경우 jointPhase=Entering으로 자동 복구 → dual-quorum 유지.
  - `runLeader` heartbeat tick에 `checkJointAdvance` 추가 (`checkLearnerCatchup` 옆). JointEnter committed 감지 시 leader가 자동으로 JointLeave propose. 5단계 idempotency (state, phase, commitIndex, log lookahead, flag).
  - `flushBatch` §4.3/§4.4 통합 가드: jointPhase != JointNone 시 일반 ConfChange reject, 동시 batch 내 JointEnter/Leave는 1개만, JointEnter는 jointPhase=None일 때만, JointLeave는 jointPhase=Entering일 때만.
  - `internal/raft/joint.go`: `serverPeerKeys`, `containsPeer`, `initLeaderStateForNewVoters`, `hasJointLeaveAfter`, `proposeJointEnter`, `proposeJointLeave`, `proposeJointEntry`, `checkJointAdvance`, `peerAddressSnapshotLocked`, `serverEntriesFromIDs` 헬퍼.

### Tests

- `TestApply_JointEnter_ActivatesJointPhase` — append-time 상태 전환
- `TestApply_JointEnter_LeaderInitsNewVotersOnly` — 새 voter만 replication state init, 기존 voter 보존
- `TestApply_JointLeave_DeactivatesAtAppendTime` — phase reset + config.Peers 갱신, jointPromoteCh는 commit-time까지 close 안 함
- `TestCheckJointAdvance_LogLookaheadIdempotency` — log에 JointLeave 이미 있으면 propose 안 함
- `TestRebuildConfigFromLog_TruncatedJointLeave_RevertsToEntering` — truncated Leave → Entering revert

### Notes

- 본 PR 시점에 propose path는 internal-only (`proposeJointEnter`/`proposeJointLeave`). 외부 caller API는 PR-J4에서 추가.
- Integration test (E2E joint cycle)도 PR-J4의 caller API와 함께 작성.

## [0.0.6.17] — 2026-04-30

### Changed

- **raft (Sub-project 2 PR-J2)**: §4.3 dual-quorum 핵심 함수. `quorumSets`/`hasMajorityInSet`/`dualMajority` 헬퍼 (`internal/raft/joint.go`) 도입. `hasQuorum`, `advanceCommitIndex`, `runPreVote`, `runCandidate`, `quorumMinMatchIndexLocked`을 dual-aware로 전환 — joint mode (`jointPhase == JointEntering`)에서 old/new 양쪽 voter set 모두 majority 도달 시에만 quorum 인정.
- **raft**: vote/quorum 카운트가 단순 정수 누적에서 `map[id]bool` set 기반으로 변경. PreVote/Election에서 voter set 합집합으로부터 peer 리스트 산출 — joint mode 시 old/new에 동시 존재하는 peer는 한 번만 RPC.

### Notes

- `jointPhase == JointNone` 인 single mode에서는 기존 majority 의미를 정확히 유지 (회귀 테스트 통과). 새 entry는 propose되지 않아 시스템 동작 변화 없음.
- 단위 테스트 4개 추가 (`TestDualQuorum_*`, `TestQuorumMinMatchIndex_JointMode_Conservative`, `TestQuorumMinMatchIndex_SingleMode`).
- Voter set lock-free read는 별도 follow-up — 본 sub-project 5개 PR 머지 후 brainstorming.

## [0.0.6.16] — 2026-04-30

### Added

- **raft (Sub-project 2 PR-J1)**: §4.3 joint consensus wire format 활성화. `JointConfChangeEntry` FBS table + `JointOp` enum 추가 (`ServerEntry` 재사용). `LogEntryType=JointConfChange (2)` slot 활성. `SnapshotMeta`에 `joint_phase` / `joint_old_voters` / `joint_new_voters` / `joint_enter_index` 4 필드 추가. PR-A `ConfChangeEntry.new_config`/`old_config`은 `(deprecated)` 표시 (mixed-version 윈도우 종료 후 별도 cleanup PR로 제거).
- **raft**: `Node`에 jointPhase/jointOldVoters/jointNewVoters/jointEnterIndex/jointLeaveProposed/jointPromoteCh 필드. `internal/raft/joint.go`에 `encodeJointConfChange`/`decodeJointConfChange` + roundtrip 테스트 3개.

### Notes

- 본 PR 머지 시점에 새 entry는 propose되지 않음 — parsing/serialization layer만 활성. 시스템 동작 변화 없음.

## [0.0.6.15] — 2026-04-30

### Tests

- **raft/chaos**: learner-first scenarios 3개 추가 (`TestChaos_LearnerFirst_LeaderChange_NewLeaderPromotes`, `TestChaos_LearnerFirst_SlowLearner_CallerCtxTimeout`, `TestChaos_LearnerFirst_RepeatedLeaderChange_EventualPromote`) — Tier 3-1 sub-project 1 (Learner-first AddVoter)의 in-memory chaos 검증. flaky QUIC E2E `TestAddVoter_E2E_LeaderChange_StillPromotes`의 stable 대체. 10/10 pass rate.

## [0.0.6.14] — 2026-04-30

### Added

- **serve**: `--seed-groups N` CLI 플래그 — leader가 부트스트랩 시 N개 데이터 그룹을 idempotent ProposeShardGroup 루프로 시드. default 0 = auto: `max(8, (1+len(peers))*4)` (솔로=8, 5-node=20, 10-node=40). 클러스터 확장 시 sharding 헤드룸 미리 확보 — 이전 default(group-0 1개만)는 후속 노드 추가 시 sharding 부재로 위험했다.

### Tests

- **e2e**: `TestE2E_SeedGroups_Multi` — 5 process boot + `--seed-groups=8` 회귀 가드.
- **e2e**: `TestE2E_ClusterScaleBench_N8/N32/N64/N128` — multi-process scale 측정 (`GRAINFS_BENCH_FULL=1` 환경변수 게이트, 각 1-2분 소요).
- **e2e**: `scale_bench_metrics_test.go` — pprof heap/goroutine + ps RSS/CPU 샘플링 helpers (재사용 가능).

### Documentation

- **specs**: `docs/superpowers/specs/2026-04-30-multi-raft-scale-microbench-design.md` — N=8/32/64/128 실측치 + Inflection Point Analysis + Recommendations 채움. 핵심 결과: boot 선형 (~0.4s/group), RSS/CPU/goroutine flat, heap만 그룹당 ~1MB 완만 증가 — N=128(운영 목표)까지 안정. multiplexed transport + raft tick coalescing 효과 확인.

## [0.0.6.13] — 2026-04-30

### Added

- **raft**: `Config.LearnerCatchupThreshold` (default 100) — voter promote 트리거 임계값 (`matchIndex+threshold >= commitIndex`).
- **raft**: `Node.SetLearnerCatchupThreshold(uint64)` — 런타임 조정.
- **raft**: `Node.AddVoterCtx(ctx, id, addr)` — context 명시 가능한 AddVoter 변형.
- **raft**: Leader heartbeat tick inline `checkLearnerCatchup()` watcher — caught-up learner 자동 promote.

### Changed

- **raft**: `AddVoter(id, addr)` 동작 변경 — 즉시 voter 등록 → 자동 learner-first 후 promote. caller는 promote commit까지 대기 (truncation-safe). 이미 voter면 즉시 nil (idempotent). Joint consensus의 안전망 (Tier 3-1 sub-project 1).
- **cluster**: `DataGroupPlanExecutor.MoveReplica` — inline 70줄 learner-first 시퀀스 (AddLearner + 폴링 + PromoteToVoter) → 새 `node.AddVoterCtx()` 한 줄로 단순화. 동일 외부 동작.

### Fixed

- **raft**: `initLeaderState`가 `learnerIDs`의 nextIndex/matchIndex를 미초기화하던 잠재 버그 수정 — follower 시절 learner를 받은 노드가 leader 될 때 watcher가 matchIndex=0을 absent map에서 읽고, replicateToAll의 learner replication이 nextIdx=0(snapshot 모드)으로 시작하던 문제. PR-A부터 잠재된 버그였으며 sub-project 1 watcher 도입으로 노출됨.

### Tests

- **raft**: 12개 unit (`TestAddVoter_*`, `TestCheckLearnerCatchup_*`, `TestApplyLoopClosesPromoteCh`, `TestInitLeaderState_InitializesLearnerReplicationState`).
- **raft**: 1개 E2E (`TestAddVoter_E2E_LearnerFirstThenPromote`). `LeaderChange_StillPromotes` variant은 QUIC transport timing-sensitive로 skip — 메커니즘은 verified, chaos harness scenario 재작성은 후속.
- **cluster**: `TestMoveReplica_*`(11개) 회귀 PASS, `TestVoterMigration_ViaDataGroupPlanExecutor` 회귀 PASS.

### Why

Multi-Raft (PR-A~E)가 master에 머지되어 voter 전환이 빈번. 새 노드 가입 시 catch-up 미완 상태에서 quorum에 들어가면 다른 노드 장애 시 quorum 손실 위험. cross-model outside voice (Claude + Gemini) 통과 — append-time → commit-time close + PR-E refactor 합류 결정 반영.

## [0.0.6.12] — 2026-04-30

### Fixed

- **nfs4server**: Go 1.26.2 컴파일러 ICE 우회 — `internal/nfs4server/xdr.go`의 `opArgPool16`/`opArgPool8` (fixed-size `*[N]byte` 풀)을 generic `pool.Pool[T]`에서 raw `sync.Pool`로 교체. `make test`(다수 패키지 + `-cover` 병렬 빌드) 조합에서 재현되던 `internal compiler error: bad ptr to array in slice go.shape.*uint8`(xdr.go:344, 85) 해결. 원인은 fixed-size array pointer를 generic type parameter로 instantiate 후 결과를 슬라이싱하는 패턴에서 shape 분석 버그 — `*XDRWriter`/`*XDRReader` 등 struct pointer pool은 영향 없음.

## [0.0.6.11] — 2026-04-30

### Fixed

- **raft**: 스냅샷에 클러스터 멤버십(servers)을 저장하지 않아 재시작 후 멤버십이 초기화되던 §2.3 silent drift 버그 수정 — `BadgerLogStore.SaveSnapshot`이 `SnapshotMeta.servers`를 인코딩하지 않아 `LoadSnapshot` 시 nil이 반환되었고, follower 재시작 시 `restoreFromStore`가 cluster config를 복원하지 못해 quorum 계산이 잘못되었음.
- **raft**: `HandleInstallSnapshot`이 `Server.Suffrage`를 무시하고 NonVoter를 voter로 등록하던 버그 수정 — `restoreConfigFromServers` 헬퍼로 Voter는 `config.Peers`로, NonVoter는 `learnerIDs`로 분리.
- **raft**: 기동 시 `restoreFromStore`가 스냅샷 index/term을 `lastApplied`/`commitIndex`에 반영하지 않아 이미 스냅샷된 엔트리를 재적용하던 버그 수정.

### Changed

- **raft**: `LogStore.SaveSnapshot`/`LoadSnapshot` API를 `Snapshot` struct로 통일 (index/term/data/servers).
- **raft**: `SnapshotManager.MaybeTrigger` 시그니처에 `servers []Server` 파라미터 추가 — caller가 `Configuration().Servers`를 전달.
- **raft**: `currentConfigServers()`/`Configuration()`이 learner(NonVoter)를 포함하도록 수정 — 스냅샷이 전체 멤버십 캡처.
- **raft**: `rebuildConfigFromLog`에 `startIndex`/`basePeers`/`baseLearners` 파라미터 추가 — 스냅샷 이후 로그만 재생 가능.

### Added

- **raft**: `restoreConfigFromServers([]Server, selfID)` 헬퍼 — Suffrage 인식, self 필터링, 두 복원 경로(`HandleInstallSnapshot`, `restoreFromStore`)에서 공유.

### Tests

- **raft**: `TestSaveLoadSnapshot_WithServers` — servers 필드 FBS round-trip (3개, NonVoter 포함).
- **raft**: `TestSaveLoadSnapshot_Legacy` — servers=nil 레거시 포맷 호환.
- **raft**: `TestRestoreConfigFromServers_VoterLearnerSplit` — Voter/NonVoter 분리 + self 필터.
- **raft**: `TestRestoreConfigFromServers_EmptyServers` — empty input 처리.
- **raft**: `TestRebuildConfigFromLog_WithBase` — basePeers + 로그 항목 합산.
- **raft**: `TestRebuildConfigFromLog_SkipsBeforeStartIndex` — startIndex 이전 항목 스킵.
- **raft**: `TestHandleInstallSnapshot_SuffrageFix` — NonVoter → learnerIDs.
- **raft**: `TestRestoreFromStore_LoadsSnapshotServers` — 기동 시 스냅샷 서버 + lastApplied/commitIndex/term 복원.
- **raft**: `TestRestoreFromStore_LegacySnapshot` — legacy snapshot (servers=nil) fallback 경로.
- **raft**: `TestSnapshotPreservesClusterMembership` — 3-노드 클러스터 §2.3 end-to-end 통합 테스트.

## [0.0.6.10] — 2026-04-30

### Fixed

- **cluster**: `putObjectEC` 링 배치에 unhealthy/제거된 노드가 포함될 때 write-all 실패하던 버그 수정 — 링이 N-노드 토폴로지로 만들어진 후 일부 노드가 죽으면 `Ring.PlacementForKey`가 dead 노드를 후보에 포함시켜 EC PUT 전체가 실패. 후보 placement를 `liveNodes` 셋과 비교해 하나라도 dead 노드가 있으면 `PlacementForNodes(liveNodes)`로 폴백 (`ringVer=0` → read는 `metaNodeIDs` 경로 사용). `TestE2E_ClusterEC_TopologyChange` 노드 종료 후 PUT 복구.

### Changed

- **cluster**: placement 선택 로직을 `selectECPlacement(ring, ringErr, cfg, liveNodes, key)` 자유함수로 추출 — 순수 함수로 만들어 backend 의존성 없이 모든 분기(no-ring / all-live / dead-node 포함 / ringErr)를 단위 테스트.

### Tests

- **cluster**: `placement_select_test.go` — `selectECPlacement` 5개 단위 테스트 (`TestSelectECPlacement_NoRing`, `RingAllLive`, `RingHasDeadNode`, `RingPartialDead`, `RingErrPropagates`). E2E `TestE2E_ClusterEC_TopologyChange`가 검증하던 ring 폴백 동작을 단위 수준에서 보장.
- **e2e**: `cluster_ec_test.go` / `degraded_test.go` 포트 대기 순차 루프를 `waitForPortsParallel`로 병렬화 — 5노드 클러스터 부팅 단계 단축.
- **e2e**: `degraded_test.go` 리더 발견 윈도우 15s → 30s — loaded macOS에서 Raft 수렴 여유 확보.
- **e2e**: `ec_shardcache_eval_test.go` 리더 발견 폴링 120s/2s → 15s/500ms — 부트 직후 빠른 리더 확인.

### Chore

- **deps**: aws-sdk-go-v2 v1.41.7, fsnotify v1.10.0, genproto/googleapis 2026-04-27 minor bump.

## [0.0.6.9] — 2026-04-30

### Fixed

- **raft**: `runCandidate()` 선거 실패 시 `state`가 `Candidate`로 유지되던 버그 수정 — 선거 패배·쿼럼 미달 시 `Follower`로 복귀하지 않아 `run()` 루프가 매 틱마다 `runCandidate`를 재호출하고 term을 무한 증가시키는 term-inflation livelock 발생. defer로 선거 종료 시 `Candidate → Follower` 복귀 보장. `TestE2E_ClusterEC_TopologyChange` 6-노드 MetaRaft 리더 선출 복구.


## [0.0.6.8] — 2026-04-30

### Fixed

- **raft**: `PreVote`/`LeaderTransfer` 필드가 QUIC wire codec에서 유실되던 버그 수정 — FlatBuffers `RequestVoteArgs` 스키마에 `pre_vote:bool`/`leader_transfer:bool` 추가, 인코더/디코더 배선. PreVote 손실 시 pre-vote 보호 무효화, LeaderTransfer 손실 시 leader transfer 선거 중단 되던 문제 해결.

### Tests

- **raft**: `TestRoundTrip_AllWireStructs` — 모든 wire struct의 reflect.DeepEqual round-trip 검증. 스키마에 없는 필드는 이 테스트에서 즉시 감지.
- **raft**: `RequestVoteArgs_ZeroFlags` subtest — FlatBuffers default-value omission 커버리지. false bool은 버퍼에 기록되지 않고 vtable default로 반환됨을 검증.

## [0.0.6.7] — 2026-04-30

### Added

- **cluster**: `FSM.LookupObjectECShards(bucket, key, versionID)` — BadgerDB에서 EC 파라미터 (k, m) 조회. N× 오브젝트(메타 없음)는 `(0, 0, nil)` 반환.
- **cluster**: `MigrationExecutor.SetShardCounter(fn)` — 오브젝트별 shard 수 콜백. N× 오브젝트(k=0)는 1 반환, EC 오브젝트는 k+m 반환, fn=nil 또는 반환값 0이면 `numShards` fallback.
- **cluster**: `ec_cluster_smoke_test.go` — `TestLookupObjectECShards_NxMode`, `TestLookupObjectECShards_ECMode` 단위 테스트.
- **cluster**: `TestECCluster_Smoke_3Node` — Phase 19에서 활성화 예정 (t.Skip stub).
- **cluster**: `TestMigrationExecutor_NxMode_*`, `TestMigrationExecutor_ECMode_*`, `TestMigrationExecutor_ShardCounter_ZeroFallback` — SetShardCounter 경로 단위 테스트.
- **serve**: `ecShardCounterFor(fsm)` — SetShardCounter용 named factory 함수.

### Changed

- **serve**: EC 스크러버 시작 조건 `scrubInterval > 0 && ECActive()` → `scrubInterval > 0` — N× 클러스터에서도 스크러버 활성화.

### Fixed

- **serve**: `ecShardCounterFor` DB 에러 시 `return 1` → `return 0` — shard 0만 복사 후 전체 삭제하던 데이터 손실 경로 수정 (numShards fallback으로 전환).

## [0.0.6.6] — 2026-04-30

### Added

- **cluster**: `DataGroupPlanExecutor` leader self-removal guard — `fromNode == localNodeID` 시 `TransferLeadership()` 호출 후 `ErrLeadershipTransferred` 반환; 새 리더가 plan을 재개.
- **cluster**: `TransferLeadership()` — `dataRaftNode` 인터페이스에 추가; serve.go에서 `nodeID` 인자로 배선.
- **cluster**: `TestAutoRebalance_E2E_ProposeAndExecute` — Mock 제거, 실 chaos data-Raft + `DataGroupPlanExecutor` 로 업그레이드. node-1=90% 부하 불균형 → voter 마이그레이션 → MetaFSM 멤버십 검증.
- **cluster**: `TestFullSharding_E2E` — MetaRaft 3-node + Rebalancer + DataGroupPlanExecutor + chaos data-Raft 완전 통합 e2e. node-3 AddLearner→PromoteToVoter→RemoveVoter 마이그레이션 검증.
- **cluster**: `TestMoveReplica_TransfersLeadershipWhenFromNodeIsLocal` — 자기 제거 가드 단위 테스트.

### Changed

- **rebalancer**: `ExecutePlan` — `ErrLeadershipTransferred`는 INFO 로그(정상 플로우), 그 외 에러만 ERROR 로그 + `ProposeAbortPlan`.

## [0.0.6.5] — 2026-04-30

### Added

- **NBD write-back async path** (`internal/volume/volume.go`) — `WriteAtDeferred()` splits local disk write from Raft commit. NBD write handler acknowledges after local write; Raft commits are deferred and batched on `NBD_CMD_FLUSH`, enabling Raft batcher coalescing (~10× fewer fdatasync calls).
- **NBD per-key flush serialization** (`internal/nbd/nbd.go`) — `flushPending()` groups deferred commits by block offset: concurrent across distinct blocks (maximizes Raft batcher throughput), sequential within each block (preserves per-block write ordering). Fixes `--end_fsync=1` flush tail latency: 34 s → 5 s (×6.8).
- **`PutObjectAsync`** across backend layers — `DistributedBackend`, `CachedBackend`, `WALBackend` each implement the write-back interface. WAL entries are appended inside `commitFn` so PITR records only committed objects.
- **`GRAINFS_VOLUME_TRACE=1`** — per-stage latency logging in `PutObject` (create_temp, copy_close, rename, badger_update), `HeadObject`, `Raft flushBatch` — diagnostic instrumentation for NBD hot path.
- `internal/nbd/nbd_bench_test.go` — in-process NBD benchmarks via `net.Pipe()`: `BenchmarkNBD_Read4K`, `BenchmarkNBD_Read64K`, `BenchmarkNBD_Write4K`, `BenchmarkNBD_Write64K`.
- `internal/nbd/nbd_test.go` — `TestNBDFlushWriteOrdering`: write same block twice then flush; asserts second write wins (guards per-key ordering regression).
- `tests/nbd_colima/` — Colima Linux VM end-to-end NBD tests (macOS server → nbd-client). `make test-nbd-colima`.
- `benchmarks/bench_nbd_profile.sh` — fio workload suite with pprof capture. `make bench-nbd`.
- `benchmarks/bench_nbd_trace.sh` — trace-mode benchmark for per-stage latency breakdown.

### Performance

- NBD `--end_fsync=1` (worst-case batch flush): **34 s → 5 s** (×6.8 improvement)
- NBD 4 KiB sequential write throughput: ~450 MB/s (net.Pipe baseline)
- Raft commit coalescing: from per-write proposal to batched-per-flush proposal

## [0.0.6.4] — 2026-04-30

### Added

- **FUSE-over-S3 호환성 검증** — GrainFS는 별도 FUSE 클라이언트 바이너리 없이 표준 S3-compatible 도구(rclone/s3fs/goofys)로 마운트할 수 있음을 e2e + 처리량 벤치로 보증. 클라이언트 머신에 grainfs 설치 불필요.
- `tests/fuse_s3_colima/` — Colima Linux VM에서 macOS 호스트 GrainFS S3 endpoint를 rclone mount로 마운트해 검증하는 e2e 4건 (smoke / directories / rename / cross-protocol). `make test-fuse-s3-colima`.
- `tests/fuse_s3_colima/bench_test.go` — Direct S3(rclone copyto) vs FUSE mount(rclone mount) 처리량 비교 벤치. 64 MiB · `--vfs-cache-mode off` · 3회 평균: Direct PUT 96.8 MB/s · Direct GET 108.0 MB/s · FUSE Write 106.7 MB/s · FUSE Read 107.3 MB/s. **FUSE 오버헤드 ≈ 0%**. `make bench-fuse-s3-colima`.
- README "FUSE-over-S3 마운트" 섹션 — rclone 설정 가이드 + 지원/미지원 연산표(rename ⚠️ non-atomic, chmod ❌, file locking ❌) + 처리량 결과.
- `internal/storage/errors.go`: sentinel errors `ErrECDegraded`, `ErrNoSpace`, `ErrQuotaExceeded`, `ErrInvalidVersion` — 향후 backend별 에러 분류용.

### Changed

- `internal/vfs/vfs.go` `grainFile.ReadAt`: `mu sync.Mutex` 추가로 동시 ReadAt에서 `rc`/`pos` 보호 (`io.ReaderAt` 계약 준수). FUSE-over-S3 도구가 발행하는 병렬 range GET 요청에 안전.

## [0.0.6.3] — 2026-04-30

### Added

- **cluster**: `DataGroupPlanExecutor` — `MoveReplica` 실제 Raft voter 마이그레이션 구현 (AddLearner → catch-up 대기 → PromoteToVoter → RemoveVoter → MetaFSM ProposeShardGroup). `StubGroupRebalancer` 대체.
- **cluster**: `DataRaftNode` 인터페이스 — `dataRaftNode` exported alias; `raft.Node`가 compile-time 구현 검증.
- **cluster**: `DataGroupPlanExecutorForTest` — 테스트용 `nodeFor` 주입 팩토리.
- **raft**: `AddLearner`/`PromoteToVoter`/`RemoveVoter` 실 구현 — `applyConfigChangeLocked`에서 `ConfChangeAddLearner`, `ConfChangePromote` 처리; `learnerIDs` 맵으로 learner 추적.
- **raft**: `PeerMatchIndex(peerKey)` — learner catch-up 대기에 필요한 replication 상태 조회.
- **raft**: `replicateToAll` — voter 외에 learner에게도 log 복제.
- **raft/chaos/scenarios**: `TestVoterMigration_AddLearnerPromoteRemove` — 4-node chaos cluster voter 교체 통합 테스트.
- **cluster**: `TestVoterMigration_ViaDataGroupPlanExecutor` — real `raft.Node` e2e 테스트.

### Fixed

- **cluster**: `TestMetaRaft_ConcurrentJoin_AtLeastOneSucceeds` — real joiner node(m1, m2) 사용으로 재작성; AddLearner/PromoteToVoter가 실 구현이 된 이후에도 conf change serial enforcement 검증 가능.

## [0.0.6.2] — 2026-04-30

### Added
- **cluster**: MetaFSM `loadSnapshot`, `activePlan` 필드 + `SetLoadSnapshot`, `ProposeRebalancePlan`, `AbortPlan` MetaCmdType 3종
- **cluster**: LoadReporter — meta-Raft leader-only 30s load commit 루프 (P1: ~44 MB/day)
- **cluster**: Rebalancer actor — imbalance 평가 + RebalancePlan 제안 + 10분 plan timeout abort
- **cluster**: GroupRebalancer interface + StubGroupRebalancer (PR-E에서 real executor 구현)
- **cluster**: MockGroupRebalancer — 테스트용 MoveReplica recorder
- **cluster**: FlatBuffers `LoadStatEntry`, `RebalancePlan`, `MetaSetLoadSnapshotCmd`, `MetaProposeRebalancePlanCmd`, `MetaAbortPlanCmd`
- **cluster**: MetaRaft `ProposeLoadSnapshot`, `ProposeRebalancePlan`, `ProposeAbortPlan`, `IsLeader` 메서드
- **cli**: `grainfs cluster plan-show` — 활성 rebalance plan + load snapshot 출력
- **cli**: `grainfs cluster rebalance --dry-run` — 불균형 감지 preview

## [0.0.6.1] — 2026-04-30

### Performance

- **NFSv4 opRead ReadAt fast path** (`internal/nfs4server/compound.go`, `internal/cluster/backend.go`, `internal/storage/cache.go`, `internal/storage/wal/backend.go`): sequential read 2.6 MiB/s → 116 MiB/s (44×). 근본 원인: opRead마다 `HeadObject×2 + GetObject(os.Open) + Seek` = 4×BadgerDB 조회 + 파일 오픈/클로즈가 128 KiB NFS READ마다 발생 (256 MiB 파일 = 2048 READ = 8192 BadgerDB reads + 2048 파일 오픈). `ReadAt(bucket,key,offset,buf)` 인터페이스 추가 — `DistributedBackend.ReadAt`은 HeadObject/EC path 우회, `os.File.ReadAt(pread)` 직접 호출. `CachedBackend.ReadAt`은 캐시 히트 시 `bytes.Reader.ReadAt` (zero-copy), 미스 시 inner backend 위임. `opRead` 패스트패스: `readAtBackend` 타입 어서션으로 HeadObject+GetObject+Seek 전면 제거. 128 KiB ReadAt 버퍼 pool화로 per-call alloc(1921 MB) 제거.

### Added

- `DistributedBackend.ReadAt` — 내부 버킷 전용 pread(2) API, EC/shardSvc/HeadObject 우회.
- `wal.Backend.ReadAt` — pass-through (read는 WAL 항목 없음).
- `CachedBackend.ReadAt` — 캐시 히트/미스 양 경로 지원.
- `readAtBackend` 인터페이스 (`internal/nfs4server/compound.go`).
- `opReadAtBufPool` — 128 KiB 읽기 버퍼 pool.
- **cluster**: `PutBucketAssignment` Raft 커맨드 — bucket→shard-group 매핑을 MetaFSM 로그에 persist; `Snapshot`/`Restore` 시리얼라이즈 포함.
- **cluster**: `MetaRaft.ProposeBucketAssignment` — 동기 propose + apply-wait.
- **cluster**: `BucketAssigner` 인터페이스 — `server.CreateBucket` 호출 시 MetaRaft로 버킷을 shard group에 자동 배정.
- **cluster**: `DataGroup.Backend` — shard group 단위 `object.Backend` 래퍼; `Router.AssignBucket`/`Sync` COW (`atomic.Pointer + CAS`) 라우팅 테이블.
- **serve**: `BucketAssigner` + `Router` + `DataGroupManager` 완전 연결 (PR-D Task 7).

### Fixed

- **cluster**: `MetaFSM.Restore()` — snapshot install 후 `onBucketAssigned` 콜백 미호출로 Router 상태 불일치. 콜백을 잠금 해제 이후 순회하여 호출.
- **cluster**: `DistributedBackend.PutObject` nil router guard, 빈 groupID 검증 추가.
- **cluster**: `Router.Sync` merge semantics — bootstrap 시 기존 라우팅을 덮어쓰지 않음.

## [0.0.6.0] — 2026-04-29

### Performance

- **NFSv4 opWrite WriteAt fast path** (`internal/nfs4server/compound.go`, `internal/storage/local.go`): RMW 경로의 `io.ReadAll`(alloc 프로파일 76.61%, 24.4 GB) 제거. `LocalBackend.WriteAt` 추가 — prefix/suffix를 `io.Copy` 스트리밍으로 처리하여 전체 파일을 힙에 올리지 않음. 원자적 temp→rename 패턴 유지로 reader visibility 보장.
- **DistributedBackend MD5 skip for internal buckets** (`internal/cluster/backend.go`): `putObjectNx`에서 NFS 내부 버킷(`IsInternalBucket` 체크)에 대해 불필요한 MD5 계산(CPU 프로파일 51.52%) 스킵.

### Added

- `LocalBackend.WriteAt(bucket, key string, offset uint64, data []byte)` — 부분 쓰기 API, streaming copy로 힙 0-alloc.
- `CachedBackend.WriteAt` — 캐시 무효화 후 inner backend WriteAt 위임.
- `TestLocalBackend_WriteAt`, `TestLocalBackend_WriteAt_NewFile`, `TestCachedBackend_WriteAt` 단위 테스트.

## [0.0.5.9] — 2026-04-29

### Fixed

- **NFSv4 동시 쓰기 race condition** (`internal/nfs4server/compound.go`): `opWrite`의 `offset == 0` 경로가 `LockPath` 없이 `PutObject`를 직접 호출 → RMW(`offset > 0`) 경로와 동시 실행 시 데이터 손실. 항상 `LockPath` 사용 + `HeadObject`로 기존 파일 size 확인하여 partial overwrite 보존 (`offset == 0 && end >= existingSize`만 직접 PutObject, 그 외는 RMW).
- **LocalBackend.PutObject torn read** (`internal/storage/local.go`): `os.Create`가 즉시 truncate하여 동시 reader가 빈 파일을 보고 `eof + empty` 반환. fio randrw 벤치마크에서 `full resid` EIO 발생의 핵심 원인. `os.CreateTemp` + `os.Rename` atomic publish로 수정 (rename → metadata write 순서 유지).

### Added

- **NFSv4 동시 쓰기 회귀 테스트** (`internal/nfs4server/concurrent_write_test.go`): writer-vs-writer race(`TestConcurrentWrite_OffsetZeroVsRMW`)와 reader-vs-writer race(`TestConcurrentWrite_ReaderVsWriter`) 두 시나리오 byte-by-byte 검증.
- **NFSv4 fio 벤치마크 스크립트** (`benchmarks/bench_nfs_profile.sh`): Colima VM에서 fio mixed workload(seq write/seq read/randrw) 실행 + pprof CPU/heap/mutex/block 프로파일 동시 수집.

## [0.0.5.8] — 2026-04-29

### Added

- **cluster**: `ShardGroupEntry` FlatBuffers schema (`cluster.fbs`) + `make fbs` 재생성
- **cluster**: MetaFSM `PutShardGroup` 커맨드 — `applyPutShardGroup`, `ShardGroups()`, `Snapshot`/`Restore` 확장
- **cluster**: `DataGroup` + `DataGroupManager` — lock-free `atomic.Pointer[groupSnapshot]` COW scaffold
- **cluster**: `Router` — bucket-level 라우팅 (`shard_map: bucket→group_id`, `defaultGroupID` fallback)
- **cluster**: `MetaRaft.ProposeShardGroup` — `waitApplied` 기반 동기 propose
- **serve**: 초기 단일 shard group `group-0` 비동기 propose (PR-D 연결 예고)

### Changed

- **generic pool wrappers** (`internal/pool`): `Pool[T]`, `SyncMap[K,V]`, `AtomicValue[T]` 추가.
  코드베이스 전체의 `sync.Pool`/`sync.Map`/`atomic.Value` 타입 단언(`.(T)`) 을 제거하여
  런타임 패닉 가능성 원천 차단 및 가독성 향상 (23 파일).

## [0.0.5.7] — 2026-04-29

### Added

- **MetaFSM** (`internal/cluster/meta_fsm.go`): FlatBuffers 기반 클러스터 멤버십 상태 머신 (AddNode/RemoveNode/Snapshot/Restore).
- **MetaRaft** (`internal/cluster/meta_raft.go`): 컨트롤 플레인 전용 `raft.Node`; lock-free apply-wait (generation channel + `atomic.Uint64`).
- **MetaTransportQUIC** (`internal/raft/meta_transport_quic.go`): QUIC 스트림 `StreamMetaRaft = 0x07`로 데이터 플레인과 분리.
- **`cluster join` CLI** (`cmd/grainfs/cluster_join.go`): 운영자용 meta-Raft 클러스터 참가 서브커맨드.
- **3-node E2E 테스트** (`internal/cluster/meta_raft_e2e_test.go`): in-process 부트스트랩, Join×2, 리더 검증.

### Fixed

- **QUIC codec MetaRPC 등록 누락** (`internal/raft/quic_rpc_codec.go`): `"MetaRequestVote"` 등 6개 타입 문자열 미등록으로 프로덕션 QUIC meta-Raft RPC 전체 실패 (silent error). 멀티-케이스 레이블 추가로 수정.
- **waitApplied TOCTOU 순서 버그**: load-then-snapshot 순서가 snapshot-then-check로 역전되면 알림 누락. snapshot-first 순서로 수정.

## [0.0.5.6] — 2026-04-29

### Added

- **NFSv4.1 SEQUENCE 리플레이 캐시** (`internal/nfs4server/`):
  - 슬롯별 at-most-once 시맨틱: 동일 (sessionID, slotID, seqID) 재요청 시 캐시된 COMPOUND 응답 반환.
  - RFC 5661 §2.10.5.1.1: SeqID 첫 요청 검증 (slotID seqID==0 이면 seqID==1 강제).
  - RFC 5661 §2.10.5.1.3: RETRY_UNCACHED_REP — cacheThis=1이었으나 캐시 없으면 NFS4ERR_RETRY_UNCACHED_REP(10070) 반환.
  - SlotEntry에 `WasCacheThis bool` 추가로 캐시 누락 구분.

- **NFSv4.1 클라이언트 관리** (`internal/nfs4server/`):
  - `OpDestroyClientID`: 클라이언트 세션 정리 + NFS4ERR_STALE_CLIENTID(10022) 검증.
  - `OpFreeStateID`, `OpTestStateID`: stub 구현 (GrainFS는 세밀한 stateid 추적 불필요).

- **NFSv4.1 MinorVer 검증**:
  - COMPOUND 요청의 minorversion 0/1/2만 허용; 그 외 NFS4ERR_MINOR_VERS_MISMATCH.
  - NFSv4.1+ 전용 연산(EXCHANGE_ID, CREATE_SESSION 등)을 v4.0 요청에서 NFS4ERR_OP_ILLEGAL로 차단.

- **NFSv4.2 연산 구현** (RFC 7862):
  - `OpSeek` (op 69): DATA/HOLE whence 처리. GrainFS에 sparse 없음; HOLE whence = EOF.
  - `OpAllocate` (op 59): 파일 크기 확장 (zero-fill read-modify-write).
  - `OpDeallocate` (op 62): 바이트 범위 zero-fill (hole punch 시뮬레이션).
  - `OpCopy` (op 60): 서버사이드 복사; savedFH → currentFH.
  - `OpIOAdvise` (op 63): 힌트 무시, 빈 비트맵 반환.

- **Colima e2e 파라미터화**: vers=4.0/4.1/4.2 동일 기능 테스트.

### Fixed

- **opAllocate TOCTOU 레이스**: HeadObject를 LockPath 바깥에서 호출하던 문제 수정; early-exit 판단을 락 내부로 이동.
- **opAllocate io.ReadAll 에러 무시**: `existing, _ = io.ReadAll(...)` → 에러 체크 추가; 부분 읽기 시 NFS4ERR_IO 반환.
- **opDeallocate uint64 오버플로우**: `offset + length` 오버플로우 클램프 처리.
- **opAllocate/Deallocate OOM 방지**: 64MB 초과 파일 → NFS4ERR_FBIG / NFS4ERR_NOTSUPP.

### Refactored

- **StateManager lock-free 전환**:
  - `clientMu + map[uint64]*ClientState` → `sync.Map` (lock-free 읽기).
  - `sessionMu + map[SessionID]*Session` → `sync.Map` (GetSession 핫패스 무락).
  - `exchSeq map[uint64]uint32` → `sync.Map[uint64]*atomic.Uint32` (ExchangeID 원자적 카운터).
  - `ClientState.Confirmed bool` → `atomic.Bool`.
  - `sessionMu` → `slotMu`로 이름 변경 (슬롯 상태 보호만 담당).

### Tests

- SEQUENCE 리플레이 캐시 유닛 테스트 (cache-hit, cache-miss, first-seqID 검증).
- OpSeek/Allocate/Deallocate/Copy/IOAdvise 유닛 테스트.
- Colima e2e: NFSv4.0/4.1/4.2 동일 기능 smoke 테스트.

## [0.0.5.5] — 2026-04-29

### Added

- **Raft §4.4 단일 서버 멤버십 변경** (`internal/raft/`):
  - `membership.go`: `AddVoter`, `RemoveVoter`, `AddLearner`, `PromoteToVoter` API.
  - `proposeConfChangeWait`: ConfChange proposal을 leader에게 보내고 커밋까지 대기.
  - `applyConfigChangeLocked`: 로그에 Append되는 시점(커밋 전)에 `config.Peers` 즉시 갱신 (§4.4 요구사항).
  - `rebuildConfigFromLog`: 로그 截断 후 메모리 config 재구성.
  - `SetMixedVersion(bool)`: 롤링 업그레이드 중 멤버십 변경 차단.
  - FlatBuffers `ConfChangeEntry` 스키마 (`raftpb/shard.fbs`): Op, ServerId, ServerAddress.

- **Chaos 시나리오**: `TestMembership_ReplaceNodeOneAtATime` — 3→4→3 노드 전환 중 가용성 유지 검증.

### Fixed

- **ID/address 혼용 버그**: `applyConfigChangeLocked`와 `rebuildConfigFromLog`에서 `cc.Address` → `cc.ID` 수정.
  라우팅은 ID 기준이므로 `config.Peers`에 주소가 들어가면 `nextIndex`/`matchIndex` 미생성.
- **TOCTOU 경쟁 조건**: `proposeConfChangeWait`의 check-release 패턴을 `flushBatch` 내부로 이동.
  같은 배치의 두 번째 ConfChange와 크로스-배치 중복 모두 `ErrConfChangeInProgress`로 거부.
- **dead error 활성화**: `ErrMixedVersionNoMembershipChange` 선언만 있던 코드에 실제 `mixedVersion` 필드 + `SetMixedVersion` + guard 구현.

### Tests

- `TestConfChange_AppliesOnAppendNotCommit_Leader`: flushBatch 직후 `config.Peers` 갱신 검증 (§4.4 leader path).
- `TestConfChange_MixedVersionRejected`: SetMixedVersion(true) 시 AddVoter 거부.
- `TestConfChange_RejectsConcurrentGoroutine`: 동일 배치 내 두 ConfChange 중 하나 즉시 거부.

## [0.0.5.4] — 2026-04-29

### Added

- **NFSv4 SETATTR 구현** (`internal/nfs4server/`):
  - **opSetAttr**: MODE(chmod), SIZE(truncate), TIME_MODIFY_SET(mtime) 속성 쓰기. `__meta` 사이드카 파일에 메타데이터 저장.
  - **opGetAttr**: `__meta` 사이드카에서 MODE, SIZE, MTIME 읽기.
  - **opCommit**: NoFH 체크 + Syncable fsync + WriteVerf(서버 재시작 감지).
  - **storage 인터페이스**: `Truncatable` + `Syncable` 추가, `LocalBackend` 구현.
  - **StateManager.WriteVerf**: 8바이트 write verifier, 서버 재시작 시 갱신.

### Fixed

- **SETATTR TIME_MODIFY_SET 비트 오류**: attr 54 = word1 bit 22; 기존 `bm1&(1<<21)` → `bm1&(1<<22)` 수정.
- **SETATTR XDR 오프셋 정렬**: TIME_ACCESS_SET (attr 48 = word1 bit 16) 소비 로직 추가.
- **SUPPORTED_ATTRS**: bit 16, 22 광고 추가.
- **e2e 빌드 오류**: PR #79 NFSv3 제거 후 `cow_e2e_test.go` 컴파일 불가 → S3 API로 대체.

### Tests

- SetAttr 10개, Commit 3개, SaveFH/RestoreFH, DestroySession, pool/XDR readOpArgs 커버리지.
- Colima e2e: mtime 검증 (TZ=UTC touch -t 202001010000 = 1577836800), chmod/truncate smoke.

## [0.0.5.3] — 2026-04-29

### Added

- **Raft PR 3 — Observer Pattern + Configuration Snapshot + Bootstrap** (`internal/raft/`):
  - **Observer pattern** (`internal/raft/observer.go`): `RegisterObserver`/`DeregisterObserver`/`notifyObservers`. COW `atomic.Value` + `observerMu`로 read lock-free, write serialize. 전달 non-blocking (full channel → drop).
  - **`EventLeaderChange`**: 리더 전환/step-down 시 발화. `IsLeader`, `LeaderID`, `Term` 필드. step-down 시 `LeaderID=""` (알려진 리더 없음).
  - **`EventFailedHeartbeat`**: AppendEntries RPC 실패 시 발화. `PeerID` 필드.
  - **`Configuration()`**: `n.mu` 보호 하에 현재 peer 목록 snapshot 반환. race-safe.
  - **`Bootstrap()`**: 첫 클러스터 부트스트랩 전용. 이미 부트스트랩됐거나 hard state(term/vote)가 있으면 `ErrAlreadyBootstrapped`. `LogStore.IsBootstrapped()`/`SaveBootstrapMarker()` 활용.
  - **`serve.go`**: 기동 시 `node.Bootstrap()` 자동 호출, `ErrAlreadyBootstrapped`는 정상 처리.
  - **단위 테스트 8건**: `TestObserver_*` 4건 + `TestBootstrap_*` 4건 (nil store no-op, 이중 호출 거부, 기존 hard state 감지, deregister/full-channel 경계).

## [0.0.5.2] — 2026-04-29

### Added

- **Raft PR 2 — Fast Log Backtracking + AE Payload Limit + TrailingLogs** (`internal/raft/`):
  - **`AppendEntriesReply`**: `ConflictTerm`/`ConflictIndex` 필드 추가 (FlatBuffers `shard.fbs` + 재생성 + codec). follower가 불일치 구간의 term과 시작 index를 리더에게 알려준다.
  - **`HandleAppendEntries`**: PrevLog 불일치 시 `ConflictTerm`/`ConflictIndex` 채우기. 불일치 term의 첫 번째 index를 스캔해 리더가 그 term 전체를 건너뛸 수 있도록 힌트 제공.
  - **`replicateTo` fast backtracking**: `applyConflictHint()` 추출. `ConflictTerm` 힌트로 `nextIndex`를 O(1) 점프 (이전: `nextIndex--` O(N)). 구버전 peer(`ConflictIndex=0`)는 자동 fallback.
  - **`Config.MaxEntriesPerAE`**: 단일 AppendEntries RPC 항목 수 한계 (default 512). 대량 페이로드로 인한 수신자 메모리 급증 방지.
  - **`Config.TrailingLogs` / `SnapshotConfig.TrailingLogs`**: 스냅샷 후에도 마지막 N개 항목 보존 (default 10240). lag < 10240인 follower는 InstallSnapshot 없이 AppendEntries로 catch-up 가능.
  - **`Node.snapshotIndex`**: 최근 스냅샷 Raft index 추적 필드.
  - **`metrics.go`**: `raft_conflict_term_jumps_total`, `raft_ae_split_count_total` Prometheus 카운터.
  - **단위 테스트 4건**: `TestAEReply_ConflictTermJumpsCorrectly`, `TestAEReply_FallbackToMinusOneForOldPeer`, `TestAESize_SplitsAtMaxEntries`, `TestTrailingLogs_KeepsLastN`/`ZeroRemovesAll`.
  - **chaos 시나리오**: `TestLaggedFollower_ConflictTermCatchup` — 12000-entry lag follower가 5초 내 catch-up.

## [0.0.5.1] - 2026-04-29

### Added

- **Raft PR 1b — Chaos Harness Phase 2: RequestVoteHook + Disrupting Prevention + Mixed-Version Upgrade** (`internal/raft/chaos/`):
  - **`RequestVoteHookFn`** (`chaos/transport.go`): per-destination-node 훅 타입. `(nil, true)` 반환 시 드롭, `(args, false)` 반환 시 (수정된) args 전달. partition/drop 체크 이후 발화.
  - **`SetRequestVoteHook`** (`ChaosTransport`, `Cluster`, `Driver`): 훅 설치/제거 API. 구버전 노드 시뮬레이션(PreVote 드롭)에 활용.
  - **`InjectRequestVote`** (`Cluster`): 모든 chaos transport 게이팅을 우회하고 `HandleRequestVote`를 직접 호출. Disrupting Prevention 검증에 활용.
  - **Chaos 시나리오 2건 추가**:
    - `TestDisruptingPrevention_HighTermVoteBlocked`: 팔로워에게 term+10 real RequestVote 주입 → stickiness가 거부 + 리더 유지 검증. PR 1a 리뷰 M3(옵션 A)의 후속 시나리오.
    - `TestMixedVersionRollingUpgrade_PreVoteGracefulFallback`: node-2가 PreVote를 드롭하는 혼합 버전 클러스터에서 node-0/1이 pre-vote 과반수(2/3)로 정상 선출되는지 검증. PR 1a baseline 주석의 명시적 요청 이행.
  - **단위 테스트 2건 추가** (`chaos/transport_test.go`): `TestChaosTransport_RequestVoteHook_API` — `applyRVHook` 직접 검증 (drop/pass-through/nil-remove). `TestChaosTransport_RequestVoteHook_ClusterElectsLeader` — 훅 하에서 클러스터 선출 통합 검증.

## [0.0.5.0] - 2026-04-29

### Added

- **Raft PR 1a — Pre-vote + CheckQuorum + Leader Stickiness** (`internal/raft/`): Raft 논문 §9.6 기반 선출 안정성 3종 세트.
  - **Pre-vote** (`runPreVote`): 후보가 term 증가 전 pre-vote 라운드 실행. 파티션 복귀 노드가 term을 부풀려 현 리더를 step-down 시키는 문제 차단.
  - **Leader Stickiness (Disrupting Prevention)** (`HandleRequestVote`): 팔로워가 `ElectionTimeout` 안에 AppendEntries를 받은 경우 RequestVote 거부. 리더 고정성 강화.
  - **CheckQuorum** (`hasQuorum`, `runLeader`): 리더가 3 heartbeat 주기(150ms) 내 과반수 ack 없으면 자발적 step-down. minority-side stale write 방지.
  - **`lastLeaderContact` 추적** (`HandleAppendEntries`, `HandleInstallSnapshot`): stickiness 판단 기준 타임스탬프.
  - **LeaderTransfer 호환성 수정** (`HandleTimeoutNow`, `runCandidate`, `RequestVoteArgs.LeaderTransfer`): `TransferLeadership` 호출 경로에서 stickiness gate 우회. `cluster/balancer.go` 프로덕션 사용처 대응.
  - **단위 테스트 8건** 추가 (`raft_test.go`): HasQuorum, HandleRequestVote pre-vote/stickiness, AppendEntries/InstallSnapshot lastLeaderContact, 2-node quorum, leader rejects pre-vote.
  - **Chaos 시나리오 3건 활성화** (`chaos/scenarios/`): `TestSplitBrain_PreVotePreventsLeaderDisruption`, `TestLeaderIsolation_CheckQuorumStepsDown`, `TestStaleLogElected_PreVotePreventsTermInflation` (t.Skip 제거).

## [0.0.4.46] - 2026-04-29

### Added

- **VFS write-amplification fix — Phase 1** (`internal/cluster/backend.go`, PR #77): `__grainfs_vfs_*` 버킷의 `PutObject`가 매번 새 ULID versionID를 생성해 NFS WRITE RPC 수 비례로 디스크가 무한 증가하던 문제 수정. 같은 key에 대한 덮어쓰기가 on-disk 1카피로 수렴.
  - **`vfsFixedVersion` 토글** (`atomic.Bool`, 기본 `true`): VFS 버킷에 고정 versionID `"current"` 사용. `--backend-vfs-fixed-version=false`로 rollback 가능.
  - **EC 비활성화 for VFS 버킷**: 고정 versionID + EC RingVersion 조합이 stale shard 누수를 유발할 수 있어 VFS 버킷은 N× 복제 경로로만 작동.
  - **`writeFileAtomic` (temp+rename)**: `putObjectNx`가 `os.WriteFile` 대신 임시 파일 생성 후 rename. POSIX rename 원자성으로 부분 쓰기 노출 없음; 동시 write 레이스는 last-writer-wins 보장.
- **`internal/storage.VFSBucketPrefix` / `IsVFSBucket()`**: 순환 의존성 없이 모든 레이어에서 VFS 버킷 판별 가능.
- **`grainFile.Write` pos 정합성 수정** (`internal/vfs/vfs.go`): `f.pos`를 무시하고 항상 buf에 append하던 구현을 pos 기반 in-place overlay(pos<len), extend(pos==len), zero-pad(pos>len) 3분기로 교체. Phase 2 rand_write_4k 측정에서 OOM 유발하던 O(n×writes) 메모리 팽창 해소.
- **테스트**: `internal/cluster/backend_vfs_test.go` — 고정 versionID 버전 누적 없음, 토글 on/off, 일반 버킷 ULID 유지, 32 고루틴 동시 write + 손상 없음 검증. `internal/storage/bucket_test.go` — `IsVFSBucket` / prefix 상수 불변성.

## [0.0.4.45] - 2026-04-29

### Added

- **Raft chaos test harness** (`internal/raft/chaos/`): PR #0 — in-memory fault injection 인프라. `ChaosTransport`가 `*raft.Node` 인스턴스 간 RPC를 동기 라우팅하며 세 가지 Driver 프리미티브를 제공:
  - `PartitionPeer`/`HealPartition`: 노드 양방향 네트워크 차단/복구
  - `DropMessage(from, to, n)`: 단방향 메시지 n건 드롭 (카운터 소진 후 정상 복귀)
  - `RestartNode`: 노드 Close 후 동일 Config으로 재시작 (in-memory state reset)
- **`Cluster` 하네스** (`chaos/cluster.go`): N-node 인메모리 클러스터. `WaitForLeader`, `CurrentLeader`, `NodeIDs`, `RestartNode` 제공. `t.Cleanup`으로 자동 Close.
- **5가지 시나리오 테스트** (`chaos/scenarios/`): split_brain, leader_isolation, stale_log_elected (3개 — PR 1a `t.Skip`), wal_torn_write (passing — BadgerDB atomic 커버리지), mixed_version_rolling_upgrade baseline (passing).
- **`Node.Close()` 고루틴 안전 종료** (`internal/raft/raft.go`): `replicateTo` 고루틴을 `wg`에 추적해 `Close()` 후 완전 종료 보장 (ghost goroutine 버그 수정).
- **TOCTOU 수정** (`resolveDelivery`): `shouldDeliver` + `lookup` 두 번의 락 획득을 단일 `resolveDelivery`로 합쳐 파티션 상태 변경 레이스 제거.

## [0.0.4.44] - 2026-04-28

### Fixed

- **Dashboard Self-Healing 배지가 cluster 모드에서 영구 "reconnecting"**: SigV4 인증 활성 시 (`--access-key`/`--secret-key` 설정된 cluster 배포) `/api/events/heal/stream`이 auth middleware로 인해 **403 Forbidden**을 반환. 브라우저 EventSource는 SigV4 헤더를 보낼 수 없으므로 onerror가 즉시 firing → 배지가 "reconnecting"에 갇힘. `/api/events`와 동일하게 read-only 모니터링 stream으로 분류해 auth bypass 리스트에 추가. 단일 노드/클러스터 모두 정상 연결 시 "live" 표시 확인. 회귀 방지 테스트(`internal/server/sse_auth_bypass_test.go`)에서 두 SSE endpoint가 모두 무인증으로 200 + `text/event-stream`을 반환하는지 검증.

## [0.0.4.43] - 2026-04-28

### Added

- **Web UI Cache Performance 패널**: Dashboard 탭에 Volume Block Cache + EC Shard Cache 카드를 추가. hit rate %, hits/misses 카운트, resident/capacity 게이지, eviction 카운터를 3초 주기로 갱신. `--block-cache-size=0` / `--shard-cache-size=0` 으로 비활성된 캐시는 "disabled" 배지 + "off" 표시. 게이지 막대는 80%+ hit rate에서 success 색, 90%+ resident에서 warning 색으로 전환되어 캐시 포화 임박을 시각적으로 알림.
- **`/api/cache/status` 단위 테스트** (`internal/server/cache_status_test.go`): 활성/비활성 두 상태에서 `block_cache` + `shard_cache` JSON 섹션이 모두 정확히 노출되는지, SigV4 auth bypass가 유지되는지 검증.

### Fixed

- **Dashboard false alert banner 노출**: `degraded-banner` / `alert-delivery-banner`가 백엔드는 `degraded:false`인데도 항상 보이던 버그. 원인: 두 banner의 인라인 `style="display:flex;..."`가 HTML `hidden` 속성의 기본 `display:none`을 specificity 차이로 이김. `el.hidden = true`로 JS가 토글해도 inline display가 살아남음. CSS에 `[hidden]{display:none !important}` 한 줄을 추가해 `hidden` 속성이 inline 스타일을 강제로 이기도록 수정. 단일 노드 / 정상 클러스터에서 false alert 사라짐.

## [0.0.4.42] - 2026-04-28

### Added

- **EC shard cache** (`internal/cache/shardcache/`): Phase 2 #3 follow-up 본구현. PR #71의 multi-node 측정에서 large 객체(>4 MB) 반복 GET이 90% shard hit rate 보였던 데이터를 근거로 도입. `getObjectEC`의 per-shard fan-out 앞단에 16-shard sharded LRU 캐시. 캐시 pre-pass에서 k개 shard를 모두 hit하면 디스크/네트워크 접근 없이 `ECReconstruct`만 실행 — full hit는 fan-out 자체를 skip. Partial hit는 missing slot에만 goroutine 발사. `--shard-cache-size` 플래그 (기본 256 MB; 운영 EC shard가 MB 단위라 per-shard 16 MB 슬롯이 필요해 64 MB 미만은 silent drop). Prometheus `grainfs_ec_shard_cache_*` 메트릭 + `/api/cache/status` JSON 응답에 `shard_cache` 섹션 추가. blockcache와 동일하게 actor 패턴 미적용 근거(hot-path channel round-trip 비용)를 소스에 명시.
- **Active-cache E2E 검증 테스트** (`tests/e2e/ec_shardcache_eval_test.go::TestE2E_ECShardCacheActive`): 3-node 클러스터에서 캐시 활성화 (`--shard-cache-size=256MB`) 후 16 MB 객체 ×10 GET — `/api/cache/status` 클러스터 합산 hit rate ≥80% 게이트. 측정 결과 85.7% (18 hit / 3 miss) — 첫 GET의 m-th shard만 cancel-race로 빠지고 이후 9회는 모두 cache pre-pass에서 short-circuit. 기존 `TestE2E_ECShardCacheEval`은 `--shard-cache-size=0`으로 simulator-only baseline 보존.

### Changed

- **getObjectEC fan-out drain 변경**: k+m shard goroutine을 띄우고 k개 응답만 받고 break하던 기존 로직을 모든 응답을 drain하도록 수정. cancel()은 그대로라 in-flight RPC는 즉시 abort되지만 이미 응답한 shard는 캐시에 populate. 이 변경 없이는 매번 m번째 shard가 캐시에 안 들어가서 실 hit rate가 k/(k+m) ceiling에 갇힌다. ECReconstruct는 여전히 k개로만 동작 — 추가 shard는 캐시 적재용.
- **`/api/cache/status` auth bypass 추가**: `/metrics`와 동일한 read-only 모니터링 엔드포인트라 SigV4 auth middleware skip 리스트에 합류. 운영 대시보드와 E2E 측정 테스트가 access-key 없이 캐시 통계 수집 가능.

## [0.0.4.41] - 2026-04-28

### Added

- **Read amplification simulator** (`internal/metrics/readamp/`): LRU 기반 캐시 시뮬레이터 — 실제 데이터를 캐싱하지 않고 hit/miss 카운터만 추적해서 "이 cache size를 도입했다면 hit rate은 얼마였을까"에 데이터로 답한다. 16/64/256 MB 동시 시뮬레이션을 volume.ReadAt + EC reconstruction + LocalBackend.GetObject에 와이어. `--measure-read-amp` 플래그로 ON (기본 OFF, 비활성 시 atomic.Bool load 1회/read 외 오버헤드 없음). Prometheus `grainfs_readamp_hits_total{tracker="..."}` + `_misses_total` 노출.
- **Volume block cache** (`internal/cache/blockcache/`): bounded sharded LRU (16 shards, FNV hash). 4 KB 블록 데이터 캐싱 + write/discard 시 invalidation. `--block-cache-size` 플래그 (기본 64 MB, 0 비활성). 측정 검증: 5000 blocks(20 MB) × 2-pass 워크로드에서 cold pass 251 ms → warm pass 3.5 ms — **72배 가속**, warm hit rate 100%. Prometheus `grainfs_block_cache_hits_total` / `_misses_total` / `_evictions_total` / `_resident_bytes` / `_capacity_bytes` 노출.
- **Read amplification 워크로드 평가 테스트** (`internal/volume/readamp_workload_test.go`, `internal/storage/readamp_workload_test.go`, `tests/e2e/ec_shardcache_eval_test.go`): volume layer 6종 + object layer 5종 + 3-node EC 3종 측정 결과를 design doc Phase 2 #3에 표로 보존. EC 측정에서 large object(>4 MB) 반복 GET이 90% shard hit rate 보임 → EC shard cache 본구현은 별도 PR로 분리.

### Changed

- **Phase 2 #3 Unified Buffer Cache — narrow scope 결정**: "Unified" 측정 결과가 path별로 매우 다른 결론. **Volume layer**: cache 부재 → 추가 시 Pareto 90% / locality 워크로드 50%-100% hit, 측정에서 72× wall-clock 가속 확인 → narrow `internal/cache/blockcache` 도입. **Object layer**: 기존 `CachedBackend(64 MB LRU)`가 이미 모든 locality 워크로드를 흡수, working set이 캐시 초과 시에만 0%→50% marginal win → 그대로 유지, UBC 추가 시 pure double-caching. EC shard layer는 multi-node telemetry 후 별도 결정. 자세한 데이터·재오픈 조건은 design doc Phase 2 #3 참조.
- **Lock-free 우선 원칙 적용 강화**: blockcache는 `sync.Mutex` 대신 (a) 16-shard sharded mutex로 contention 분산 (b) 카운터는 `atomic.Uint64` (c) actor goroutine 패턴 부적합 근거 (hot-path channel round-trip ≫ sharded mutex)를 소스에 명시. `volume.Manager.mu` 의 단일 mutex 사용 근거(cross-volume RMW 원자성)도 코드 주석에 풀어 적음.

## [0.0.4.40] - 2026-04-28

### Added

- **QUIC 내부 통신 압축 평가 벤치마크** (`internal/transport/compression_bench_test.go`): 6종 대표 payload(NodeStatsMsg gossip, ReceiptGossipMsg, HealReceipt JSON, PutObjectMetaCmd, raft AppendEntries batch, SetRingCmd)에 대해 zstd / s2(Snappy fork)의 압축비 + encode/decode round-trip 측정. 결과는 design doc에 보존.

### Changed

- **Phase 2 QUIC 내부 통신 압축 — 미구현 결정**: 측정 결과 raft_batch(45 KB AppendEntries)에서만 zstd가 32배 압축 + 1 Gbps에서 +311 µs 순이득. 그 외 모든 payload class(작은 gossip, receipt, single Raft cmd)는 압축/해제 cost가 wire saving을 초과. 10 Gbps DC LAN에서는 raft_batch조차 동률, 25 Gbps에서는 net loss. WAN/cross-region 배포가 표준이 되거나 batch-aware Raft pipelining이 도입될 때 재평가. 자세한 측정 데이터는 `~/.gstack/projects/gritive-grains/whitekid-master-design-20260427-180827-stability-perf-roadmap.md` Phase 2 #2 항목 참조.

### Fixed

- **NBD dedup E2E**: `TestNBD_Dedup`의 SavingsRatio 시나리오가 fresh volume에서 실패하던 문제 수정 (`docker/nbd-dedup-test.sh`). 새 볼륨은 `AllocatedBlocks=-1` (legacy 호환용 untracked sentinel)을 리턴하는데 스크립트의 `BASELINE+1` assertion이 -1+1=0 != 실제 1로 어긋남. 스크립트에서 `-1`을 `0`으로 정규화. 프로덕션 시맨틱은 변경 없음.

## [0.0.4.39] - 2026-04-28

### Added

- **Direct I/O on EC shard writes** (`internal/storage/directio/`, `cmd/grainfs/serve.go`): 새 `directio` 패키지 — Linux는 `O_DIRECT`, macOS는 `F_NOCACHE`, 그 외 플랫폼은 pass-through. `ShardService.WithDirectIO()` 옵션 + `--direct-io` 플래그(기본 `true`)로 활성. 1MB shard 쓰기는 ~10x, 4MB는 +40% 빨라짐 (Docker Linux VM 측정). overlayfs/일부 tmpfs가 `O_DIRECT`를 거부하면 자동으로 buffered 경로로 fallback — 운영자 조치 불필요.
- **Phase 2 research benchmarks** (`internal/cluster/shardio_directio_bench_test.go`): EC shard write 경로의 O_DIRECT vs default 비교 + 동시성 1/4/16/64 스케일링. 결과를 주석으로 함께 보존해 후속 결정의 근거 제공. `make test-e2e-docker`로 실행 가능.

### Changed

- **Phase 2 io_uring 평가 — 미구현 결정**: 동시성 벤치마크에서 throughput이 ~600-700 MB/s에서 plateau, 즉 disk fsync가 병목이고 syscall layer가 아님이 확인됨. io_uring의 가치 명제(batched submission, no per-op syscall)가 GrainFS의 fsync-dominated EC shard write에 적용되지 않음. WAL 등 append-only 경로 도입 시 재평가.

## [0.0.4.38] - 2026-04-28

### Added

- **EC degraded mode** (`internal/cluster/degraded_monitor.go`, `internal/server/server.go`): 클러스터에서 EC 데이터 샤드 수 미만으로 노드가 죽으면 PUT/POST/DELETE는 503을 반환하고 GET/HEAD는 계속 서비스. 30초 ticker로 active UDP probe 기반 liveness 측정 — PeerHealth가 shard I/O 없이는 갱신되지 않는 한계 보완. `Server.degradedFlag atomic.Bool`로 핫패스 제로 비용 체크. 단일 노드 모드에서는 `ECActive()` guard로 false-positive 방지.
- **Raft quorum-lost alert** (`internal/cluster/degraded_monitor.go`): 이 노드가 follower이고 leader가 없는 상태가 2 연속 tick(~60s) 지속되면 critical webhook 발송. `QuorumMinMatchIndex()`(GC 워터마크) 대신 `State() == Follower && LeaderID() == ""`로 판정 — write 없는 클러스터의 false positive 제거.
- **Predictive disk warnings** (`internal/cluster/disk_collector.go`, `cmd/grainfs/serve.go`): 80% warn / 90% critical 임계값을 가로지르는 transition 시점에 1회 webhook + zerolog. 같은 레벨 유지 중에는 침묵. `--disk-warn-threshold` / `--disk-critical-threshold` 플래그.
- **Operator-friendly errors** (`cmd/grainfs/serve.go`): BadgerDB open / Raft store / QUIC listen / encryption setup 실패 시 원인 + 복구 명령(lsof, --raft-addr=:0, --no-encryption 등) 포함. fmt.Errorf("%w\n recovery: ...") 패턴.
- **Startup config snapshot + drift detection** (`cmd/grainfs/serve.go`): 서버 시작 시 모든 플래그 값을 debug 로그로 덤프하고 `{dataDir}/.last-config.json`에 0o600 모드로 저장. 다음 시작 시 이전 스냅샷과 비교해 변경된 키만 info 레벨로 보고. 비밀(secret-key, cluster-key, alert-webhook-secret, heal-receipt-psk)은 `<redacted>` 처리.
- **대시보드 degraded 표시** (`internal/server/handlers.go`): `/api/cluster/status`에 `degraded` + `down_nodes` 필드. SSE hub로 실시간 broadcast.
- **단위 테스트 11종**: `degraded_monitor_test.go`(quorum 5종) + `disk_collector_test.go`(threshold transition 4종 추가) + `degraded_test.go` E2E(노드 3개 kill → PUT 503).

### Fixed

- **DegradedMonitor 단일 노드 false-positive** (`internal/cluster/degraded_monitor.go`): EC가 설정되어 있지만 클러스터 크기가 부족해 stripe 형성 불가능한 경우(`ECActive()` false) degraded 진입하던 버그. `DataShards == 0` 외에 `!ECActive()` 가드 추가. 단일 노드 EC 테스트가 503을 받던 회귀 해결.

### Changed

- **`alerts_api.go` secondary callbacks**: `sync.Mutex`에서 `atomic.Pointer[[]func(bool)]` CAS 패턴으로 교체. lock-free 핫패스 읽기.

## [0.0.4.37] - 2026-04-27

### Added

- **`--dedup` 플래그** (`cmd/grainfs serve`): `--dedup` 플래그로 블록 레벨 중복 제거 활성화. BadgerDB 인덱스를 `{data}/dedup/`에 생성. NFS·NBD·HTTP 서버가 단일 `volume.Manager` 인스턴스를 공유해 dedup 상태 일관성 보장.
- **CoW E2E 테스트 3종** (`tests/e2e/cow_e2e_test.go`): NFS 기반 스냅샷 롤백, 스냅샷 list/delete, 클론 라이프사이클 독립성 검증. `GRAINFS_DEDUP=1` 환경변수로 dedup 활성화 상태에서도 실행 가능.

### Fixed

- **`AllocatedBlocks` dedup 계산 버그** (`internal/volume/volume.go`): dedup 모드에서 블록 위치(position)가 아닌 실제 S3 객체(`res.IsNew`/`res.ToDelete`) 기준으로 카운트하도록 수정. 동일 내용 4회 쓰기 시 `AllocatedBlocks = 1` 정상 반환.
- **`Discard` dedup refcount 버그** (`internal/volume/volume.go`): refcount > 1인 객체 해제 시 `freed`를 잘못 증가시키던 버그 수정. S3 객체가 실제 삭제(`shouldDelete = true`)될 때만 카운트 감소.

## [0.0.4.36] - 2026-04-27

### Added

- **`WalkObjects` — O(1) 메모리 블록 순회** (`storage.Backend` 인터페이스, `LocalBackend`, `DistributedBackend`, `PackedBackend`, `SwappableBackend`): `ListObjects(maxKeys=1M)` 대신 콜백 기반 스트리밍 순회. `volume.Delete`, `Recalculate`, `CreateSnapshot`, `ListSnapshots`, `Clone` 5개 call site 교체. 초대형 볼륨에서 메모리 폭증 제거.
- **Peer fetch 응답 zero-copy** (`internal/cluster/shard_service.go`): `okResponse`/`errorResponse`에서 `marshalResponseDirect` 사용. 응답 FlatBuffer를 pooled builder로 직렬화한 뒤 `make+copy`하던 것을 non-pooled builder + `FinishedBytes()` 직접 반환으로 변경. 샤드당 1회 allocation 제거.
- **PGO 빌드 타겟** (`Makefile`): `make build-pgo` 추가. `make bench-profile`로 수집한 pprof CPU 프로파일로 `-pgo=<profile>` 빌드. 핫 경로에서 5–15% CPU 개선 가능. 프로파일 경로: `PGO_PROFILE ?= /tmp/grainfs-bench-cpu.out`.

### Fixed

- **NBD clean shutdown** (`cmd/grainfs/node_services.go`): `startNBDServer` 반환값을 버리던 버그 수정. `nodeServices.nbdSrv` 필드에 보관해 종료 시 `Close()` 호출.

## [0.0.4.35] - 2026-04-27

### Added

- **Block-level Deduplication Phase A** (`internal/volume/dedup/`, `internal/volume/`): 동일 내용 블록을 SHA-256 해시로 식별해 S3 객체 공유. BadgerDB로 레퍼런스 카운트 관리.
  - **`DedupIndex` 인터페이스** (`internal/volume/dedup/dedup.go`): `WriteBlock`/`ReadBlock`/`FreeBlock`/`DeleteVolume` — BadgerDB 기반 블록 매핑 + refcount 관리.
  - **BadgerDB 구현체** (`internal/volume/dedup/dedup.go`): `vd:h:{sha256hex}` → canonicalKey (해시 인덱스), `vd:r:{objectKey}` → {refcount int32 BE, hash [32]byte} (레퍼런스 테이블), `vd:b:{vol}:{blkNum:12d}` → canonicalKey (블록 매핑). `ErrConflict` 시 최대 3회 재시도.
  - **Manager 통합** (`internal/volume/volume.go`): `ManagerOptions.DedupIndex` 필드. WriteAt: `isNew=true` 일 때만 `backend.PutObject` 호출. Discard: refcount-aware 삭제 — 마지막 참조 제거 시에만 S3 객체 삭제.
  - **단위 테스트** (`internal/volume/dedup/dedup_test.go`): NewBlock/DedupHit/Overwrite/SameContent/ReadBlock/FreeBlock/DeleteVolume/ConcurrentWrites 11종.
  - **통합 테스트** (`internal/volume/volume_test.go`): DedupWriteSameContent/DedupDiscardReleasesRef/DedupOverwriteReleasesOldRef + P0 부분 쓰기 보존 회귀 테스트 4종.
  - **벤치마크** (`internal/volume/bench_test.go`): 로컬 FS 기준 NoDedup 40 MB/s → DedupHit 28 MB/s (−30%: BadgerDB txn 오버헤드). 실 S3 환경에서는 PUT 레이턴시가 지배적이므로 오버헤드 무시 가능.

### Fixed

- **P0: dedup 모드 부분 쓰기 데이터 손실** (`internal/volume/volume.go`): 기존 블록 읽기 시 `physicalKey(nil)` 대신 `m.dedup.ReadBlock()` + `GetObject(canonical)` 사용. 부분 오버라이트 시 블록 나머지 바이트가 0으로 덮어쓰여지던 버그 수정.
- **P1: PoolQuota dedup 모드 오버카운트** (`internal/volume/volume.go`): `HeadObject(physicalKey)` 항상 실패 → quota 과다 계산 버그. `m.dedup.ReadBlock()` 기반으로 대체.
- **P1: DeleteVolume BadgerDB 에러 무시** (`internal/volume/volume.go`): `toDelete, _ =` 패턴을 에러 전파로 수정.
- **P2: decodeRefVal 부패 데이터 silent 처리** (`internal/volume/dedup/dedup.go`): 36바이트 미만 페이로드를 0으로 반환하던 버그 → 에러 반환으로 수정.
- **P2: 볼륨 이름 ':' 포함 시 BadgerDB 키 충돌** (`internal/volume/dedup/dedup.go`): `WriteBlock` 진입 시 즉시 에러 반환.

## [0.0.4.34] - 2026-04-27

### Added

- **CoW Snapshot Phase B** (`internal/volume/`, `internal/server/`, `cmd/grainfs/`): 볼륨 스냅샷 + Copy-on-Write + 클론 전체 구현.
  - **FlatBuffers 스키마 확장** (`volumepb/volume.fbs`): `snapshot_count:int32 = 0` 필드 추가 — 하위 호환 기본값.
  - **live_map 메커니즘**: 논리 블록 번호 → 물리 오브젝트 키 매핑. `__vol/{name}/live_map` 에 탭 구분 텍스트로 persist. `SnapshotCount == 0` 이면 nil 반환으로 오버헤드 제로.
  - **`Manager.CreateSnapshot(name)`**: 현재 live_map을 스냅샷에 복사 → `__vol/{name}/snap/{snapID}/map` persist. SnapshotCount 증가. UUIDv7 기반 스냅샷 ID.
  - **`Manager.ListSnapshots(name)`**: 스냅샷 메타 + 블록 수 조회.
  - **`Manager.DeleteSnapshot(name, snapID)`**: 스냅샷 메타/블록 전체 삭제. SnapshotCount가 0이 되면 live_map 초기화 (CoW 모드 해제).
  - **`Manager.Rollback(name, snapID)`**: 스냅샷 live_map을 복원. 스냅샷 이후 추가된 CoW 블록 삭제.
  - **`Manager.Clone(srcName, dstName)`**: live_map + 모든 블록 복사(Copier 인터페이스 → CopyObject, 없으면 read+write fallback).
  - **CoW WriteAt**: `SnapshotCount > 0` 일 때 신규 UUIDv7 물리 키로 블록 기록 → live_map 갱신 → persist.
  - **5 HTTP 엔드포인트** (`internal/server/`): `POST /volumes/:name/snapshots`, `GET /volumes/:name/snapshots`, `DELETE /volumes/:name/snapshots/:snap_id`, `POST /volumes/:name/snapshots/:snap_id/rollback`, `POST /volumes/clone`.
  - **CLI 서브커맨드** (`cmd/grainfs/volume.go`): `volume clone`, `volume rollback`, `snapshot create`, `snapshot list`, `snapshot delete`.
  - **통합 테스트** (`internal/server/server_test.go`): `TestSnapshotHandlers_*` 4종 추가.
  - **유닛 테스트** (`internal/volume/volume_test.go`): `TestCreateSnapshot`, `TestListSnapshots`, `TestSnapshotIsolation`, `TestDeleteSnapshot`, `TestClone`, `TestNoOverheadWithoutSnapshots`, `TestLiveMapParseRoundTrip` 등 10종 추가.

## [0.0.4.33] - 2026-04-26

### Added

- **`grainfs volume recalculate <name>`** (`cmd/grainfs/volume.go`): `AllocatedBlocks` drift 복구 CLI. `POST /volumes/:name/recalculate` 호출 후 before/after 출력.
- **`Manager.Recalculate(name)`** (`internal/volume/volume.go`): ListObjects로 실제 블록 수 재산정, drift 있을 때만 메타데이터 갱신. `maxBlockListLimit = 1_000_000` 상수 도입 (Delete()도 공유).
- **`POST /volumes/:name/recalculate`** (`internal/server/`): recalculate HTTP 엔드포인트. `{"volume","before","after","fixed"}` JSON 응답.
- **No-auth 경고** (`cmd/grainfs/serve.go`): `--access-key`/`--secret-key` 미설정 시 서버 기동 시 `WARN` 로그 출력.
- **NBD 서버 크로스 플랫폼** (`internal/nbd/nbd.go`, `cmd/grainfs/serve_nbd.go`): `//go:build linux` 태그 제거. NBD 서버 자체는 모든 OS에서 빌드·실행 가능(클라이언트 `nbd-client`는 여전히 Linux 필요).

### Changed

- **`--nbd-port` 설명 개선**: `Linux only` → `Client-side nbd-client still requires Linux.`

### Refactored

- **`volume.ErrNotFound` sentinel** (`internal/volume/volume.go`): "not found" 에러를 문자열 매칭 대신 `errors.Is()` 로 처리. 기존 `strings.Contains(err.Error(), "not found")` 패턴 제거.

## [0.0.4.32] - 2026-04-26

### Fixed

- **make test e2e 테스트 격리** (`Makefile`): `UNIT_PKGS` 변수 도입으로 `make test`/`make test-race`에서 `tests/e2e` 제외. E2E 서버 기동이 유닛 테스트 QUIC handshake 고루틴을 CPU 기아 상태로 만들어 발생하던 `TestQUICTransport_ThreeNodes` 간헐적 실패 수정.

## [0.0.4.31] - 2026-04-25

### Added

- **Thin Provisioning Phase A** (`internal/volume/`): 블록 단위 할당 추적 + TRIM + PoolQuota.
  - `Volume.AllocatedBlocks int64`: `-1`=untracked(기존 볼륨), `0`=빈 볼륨, `>0`=블록 카운트. FlatBuffer `allocated_blocks = -1` 기본값으로 하위 호환.
  - `Volume.AllocatedBytes()`: `AllocatedBlocks * BlockSize` 반환 (`-1`이면 `-1`)
  - `Manager` 인메모리 캐시 (`volumes map[string]*Volume`): S3 메타데이터 왕복 제거
  - `Manager.WriteAt`: 신규 블록(`GetObject` 실패) 감지 → `AllocatedBlocks` 증가 후 메타 persist
  - `Manager.Discard`: ceil/floor 수학으로 완전 커버된 블록만 삭제 + 카운터 감소 (0 미만 clamping)
  - `ManagerOptions.PoolQuota`: `HeadObject` 기반 pre-scan으로 쓰기 전 풀 전체 용량 초과 방지 (`ErrPoolQuotaExceeded`)
  - **NBD TRIM**: `NBD_CMD_TRIM(4)` 핸들링 + `NBD_FLAG_SEND_TRIM`(bit 5) handshake 협상 → `Manager.Discard` 연동
  - **REST API**: `GET/POST /volumes/:name` 응답에 `allocated_bytes`, `allocated_blocks` 필드 추가

## [0.0.4.30] - 2026-04-25

### Added

- **Consistent Hash Ring** (`internal/cluster/ring.go`, `ring_store.go`): 결정론적 EC 샤드 배치 알고리즘.
  - `Ring.PlacementForKey(cfg, key)`: 가상 노드 기반 CW 탐색으로 k+m 배치 결정
  - `ringStore`: 링 버전 관리 + ref counting + GC eligibility
  - `FSM.applySetRing`: Raft 커밋 → in-memory ring store 반영
  - `CmdPutShardPlacement`/`CmdDeleteShardPlacement`: no-op 전환 (배치 레코드 불필요)

- **Follower Write Forwarding** (`internal/cluster/backend.go`): 팔로워 propose → 리더 QUIC 포워딩.
  - `propose()`: IsLeader() 분기 — 리더면 직접, 팔로워면 QUIC StreamProposeForward 전달
  - `RegisterProposeForwardHandler()`: 리더 핸들러 등록
  - `internal/raft/raft.go`: `IsLeader()` 메서드 추가

- **Ring-aware EC 읽기/쓰기** (`backend.go`): 3단계 fallback 경로.
  1. Ring 기반 재계산 (RingVersion > 0)
  2. FSM placement record (레거시)
  3. 오브젝트 메타 NodeIDs (CmdPutShardPlacement no-op 과도기)

- **ReshardManager 링 인식** (`reshard_manager.go`): 멤버십 변경 후 오브젝트 링 전환 리샤드.

- **PutObjectMetaCmd 확장** (`fsm.go`, `codec.go`): `RingVersion`, `ECData`, `ECParity`, `NodeIDs` 필드 추가.

### Fixed

- `deleteShardsAsync`: bucket="" 전달 버그 수정 → 올바른 샤드 경로 삭제
- `applyDeleteObjectVersion`: 링 refcount decRef 누락 수정 → ring GC 누출 방지
- `putObjectEC`: 배치 크기 != k+m 시 명시적 오류 반환 (panic 방지)
- `ReshardToRing`: EC 재구성 후 ETag 검증 추가

## [0.0.4.29] - 2026-04-25

### Refactored

- **DegradedTracker Actor 전환** (`internal/alerts/`): mutex 제거 → actor 고루틴 단일 소유 패턴.
  - `Report/Degraded/Status`: `stopCh` guard로 Stop 이후 블로킹 방지
  - `processReport/checkFlapThreshold`: actor goroutine 전용, mutex contention 완전 제거
  - `recentTransitions`: copy-on-trim으로 backing array 누수 방지
  - `OnStateChange` 콜백: actor goroutine 직렬화 → Prometheus gauge와 상태 불일치 zero window
  - `OnHold` 콜백: flap 임계 초과 시 critical webhook 발송

- **receiptTrackingEmitter Actor 전환** (`internal/server/`): mutex 제거 → actor goroutine + sync.Once.
  - `Close()`: `sync.Once` 래핑으로 double-close panic 방지
  - `FinalizeSession`: reply 수신에 `stopCh` guard 추가 — Close 이후 영구 블로킹 방지
  - `sessionCount`: drain-then-count 패턴으로 Emit 순서 보장

- **scrubber Stats Actor-snapshot** (`internal/scrubber/`): `atomic.Value` 스냅샷으로 hot-path mutex 제거.

### Fixed

- `AlertsState.Close()` 추가 + `Server.Shutdown()` 연동: DegradedTracker actor goroutine 누수 방지
- `fix(actor)`: Report/Degraded/Status reply 수신 — Stop/Close race 시 영구 블로킹 방지
- `OnStateChange`에서 `require.False` → `assert.False`: actor goroutine에서 `t.Fatal` 호출 방지

## [0.0.4.28] - 2026-04-25

### Performance

- **Phase 18 — Actor 패턴 + EC 병렬 팬아웃** (`internal/cluster/`): 핫패스 뮤텍스 제거, EC 병렬화로 처리량 향상.
  - `putObjectEC` / `getObjectEC` / `upgradeObjectEC`: `errgroup` 병렬 팬아웃 — EC 쓰기/읽기 Σ(latency) → max(latency)
  - `getObjectEC` k-of-n 조기 종료: k 샤드 수신 즉시 나머지 고루틴 취소 (context cancel)
  - `Registry.InvalidateAll`: 직렬 호출 → `sync.WaitGroup` 병렬 팬아웃 — 캐시 무효화 지연 Σ → max
  - `BalancerProposer` Actor 전환: `sync.Mutex` 전면 제거, `chan balancerMsg` 기반 단일 소유자 패턴
  - `MigrationExecutor.Stop()`: `sync.Once` 래핑으로 이중 호출 안전성 보장

### Fixed

- `NotifyMigrationDone`: blocking send → non-blocking select/default — FSM Apply 고루틴 stall 방지
- `Status()`: `stopCh` guard 추가 — `Run()` 종료 후 호출 시 영구 데드락 방지
- `getObjectEC`: k-of-n `context.Canceled`를 peer failure로 잘못 분류하던 버그 수정
- `upgradeObjectEC`: `peerHealth` 추적 누락 — `putObjectEC`와 동일한 패턴으로 통일

### Changed

- `3*time.Second` magic number → `const shardRPCTimeout` (backend.go)
- `64` channel buffer magic number → `const balancerChanBuf` (balancer.go)

## [0.0.4.27] - 2026-04-25

### Performance

- **Phase 17 — Lock-free hot path** (`internal/nfs4server/`, `internal/cluster/`, `internal/raft/`): mutex contention 제거로 핫패스 최대 16.7×(193.6 → 11.62 ns/op) 단축.
  - `StateManager.GetOrCreateFH` CoW + `atomic.Pointer` — 16.7× 개선 (193.6 → 11.62 ns/op, 8 CPU); lock-free read, writer-only clone
  - `NodeStatsStore.Get` CoW + `atomic.Pointer` — lock-free read; `Set`은 1-writer 복사 후 스왑
  - `raft.batcherLoop` `adaptiveMetrics` 분리 — `Node.mu` 경합 제거; 독립 `sync.Mutex` + 지수 백오프 튜닝
  - `BenchmarkMutexContention` + mutex profile infra 추가 (`internal/nfs4server/`, `internal/vfs/`)

### Added

- **`--pprof-port` flag** (`cmd/grainfs/serve.go`): 지정 포트에서 `net/http/pprof` 노출 (0 = 비활성화).
  - `runtime.SetMutexProfileFraction(1)` + `SetBlockProfileRate(1)` 자동 활성화
  - `docker/nbd-test.sh`: `GRAINFS_PPROF=1` 환경변수로 CPU·allocs·mutex·goroutine 프로파일 자동 수집
  - E2E `GRAINFS_PPROF=1` 연동 (`tests/e2e/helpers_test.go`)

## [0.0.4.26] - 2026-04-25

### Changed

- **zerolog 전면 마이그레이션** (`internal/`, `cmd/`): `log/slog` → `github.com/rs/zerolog` v1.35.1.
  - 전체 48개 파일, 구조화 로그 필드 일관성 확보 (`Str`, `Int`, `Err` 등)
  - SSE 대시보드 팬아웃 재구현: `BroadcastHandler(slog)` → `broadcastWriter(zerolog.LevelWriter)`
  - `HasSubscribers()` atomic guard: 무구독자 시 브로드캐스트 alloc 제로
  - `WriteLevel()` InfoLevel 필터: Debug/Trace 로그가 비인증 `/api/events` SSE로 누출되지 않음
  - `broadcastLoggerOnce sync.Once`: 다중 Server 인스턴스(테스트 등)에서 global logger 이중 래핑 방지
  - `internal/server/slog_handler.go` 삭제 (BroadcastHandler 제거)

## [0.0.4.25] - 2026-04-24

### Performance

- **NFS COMPOUND round-trip heap alloc ≤2** (`internal/nfs4server/`): Zero-Alloc Phase 3 완료.
  - `XDRReader.r bytes.Reader` embed-by-value → 1 alloc 제거 (`xdrReaderPool` + `pool *sync.Pool` 필드)
  - `XDRWriter sync.Pool` + cap guard (`putXDRWriter`: cap > 64KB → 폐기)
  - `opArgPool8/16 sync.Pool` + full-cap restore (`putOpArg8/16`: `b[:cap(b)]` 후 Put)
  - `Op.poolKey int` 필드 → `Dispatch` 루프에서 `putOpArg8/16` 자동 반환
  - `ParseCompound` into-req 시그니처: caller 소유 req 직접 채움, `compoundReqPool`
  - `Dispatch` into-resp 시그니처 + `compoundRespPool` / `dispatcherPool`
  - `encodeCompoundResponseInto` + `handleCompoundInto`: Into 패턴으로 `EncodeCompoundResponse` / `BuildRPCReply` alloc 제거; `handleConn`이 단일 pooled writer 소유
  - `opGetAttr` 인라인: `encodeMinimalDirAttrs()` / `encodeFileAttrs()` 중간 writer 제거

## [0.0.4.24] - 2026-04-24

### Performance

- **NFS XDR WriteUint32/WriteUint64 zero-alloc** (`internal/nfs4server/xdr.go`): `make([]byte, 4/8)` → `var b [4/8]byte` 스택 배열로 교체. 인코딩 핫패스에서 heap alloc 제거.
- **NFS XDR ReadUint32/ReadUint64 stack escape 방지** (`internal/nfs4server/xdr.go`): `io.ReadFull(r.r, b[:])` → `r.r.Read(b[:])` 직접 호출로 `bytes.Reader`의 인터페이스 dispatch를 통한 스택 배열 escape 방지.
- **NFS RPC writeRPCFrame zero-alloc** (`internal/nfs4server/rpc.go`): `make([]byte, 4)` → `var header [4]byte`. TCP 프레임 헤더 heap alloc 제거.
- **NFS RPC readRPCFrame single-fragment fast-path** (`internal/nfs4server/rpc.go`): 단일 fragment(일반적 경우) pre-alloc으로 result slice 불필요한 재할당 방지.
- **NFS opRead sync.Pool** (`internal/nfs4server/compound.go`): `&bytes.Buffer{}` → `sync.Pool` 재사용. NFS READ 응답 조립 버퍼의 per-RPC heap alloc 제거.
- **VFS grainFile bytes.Buffer sync.Pool** (`internal/vfs/vfs.go`): Open/Write/Close 경로의 `bytes.Buffer`를 `sync.Pool`로 풀링. 파일 단위 heap alloc 감소.
- **S3Auth DecodeAWSChunkedBody Grow 사전할당** (`internal/s3auth/chunked.go`): `result.Grow(len(data))` 추가로 청크 디코딩 중 `bytes.Buffer` 재할당 방지.

### Fixed

- **VFS ReadAt io.ReaderAt 계약 준수** (`internal/vfs/vfs.go`): 스트리밍 패스에서 `rc.Read(p)` → `io.ReadFull(f.rc, p)`로 교체. 단축 읽기 시 비-nil 에러 반환으로 NFS 클라이언트 데이터 잘림 방지.
- **VFS O_RDONLY grainFile pool buffer 누수** (`internal/vfs/vfs.go`): Seek/ReadAt 호출로 buf 모드 전환된 읽기 전용 파일이 Close 시 pool buffer를 반환하지 않던 버그 수정.


## [0.0.4.23] - 2026-04-24

### Performance

- **ECSplit double-append → copy** (`internal/cluster/ec.go`): shard 조합 루프에서 `make(0,cap)+append×2` → `make(size)+copy×2`로 교체. capacity는 이미 정확히 예약되어 있으므로 append 호출 오버헤드(slice header 갱신, 경계 체크) 제거. `BenchmarkECSplit` 추가.
- **grainFile 스트리밍 모드** (`internal/vfs/vfs.go`): 읽기 전용 Open 시 `loadExisting()` 없이 `rc io.ReadCloser`를 저장해 스트리밍 모드로 진입. `ReadAt(off==pos)` 순차 접근에서 rc 직접 읽기(NFS READ RPC 핫패스); `ReadAt(off!=pos)` 랜덤 접근 및 `Seek()` 호출 시 `loadExisting()` fallback으로 buf 모드 전환. `Close()` 시 rc 미소비 리소스 릭 방지. EC 백엔드 경로의 2nd copy(VFS 버퍼링) 제거.

## [0.0.4.22] - 2026-04-24

### Performance

- **EncryptWithAAD 3→1 alloc** (`internal/encrypt/encrypt.go`): nonce를 별도 변수 대신 `out[2:14]` sub-slice로 사용해 heap 탈출 제거. alloc 3→1.
- **FlatBuffers Builder sync.Pool** (`internal/cluster/codec.go`, `internal/storage/codec.go`, `internal/raft/quic_rpc_codec.go`, `internal/raft/store.go`, `internal/volume/codec.go`): 5개 패키지에 패키지별 Builder pool 추가. `fbFinish`/`fbFinishRPC`에서 `b.Reset()+Pool.Put()`. make+copy는 BadgerDB/Raft 소유권 이전 상 유지.
- **Reed-Solomon encoder 캐싱** (`internal/cluster/ec_pool.go`): `sync.Map` 기반 ECConfig별 encoder 캐시. `reedsolomon.New()` alloc 제거. ECSplit 59→11, ECReconstruct 48→3 alloc.
- **encodeShardHeader 스택 배열** (`internal/cluster/ec.go`): 반환 타입 `[]byte`→`[8]byte`로 변경, 8B heap alloc 제거. 컴파일러가 inline+stack 배치.

## [0.0.4.21] - 2026-04-24

### Fixed

- **E2E timeout 예산 부족** (`Makefile`): `make test-e2e` timeout 300s → 600s. TopologyChange(~213s) + ECSpike(~43s) ≈ 256s 소진으로 ClusterScrubber 테스트가 시간 초과되던 문제 해결.
- **ClusterScrubber E2E skip guard 제거** (`tests/e2e/cluster_scrubber_test.go`): 오진된 "port/dir contention" skip guard 제거. 실제 원인은 300s global timeout이었으며, 600s 예산으로 전체 suite에서 안정적으로 통과.
- **ShardPlacementMonitor onMissing repair 실패** (`cmd/grainfs/serve.go`): `IterShardPlacements`가 `key = objectKey+"/"+versionID` 형식으로 shardKey를 전달하는데, `onMissing` 콜백이 이를 그대로 `RepairShardLocal(bucket, shardKey, "", shardIdx)` 로 넘겨 `LookupLatestVersion`이 "Key not found"로 실패하던 버그. shardKey를 마지막 `/` 기준으로 분리해 objectKey, versionID를 각각 전달하도록 수정.

## [0.0.4.20] - 2026-04-24

### Added

- **EC→EC reshard 검증 테스트** (`internal/cluster/reshard_manager_test.go`):
  - `TestReshardManager_Run_UpgradesECObjects_OnKMismatch`: ReshardManager가 저장된 k,m이 effective config와 다를 때 `upgradeObjectEC`를 호출하는 경로 검증. N×→EC 변환과 EC→EC 업그레이드가 한 pass에서 동시 처리됨을 확인.
  - `TestReshardManager_Run_SkipsECObjects_OnKMatch`: k,m이 일치하는 EC 객체는 skip(converted=0, skipped=1)되는 동작 검증.
  - `TestUpgradeObjectEC_RoundTrip`: 실제 `DistributedBackend` + 로컬 `ShardService`로 EC→EC 라운드트립 통합 테스트. k=2,m=1 shard를 seed → `upgradeObjectEC(k=3,m=2)` 호출 → Raft 커밋 확인 → `GetObject`로 데이터 무결성 검증.

### Fixed

- **Raft no-op on leader election** (`internal/raft/raft.go`, `internal/cluster/backend.go`, `internal/cluster/fsm.go`, `internal/cluster/codec.go`, `internal/cluster/apply.go`): 새 리더 선출 시 이전 term entry를 `advanceCommitIndex`가 커밋하지 못하는 문제 해결. `CmdNoOp CommandType = 0` 추가, `NewDistributedBackend`에서 no-op 명령을 Raft Node에 등록, `runLeader()` 진입 시 자동 propose. `TestE2E_ClusterEC_3Node_ActiveKM21` 플레이크 근본 원인 수정.
- **`persistLogEntries` panic race** (`internal/raft/raft.go`): `node.Stop()` 후 `logStore.Close()` race로 발생하던 "DB Closed" panic. `stopCh` 체크로 정지 중 store 쓰기 실패 suppress.

## [0.0.4.19] - 2026-04-24

### Performance

- **BinaryCodec.EncodeWriterTo** (`internal/transport/codec.go`): `io.WriterTo` 인터페이스를 이용해 FlatBuffers Builder의 `FinishedBytes()`를 QUIC 스트림에 make+copy 없이 직접 기록. 기존 `Encode`는 항상 슬라이스 복사가 발생했으나 이 경로는 제로-카피.
- **QUICTransport.CallFlatBuffer** (`internal/transport/quic.go`): `*FlatBuffersWriter`를 받아 `EncodeWriterTo`로 전송하는 전용 메서드 추가. ShardService RPC(Write/Read/Delete) 전체가 이 경로를 사용하도록 전환.
- **ShardService buildShardEnvelope** (`internal/cluster/shard_service.go`): WriteShard/ReadShard/DeleteShards 세 메서드가 make+copy 기반 `marshalShardRequest` 대신 `buildShardEnvelope` + `CallFlatBuffer`를 사용하도록 재작성. 클러스터 shard RPC 당 최소 1회 힙 할당 제거.
- **vfs.Rename io.Pipe** (`internal/vfs/vfs.go`): 파일 이동 시 소스 파일을 전량 메모리에 읽은 뒤 복사하던 방식에서 `io.Pipe` + goroutine 스트리밍으로 교체. 힙 사용량 5MB(파일 크기) → 76KB(OS 파이프 버퍼).

### Fixed

- **ShardService pool 누출 방지** (`internal/cluster/shard_service.go`): `CallFlatBuffer` 패닉 시 FlatBuffers builder가 `shardBuilderPool`로 반환되지 않는 문제를 `defer`로 수정.

## [0.0.4.17] - 2026-04-24

### Fixed

- **LookupShardPlacement 리턴 타입 테스트 반영** (`internal/cluster/shard_placement_test.go`, `internal/cluster/ec_fix_test.go`): v0.0.4.14에서 `LookupShardPlacement` 반환 타입이 `[]string` → `PlacementRecord{Nodes,K,M}`으로 변경되었으나 테스트 업데이트가 누락되어 13개 단위 테스트가 실패하던 문제 수정. `got` → `got.Nodes`, `assert.Nil(t, got)` → `assert.Equal(t, PlacementRecord{}, got)` 등 기계적 반영.

## [0.0.4.16] - 2026-04-24

### Removed

- **`--cluster-ec` CLI 플래그**: 3노드 이상 클러스터에서 EC를 끌 실무적 이유가 없고 테스트 flakiness만 유발하던 토글 제거. EC는 `clusterSize >= MinECNodes(3)`일 때 항상 활성, 1-2 노드는 N× replication으로 자동 fallback.
- **Per-bucket EC policy API**: `PUT /:bucket?ec=true|false` 엔드포인트, `GET /admin/buckets/ec` 대시보드 API, `CmdSetBucketECPolicy` Raft 명령(opcode 17 회수), `SetBucketECPolicyCmd` FlatBuffers 테이블, `DistributedBackend.SetBucketECPolicy`/`FSM.GetBucketECEnabled`, `ECPolicySetter`/`ECPolicyProvider` 인터페이스 전부 제거.
- **`ECConfig.Enabled` 필드** (`internal/cluster/ec.go`): 더 이상 수동 on/off가 없으므로 필드와 `IsActive`/`EffectiveConfig`의 `Enabled` 체크 제거.
- **`SnapshotBucket.ECEnabled` 필드** (`internal/storage/storage.go`): per-bucket 정책 소멸에 따라 스냅샷 페이로드에서 제거.
- **`tests/e2e/ec_policy_test.go`**: skip 처리되어 있던 `TestBucketECPolicy_Toggle` 포함 파일 전체 삭제.
- **`TestECSpike_RawShardP95` + `startEcspikeClusterNoEC`** (`tests/e2e/cluster_ecspike_test.go`): `--cluster-ec=false` 기반 "raw shard p95" 측정 경로가 불가능해져 제거. Phase 18 Stage 3 CONDITIONAL GO 게이트는 이미 통과.

## [0.0.4.15] - 2026-04-23

### Fixed

- **Cluster EC topology change 라이브락** (`tests/e2e/cluster_ec_test.go`): `TestE2E_ClusterEC_TopologyChange`가 stage 1/stage 2 노드의 Raft peer 구성 불일치로 인한 리더 step-down 라이브락으로 실패했던 문제 해결. 모든 6개 노드가 시작부터 동일한 6-node peer 리스트 사용 (`startNodeStage1` 제거). Stage 2 노드의 election timeout이 발생해도 leader는 이미 모든 peer를 알고 있으므로 heartbeat로 방지.
- **`liveNodes()` peerHealth 필터링** (`internal/cluster/backend.go`): Raft peer 리스트는 정적이라 죽은 노드를 계속 포함 → `putObjectEC`가 write-all consistency 정책으로 dead node에 shard 쓰기 시도 → 전체 PUT 실패. `liveNodes()`가 `peerHealth`-unhealthy peer를 제외하도록 수정. 첫 실패가 peer를 unhealthy로 마킹한 뒤 재시도에서 N-1 노드 기반 placement 사용.
- **`putObjectEC` write 타임아웃** (`internal/cluster/backend.go`): 원격 shard 쓰기에 3s per-shard 타임아웃 추가 (read-side와 동일). 죽은 peer가 QUIC `quicMaxIdleTimeout=10s`까지 블록하지 않고 빠르게 실패.
- **E2E 테스트 `time.Sleep` 제거** (`tests/e2e/cluster_ec_test.go`): `TestE2E_ClusterEC_PutGet_5Node`의 `time.Sleep(3s)`를 `require.Eventually`로 교체. `getObjectEC`의 3s per-shard 타임아웃이 이미 dead-node 감지를 보장.
- **Cluster 테스트 cluster-ec 플래그 정합성** (`tests/e2e/*.go`): 다른 e2e 테스트 파일들의 `--ec=false`를 `--cluster-ec=false`로 통일.
- **Follower default bucket 생성 실패 처리** (`cmd/grainfs/serve.go`): 클러스터 모드에서 follower 노드의 default bucket 생성 실패는 경고만 하고 서버 시작을 차단하지 않음 (leader만 propose 가능).

## [0.0.4.14] - 2026-04-23

### Changed

- **Dynamic EC 파라미터** (`internal/cluster/ec.go`): `IsActive` 임계값을 `n>=k+m`에서 `n>=MinECNodes=3`으로 변경. 3노드 이상에서 항상 EC 활성화. `EffectiveConfig(n, target) ECConfig` 함수 추가 — n에 비례한 k,m 계산 (공식: `m_eff=max(1,round(n×m/(k+m)))`, `k_eff=n-m_eff`). 예: n=3, 4+2 target → k=2,m=1.
- **PlacementRecord** (`internal/cluster/shard_placement.go`): `LookupShardPlacement` 반환 타입 `[]string` → `PlacementRecord{Nodes,K,M}` 변경. 인코딩 포맷 단순화 (`<k><m><count><nodes>`). `ECConfigOrFallback` 헬퍼 추가.
- **EC 쓰기 시 k,m 저장** (`internal/cluster/backend.go`): `putObjectEC`와 `ConvertObjectToEC`가 `EffectiveConfig`로 실제 k,m 계산 후 `PutShardPlacementCmd`에 K,M 기록. `getObjectEC`는 저장된 k,m으로 재구성 (클러스터 확장 후에도 기존 객체 정확히 재구성).
- **EC→EC 업그레이드** (`internal/cluster/reshard_manager.go`): 클러스터 확장으로 effective k,m이 바뀐 경우 기존 EC 객체를 새 k,m으로 재인코딩. `upgradeObjectEC` 메서드 구현 (reconstruct→re-encode→fan-out→propose→delete old shards).
- **FlatBuffers 스키마** (`internal/cluster/clusterpb/cluster.fbs`): `PutShardPlacementCmd`에 `k:int32=0; m:int32=0` 필드 추가 (backward compatible). `flatc` 재생성.
- **CLI 플래그 설명** (`cmd/grainfs/serve.go`): `--ec-data`/`--ec-parity`가 "고정 k,m"이 아닌 "target max k,m"임을 명확히 표기.

### Breaking

- **E2E 테스트 재작성** (`tests/e2e/cluster_ec_test.go`): `TestE2E_ClusterEC_FallbackToNx_3Node` → `TestE2E_ClusterEC_3Node_ActiveKM21`. 3노드가 N×로 fallback하는 것이 아닌 EC(2,1)으로 활성화됨을 검증.

## [0.0.4.13] - 2026-04-23

### Added

- **Topology change E2E 테스트** (`tests/e2e/cluster_ec_test.go`): `TestE2E_ClusterEC_TopologyChange` 추가. 5-node 3+2 클러스터에서 노드 제거 후 기존 객체의 FSM placement record 불변성 검증 (GET 재구성 가능), 그리고 신규 객체가 변경된 liveNodes() 집합을 기반으로 올바르게 기록됨을 확인.
- **Min-node 파라미터화** (`tests/e2e/cluster_ec_test.go`): `TestE2E_ClusterEC_FallbackToNx_3Node`에 `ecData=4`, `ecParity=2` 상수 추가. 로그 메시지의 하드코딩 `k+m=6`을 동적 `k+m=%d` 포맷으로 교체.

## [0.0.4.12] - 2026-04-23

### Fixed

- **LookupShardPlacement 반환 타입** (`internal/cluster/shard_placement.go`): `([]string, bool)` → `([]string, error)` 변경. `ErrKeyNotFound`는 `(nil, nil)`로, 실제 BadgerDB 오류는 `(nil, err)`로 구분. N× fallback 경로에서 오류를 무시하던 silent data-loss 가능성 차단.
- **Placement record lifecycle** (`internal/cluster/apply.go`): `applyDeleteObject` tombstone 경로에서 versioned placement key 누락 삭제 버그 수정. 이전 `latestKey`를 읽어 `shardPlacementKey(bucket, key+"/"+prevVersionID)` 포함 삭제. `applyDeleteObjectVersion`도 동일 수정. stale record 누적으로 인한 BadgerDB 비대화 방지.
- **Dynamic allNodes topology** (`internal/cluster/backend.go`, `internal/raft/raft.go`): `SetShardService` 호출 시 고정되던 allNodes 대신 `liveNodes()` 메서드로 runtime Raft peer 목록 반영. `raft.Node.Peers()` 추가. peer 없는 경우 기존 allNodes fallback 유지.

## [0.0.4.11] - 2026-04-23

### Added

- **per-bucket EC policy Raft 직렬화** (`internal/cluster/`): `CmdSetBucketECPolicy` FSM 명령 추가 (opcode 17). `SetBucketECPolicy`가 Raft를 통해 복제되어 클러스터 전체 노드에 원자적으로 적용. BadgerDB에 `bucketec:<bucket>` 키로 저장; 레코드 없을 때 기본값 true(EC 활성). `PutObject`에서 버킷별 EC 정책 확인 후 ClusterEC 경로 선택.
- **FlatBuffers `SetBucketECPolicyCmd`** (`internal/cluster/clusterpb/`): `cluster.fbs`에 `SetBucketECPolicyCmd` 테이블 추가, 생성된 Go 코드 포함.
- **SetBucketECPolicy FSM/백엔드 테스트** (`internal/cluster/apply_test.go`, `backend_test.go`): `CmdSetBucketECPolicy` 활성화·비활성화·기본값·버킷 없음 케이스 커버 단위 테스트 4개 추가.

### Fixed

- **QUIC half-open 연결 무한 블로킹** (`internal/transport/quic.go`): `evict()` 호출 시 맵에서 제거 후 실제 `conn.CloseWithError()` — 블로킹 중인 `OpenStreamSync` goroutine 즉시 unblock. `Call()`에 stale 연결 감지 시 1회 재다이얼 retry 추가.
- **Raft RPC 타임아웃 누락** (`internal/raft/quic_rpc.go`): `AppendEntries`, `RequestVote`, `TimeoutNow`에 `context.WithTimeout(80ms)` 추가. election timeout(150ms)보다 짧아 spurious election 방지. `InstallSnapshot`은 60s 별도 타임아웃.
- **QUIC 연결 유지 설정** (`internal/transport/quic.go`): `KeepAlivePeriod 3s`, `MaxIdleTimeout 10s` 설정으로 dead 연결 빠른 감지. 리스너·클라이언트 양쪽 동일 값 적용. 연결 종료 시 `handleConnection` defer로 `conns` 맵 자동 정리.
- **E2E 5-node 스테이지드 스타트업** (`tests/e2e/cluster_ec_test.go`): 5개 노드 동시 시작 시 split-vote 루프 발생. 3개 먼저 시작해 리더 확보 후 나머지 2개 추가. CI 안정성 개선.

### Changed

- **QUIC inbound/outbound 연결 분리** (`internal/transport/quic.go`): 수락된 인바운드 연결을 `handleInboundConnection`으로 분리 — 임시(ephemeral) 포트 키로 `conns` 맵에 저장하지 않으므로 `evict()`·`Call()` 경로와 키 충돌 없음. 아웃바운드 연결만 `handleConnection`에서 관리.
- **QUIC 타임아웃 named constants** (`internal/transport/quic.go`): `10 * time.Second`, `3 * time.Second` 리터럴을 `quicMaxIdleTimeout`, `quicKeepAlivePeriod` 상수로 추출.

## [0.0.4.9] - 2026-04-23

### Added

- **S3 ACL Raft 직렬화** (`internal/cluster/`): `CmdSetObjectACL` FSM 명령 추가. `SetObjectACL`이 이제 Raft를 통해 복제되어 클러스터 전체 노드에 원자적으로 적용됨. `ObjectMeta`에 `acl:uint8` 필드 추가 (FlatBuffers 하위 호환, 이전 데이터는 0=private으로 읽힘).
- **Versioned ACL 동기화** (`internal/cluster/apply.go`): Versioning 활성 버킷에서 ACL을 설정하면 legacy key와 versioned key 양쪽 모두 동일 트랜잭션에서 원자적으로 업데이트. 이전에는 legacy key만 업데이트되어 versioned 객체 읽기 시 ACL이 무시되는 버그 수정.
- **BucketVersioning Raft 직렬화** (`internal/cluster/`): `CmdSetBucketVersioning` FSM 명령 추가. `SetBucketVersioning`이 이제 Raft를 통해 복제되어 멀티-노드 클러스터에서 일관성 보장. 이전에는 로컬 BadgerDB에만 기록하여 노드 간 불일치 가능.
- **`LocalBackend.SetObjectACL`** (`internal/storage/local.go`): Solo 모드(단일 노드)용 ACL 직접 저장 구현. `storage.ACLSetter` 인터페이스 구현, HTTP 레이어 ACL E2E 테스트 활성화.

### Fixed

- **`HeadObject`, `ListObjects`, `HeadObjectVersion`의 ACL 필드 누락** (`internal/cluster/backend.go`): `storage.Object` 반환 시 `ACL: m.ACL` 필드가 빠져 있어 클라이언트가 ACL 값을 읽지 못하던 버그 수정.

## [0.0.4.8] - 2026-04-22

### Fixed

- **ShardOwner 필터 재활성화** (`internal/cluster/scrubbable.go`): `NodeID()`가 Raft node 이름(`b.node.ID()`)을 반환하던 버그 수정 — placement 벡터에는 raft 주소(`selfAddr`)가 저장되므로 `OwnedShards` 비교가 항상 빈 결과를 반환하는 no-op 상태였음. `NodeID()`가 `b.selfAddr`를 반환하도록 수정, `scrubber.ShardOwner` 계약 완전 구현.
- **PlacementMonitor `onMissing` txn 외부 실행** (`internal/cluster/shard_placement_monitor.go`): `onMissing` 콜백(QUIC 네트워크 repair 호출)이 BadgerDB `db.View` 트랜잭션 내부에서 실행되어 MVCC 버전을 장시간 핀하던 버그 수정. repair 목록을 `[]pendingRepair`로 수집 후 트랜잭션 종료 이후에 호출.
- **PlacementMonitor `Start` goroutine 누락** (`cmd/grainfs/serve.go`): `placementMonitor.Start(ctx)` 호출에 `go` 접두사 누락으로 scrub-interval > 0 + EC 활성 시 서버가 시작 시 블로킹되던 버그 수정.

### Changed

- **PlacementMonitor serve.go 연결** (`cmd/grainfs/serve.go`): `scrub-interval > 0` + EC 활성 시 `ShardPlacementMonitor`를 생성하고 `SetOnMissing → RepairShardLocal` 콜백을 연결. 로컬 missing shard가 자동 repair 루프에 진입.

## [0.0.4.7] - 2026-04-22

### Added

- **GCM AAD 위치 바인딩** (`internal/encrypt/encrypt.go`): `EncryptWithAAD`/`DecryptWithAAD` 추가. 2-byte 매직 헤더(`0xAE 0xE1`) + AES-256-GCM with AAD 포맷. 파일시스템 쓰기 권한을 가진 공격자가 shard 블롭을 다른 위치로 복사해도 AAD 불일치로 복호화가 실패함 (위치 바인딩).
- **암호화 다운그레이드 감지** (`internal/cluster/shard_service.go`): `ReadLocalShard`/`DecryptPayload`에서 encryptor가 없을 때 매직 헤더 감지 후 명시적 오류 반환. 암호화로 쓴 shard를 암호화 없이 시작한 노드가 garbage로 반환하는 문제 방지.

### Changed

- **Shard 파일 권한 0o644 → 0o600** (`eccodec/shardio.go`, `shard_service.go`, `scrubbable.go`): 암호화 여부와 무관하게 shard 파일은 소유자 전용 권한으로 생성.
- **NBD Docker 테스트 암호화 활성화** (`docker/nbd-test.sh`): `--no-encryption` 제거. 서버가 자동 생성 키로 at-rest 암호화 경로를 사용하므로 NBD 테스트가 실제 암호화 경로를 검증.

## [0.0.4.6] - 2026-04-22

### Fixed

- **NFS/VFS path 충돌 수정** (`internal/cluster/backend.go`): `ListObjects` 결과에서 버전 키 `"{key}/{versionID}"`가 dedup 후 `"{key}"`로 변환될 때 `prefix="key/"` 요청에서 `baseKey="key"`가 반환되어 `isDir("key")=true`로 잘못 인식되는 버그 수정. `!strings.HasPrefix(baseKey, prefix)` 필터 추가로 요청 prefix 밖의 deduped key를 무시하도록 수정.
- `TestNFS_MountAndWriteReadFile`, `TestNFS_MultipleFiles`, `TestNFS_DeleteFile` — t.Skip 제거, 전부 통과 확인.
- `TestCrossProtocolS3PutVFSStat` — 동일 근본 원인으로 t.Skip되어 있었음. 수정 후 통과 확인.

## [0.0.4.5] - 2026-04-22

### Fixed

- **WriteLocalShard 원자적 쓰기** (`internal/cluster/shard_service.go`): `os.WriteFile` 직접 쓰기를 tmp→fsync→rename 패턴으로 교체. 프로세스가 쓰기 도중 크래시해도 shard 파일이 torn state(빈 파일 또는 부분 데이터)로 남지 않고 이전 완전한 버전 또는 새 완전한 버전으로 유지됨. `scrubbable.go:WriteShard`와 동일한 원자적 쓰기 패턴 적용.
- **WriteLocalShard 테스트 추가**: 성공 시 `.tmp` 파일 미잔류 검증 및 읽기 전용 디렉터리 환경에서 실패 시 원본 shard 보존 검증.

## [0.0.4.4] - 2026-04-22

### Added

- **ShardService AES-256-GCM at-rest 암호화** (`internal/cluster/shard_service.go`): `WithEncryptor(enc)` 함수형 옵션 추가. `WriteLocalShard`/`ReadLocalShard`가 encryptor가 설정된 경우 쓰기 전 encrypt, 읽기 후 decrypt 수행. QUIC RPC 경로(`handleWrite`→`WriteLocalShard`, `handleRead`→`ReadLocalShard`)도 동일 경로 통과.
- **Scrubber 암호화 통합** (`internal/cluster/scrubbable.go`): `DistributedBackend.ReadShard`/`WriteShard`에 `EncryptPayload`/`DecryptPayload` 연결. RS 복구 경로도 암호화·복호화를 거치도록 보장.
- **serve.go 암호화 회로 복구** (`cmd/grainfs/serve.go`): `loadOrCreateEncryptionKey` 반환값을 `runCluster`까지 전달하여 `ShardService`에 실제로 연결. `--encryption-key-file` 명시 경로가 존재하지 않으면 자동 생성 대신 오류 반환 (mount 실패 시 키 교체 방지).

### Fixed

- `--encryption-key-file`로 명시적 경로 지정 시 파일이 없으면 새 키를 자동 생성하던 버그 수정. 기존 샤드가 영구적으로 복호화 불가 상태가 되는 데이터 손실 시나리오 제거.
- `DistributedBackend.ReadShard`(scrubber 경로)가 암호화된 shard를 평문으로 RS 재구성하던 문제 수정.

## [0.0.4.3] - 2026-04-22

### Added

- **HealReceipt 자동 발행** (`internal/server/receipt_emitter.go`): `ReceiptTrackingEmitter`가 `scrubber.Emitter`를 래핑해 correlationID 단위 HealEvent를 버퍼링 후 `FinalizeSession` 호출 시 서명된 `HealReceipt`를 `receipt.Store`에 퍼시스트. orphan-sweeper goroutine이 5분 TTL로 미완료 세션을 정리.
- **서명 건강 게이트** (`internal/scrubber/scrubber.go`): `SigningHealthChecker` 인터페이스 도입. KeyStore가 없거나 활성 키가 없으면 해당 사이클의 repair를 전부 건너뜀 — "서명 없는 receipt는 퍼시스트되지 않는다" 감사 불변식 보장. Cycle 중간에 서명 불가로 전환될 경우에도 FinalizeSession 호출 직전 재확인.
- **correlation_id 인덱스** (`internal/receipt/store.go`): `cidx:<correlationID> → receiptID` 보조 인덱스를 주 receipt 키와 동일 트랜잭션에 기록. `GetByCorrelationID`가 단일 BadgerDB View 트랜잭션 내에서 cidx + primary를 함께 읽어 TOCTOU 경쟁 제거.
- **correlation_id API 엔드포인트** (`internal/receipt/api.go`): `GET /api/receipts?correlation_id=X`로 수리 세션 ID 기반 단일 receipt 조회 지원.
- **OTel OTLP HTTP 추적** (`internal/otel/tracer.go`): `--otel-endpoint` / `--otel-sample-rate` 플래그로 OTLP HTTP exporter 초기화. scrub cycle 및 repair 작업에 `scrub.cycle` / `scrub.repair` span 추가.
- **Blame Mode v1 UI** (`internal/server/ui/index.html`): scrub 이벤트 로그에 `Blame` 버튼 — 클릭 시 해당 객체의 HealReceipt를 `/api/receipts?correlation_id=` 로 조회하고 signature · shard 요약 오버레이 표시.

### Fixed

- `GetByCorrelationID` TOCTOU: cidx 조회와 primary 조회를 동일 트랜잭션으로 통합 (adversarial review 지적).
- Mid-cycle 키스토어 교체 시 서명 실패로 receipt가 조용히 유실되던 문제 — FinalizeSession 직전 `SigningHealthy()` 재확인으로 명시적 경고 출력.

## [0.0.4.2] - 2026-04-22

### Fixed

- **EC 경로 node ID 불일치 수정** (`internal/cluster/backend.go`): `putObjectEC`, `getObjectEC`, `ConvertObjectToEC`, `RepairShard`에서 `b.node.ID()` (UUID)를 `b.selfAddr` (raft 주소)로 교체. 기존 코드에서 self-check가 항상 false → 모든 shard가 로컬 저장 없이 피어로 전달되는 심각한 버그 수정.
- **Placement key skew 수정** (`internal/cluster/backend.go`, `scrubbable.go`): `putObjectEC`가 FSM에 placement record를 bare `key`로 저장하던 것을 `shardKey = key + "/" + versionID`로 통일. `GetObject`, `RepairShard`, `OwnedShards`의 lookup도 `shardKey` 기반으로 변경. 버전이 다른 동일 key의 placement record가 서로 덮어쓰이던 문제 해결.

- **RepairShard LookupLatestVersion 실패 시 오류 반환** (`internal/cluster/backend.go`): 기존에 `LookupLatestVersion` 실패 시 조용히 bare key로 진행해 placement를 찾지 못하고 "not EC-managed"를 잘못 반환. 이제 적절한 에러를 반환해 scrubber가 retry 가능.
- **waitForPort e2e 호출 시그니처 수정** (`tests/e2e/`): `waitForPort(port, timeout)` → `waitForPort(t, port, timeout)` (17개 e2e 파일).

### Added

- **EC 버그 수정 단위 테스트** (`internal/cluster/ec_fix_test.go`): `selfAddr` vs `node.ID()` 불일치, 버전별 placement shardKey 저장/조회, 멀티버전 no-collision 등 5개 테스트.

### Changed

- **Phase 18 Stage 0-2 완료 및 CONDITIONAL GO 검증**: `TestECSpike_RawShardP95` 추가 — `--ec=false --no-encryption` 6-node 클러스터에서 raw shard p95 측정. 결과 53.5ms (임계값 500ms의 9.3×), Phase 18 Stage 3 진입 가능 판정
- **ecspike 클러스터 헬퍼 리팩터**: `startEcspikeCluster` → `startEcspikeClusterOpts(t, noEC bool)` 파라미터화, `startEcspikeClusterNoEC` 추가
- **TODOS.md 정리**: Phase 18 Placement map 설계 확정 선결 과제 해소, Stage 3 신규 선결 과제 5건 추가

## [0.0.4.1] - 2026-04-21

### Added

- **`storage.Snapshotable` 인터페이스 구현** on `DistributedBackend`:
  - `ListAllObjects()` — BadgerDB `lat:` 포인터 인덱스로 모든 bucket의 live(non-tombstone) 객체 열거
  - `RestoreObjects()` — 스냅샷 외 객체 메타데이터 하드-삭제 후 스냅샷 객체 Raft propose 재적용; blob 없는 객체는 `StaleBlob`으로 반환
  - `blobExists()` — versioned path / EC shard / legacy unversioned path 순서로 blob 존재 확인; 빈 versionID는 `lat:` 포인터로 자동 resolve
  - `internal/cluster/snapshotable_test.go` 8개 단위 테스트 (커버리지 78~84%)
- **Snapshotable 기반 e2e 테스트 활성화**: `TestAutoSnapshot_*`, `TestPITR_*`, `TestSnapshot_*` 의 `t.Skip` 제거

### Fixed

- **e2e 포트 TOCTOU 경합 제거**: `freePort()`를 `sync/atomic` 카운터 방식으로 교체, listen-then-close 창을 없애 `TestE2E_ClusterEC_FallbackToNx_3Node` / `TestE2E_HealReceiptAPI_3Node` 간헐적 실패 해소

## [0.0.4.0] - 2026-04-21

### Changed

- **단일 storage 경로 통합 — ECBackend 완전 삭제** (`refactor/unify-storage-paths`).
  GrainFS는 이제 어떤 peer 개수에서도 `DistributedBackend` 하나만 사용. `--peers`를
  비워두면 singleton Raft (`127.0.0.1:0`, 커널 할당 포트)로 부팅되어 단일-노드
  사용자 경험은 유지되면서 versioning / scrubber / lifecycle / WAL-PITR 모두
  클러스터 경로에서 동작.
  - `runLocalNode` 삭제, `ecDeleterAdapter` 삭제, `joinClusterLive` 삭제, `SwappableBackend` 사용처 제거
  - `internal/erasure/` 패키지 완전 삭제 (`ECBackend` 1958줄 포함, Reed-Solomon 코덱 테스트 + ACL 테스트)
  - `packblob` / `pull-through` / auto-snapshotter / `DiskCollector`를 `runCluster`에 포팅
  - `cmd/grainfs/serve.go` 약 370줄 감소

### Added

- **Cluster-mode versioning** (`DistributedBackend`):
  - `PutObject`/`CompleteMultipartUpload`가 UUIDv7 `VersionID`를 생성하여 반환
  - FSM 키 스키마 확장: `obj:{bucket}/{key}:{versionID}` 버전별, `lat:{bucket}/{key}` 최신 포인터
  - `GetObjectVersion` / `HeadObjectVersion` / `DeleteObjectVersion` / `ListObjectVersions`
  - `DeleteObject`는 tombstone(delete marker)을 새 버전으로 생성. Hard-delete는
    `DeleteObjectVersion`. `HeadObjectVersion`은 delete marker에 대해
    `storage.ErrMethodNotAllowed` 반환 → HTTP 405 + `x-amz-delete-marker: true`
  - `SetBucketVersioning` / `GetBucketVersioning` (`bucketver:{bucket}` 로컬 저장;
    Raft 직렬화는 follow-up)
  - `DeleteObjectReturningMarker`로 delete marker의 version ID를 S3 응답 헤더로 노출
- **`scrubber.Scrubbable` 인터페이스 구현** on `DistributedBackend`:
  `ScanObjects`/`ObjectExists`/`ShardPaths`/`ReadShard`/`WriteShard` + `OwnedShards` /
  `RaftNodeID` / `RepairShardLocal` (optional `ShardOwner`/`ShardRepairer`)
- **Cluster-mode scrubber**: `runCluster`에서 EC 활성 시 scrubber 자동 시작,
  peer-sourced repair via `RepairShard`
- **Cluster-mode lifecycle**: `LifecycleManager`가 Raft leadership을 polling하여
  leader 전용으로 `lifecycle.Worker` 실행 (double-delete 방지). `FSMDB()` accessor로
  DistributedBackend의 BadgerDB를 lifecycle store와 공유
- **Cluster-mode WAL-PITR**: WAL 엔트리 스키마 v2 (`VersionID` 포함, v1 backward
  compatible), `OpDeleteVersion` 추가, `wal.Backend`가 versioning 메서드 pass-through
- **`internal/storage/eccodec`**: 공용 shard I/O + CRC32 footer 원시
- **`internal/cluster/NewSingletonBackendForTest`**: 다른 패키지 테스트에서 DistributedBackend 부팅 도우미
- **UUIDv7 VersionID**: `oklog/ulid` 의존성 제거, `google/uuid` NewV7()만 사용

### Fixed

- **Phase 18 identity mismatch**: `SetShardService`가 self raft 주소를 캐시하고
  fan-out 시 `peer == b.selfAddr`로 비교 (기존에는 `peer == b.node.ID()`로 UUID와
  비교해 항상 일치하지 않던 버그 → singleton에서 자신에게 RPC 시도하여 타임아웃)
- `IterObjectMetas` / `IterShardPlacements`의 `obj:{bucket}/{key}:{vid}` 파싱 (단일 slash 분리가 버전 suffix로 깨지던 문제)
- `getObjectEC`/`RepairShard`/`ShardPlacementMonitor`가 versioned 경로 사용 (기존에 unversioned 경로로 읽어 발생하던 ENOENT)

### Removed

- `internal/erasure/` 패키지 전체 (ECBackend, 기존 Reed-Solomon codec, FlatBuffers 스키마)
- `runLocalNode` 함수와 관련 어댑터 (`ecDeleterAdapter`, `joinClusterLive`)
- 런타임 local→cluster 전환 경로 (이제 모든 경로가 cluster이므로 의미 없음)
- 사용되지 않는 `VFSInstance` 스텁 (circular dep 우려는 preemptive였음; 실제 cycle 없음)

### Known issues (deferred)

- `TestNFS_MountAndWriteReadFile` / `TestNFS_MultipleFiles` e2e 실패: versioned
  `objectPathV` (`data/bucket/key/.v/vid`)가 unversioned `objectPath`
  (`data/bucket/key` as a file)와 path 충돌. NFS 볼륨 경로에서 트리거됨 —
  key/version path 스킴 재설계 필요 (follow-up PR)
- `TestSnapshot_List` / `TestSnapshot_NotFound`: `DistributedBackend`가 아직
  `storage.Snapshotable` 미구현 (RestoreObjects가 Raft propose 필요). WAL 기록은
  정상 동작
- Cluster-mode at-rest encryption: 기존 ECBackend의 AES-GCM 경로가 사라졌고
  `ShardService.WriteLocalShard`는 raw bytes. `SetBucketVersioning`의 Raft
  직렬화, S3 ACL의 `SetObjectACL` Raft propose 함께 follow-up
- Phase-18 `ShardOwner` filtering은 scrubber 인터페이스에 남아있지만 비활성 — 위의
  identity mismatch 수정으로 이제 on-the-fly 활성화 가능 (별도 slice)

## [0.0.3.1] - 2026-04-21

### Changed

- **"solo" 용어 코드베이스에서 완전 제거** — 함수/변수/타입/테스트 이름, 주석, 로그 메시지, UI 문구, 문서에서 모두 삭제. GrainFS는 이제 오직 **cluster mode** 하나로 존재. `--peers=""`는 "no-peers 모드"(로컬 저장소 경로)로 동작 — 아키텍처상 내부 분기는 유지되지만 사용자 모델은 "cluster 하나"로 통일.
  - `runSoloWithNFS` → `runLocalNode`
  - `setupSoloReceipt` → `setupLocalReceipt`
  - `MigrateSoloToCluster` → `MigrateLegacyMetaToCluster`
  - `soloNodeID`, `soloManagedMode`, `soloLogGCInterval` → `local*`
  - Test `TestCluster_SoloRaft_*` → `TestCluster_NoPeers_*`
  - Test `TestSplitBrain_SoloIsZero` → `TestSplitBrain_NoPeersIsZero`
  - Raft 내부 `newSoloLeader` → `newSingletonLeader`, `TestProposeWait_Solo` → `TestProposeWait_Singleton`, `TestQuorumMinMatchIndex_Solo` → `TestQuorumMinMatchIndex_Singleton`
  - UI: "No cluster configured (solo mode)" → "No cluster configured (no-peers mode)"
  - 로그 mode label `"solo"/"solo-ec"/"cluster"` → 제거 (component = "server" 만 노출)
  - Docs (README/ROADMAP/RUNBOOK): "Solo → Cluster", "Solo 모드" → "단일-노드 → Cluster", "단일-노드 모드"
  - `Reed-Solomon`은 알고리즘명으로 보존 (모드 이름이 아님)

### Deferred

- 아키텍처 통합 (ECBackend + scrubber + lifecycle + WAL-PITR을 DistributedBackend로 흡수) — 별도 PR. 이번 PR은 용어/이름 통일에 집중, 기능 변경 없음. `runLocalNode` 내부 구현은 유지됨.

## [0.0.3.0] - 2026-04-21

### Added

- **NFS / NFSv4 / NBD servers available in cluster mode** (`cmd/grainfs/node_services.go`) — formerly solo-only. Cluster deployments now get volume-serving protocols alongside the S3 HTTP API. Same flags (`--nfs-port`, `--nfs4-port`, `--nbd-port`), same default volume layout, same localhost-only NFSv4 binding for AUTH_SYS security.

### Changed

- **Shared `startNodeServices` helper extracted** (`cmd/grainfs/node_services.go`, `cmd/grainfs/serve.go`) — NFS/NFSv4/NBD wiring lives in one place, called by both solo (`runSoloWithNFS`) and cluster (`runCluster`) paths. Solo path reduced by 46 lines.

### Deferred

- Solo mode 완전 삭제는 A.2 PR로 이연 — scrubber와 lifecycle worker가 `*erasure.ECBackend` 타입에 하드커플링되어 있어서 cluster의 DistributedBackend로 단순 포팅 불가. 먼저 ECBackend를 cluster storage layer로 통합해야 solo dispatch를 제거할 수 있음. TODOS.md에 단계별 계획 기록됨.

## [0.0.2.0] - 2026-04-21

### Added

- **Phase 18 Cluster EC end-to-end** (`internal/cluster/ec.go`, `internal/cluster/backend.go`, `cmd/grainfs/serve.go`) — Cluster mode now splits every object into k+m Reed-Solomon shards placed across distinct nodes. `PutObject` fans out shards with write-all semantics and commits `CmdPutShardPlacement` through Raft before meta; `GetObject` looks up placement and reconstructs from any k shards. Opt-in via `--cluster-ec` (default true). Auto-falls back to N× replication when cluster size < k+m — small deployments keep working unchanged.
- **ShardPlacementMonitor** (`internal/cluster/shard_placement_monitor.go`) — 각 노드가 자신의 배치된 shard를 주기적으로 스캔하여 누락 감지. Replaces dead `ReplicationMonitor` (0 production callers). Hook (`SetOnMissing`) for Slice 5 repair integration.
- **Background N×→EC re-placement** (`internal/cluster/reshard_manager.go`) — 기존 N× 객체를 EC로 변환하는 leader-only background task. `ConvertObjectToEC` primitive uses ETag check before commit to tolerate concurrent PUT. Start/Stop/Stats for observability.
- **RepairShard primitive** (`internal/cluster/backend.go`) — 누락 shard를 k-of-(k+m) 나머지에서 재구성하여 원 위치에 복원. Building block for auto-heal (full wiring deferred).
- **ConvertObjectToEC** (`internal/cluster/backend.go`) — 기존 N×-replicated object를 EC로 마이그레이션하는 primitive. ETag mismatch 감지 시 안전하게 abort + rollback.

### Changed

- **Placement algorithm deterministic** (`internal/cluster/ec.go`) — `(FNV32(key) + shardIdx) mod N` placement. When N == k+m, 한 key의 모든 shard가 N개 별개 노드에 배치. Spike가 검증한 동일 공식.
- **ShardService.WriteLocalShard / ReadLocalShard / DeleteLocalShards** (`internal/cluster/shard_service.go`) — self-placement 시 QUIC loopback 생략하는 로컬 IO 경로 추가. Peer로는 기존 WriteShard/ReadShard 그대로 사용.
- **DistributedBackend.SetShardService가 allNodes 정렬** — cluster 전체 deterministic placement 위해 정렬된 노드 리스트 사용.
- **DeleteObject가 EC shards cascade** — legacy N× 전체 객체 파일 + local EC shards + peer shard dirs 모두 삭제.
- **RecoveryManager → ShardPlacementMonitor 연결** (`internal/cluster/recovery.go`) — 복구 시 placement scan 실행.

### Removed

- **ReplicationMonitor dead code** (`internal/cluster/replication.go`, `replication_test.go`) — 0 production callers, 설계 문서에서 이미 rename 명시. ShardPlacementMonitor가 FSM-backed 대체로 Phase 18 Slice 4에서 도입.

### Tests

- **12+ unit tests (EC helpers, placement isolation, IterShardPlacements, IterObjectMetas, monitor scan, reshard manager)** — TDD 기반 Slice 2~5 구현.
- **E2E TestE2E_ClusterEC_PutGet_5Node** (`tests/e2e/cluster_ec_test.go`) — 5-node cluster, 3+2 EC, varied-size 객체 round-trip + node kill 후 read-k 재구성 검증.
- **E2E TestE2E_ClusterEC_FallbackToNx_3Node** — 3-node 클러스터가 k+m=5 미달 시 N× replication 자동 fallback 확인.

### Deferred

- Solo mode 삭제는 별도 PR로 분리 (runSoloWithNFS가 NFS/NBD/scrubber/snapshot/WAL/vfs/volume/lifecycle/packblob/pullthrough feature wiring을 단독 보유 — 단순 삭제 불가, consolidation 필요). `TODOS.md`에 상세 포팅 계획 기록.

## [0.0.1.0] - 2026-04-21

### Changed
- **Cluster storage 모델 정직화** (`ROADMAP.md`, `README.md`) — Phase 4 "EC + Fan-out ✅" 를 solo-only EC / cluster N× replication 으로 분리. `--ec*` 플래그는 solo 모드 전용임을 명시. `ReplicationMonitor`는 dead code (production caller 0), `migration_executor` 는 shardIdx 0..N-1 를 가정하지만 PutObject 가 shardIdx=0 에만 전체 객체를 쓰므로 balancer-triggered migration 은 로그 에러만 뱉고 실패한다 (FSM atomic cancel 로 데이터 안전). 이 모든 사실을 코드 주석에 inline 으로 기록.
- **Phase 번호 재정렬** (`TODOS.md`) — Phase 18(Performance) → 19, Phase 19(Protocol Extensions) → 20. 새 Phase 18: Cluster EC 를 최상위 storage durability 작업으로 지정.

### Added
- **Phase 18 Cluster EC Slice 1: ShardPlacement FSM metadata** (`internal/cluster/shard_placement.go`, `internal/cluster/clusterpb/`) — Raft FSM 에 object 별 EC shard 배치 metadata CRUD + lookup 레이어 추가. PutObject/GetObject 통합은 Slice 2 로 이연. `CmdPutShardPlacement` / `CmdDeleteShardPlacement` 커맨드, `FSM.LookupShardPlacement(bucket, key)` read API, `applyDeleteObject` cascade GC, BadgerDB key prefix `placement:<bucket>/<key>`. k+m 은 NodeIDs 슬라이스 길이로 동적 결정 (하드코딩 없음). FSM v1→v2 breaking migration 불필요 — BadgerDB backend 특성상 unknown 커맨드는 warn+skip 으로 forward/backward compat 자동.
- **48h de-risk spike: ecspike package** (`internal/cluster/ecspike/`) — Phase 18 commitment 전 4+2 Reed-Solomon cluster 기술 리스크 검증. `klauspost/reedsolomon` 직접 import (throwaway 불변 보존, `internal/erasure` 무손상), FNV32 기반 결정적 placement, S3 API per-node shard storage. 6-node loopback multi-process 클러스터에서 10×16MB PUT → node-0 kill → 10 GET SHA256 완전 일치 검증. LOC 219 (< 500 budget), correctness PASS. p95 904ms (nested solo-EC + S3 API overhead 포함 upper bound) 는 Phase 18 raw shard 경로에서 개선 예정 — 정확성 리스크 해소로 CONDITIONAL GO 판정.

### Tests
- **TDD coverage — Slice 1** (`internal/cluster/shard_placement_test.go`) — 11 테스트 케이스: encode/decode round-trip (4+2, 6+3, 1+1, unicode, empty nodes), Apply + Lookup + Delete + Overwrite, Snapshot/Restore 보존, DeleteObject cascade, BadgerDB key prefix isolation, 다중 객체 격리.
- **E2E spike test** (`tests/e2e/cluster_ecspike_test.go`) — `TestECSpike_KillOneNodeStillReadable` 6-node multi-process bootstrap (기존 `exec.Command + Process.Kill()` 패턴 재사용), 10×16MB correctness verification + 100×16MB p95 measurement.

### Documentation
- **Office-hours 설계 + eng review 산출물** (`~/.gstack/projects/gritive-grains/whitekid-master-design-20260421-024627.md`) — 3-stage 플랜(Stage 0 실험 → Stage 1 문서 → Stage 2 48h spike), 5개 Stage 3 critical gap 이연, outside voice cross-model tension 3건 해결, go/no-go appendix.

## [0.0.0.22] - 2026-04-21

### Added
- **Phase 16 Week 5 Slice 2 — Heal Receipt API + cluster fan-out** — `/api/receipts/:id` 단건 조회와 `/api/receipts?from=&to=&limit=` 범위 조회를 노출. 해결 순서는 (1) 로컬 store 히트, (2) gossip이 학습한 routing cache 경유 단일-피어 쿼리, (3) 3초 타임아웃의 broadcast fan-out. 타임아웃은 `503 X-Heal-Timeout` 으로 구분해 SRE가 "존재하지 않음(404)" 과 "클러스터 도달 실패(503)"를 구별 가능. `Store.List(from, to, limit)`는 `ts:<unix_nano>:<id>` 세컨더리 인덱스 역스캔으로 O(결과 크기) 보장.
- **Rolling-window gossip** (`internal/cluster/receipt_gossip.go`) — 노드별 최근 N개 receipt ID 를 `StreamReceipt` 로 주기 브로드캐스트. 수신측 `GossipReceiver.SetReceiptCache`로 `RoutingCache`에 투영, import cycle 방지용 `ReceiptRoutingCache` 인터페이스 경유. 네임스페이스 분리된 `StreamReceipt=0x04` (one-way gossip) / `StreamReceiptQuery=0x05` (RPC) stream types 신설.
- **ReceiptBroadcaster fan-out** (`internal/cluster/receipt_broadcast.go`) — routing cache miss 시 모든 peer 에 first-success-wins 병렬 쿼리, 남은 peer 는 context cancel 로 조기 해제. 5개 Prometheus counter(`grainfs_receipt_broadcast_{total,hit,miss,timeout,partial_success}_total`) 로 SLO 가시성 — partial-success 는 "적어도 한 peer 가 answer 했지만 전체 응답 전"의 degraded-cluster 신호.
- **serve.go 통합 wiring** (`cmd/grainfs/receipt_wiring.go`) — Solo 모드는 `receipt.Store` + 로컬 전용 API, cluster 모드는 full stack (Store + KeyStore + RoutingCache + Broadcaster + GossipSender + `StreamReceiptQuery` handler 등록 + gossip receiver 연결). PSK 는 기본 `--cluster-key` 재사용, `--heal-receipt-psk` 로 override. CLI: `--heal-receipt-enabled`, `--heal-receipt-retention` (기본 720h), `--heal-receipt-gossip-interval` (기본 5s), `--heal-receipt-window` (기본 50).
- **Multi-node E2E** (`tests/e2e/heal_receipt_api_test.go`) — 3-node 클러스터로 4개 해결 경로 검증: (a) local hit, (b) gossip routing-cache 경유 peer 쿼리, (c) rolling window 밖 id 의 broadcast fallback, (d) 전역 미존재 404. Slice 3의 scrubber wiring 전이므로 BadgerDB 에 사전 시드.

### Fixed
- **cluster mode 부팅 — default bucket 재시도** (`cmd/grainfs/serve.go`) — `backend.CreateBucket("default")` 가 단발 호출이라 peer 가 quorum 전에 시작하면 "not the leader" 로 프로세스가 즉시 종료. 100ms→2s exponential backoff + 30s 데드라인으로 재시도, `ErrBucketAlreadyExists`는 성공 처리, ctx 취소는 즉시 중단. 실제 부팅은 Raft leader 선출 직후 한번에 성공하므로 시간 패널티 없음.
- **receipt API nil verifier panic** (`internal/server/receipt_api.go`) — `--access-key`/`--secret-key` 미설정 시 `s.verifier == nil` 인데 `registerReceiptAPI` 가 `s.authMiddleware()` 를 무조건 attach 해 요청마다 nil-pointer NPE 발생(server-level 미들웨어는 nil 가드로 이미 skip). 이제 조건부 체인으로 전역 auth 패턴과 동작 일치 — "`--access-key` 없음 = 전면 auth 없음".
- **cluster mode S3 auth propagation** (`cmd/grainfs/serve.go`) — `runCluster` 가 `authOpts` 를 받지 않아 `--access-key`/`--secret-key` 가 cluster 모드에서 조용히 무시되던 기존 갭. 이제 `runServe` 에서 `authOpts` 를 전달, `runCluster` 가 `srvOpts` 에 append. HealReceipt API 를 포함한 모든 cluster-mode endpoint 가 플래그대로 auth 를 강제.
- **fresh cluster bootstrap spurious auto-migration** (`cmd/grainfs/serve.go`) — `runCluster` 가 `os.MkdirAll(metaDir)` 를 migration 체크 전에 호출해 빈 dataDir 에서도 solo→cluster migration branch 에 진입, migration 은 이미 열린 meta BadgerDB 를 다시 열려다 dir lock 충돌로 종료시키던 문제. 이제 migration 체크를 `MkdirAll` 및 `badger.Open` 위로 이동, `os.ReadDir` 결과로 빈 meta 디렉토리를 감지해 무관한 진입을 차단. Slice 3+ 에서 새 3-node 클러스터 부팅이 단발 시도로 성공.

### Pre-landing review fixes
- **gossipReceiver always-on when heal-receipt enabled** (`cmd/grainfs/serve.go`) — `gossipReceiver` 가 `--balancer-enabled=true` 일 때만 생성되어, balancer 를 끄고 heal-receipt 만 쓰는 배포에서 `StreamReceipt`(rolling-window gossip) 메시지를 소비할 주체가 없었음. `tr.Receive()` 를 드레인하지 않으니 `RoutingCache` 는 영구 empty, `/api/receipts/:id` 는 항상 3초 broadcast 경로로 떨어짐. 이제 balancer 가 꺼져 있어도 heal-receipt 가 켜지면 독립 `GossipReceiver` 를 부팅해 `tr.Receive()` 를 한 consumer 가 드레인하도록 강제. Receive()는 단일 채널이라 여러 consumer 가 경쟁하면 메시지가 한쪽에만 전달되므로 반드시 하나만 동작.
- **nodeIDMatchesFrom spoofing gap** (`internal/cluster/gossip.go`) — `from=IP`, `nodeID=hostname` 조합에서 무조건 `true` 를 반환하던 허용적 fallback 을 `net.LookupHost` 기반 strict verification 으로 교체. 인증된 cluster-key 보유자가 hostname 을 위장해 `RoutingCache`/`NodeStatsStore` 를 오염시키는 경로를 닫음. DNS 실패 시 reject. `TestNodeIDMatchesFrom` 에 `localhost ↔ 127.0.0.1` positive case 와 `node-a` 가상 hostname negative case 추가.
- **ListReceipts 시간 범위 상한** (`internal/receipt/api.go`) — `NewAPI` 에 `maxRange time.Duration` 파라미터 추가, `(to - from) > maxRange` 면 `400`. wiring 에서 `retention` 을 그대로 전달해 인증된 사용자가 거대 ts:* 스캔으로 BadgerDB 를 DoS 하지 못하도록. retention 바깥은 어차피 TTL expired 라 실 결과 가림 없음. `TestAPI_ListReceipts_RejectsOversizedRange` 회귀 테스트.
- **Store.Put Timestamp 검증** (`internal/receipt/store.go`) — `Timestamp.IsZero()` 또는 `UnixNano() < 0` 이면 `ErrInvalidTimestamp`. `tsIndexKey` 의 `%019d` padding 은 non-negative unix-nano 를 전제로 하며, 음수는 `"-…"` 접두사를 만들어 digit sort 보다 앞서 정렬돼 `List` 시간순 및 `RecentReceiptIDs` reverse scan 을 오염. `TestStore_Put_RejectsZeroTimestamp` 추가.
- **빈 peer 필터링** (`cmd/grainfs/serve.go`) — `strings.Split("", ",")` → `[""]`, `"a,,b"` → `["a","","b"]` 가 gossip sender 의 매 tick 경고 로그와 broadcaster 의 무의미한 `""` fan-out 시도를 유발. `filterEmpty` 헬퍼로 `runCluster` 와 `joinClusterLive` 의 split 결과를 정제.

### Tests
- `TestRoutingCache_*` — Update/Lookup/Evict 의미, concurrent reader/writer race detector 통과.
- `TestReceiptGossip*` / `TestReceiptBroadcast*` — FlatBuffers 페이로드 round-trip, NodeId mismatch drop, fan-out 타임아웃, first-success 조기 취소.
- `TestAPI_*` — 3-tier resolution, 503 vs 404 분기, RFC 3339 parsing, limit 기본값/캡.
- `TestBroadcastMetrics_*` — 5 카운터 Inc 검증 + partial_success 가 responded/total 에 무관.
- `TestIntegration_*` — in-process stitchedCluster 로 local/cache/broadcast/notfound/ordering-conflict 커버.
- `TestE2E_HealReceiptAPI_3Node` — 3-노드 실클러스터 부팅 + 4 해결 경로.

## [0.0.0.21] - 2026-04-20

### Added
- **`internal/receipt` 패키지 (Phase 16 Week 5 Slice 1)** — Heal Receipt 핵심 레이어. 한 repair 세션을 audit-ready 아티팩트로 요약: `HealReceipt` struct, HMAC-SHA256 서명, JCS 스타일 canonicalization(RFC 8785 부분 준수 — 키 알파벳 정렬 + 공백 없음), `KeyStore`로 `key_id` rotation 지원(기본 previous 3개 유지), BadgerDB 로컬 저장(Raft FSM 복제 없음) + 기본 30일 TTL, batch write(기본 100 buffer 또는 50ms flush 중 먼저 도달). 서명 실패 시 `ErrNoActiveKey`, 미서명 receipt 저장 거부(`ErrUnsigned`) — Phase 16 audit 무결성 원칙(무서명 receipt 절대 생성 금지) 강제.
- TODOS.md Phase 16 섹션 재편 — Week 5(Slice 1–4), Week 6(Grafana + demo) 설계 완료/미진행 항목 명시.

### Notes
- 이번 슬라이스는 패키지 단독. API 엔드포인트(`/api/receipts/*`), gossip rolling window, scrubber repair 경로와의 wiring, Blame Mode UI, OTel spans는 Slice 2–4로 분리.

### Tests
- `TestCanonicalize_*` — 호출 간 byte-identical, CanonicalPayload/Signature 필드 제외, 알파벳 정렬, whitespace 금지.
- `TestSign_*` / `TestVerify_*` — 서명 라운드트립, 필드/서명 위조 감지, 알 수 없는 key_id, empty keystore 감지.
- `TestKeyStore_Rotate_*` — 이전 키로 검증 성공, retention 초과 시 eviction, 중복 ID rotation 거부.
- `TestStore_*` — put/get 라운드트립, threshold/interval flush, Close 시 pending drain, 1000건 burst 전건 영속.

## [0.0.0.20] - 2026-04-20

### Fixed
- **alerts.Dispatcher dedup race** (`internal/alerts/webhook.go`) — 이전 구조에서 `shouldSuppress` 체크 후 HTTP retry loop 동안 lock이 풀려 있어, 동일 `(Type, Resource)` 키로 거의 동시에 호출된 두 `Send` 가 모두 suppress를 통과해 웹훅이 중복 발송될 수 있었다. `inFlight` 집합을 추가하고 `claimSend`가 `lastSent` 확인과 함께 키를 선점, `defer`로 전송 수명 내 panic까지 포함해 release를 보장. 동시 Send 중 하나만 HTTP에 도달 + panic-safe 계약 regression 테스트 포함.
- **alerts.Dispatcher failure-path dedup** (`internal/alerts/webhook.go`) — 실패 시 `lastSent`를 기록하지 않아 5xx 반복 발생 시 재시도 사이클마다 웹훅 스팸이 가능했다. 이제 성공/실패 무관 delivery 완료 시 `lastSent` 기록 → outage storm 동안도 dedup window가 페이징을 억제. 수동 재시도는 `AlertsState` Force Resend 경로 유지.
- **grainfs_degraded gauge race** (`internal/server/alerts_api.go`) — `gaugeTracker` wrapper가 `inner.Report()` → `inner.Degraded()`를 분리해 수행하던 탓에, 두 스텝 사이 다른 goroutine의 `Report`가 상태를 뒤집으면 Prometheus 게이지가 실제 상태와 불일치했다. `DegradedConfig.OnStateChange` 콜백을 추가해 tracker lock 보유 중에 게이지를 업데이트, wrapper 자체를 제거. `AlertsState.Tracker()` 반환 타입은 `*alerts.DegradedTracker`로 변경(unexported 타입이었으므로 외부 API 영향 없음).
- **OnHold synchronous blocking** (`internal/server/alerts_api.go`) — flap threshold 트립 시 `OnHold` 콜백이 synchronous `dispatcher.Send`를 호출해, webhook HTTP retry 수십 초 동안 scrubber/raft/disk collector의 critical-path `Report()`가 block 가능했다. 이제 `go s.dispatcher.Send(...)`로 fire-and-forget — `onFailure` 콜백이 이미 실패를 dashboard banner + Force Resend 경로에 기록.
- **startup recovery WalkDir 에러 신호 보존** (`internal/server/startup_cleanup.go`) — 기존 `walkFn`은 모든 per-entry err를 res.Errors에 기록하므로 현 구조상 `WalkDir`의 top-level return err는 `context.Canceled` 외에 발생하지 않지만, 향후 walkFn 변경 시 non-context err가 silently drop될 위험을 명시적으로 차단. `res.Errors`에 `walkdir root=...` prefix로 기록 + `slog.Warn`으로 operator 가시성 확보.

### Added
- `DegradedConfig.OnStateChange func(degraded bool)` 옵션 — tracker lock 내에서 degraded↔healthy 전이 직후 호출되어 downstream mirror(Prometheus gauge 등)가 tracker 상태와 bit-exact-consistent하게 유지.
- webhook.go `lastSent` 필드 godoc — low-cardinality Resource invariant 명시 (자동 sweep 없음, 현재 production 호출자 `degraded_hold` 단일 경로).

### Tests
- `TestDegradedTracker_OnStateChangeFiresOnTransition` — enter/exit 시 콜백 호출, 동일 상태 반복 시 콜백 미호출.
- `TestDegradedTracker_OnStateChangeHeldUnderLock` — 콜백 실행 중 peer goroutine의 `Status()` 호출이 블록됨으로 lock 보유를 증명.
- `TestAlertsState_ConcurrentReportsHaveNoRace` — 4 goroutine × 50회 Report + `-race -count=3` 통과.
- `TestDispatcher_ConcurrentSameKeyOnlyOneDelivered` — barrier-channel mock 수신자로 동시 Send 중 1개만 HTTP 도달, release 후 재시도 허용 확인.
- `TestDispatcher_RecordSentOnFailureDedupsOutageStorm` — 5xx 반복 시 첫 delivery는 재시도 exhaustion, 두 번째 Send는 dedup window 내 suppress + 윈도우 이후 재개.

## [0.0.0.19] - 2026-04-20

### Added
- **Phase 16 Week 4 — Webhook Alert Framework + Degraded Mode (foundation)** — Slack-compatible 인커밍 webhook을 단일 URL로 보내는 alerts 디스패처 + degraded 상태 트래커.
  - **`internal/alerts/webhook.go` — Dispatcher**: severity {critical, warning}, dedup key=(type+resource) 10분 억제, 5회 지수 backoff 재시도, 옵션 시 `X-GrainFS-Signature` HMAC-SHA256 헤더, 빈 URL이면 no-op (operator opt-in).
  - **`internal/alerts/degraded.go` — DegradedTracker**: entry-immediate 히스테리시스, 30s exit-stable 윈도우, 5분 5-flap 카운터 (3회 초과 시 hold + critical webhook 자동 발사).
  - **`internal/server/alerts_api.go`**: `Server.Alerts()` accessor, `WithAlerts(...)` 옵션, gauge-mirroring `Tracker()` 래퍼로 `grainfs_degraded` 자동 갱신.
- **Admin API**: `GET /api/admin/alerts/status` (banner snapshot — degraded/held/flap count/last failure), `POST /api/admin/alerts/resend` (Force Resend 버튼). `localhostOnly()` 가드.
- **Dashboard 배너**:
  - "Cluster degraded" 빨간 배너 — `role="alert"` `aria-live="assertive"` (스크린리더 즉시 공지). degraded 시에만 표시, last_reason/last_resource/held/flap_count 노출.
  - "Alert delivery failed" 노란 배너 + Force Resend 버튼 — webhook 재시도 소진 시에만 표시. resend 성공하면 자동 사라짐.
  - 5초 폴링 (간단한 contract — SSE 불필요).
- **CLI flags**: `--alert-webhook URL`, `--alert-webhook-secret SECRET` (solo + cluster 양쪽).
- **Prometheus 메트릭**: `grainfs_degraded`(게이지), `grainfs_alert_delivery_attempts_total{outcome}`, `grainfs_alert_delivery_failed_total`.
- **Phase 16 Week 3 — Self-healing Storage at Startup** — 부팅 시 충돌 잔여물 자동 청소.
  - `*.tmp` 파일 (atomic write 중 죽은 흔적) — 5분 in-flight guard 통과한 것만 삭제 (live writer 보호).
  - `parts/<uploadID>/` 디렉토리 (포기된 multipart upload) — 24시간 미사용 후 삭제.
  - 청소 액션마다 `HealEvent{Phase: startup, ErrCode: orphan_tmp|orphan_multipart}` 발행. eventstore에 영속 → 재시작 후 dashboard 새로고침해도 "Restart Recovery" 라인에 표시.
  - 깨끗한 부팅 시 per-action 이벤트 0건 (대시보드 노이즈 방지).
  - context 취소 지원 — 거대 데이터 디렉토리에서도 다음 부팅을 막지 않음.
  - 의도적 비대상: flock 기반 lock 파일 (커널이 프로세스 죽음에 자동 해제), in-memory 캐시 (디스크 저장 없음), BadgerDB 내부 (Phase 17 atomic recovery).
- **BadgerDB preflight 무결성 체크** — `badger.Open` 직후 sentinel write/read/delete 사이클로 DB가 실제로 운영 가능한지 확인. 실패 시 fail-fast + 운영자 친화적 복구 가이드 (디스크 공간/권한/snapshot 복원 안내). solo·cluster·migrate 3개 경로 모두 적용.

### Tests
- Week 3 단위 8개: `TestStartupRecovery_DeletesOldTmpFiles`, `TestStartupRecovery_DeletesOldMultipartParts`, `TestStartupRecovery_NothingToCleanEmitsNoEvents`, `TestStartupRecovery_MissingDataRoot`, `TestStartupRecovery_NilEmitterIsSafe`, `TestStartupRecovery_ContextCancelStops`, `TestPreflightBadger_HealthyDB`, `TestPreflightBadger_NilDB`, `TestPreflightBadger_RecoveryGuideOnFailure`.
- Week 3 E2E `TestRestartRecovery_SweepsOrphanArtifacts` — orphan .tmp + multipart 디렉토리 심어 두고 부팅 → 두 아티팩트 삭제 확인 + eventstore에서 startup HealEvent 두 종류 모두 확인.
- Week 4 단위 13개: dispatcher 8 (Slack JSON, HMAC sign/no-sign, dedup window suppress/release, 다른 resource 동일 type 통과, 5xx 재시도→실패 callback, 2xx 즉시 성공, no-URL no-op), degraded tracker 5 (entry-immediate, exit-stable 30s, 자가 fault 시 stability 리셋, flap counter hold + window cool-off, status snapshot 불변), alerts API 5 (status healthy/failed, resend 성공 시 banner clear, no-failed → reason 텍스트, gaugeTracker mirror).
- mock webhook 수신 서버로 dispatch + dedup + retry + HMAC 통합 검증.
- `-race -count=1` alerts / server / scrubber / eventstore 패키지 통과.

### Out of scope (follow-up)
- EC backend per-object 503 + `X-GrainFS-Degraded: isolated` 헤더 통합 — erasure 패키지 surface 변경이 커서 별도 PR로 분리.
- 실제 fault injection E2E (degraded mode + alert path 전체) — EC 통합 후 같이.
- `docs/alerts.md` PagerDuty 매핑 표 — 이번 PR은 Slack JSON 한정.
- BadgerDB atomic auto-recovery — Phase 17.

## [0.0.0.18] - 2026-04-20

### Added
- **Phase 16 Week 2 — Self-Healing Dashboard Card** — 대시보드에 "납득 가능한 자가 치유" 패널 추가. 대시보드를 열면 Last Heal(가장 최근 복구 + 경과 시간), Heal Rate(1시간 단위 sparkline + 분당 버킷팅), Restart Recovery 카운트, Live Heal Events(최근 5건, role="log"/aria-live="polite") 4개 카드가 보인다. 비어 있을 때 "No recent heal events. Your storage is healthy." 친절 문구 (`internal/server/ui/index.html`).
- **HealEvent 데이터 흐름 완성 (Week 1 잔여 작업)** — 스캐폴딩만 있던 `HealEvent`를 실제 운영 경로에 연결.
  - `BackgroundScrubber.runOnce`가 missing/corrupt shard마다 detect 이벤트 emit, 동일 객체의 모든 이벤트는 UUIDv7 correlation_id로 묶임 (Phase 16 Week 5 Heal Receipt 사전 작업).
  - `RepairEngine`이 reconstruct/write/verify phase별 이벤트 emit (성공/실패/duration/bytes_repaired 포함).
  - 사이클 캡 도달 시 skipped + err_code="cycle_cap" 기록.
  - `scrubber.WithEmitter` / `RepairEngine.WithRepairEmitter` / `BackgroundScrubber.SetEmitter` 옵션으로 주입. 기본값은 `NoopEmitter`라 기존 호출자 회귀 없음 (`internal/scrubber/scrubber.go`, `internal/scrubber/repair.go`).
- **`/api/events/heal/stream` SSE 엔드포인트** — `Event.Type == "heal"` 만 필터링해서 스트리밍. EventSource 클라이언트는 verbose log/metric 스트림과 무관하게 heal 카드만 구독. SSE keep-alive comment(15s)로 idle 연결 유지 + 헤더 즉시 flush (`internal/server/server.go`, `internal/server/sse_hub.go`).
- **SSE Hub 카테고리 필터** — `Hub.Subscribe(categories...)` / `Hub.WriteSSE(ctx, w, categories...)` 가변인자 추가. 카테고리 미지정 시 모든 이벤트 수신 (legacy 호환). 채널 버퍼 초과 시 비차단 drop + 카테고리별 카운터 (`internal/server/sse_hub.go`).
- **`eventstore.Event.Metadata`** — `map[string]any` 필드 추가 (`json:"metadata,omitempty"`). HealEvent 영속화에 사용 (phase, shard_id, correlation_id, duration_ms 등). 라운드트립 단위 테스트 포함 (`internal/eventstore/store.go`).
- **Self-healing Prometheus 메트릭** — `grainfs_heal_events_total{phase,outcome}`, `grainfs_heal_shards_repaired_total`, `grainfs_heal_duration_ms{phase}` (히스토그램), `grainfs_heal_stream_dropped_events_total` (`internal/metrics/metrics.go`).

### Changed
- **`scrubber.New(...)` 호출 후 `srv.HealEmitter()` 주입 순서 도입** — `cmd/grainfs/serve.go`에서 server 생성 후 scrubber emitter를 SetEmitter로 wiring하고 그 다음에 `sc.Start(ctx)`. 이 순서가 깨지면 대시보드에 HealEvent가 절대 도달하지 않음.

### Tests
- 단위 테스트 9개 신규: `TestHub_CategoryFilter_OnlyMatchingDelivered`, `TestHub_NoCategory_ReceivesAll`, `TestHub_MultipleCategories`, `TestHub_WriteSSE_HealCategoryOnly`, `TestHealEmitter_BroadcastsToHealCategoryOnly`, `TestHealEmitter_PersistsToEventStore`, `TestHealEmitter_NilHubAndEnqueue_NoPanic`, `TestHealEmitter_DoesNotBlockOnSlowSubscriber`, `TestRepairEngine_EmitsPhaseEvents`, `TestRepairEngine_NoEmitterIsSafe`, `TestStore_MetadataRoundTrip`, `TestStore_NoMetadataOmitsField`.
- E2E `TestDashboardHealingCard_HTMLAndStream` — 대시보드 HTML에 카드 마크업 존재 + `/api/events/heal/stream`이 `text/event-stream` + `Cache-Control: no-cache`로 응답하는지 검증 (`tests/e2e/dashboard_healing_card_test.go`).
- `-race -count=10` 회귀 통과 (server, scrubber, eventstore 패키지).

## [0.0.0.17] - 2026-04-20

### Added
- **Phase 16 Event Spine 스캐폴딩** — `internal/scrubber/event.go` 신규. `HealEvent` 타입 (UUIDv7 ID, RFC3339 timestamp, phase/outcome enum, correlation_id), `HealPhase` 상수 6개(detect/reconstruct/write/verify/startup/degraded), `HealOutcome` 상수 4개(success/failed/skipped/isolated), `Emitter` 인터페이스, `NoopEmitter` 기본 구현. 엔진에 아직 주입되지 않음 — 후속 PR에서 scrubber repair/orphan/verify 경로에 emit 지점 연결.
- **Unit + race 테스트** — `event_test.go` 8개 케이스. ID 유일성(200회), JSON 라운드트립, `omitempty` 검증, enum 문자열, 32 goroutine × 25 emit 동시성 확인 (`-race` 통과).

### Changed
- **TODOS.md Phase 16/17 경계 재조정** — Phase 16 "Self-healing storage" 항목에서 BadgerDB auto-recovery 하위 요소 제외 (Phase 17로 이동). Phase 17에 추가: BadgerDB atomic auto-recovery, Blame Mode v2(shard-level 시각적 replay), PagerDuty 네이티브 webhook. Phase 16 Transparent Self-Healing 설계(6주 스코프)와 정합성 맞춤.

## [0.0.0.16] - 2026-04-20

### Fixed
- **slog↔stdlib log 재귀 데드락 근본 수정** — `server.New()`의 `BroadcastHandler`가 `slog.Default().Handler()`를 wrap했는데, `slog.SetDefault`가 stdlib log 출력을 slog로 리다이렉트하여 첫 `slog.Info` 호출에서 `log.Logger` 뮤텍스 자기-재귀 데드락 발생. **프로덕션 바이너리 시작 시 데드락되어 NFS 서버가 accept를 시작하지 못하는 근본 원인**이었음. `BroadcastHandler.next`를 `slog.NewTextHandler(os.Stderr)`로 교체해 루프 차단. `New()` 반복 호출 시 재-래핑 가드 추가 (`internal/server/server.go`).
- **Event Log `emitEvent` unbounded goroutine leak** — 이벤트마다 새 goroutine 생성 → bounded channel(4096) + 단일 worker로 전환. 큐 가득 시 `eventDropsTotal` atomic counter로 drop 기록 (`slog.Warn` 제거로 재귀 데드락 회피). `Server.Shutdown`에서 drain 보장 (`internal/server/events_api.go`, `internal/server/server.go`).
- **`/api/eventlog` since/until dual-mode 혼란** — `<=3600`이면 상대 오프셋, 초과하면 절대 Unix 초로 해석하는 이중 의미 제거. UI의 "Last 24h"/"Last 7d" 옵션이 1970년 근처를 가리키던 버그 수정. 이제 since/until 모두 "현재로부터 상대 초"로 통일 (`internal/server/events_api.go`).
- **`handleFormUpload` event emit 누락** — 다른 PUT 경로는 모두 `EventActionPut`을 emit하는데 S3 POST form upload 경로만 누락되어 있던 관찰성 격차 해결 (`internal/server/handlers.go`).
- **TestNBD_Docker 포트 대기 레이스** — `docker/nbd-test.sh`가 S3 `/metrics`만 확인하고 NBD 포트는 대기하지 않아 `nbd-client` 연결 시 race. bash `/dev/tcp` TCP probe로 NBD 포트 10809 리스닝 대기 추가.

### Added
- **TODOS.md "Zero Config, Zero Ops" 섹션** — 운영자 개입 없이도 안정적으로 동작하기 위한 self-healing, preflight check, critical alert, predictive warning, auto-recovery 등 작업 항목 정의.
- **회귀 테스트 3개** — `TestEmitEvent_BoundedQueueNoGoroutineLeak`, `TestFormUpload_EmitsEvent`, `TestQueryEventLog_SinceLargeRelativeOffset` (`internal/server/events_api_test.go`).

## [0.0.0.15] - 2026-04-20

### Added
- **Phase 15: Unified Event Log** — S3 operations + system events를 하나의 BadgerDB-backed append-only log에 기록. `internal/eventstore` 신규 패키지 (ev: 키 prefix, 나노초 big-endian 키, 7일 TTL).
  - `GET /api/eventlog?since=<초>&type=<s3|system>&limit=<N>` — 시간/타입 필터 지원.
  - **S3 핸들러 이벤트**: `createBucket`, `deleteBucket`, `handlePut`, `getObject`, `deleteObject`, cluster join.
  - **System 이벤트**: snapshot create/restore/delete.
  - **대시보드 Events 탭** — 타입 필터(All/S3/System), 시간 범위 필터(5min/1hr/24hr), 이벤트 테이블(time/type/action/bucket/key/size).
  - **대시보드 Snapshots 탭** — create(reason 입력), list, restore, delete UI.
- **`storage.DBProvider` 인터페이스** — `DB() *badger.DB` 노출. `LocalBackend`가 구현하여 eventstore 공유 가능.
- **Fire-and-forget `emitEvent`** — 이벤트 저장 실패해도 S3 요청 실패하지 않음.

### Fixed
- **Events 탭 "Invalid Date" 표시 버그** — `e.ts`는 나노초(int64), JS `Date()`는 밀리초 기대. `new Date(Math.floor(e.ts / 1e6))`으로 변환 (`internal/server/ui/index.html:779`).

## [0.0.0.14] - 2026-04-20

### Added
- **SSE 실시간 이벤트 스트림** (`GET /api/events`) — `Hub` fan-out 패턴으로 N개 대시보드 클라이언트에 `text/event-stream` 전달. 느린 클라이언트는 non-blocking drop으로 head-of-line 방지.
- **라이브 로그 스트림** — `BroadcastHandler`가 slog 기본 핸들러를 감싸 모든 로그 레코드를 SSE `log` 이벤트로 팬아웃. 대시보드에서 ERROR/WARN/INFO/DEBUG 레벨 필터 지원.
- **Hot Config API** (`PATCH /api/admin/config`, localhost 전용) — 재시작 없이 스크러버 인터벌 변경. `scrubber.SetInterval(d)` 채널 기반 핫-리로드, 대시보드 Hot Config 패널에서 즉시 적용.
- **대시보드 UI 개선** — SSE 연결 상태 배지(live/reconnecting/disconnected), 라이브 로그 패널(100줄 링버퍼, 색상 레벨), Hot Config 폼 추가.

## [0.0.0.13] - 2026-04-20

### Fixed
- **Raft waiter correctness 버그 수정** — `HandleAppendEntries` log truncation 및 `HandleInstallSnapshot` 시 `n.waiters` map이 정리되지 않아 발생하던 false-success 시나리오 제거.
  - `waiters map[uint64]chan struct{}` → `waiters map[uint64]chan error`로 전환. `close(ch)` = nil = 성공, `ch <- ErrProposalFailed` = 실패.
  - `abortWaitersFrom(from uint64)` 헬퍼 추가 — truncation 시 영향받는 index의 goroutine을 즉시 종료.
  - `HandleAppendEntries` 두 truncation 경로 및 `HandleInstallSnapshot` 에 `abortWaitersFrom` 호출 추가.
  - split-brain 상황에서 다른 Leader가 같은 index에 다른 엔트리를 커밋할 때 원래 제안자에게 SUCCESS가 잘못 전달되던 Raft 안전성 불변식 위반 수정.

## [0.0.0.12] - 2026-04-20

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

## [0.0.0.11] - 2026-04-19

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

## [0.0.0.10] - 2026-04-19

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

## [0.0.0.9] - 2026-04-19

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

## [0.0.0.8] - 2026-04-18

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

## [0.0.0.7] - 2026-04-18

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

## [0.0.0.6] - 2026-04-18

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

## [0.0.0.5] - 2026-04-18

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

## [0.0.0.4] - 2026-04-18

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

## [0.0.0.3] - 2026-04-18

### Added
- **Self-healing MVP** — background EC shard scrubber (`--scrub-interval`, 기본 24h). 누락·손상 shard 자동 감지 후 Reed-Solomon으로 복구. `GET /admin/health/scrub`으로 상태 확인.
- **CRC32 shard footer** — 모든 EC shard에 4바이트 CRC32-IEEE footer 기록/검증. bit-rot 감지.
- **Crash-safe WriteShard** — tmp+fsync+rename+dir-fsync 패턴으로 전원 손실 시 partial shard 방지.
- **RWMutex per-key locking** — 스크러버 Verify는 RLock (클라이언트 GET 동시 허용), Repair는 Lock (exclusive).
- **Scrub metrics** — `grainfs_scrub_shard_errors_total`, `grainfs_scrub_repaired_total`, `grainfs_ec_degraded_total`, `grainfs_scrub_objects_checked_total`, `grainfs_scrub_skipped_over_cap_total` Prometheus 지표 추가.

### Changed
- **`--scrub-interval` CLI flag** — `grainfs serve --scrub-interval=24h` (기본값). `0` 으로 비활성화.
- **EC shard format** — CRC32 footer 추가로 기존 shard(CRC 없음)는 scrubber에서 corrupt로 감지됨. 첫 scrub cycle에 자동 rewrite.

## [0.0.0.2] - 2026-04-18

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

## [0.0.0.1] - 2026-04-17

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
