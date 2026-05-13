# Changelog

## [0.0.186.0] - 2026-05-14 — feat: QUIC mux capability exchange handshake

### Added

- **`ProtocolVersionMux = "grainfs-mux-v1"`** — `internal/transport/version.go`에 단일 소스 프로토콜 버전 상수 추가. `muxALPN()`이 이 상수를 반환하도록 변경.
- **`StreamCapabilityExchange = 0x12`** — mux QUIC 연결의 첫 스트림으로 사용하는 CE(Capability Exchange) 스트림 타입 추가.
- **Capability Exchange 핸드셰이크** — 모든 mux QUIC 연결 수립 시 2바이트(`version=0x01, features=0x00`) CE를 교환. 버전 불일치 시 `"capability exchange failed"` 에러와 함께 연결 종료.
- **`ceRejectionCloseDelay = 200ms`** — CE 거부 시 피어가 에러 응답을 읽을 시간을 확보한 후 `CloseWithError` 호출.
- **5개 CE 테스트** — `TestMuxALPNConstant`, `TestVersionHandshakeSuccess`, `TestMixedVersionRejection`, `TestCapabilityExchangeTimeout`, `TestCapabilityWrongFirstStream`.

### Fixed

- **`TestQUICTransport_MuxRejectedWithoutHandler`** — CE 실패 경로에 맞게 테스트 단순화.

### Verification

- `go test ./internal/transport/... -run TestVersionHandshake` PASS
- `go test ./internal/transport/... -run TestMixedVersion` PASS
- `go test ./internal/transport/...` PASS (coverage: 73.4%)

## [0.0.185.0] - 2026-05-14 — fix: Colima Linux tests and NBD/NFS fast paths

### Added

- **Colima Linux test integration** — Linux-dependent NBD/NFS/direct I/O coverage now runs through Colima without Docker and is wired into `make test`.
- **NBD/NFS profiling harness updates** — benchmark scripts run directly against the host binary, support pprof/direct fio options, and record NBD write-path trace data.
- **S3 user metadata persistence** — storage and cluster object metadata now carry user metadata through PutObject/CopyObject paths.

### Fixed

- **Docker removal** — deleted Docker-based e2e/benchmark scaffolding and updated docs/scripts to use direct host binary + Colima VM clients.
- **NFSv4 COPY/READDIR/rename behavior** — fixed offset-aware COPY, READDIR attr encoding, parent cache invalidation, and internal-bucket rename writes.
- **Internal bucket partial I/O routing** — internal buckets bypass user object-index paths and hard-delete internal metadata instead of creating S3 delete markers.
- **NBD fast path capability propagation** — pull-through now forwards `PartialIO`, `PreferWriteAt`, and async put capabilities so NBD volume writes can use `DistributedBackend.WriteAt`.
- **Single-node duplicate-self topology** — routing/backend write-at checks treat repeated local peer entries as one physical voter, preserving local pwrite fast paths in single-node EC-shaped topologies.

### Verification

- `go test ./internal/storage/pullthrough ./internal/cluster ./internal/volume -run 'TestPullThrough_ForwardsPartialIOCapabilities|TestOpRouter_RouteBucket_DuplicateSelfIsOnlyVoter|TestPreferWriteAt|TestClusterCoordinator_PreferWriteAt|TestClusterCoordinator_WALWriteAtReadAt'`
- `go test ./internal/nbd -run 'Test' -timeout 60s`
- `go test ./internal/volume/dedup -run '^$'`
- `make build`

## [0.0.184.0] - 2026-05-14 — feat: bucket policy/versioning admin API + CLI

### Added

- **`grainfs bucket policy get/set/delete <bucket>`** — admin UDS를 통해 버킷 S3 bucket policy 조회·설정·삭제. `set`은 `--file <path>` 또는 stdin(`-`)으로 JSON 정책 수신.
- **`grainfs bucket versioning get/enable/suspend <bucket>`** — 버킷 버저닝 상태 조회·활성화·일시정지.
- **`bucket list` + `bucket info`** — `HAS_UPSTREAM` 열 추가. `bucket info`는 `VERSIONING` 열도 추가.
- **`GET/PUT/DELETE /v1/buckets/:name/policy`** — admin HTTP API. PUT은 빈 바디 시 400 반환.
- **`GET/PUT /v1/buckets/:name/versioning`** — admin HTTP API. PUT의 `status`는 `Enabled` / `Suspended` 만 허용.
- **`AdminGetBucket` 응답 보강** — `has_upstream`, `versioning` 필드 포함.
- **`AdminListBuckets` 응답 보강** — `has_upstream` 필드 포함.

### Fixed

- **`bucket upstream list` 파싱** — 서버가 raw JSON array를 반환하는데 wrapped struct로 unmarshal 시도하던 버그 수정.
- **PUT `/v1/buckets/:name/policy` 빈 바디 허용** — body 없이 호출 시 `policy` 필드 누락 검사 없이 통과하던 버그. 이제 400 반환.

### Verification

- `make test-e2e -run TestBucketUpstream_CLIRoundtrip` PASS
- `make test-e2e -run TestBucketUpstream_LegacyCLI_Removed` PASS

## [0.0.183.0] - 2026-05-14 — test: dynamic MR cluster E2E + clusterpb fbs fix

### Added

- **`TestE2E_TwoNodeAvailabilityTrap`** — 2-노드 quorum 손실 시 쓰기가 hang(context.DeadlineExceeded)으로 실패함을 문서화하는 회귀 테스트 추가.
- **`TestE2E_DynamicGroupSeeding_1to5`** — `addNode`를 통한 1→5 순차 노드 확장 후 shard group 수가 `seedGroupCountForClusterSize(n)=max(n*4,8)` 공식에 따라 증가함을 검증하는 회귀 테스트 추가.
- **`mrCluster.addNode`** — 동적 노드 추가: `.join-pending` 파일 쓰기 후 노드 기동, HTTP 준비 대기, leaderIdx 갱신.
- **`startMRCluster` / `tryStartMRCluster`** — 동적 sequential join 방식(노드 0 부터 순차 부팅)으로 클러스터 기동. `FastBootstrap` 옵션으로 `time.Sleep(8s)` 대신 shard-group 폴링 사용.
- **`waitForShardGroupCount`** — admin UDS `/v1/cluster/status`를 폴링해 shard group 수가 충족될 때까지 대기하는 헬퍼.
- **`liveURLs()` 헬퍼** — `MaxNodes > nodeCount`인 동적 클러스터에서 미기동 노드 URL을 제외하고 실행 중인 노드만 순회.

### Fixed

- **`clusterpb` fbs 스키마** — `cluster.fbs`에 `MigrationJobStart/Done/Failed` enum 값 누락으로 `make build`(`flatc` 재생성) 후 `MetaCmdType.go`에서 상수가 사라지던 버그 수정. (동일 수정이 PR #340에도 포함됨)

### Verification

- `go test -count=1 ./tests/e2e/ -run TestE2E_MultiRaftSharding` (145s, PASS)
- `go test -count=1 -race ./tests/e2e/ -run TestE2E_DynamicGroupSeeding_1to5` (344s, PASS)

## [0.0.182.0] - 2026-05-14 — feat: bucket & IAM CLI DX + security hardening

### Added

- **`grainfs bucket info <name>`** — admin UDS를 통해 버킷 정보(객체 수 포함) 조회. `--json` 플래그로 JSON 출력.
- **`grainfs bucket upstream` 서브커맨드** — `put / get / list / delete`로 버킷별 pull-through upstream 자격증명 관리.
- **tabwriter 테이블 출력** — `bucket list`, `bucket info`, `upstream get`, `upstream list`, `iam sa list`가 열 정렬된 테이블로 출력.
- **`--json` 플래그** — `bucket` 및 `iam` 커맨드에 `--json` persistent flag 추가. 스크립트 파이프라인 지원.
- **`GRAINFS_ADMIN_SOCKET` 환경변수** — `--endpoint` 생략 시 자동 폴백. 반복 입력 제거.
- **사용자 피드백 메시지** — `bucket create/delete`, `upstream put/delete`가 성공 시 명확한 확인 메시지 출력.
- **`iam sa` create/get/delete 출력** — SA 생성 시 access_key/secret_key 테이블 출력; get은 SA 상세 정보 표시.

### Fixed

- **`AdminGetBucket` UI 경로 노출 차단** — `RegisterUI`에서 `registerBucket` 제거. dashboard 토큰 보유자가 `CountObjects` 풀 스캔을 원격 트리거해 Badger 쓰기를 아사시킬 수 있는 취약점 제거. bucket admin ops는 admin UDS 전용.
- **upstream 라우팅 충돌** — `GET|PUT /v1/buckets/upstream`을 `GET|PUT /v1/upstreams`로 이동. Hertz static-beats-param 규칙으로 `bucket info upstream`이 upstream-list를 반환하던 버그 해소.
- **CLI 무한 대기** — `iamHTTPClient`에 30초 timeout 추가. 서버 비응답 시 CLI가 무한 대기하던 문제 해소.

### Verification

- `go test -count=1 ./cmd/grainfs/... ./internal/server/admin/... ./internal/serveruntime/...`
- `go list ./... | grep -v '^github.com/gritive/GrainFS/tests/e2e$' | xargs go test -count=1` — all PASS

## [0.0.181.0] - 2026-05-14 — fix: ForceDeleteBucket 버그 3종 수정

### Fixed

- **ForceDeleteBucket Badger MVCC snapshot 누수** — `db.View` 내부에서 Raft `propose`를 호출하면 N×RTT 동안 MVCC 스냅샷이 유지돼 Badger GC를 차단했던 문제 수정. 스캔과 propose를 분리(View → collect refs → propose).
- **ForceDeleteBucket ctx 전파 누락** — 내부 루프에서 `ctx`를 전달하지 않아 컨텍스트 취소가 무시되던 버그 수정.
- **ForceDeleteBucket multi-version 오브젝트 미삭제** — `WalkObjects`는 키당 최신 버전만 반환해 이전 버전 Badger 키가 남아 `DeleteBucket`이 `ErrBucketNotEmpty`를 반환했던 버그 수정. `obj:<bucket>/` 전체 키를 직접 스캔하도록 변경.
- **ForceDeleteBucket ring refcount double-decRef** — 버전된 오브젝트의 unversioned ObjectMetaKey(`obj:<bucket>/<key>`)와 versioned 키를 모두 삭제할 때 ring `decRef`가 이중으로 호출되던 버그 수정. versioned ref를 먼저 처리(two-pass)해 `applyDeleteObjectVersion`이 ObjectMetaKey를 정리한 뒤 unversioned ref가 처리되도록 함.
- **`AdminDeleteBucket` force=true 시 ErrBucketNotEmpty → 503 retry** — `--force`로 삭제 중 concurrent write로 `ErrBucketNotEmpty`가 발생하면 `"use --force"` 메시지 대신 503 retry 응답 반환.

## [0.0.180.2] - 2026-05-14 — fix: cluster benchmark and e2e latency regressions

### Fixed

- **Cluster runtime topology publication** — runtime join paths now publish cluster node topology and EC config as immutable snapshots so writes do not stay pinned to boot-time placement after nodes join. Coordinator routing/execution state now refreshes atomically with EC config.
- **Cluster benchmark harnesses** — NFS, NBD, S3, and Iceberg cluster benchmarks now use dynamic join flow, shared encryption keys, admin socket readiness checks, node log archival, configurable node counts, and profile/runtime parameters.
- **Benchmark auth and partial I/O setup** — Iceberg benchmark setup signs bucket creation with IAM credentials; NFS/NBD benchmark scripts wait for admin socket/CPU profile completion and quote runtime parameters correctly.
- **Raft log reads** — badger raft log range reads now fetch contiguous indexes directly and fail on missing or mismatched entries instead of iterator-skipping metadata keys.
- **NFSv4 backend capability checks** — NFS operations now have explicit backend capability coverage for partial I/O behavior.
- **e2e harness latency** — static cluster startup removed fixed sleeps, process cleanup terminates signal-ignoring test children immediately, S3 e2e clients disable keep-alives, and expiring-key tests poll observed expiry instead of sleeping.
- **IAM plaintext secret test scope** — the no-plaintext-secret e2e check now scans the IAM control-plane `meta_raft` persistence path instead of unrelated data-plane directories.
- **Small Badger metadata DBs** — `SmallOptions` now caps value log files at 64 MiB to reduce test/runtime metadata store footprint.
- **Auto-snapshot hot reload** — disabled snapshot polling idle interval reduced from 5s to 1s, bounding cluster config hot-reload latency.

### Verification

- `go test -count=1 ./internal/badgerutil ./internal/iam ./internal/snapshot ./internal/cluster ./internal/nfs4server ./internal/serveruntime ./internal/raft`
- `go build -o bin/grainfs ./cmd/grainfs`
- `GRAINFS_BINARY=$PWD/bin/grainfs go test -json -short -count=1 -timeout 5m ./tests/e2e` — PASS, 50.658s
- `go list ./... | grep -v '^github.com/gritive/GrainFS/tests/e2e$' | xargs go test -count=1`

## [0.0.180.1] - 2026-05-13 — fix: RUNBOOK bootstrap 절차 및 snapshot audit log

### Fixed

- **Bootstrap docs** — RUNBOOK deployment section now documents direct host binary startup and host-side `admin.sock` bootstrap.
- **K8s bootstrap** — RUNBOOK K8s 섹션에 최초 배포 후 admin SA 생성 절차(`kubectl exec deploy/grainfs -n grainfs -- grainfs iam sa create admin`) 추가.
- **snapshot-interval / snapshot-retain audit log 누락** — `ClusterConfigPatch`의 `SnapshotInterval`, `SnapshotRetain` 필드가 FSM에 적용될 때 audit dict에 포함되지 않아 변경 이력 추적이 불가능했던 문제 수정.

## [0.0.180.0] - 2026-05-13 — feat: bucket & IAM admin API 통합 + CLI bucket 커맨드

### Added

- **Bucket admin API** — admin UDS를 통해 버킷 생성(`POST /v1/buckets`), 목록 조회(`GET /v1/buckets`), 삭제(`DELETE /v1/buckets/:name?force=true`)를 수행할 수 있는 REST 엔드포인트 추가. `--force` 플래그로 비어있지 않은 버킷도 강제 삭제 가능.
- **`grainfs bucket create/list/delete` CLI 커맨드** — 운영자가 admin UDS를 통해 버킷을 직접 관리. `grainfs bucket delete --force <name>`으로 모든 오브젝트 삭제 후 버킷 제거.
- **IAM admin 핸들러를 volume 패턴으로 통합** — 기존 `iam_admin.go` / `bucket_admin.go`가 hertz_adapter의 `registerIAM` / `registerBucket`으로 대체됨. 순수 함수 핸들러 + thin adapter 구조로 단위 테스트 용이.
- **`IAMService` / `BucketOps` 인터페이스** — `admin.Deps`가 구체 타입 대신 인터페이스를 참조. 테스트에서 fake 구현으로 대체 가능.
- **`ForceDeleteBucket`** — `Backend` 인터페이스에 추가. 모든 구현체(LocalBackend, Operations, DistributedBackend, SwappableBackend, PackedBackend, RecoveryWriteGate)에 구현.

### Fixed

- **S3 ListBuckets에서 내부 버킷 노출 차단** — `__grainfs_*` 접두사 버킷이 S3 ListBuckets 응답에 포함되던 버그 수정.
- **admin HTTP 403 복원** — `statusForCode("forbidden")` 누락으로 wildcard grant 거부 시 500을 반환하던 버그 수정 → 403으로 복원.
- **`AdminCreateBucket` 버킷명 검증** — 슬래시/대문자/특수문자를 포함한 이름이 허용되던 버그 수정. Badger 키 충돌 및 LocalBackend path traversal 방지. `storage.ValidBucketName` 추가.
- **`AdminDeleteBucket` 내부 버킷 삭제 차단** — `__grainfs_*` 버킷에 대한 force-delete가 허용되던 버그 수정 → 403 Forbidden 반환.

### Changed

- `POST /v1/iam/sa` 등 생성 엔드포인트가 200 대신 **201 Created** 반환 (RFC 9110 준수).

## [0.0.179.0] - 2026-05-13 — chore: remove non-EC object write path

### Removed

- **Non-EC write path** (`putObjectNxSpooled`, `putObjectNxSpooledAsync`, `writeSpooledReplicaShardStream`)
  eliminated. All object writes now go through EC storage exclusively. Clusters that do not have
  a `ShardService` configured (EC not active) will receive a clear error on write rather than
  silently falling back to a replication-only path.
- `ReplicationSkippedTotal` Prometheus metric removed (no remaining callers after Nx path deletion).
- `shardWriter` and `shardBufferedWriter` interfaces removed along with their only implementations.

### Changed

- `CreateMultipartUpload` guard relaxed for direct `DistributedBackend` callers: missing placement
  context is now permitted when `bypassBucketCheck` is false (resolves to `group-0` at write time).
  `GroupBackend` callers with `bypassBucketCheck=true` still receive an error for missing placement.
- `PutObjectAsync` simplified to a thin wrapper around `putObjectECSpooled`; returned `commitFn` is
  always a no-op for API compatibility.
- `PeerUnhealthy` metric help text updated to reflect EC stripe degradation (not N-way replication).

## [0.0.178.0] - 2026-05-13 — fix: PromoteToVoter orphan recovery in Raft v2 becomeLeader

### Fixed

- **`recoverOrphanedPromote()`** added to `internal/raft/membership.go`, called from
  `becomeLeader()` after `recoverInFlightJoint()`. Handles the crash scenario where the
  prior leader committed Stage-1 (`ConfChangePromoteStage1` — drops target from learners)
  but crashed before appending Stage-2 (`LogEntryJointConfChange`). The orphaned target
  is left in neither voters nor learners, blocking it from participating in consensus.
- Recovery synthesises `pendingSingleConf` (pointing to the Stage-1 log index) and
  `pendingPromote` so the existing `advanceSingleConfPhase` machinery dispatches Stage-2
  on the new leader. When Stage-1 is already committed at `becomeLeader` time the call
  is driven inline; otherwise `applyCommitted → advanceSingleConfPhase` fires it.
- `matchIndex`/`nextIndex` for the orphaned target is seeded to
  `(0, lastLogIndex+1)` when absent — the normal path seeds these when the target joins
  as a learner, but `becomeLeader` skips it since the target is no longer in
  `currentConfig.learners` after Stage-1.
- **`handleCreateSnapshot` snapshot guard** (`internal/raft/snapshot_actor.go`): refuses
  to compact the log past the Stage-1 index while `pendingPromote` is in-flight. Without
  this guard, a periodic FSM snapshot taken between Stage-1 commit and leader crash would
  erase the Stage-1 log entry, silently disabling `recoverOrphanedPromote` on the new
  leader. Error message instructs the operator to retry after Stage-2 commits.

### Notes

- **MetaRaft.Join operator action**: `recoverOrphanedPromote` completes the Raft membership
  promotion but `ProposeAddNode` (the `MetaNodeEntry` write that follows `PromoteToVoter`
  in `MetaRaft.Join`) never ran on the crashed leader. After recovery, the operator must
  re-issue `Join` for the orphaned target to register it in the meta-Raft node table.

### Verification

- `go test -race ./internal/raft/ -run TestPromoteToVoter_OrphanRecovery -count=20` — all PASS
- `go test ./internal/raft/ -timeout 120s -count=1` — all PASS (63 s, 63 tests)

## [0.0.177.0] - 2026-05-13 — fix: RouteObjectWrite preserves forward peers when self is leader

### Fixed

- `OpRouter.RouteObjectWrite` now populates `RouteTarget.Peers` even when
  `SelfIsLeader` is true. Previously, `routeGroup` short-circuited and left
  `Peers` empty, so if leadership changed between routing and execution the
  write had no forward candidates. `RouteBucket` still uses the short-circuit
  path (peers empty on leader) — only the object-write path resolves peers.

### Verification

- `go test -count=3 ./internal/cluster/ -run TestOpRouter_Route` — all PASS
- `go test -count=1 ./internal/cluster/` — all PASS

## [0.0.176.0] - 2026-05-13 — feat: SendTimeoutNow QUIC RPC (leader transfer)

### Added

- `SetTimeoutNowTransport` on `RaftNode` interface and `raftNodeAdapter`/`raftTransportBridge`;
  uses `atomic.Pointer[timeoutNowFn]` for lock-free late binding. Returns `ErrNotImplemented`
  when not wired (nil pointer), same fallback contract as `SendInstallSnapshot`.
- `sendTimeoutNow` + `SetTimeoutNowTransport` on `RaftQUICRPCTransport`; wire format is
  byte-identical to the v1 QUIC codec using a new `v2RPCTypeTimeoutNow` message type.
- `v2RPCTransport.SetTimeoutNowTransport()` call in `serveruntime.Run`; logged as
  "raft v2: QUIC RPC transport wired (TimeoutNow enabled)".

### Fixed

- `TransferLeadership` (Raft §3.10) now works end-to-end over QUIC in multi-node v2 clusters.
  Previously `SendTimeoutNow` returned `ErrNotImplemented`, causing the transfer target to miss
  the TimeoutNow signal and rely on the natural [T, 2T) election window instead.

### Verification

- `go build ./...`
- `go test ./internal/cluster/ -run TestSendTimeoutNow` (unit: ErrNotImplemented when unwired)
- `go test ./internal/cluster/ -run TestSetTimeoutNowTransport` (unit: nil-bridge no-panic)
- `go test ./internal/cluster/ -run TestV2QUICCluster_ThreeNode_TransferLeadership -count=5`
  with ET=5s discriminator: new leader appears within 2s, proving TimeoutNow fired.

## [0.0.175.0] - 2026-05-13 — fix: eliminate peerHealth race in ecObjectReader goroutine drain

### Fixed

- Moved `peerHealth.MarkHealthy`/`MarkUnhealthy` calls from spawned shard-fetch
  goroutines to the main goroutine in `ecObjectReader.readShards`. Previously,
  k-of-n early exit could leave a goroutine still executing `MarkUnhealthy` while
  the caller already read `health.unhealthy`, producing a DATA RACE under
  `-race`. The fix encodes peer state (`peer`, `peerOK`, `canceled`) in
  `shardResult` and processes it in `applyShardResult` and the drain loop —
  both running on the single main goroutine.

### Verification

- `go test -race -count=100 ./internal/cluster/ -run TestECObjectReader_ReadObject_MarksUnhealthyPeerOnFetchError` — 100/100 PASS, 0 DATA RACE
- `go test -race -count=100 ./internal/cluster/ -run TestECObjectReader_ReadObject_MarksHealthyPeerOnSuccess` — 100/100 PASS, 0 DATA RACE

## [0.0.174.0] - 2026-05-13 — fix nbd cow snapshot cli flags

### Fixed

- nbd cow snapshot cli flags

## [0.0.173.0] - 2026-05-12

### Fixed

- fix raft quic e2e raft identity wiring

## [0.0.172.0] - 2026-05-12

### Fixed

- fix cluster leader route short circuit

## [0.0.171.0] - 2026-05-09

### Fixed

- raft: restore raft v2 node consensus test (#316)

## [0.0.170.0] - 2026-05-09

### Added

- raft: add dead peer detection via PeerHealth for QUIC transport (#315)

## [0.0.169.0] - 2026-05-08

### Fixed

- raft: fix quic transport race condition in peer health tracking (#313)

## [0.0.168.0] - 2026-05-08

### Added

- raft: implement basic QUIC transport for Raft v2 (#310)

## [0.0.167.0] - 2026-05-06

### Added

- raft: implement hashicorp/raft adapter for Raft v2 (#308)

## [0.0.166.0] - 2026-05-02

### Added

- grpc: implement basic gRPC transport for Raft v2 (#306)

## [0.0.165.0] - 2026-04-30

### Added

- raft: implement Raft v2 node (#303)
