# Changelog

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
