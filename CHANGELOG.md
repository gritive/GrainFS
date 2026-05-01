# Changelog

## [0.0.7.2] — 2026-05-01 — Raft stuck-joint abort (JointOpAbort)

### Added

- **raft**: `JointOpAbort` — new `JointOp` enum value (= 2) that reverts the cluster from `JointEntering` back to C_old under C_old-only quorum. Allows recovery when C_new loses majority after `JointEnter` commits, ending the deadlock where no entry (including `JointLeave`) could commit.
- **raft**: `ForceAbortJoint(ctx) error` — public API on `*Node`. Leader-only, valid in `JointEntering` phase. Proposes `JointOpAbort`; returns `nil` on success, `ErrNotLeader` / `ErrNotInJointPhase` / `ErrProposalFailed` / `ctx.Err()` otherwise.
- **raft**: `Config.JointAbortTimeout time.Duration` — optional auto-abort. When set, `checkJointAdvance` triggers abort after this duration elapses from `JointEnter` commit. Default `0` (disabled).
- **raft**: `ErrJointAborted` / `ErrNotInJointPhase` — two new sentinel errors. `ErrJointAborted` propagates to `ChangeMembership` callers so they can distinguish abort from success.
- **raft**: `jointResultCh chan error` (buffered 1) replaces `jointPromoteCh chan struct{}`. Sends `nil` on `JointLeave` commit, `ErrJointAborted` on `JointAbort` commit. Buffer prevents apply-loop deadlock when caller context cancels before abort commits.
- **raft**: `rebuildConfigFromLog` handles `JointOpAbort` — restores C_old from entry payload (`OldServers`) so a node restarted after an abort does not re-enter `JointEntering` on the next leader tick.

### Fixed

- **raft**: `applyJointConfChangeLocked` JointOpAbort idempotency guard — no-op when `jointPhase != JointEntering`, preventing double-apply if both `JointLeave` and `JointAbort` are in flight.
- **raft**: `initLeaderState` now resets `jointLeaveProposed` and `jointAbortProposed` on every leader election, allowing the new leader to re-trigger auto-abort if a previous abort goroutine was in flight when the old leader stepped down.
- **raft**: `rebuildConfigFromLog` now resets the `jAborted` flag on `JointOpEnter`, preventing a prior cycle's abort from silently skipping the subsequent cycle's `JointOpLeave` during log replay (multi-cycle abort→reenter→leave sequence).
- **raft**: `applyJointConfChangeLocked` `JointOpLeave` now resets `jointAbortProposed = false`, clearing any stale flag from a racing abort proposal that resolved before the leave committed.
- **raft**: `RestoreJointStateFromSnapshot` now also resets `jointAbortProposed` and `jointEnterTime` so a newly elected leader starting from a snapshot does not inherit stale abort-proposal state.
- **raft**: `triggerAbortAsync` goroutine now tracked by `n.wg`, preventing it from accessing node state after `Close()` returns.

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
