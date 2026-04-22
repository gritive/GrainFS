# Changelog

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
