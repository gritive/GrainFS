# Changelog

## [0.0.271.0] - 2026-05-20 - perf(tests): speed up cluster and server suites

### Changed

- Shortened slow `internal/cluster` tests by replacing fixed sleeps with
  condition-based waits, tightening MetaRaft/QUIC timing windows, reducing
  oversized multipart and EC payloads, and using test-only chunk/reply limits
  where the behavior under test does not require production-sized buffers.
- Shortened `internal/server` tests by waiting for TCP readiness instead of
  sleeping, using bounded test shutdowns for Hertz servers, and disabling
  read-after-write retry only in generic test server helpers so production
  retry behavior remains unchanged.
- Reduced Badger allocation pressure in local metadata, audit outbox, and
  test-only stores by reusing `badgerutil.SmallOptions` for small metadata DBs.
- Trimmed avoidable storage allocations by letting `SegmentWriter` pass owned
  chunk buffers directly to byte-oriented segment backends instead of copying
  each chunk into a second slice.

## [0.0.270.0] - 2026-05-20 - feat(auth): §4 Iceberg JWT + OAuth + warehouse-aware MetaCatalog

§4 lands the Iceberg Auth layer: clients can now mint short-lived bearer tokens
via OAuth2 `client_credentials`, hit any `/iceberg/v1/*` route with that token,
and operate on per-warehouse table state that stays isolated across tenants.
JWT signing keys rotate atomically across all cluster nodes (no split-brain),
persist wrapped-at-rest in the meta-raft snapshot, and the catalog FSM is
re-keyed per `(warehouse, namespace, table)` so two warehouses sharing a name
no longer collide.

### Added

- **`internal/iam/jwt`** — HS256 mint/verify with `kid` dual-key rotation
  window, `alg=none`/`RS256` rejection, 30s clock-skew, wrap-at-rest seeds
  unwrapped via DEK. New errors: `ErrAlgNotHS256`, `ErrKidUnknown`,
  `ErrClockSkew`, `ErrPrunePrev`.
- **OAuth2 token endpoint** at `POST /iceberg/v1/oauth/tokens` and
  `POST /_iceberg/v1/oauth/tokens`. Accepts `client_credentials` via form body
  or HTTP Basic, validates `client_secret` in constant time, gates token mint
  on `iceberg:GetCatalogConfig`, returns RFC 6749 `bearer` token type. Rejects
  empty/URI-shaped/multi `PRINCIPAL_ROLE` scopes.
- **Iceberg bearer middleware** (`internal/server/iceberg_authn.go`) — anon
  short-circuit when `iam.anon-enabled=true`, JWT verify with case-insensitive
  `Bearer ` prefix, warehouse-claim cross-check (`?warehouse=` query or path
  segment must match `claims.Warehouse`), policy gate per-action.
- **Warehouse-aware MetaCatalog** (D#14) — every method takes
  `warehouse string`. FSM `icebergNamespaces`/`icebergTables` maps re-keyed
  `map[warehouse]map[ns]X`. Metadata cache also warehouse-scoped to prevent
  cross-warehouse evictions.
- **JKEY snapshot trailer** (`0x59454B4A`) — wrapped JWT signing seeds
  persisted as the outermost meta-FSM snapshot trailer (peels before IPST →
  DKVS → GCFG → IAMG). MetaCmds 63 (`JWTSigningKeyRotate`) and 64
  (`JWTSigningKeyPrune`) carry deterministic payloads minted on the leader.
- **Iceberg snapshot schema v2** — entries carry warehouse field; v1
  snapshots with data fail loud at restore (no silent default-warehouse
  routing).

### Changed

- **Default warehouse identifier** is the constant `IcebergDefaultWarehouse`
  (`"default"`) for FSM keys, separated from the S3 URL prefix used for object
  paths. Boot constructors pass `defaultWarehouse + s3URLPrefix` separately.
- **Metadata object paths** include the warehouse segment when the warehouse
  is non-default and not equal to the S3 prefix (defense against bearer-claim
  URI-shaped warehouse names plus segment collisions across tenants).
- **Bearer requests bypass SigV4 verifier** at the auth middleware boundary.
  Before, any `Authorization: Bearer …` Iceberg request was rejected as a
  malformed SigV4 signature before reaching `icebergGuarded`.
- **Restore atomicity** — meta-FSM Restore stages every decoded section in
  locals and commits to `f.*` only after every trailer decode succeeds.
  JKEY `LoadFromSeeds` runs against a scratch KeySet before commit.

### Fixed

- **Deterministic JWT MetaCmd apply** — `MetaCmdTypeJWTSigningKeyRotate`/
  `Prune` previously called `rand.Read` + `Seal` + `time.Now` inside FSM
  apply, so every node minted a different secret and tokens minted on node A
  failed on node B. Mint moved to proposer; payload carries
  `(kid, wrapped_secret, dek_gen, demoted_at_unix)`.
- **JWT KeySet production wiring** — `bootSrvOptsAndReceipt` now threads
  `metaRaft.FSM().JWTKeySet()` through `server.WithJWTKeySet(...)`. Previously
  `s.jwtKeys` was nil at runtime, so OAuth returned 503 and bearer middleware
  said "not configured".
- **OAuth invalid-client timing** — unknown access_key path now runs a
  constant-time compare against a sentinel before returning; access-key
  enumeration via response latency no longer works.
- **Legacy `icebergcatalog.Store` warehouse guard** — single-warehouse
  fallback Store rejects non-default warehouse values instead of silently
  ignoring them.

### Removed

- Silent fallback that mapped empty PRINCIPAL_ROLE scope to the default
  warehouse. OAuth now returns `400 invalid_scope` when the warehouse is
  empty, URI-shaped, contains `/`, or contains `..`.



Large multipart completion in cluster mode now streams completed parts directly
into the segment writer and commits the final object with one atomic raft
command. The slice removes the old large-object complete spool path while
preserving segment metadata, multipart part metadata, tags, and ring-version
placement evidence.

### Added

- **Multipart complete manifest reader**: validates requested parts, enforces S3
  part-number limits, streams part bodies in order, and surfaces pending close
  errors instead of buffering the completed object into a temp spool.
- **Atomic `CmdCompleteMultipart` segment commit**: chunked multipart completion
  now proposes `CompleteMultipartCmd` with final object metadata, multipart
  parts, segment refs, tags, placement, and ring version in the same raft entry.
- **Chunked multipart e2e coverage**: verifies large multipart upload completion
  through the cluster chunked path, including `GET ?partNumber=N` reads.

### Changed

- **Large multipart complete hot path**: routes chunk-threshold completions
  through `putMultipartObjectChunked` so payload bytes are streamed from part
  files to segment writes without materializing a full completed object spool.
- **Chunked PUT parts support**: keeps regular chunked PUT able to commit
  multipart part metadata via `PutObjectMetaCmd`, while multipart completion
  uses the atomic `CmdCompleteMultipart` path.
- **Multipart part metadata copies**: clones part metadata at command/object
  boundaries so caller mutation cannot rewrite committed object state.

### Fixed

- **Ring-version preservation**: `CompleteMultipartCmd` now carries placement
  ring version for multipart segment metadata, avoiding stale placement records
  after completion.
- **Duplicate complete guard**: `applyCompleteMultipart` now verifies the upload
  row still exists and matches the target bucket/key in the same Badger
  transaction before writing final object metadata, preventing stale duplicate
  complete commands from overwriting latest metadata.
- **Multipart validation correctness**: rejects out-of-range part numbers and
  aligns cluster tests with the S3 multipart part-size rules.

### Verified

- `go test ./internal/cluster -run 'PutObjectChunked|RunChunkedPutWithParts|CompleteMultipart|Chunked|Segment|Tags|Ring_CompleteMultipartEC_UsesRingVersion' -count=1`
- `go test ./internal/cluster -count=1`
- `go build -o bin/grainfs ./cmd/grainfs`
- `go test ./tests/e2e -run 'TestMultipartsE2E/ChunkedUploadPart' -count=1`

### Known limitations

- Full `go test ./... -count=1` currently fails in unrelated `tests/e2e`
  bucket/IAM bootstrap and admin-grant cases (`AccessDenied` / admin UDS 404
  patterns). The focused chunked multipart e2e path passes.

## [0.0.268.0] - 2026-05-20 - fix(s3): stabilize warp benchmark coverage

Short 4-node S3 and Iceberg benchmark runs now cover the full requested matrix without multipart destabilizing the cluster. The branch also keeps benchmark setup closer to production behavior by precreating service-account buckets, attaching the Iceberg warehouse policy, and using 4-node Iceberg defaults.

### Changed

- **S3 benchmark harness bootstrap**: precreates warp buckets with service-account policy and attaches the Iceberg warehouse policy so signed benchmark clients exercise the intended auth path instead of failing during setup.
- **Iceberg cluster parity**: changed Iceberg benchmark defaults to 4 nodes, matching the S3 cluster benchmark topology.
- **Server request logging**: added structured S3 request logs with operation, subresource, bucket/key, status, byte counts, latency, service-account ID, and mapped error reason without draining streamed request bodies or reading streamed response bodies into memory.

### Fixed

- **Retention/versioning auth compatibility**: added IAM/action mapping and policy compile coverage for bucket versioning, versions listing, object retention, and object-lock configuration APIs used by warp compatibility runs.
- **Object-lock/retention compatibility endpoints**: accepts object-lock configuration reads and retention PUTs without overwriting object data, allowing compatibility workloads to complete while retention enforcement remains out of scope.
- **Cluster forwarding correctness**: shifted follower-forwarded data-group proposals so the leader waits for apply errors and followers no longer wait on their own unrelated apply index.
- **Multipart capability gossip**: reports local capability evidence under dynamic address aliases even when node stats are not yet populated, preventing localhost node-ID/address mismatches from blocking multipart runs.
- **Large multipart completion metadata**: chunked large-object completion now commits multipart part metadata together with segment metadata, preserving `?partNumber=N` semantics.
- **Multipart read memory pressure**: non-versioned `GET ?partNumber=N` now computes the part range from `HeadObject` and streams only that byte range through `ReadAt`, instead of opening and reconstructing the whole object first.
- **SegmentWriter allocation pressure**: added a byte fast path for segment backends that can consume owned chunk bytes directly, avoiding a redundant `io.ReadAll` copy on the cluster write hot path.
- **Audit status classification**: 404 object-not-found audit envelope records are classified as request errors, not authorization denies.

### Verified

- `make test-unit`
- `make build`
- `go test ./internal/server ./internal/storage ./internal/cluster ./internal/serveruntime ./internal/s3auth ./internal/policy ./internal/iam/builtin ./benchmarks ./cmd/grainfs -count=1`
- Full S3 warp matrix on 4-node GrainFS cluster: `put`, `get`, `delete`, `mixed`, `list`, `stat`, `versioned`, `retention`, `multipart`, `multipart-put`, and `append` all completed with `errors=0` in `benchmarks/profiles/s3bench-all-readat-20260520-021246`.

## [0.0.267.0] - 2026-05-20 - feat(cluster): CreateMultipartUploadWithTags real support

Object Tagging API Phase 2 cluster gap 종결. `CreateMultipartUploadWithTags`가 cluster 모드에서 실제로 동작 (Phase 1의 `len(tags) > 0` fail-fast 제거).

### Changed

- **Cluster `CreateMultipartUploadWithTags` 본격 지원**: `clusterpb.MultipartMeta` + `CreateMultipartUploadCmd` FBS schema에 `tags:[Tag]` 추가. Initiate 시 `clusterMultipartMeta`에 Tags 저장, `CompleteMultipartUpload` 시 production Raft path (`CmdPutObjectMeta`)에 Tags propagation해서 finalised `objectMeta.Tags` 직접 materialise (single Raft entry — 별도 `CmdSetObjectTags` proposal 불필요).
- **Tag copy discipline 통일**: defensive copy는 cluster API boundary 한 곳 (`createMultipartUploadInternal`)에만 존재. apply / EC commit path는 alias 그대로 전달 (`Parts` 패턴과 일치). hot-path alloc 감소.
- **`CreateMultipartUpload[WithTags]` dedupe**: 두 public 메서드가 `createMultipartUploadInternal` 헬퍼로 통일되어 placement-group 부트스트랩/rollback 로직 ~30줄 중복 제거.
- **Cluster forward path Tags 전파**: `ForwardObjectMeta` / `ForwardObjectVersionMeta` FBS schema에 `tags:[Tag]` 추가. 모든 cross-node forwarded read (Get/Head/List/ListVersions)가 Tags 보존. `ClusterCoordinator.GetObjectTags`가 `ForwardOpGetObjectTags` op으로 multi-group routed read 지원 (이전엔 "peer forwarding not implemented" 에러). Regression guards: `TestForwardObjectMeta_CarriesTags`, `TestClusterCoordinator_GetObjectTags_Forwarded`.
- **`DistributedBackend` List paths Tags**: `ListObjects` / `ListObjectsPage` / `WalkObjects`가 `storage.Object.Tags`를 채움 (이전엔 `HeadObject` + `ListObjectVersions`만 propagate → single/cluster parity 위배). Regression guard: `TestDistributedBackend_ListObjects_PreservesTags`.
- **`wal.Backend` / `pullthrough.Backend` / `packblob.PackedBackend` `CreateMultipartUploadWithTags` pass-through**: production hot path wraps `storage.Backend` (interface) inside `wal.Backend`, `pullthrough.Backend`, 그리고 single-node packed mode에서는 `PackedBackend` (non-embedded `inner` field). 어느 wrapper도 underlying concrete type의 method를 promote하지 않아서 `Operations.CreateMultipartUploadWithTags`의 `(tagsCreator)` type assertion이 wrapper에서 실패 → silently no-tags overload로 fallback → `x-amz-tagging` on multipart-initiate가 drop되던 문제 해결. Regression guards: `TestWALBackend_CreateMultipartUploadWithTags_DelegatesToInner`, `TestPullthroughBackend_CreateMultipartUploadWithTags_DelegatesToInner`, `TestPackedBackend_CreateMultipartUploadWithTags_DelegatesToInner`.
- **`ClusterCoordinator.CreateMultipartUploadWithTags`**: cluster mode 진입점. local data group은 `GroupBackend.CreateMultipartUploadWithTags`로 직접 dispatch, remote는 `ForwardOpCreateMultipartUpload`로 routing. forward schema `CreateMultipartUploadArgs`에 `tags:[Tag]` 필드 추가 (FBS regenerated), receiver는 `TagsLength() > 0`에 따라 `CreateMultipartUploadWithTags` / `CreateMultipartUpload` 분기 (older sender wire-compat). Regression guard: `TestClusterCoordinator_CreateMultipartUploadWithTags_PreservesTags`.

### Fixed

- **`upgradeObjectEC` Tags propagation**: EC config upgrade 시 `CmdPutObjectMeta` propose에 기존 `objectMeta.Tags`를 forward. `applyPutObjectMeta`가 `c.Tags`를 unconditional하게 write하므로, 이 fix 없이는 reshard 경로가 사용자 tag를 nil로 clobber. `headObjectMeta`가 `storage.Object.Tags`를 채우도록 보강하여 callers (현재는 `upgradeObjectEC`)가 tag를 propose에 실어보낼 수 있게 함. Regression guard: `TestUpgradeObjectEC_PreservesTags` (`internal/cluster/reshard_manager_test.go`).
- **Chunked PUT Tags propagation**: large-object PUT (≥ chunked threshold) via
  `putObjectChunked` was dropping the `tags` argument before reaching
  `PutObjectMetaCmd`. Threaded through, with regression test
  `TestChunkedPut_PreservesTags`.
- **Snapshot restore Tags**: `RestoreObjects` propose path was building
  `PutObjectMetaCmd` without `Tags: snap.Tags`. Fixed; regression test
  `TestRestoreObjects_PreservesTags`.

### Verified (no code change)

- Cluster versioned-record tags (`SetObjectTags`/`GetObjectTags` with `versionID != ""`) — 이미 v0.0.264.0에 구현되어 있음 (`apply.go:691-721` versionID-branch, `backend.go:1377-1379` versioned-key GET). Unit 테스트 통과: `TestFSM_SetObjectTags`, `TestFSM_SetObjectTags_NotFound`, `TestFSM_SetObjectTags_VersionedBucket`, `TestFSM_SetObjectTags_SpecificVersion`.

### Known limitations

- **E2E harness IAM bootstrap probe regression** (v0.0.263.0 이후 cluster e2e 전체가 `IAM bootstrap not ready within 30s`로 실패). Phase 2와 무관, 본 릴리스에서 별도 fix 필요.
- **E2E 검증 갭**: 위 harness regression으로 인해 `MultipartCreate_TagsMaterialiseOnComplete` cluster assertion (Phase 2 Task Step 12에서 fail-fast bypass 제거)이 **code-only-verified** 상태 — bypass 제거 + cluster apply unit tests (`TestFSM_CreateMultipartUpload_PersistsTags`, `TestFSM_CompleteMultipartUpload_MaterialisesTags`) PASS는 확인했으나 실제 S3 클라이언트 round-trip은 harness fix 전까지 runtime-verify 불가.

## [0.0.266.0] - 2026-05-19 - feat(cluster): segment-backed large object chunking phase 2

Cluster large-object chunking phase 2. Chunked PUT/GET now routes object metadata through the cluster raft path while segment payloads are stored and read through the segment store. The branch also hardens the single-node segment-backed storage compatibility paths that were exposed after rebasing onto `origin/master`.

### Added

- Cluster segment metadata FBS wiring for object metadata replication, including segment references and placement entries.
- Segment store and cluster segment backend coverage for chunked object read/write paths.
- E2E coverage for cluster chunked PUT roundtrip and fan-out breadth behavior.

### Changed

- Large-object GET in cluster mode now reads through the segment store instead of assuming a flat local object file.
- Appendable/coalesced object handling avoids publishing metadata before the coalesced temp file is durable and ready.
- Local singleton cluster reads now resolve to the local backend even when the only voter has a non-leader raft probe.
- VFS rename uses backend copy support for segment-backed objects, avoiding the high-memory `PutObject` streaming path.
- LocalBackend partial I/O compatibility now handles segment-backed objects for `WriteAt`, `Truncate`, `Sync`, and `OpenLocalReplica`.
- Scrubber verification and tests now use segment-aware local replica opening and stored object ETags.
- Append-object tests create setup buckets through the backend, matching the admin-UDS-only bucket lifecycle guard from `origin/master`.

### Tests

- `make test-unit`
- `go test ./internal/cluster ./internal/storage ./internal/nfs4server ./internal/p9server ./internal/scrubber ./internal/server ./internal/vfs -count=1`
- Focused regression coverage for cluster singleton reads, NFS truncate/commit, 9P overwrite/extend, VFS large-file rename memory bounds, scrubber missing/corrupt detection, and append-object streaming trailer handling.

## [0.0.265.0] - 2026-05-19 - cleanup(auth): §1-§3 잔재 fix — DEK boot wiring, SA→_grainfs deny, IPST snapshot trailer

§1-§3 deferred 잔재 정리 cleanup 슬라이스. v0.0.263.0 (§2 IAM Core + §3 Bucket Lifecycle) 머지 후 review-forever Pass에서 발견된 boot-wiring 갭 2건과 snapshot 누락 1건을 정리. 새 기능 추가 없음 — 기존 §1-§3 구현의 wiring/coverage 완성.

### Added

- `cluster.IPST` snapshot trailer (magic `0x54535049`): PolicyStore + GroupStore + PolicyAttachStore + BucketPolicyStore 4개를 단일 FlatBuffers payload로 묶어 `meta_fsm.Snapshot`/`Restore` 체인의 outermost trailer로 추가. cluster 재시작 시 Raft log 전체 replay 의존을 제거하고, snapshot install로 정책 상태를 빠르게 복원. peel chain: IPST → DKVS → GCFG → IAMG.
- 4개 store에 `Snapshot()` / `ReplaceAll()` API 추가 (`policystore`, `group`, `policyattach`, `bucketpolicy`). `policyattach`는 SA-attach + group-attach가 하나의 단위로 직렬화돼야 하므로 `AttachSnapshot` 구조체 wrap.
- `cluster.ApplyCmdForTest` + `cluster.EncodeMetaCmdForTest`: 외부 패키지에서 FSM apply 경로 단위 검증을 위해 노출. 프로덕션 코드 호출 금지.
- `serveruntime.wireDEKKeeper(state, fsm)` 추출: bootMetaRaftWiring의 DEK wiring을 unit-testable 함수로 분리.

### Changed

- `serveruntime/boot_phases_raft.go`: §1 잔재 갭 fix (C2). `nodeconfig.KEKSource()` → `encrypt.LoadOrGenerateKEK` → `encrypt.NewDEKKeeper` → `MetaFSM.SetDEKKeeper` → `WireDEKPostCommit` 호출이 production boot에 연결됨. 이전엔 `MetaFSM.dekKeeper`가 nil로 남아 `DEKRotate` / `DEKVersionPrune` MetaCmd가 silent no-op이었음.
- `s3auth.Authorizer.Authorize`: 내부 버킷 (`_grainfs/*`) deny가 익명에 더해 인증된 SA에도 적용 (C3). 이전엔 `readonly` builtin policy를 attach한 SA가 `_grainfs/audit.evaluations`를 읽을 수 있었음. audit-internal SA의 localhost 경로는 `authenticateAuditInternalRequest` early-return으로 Authorize 우회하므로 영향 없음.
- `meta_fsm.go` Restore IPST 경로: partial-nil store 시 per-store WARN 로그 추가. 이전엔 all-nil만 warn했고 일부 nil은 silent skip해 다른 store와 desync 위험.

### Tests

- `internal/cluster/meta_fsm_iam_policy_stores_snapshot_test.go`: IPST snapshot RoundTrip + LegacySnapshot_NoIPST + NilStores_WarnOnly + EmptyStores + WithAllTrailers (5건). WithAllTrailers는 IAMG/GCFG/DKVS/IPST 4개 trailer 공존 시 peel chain 검증.
- `internal/serveruntime/dek_keeper_wiring_test.go`: LoadOrGenerateKEK 멱등성 + WireDEKKeeper_InjectsAndRegistersHook (DEKRotate apply가 keeper generation을 0→1로 증가).
- `internal/s3auth/authorizer_test.go`: SA가 readonly policy로 `_grainfs/*` 접근 시 Deny 검증.
- `internal/server/authz_test.go`: IAM-enabled mode에서 인증 SA의 `_grainfs/*` 접근이 403 검증.

### Documentation

- `meta_fsm.go` IPST trailer 상수 doc: GCFG/DKVS 패턴과 일관되게 Wire layout ASCII 다이어그램 추가.

## [0.0.264.0] - 2026-05-19 - feat(s3): Object Tagging API

MinIO-parity S3 Object Tagging API 구현. PUT/GET/DELETE `?tagging` 엔드포인트 + `x-amz-tagging` 헤더 (PutObject / POST / CreateMultipartUpload / CopyObject) + `x-amz-tagging-directive` (COPY/REPLACE). Tags는 FBS `Object` table inline 저장; 클러스터 모드에서 `CmdSetObjectTags` Raft cmd로 versionID-aware 복제.

### Added

- **HTTP endpoints**: `PutObjectTagging` / `GetObjectTagging` / `DeleteObjectTagging` (`?tagging` 쿼리, `?versionId` 선택). DELETE는 idempotent (204).
- **헤더 통합**: `x-amz-tagging` (URL-encoded k=v&k=v) on PutObject / CreateMultipartUpload / CopyObject; POST Object는 `tagging` form 필드 (XML).
- **CopyObject directive**: `x-amz-tagging-directive: COPY` (기본, source tags 상속) / `REPLACE` (request tags).
- **Tag 저장**: FBS `Object.tags:[Tag]` inline; snapshot/restore round-trip 보존; `ListObjectVersions`에 Tags projection.
- **AWS-strict 검증**: ≤10 tags, key 1..128, value 0..256, Unicode letter/digit/space + `_ . : / = + - @`, `aws:` 접두사 거부; 단일 `internal/storage/tagging.Validate`가 XML body + header 양쪽 권위 소스.
- **Cluster mode**: `CmdSetObjectTags` Raft cmd (versionID-aware: `versionID=""`는 legacy+latest 듀얼 라이트, 명시 versionID는 해당 record만) + `ForwardOpSetObjectTags=21` dispatch/receiver; `clusterpb.ObjectMeta.tags` 추가로 cluster `objectMeta` 라운드트립.
- **Multipart Tags**: `CreateMultipartUploadWithTags` — Initiate 시 upload entry에 보존, Complete 시 객체에 materialize.
- **Metrics**: `grainfs_object_tagging_requests_total{op,result}`, `grainfs_object_tagging_validation_errors_total{reason}`, `grainfs_object_tags_per_object` histogram.

### Notes

- ETag, LastModified, blob bytes는 tag mutation으로 변경되지 않음 (AWS S3 시맨틱). 라이프사이클 tag-기반 필터링이 객체 age clock을 리셋하지 않고, ETag 기반 HTTP 캐시 무효화도 발생하지 않음.
- **PutObject + `x-amz-tagging` 헤더는 non-atomic 2-step** (object put → SetObjectTags). AWS S3 자체도 내부적으로 동일 시맨틱. 클라이언트에 200 응답이 돌아갈 때까지는 둘 다 적용 완료. SetObjectTags 실패 시 object는 commit되어 있고 tags만 미적용된 partial state로 노출됨 (5xx 응답으로 신호). 향후 PutObjectMetaCmd FBS에 tags 통합 시 단일 Raft entry로 원자화 가능.
- **LocalBackend (single-node) versionID 미지원**: SetObjectTags/GetObjectTags에 `versionID != ""` 전달 시 `UnsupportedOperationError` 반환 (501). 단일 노드는 per-version metadata store 없음. Versioned bucket의 versionID 명시 tagging은 cluster 모드 (`DistributedBackend`/`ClusterCoordinator`)에서만 동작.
- Cluster 모드 `CreateMultipartUploadWithTags`는 Phase 1에서 fail-fast (`UnsupportedOperationError`) — `clusterMultipartMeta` widening은 후속 작업으로 미룸. Single-node + cluster-mode `PutObject` x-amz-tagging은 정상 동작.
- POST form upload의 `tagging` 필드는 AWS 스펙대로 XML payload (URL-encoded 아님).
- 초기 design doc은 "ACL과 동일 패턴, no FSM cmd"라고 적혔으나 정정: ACL도 `CmdSetObjectACL` Raft cmd 사용. Tags는 ACL과 동일한 cmd-dispatch infrastructure이되 별도 schema/cmd (versionID-aware vs ACL의 versionID-unaware).

## [0.0.263.0] - 2026-05-19 - feat(auth): §2 IAM Core + §3 Bucket Lifecycle — zero-config progressive application

§1 Foundation (v0.0.260.0)에 이어 §2 IAM Core + §3 Bucket Lifecycle 슬라이스가 결합되어 들어왔습니다. legacy Role/Grant model 완전 제거, 새 AWS-style JSON policy 엔진, 4개 in-memory store + StoreAdapter + Resolver, 4개 built-in managed policy (readonly/readwrite/writeonly/bucket-admin), bucket-lifecycle data-plane 거부, reserved-name 보호, default bucket implicit-anon, Phase 0→2 자동 전환, _grainfs reserved bucket bootstrap seed. `s3auth.Authorizer`가 production 부트 경로에 wire되어 Layer 1 iamCheck가 `policy.Evaluate`를 실제 호출합니다.

### Added

- `internal/iam/policy`: AWS-style policy document parser + evaluator. `explicit Deny > explicit Allow > implicit Deny`. Action namespaces 제한 (`s3:*`, `iceberg:*`); condition keys 제한 (`aws:SourceIp`, `s3:prefix`); `NotAction`/`NotResource`/`NotPrincipal` parse-time 거부.
- `internal/iam/policy/Resolver`: SA → effective-policy (SA-attached + group-attached + bucket-policy union) resolver with TTL cache (default 5s) + `Invalidate(saIDs, buckets)` 계약. 모든 MetaCmd apply가 영향 받은 캐시 항목을 동기적으로 무효화.
- `internal/iam/policystore`, `internal/iam/group`, `internal/iam/policyattach`, `internal/iam/bucketpolicy`: 4개 in-memory store. PolicyStore는 built-in 보호 (`ErrBuiltinPolicy`).
- `internal/iam/policy.StoreAdapter`: 4개 store를 `policy.Store` 인터페이스로 묶는 단일 어댑터.
- `internal/iam/builtin`: 4개 built-in managed policy 시드. `bucket-admin`은 admin-UDS-only 액션 4개 (`s3:CreateBucket`, `s3:DeleteBucket`, `s3:PutBucketPolicy`, `s3:DeleteBucketPolicy`) 의도적 제외 (D#8).
- `internal/reservedname`: leaf 패키지. `IsInternalBucket` (`_grainfs` 접두사), `IsReservedDefaultName` (정확히 `default`), `IsReservedBucketName` (둘의 OR).
- `internal/s3auth.Authorizer`: 단일 진입점. 우선순위: admin-UDS-only deny → anon + internal bucket deny → default bucket implicit-anon → `iam.anon-enabled` short-circuit → 전체 `policy.Evaluate`.
- `MetaCmd` enum 50-62: PolicyPut/PolicyDelete, GroupPut/Delete/MemberPut/MemberDelete, PolicyAttachToSAPut/Delete, PolicyAttachToGroupPut/Delete, BucketPolicyPut/Delete, CreateBucketWithPolicyAttach.
- `internal/serveruntime.WireIAMPolicyStores`: 부트 시 store 인스턴스화 + FSM 주입 + built-in seed. `WithPolicyAuthorizer` option으로 server에 wired.
- `CreateBucketWithPolicyAttach` (atomic MetaCmd 62): SA 존재 검증 후 정책 attach. admin handler가 data-plane CreateBucket 실패 시 IAM 부분 롤백 (sequenced atomicity, F#2).
- `internal/cluster/clusterpb/CreateBucketCmd.bypass_reserved`: bootstrap이 reserved name(`default`, `_grainfs`) 시드를 위해 사용. 공개 API에서는 항상 false.
- `cluster.ApplyCmdForTest` + `EncodeMetaCmdForTest`: 외부 패키지가 FSM apply 경로를 단위 테스트로 검증할 수 있도록 노출. 프로덕션 코드 호출 금지.

### Changed

- `internal/server` S3 데이터 플레인: `CreateBucket`/`DeleteBucket`/`PutBucketPolicy`/`DeleteBucketPolicy` 4개 엔드포인트가 무조건 403 AccessDenied 반환 (D#8). admin UDS 경로는 유지. 약 18개 E2E 테스트가 PUT `/<bucket>` 셋업 대신 `backend.CreateBucket` 직접 호출로 마이그레이션.
- `internal/server.IAMChecker` 시그니처: `(saID, bucket string, action S3Action) bool` → `(saID, bucket, key string, action S3Action) bool`. object-scope Deny (`Resource: arn:aws:s3:::bucket/path/*`) 가 L1에서 매칭되도록 object key를 전달. 모든 RequestAuthorizer 테스트 픽스처 일괄 업데이트.
- `internal/cluster/meta_fsm`: `applyIAMSACreate`에서 첫 SA 생성 시 (`wasEmpty && !IsEmpty()`) `iam.anon-enabled=false`로 원자적 flip + resolver invalidate (D#3, F#16).
- `internal/cluster/apply.go`: `applyCreateBucket`/`applyDeleteBucket`이 `reservedname.IsReservedBucketName` 거부. `applyBucketPolicyPut`/`applyBucketPolicyDelete`는 `IsInternalBucket`만 거부 (`default`는 explicit policy 허용).
- `internal/server.icebergS3CredOverrides`: cred 포워딩이 `iceberg:GetCatalogConfig` policy gate를 통과해야 SA secret_key 노출. policyAuthorizer wired에서는 fail-closed.
- `internal/server.WithPolicyAuthorizer`: option으로 `s3auth.Authorizer` 주입. buildAuthorizer 래퍼가 wired면 `policy.Evaluate` 호출, nil이면 deny-by-default (legacy/test 픽스처).
- `internal/iam`: legacy Role/Grant 완전 제거. SA + AccessKey 코드 유지. `internal/iam/iampb`의 Role enum + GrantPut* table은 backcompat용 reserved (pre-§2 snapshot용).
- `internal/cluster/clusterpb/cluster.fbs`: enum 25-31 (IAMGrant*/IAMInitFirstSA) reserved 유지, apply switch에서 제거되어 default-case (log warn + metric) fall-through. 새 노드가 pre-§2 snapshot replay 시 silent skip.
- `internal/iam/policy.principalMatches`: Named-form `Principal:{"AWS":["*"]}` wildcard도 `AllowAnonBucket` gate 적용 (이전: Star branch만 gate; Named branch는 bypass). 보안 회귀 수정.

### Removed

- `internal/iam` legacy: `Role`, `RoleAllows`, `Grant`, `WildcardBucket`, `SystemBucket`, `DefaultSAID`, `ProposeInitFirstSA`, `ProposeGrant*`, `internal/iam/init_first_sa.go`, `internal/iam/role_matrix_test.go`.
- `internal/server/admin`: `PutGrant`/`DeleteGrant`/`ListGrants` 핸들러 및 어댑터.
- `internal/server`: `issueCreatorGrant` (T27 `CreateBucketWithPolicyAttach`로 대체), `LookupGrant` 기반 cred 게이트 (T33 policy gate로 대체), `bucket_mutation_runtime.go` 데드 코드.

### Tests

- `internal/iam/policy`: parse/match/evaluate/resolver 매트릭스 18+ 케이스. 신규: Named-form `Principal:{"AWS":["*"]}` AllowAnonBucket gate 회귀 테스트 2건.
- `internal/iam/builtin`: 4개 built-in × 4개 admin-UDS-only 액션 table-driven (D#8 회귀 보호). testify `require`/`assert` 일관화.
- `internal/serveruntime`: `WireIAMPolicyStores`가 5개 store 모두 FSM에 주입했는지 PolicyPut/GroupPut/PolicyAttachToSAPut/BucketPolicyPut MetaCmd로 검증.
- `internal/cluster`: reserved-name guard (4 apply path × 4 케이스), `CreateBucketWithPolicyAttach` atomic apply, anon-flip atomicity (3 케이스), bypass=true 시 reserved 시드 성공.
- `internal/server`: bucket-lifecycle 데이터-플레인 거부 (4 엔드포인트 × 403). `TestAuthz_InternalAuditBucket_*` 3건 유지.

### Documentation

- `CLAUDE.md`: internal 패키지 리스트에 `iam/policy`, `iam/policystore`, `iam/group`, `iam/policyattach`, `iam/bucketpolicy`, `iam/builtin`, `reservedname` 추가.

### Deferred

- `meta_fsm` snapshot/restore에 policystore/groupstore/policyattach/bucketpolicy 포함 — 현재는 Raft 로그 재플레이 의존. TODOS에 follow-up 등록.
- `SetDEKKeeper` + `WireDEKPostCommit` 프로덕션 부트 연결 (§1 잔여 갭) — TODOS.
- `meta_fsm.go` 3509줄 모놀리딕 → 영역별 파일 분리 — TODOS.
- IAM-enabled 모드에서 SA가 `_grainfs/*`에 접근 시도 시 거부 검증 e2e 테스트 — TODOS.

## [0.0.262.19] - 2026-05-19 - test(e2e): further-group 17 entries into single handles

48개 별 entry를 17개 단일 entry로 추가 통합 (ClusterTransferLeader, ClusterEC, IAMBootstrap, ClusterBootstrapJoin, ClusterJoinServices, NormalizeOptions, WaitForWritableEndpoint, IAMBootstrapHelpers, ClusterGrantAdminHelpers, ClusterPSK, NoPeers, IcebergAuth, IcebergDuckDB, AuditIceberg, AppendObjects, Multiparts, ClusterAdminCLI). 각 함수는 `run*` helper로 rename + 새 entry에서 `t.Run` 디스패치. production code 변경 없음.

## [0.0.262.18] - 2026-05-19 - test(e2e): unify all entries under dual sub-test pattern

`tests/e2e/`의 200+ test entry를 canonical `TestXxxE2E + SingleNode/Cluster{N}Node` sub-test 모양으로 통일. 관련 그룹들은 단일 entry로 합치고, single-only / cluster-only entry에는 fixture-가능한 mirror를 추가해 인벤토리 일관성 확보. production code 변경 없음 (test infrastructure only).

## [0.0.262.17] - 2026-05-19 - test(e2e): merge volume_cli_test.go entries into single TestVolumeCLIGuardsE2E

Two negative-path entries from v0.0.262.16 (`TestVolumeCLIAutoDiscoveryE2E` + `TestVolumeDataPlaneGuardE2E`) collapsed into one entry. Both cover the same conceptual area — guards on the volume CLI / data plane surface — so a single entry with two sub-tests is the right shape.

### Shape

```
TestVolumeCLIGuardsE2E
  ├─ t.Run("SingleNode")  ─┐
  └─ t.Run("Cluster4Node") ┴─ runVolumeCLIGuardsCases(t, tgt s3Target)
                                ├─ t.Run("CLIHintWhenNoEndpoint")
                                └─ t.Run("DataPlaneVolumesPathHidden")
```

`CLIHintWhenNoEndpoint` is fixture-independent by design (asserts binary behavior, not server state); it runs under both branches for grep/inventory consistency. `DataPlaneVolumesPathHidden` reads `tgt.endpoint(0)` directly off the shared fixture.

Verified: `make build` clean; e2e package compiles (`go test -c`).

## [0.0.262.16] - 2026-05-19 - test(e2e): wrap remaining standalone E2Es in SingleNode/Cluster4Node sub-tests

`TestVolumeCLIAutoDiscoveryE2E` and `TestVolumeDataPlaneGuardE2E` landed in v0.0.262.14 as standalone E2Es. Even though one is fixture-independent (CLI hint check before any server connection) and the other only needs an HTTP endpoint, **every e2e entry point in the suite must follow the dual SingleNode/Cluster4Node shape** for grep/inventory consistency. This PR brings the two stragglers into the pattern.

### Shape

```
TestVolumeCLIAutoDiscoveryE2E
  ├─ t.Run("SingleNode")  ─┐
  └─ t.Run("Cluster4Node") ┴─ runVolumeCLIAutoDiscoveryCases(t)
                                └─ t.Run("HintWhenNoEndpoint")

TestVolumeDataPlaneGuardE2E
  ├─ t.Run("SingleNode")  ─┐
  └─ t.Run("Cluster4Node") ┴─ runVolumeDataPlaneGuardCases(t, tgt s3Target)
                                └─ t.Run("VolumesPathDoesNotExposeAdminShape")
```

### Changed

- `TestVolumeCLIAutoDiscoveryE2E`: both branches reference the corresponding shared fixture (`newSingleNodeS3Target()` / `newSharedClusterS3Target(t)`) to keep the boot ordering consistent with the rest of the suite, then run the same CLI hint check in `HintWhenNoEndpoint`. The check is identical on both branches by design — it asserts behavior of the binary itself, not of any fixture.
- `TestVolumeDataPlaneGuardE2E`: uses `tgt.endpoint(0)` instead of a per-test `startTestServer`; runs against shared single + shared cluster fixtures.

Verified: `make build` clean; e2e package compiles (`go test -c`).

## [0.0.262.15] - 2026-05-19 - test(e2e): dual-integrate Dashboard set

Three Dashboard entry points (scattered across three files) collapsed into one entry, `TestDashboardE2E`, with the canonical dual fixture pattern.

### Shape

```
TestDashboardE2E
  ├─ t.Run("SingleNode")  ─┐
  └─ t.Run("Cluster4Node") ┴─ runDashboardCases(t, mk dashboardFactory)
                                ├─ t.Run("Serves")                (GET /ui/ → HTML)
                                ├─ t.Run("HealingCardHTMLMarkup") (Phase 16 Self-Healing card markup)
                                ├─ t.Run("HealingCardSSEStream")  (GET /api/events/heal/stream → text/event-stream)
                                └─ t.Run("TokenURLAndRotate")     (dashboard CLI token + rotate)
```

### Changed

- **`TestDashboard_Serves` (`presigned_test.go`) + `TestDashboardHealingCard_HTMLAndStream` (`dashboard_healing_card_test.go`) + `TestE2E_Dashboard_TokenURLAndRotate` (`volume_cli_test.go`) → single `TestDashboardE2E`** (`tests/e2e/dashboard_test.go`, new).
- `dashboardFactory` mirrors `volumeScrubFactory` — each case gets a dedicated fixture so `TokenURLAndRotate`'s rotate cannot invalidate another case's expectations.
- `TokenURLAndRotate` simplified: dropped the `--public-url` plumbing. URL assertion is `Contains(t, resp1.URL, "#token="+resp1.Token)` — token suffix only — which holds regardless of the URL prefix.
- `dashboardDataDir(tgt)` and `dashboardPort(tgt, nodeIdx)` helpers extract the admin dataDir and HTTP port from any target.
- `callUI(t, port, token)` moved into `dashboard_test.go`.
- Deleted `tests/e2e/dashboard_healing_card_test.go`.

### Known parity risks (cluster branch)

`Cluster4Node` is the first end-to-end coverage of these endpoints on a 4-node DynamicJoin fixture. The dashboard token is per-node state in some prior implementations; if it isn't replicated/leader-canonical, `TokenURLAndRotate` cluster branch may flap (rotated token on leader vs. callUI hitting the same node). Captured as signal — not fixed here per the e2e-unify session policy.

Verified: `make build` clean; e2e package compiles (`go test -c`).

## [0.0.262.14] - 2026-05-19 - test(e2e): absorb TestE2E_VolumeCLI_* into TestVolumeE2E (single admin CLI entry)

`TestE2E_VolumeCLI_*` and `TestVolumeE2E` (landed in v0.0.262.12) covered the same admin-CLI volume surface from two entry points. This PR collapses the admin-CLI case set into one entry — `TestVolumeE2E` — and pulls out the two genuinely-not-admin-CLI tests as standalone E2Es.

### Absorbed into `TestVolumeE2E` (now 9 sub-tests)

| Was | Now (sub-test under `TestVolumeE2E`) |
|---|---|
| `TestE2E_VolumeCLI_FullLifecycle` | `FullLifecycle` (list/create/info/resize/snapshot/delete-refused/delete-force) |
| `TestE2E_VolumeCLI_ListIncludesHealth` | `ListIncludesHealth` |
| `TestE2E_VolumeCLI_ListJSONIncludesHealthReasons` | `ListJSONIncludesHealthReasons` |
| `TestE2E_VolumeCLI_ShrinkRejected` | `ShrinkRejected` |
| `TestE2E_VolumeCLI_NotFound` | `NotFound` |

All five cases now run under `SingleNode` and `Cluster4Node` via the existing `runVolumeCases(t, tgt s3Target)` set helper — six fixture-paths per case from one entry. Per-case unique volume names via `uniqueVolName(tgt, …)` so cluster reruns and parallel cluster tests can't collide on the volume namespace.

### Split out (not admin CLI)

- **`TestE2E_VolumeCLI_AutoDiscoveryFailureMessage` → `TestVolumeCLIAutoDiscoveryE2E`**. Fixture-independent: invokes the binary in a cwd with no grainfs context and asserts the actionable hint is printed before any server connection. No single/cluster split.
- **`TestE2E_VolumeCLI_NoVolumesViaDataPlane` → `TestVolumeDataPlaneGuardE2E`**. HTTP-level guard against the removed `/volumes/*` admin endpoints on the data plane (A6 regression). Not a CLI invocation.

### Files

- `tests/e2e/volume_test.go` — five sub-tests appended to `runVolumeCases`. Helpers (`createVolumeEventually`, `cleanupVolume`, `uniqueVolName`) reused.
- `tests/e2e/volume_cli_test.go` — five absorbed functions removed; the two non-admin-CLI tests renamed to the canonical `TestXxxE2E` form. `startTestServer`, `runCLI`, `waitForVolumeReady`, `containsFlag`, `TestE2E_Dashboard_TokenURLAndRotate` (separate group, queued for a later PR) preserved.

Verified: `make build` clean; e2e package compiles (`go test -c`).

## [0.0.262.13] - 2026-05-19 - test(e2e): dual-integrate VolumeScrub set + collapse _Cluster4Node suffix entries

Three test groups re-shaped into the canonical single-entry dual pattern.

### Shape

```
TestVolumeScrubE2E
  ├─ t.Run("SingleNode")  ─┐
  └─ t.Run("Cluster4Node") ┴─ runVolumeScrubCases(t, mk volumeScrubFactory)
                                ├─ t.Run("HealthyNoop")                  (dedup=false)
                                ├─ t.Run("HealthyNoop_Dedup")            (dedup=true)
                                ├─ t.Run("DryRunDetectsCorruption")      (truncate + --dry-run)
                                ├─ t.Run("DryRunDetectsCorruption_Dedup")
                                ├─ t.Run("RepairBehavior")               (single→Unrepairable=1 / cluster→Repaired=1)
                                ├─ t.Run("RepairBehavior_Dedup")
                                ├─ t.Run("AdminTriggerWorksAtZeroInterval")  (--scrub-interval=0)
                                └─ t.Run("StatusListCancel")             (--detach + list + status)
```

### Changed

- **`TestE2E_VolumeScrub_*` (8 entries) + `TestE2E_VolumeScrub_MultiNodeRepair` → single `TestVolumeScrubE2E`** (`tests/e2e/volume_scrub_test.go`). `MultiNodeRepair` is absorbed by `RepairBehavior`'s cluster branch — same truncate-then-scrub flow, fixture-divergent expectation (single: `Unrepairable=1`, cluster: `Repaired=1`).
- New `volumeScrubFactory` type — each scrub case needs its own `--dedup`/`--scrub-interval` flags, so the case set is parametrised on a fixture factory rather than a single `s3Target`. Single branch wraps `newDedicatedSingleNodeS3Target`; cluster branch wraps `newClusterS3TargetWithExtraArgs(t, 4, args)`.
- New `scrubDataDir(tgt, nodeIdx)` and `truncateAVolumeBlock(t, tgt, vol, blockNum)` helpers — encapsulate single-vs-cluster dataDir selection and on-disk shard truncation (picks first holder for cluster).
- `filepathWalkBlock` helper moved from the deleted `volume_scrub_multinode_test.go` into `volume_scrub_test.go` (still used by `nbd_multinode_replication_test.go`).
- Deleted `tests/e2e/volume_scrub_multinode_test.go`.

### Also (`_Cluster4Node` suffix cleanup)

- **`TestAppendForwardBufferSaturationE2E_Cluster4Node` → `TestAppendForwardBufferSaturationE2E`** with a single `t.Run("Cluster4Node", …)` branch that calls `runAppendForwardBufferSaturationCases(t, tgt s3Target)`. Cluster-only today (single-node has no forward buffer); shape kept consistent so a future single-node analogue (e.g. per-bucket admission control) can drop in as a sibling `t.Run("SingleNode", …)`.
- **`TestOrphanSegmentSweepE2E_Cluster4Node` → `TestOrphanSegmentSweepE2E`** with one `t.Run("Cluster4Node", …)` calling `runOrphanSegmentSweepCases(t)`. Cluster-only today (single-node scrubber is covered separately); same forward-compatibility rationale.

Verified: `make build` clean. e2e package compiles (`go test -c`).

### Known parity risks (cluster branch, first run)

- `DryRunDetectsCorruption{,_Dedup}` cluster branch corrupts an EC shard rather than a `current` file — never previously exercised through the dry-run CLI path.
- `RepairBehavior{,_Dedup}` cluster branch expects `Repaired=1` via EC peer-pull on a 4-node DynamicJoin fixture; `MultiNodeRepair` previously asserted this on 3-node StaticPeers. Fixture difference may flap initial-placement races on the first write.
- `HealthyNoop_Dedup` cluster branch is the first cluster coverage of dedup-mode volume scrub. If dedup-on-cluster has wiring gaps, the assert fails — captured as signal, not fixed here (classification-only scope per ongoing e2e-unify session policy).

## [0.0.262.12] - 2026-05-19 - test(e2e): dual-integrate TestVolume admin CLI set

Same one-entry-point shape as v0.0.262.11 (BucketPolicy). Single `TestVolumeE2E` owns the volume admin CLI test set and applies it to both fixtures.

### Shape

```
TestVolumeE2E
  ├─ t.Run("SingleNode")  ─┐
  └─ t.Run("Cluster4Node") ┴─ runVolumeCases(t, tgt s3Target)
                                ├─ t.Run("CreateAndGet")
                                ├─ t.Run("List")
                                ├─ t.Run("Delete")
                                └─ t.Run("CreateWithRawByteSize")
```

### Changed

- **`TestVolume_{CreateAndGet,List,Delete,CreateWithRawByteSize}` → single `TestVolumeE2E`** (`tests/e2e/volume_test.go`).
- `dataDir := filepath.Dir(tgt.adminSockPath())` derives the admin-UDS directory from the target (single → `testServerDataDir`; cluster → leader dataDir).
- Helpers (`createVolumeEventually`, `getVolume`, `listVolumes`, `deleteVolume`, `deleteVolumeEventually`, `cleanupVolume`, `requireVolumeMissingEventually`, `requireVolumePresentEventually`) extended with explicit `dataDir` argument so they no longer pin to `testServerDataDir`.
- New `uniqueVolName(tgt, caseLabel)` helper produces per-target/per-case names with a nanosecond suffix so cluster reruns and parallel cluster tests can't collide.

### Known parity gap (pre-existing)

`Delete` sub-test fails on both `SingleNode` and `Cluster4Node`: `deleteVolume` reports `deleted=true` and exit 0, but `volume info` still returns the volume for 30s afterwards. Same shape as the `TestEcDeleteAndOverwriteE2E` versioning regression captured in v0.0.262.2 and the `TestSmokeDeploymentE2E/SingleNode/ListObjects` regression captured in v0.0.262.1. Not fixed here per the classification-only scope — captured for a follow-up session. The Delete sub-test stays in the suite as a regression signal.

## [0.0.262.11] - 2026-05-19 - test(e2e): collapse BucketPolicy into single TestBucketPolicyE2E + 3 sub-tests

Follow-up to v0.0.262.10. That PR landed three separate `TestBucketPolicy*E2E` entry functions, each with its own `SingleNode/Cluster4Node` split — three trees, three single boots, three cluster boots. The correct shape is **one entry point that owns the test set and applies it to both fixtures**, the TestBucketsE2E pattern: a single `TestBucketPolicyE2E` with `t.Run("SingleNode") + t.Run("Cluster4Node")` calling one `runBucketPolicyCases(t, tgt s3Target)` set helper, which in turn runs three sub-tests (`SetAndGet`, `InvalidJSON`, `DenyAction`).

### Changed

- **`TestBucketPolicy{SetAndGet,InvalidJSON,DenyAction}E2E` → single `TestBucketPolicyE2E`** (`tests/e2e/policy_test.go`).
- New shape: `TestBucketPolicyE2E` -> `t.Run("SingleNode") | t.Run("Cluster4Node")` -> `runBucketPolicyCases(t, tgt)` -> `t.Run("SetAndGet") | t.Run("InvalidJSON") | t.Run("DenyAction")`. Six fixture-paths run from one entry point.
- `signedPolicyRequest(t, tgt, ...)` signature unchanged from v0.0.262.10.

Verified: `make build` clean; full tree `TestBucketPolicyE2E` runs all six paths (6.83s incl. shared cluster boot for the first cluster sub-test).

## [0.0.262.10] - 2026-05-19 - test(e2e): dual-integrate BucketPolicy onto TestBucketsE2E pattern

First PR-D batch shifts from rename-only to **dual integration** — the actual goal is to prove single-node and 4-node cluster paths run the same test set and the policy plane is at parity. PR-A/B/C cluster-only renames stay; this PR (and follow-ups) reshape single-or-mixed groups into the proper dual pattern.

### Changed

- **`TestE2E_BucketPolicy_SetAndGet` → `TestBucketPolicySetAndGetE2E`** (`tests/e2e/policy_test.go`) — body extracted into `runBucketPolicySetAndGetCases(t, tgt s3Target)`. `t.Run("SingleNode") + t.Run("Cluster4Node")` runs the same PUT/GET/DELETE BucketPolicy sequence on both fixtures. Hard-coded `policy-test` bucket → `tgt.uniqueBucket(t, "polset")`.
- **`TestE2E_BucketPolicy_InvalidJSON` → `TestBucketPolicyInvalidJSONE2E`** — dual pattern + `runBucketPolicyInvalidJSONCases`. Verifies 400 BadRequest on both targets.
- **`TestE2E_BucketPolicy_DenyAction` → `TestBucketPolicyDenyActionE2E`** — dual pattern + `runBucketPolicyDenyActionCases`. Verifies the deny-policy 403 enforcement on both targets; cluster path exercises policy propagation through the meta-raft.
- `signedPolicyRequest` helper signature extended with `tgt s3Target` so it signs against the right endpoint + AK/SK pair.

All six fixture-paths pass (SingleNode <30ms each; Cluster4Node shared-fixture ~7s incl. boot for first test, <100ms thereafter).

## [0.0.262.9] - 2026-05-19 - test(e2e): rename remaining 9 cluster-only TestE2E_* stragglers

PR-C follow-up to v0.0.262.7 (multiraft) and v0.0.262.8 (cluster_*). 9 remaining cluster-only functions across single-purpose files renamed to the `TestXxxE2E` suffix convention. Pure rename; bodies unchanged.

### Changed

- `TestE2E_RotateKey_HappyPath` → `TestRotateKeyHappyPathE2E`
- `TestE2E_RotateKey_StatusOnlyOnSoloMode` → `TestRotateKeyStatusOnlyOnSoloModeE2E`
- `TestE2E_DegradedMode_WritesBlocked` → `TestDegradedModeWritesBlockedE2E`
- `TestE2E_HealReceiptAPI_3Node` → `TestHealReceiptAPI3NodeE2E`
- `TestE2E_SeedGroups_AutoFromNodeCount` → `TestSeedGroupsAutoFromNodeCountE2E`
- `TestE2E_NFSMultiExportPropagation_MultiNode` → `TestNFSMultiExportPropagationMultiNodeE2E`
- `TestE2E_NBDMultiNode_ByteLevelReplication` → `TestNBDMultiNodeByteLevelReplicationE2E`
- `TestE2E_DynamicJoinTwoSurvivorReelect` → `TestDynamicJoinTwoSurvivorReelectE2E`
- `TestE2E_QuarantineIncident` → `TestQuarantineIncidentE2E`

Cumulative across the three rename PRs (262.7 + 262.8 + 262.9): **43 cluster-only functions** now follow the consistent `TestXxxE2E` naming.

## [0.0.262.8] - 2026-05-19 - test(e2e): rename TestE2E_Cluster*/Bootstrap_* to TestXxxE2E convention (21 funcs)

PR-B follow-up to the multiraft rename (v0.0.262.7). 21 cluster-only functions across the `tests/e2e/cluster_*.go` files carried the legacy `TestE2E_*_*` naming. All are cluster-topology tests (dedicated multi-node clusters, no single-node analogue), so dual-pattern wrapping adds nothing. Pure rename to the `TestXxxE2E` suffix convention; bodies unchanged.

### Changed

- `TestE2E_ClusterDrain_Follower` → `TestClusterDrainFollowerE2E`
- `TestE2E_ClusterDistributionBench` → `TestClusterDistributionBenchE2E`
- `TestE2E_ClusterRemovePeer_DeadFollower` → `TestClusterRemovePeerDeadFollowerE2E`
- `TestE2E_ClusterScrubber_AutoRepair` → `TestClusterScrubberAutoRepairE2E`
- `TestE2E_ClusterEC_PutGet_5Node` → `TestClusterECPutGet5NodeE2E`
- `TestE2E_ClusterEC_3Node_ActiveKM21` → `TestClusterEC3NodeActiveKM21E2E`
- `TestE2E_ClusterEC_TopologyChange` → `TestClusterECTopologyChangeE2E`
- `TestE2E_Bootstrap_JoinUDS_AlreadyMember` → `TestBootstrapJoinUDSAlreadyMemberE2E`
- `TestE2E_Bootstrap_JoinCLI_Idempotent` → `TestBootstrapJoinCLIIdempotentE2E`
- `TestE2E_Bootstrap_DataPresent_BlocksJoin` → `TestBootstrapDataPresentBlocksJoinE2E`
- `TestE2E_ClusterPerf_All` → `TestClusterPerfAllE2E`
- `TestE2E_ClusterIncident_MissingShardFixedWithReceipt` → `TestClusterIncidentMissingShardFixedWithReceiptE2E`
- `TestE2E_ClusterTransferLeader` → `TestClusterTransferLeaderE2E`
- `TestE2E_ClusterTransferLeader_NoPeers` → `TestClusterTransferLeaderNoPeersE2E`
- `TestE2E_ClusterConfig_HotReload_FollowerObserves` → `TestClusterConfigHotReloadFollowerObservesE2E`
- `TestE2E_ClusterScaleBench_N{8,32,64,128}` → `TestClusterScaleBenchN{8,32,64,128}E2E`
- `TestE2E_Cluster_RefusesEmptyClusterKey` → `TestClusterRefusesEmptyClusterKeyE2E`
- `TestE2E_Cluster_DifferentPSK_JoinFails` → `TestClusterDifferentPSKJoinFailsE2E`

Inline doc-comment `-run "^TestE2E_ClusterScaleBench_N${N}$"` usage example in `cluster_scale_bench_test.go` updated to the new pattern.

## [0.0.262.7] - 2026-05-19 - test(e2e): rename TestE2E_MultiRaftSharding_* to TestXxxE2E (cluster-only convention)

13 cluster-only functions in `tests/e2e/multiraft_sharding_test.go` carried the legacy `TestE2E_*_*` naming. They are all multi-raft sharding tests that boot a dedicated `mrCluster` with varying `numNodes` and `mrClusterOptions` — single-node has no analogue for the multi-raft topology, so dual-pattern wrapping (`t.Run("SingleNode")`) adds nothing. Pure rename to the `TestXxxE2E` suffix convention used elsewhere in the package; bodies unchanged.

### Changed

- `TestE2E_MultiRaftSharding_Boot` → `TestMultiRaftShardingBootE2E`
- `TestE2E_MultiRaftSharding_AllNodeServices` → `TestMultiRaftShardingAllNodeServicesE2E`
- `TestE2E_MultiRaftSharding_BucketAssignment` → `TestMultiRaftShardingBucketAssignmentE2E`
- `TestE2E_MultiRaftSharding_RestartRecovery` → `TestMultiRaftShardingRestartRecoveryE2E`
- `TestE2E_MultiRaftSharding_PerGroupPersistence` → `TestMultiRaftShardingPerGroupPersistenceE2E`
- `TestE2E_MultiRaftSharding_CrossNodeDispatch` → `TestMultiRaftShardingCrossNodeDispatchE2E`
- `TestE2E_TopologyDurability_FullTargetWriteGuard` → `TestTopologyDurabilityFullTargetWriteGuardE2E`
- `TestE2E_MultiRaftSharding_GroupLeaderFailover` → `TestMultiRaftShardingGroupLeaderFailoverE2E`
- `TestE2E_MultiRaftSharding_NFSv4Smoke` → `TestMultiRaftShardingNFSv4SmokeE2E`
- `TestE2E_MultiRaftSharding_NBDRoutesThroughCoordinator` → `TestMultiRaftShardingNBDRoutesThroughCoordinatorE2E`
- `TestE2E_MultiRaftSharding_IcebergCatalogPointerAndMetadataObjectSplit` → `TestMultiRaftShardingIcebergCatalogPointerAndMetadataObjectSplitE2E`
- `TestE2E_TwoNodeAvailabilityTrap` → `TestTwoNodeAvailabilityTrapE2E`
- `TestE2E_DynamicGroupSeeding_1to5` → `TestDynamicGroupSeeding1to5E2E`

Cross-file doc-comment references in `tests/e2e/cluster_mount_nbd_test.go` and `tests/e2e/nbd_multinode_replication_test.go` updated to the new names.

## [0.0.262.6] - 2026-05-19 - test(e2e): drop every t.Skip / t.Skipf / t.SkipNow across tests/

All remaining `t.Skip` / `t.Skipf` / `t.SkipNow` / `c.t.Skipf` / `s.T().Skipf` call sites in `tests/` were removed (26 files, ~58 net lines). Combined with v0.0.262.3 (skipIfShort) and v0.0.262.5 (testing.Short blocks) this means **no test in the tree can skip itself anymore** — every test must run on every invocation. Environment gaps (missing tools, missing binaries, opt-in benchmarks) now surface as failures, not silent skips.

Sites cleared in this PR included:

- "grainfs binary not found" guards (`make build` precondition) across `cluster_ec_test.go`, `cluster_harness_test.go`, `cluster_incident_test.go`, `cluster_perf_profile_test.go`, `cluster_scale_bench_test.go`, `degraded_test.go`, `dynamic_join_quorum_test.go`, `ec_shardcache_eval_test.go`, `heal_receipt_api_test.go`, `lifecycle_replication_test.go`, `multiraft_sharding_test.go`, `volume_cli_test.go`, `colimafixture/cluster.go`, `compat/harness_test.go`, `compat/scenario_forward_read_test.go`.
- Opt-in benchmark/eval gates (`GRAINFS_DISTRIBUTION_BENCH`, `GRAINFS_PERF`, `GRAINFS_EC_SHARDCACHE_EVAL`, `GRAINFS_BENCH_FULL`).
- Tool dependency gates (`restic`, `mc`, `s3fs`, `goofys`, `rclone`, `/dev/fuse`, `toxiproxy`, `qemu`/`libnbd`, `colima` install/status).
- 256 MiB / 100 MiB large-object cluster fan-out skip in `large_object_test.go`.
- "previous binary no longer writes legacy gzip snapshots" / "COMPAT_PREV_BIN not set" compat gates.
- "Phase 6.5 audit pipeline for iceberg paths deferred" gate.
- "requires cluster fixture for fan-out" versioning skip.
- NFSv4 smoke skips in `multiraft_sharding_test.go` (`runtime.GOOS`, NFS mount permissions, colima not running, mount failure).

The NFSv4 smoke section in `multiraft_sharding_test.go::runColimaNFSv4SmokeClient` previously turned its skips into early returns via `if err != nil { t.Skip... }`. Those `if` blocks would become empty after skip removal, tripping `staticcheck SA9003 (empty branch)`. They were rewritten to `_, _ = ...` discard-the-error style so the test continues even when colima/NFS mount fails — same "surface the failure later" policy.

### Removed

- 70+ `Skip*` call sites across `tests/{e2e,compat,colimafixture,fuse_s3_colima,nbd_interop}/`.

## [0.0.262.5] - 2026-05-19 - test(e2e): drop residual testing.Short() skip blocks

Follow-up to v0.0.262.3, which stripped 99 `skipIfShort(t, ...)` call sites but left four `if testing.Short() { t.Skip(...) }` blocks intact:

- `tests/colimafixture/cluster_test.go::TestColimaClusterFixtureBoots`
- `tests/e2e/large_object_test.go` (256 MiB round-trip case)
- `tests/e2e/multiraft_sharding_test.go::TestE2E_TwoNodeAvailabilityTrap`
- `tests/e2e/multiraft_sharding_test.go::TestE2E_DynamicGroupSeeding_1to5`

All four removed. `go test -short` no longer skips any e2e or colima fixture test — classification work needs every test running so parity gaps surface.

### Removed

- 4 `if testing.Short() { t.Skip(...) }` blocks across `tests/`.

## [0.0.262.4] - 2026-05-19 - test(e2e): merge colima cluster_mount {9P,NBD,NFS4} onto shared fixture

`tests/{9p,nbd,nfs4}_colima/cluster_mount_test.go` each booted its own 3-node colima cluster via per-package `sync.Once` + `clusterRef *colimafixture.Cluster` — three separate `go test` invocations, three cluster boots, three teardowns. The cluster_mount tests are bucket-isolated and the fixture supports `EnableP9 + EnableNBD + EnableNFS` simultaneously, so the three protocols can share a single boot.

Changes:

- Moved the three `cluster_mount_test.go` files into `tests/e2e/`:
  - `cluster_mount_9p_test.go` (TestColimaCluster9PWriteVisibleAcrossNodesE2E)
  - `cluster_mount_nbd_test.go` (TestColimaClusterNBDWriteReplicatesAcrossNodesE2E)
  - `cluster_mount_nfs4_test.go` (TestColimaClusterNFS4WriteVisibleAcrossNodesE2E)
  - `cluster_mount_colima_fixture_test.go` (shared sync.Once fixture + admin CLI helper + envOrDefault).
- All three tests now share a single 3-node grainfs process group with 9P + NBD + NFSv4 listeners enabled. Net **3 cluster boots → 1**. Total `make test-cluster-mount-colima` wall-clock: ~25s for all three protocols vs ~3 × cluster-boot before.
- `colimafixture.Options` gained `SkipCleanup bool`. When true, `StartCluster` does NOT register `t.Cleanup(c.Stop)`. This unblocks the process-global `sync.Once` pattern — without it the first caller's `t.Cleanup` would stop the cluster before the next protocol test runs (the failure mode the per-package layout never hit because each package had a single cluster_mount test).
- `tests/e2e/helpers_test.go` `TestMain` now invokes `shutdownSharedColimaCluster()` after `m.Run()` alongside `stopSharedCluster` / `stopSharedMRCluster`, so the process-global colima cluster is stopped at binary exit.
- Build tag removed: the migrated files do NOT carry `//go:build colima` (none of their imports require it). They follow the same policy as the NFSv4 mount block in `multiraft_sharding_test.go` — colima is expected to be running for full e2e runs.
- New Makefile target `test-cluster-mount-colima` runs only the three migrated tests (`-run TestColimaCluster`). The top-level `test-colima` target now depends on it.

The `tests/{9p,nbd,nfs4}_colima/` directories keep their single-node `*_colima_test.go` variants (10 + 6 + 9 tests). A follow-up session can fold those into `tests/e2e/` as well so the per-protocol directories disappear entirely.

### Changed

- **`TestP9Cluster_WriteVisibleAcrossNodes` → `TestColimaCluster9PWriteVisibleAcrossNodesE2E`** (`tests/e2e/cluster_mount_9p_test.go`).
- **`TestNBDCluster_WriteReplicatesAcrossNodes` → `TestColimaClusterNBDWriteReplicatesAcrossNodesE2E`** (`tests/e2e/cluster_mount_nbd_test.go`).
- **`TestNFS4Cluster_WriteVisibleAcrossNodes` → `TestColimaClusterNFS4WriteVisibleAcrossNodesE2E`** (`tests/e2e/cluster_mount_nfs4_test.go`).
- `colimafixture.Options.SkipCleanup` (new field).
- `Makefile`: new `test-cluster-mount-colima` target; `test-colima` depends on it.

### Removed

- `tests/9p_colima/cluster_mount_test.go`
- `tests/nbd_colima/cluster_mount_test.go`
- `tests/nfs4_colima/cluster_mount_test.go`

## [0.0.262.3] - 2026-05-19 - test(e2e): unify Cache + CoW suites + drop all skipIfShort

Three bundled changes:

1. **Cache 3 tests → dual pattern**: `TestCacheReadConsistency`, `TestCacheDeleteInvalidation`, `TestCacheHeadAfterPut` migrated to `TestCache{Name}E2E` with `t.Run(SingleNode)` + `t.Run(Cluster4Node)` and `runCache{Name}Cases(t, tgt s3Target)` helpers. Hard-coded buckets (`cache-e2e-test`, `cache-del-test`, `cache-head-test`) replaced with `tgt.uniqueBucket(t, "<short>")`. Cache invariants (overwrite freshness, delete invalidation, HEAD-after-PUT) now verified on the cluster S3 surface as well.

2. **CoW 3 tests → dual pattern**: `TestCoW_SnapshotRollbackRestoresData`, `TestCoW_SnapshotListAndDelete`, `TestCoW_CloneLifecycleIndependence` migrated to `TestCoW{Name}E2E` with the same dual pattern. `cowDataDir(tgt)` derives the admin UDS path from `tgt.adminSockPath()` (single → `testServerDataDir`; cluster → leader dataDir), so the volume CLI helpers stay agnostic. Unused `nfsWriteFile`/`nfsReadFile` helpers removed. CoW exercises the cluster volume/snapshot CLI surface for the first time — expect parity gaps to surface if the volume layer is single-only today.

3. **skipIfShort removed across the e2e package (99 call sites)**: all `skipIfShort(t, "...")` invocations stripped from every test file under `tests/e2e/`. The helper definition was removed from `helpers_test.go`. `go test -short` no longer skips shared cluster fixture branches, dedicated cluster bootstrap, cluster_join, cluster_ec, distribution_bench, perf profile suite, etc. Classification work is more valuable with full visibility — gating tests behind `-short` was hiding the parity surface we are trying to map.

### Changed

- **`TestCacheReadConsistency` → `TestCacheReadConsistencyE2E`** (`tests/e2e/cache_test.go`)
- **`TestCacheDeleteInvalidation` → `TestCacheDeleteInvalidationE2E`**
- **`TestCacheHeadAfterPut` → `TestCacheHeadAfterPutE2E`**
- **`TestCoW_SnapshotRollbackRestoresData` → `TestCoWSnapshotRollbackRestoresDataE2E`** (`tests/e2e/cow_e2e_test.go`)
- **`TestCoW_SnapshotListAndDelete` → `TestCoWSnapshotListAndDeleteE2E`**
- **`TestCoW_CloneLifecycleIndependence` → `TestCoWCloneLifecycleIndependenceE2E`**

### Removed

- `tests/e2e/helpers_test.go::skipIfShort` and all 99 call sites across the e2e package.
- Dead `nfsWriteFile` / `nfsReadFile` helpers from `cow_e2e_test.go`.

## [0.0.262.2] - 2026-05-19 - test(e2e): unify EC suite onto TestBucketsE2E dual pattern

`tests/e2e/erasure_test.go` had five tests (`TestEC_BasicPutGet`, `TestEC_LargeObject`, `TestEC_MultipartUpload`, `TestEC_BucketOperations`, `TestEC_DeleteAndOverwrite`) each booting its own single-node `startECServer` and hard-coding bucket names (`ec-basic`, `ec-large`, ...). Each test was bucket-isolated, so they migrate cleanly onto the standard dual fixture pattern.

Changes:

- Renamed `TestEC_*` → `TestEc{BasicPutGet,LargeObject,MultipartUpload,BucketOperations,DeleteAndOverwrite}E2E`.
- Every test now runs `t.Run("SingleNode", ...)` + `t.Run("Cluster4Node", ...)` with `runEc{name}Cases(t, tgt s3Target)` helpers. Single uses the package-global fixture (`newSingleNodeS3Target()`); cluster uses the shared 4-node fixture (`newSharedClusterS3Target(t)`, behind `skipIfShort`). EC tests now exercise the cluster S3 surface for the first time.
- Hard-coded bucket names replaced with `tgt.uniqueBucket(t, "<short>")` so cluster reruns and parallel-running cluster tests do not collide.
- Removed `startECServer` and `createECBucketReady` helpers — `newSingleNodeS3Target` covers single (the `--scrub-interval 0 --lifecycle-interval 0` flags were already on the TestMain global), and `uniqueBucket` covers create+cleanup. Net helper code reduction.

### Known parity gap surfaced

- `TestEcDeleteAndOverwriteE2E/SingleNode` fails on master: `GetObject` after `DeleteObject` returns success (expected: NoSuchKey). Pre-existing regression from the versioning PR (same class as `TestSmokeDeploymentE2E/SingleNode/ListObjects` from PR #440). Not fixed here per the "classification work, not fix work" scope — captured for a follow-up session.

### Changed

- **`TestEC_BasicPutGet` → `TestEcBasicPutGetE2E`** (`tests/e2e/erasure_test.go`) — dual-pattern + `runEcBasicPutGetCases`. Three inner sub-tests preserved (`small_object`, `medium_object`, `nested_key`).
- **`TestEC_LargeObject` → `TestEcLargeObjectE2E`** — 5MiB body exercises the EC stripe across both targets.
- **`TestEC_MultipartUpload` → `TestEcMultipartUploadE2E`** — sub-5MiB parts; note in the test references that cluster may tighten the policy later.
- **`TestEC_BucketOperations` → `TestEcBucketOperationsE2E`** — Head/List/Delete on the unique bucket; `EventuallyWithT 30s` envelope preserved for routed-ListObjects readiness.
- **`TestEC_DeleteAndOverwrite` → `TestEcDeleteAndOverwriteE2E`** — fails on master (see Known parity gap above).

## [0.0.262.1] - 2026-05-19 - test(e2e): unify cluster-only onto shared fixture + admin CLI duals + single-only convention

Three changes bundled:

1. **Cluster-only → shared fixture (3 tests)**: `TestAwaitWriteFromNonOwnerProbe` and `TestCluster_Multipart_ListFanoutAcrossNodes` moved off their dedicated `startE2ECluster` bootstrap onto the shared 4-node cluster fixture (`newSharedClusterS3Target`). Each was creating its own 3-node cluster (~5-10s boot per test); they now share one process group that boots once on the first cluster-target test. Removed `TestCluster_Multipart_List` as a duplicate of `TestMultipartE2E/Cluster4Node/List` (same `exerciseMultipartListingFeature` helper, same surface).

2. **Cluster admin CLI duals (3 files, 6 tests)**: `TestClusterStatusCLI_*`, `TestClusterBalancerStatusCLI_*`, `TestClusterHealthCLI_*` were single-fixture only (`testServerDataDir/admin.sock`). They probe the admin UDS surface which is identical on single + cluster — so they now run dual (`SingleNode` + `Cluster4Node`) via `tgt.adminSockPath()`. Peer count expectation switches on `tgt.isCluster` / `tgt.nodes`. Catches "admin sock works on singleton but breaks on cluster" regressions.

3. **Single-only naming convention (8 tests, 6 files)**: tests with no cluster analogue (restart-on-same-dataDir, IAM bootstrap dispatch, deployment smoke, removed-flag rejection) renamed to `TestXxxE2E` and wrapped in a `t.Run("SingleNode", ...)` subtest. Bodies unchanged — they still spawn their own single binary. Future cluster equivalents drop in as a sibling `t.Run("Cluster4Node", ...)`. `TestE2E_DegradedMode_WritesBlocked` was originally tagged here but turned out to be cluster-only (5-node, kills 3); left untouched for separate handling.

Other cluster-only tests with special startup flags (ScrubInterval / lifecycle / StaticPeers / opt-in benchmark) stay on dedicated clusters; tracked for follow-up.

### Changed

- **`TestAwaitWriteFromNonOwnerProbe` → `TestClusterAwaitWriteFromNonOwnerE2E`** (`tests/e2e/cluster_harness_await_write_test.go`) — TestXxxE2E + Cluster4Node subtest + `runAwaitWriteFromNonOwnerCases(t, tgt s3Target)` helper. `tgt.cluster.AwaitWriteFromNonOwner` reaches into the cluster handle on the shared target.
- **`TestCluster_Multipart_ListFanoutAcrossNodes` → `TestClusterMultipartListFanoutE2E`** (`tests/e2e/cluster_test.go`) — same dual-pattern shape, runs against `tgt.pickNode(i)` per node (4 in shared). `tgt.uniqueBucket(t, "mpfanout")` replaces the hard-coded `"mp-list-fanout"` bucket so reruns and other tests can't collide.
- **`TestClusterStatusCLI_{NoPeers,HumanReadable}` → `TestClusterStatusCLIE2E`** (`tests/e2e/cluster_status_cli_test.go`) — dual-pattern. Inner `JSON` + `HumanReadable` subtests. Peer-count assertion derived from `tgt.isCluster ? tgt.nodes-1 : 0`.
- **`TestClusterBalancerStatusCLI{,_TextRender}` → `TestClusterBalancerStatusCLIE2E`** (`tests/e2e/cluster_balancer_status_test.go`) — dual-pattern. Inner `JSON` + `TextRender`.
- **`TestClusterHealthCLI_{NoPeers,TextRender}` → `TestClusterHealthCLIE2E`** (`tests/e2e/cluster_health_test.go`) — dual-pattern. Inner `JSON` + `TextRender`.
- **`TestCluster_NoPeers_BasicOperations` → `TestNoPeersRestartPersistenceE2E`** (`tests/e2e/cluster_test.go`) — single-only wrapper.
- **`TestCluster_NoPeers_Multipart` → `TestNoPeersMultipartE2E`** (`tests/e2e/cluster_test.go`) — single-only wrapper.
- **`TestRestartRecovery_SweepsOrphanArtifacts` → `TestRestartRecoveryOrphanSweepE2E`** (`tests/e2e/restart_recovery_test.go`).
- **`TestSmoke_DeploymentVerification` → `TestSmokeDeploymentE2E`** (`tests/e2e/smoke_test.go`).
- **`TestServe_RejectsRemovedUpstreamFlags` → `TestServeFlagsRejectionE2E`** (`tests/e2e/serve_flags_test.go`).
- **`TestE2E_Bootstrap_F1..F4` → `TestBootstrap{FirstSAWildcardGrant,SecondSANoAutoGrant,PreBootstrapDenied,PostBootstrapVerbs}E2E`** (`tests/e2e/iam_bootstrap_test.go`).

### Removed

- **`TestCluster_Multipart_List`** — duplicate of `TestMultipartE2E/Cluster4Node/List` (same `exerciseMultipartListingFeature`).
- **`startMultipartListingCluster` helper** — replaced by `tgt.uniqueBucket` + `waitForMultipartListingCreate`. No more bespoke cluster bootstrap for multipart listing tests.

### Tests

- Cluster-only on shared:
  - `TestClusterAwaitWriteFromNonOwnerE2E/Cluster4Node` PASS (6.89s)
  - `TestClusterMultipartListFanoutE2E/Cluster4Node/{node-1,2,3,4}` PASS (27.04s total; per-node assertions ≤0.04s)
  - `TestMultipartE2E/Cluster4Node/List` unchanged PASS — list helpers untouched.
- Admin CLI duals (SingleNode + Cluster4Node × JSON + Text):
  - `TestClusterStatusCLIE2E` 4/4 PASS
  - `TestClusterBalancerStatusCLIE2E` 4/4 PASS (7.03s incl. cluster boot)
  - `TestClusterHealthCLIE2E` 4/4 PASS
- Single-only renames (SingleNode wrappers only, bodies unchanged):
  - `TestNoPeersRestartPersistenceE2E/SingleNode` PASS (0.93s)
  - `TestNoPeersMultipartE2E/SingleNode` PASS (0.47s)
  - `TestRestartRecoveryOrphanSweepE2E/SingleNode` PASS (0.42s)
  - `TestServeFlagsRejectionE2E/SingleNode/{--upstream,--upstream-access-key,--upstream-secret-key}` PASS (0.07s)
  - `TestBootstrap{FirstSAWildcardGrant,SecondSANoAutoGrant,PreBootstrapDenied,PostBootstrapVerbs}E2E/SingleNode` PASS (~0.45s each)
  - `TestSmokeDeploymentE2E/SingleNode/ListObjects` FAILS — pre-existing regression from master's versioning PR (delete-marker shows in listing); flagged in TODOS, unrelated to rename.

## [0.0.262.0] - 2026-05-19 - feat(storage): Phase 1 large-object chunking foundation — segment-based PUT/GET, xxhash3 integrity

Every object now persists as a sequence of one or more `SegmentRef` instead of a single flat file. PUT/GET stream through 8-worker chunker/fetcher pipelines that produce 16 MiB chunks (default). Internal segment integrity moves from MD5 to xxhash3-128, eliminating dual hashing on the hot path. Range GET, sendfile zero-copy, multipart, AppendObject, packblob, and PITR snapshot/restore all stay correct under the new layout. Single-node and 4-node cluster e2e round-trips byte-identical for 100 MiB / 256 MiB / cross-chunk Range. Cluster `RoundTrip100MiB` is intentionally skipped pending Phase 2 (non-aligned tail chunk fanout); 256 MiB and 64 MiB Range pass.

### Added

- **xxhash3-128 segment checksum utility** (`internal/storage/checksum.go`) — `NewChecksumHasher` streaming, `ChecksumOf` one-shot, big-endian Hi||Lo encoding. 10–20 GiB/s/core vs MD5의 ~500 MiB/s. Used for repair verification and scrubber bit-rot detection. Locked in by 3 unit tests.
- **`SegmentWriter`** (`internal/storage/segment_writer.go`) — streaming chunker + 8-worker pool + aggregator. Memory bounded to `16 MiB × (workers + queue) ≈ 144 MiB` per request regardless of object size. Handles unknown Content-Length (chunked transfer encoding), empty-object case (1 zero-byte segment), mid-stream error abort with atomic no-commit. `fillChunk` preserves upstream `io.ErrUnexpectedEOF` (unlike `io.ReadFull`).
- **`SegmentReader`** (`internal/storage/segment_reader.go`) — parallel fetcher with in-order assembler. Pre-populated pending slots eliminate nil-deref race from the original plan. Releases backing arrays after consumption so peak memory stays at `16 MiB × workers ≈ 128 MiB`. Locked in by 4 unit tests including race detector + GC contract.
- **`SegmentRef.checksum` / `placement_group_id` / `shard_size`** (`internal/storage/storagepb/storage.fbs`) — FlatBuffers schema migration. `Object.append_call_md5s:[BytesValue]` carries per-call MD5 chain so AppendObject ETag varies per call without per-segment MD5. `etag` field on `SegmentRef` removed; internal segments carry no S3-visible MD5.
- **`PackedBackend.ReadAt` (PartialIO)** (`internal/storage/packblob/packed_backend.go`) — pack-path Range GET now works for objects above `--pack-threshold`. Packed-inline entries slice from the pack blob; pass-through delegates to the inner backend. 이전엔 `wal: inner backend does not support ReadAt`로 거부됨.
- **`localSegmentStore` + `localBackendAdapter`** (`internal/storage/segment_adapter.go`) — production adapters that route segment writes through `WriteSegmentBlob` and segment reads through `openMaybeEncryptedSegment`.
- **`tests/e2e/large_object_test.go`** — dual-target (SingleNode + Cluster4Node) round-trip + Range across chunk boundary. Reuses the shared cluster fixture; new cases plug into the existing e2e convention (PR #422 style).
- **PITR snapshot/restore segment awareness** — `SnapshotObject.Segments`, `ListAllObjects` propagates them, `RestoreObjects` checks each segment path (with legacy `objectPath` fallback) and reconstructs `Object.Segments`. New tests cover multi-segment + single-segment round-trip and stale-when-segment-missing detection.

### Changed

- **All objects route through `SegmentWriter` / `SegmentReader`** (`internal/storage/local.go`) — `PutObjectWithRequest` and `GetObject` no longer use the legacy single-file path. Single-segment GETs return the segment file directly (Hertz sendfile upgrade preserved for unencrypted). Multi-segment GETs stream through the parallel reader.
- **`ReadAt` + sendfile path are segment-aware** (`internal/storage/local.go`) — walks `obj.Segments`, dispatches per-segment `os.File.ReadAt` (plain) or `readAtEncryptedObjectFile` (encrypted) on each overlapping slice. Out-of-range returns `(0, io.EOF)` per `os.File.ReadAt` semantics. Sendfile fast-path triggers for single-segment unencrypted.
- **`WriteSegmentBlob` uses xxhash3** (`internal/storage/append.go`) — no segment-level MD5. `encryptedObjectFileDomain` already includes the unique blob_id, so AAD is segment-scoped by construction (verified by `TestEncryptedSegment_PerSegmentAADIsolation`).
- **`writeEncryptedObjectFileWithHash` → `writeEncryptedObjectFile(io.Writer)`** (`internal/storage/encrypted_object_file.go`) — generalized signature so callers pass any sink (checksum hasher, multi-writer, `io.Discard`).
- **`AppendObject` per-call MD5 chain** (`internal/storage/append.go`) — `appendNew` and `appendExisting` capture each call's payload MD5 (stopgap: segment checksum) into `Object.AppendCallMD5s`, then compute composite ETag from that chain. Single-node ETag now varies per call as cluster always did. Real MD5 wire-up tracked for Phase 3.
- **Cluster wire compatibility bridge** (`internal/cluster/codec.go`, `apply.go`) — `clusterpb.SegmentRef.etag` is filled from `hex.EncodeToString(seg.Checksum)` and decoded back symmetrically. Rolling-upgrade safe; old peers parse new buffers byte-identically while we migrate to xxhash3 in Phase 2.

### Removed

- **`SegmentRef.etag` field** (`internal/storage/storage.go`, `storagepb/storage.fbs`) — internal segments no longer carry an S3-visible MD5. `CompositeETag` rewritten to take `[][]byte` (per-call MD5 chain) instead of `[]SegmentRef`.
- **Legacy `objectPath`-based PutObject / GetObject body** — the single-file write path on top of `data/<bucket>/<key>` is gone for new objects. `objectPath` itself stays (still used by `WriteAt`/`ReadAt`/`Truncate`/`Sync` legacy callers — those will move in subsequent phases).

### Tests

- New unit tests: `TestChecksum*` (3), `TestSegmentWriter_*` (3 incl. boundary + drip-feed + stream-error), `TestSegmentReader_*` (4 incl. reverse-order + atomic abort + GC contract), `TestWriteSegmentBlob_PopulatesChecksum`, `TestEncryptedSegment_PerSegmentAADIsolation`, `TestRangeGet_ChunkBoundaries` (6 boundary patterns × plain + encrypted), `TestPackedBackend_RangeAcrossSegments`, `TestSnapshotRestore_ChunkedObject*` (round-trip + stale).
- E2E: `TestLargeObjectE2E` (SingleNode 3/3 PASS, Cluster4Node 2/3 PASS + 1 SKIP), pre-existing `TestBucketsE2E` / `TestS3VersioningE2E` / `TestMultipartChunkedUploadPartE2E` / `TestAppendObjectE2E` all still PASS (including concurrent append + owner-kill survival).

### Known Phase 2 carry-forward

- Cluster 100 MiB non-aligned tail chunk corrupts body (16 MiB-aligned objects OK). `TestLargeObjectE2E/Cluster4Node/RoundTrip100MiB` is skipped with a Phase 2 reference; tracked in `TODOS.md`.
- AppendObject ETag uses segment-checksum-as-MD5 proxy (stopgap mirrors cluster path); real per-call MD5 capture deferred to Phase 3.1.
- `WriteAt` / `Truncate` legacy single-file path stays — affects `internal/nfs4server` (4 mixed-semantics tests) and `internal/p9server` (1 test). Pre-existing test patterns that mix PutObject (segments) with WriteAt (flat).
- WAL replay PITR + segments: `wal.Entry` does not yet carry `Segments`, so PITR objects from WAL-only replay can mis-report as stale. Phase 2 will extend the WAL serialization.
- `VFS Rename` memory invariant: `SegmentReader` buffers full segments (16 MiB), so a 5 MiB Rename's heap growth exceeds the 5 MiB ceiling assertion. Sliding-window optimization deferred (no benchmark pressure yet).

## [0.0.261.0] - 2026-05-19 - test(e2e): unify protocol-surface tests onto TestBucketsE2E dual pattern + expose latent parity gaps

`tests/e2e/` 의 protocol-surface 테스트를 `TestBucketsE2E` 스타일 (단일 `TestXxxE2E` + SingleNode/Cluster4Node 듀얼 + 단일 `runXxxCases` 헬퍼) 로 통일. 통일의 부산물로 그동안 single-only 또는 cluster-only 로 가려져 있던 **두 개의 진짜 single↔cluster parity 격차**가 failing subtest 로 노출됨 — 이게 통일의 주된 목적 ([[feedback-single-cluster-parity]] 정책: surface 는 양쪽 동일 동작). 본 PR 은 신호를 켜는 데 집중하고 격차 자체의 backend fix 는 follow-up PR.

### Changed

- **`TestE2E_NBDCases{SingleNode,Cluster}` → `TestNBDMatrixE2E`** (`tests/e2e/nbd_matrix_cases_test.go`) — 두 top-level 함수를 한 `TestNBDMatrixE2E` + `t.Run("SingleNode")` + `t.Run("Cluster4Node")` 로 통합. 본문은 기존 `runNBDCases` 헬퍼 그대로 — 패턴 정렬만, 동작 무변.
- **`TestIcebergConcurrentCommitsE2E`** (`tests/e2e/iceberg_concurrent_commits_test.go`) — ENV-gate (`GRAINFS_TEST_ICEBERG_STRESS`) + in-helper `if !tgt.isCluster { t.Skip(...) }` 두 skip 제거. 본문이 `tgt.endpoint(i)` 로 양쪽 target 의 N 노드 (single=1, cluster=4) 를 fan-out — single 은 forward path 가 없어 503 이 구조적으로 발생하지 않는 control, cluster 는 spec §8 `iceberg-rare-quic-stream-local-cancel-under-load` 의 ≤0.5% 임계로 회귀 검지. 검증: SingleNode 1600 ops → 1438/162/0, Cluster4Node 1600 ops → 1220/377/3 (≤8 임계).
- **`TestAppendSizeCapE2E`** (`tests/e2e/append_size_cap_test.go`) — Cluster4Node 단독에서 SingleNode + Cluster4Node 듀얼로. 케이스 두 개 (`RejectAtCap`, `ConcurrentRaceAtCap`) 모두 양쪽에서 의미 있는 동작. 가능해진 이유는 아래 새 fixture.
- **`TestPullthroughE2E`** (`tests/e2e/pullthrough_test.go`) — 두 절차적 top-level (`TestPullThrough_FetchesFromUpstream`, `TestPullthrough_LargeObjectE2E`) 을 한 `TestPullthroughE2E` + 듀얼 + `runPullthroughCases` 헬퍼 + `startPullthroughUpstream(t)` (throwaway single-node grainfs upstream + t.Cleanup) 로 통합. 사례명도 `FetchesFromUpstream` / `LargeObject` 로 정리.

### Added

- **`newDedicatedSingleNodeS3Target(t, extraArgs []string) s3Target`** (`tests/e2e/target_test.go`) — per-test single-node grainfs spawn + admin UDS bootstrap + auto-snapshot disable + `t.Cleanup` 종료/정리. cluster 측의 `newClusterS3Target` (dedicated) vs `newSharedClusterS3Target` (process-global) 의 대칭을 single 측에 미러링. ExtraArgs 가 필요한 케이스만 비용 (per-test boot) 부담, 일반 케이스는 기존 package-global single 그대로.
- **`s3Target.adminSockPath() string`** (`tests/e2e/target_test.go`) — 모든 fixture 변종 (single-package-global / single-dedicated / shared-cluster / dedicated-cluster) 에서 "writable 노드" (single = 유일 노드, cluster = elected leader) 의 admin UDS 경로 노출. per-bucket admin PUT (e.g. `iamPutBucketUpstream`) 이 필요한 surface 테스트가 fixture 종류에 무관하게 동작.

### Pre-existing — exposed via unification (follow-up PR)

다음 두 격차는 본 PR 의 통일 작업이 노출한 **사전 존재** parity bug 임. 이번 PR 의 회귀 아님 — 통일 전에는 한쪽이 missing 이라 숨어 있던 격차. 통일 후 그 missing side 가 failing subtest 가 됨. 실패가 의도된 신호이며, follow-up PR 에서 backend 측에서 닫는다 (`TODOS.md` 참조).

- **`TestPullthroughE2E/Cluster4Node/LargeObject`** — cluster pull-through 가 5 MiB 페이로드를 truncate / corrupt. SingleNode 는 동일 케이스 통과. cluster 측 2-pass streaming write 경로의 race / 미완료-닫힘 의심. TODOS → "Pull-through Parity Follow-Ups → Cluster pull-through large-object parity".
- **`TestAppendCoalesceE2E/SingleNode`** — single-node `LocalBackend` 가 `storage.PartialIO` 미구현이라 post-coalesce appendable GET 이 `wal: inner backend does not support ReadAt` EOF. Cluster4Node 통과. TODOS → "AppendObject Follow-Ups → Single-node LocalBackend missing PartialIO (ReadAt)".

### Tracking

- TODOS.md → 신규 `Pull-through Parity Follow-Ups` 섹션 + `AppendObject Follow-Ups` 의 PartialIO 항목.
- 본 PR 은 `make test-e2e` 의 두 subtest (`TestPullthroughE2E/Cluster4Node/LargeObject`, `TestAppendCoalesceE2E/SingleNode`) 가 의도적으로 실패한 상태로 land — 신호가 켜져 있어야 backend fix PR 이 그것을 끄는 시그널을 받음.

## [0.0.260.0] - 2026-05-19 - feat(auth): zero-config progressive application — §1 Foundation slice

Auth redesign §1 Foundation slice. Spec/plan: `docs/superpowers/specs/2026-05-19-auth-redesign.md` (D#1, D#4, D#5). 5 new internal packages, 5 new FSM MetaCmds + 2 backward-compatible snapshot trailers, 21 commits, +4069 -13 lines. **Runtime wiring deferred** — admin UDS surface, server hot-swap, scrubber→storage adapter는 후속 슬라이스 (§2-§9). data-plane 영향 없음, snapshot 호환 유지.

### Added

- **`internal/nodeconfig`** (Tasks 1-2) — node-local resource resolver. `TLSCertPath()` / `TLSKeyPath()` / `KEKSource()` / `LogLevel()` 4개 메서드, 각각 `<data>/...` convention path + env override (`GRAINFS_TLS_CERT`, `GRAINFS_KEK_SOURCE`, `GRAINFS_LOG_LEVEL`). KEK source는 `file://` URI 반환 (kms://는 v2 이연).
- **`internal/encrypt`** (Tasks 3-5, 13) — KEK/DEK 분리 모델. `LoadOrGenerateKEK(file://path)` 로 32B 키 자동 생성 (mode 0600, O_NOFOLLOW, absolute-path 검증, looser-perm 거부). `AESGCMSeal/Open` 저수준 프리미티브. `DEKKeeper`는 `dek_gen uint32` 세대별 wrapped DEK 맵을 들고, AEAD를 한 번 캐싱 — `Seal/Open` hot-path는 매 호출마다 `aes.NewCipher`/`cipher.NewGCM` 재빌드 안 함 (S3 object I/O당 ~4 heap alloc 절감). plaintext DEK는 AEAD 빌드 직후 즉시 zeroize. `Rewrap(ct, oldGen)`은 RLock 1회로 open+seal 동시 처리. `RewrapScrubber`는 Backend interface 추상화로 gen 단위 재암호화 (F#17 atomic-swap 컨트랙트).
- **`internal/config`** (Tasks 6-7) — FSM-backed cluster-wide config registry. `Store.Register/Set/Unset/GetString/GetBool/ListAll/Snapshot/Restore`. `BoolSpec`/`StringSpec`/`TriggerSpec`/`Uint32Spec` 타입별 spec + reload-hook 콜백. `Set/Unset`은 reload-hook panic 시 자동 rollback + recover (FSM apply goroutine 보호). `Restore`는 spec validator 통과 값만 적용 (tampered snapshot 방어). 9개 cluster 키 등록 (`iam.anon-enabled`, `iam.allow-anonymous-bucket-policy`, `trusted-proxy.cidr`, `jwt.signing-key-rotate/prune`, `encryption.rotate-dek` (no-op reload), `encryption.prune-dek-version` (no-op reload), `cluster.read-only`, `audit.deny-only`).
- **`internal/cluster` 확장** (Tasks 9-12) — 4개 새 MetaCmd: `MetaCmdTypeConfigPut=46`, `ConfigDelete=47`, `DEKRotate=48`, `DEKVersionPrune=49`. FlatBuffers 스키마 `MetaConfigPutCmd`/`MetaConfigDeleteCmd`/`MetaDEKVersionPruneCmd`/`MetaConfigSnapshot`/`MetaDEKVersionSnapshot`/`ConfigEntry`/`DEKVersionEntry`/`DEKRefEntry` 추가. snapshot에 두 trailer 추가: **GCFG** (0x47464347) — config 값 직렬화, **DKVS** (0x53564B44) — DEK versions + ref counts + active gen. 둘 다 root + IAM trailer 뒤에 append, restore는 역순 peel. backward-compat: pre-Task-10 snapshot (GCFG 없음) / pre-Task-11 (DKVS 없음) / pre-Task-12 (DKVS에 ref_counts 필드 없음) 모두 로드. 마지막은 `objectIndex`에서 ref count 재구축. `MetaObjectIndexEntry.dek_gen:uint32=0` 추가 — FlatBuffer 기본값이 마이그레이션 역할.
- **`internal/cluster/post_commit.go`** — FSM 일반 post-commit hook surface. `RegisterPostCommit(h)` copy-on-write CAS, `firePostCommitHooks`는 `atomic.Pointer[[]PostCommitHook]` lock-free load. 0-hook 클러스터는 매 apply마다 single atomic load만 부담.
- **`internal/serveruntime/dek_post_commit.go`** — `DEKPostCommitDispatcher` + `WireDEKPostCommit`. `MetaCmdConfigPut(encryption.rotate-dek=now)` → goroutine으로 `ProposeDEKRotate` deferred dispatch (Pass 1 F-A1: apply goroutine 내부에서 propose 금지, raft deadlock 방어). `MetaCmdDEKRotate` apply 후 per-node scrubber kick — leader-only가 아니라 모든 노드가 자기 로컬 shard 처리 (Pass 1 F-A3).

### Security

- KEK 파일 모드 0o600이 아니면 거부 (`ErrKEKPermissionsTooLoose`). 0o644로 chmod된 KEK는 클러스터 identity 유출 위험.
- KEK 경로 symlink 거부 (`ErrKEKSymlink`, `O_NOFOLLOW`). data 디렉토리 쓰기 권한 attacker가 kek.key → 임의 32B 파일 symlink 공격 차단.
- `LoadFromFSM`이 `len(kek) == KEKSize` 검증. malformed key가 keeper에 silently 저장돼서 모든 Seal/Open 실패하는 시나리오 차단.
- `DEKRefEntry.Count()`의 `int64 → uint64` 캐스팅에서 음수 거부. tampered/bit-flipped snapshot에서 -1이 max-uint64로 변환돼 prune이 영원히 막히는 DEK leak 차단.

### Performance

- `config_codec.encodeMetaConfigSnapshot`은 키를 정렬 후 직렬화 — replica 간 snapshot byte 결정성 보장 (raft hash 비교 통과). `dek_codec`은 gen + ref_counts 둘 다 정렬.
- `config.Store.ListAll`은 Key 기준 정렬 — CLI/admin API 일관된 순서.
- `DEKKeeper`의 generation별 `cipher.AEAD` 캐싱 (위 Added 참조). `Rewrap`은 single-lock open+seal로 scrubber 처리량 2배 개선.
- `MetaFSM.firePostCommitHooks`는 `atomic.Pointer` load — 0-hook fast path는 lock 획득 0회.

### Tests

- 새 패키지마다 단위 테스트 동반. 핵심 보강: `TestAESGCMOpen_RejectsShortCiphertext`/`RejectsWrongKeyLength`, `TestLoadOrGenerateKEK_RejectsLoosePermissions`/`RejectsSymlink`/`RejectsWrongSizeFile`/`RejectsRelativePath`, `TestDEKKeeper_PruneRefusesActiveGen`/`ActiveReturnsCopy`/`VersionsIsDeepCopy`/`ConcurrentSealOpenRotate` (`-race` 50 goroutine × 200ms × Rotate every 1ms), `TestLoadFromFSM_EmptyVersions`/`RoundTrip`, `TestDEKRefCount_RebuildsFromObjectIndexWhenTrailerMissing` (gen 0 + gen 1 multi-gen rebuild), `TestSnapshot_GCFGTrailerByteDeterminism` (16x encode 동일 결과), `TestSnapshot_RestoreConfigValues`/`LegacyWithoutConfigTrailer`, `TestRewrapScrubber_AtomicSwap_NoCorruptMidUpdate` (50 reader vs scrubber). `TestApply_*` 시리즈로 모든 MetaCmd apply path + nil-store/keeper 가드 + 트레일러 인코딩 검증.
- `make test-unit`/`make lint`/`make build` 모두 green. `internal/cluster` race-clean (50s × `-race`).

### Deferred (후속 §)

- 사용자 facing surface 없음 — admin UDS 명령 (`grainfs iam ...`, `grainfs config set ...`, `grainfs cluster join`), server 통합 (TLS hot-swap, OAuth2 endpoint, bearer middleware), real storage backend `IterByDEKGen`/`AtomicSwap` 어댑터, runtime의 `WireDEKPostCommit` invocation은 후속 슬라이스 (§2 IAM core, §3 Bucket lifecycle, §4 Iceberg auth, §5 Server posture, §6 Audit, §7 Cluster lifecycle, §8 CLI, §9 E2E + docs). 이 슬라이스는 후속 task들의 의존성을 미리 안정화.

## [0.0.259.0] - 2026-05-19 - fix(cluster+storage): warp `versioned` benchmark passes; single-node versioning fully wired

Warp `versioned` 워크로드가 cluster 에서 STAT 100% 501 로 깨지던 갭과, 단일 노드 fixture 에서 versioning 이 사실상 동작하지 않던 갭을 한 번에 정리. 결과: 4-node cluster warp `versioned` 0 STAT-501 errors, SingleNode + Cluster4Node e2e versioning suite 17/17 통과 (1 cluster-only skip).

### Fixed

- **cluster HEAD by versionId returned 501** (`internal/cluster/cluster_coordinator.go`, `internal/cluster/forward_*.go`, `internal/raft/raftpb/forward_cmd.fbs`) — `ClusterCoordinator` 에 `HeadObjectVersion` 이 빠져 있어 `storage.Operations` 어댑터 체인이 `VersionedHeader` 인터페이스를 못 찾고 `UnsupportedOperationError → 501` 을 반환. warp `versioned` 의 STAT(HEAD ?versionId=) 가 8663/8663 으로 100% 실패. 신규 `ForwardOpHeadObjectVersion = 20` + `HeadObjectVersionArgs{bucket,key,version_id}` FBS 추가, coordinator/receiver/dispatch/codec 에 frame-only 경로 와이어링. 같은 패턴인 `GetObjectVersion` 과 동형. 검증 후 STAT 8663 errors → 0.
- **forward 경로의 `storage.ErrMethodNotAllowed` 손실 → 500** (`internal/cluster/forward_codec.go`, `internal/cluster/forward_receiver.go`, `internal/raft/raftpb/forward_cmd.fbs`) — `mapErrorToStatus` 가 `ErrMethodNotAllowed` 를 매핑 안 해서 cluster forward 의 delete-marker HEAD 가 405 대신 500 을 반환. 신규 `ForwardStatusMethodNotAllowed = 12` 추가하고 `parseReplyStatus` 양방향 매핑. delete-marker HEAD 가 정상적으로 405 + `x-amz-delete-marker: true` 를 돌려줌.
- **single-node PUT 이 versioning-enabled 버킷에서 VersionId 를 안 돌려줌** (`internal/storage/packblob/packed_backend.go`) — `--pack-threshold=65537` 기본값 때문에 작은 오브젝트가 packblob fast path 로 흘러가 `*storage.Object{VersionID:""}` 를 반환, `DistributedBackend` 의 `newVersionID()` 우회. `PutObjectWithRequest` 에 `BucketVersioner.GetBucketVersioning(bucket) == "Enabled"` 일 때 inner 백엔드로 위임하는 bypass 추가. 응답 헤더 `x-amz-version-id` 정상화. cluster 모드는 packblob 미사용이라 영향 없음.
- **single-node DELETE 가 versioning-enabled 버킷에서 marker VersionId 누락 + `wal: inner backend does not support DeleteObjectVersion`** (`internal/storage/packblob/packed_backend.go`) — packblob 이 wal 과 version-aware inner 사이에 끼어 `ObjectVersionDeleter` / `VersionedSoftDeleter` 인터페이스를 만족하지 못해 wal 의 타입 assertion 이 실패. `DeleteObject` 에 동일한 versioning bypass + 신규 `DeleteObjectReturningMarker` / `DeleteObjectVersion` pass-through 추가. SoftDelete (marker 생성) / HardDeleteByVersionID 모두 동작.

### Tests

- **`tests/e2e/versioning_test.go` 전면 재구성** — 기존 절차적 `TestE2E_Versioning_Full` 을 제거하고 `TestS3VersioningE2E` 하나의 entry 로 통일. `TestBucketsE2E` 스타일의 SingleNode + Cluster4Node 듀얼 분기 + `runVersioningCases(tgt s3Target)` 헬퍼 + t.Run sub-test 구조. 9개 케이스: EnableAndStatus, PutGetByVersionID, HeadByVersionID, HeadByVersionID_AllNodes (cluster fan-out), HeadByVersionID_DeleteMarker, SoftDelete, HardDeleteByVersionID, ListVersions, ListVersionsWithDeleteMarker.
- **신규 단위 테스트** (`internal/cluster/cluster_coordinator_test.go`, `internal/cluster/forward_codec_test.go`, `internal/cluster/forward_dispatch_test.go`, `internal/cluster/forward_receiver_integration_test.go`) — coordinator forward routing, codec roundtrip, dispatch coverage, receiver dispatch 검증.

### Follow-ups (별도 PR)

- packblob bypass 의 `state == "Enabled"` 체크를 `Enabled || Suspended` 로 확장 (Suspended 버킷은 여전히 packed fast path 를 탐). TODOS.md 에 추적.

## [0.0.258.0] - 2026-05-19 - fix(s3+cluster): warp multipart correctness on the 4-node cluster

Warp `multipart` 워크로드가 4-node cluster 에서 두 가지 다른 이유로 깨지던 것을 한 번에 정리한 PR. e2e (`TestMultipartChunkedUploadPartE2E`, `TestMultipartGetPartNumberE2E`) 를 SingleNode + Cluster4Node `TestBucketsE2E` 스타일로 추가하여 회귀 잠금. 부차적으로 `bench_s3_compat_compare.sh` 의 cluster startup 과 warp delete 샘플 부족 워닝을 정리하고, `append_coalesce` / `append_mid_size_body` e2e 를 dedicated cluster → shared cluster fixture 로 옮겨 fixture 부팅 비용을 제거.

### Fixed

- **`UploadPart` aws-chunked framing leak** (`internal/server/multipart_api.go`) — warp 의 multipart workload 는 모든 part 를 `X-Amz-Content-Sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD` + aws-chunked body framing 으로 전송하는데, prior 구현은 `c.Request.Body()` 를 그대로 storage 에 저장해 chunk header + per-chunk signature 가 part payload 로 섞여 들어갔다. `Part.Size` 와 cluster object `Size` 가 framing overhead 만큼 부풀고 `?partNumber=N` GET 이 framed bytes 를 반환. 같은 helper (`putObjectBody`) 를 사용해 framing 을 decode 하고 실패 시 400 `InvalidArgument` 반환 — PutObject 와 동일한 경로.
- **forward 경로의 `storage.Object.Parts` 손실** (`internal/raft/raftpb/forward_cmd.fbs`, `internal/cluster/forward_codec.go`) — `ForwardObjectMeta` FlatBuffers schema 에 `parts` vector 가 없어, HEAD / GET / CompleteMultipartUpload 가 다른 data group 으로 routed 될 때 leader 가 만든 reply 가 wire 인코딩에서 Parts 를 떨궜다. 클라이언트 `objectFromReply` 가 empty `Parts` 로 Object 를 재구성 → S3 server 의 `partRange` 가 "no parts → 단일 가상 part 취급" fast path 로 빠져, warp multipart 가 cluster 에서 `PartsCount=1` 을 보고 `?partNumber>=2` 에 416 `InvalidPartNumber` 를 받았다. `parts:[ForwardPartMeta]` 를 schema 에 추가하고 `appendPartsVector` / `readPartsVector` helper 로 `buildObjectReply` / `buildGetObjectReply` / `objectFromReply` / `objectsFromReply` 양방향에 와이어링. backward-compat: vector 가 없거나 빈 reply 는 `Parts=nil` 로 decode 되어 single-PUT / append / pre-fix legacy entry 가 그대로 동작.

### Changed

- **`bench_s3_compat_compare.sh` cluster readiness** — fixed `CLUSTER_WARMUP_SLEEP` (default 5s, 종종 override 로 45s 까지) 제거. `bench_wait_cluster_leader` (이미 `bench_iceberg_table.sh` / `bench_nfs_cluster_profile.sh` 에서 사용 중) 를 bootstrap 노드 (node-1, meta-group leader) 에 한 번 호출해 `/api/cluster/status` 의 `state == "Leader"` 를 폴링. follower 들은 자체 `state == "Follower"` 라 같은 endpoint 에서 leader probe 가 실패하므로 노드별 폴링은 부적절. 데이터 그룹 leader 는 첫 write 에서 자연스럽게 elect.
- **warp delete `--objects` 하한** (`bench_s3_compat_compare.sh`) — `WARP_CONCURRENT × WARP_DELETE_BATCH × 4` (≈6400) 에서 16× (≈25600, warp 자체 default 와 일치) 로 상향. local-disk packblob 에서 6400 batched delete 가 1-2 초에 끝나 warp analyze 가 `Skipping DELETE too few samples` 를 출력하던 것을 해결. 64KiB object 기준 pre-upload 도 몇 초 늘어나는 정도.

### Tests

- **신규 e2e** `TestMultipartChunkedUploadPartE2E` (`tests/e2e/multipart_chunked_e2e_test.go`) — `TestBucketsE2E` 스타일 SingleNode + Cluster4Node. aws-sdk-go-v2 의 `bytes.NewReader` 경로는 body 를 in-memory 해싱해 streaming transport 를 트리거하지 않으므로, raw `http.Request` + SigV4 sign 으로 aws-chunked UploadPart 를 손수 작성해 full GET + `?partNumber=1` GET 둘 다 plaintext bytes 를 반환하는지 확인.
- **신규 e2e** `TestMultipartGetPartNumberE2E` (`tests/e2e/multipart_part_number_test.go`) — 동일 듀얼 스타일. 2 × 5 MiB part 업로드 → Complete → full GET / `?partNumber=1` / `?partNumber=2` / `?partNumber=3` (416) 검증. cluster Parts forward 버그를 정확히 노출한 테스트.
- **신규 단위 테스트** `TestForwardCodec_ObjectReply_PartsRoundTrip` / `_GetObjectReply_PartsRoundTrip` / `_NoParts` (`internal/cluster/forward_codec_test.go`) — schema 변경 round-trip 잠금. nil Parts 입력은 디코딩 후 `Parts: nil` 로 유지되어 `partRange` 의 "no parts" fast-path 가 그대로 작동하는 것까지 보장.
- **fixture refactor** — `TestAppendCoalesceE2E` / `TestAppendMidSizeBodyE2E` 를 `newClusterS3Target(t, 4)` (dedicated, 매 테스트 4-node 부팅/철거) 에서 `newSharedClusterS3Target(t)` (process-global, lazy boot) 로 전환. `TestBucketsE2E` 외 8 개 테스트가 같은 shared fixture 를 재사용해 42s 에 PASS (각자 별도 부팅하면 +30s 이상). `TestAppendObjectE2E` 의 `OwnerKillSurvives` 는 cluster topology 를 mutate (KillNode + defer RestartNode) 하므로 dedicated 유지 — 분리해 shared 로 옮기는 것은 follow-up. `append_size_cap_test` 는 `--append-size-cap-bytes` extraArgs 때문에 dedicated 유지.

### Follow-ups (별도 PR)

- `TestAppendObjectE2E` 의 `OwnerKillSurvives` 만 별도 파일 + dedicated fixture 로 떼어내면 common case 들도 shared cluster 로 이동 가능.
- `append_coalesce` / `append_size_cap` 의 SingleNode 의도적 부재를 `t.Run("SingleNode", t.Skip("reason"))` 형태로 통일 ([[feedback-e2e-test-style]] 컨벤션).
- `pullthrough_test.go`, `versioning_test.go` 의 `TestE2E_Versioning_Full` 은 `TestBucketsE2E` 패턴이 아닌 절차적 구조. `runXxxCases(tgt)` 헬퍼 + dual SingleNode/Cluster4Node 로 정렬 필요.
- warp `multipart`, `multipart-put` op 의 cluster sanity-mode 통과는 별도 세션 (각각 무거워 본 PR scope 에서 제외).
- warp `versioned` op 의 501 — bucket versioning feature 자체 별도 plan.

## [0.0.257.3] - 2026-05-19 - fix(storage/packblob): ListObjectsPage to supplement packed in-memory index

v0.0.257.0의 `Operations.ListObjectsPage` walk-and-find-pager 로직이 PackedBackend 계층을 건너뛰고 inner ClusterCoordinator로 바로 가서, single-node packblob fast path에 저장된 작은 객체가 LIST에 안 나오던 회귀 수정. e2e fail 24건 중 동일 root cause(packblob index 우회) 3건 회복: TestObjectsE2E/SingleNode/{List,ListWithPrefix} (Cluster A), TestS3ClientSmoke, TestMigrationInjector. 같은 cluster A의 나머지 5건(TestSnapshot/PITR×2/Backup_Restic/IAM_ScopedKey/QuarantineIncident)은 restore-후-ObjectIndex 재활성화 갭으로 별도 fix 필요.

### Fixed

- **`PackedBackend.ListObjectsPage`** (`internal/storage/packblob/packed_backend.go`) — 신규 메서드. inner 페이저(있으면) 호출 → packed in-memory index에서 prefix+marker 매칭 entries 보충 → key 정렬 → marker/maxKeys 적용 + truncated flag. 기존 `ListObjects`와 동일한 supplementation 의미 유지.
- **회귀 테스트** `TestPackedBackend_ListObjectsPage` (`internal/storage/packblob/packed_backend_test.go`) — empty marker / prefix filter / marker resume / maxKeys truncation 네 시나리오.

### Notes

- 영향: SingleNode write→list 경로 (8 tests 중 3 회복: TestObjectsE2E, TestS3ClientSmoke, TestMigrationInjector). 나머지 5 tests (TestSnapshot/PITR/Backup/IAM Scoped Key/QuarantineIncident)는 restore-후-ObjectIndex 재활성화 갭 (별도 issue).
- DuckDB Iceberg `https://http://` 이중 스킴 (v0.0.255.0 SigV4 BREAKING의 부작용, 6 tests), Volume Scrub on-disk block 누락 (5 tests), Encryption/Versioning/NBD multi-node replication 등은 별도 follow-up.

## [0.0.257.2] - 2026-05-19 - test(reorg): binary-vs-in-process classification + per-protocol matrix

테스트 정리 PR (코드 변경 없음, test-only). e2e/integration/unit 경계 명확화 + S3 외 4개 protocol(iceberg/NFS/NBD/9p)에 single/cluster matrix 패턴 확장 + colima cluster mount 신규 커버리지.

### Changed

- **분류 정정 (rename, 5 files)**: in-process 컴포넌트만 결합하는 `internal/**/*_e2e_test.go`는 `bin/grainfs` 자식 프로세스 + 외부 wire client 기준으로 보면 integration. `internal/{nbd,nfs4server}/e2e_test.go`, `internal/nfs4server/nfs4_e2e_coverage_test.go`, `internal/server/sendfile_e2e_require_test.go`, `internal/raft/learner_promote_e2e_race_test.go` → `*_integration_test.go` rename (함수명은 git blame 보존을 위해 유지). `internal/server/sendfile_zerocopy_integration_test.go`는 동명 기존 파일과 충돌 회피용 정밀화.
- **misclassified file 역이동**: `tests/e2e/nfs4_largefile_test.go`는 `storage.NewLocalBackend` 직접 호출이라 binary 없음 → `internal/nfs4server/largefile_integration_test.go`로 이동. `skipIfShort` → `testing.Short()` 인라인 치환.
- **`getOrInitSharedCluster`에서 `DisableNBD: true` 제거** (`tests/e2e/target_test.go`). NBD가 S3 generic shared fixture에서도 가동 → `newSharedClusterNBDTarget`이 별도 cluster boot 없이 재사용.

### Added

- **Per-protocol matrix Target 인프라 (s3Target 패턴 확장)**:
  - `tests/e2e/iceberg_target_test.go` — `icebergTarget` + `newSingleNodeIcebergTarget*`/`newSharedClusterIcebergTarget*` (audit-enabled variants 포함). `runIcebergAuditCases`로 `TestAuditIcebergSingleDuckDB`/`TestAuditIcebergClusterDuckDB` 통합. `uniqueNamespace`로 per-case isolation.
  - `tests/e2e/nfs_target_test.go` — `nfsTarget` + factories. `uniqueExport`로 per-case bucket+export 격리. `listNfsExportsOnDataDir`로 dataDir-parameterized variant.
  - `tests/e2e/nbd_target_test.go` — `nbdTarget` + factories. NBD wire export name은 `"default"` 고정 (handshake 제약, `internal/nbd/handshake.go:36`).
  - `tests/e2e/shared_mrcluster_test.go` — `getOrInitSharedMRCluster` (iceberg + NFS 공용 *mrCluster). static-peer boot 후 `c.nodeCount = 3` + `c.stopped = true` 명시 (TestMain teardown까지 lifecycle 보존; 미설정 시 첫 caller t.Cleanup이 fixture 조기 종료).
- **NEW cluster coverage**: `tests/e2e/nfs_multi_export_bucket_delete_e2e_test.go` BucketDelete cases (이전 single-only)를 `runNFSExportCases` matrix로 승격. `tests/e2e/nbd_matrix_cases_test.go` ReadWriteRoundTrip 신설 (single + cluster).
- **Colima cluster mount 테스트** (`tests/colimafixture/` 신규 패키지 + 3 protocol):
  - `tests/colimafixture/cluster.go` — macOS host에 3-node grainfs cluster 부팅, 모든 protocol port를 `0.0.0.0`에 바인딩해서 colima VM이 `192.168.5.2:<port>`로 접근. `StartCluster(t, Options)` + `Stop()` public API. macOS-side `TestColimaClusterFixtureBoots`로 6초 boot 검증.
  - `tests/nfs4_colima/cluster_mount_test.go` — NFS4 mount → write → 3-node S3 visibility 검증 (12.4s PASS).
  - `tests/9p_colima/cluster_mount_test.go` — 9p mount → write → 각 노드 9p 재마운트 read-back 검증 (12.1s PASS).
  - `tests/nbd_colima/cluster_mount_test.go` — NBD write via node 0 → 각 노드 `__vol/default/` S3 ListObjectsV2 raft 복제 검증 (15.0s PASS). NBD read는 leader-only가 cluster contract — 기존 `TestE2E_MultiRaftSharding_NBDRoutesThroughCoordinator` 패턴 미러.
- **`testServerNFSPort`/`testServerNBDPort` 패키지 var 노출** (`tests/e2e/helpers_test.go`). 이전엔 TestMain inline `freePort()` 호출만 했음 → Target single fixture 재사용에 필요.

### Notes

- **MICRO bump** (test-only follow-up — `0.0.251.1` 패턴 답습).
- **Pre-existing 미해결**: `TestAuditIcebergSingleDuckDB`/`TestAuditIcebergClusterDuckDB`/`TestNFS4_Allocate`는 master에서도 fail (각각 #427/#428 audit 회귀, fallocate 회귀로 추정). 본 reorg 작업 무관.
- 운영 모델 명문화 (CONTEXT.md 후속 후보): server = macOS, mount client = colima VM. 모든 `*_colima` 디렉토리가 이 구조.

## [0.0.257.1] - 2026-05-19 - fix(storage): persist Parts on LocalBackend CompleteMultipartUpload

v0.0.257.0의 single-node (LocalBackend) follow-up. `CompleteMultipartUpload`이 완료 객체를 `Parts` 없이 저장해서, 이후 HeadObject (또는 프로세스 재시작) 시 part 레이아웃이 사라지고 `?partNumber=N`이 legacy single-PUT으로 degrade되던 문제 해소. cluster 경로는 이미 `PutObjectMetaCmd`로 Parts를 영속화했음 — 이제 single-node도 동일 동작.

### Fixed

- **`LocalBackend.CompleteMultipartUpload` Parts 영속화** (`internal/storage/multipart.go`) — 완료 객체에 `obj.Parts = partsCopy` 채워서 HeadObject가 part 레이아웃을 복원하도록. 암호화/평문 분기 공통 literal 경유.
- **`storage.fbs` Object schema** — `parts:[MultipartPartEntry]` + `MultipartPartEntry` table 추가 (`part_number`/`size`/`etag`). `make fbs` 재생성. 기존 레코드는 `PartsLength()==0`으로 읽혀 legacy single-PUT 동작 유지 (마이그레이션 불필요).
- **`codec.go` marshalObject/unmarshalObjectInto** — Parts vector encode/decode.

### Notes

- 회귀 테스트 `TestCompleteMultipartUploadPersistsParts` (`internal/storage/multipart_test.go`) — Complete 후 HeadObject로 `len(Parts)==2` + PartNumber/Size/ETag 일치 검증.

## [0.0.257.0] - 2026-05-19 - feat(s3): multipart ?partNumber=N (GET/HEAD) + cluster capability admin probe + ListObjects pagination hardening

`warp s3 multipart` 4-node cluster 통과율 0% → 99.99% (16/~200K errors는 follow-up). `?partNumber=N`을 GET/HEAD에서 honor하고, `multipart_listing_v1` capability ready를 admin UDS로 노출해서 bench warmup이 45s blind sleep 대신 active probe로 전환. ListObjects pagination은 forward/local-exec fallback의 marker silently truncate 결함을 잡고 V1/V2 응답 struct를 분리.

### Added

- **`storage.MultipartPartEntry` + `Object.Parts`** — multipart 객체의 part metadata (PartNumber/Size/ETag)를 cluster 영속화 전 경로에 추가. FlatBuffers schema (`ObjectMeta`/`PutObjectMetaCmd`/`MetaObjectIndexEntry`) parts vector + codec encode/decode + apply.go + buildObjectIndexEntry + objectIndexEntryToObject + 4× backend.go BadgerDB 읽기 사이트 + CompleteMultipartUpload (`ecObjectWriteResult.Parts`).
- **`GET/HEAD ?partNumber=N`** (`internal/server/object_part_range.go`, `object_api.go`, `object_head_api.go`) — 206 + `Content-Range` + `x-amz-mp-parts-count` + part ETag. `Range`+`partNumber` 동시 사용 시 400 `InvalidArgument`, N out-of-range 시 416 `InvalidPartNumber`. 비-multipart 객체는 N=1만 허용 (whole object). 0-byte part는 empty 206으로 직접 응답.
- **Admin UDS `GET /v1/cluster/capabilities`** (`internal/server/cluster_capabilities_api.go`) — peer→capability→ready JSON. `CapabilityGate.EvidenceSnapshot()` + `ClusterInfo.CapabilityEvidence()` + `RaftClusterInfo.WithCapabilityGate`. bench/운영툴/CI 모두 활용.
- **`bench_wait_capability_ready()`** (`benchmarks/lib/common.sh`) — admin sock unix-socket curl로 모든 노드가 capability ready될 때까지 polling. multipart workload warmup이 45s sleep 대신 평균 ~5–25s active probe.
- **`ListObjects` marker-aware native pagination** — `LocalBackend.ListObjectsPage` + `DistributedBackend.ListObjectsPage` (badger seek-after-marker, truncated flag). `ListObjectsArgs.marker` FBS field로 forward RPC plumb-through.
- **`ListObjectsV1` (marker)/`V2` (continuation-token base64) 페이지네이션 응답** (`internal/server/list_objects_api.go`, `bucket_xml.go`) — V1은 `<Marker/>` 항상, V2는 `<KeyCount>` 항상. `?continuation-token` base64 decode 실패 → 400 `InvalidArgument`. `max-keys=0` 허용, 음수/non-int → 400.
- **bench script optional pprof capture** (`BENCH_PPROF=1`) + `EXTRA_GRAINFS_SERVE_FLAGS` forward.

### Changed

- **`Operations.ListObjectsPage` fallback** non-pager 백엔드 + 비어있지 않은 marker 조합에서 silently truncate 대신 `UnsupportedOperationError` 반환. LocalBackend/DistributedBackend가 모두 pager 구현하므로 production 경로는 영향 없음.
- **`forward_receiver.handleListObjects`** marker 인자 처리 + receiver가 `maxKeys+1` 프로브로 coordinator의 `len > maxKeys` truncated 검출을 가능하게 함 (이전엔 forward 경로 IsTruncated 항상 false).
- **`HEAD ?partNumber=N`** 200 → 206 Partial Content (S3 spec 준수).
- **bench multipart warmup** fixed 45s sleep → active capability probe.

### Fixed

- **multipart capability gate readiness** (`internal/cluster/capability_gate.go` + bench script) — gossip 전파 30~45s 동안 spurious "rolling upgrade" 거부로 multipart workload 100% 실패하던 회귀 해소.
- **`ListObjects` 30% errors** — minio-go가 pagination 기대했으나 GrainFS가 single-page 응답으로 종료하던 회귀. V1+V2 응답 + meta-FSM 네이티브 pager로 0 errors at 291k obj/s.

### Performance

- 4-node cluster baseline 재측정 (`docs/reference/benchmarks.md`).

### Notes

- single-node (LocalBackend) multipart partNumber 경로는 `internal/storage` codec 미반영으로 동작 안 함 (follow-up). cluster 4-node 경로만 동작.
- warp multipart 잔여 16/~200K errors는 follow-up.

## [0.0.256.1] - 2026-05-19 - fix(cluster): retry follower propose during data-group election convergence

3-노드 cluster에서 비리더 노드로 들어온 첫 S3 PutObject가 500 "not the leader"로 떨어지던 회귀 수정. 갓 instantiate된 data-group raft가 첫 election 완료 전에 propose를 받으면 모든 peer가 ErrNotLeader 반환 → `b.propose` follower 분기가 peer 한 바퀴만 돌고 surface. iceberg metadata-object PUT을 follower로 보내는 e2e 2건 (`TestE2E_MultiRaftSharding_IcebergCatalogPointerAndMetadataObjectSplit`, `TestE2E_DynamicJoinServices_NodeCounts/3_nodes`)이 PR #427 이후 RED 상태였던 원인.

### Fixed

- **Follower propose path retries on `raft.ErrNotLeader` with bounded backoff** (`internal/cluster/backend.go`). `b.propose` follower 분기를 5s deadline / 50ms retry loop로 감싸 election 수렴을 기다림. 매 iteration에서 `b.node.IsLeader()` 재확인 (self가 winner가 될 수 있음) + `LeaderID()`가 알려져 있으면 그쪽으로만 forward (피tile peer round-robin 회피, 모를 땐 기존 fan-out fallback). 비-`ErrNotLeader` 에러는 try-all-peers 후 surface해서 transport 실패가 실제 리더 peer를 가리지 않도록 원본 의미 보존. 두 e2e 테스트 PASS 복구.

## [0.0.256.0] - 2026-05-19 - feat(iceberg)!: warp catalog-commits/mixed/sustained clean — caller-identity creds + concurrency hardening (BREAKING)

`warp iceberg` 3-subcommand 측정이 4-node cluster에서 strict gate (failed_requests≈0, p99<1s, max<3s) 통과. catalog-commits 11 240 ops × 0 errors, catalog-mixed 123 850 ops × 3 (0.0024%), sustained 3 500 ops × 1 (0.029%) — 잔존 에러는 모두 알려진 QUIC stream transient. `/v1/config` 자격증명 publish는 호출자 본인의 IAM 키만 반환하도록 변경 (privilege amplification 차단).

### BREAKING

- **`/v1/config`이 호출자 본인의 access/secret 키만 publish.** 이전엔 warehouse bucket에 RoleWrite 이상을 가진 *임의의* SA 키를 publish해서, RoleRead 호출자가 RoleAdmin 자격증명을 받아갈 수 있었음 (privilege amplification). 이제 SigV4로 식별된 호출자 본인의 키만 lookup해서 반환하며, 호출자가 warehouse bucket에 RoleRead 이상 권한 없으면 빈 `overrides` 반환. 호출자가 RoleRead 미만이면 iceberg-go가 ambient AWS chain fallback 후 `403 InvalidAccessKeyId` (fail-closed). 데이터 평면 access는 호출자가 catalog 평면에서 이미 authn한 권한과 동일.

### Added

- **`/v1/config` `s3.endpoint` scheme 미러:** `c.Request.Scheme()`을 반영해서 HTTPS 호출자가 HTTP로 downgrade되지 않음.
- **ENV-gated 진단 미들웨어:** `GRAINFS_ICEBERG_ACCESS_LOG=1`로 iceberg REST 호출 단위 zerolog access line (`method`/`path`/`status`/`elapsed_ms`) 활성화. atomic.Bool 분기로 비활성 시 zero alloc.
- **ENV-gated slow-commit 진단 scaffolding:** `GRAINFS_ICEBERG_COMMIT_TRACE_MS=<ms>`로 임계값 초과 commit에 대한 trace 활성화 (parse + boot wire, 실제 trace 로직은 후속).
- **Per-instance MetaCatalog requestID prefix:** `crypto/rand` 8-byte hex prefix. 4-node cluster에서 모든 노드가 동일한 "create-table-1" requestID를 생성해 waiter map collision으로 10s hang하던 버그 해결.
- **`internal/iam.LookupKey`** access_key 직접 lookup helper (caller-identity cred publish 경로용).
- **E2E stress repro:** `tests/e2e/iceberg_concurrent_commits_test.go` (16 goroutine × 100 commits × 4 tables, `GRAINFS_TEST_ICEBERG_STRESS=1` opt-in). 503 임계점 추적용 — spec §8 `iceberg-rare-quic-stream-local-cancel-under-load` follow-up과 페어링.

### Changed

- **Server-side bounded retry on `ErrCommitFailed` for unconditional commits:** `requirements` 비어있는 CommitTable에 한해 최대 5회 reload+retry. warp 1.5의 `IsConflictError`가 iceberg-go의 `CommitFailedException` 문자열을 매칭 못해 retry 안 하던 갭을 server-side에서 흡수. requirements 있으면 spec-compliant 409 그대로 surface.
- **409 응답 메시지 포맷:** `"table metadata pointer changed"` → `"409 Conflict: table metadata pointer changed"`. warp `IsConflictError`의 substring matcher가 "409"/"Conflict" 모두 인식하도록 bridge.
- **`MetaForwardDialer` 시그니처:** `(peer, payload)` → `(ctx, peer, payload)`. QUIC stream call이 호출자 ctx의 deadline/cancel을 존중하도록.
- **`MetaCatalog.readMetadata` bounded retry on `storage.ErrObjectNotFound`:** 5/15/35/75ms cumulative (~130ms worst case). 고동시성 CommitTable 직후 LoadTable에서 backend visibility race로 500 떨어지던 회귀를 catalog state 일관성 유지하며 흡수.
- **Follower `CreateTable` early-return with request metadata:** propose 후 즉시 LoadTable round-trip 대신, 요청 body의 metadata를 클라이언트에 반환. follower→leader propose→follower fetch round-trip race로 100k+ ops 중 hang 발생하던 패턴 해결.
- **Bench 스크립트 log level configurable:** `GRAINFS_LOG_LEVEL` env var, default `info` (기존 `warn`). 4-node cluster 디버깅 가시성 확보.

### Fixed

- **`storage.ErrObjectNotFound` cross-forward 분류:** meta-forward boundary에서 storage sentinel을 `service-unavailable` wire type으로 lossy하게 인코딩하던 버그. 새로운 `storage-not-found` wire type 추가. 결과: 503 flood (catalog 살아있는데도) → 500 (정상적 storage error) 또는 404 (NoSuchBucket).
- **503 응답 body에 wrapped error message embed:** `ErrServiceUnavailable`만 반환하던 곳에서 `err.Error()` full chain 포함. empty-peers vs all-peers-failed 구분 가능.
- **`io` import:** 새 retry loop에서 사용.

### Removed

- **`internal/iam.FirstActiveKeyForBucketGrant`** / **`FirstActiveKeyForSA`** 헬퍼: caller-identity 전환 후 사용처 없음. amplification 위험 코드 경로를 빌드에서 제거.

### Tests

- **`TestIcebergS3CredOverrides_CallerIdentity` (7 케이스):** RoleRead caller 본인 키 반환, RoleAdmin caller 본인 키 반환 (admin SA 키 아님), no-grant=empty, no-identity=empty, unknown-ak=empty, malformed-warehouse=empty, wildcard-grant=OK.
- **`TestIcebergConfigHandler_SchemeReflection`:** SigV4-signed GET `/iceberg/v1/config` end-to-end. `s3.endpoint`이 test server scheme (`http://`)을 미러하는지, caller-identity가 핸들러 경로까지 propagate되는지 검증. helper 단위 테스트가 못 잡는 scheme reflection 회귀 가드.
- **`TestRequestIDPerInstanceUnique`:** MetaCatalog 인스턴스 N개의 prefix가 distinct함을 검증.
- **`iceberg_diag_test.go`:** access log middleware의 ENV 분기/zero-alloc 검증.

### Docs

- **`docs/cluster`:** orphan sweep status 정정 — best-effort 처리, full sweep은 deferred. 잘못된 "production-ready full sweep" 표현 수정.

### Deferred (TODOS.md entry)

- **HTTP plaintext에서 `/v1/config` secret 노출:** `s3.secret-access-key`가 응답 JSON에 평문으로 들어가므로 HTTP catalog 호출 시 secret이 와이어로 노출. branch가 도입한 회귀는 아니나 (pre-Option-B에서는 admin SA secret 누출, 이제는 호출자 본인 키), reopen 조건과 3 옵션 (TLS gate / docs / `--iceberg-allow-http-creds`)을 TODOS.md `## Deferred Until Triggered`에 기록.
- **QUIC `local cancel error code 1` transient:** catalog-mixed/sustained의 잔존 0.002~0.029% 에러. transport-layer 별도 audit (spec §8 `iceberg-rare-quic-stream-local-cancel-under-load`).

## [0.0.255.0] - 2026-05-19 - feat(iceberg)!: SigV4 required on REST Catalog (BREAKING)

Iceberg REST Catalog now shares the S3 SigV4 trust boundary. Every endpoint
under `/iceberg/v1/*` and `/_iceberg/v1/*` — including `GET /iceberg/v1/config`
— requires SigV4 signed by a bootstrapped ServiceAccount's
`access_key`/`secret_key`. Anonymous catalog discovery is no longer available.

### BREAKING

- **Iceberg REST Catalog requires SigV4** on every endpoint (`/iceberg/v1/*`,
  `/_iceberg/v1/*`). Clients must configure
  `rest.sigv4-enabled=true`, `rest.signing-name=s3`,
  `rest.signing-region=us-east-1` (or the cluster's configured region) and
  supply a bootstrapped ServiceAccount's `access_key`/`secret_key`. Anonymous
  catalog discovery via `/v1/config` is no longer available.
  DuckDB iceberg extension users must bump to v1.5.2+ and switch
  `AUTHORIZATION_TYPE 'none'` to `'sigv4'`. See
  `docs/users/iceberg-duckdb.md` for the migration. Spec:
  `docs/superpowers/specs/2026-05-19-iceberg-rest-auth-design.md`.

## [0.0.254.0] - 2026-05-19 - feat(scrubber): production orphan raw-segment sweep

AppendObject가 남기는 raw segment 파일의 production-grade orphan cleanup. 기존 EC shard용 `OrphanWalkable`는 변경 없이, 새로운 optional `OrphanSegmentWalkable` 인터페이스 + `DistributedBackend` production impl 추가. AppendObject best-effort cleanup이 실패해도 scrubber cycle 2회 안에 디스크에서 자동 회수.

### Added

- **`OrphanSegmentWalkable` 인터페이스** (`internal/scrubber/orphan_segment.go`): scrubber의 optional 확장. EC shard용 `OrphanWalkable`와 독립적으로 raw segment lifecycle 관리.
- **`AppendableScannable` 인터페이스** + `AppendableRecord{Bucket, Key, SegmentBlobIDs}` 타입 (`internal/scrubber/scrubber.go`): metadata 인덱스에서 IsAppendable 객체를 streaming하여 known-segment set 구축. `Scrubbable.ScanObjects`의 EC-only 의미 보존.
- **`DistributedBackend.WalkOrphanSegments` + `DeleteOrphanSegment`** production impl (`internal/cluster/orphan_segment_walker.go`): `<root>/data/<bucket>/<key>_segments/<blobID>` 경로의 disk walker. `filepath.WalkDir` 기반 재귀로 nested S3 key (`folder/sub/file`) 완전 커버. Bucket ENOENT race, 권한 거부, partial-unlink 모두 graceful 처리.
- **`DistributedBackend.ScanAppendableObjects`** production impl (`internal/cluster/scan_appendable.go`): `lat:` 인덱스 iteration, IsAppendable filter, SegmentBlobIDs 채워서 yield. `deleteMarkerETag` tombstone skip.
- **`segmentSweepBucket` per-bucket orchestration**: 2-cycle tombstone gate + cycle-shared cap 50 + 5분 age gate. `s.segmentTombstone` cluster-wide map (기존 `s.orphanTombstone`와 parallel).
- **CLI flag `--scrub-orphan-age <duration>`** (default `5m`): age gate 운영자 조정. Long-running large appends가 5분 초과 시 안전 마진 확보.
- **5 신규 Prometheus counters:** `grainfs_scrub_orphan_segments_found_total`, `grainfs_scrub_orphan_segments_deleted_total`, `grainfs_scrub_orphan_segment_sweep_capped_total`, `grainfs_scrub_orphan_segment_walk_errors_total`, `grainfs_scrub_orphan_segment_delete_errors_total`.
- **Test coverage:** 5 scrubber unit tests (Tombstone/AgeGate/Cap/RecoveredBetweenCycles/CapAcrossBuckets) + 5 walker unit tests (Production/NestedKey/BucketENOENT/Delete/ErrorPaths) + 4 ScanAppendable tests + 1 e2e test (`TestOrphanSegmentSweepE2E_Cluster4Node`, 4-node cluster, 4.73s).

### Changed

- **Scrubber main loop**: per-bucket segment sweep을 기존 EC sweep 다음 위치에 추가. 두 메커니즘은 완전 독립 (state, cap, tombstone 모두 분리). 기존 `OrphanWalkable.WalkOrphanShards` 호출 위치 / 시그니처 변경 없음.

### Operations

- **EC shard orphan cleanup은 별도 follow-up** (`TODOS.md` P2). coalesce 도중 EC 쓰기 후 propose 실패로 남는 shard dir (`<shardRoot>/<bucket>/<userKey>/coalesced/<id>/coalesced/<id>/shard_<i>`)은 기존 `OrphanWalkable.WalkOrphanShards`가 plain EC만 cover하는 한계 때문에 이번 PR 범위 외. storage layout 조사 + tracking mechanism 확장 후 별도 cycle에서 처리.

## [0.0.253.0] - 2026-05-19 - feat(s3): AppendObject hardening — size cap + memory budget + owner-kill e2e

AppendObject (v0.0.249.0)을 production-readiness 단계로 hardening. F1-F5 묶음으로 5개 follow-up을 단일 PR로 처리.

### Added

- **Per-object size cap** (`storage.ErrAppendObjectTooLarge`, default 5 TiB matching S3 PutObject parity). FSM-side authoritative check in `applyAppendObjectFromCmd` + coordinator pre-check fast-reject (false-negative forbidden tolerance contract). CLI: `--append-size-cap-bytes`. ForwardStatus enum value `AppendObjectTooLarge = 11`. HTTP 400 EntityTooLarge.
- **Forward-buffer byte-based semaphore** (`cluster.appendForwardBuffer`, default 512 MiB pool). Replaces unbounded body buffering for non-owner → owner AppendObject forwards. Saturation surfaces as HTTP 503 SlowDown with `Retry-After: 1`. CLI: `--cluster-append-forward-buffer-{total-bytes,max-per-request}-bytes`.
- **6 new Prometheus metrics:** `grainfs_cluster_append_forward_buffer_inflight_bytes` (Gauge), `grainfs_cluster_append_forward_buffer_rejected_total` (Counter), `grainfs_append_coalesced_depth` / `grainfs_append_coalesced_total_bytes` (Histograms), `grainfs_append_size_cap_rejected_total` / `grainfs_append_coalesced_entries_at_cap_total` (Counters).
- **e2e fault-injection harness:** `e2eCluster.KillNode(i)`, `e2eCluster.RestartNode(t, i)`, `e2eCluster.AwaitWriteFromNonOwner(bucket, key, deadline)` (uses `__grainfs_probe` internal namespace).
- **e2e coverage:** `TestAppendMidSizeBodyE2E` (8 MiB body proves 64 MiB cap), `TestAppendForwardBufferSaturationE2E` (concurrent forwards trigger 503), `TestAppendSizeCapE2E` (RejectAtCap + ConcurrentRaceAtCap), `TestAppendObjectE2E/Cluster4Node/OwnerKillSurvives` (real raft leader rotation + EC reconstruct).

### Changed

- **`DefaultMaxForwardBodyBytes` raised 5 MiB → 64 MiB** (matches HTTP-layer `appendBodyMaxBytes`). 5 MiB-64 MiB chunks now flow through forward path without stale-placement retry being severed.
- **`DistributedBackend.coalesceCfg` is now `atomic.Pointer[CoalesceConfig]`** (was plain struct). Closes a latent data race between `coalesceBackstopScan` goroutine and `SetCoalesceConfig` callers. Test setups migrated to `SetCoalesceConfig` (no direct field assignment).
- **`bootState.instantiateGroupWithConfig` helper** bundles `cluster.InstantiateLocalGroup` + `gb.SetCoalesceConfig(state.coalesceCfg)`. Compile-time guarantee: future per-group config flags reach every group, including dynamically-instantiated shard groups. Fixes a wiring bug where groups 1-N silently inherited the default 5 TiB cap regardless of `--append-size-cap-bytes`.
- **e2e fixture consolidation:** `appendTarget` removed in favor of `s3Target` (now carries `cluster *e2eCluster` field). `runCommonAppendCases`/`runClusterOnlyAppendCases` take `s3Target` directly. `TestAppendObjectCoalesceE2E_Cluster4Node` renamed to `TestAppendCoalesceE2E`.

### Fixed

- **`TestCoalesceMetricsObserved` flake:** `metrics.AppendCoalesceTotal.Inc()` runs in a `defer` block in `coalesce.go:158` — after `obj.Coalesced` becomes visible to the test's `Eventually`. Test now wraps the counter read in `Eventually` too.

### Operations

- Calibration follow-up: `warp append --concurrent 32 --duration 60s --obj.size '1-16MiB'` rejection ratio < 1% for default 512 MiB pool. Deferred to operator validation post-ship (TODOS.md).

## [0.0.252.0] - 2026-05-19 - chore: drop legacy JSON guards from FB decoders

Wipe-and-restart is the only supported upgrade path (see v0.0.251.0 CHANGELOG),
and pre-FlatBuffers JSON bytes will not appear in storage or on the wire after
upgrade. The diagnostic `'{'` legacy-byte guards in 8 FB decoders were dead
defense:

- 4 storage decoders — packblob `decodeIndexStorage`, cluster
  `decodePutObjectQuarantineCmdStorage`, `receipt.DecodeReceiptStorage`,
  `eventstore.decodeEventStorage`.
- 4 RPC decoders — `decodeMetaCatalogReadRequest`,
  `decodeMetaLoadTableReply`, `decodeJoinRequest`, `decodeJoinReply`.

Removed all 8 guards plus the four per-package `ErrLegacyStorageFormat`
sentinels (packblob, cluster, receipt, eventstore) and the eight
`Test*RejectsLegacyJSON` / `Test*LegacyJSONRejected` tests that exercised
them. defer-recover already catches malformed-FB panics — the legacy guard
only added a separate error message for a class of bytes that cannot exist
in supported deployments.

Closes Task #19 (PR #413 meta_forward reply legacy guard review — answer:
guard removed entirely, not strengthened).

## [0.0.251.1] - 2026-05-19 - test: e2e consolidation — shared cluster fixture + integration rename

- Add `tgt.uniqueBucket(t, "case")` helper to `s3Target`: derives a S3-spec
  bucket name from `t.Name()`+case (sanitize → 50-char SHA8 fallback) and
  registers auto-cleanup. Prevents bucket-name collisions now that the
  cluster fixture is process-global.
- Promote `tests/e2e/` cluster fixture to a process-global shared instance
  via `sync.Once` lazy boot. First cluster-target test triggers boot;
  TestMain teardown calls `stopSharedCluster`. `-short` skips boot
  automatically (cluster-target tests guarded by `skipIfShort`). Migrates
  `TestBucketsE2E`, `TestS3Multipart*`, `TestS3Presigned*`, `TestS3Objects*`
  callers; drops the per-test `newClusterS3Target(t, 4)` helper. CI time
  for adding new S3-domain e2e tests scales sub-linearly.
- Add `TestS3VersioningE2E` (cluster-only, 2 cases: `PutGet`,
  `GetByVersionID`) under SDK. Drops the equivalent `_EC` cases from
  `internal/server/versioning_test.go`. The other 3 `_EC` cases stay in
  internal — the 4-node cluster's `ListObjectVersions` returns an extra
  "null" version per `PutObject`, semantically different from the
  in-process EC fixture, so cluster-fixture SDK assertions don't match.
- Drop `TestAppendableObjectOverwriteByPlainPut` from
  `internal/server/object_append_test.go` — the SDK equivalent already
  exists as `TestAppendObjectE2E/{SingleNode,Cluster4Node}/PlainPutOverwritesAppendable`
  in `tests/e2e/append_object_test.go`.
- Rename `internal/*/e2e_test.go` (5 files) → `*_integration_test.go`:
  `internal/cluster/{ring,meta_raft,meta_raft_mux}`,
  `internal/server/acl`, `internal/storage/packblob/compression`.
  These tests wire up a single subsystem in-process — they were never
  end-to-end. Content unchanged.
- `tests/e2e/append_object_test.go` (own `appendTarget` abstraction with
  distinct `ClusterKey: "E2E-APPEND-KEY"`) is intentionally NOT migrated
  to the shared cluster. Out of scope for this PR.

Operator impact: none (test-only change, production code unchanged).
Developer impact: `make test-e2e` cluster boot amortizes across S3 domains
(was per-test 30s+); new S3-domain e2e tests follow the
`runVersioningCases(t, tgt)` matrix pattern in `tests/e2e/versioning_test.go`.

## [0.0.251.0] - 2026-05-19 - feat: internal storage v2 (FlatBuffers) (BREAKING)

- BREAKING: internal storage format v2 (FlatBuffers) for quarantine, receipt,
  eventstore badger values, and packblob index.
  Upgrade procedure:
    1. Stop cluster.
    2. WIPE `<data>/raft/` and `<data>/meta/` ONLY.
    3. PRESERVE packblob `*.blob` files (contain user object data).
       Optionally delete legacy `<data>/<packblob_dir>/index.json`
       (ignored by new binary).
    4. Restart. packblob `index.bin` rebuilds automatically from blob scan.
- eventstore.Event drops the `User` field and the polymorphic
  `map[string]any` `Metadata` field. The 12 audit keys previously stored
  under `Metadata` are promoted to typed top-level fields: `id`, `phase`,
  `outcome`, `shard_id`, `peer_id`, `bytes_repaired`, `duration_ms`,
  `err_code`, `correlation_id`, `version_id`, `removed_id`, `force`.
  Wire format: top-level keys (e.g. `event.phase` instead of
  `event.metadata.phase`).
- LookupReceiptJSON renamed to LookupReceipt (returns *HealReceipt). HTTP API
  re-marshals to JSON at the boundary; intra-cluster broadcast encodes FB.
- Wire field ReceiptQueryResponseMsg.receipt_json_bytes renamed to receipt_bytes
  (FB Go accessor: ReceiptBytes()).
- Internal RPC remains FlatBuffers (PR #406, #416 unchanged).

## [0.0.250.1] - 2026-05-19 - chore(bench): warp iceberg benchmark scaffolding (catalog-read + catalog-commits)

Adds a per-subcommand wrapper around `bench_iceberg_table_cluster.sh` and the
first two warp iceberg result reports for the 3-node cluster topology. No
production code changes — bench data and tooling only. Used by the follow-up
investigation into Iceberg REST commit latency under contention.

### Added

- `benchmarks/run_iceberg_warp.sh`: wrapper that injects `ICEBERG_WARP_COMMAND`,
  `DURATION` (30s for read/commits/mixed, 2m for sustained), and a per-run
  `PROFILE_ROOT` so the four warp iceberg subcommands write isolated profile
  artifacts.
- `benchmarks/iceberg_warp_catalog-read_report.json`: clean run summary
  (3 nodes, 27s, concurrency=10) — `failed_requests=0`, total ~4013 ops/s,
  NS_* ~669 ops/s @ p99 0.7ms, TABLE_* ~669 ops/s @ p99 ~11.7ms.
- `benchmarks/iceberg_warp_catalog-commits_report.json`: dirty run summary
  documenting 165 errors / 1988 ops on TABLE_UPDATE with p99=2549ms,
  slowest=10026ms (warp client timeout). Most errors are spec-compliant
  `409 CommitFailedException` for optimistic-concurrency conflicts that warp
  does not retry; the 10s tail indicates server-side commit-path latency
  worth tracing.

### Notes

- catalog-mixed and sustained are intentionally deferred — same root-cause
  cluster commit contention is highly likely; they re-open after the commit-
  path investigation in the linked design spec.
- One pre-existing flaky test (`TestCoalesceMetricsObserved` in
  `internal/cluster`) failed under the full parallel suite during ship
  verification but passes when run alone. Unrelated to this PR.

## [0.0.250.0] - 2026-05-19 - perf(nbd): block-range pending mutation queue

NBD write-back flush now orders deferred Raft commits by affected volume block.
Writes touching the same block flush in append order even when request offsets
differ, while writes to distinct blocks can still commit concurrently.

### Added

- Private `mutationQueue` for each NBD connection, with block-range wave
  scheduling, queue clearing on flush, and best-effort disconnect drain.
- Unit coverage for same-block serialization, distinct-block parallelism,
  configured block sizes, transitive overlaps, flush error clearing, drain
  behavior, and copied commit function slices.
- NBD wire-level smoke coverage proving `FLUSH` runs deferred same-block commit
  functions after different-offset writes.
- Mutation queue benchmarks for distinct-block and same-block flush workloads.

### Changed

- `WRITE`, `WRITE_ZEROES`, `FLUSH`, and pre-`TRIM` paths now use the
  per-connection mutation queue instead of an offset-keyed pending slice.
- `WRITE_ZEROES` records successful chunk commits as one request-range mutation,
  so its flush ordering follows the original command range.
- NBD architecture context now documents pending mutation queue ownership and
  block-level ordering semantics.

## [0.0.249.0] - 2026-05-18 - feat(s3): AppendObject API (Phase A + B1 + B2 + B3)

S3 Express AppendObject (`x-amz-write-offset-bytes`)를 single-node와 4-node
cluster 양쪽에서 지원. Sequential append + range read + cluster-wide durability
via lazy EC 분산. 4-digit version에 큰 surface이지만 patch bump 유지 (기존
repo 패턴).

### Added

- **HTTP entry point.** `PUT /{bucket}/{key}` + `x-amz-write-offset-bytes: <N>`
  헤더로 sequential append. Versioning-enabled bucket은 `501 NotImplemented`,
  잘못된 offset은 `400 InvalidWriteOffset` XML, segment cap 도달은
  `503 SlowDown` + `Retry-After`. 64 MiB body cap (HTTP layer).
- **Storage layer.** `storage.Object`에 `Segments []SegmentRef` +
  `IsAppendable bool` + `Coalesced []CoalescedRef`. `WriteSegmentBlob`,
  `CompositeETag`, `SegmentedReader` (full-stitch + range across segments)
  + encrypted-segment tamper detection.
- **Cluster FSM.** 새 명령 `CmdAppendObject` (B2) + `CmdCoalesceSegments`
  (B2/B3). AppendObject가 propose-time에 UUIDv7 VersionID 생성 후 legacy
  + versioned + latest pointer 3-key write.
- **Phase A 인프라.** Data-Raft generic apply-error propagation
  (`applyErrs` map + `recordApplyResult` + `ApplyError` exported). Forward
  response codec 확장 (1-byte trailing wire + backward compatible).
- **Phase B1 forward-on-read.** `StreamReadAppendSegment` (0x15) transport
  + `appendableSegmentReader` ENOENT fallback peer fetch.
- **Phase B2 coalesce.** Background worker queue + in-process trigger
  (16 segments / 64 MiB / 30s idle / 60s backstop) + snapshot-based atomic
  apply (concurrent append과의 race 단순화) + idempotent
  `applyCoalesceSegments`.
- **Phase B3 lazy EC.** Coalesced blob을 Reed-Solomon 4+2 EC로 분산
  (`PutObject` 패턴 재사용: `ecObjectShardKey`, `selectECPlacement`,
  `newECObjectWriter.writeDataShards`). shardKey = `<key>/coalesced/<id>`.
  `appendableReader` 확장 — coalesced (EC reconstruct) + raw (forward-on-read)
  chain stitching. Range read는 prefix-sum + binary search across boundaries.
  Encryption은 PutObject EC와 동일 encryptor 적용.
- **Metrics.** `grainfs_append_coalesce_total{result}`,
  `grainfs_append_coalesce_bytes`, `grainfs_append_coalesce_latency_seconds`,
  `grainfs_append_segments_{raw,coalesced}` (gauge),
  `grainfs_append_forward_on_read_total`.

### Changed

- **Forward reply codec.** `ForwardStatus` enum에 typed append errors
  추가 (`AppendOffsetMismatch`, `AppendNotSupported`, `AppendCapExceeded`).
  cluster forward path가 storage sentinel을 그대로 client까지 전달.
- **DistributedBackend.GetObject.** Appendable branch가 segment / coalesced
  / raw 통합 reader 호출.
- **objectMeta 3-key write.** AppendObject + CoalesceSegments가 legacy
  `ObjectMetaKey` + versioned `ObjectMetaKeyV` + `LatestKey` pointer 모두
  업데이트하여 `HeadObject` (latest pointer 따라감)와 일관.
- **wrapper chain wiring.** Single-node 데이터 plane (`pullthrough → wal →
  packblob → ClusterCoordinator`)에 AppendObject delegate 추가.

### Tests

- **Storage layer.** OffsetMismatch / Sequential / Cap / Legacy
  non-appendable / SegmentedReader full + range + encrypted tamper.
- **Cluster FSM.** AppendObject apply idempotency + concurrent race
  + ApplyError propagation + objectIndex sync.
- **HTTP layer.** Invalid header (400 InvalidArgument) + InvalidWriteOffset
  XML + versioning 501 + plain-PUT overwrite.
- **e2e 통합 (target table-driven).** `TestAppendObjectE2E` (SingleNode + 
  Cluster4Node 공통 4 케이스 + cluster-only 2 케이스). 기존
  `TestBucketsE2E / TestObjectsE2E / TestMultipartE2E / TestPresignedE2E`도
  같은 패턴으로 통합 — 29 case × 2 target = 58 PASS, 중복 제거.
- **Coalesce e2e.** `TestAppendObjectCoalesceE2E_Cluster4Node` — coalesce
  trigger → EC distribute → cross-node read. 
- **Unit tests.** Owner-local file 삭제 시 EC reconstruct
  (`TestCoalescedReadAfterOwnerFailure`) + crash recovery
  (`TestCoalesceRecoveryOnRestart`) + encryption-enabled coalesce verify.

### Known issues / follow-ups (TODOS.md 등록)

- **Owner-kill real raft leader rotation e2e [P1]** — Phase B3 omnibus는
  owner-local file 삭제로 EC reconstruct path만 unit 수준 검증.
  multi-node real raft leader rotation 추가 e2e 필요.
- **Coalesce recoalesce depth audit [P2]** — `MaxCoalescedEntries=1024` cap
  외 measurement-driven 정책 (max depth, periodic 통합).
- **5 MiB body cap 정합성 [P2]** — HTTP layer 64 MiB vs ClusterCoordinator
  `maxBody=5 MiB` retry buffer 사이 불일치. forward retry 단념 시 typed
  error 또는 maxBody 64 MiB로 정합화.
- **`TestCoalesceMetricsObserved` flake [P2]** — concurrent test 환경에서
  간헐적 fail (isolated 실행 시 PASS). metric counter race 의심, 별도
  안정화 필요.

## [0.0.248.0] - 2026-05-18 - perf(cluster): reduce forwarded ReadAt allocations

Forwarded `ReadAt` replies now parse directly into the caller buffer on the
coordinator side, and follower-side small `ReadAt` buffers reuse zeroed size
classes instead of allocating a fresh exact-size byte slice for every request.

### Changed

- `internal/cluster/forward_codec.go`: centralized `ReadAt` reply parsing in
  `readAtReplyInto`, including malformed FlatBuffers recovery and oversized
  reply body rejection.
- `internal/cluster/cluster_coordinator.go`: small forwarded `ReadAt` calls now
  copy reply bytes directly into the caller-owned destination buffer.
- `internal/cluster/forward_receiver.go`: pooled 4 KiB / 16 KiB / 64 KiB
  follower read buffers for forwarded `ReadAt` requests; larger requests remain
  unpooled.
- `internal/cluster/forward_sender.go`: malformed not-leader FlatBuffers replies
  no longer panic while extracting leader hints.

### Performance - Forwarded ReadAt 4 KiB (Apple M3, count=6, benchtime=5x)

| Metric | Before | After | Change |
|---|---:|---:|---:|
| B/op | 12,742 | 8,576-8,662 | ~32% lower |
| allocs/op | 63 | 62 | -1 alloc/op |

Latency remains noisy at this short benchtime, so the measured win claimed here
is allocation reduction rather than stable wall-clock improvement.

### Tests

- Added forwarded `ReadAt` benchmarks covering coordinator and receiver paths.
- Added tests for direct reply parsing, short/oversized/malformed reply bodies,
  receiver buffer size classes and zeroing, backend error handling, stream
  cutoffs, and malformed not-leader leader hints.

## [0.0.247.0] - 2026-05-18 - perf(cluster): internal RPC JSON → FlatBuffers (catalog_read + join)

Converts the last two cluster-internal RPC paths still on `encoding/json`
to FlatBuffers, mirroring the PR #413 meta_forward pattern. Closes the
"no internal JSON" rule for in-cluster network RPC.

### Changed

- `internal/cluster/meta_forward.go`: `MetaCatalogReadSender/Receiver`
  (iceberg catalog read RPC — LoadNamespace / ListNamespaces / LoadTable /
  ListTables) now encodes with FlatBuffers. Wire format prefixes
  `GFSMCR2` on requests; replies are bare FB. Legacy JSON shape (`{`)
  rejected on both request and reply decoders with a typed
  `ErrServiceUnavailable + mixed-version` error.
- `internal/cluster/meta_join.go`: cluster join handshake
  (`MetaJoinSender/Receiver`) now encodes with FlatBuffers. Wire prefix
  `GFSMJN2` on requests. Same legacy-JSON guard pattern.
- `internal/cluster/clusterpb/cluster.fbs`: schemas for `JoinStatus`,
  `JoinRequest`, `JoinReply`, `CatalogReadOp`, `CatalogKV`,
  `CatalogNamespace`, `CatalogIdentifier`, `CatalogTable`,
  `MetaCatalogReadRequest`, `MetaCatalogReadReply`. `CatalogTable` is
  carried in `MetaCatalogReadReply.loaded_table` (not `table`) to avoid
  colliding with FB Go's built-in `Table()` accessor.
- `encoding/json` removed from both files; no remaining JSON encode in
  cluster-internal RPC paths.

### Performance — MetaCatalogRead (Apple M3, benchstat count=6 / 15s)

| Sub-bench | sec/op Δ | allocs/op Δ |
|---|---|---|
| Request/load-namespace | −86.4% | −71.4% |
| Request/load-table | −79.1% | −62.5% |
| Request/list-tables-1k | −87.7% | −75.0% |
| Reply/load-namespace | −58.2% | −59.6% |
| Reply/load-table-64KB | −97.5% | −44.4%¹ |
| Reply/list-tables-1k | −78.8% | −0.6%² |
| **geomean** | **−85.9%** | **−57.4%** |

¹ Marginal alloc miss vs strict 50% gate; throughput dominates.
² Alloc cost dominated by callee-side `[]Identifier{Namespace: []string{…}}` construction, unaffected by wire format. Speed-up still −78.8%.

p-value 0.002 across all six sub-benches.

### Performance — MetaJoin (cold path, alloc snapshot only)

- BenchmarkMetaJoinRequest_RoundTrip: ~123 ns/op, 88 B/op, 3 allocs/op
- BenchmarkMetaJoinReply_RoundTrip/ok: ~198 ns/op, 200 B/op, 4 allocs/op
- BenchmarkMetaJoinReply_RoundTrip/not-leader: ~196 ns/op, 216 B/op, 5 allocs/op

### Tests

- 11 new tests covering MetaCatalogRead round-trip (every op + every
  reply shape), 64KB Iceberg metadata byte fidelity, every iceberg
  error symbol round-trips via `errors.Is`, legacy JSON shape rejection
  on both decoders, malformed FB panic recovery, and `CatalogReadOp`
  drift guard.
- 6 new tests covering MetaJoin equivalents (every JoinStatus, legacy
  reject, malformed FB, drift guard). All 6 pre-existing MetaJoin tests
  still pass against the new FB encoders — proof the helpers are
  drop-in compatible.
## [0.0.246.0] - 2026-05-18 - perf(nfs4): range-read COPY source data

NFSv4.2 `COPY` now reads only the requested source range instead of buffering
the whole source object before slicing. Counted copies use `ReadAt` when the
backend advertises it, and the fallback path streams only the needed
`srcOffset+count` bytes.

### Added

- Added targeted COPY coverage for bounded fallback reads, `ReadAt` fast-path
  reads, copy-to-EOF/count-clamp semantics, EOF/huge-offset zero-byte copies,
  oversized copy rejection, destination offset overflow, and source read error
  mapping.

### Fixed

- Oversized COPY source ranges now return `NFS4ERR_FBIG` before reading source
  data, avoiding full-object buffering and truncated success on requests larger
  than the object RMW cap.
- Destination offset arithmetic is checked before writing so overflow returns
  `NFS4ERR_FBIG` instead of wrapping.

### Performance

Benchstat (`-benchtime=5x -count=6`, Apple M3, 4 KiB COPY):

| Source size | sec/op delta | B/op delta | allocs/op delta |
|---|---:|---:|---:|
| 16 MiB | 8523.3 µs → 470.1 µs (−94.48 %) | 35401.65 KiB → 29.09 KiB (−99.92 %) | 163.0 → 128.5 (−21.17 %) |
| 64 MiB | 22409.5 µs → 448.0 µs (−98.00 %) | 161502.00 KiB → 29.11 KiB (−99.98 %) | 257.5 → 129.0 (−49.90 %) |

## [0.0.245.0] - 2026-05-18 - chore: lint cleanup and CopyObject error propagation fix

Made `make build` depend on `make lint` so dead code, unused declarations, and
gosimple findings surface during normal builds instead of only in CI. Cleared
the existing lint backlog, and fixed a swallowed-error bug in the streaming
CopyObject fallback uncovered while running lint.

### Added

- `make build` now runs `make lint` first; `golangci-lint` is required in any
  environment that compiles GrainFS (noted in README + CLAUDE.md).
- Regression test asserting `Operations.CopyObject` propagates
  `putObjectWithRequest` errors through `errors.Is` on the streaming fallback
  path.

### Changed

- `internal/storage/codec.go`: moved test-only `unmarshalObject` wrapper into
  `codec_test.go`; production code uses `unmarshalObjectInto` exclusively.
- `internal/raft/quic_rpc_codec.go`: dropped the test-only
  `encodeAppendEntriesArgs` wrapper; heartbeat coalescer tests call
  `encodeRPCPayload` directly.
- `internal/server/delete_objects_api.go`: replaced
  `deleteObjectsDeleted{Key: obj.Key}` with `deleteObjectsDeleted(obj)`
  (gosimple S1016).

### Fixed

- `Operations.CopyObject` streaming fallback now returns errors from
  `putObjectWithRequest` instead of silently overwriting them with
  `mutationObjectFacts` failures.

### Removed

- Dropped unused `readEncryptedObjectRecord` wrapper in
  `internal/storage/encrypted_object_file.go`; only the buffer-reusing
  `readEncryptedObjectRecordInto` variant remains.
- Dropped 7 unused Iceberg route path constants from
  `internal/server/route_paths.go`.

## [0.0.244.0] - 2026-05-18 - perf(cluster): meta_forward JSON → FlatBuffers (GFSMFWD2)

Cluster-internal meta-Raft proposal forwarding now uses FlatBuffers instead of
JSON on the wire, cutting allocations on every forwarded RPC and lifting the
throughput ceiling that JSON parsing imposed on large commands. Closes a
[[feedback_no_internal_json]] policy gap that meta_forward was the last holdout
for; `MetaCatalogReadSender` in the same file is still on JSON and tracked
separately.

### Added

- New FB schema in `internal/cluster/clusterpb/cluster.fbs`: `CompatScope`,
  `CompatSeverity`, `CompatOperation` enums + `StaleNode`, `CompatGatePlan`,
  `MetaForwardRequest`, `MetaForwardReply` tables.
- 10 unit tests covering nil/full plan round-trip, unframed passthrough,
  legacy `GFSMFWD1` magic explicit rejection, malformed FB recovery, every
  reply error-type discriminator, unknown error-type fallback, enum converter
  round-trip, and an enum drift guard that fails when a new `compat.Scope` /
  `Severity` / `Operation` constant lands without a matching FB enum entry.
- `BenchmarkMetaForward` round-trip microbench (3 request sizes + 2 reply
  shapes) for ongoing regression measurement.

### Changed

- `encodeMetaForwardRequest` / `decodeMetaForwardRequest` and
  `encodeMetaForwardReplyWithIndex` / `decodeMetaForwardReplyWithIndex` now
  build/parse FlatBuffers through a pooled `flatbuffers.Builder` instead of
  marshaling/unmarshaling JSON. External function signatures are unchanged;
  no caller in `internal/serveruntime/boot_phases_forwarders.go` needs to
  change.
- Request wire magic bumped `GFSMFWD1` → `GFSMFWD2`. The decoder explicitly
  detects the legacy `GFSMFWD1` prefix and returns a clear
  `ErrServiceUnavailable`-wrapped error so mixed-version clusters fail loudly
  rather than silently passing JSON bytes through the raw-command fallback.

### Performance

Benchstat (`-benchtime=15s -count=6`, Apple M3, all metrics `p=0.002 n=6`):

| Path | sec/op delta | B/op delta | allocs/op delta |
|---|---:|---:|---:|
| Request 256B   | −92.28 % | −59.28 % | 9 → 2 (−77.78 %) |
| Request 4 KB   | −96.46 % | −48.59 % | 9 → 2 (−77.78 %) |
| Request 64 KB  | −97.11 % | −45.18 % | 9 → 2 (−77.78 %) |
| Reply success  | −83.48 % | −92.84 % | 7 → 1 (−85.71 %) |
| Reply error    | −78.95 % | −73.21 % | 10 → 3 (−70.00 %) |

64 KB Command round-trip throughput jumps from 147 MB/s to 5.1 GB/s.
## [0.0.243.0] - 2026-05-18 - perf(cluster): spool EC conversion writes

EC conversion now migrates legacy full-object replicas through the spooled EC
shard writer instead of reading the whole object into memory, preserving object
metadata while avoiding full-buffer split/encode during conversion.

### Added

- Added regression coverage for legacy full-object conversion through the
  spooled EC shard encoder.
- Added coverage for conversion metadata CAS and pre-commit abort cleanup on
  parity EC and single-local EC write paths.

### Changed

- `ConvertObjectToEC` now spools the source object and reuses the existing
  spooled EC shard writer for shard materialization.
- EC shard key construction now preserves bare keys for pre-versioned legacy
  objects while keeping versioned shard keys unchanged.
- Conversion commits now preserve the original object `LastModified` timestamp.

### Fixed

- Prevented EC conversion from committing stale metadata if object metadata
  changes before the conversion metadata commit.

## [0.0.242.0] - 2026-05-18 - perf(cluster): spool small parity EC writes

Small parity EC writes now avoid the in-memory full-object split path and reuse
the existing spooled EC shard encoder, reducing peak memory for small multi-shard
object writes while preserving the single-local fast path.

### Added

- Added regression coverage for small parity EC writes from both sized readers
  and streaming readers, including round-trip reads through `GetObject`.

### Changed

- Parity EC object writes now bypass the memory-shard fast path and route
  through the spooled shard encoder.
- Metadata preservation coverage now follows the spooled EC shard encoder path.

## [0.0.241.0] - 2026-05-18 - perf(packblob): reduce blob append allocations

Packed small-object writes now allocate less on the blob append hot path while
keeping the on-disk entry format and safe Go memory semantics.

### Added

- Added a `BlobStore.Append` allocation-budget regression test for the
  non-compressed 64 KiB write path.
- Added CRC coverage proving the optimized blob-entry checksum matches the
  standard IEEE CRC32 stream calculation.
- Added a direct `BlobStore.Append` benchmark to track allocation cost without
  higher-level `PutObject` overhead.

### Changed

- `BlobStore.Append` now uses stack-backed fixed headers and `WriteString` for
  entry key writes instead of heap-allocating temporary byte slices.
- Blob-entry CRC calculation now uses `crc32.Update` directly, avoiding the
  per-entry hash object and one-byte flag slice allocations.
- Encrypted blob AAD construction now copies keys directly from string input
  without an intermediate key byte slice.

## [0.0.240.0] - 2026-05-18 - perf(packblob): bound large-object intake

Packed object storage now routes large writes after reading only the configured
packing threshold, so oversized objects can stream through to the inner backend
without a full-body buffering pass.

### Added

- Added packed-object threshold routing coverage for below-threshold,
  exact-threshold, and above-threshold writes.
- Added a large-object intake regression test proving delegation memory does
  not scale with the full object size.

### Changed

- `PackedBackend.PutObjectWithRequest` now reads only the packing threshold
  before deciding whether to pack a small object or stream a large object
  through with the buffered prefix.

## [0.0.239.0] - 2026-05-18 - perf(raft): borrow heartbeat FlatBuffer payloads

Raft heartbeat batch encoding now avoids the per-item owned FlatBuffer payload
copy while keeping the returned batch payload fully owned by the caller.

### Added

- Added borrowed-vs-owned AppendEntries payload parity coverage for both empty
  heartbeats and entries-bearing AppendEntries payloads.
- Added a regression test proving encoded heartbeat batches survive FlatBuffers
  builder pool reuse after borrowed builders are released.

### Changed

- `encodeHeartbeatBatch` now borrows per-item AppendEntries FlatBuffer bytes,
  copies them into the final batch buffer, and releases builders after the copy.
- AppendEntriesArgs FlatBuffer construction is shared between the owned encoder
  and heartbeat borrowed-payload path to prevent wire-format drift.

### Fixed

- `BenchmarkHeartbeatEncodeBatch` improved from `10 allocs/op` to `1 alloc/op`,
  `1857 B/op` to `896 B/op`, and `1089.0 ns/op` to `915.6 ns/op` in the saved
  `benchstat` run.

## [0.0.238.0] - 2026-05-18 - perf(raft): reduce heartbeat batch encode allocation

Raft heartbeat batch encoding now allocates less on the sender hot path for
typical coalesced heartbeat batches. The release keeps the existing wire format
and preserves the large-batch fallback path with direct round-trip coverage.

### Added

- Added saved benchmark artifacts under `benchmarks/raft-read-frame/` showing
  the read-frame attribution, heartbeat encode baseline, after run, and
  `benchstat` comparison.
- Added a large heartbeat batch round-trip test covering the heap fallback path
  used when a batch exceeds the inline encode scratch capacity.

### Changed

- `encodeHeartbeatBatch` now uses an inline `[][]byte` scratch array for common
  small batches, avoiding one heap allocation per encoded heartbeat batch.

### Fixed

- `BenchmarkHeartbeatEncodeBatch` improved from `10 allocs/op` to `9 allocs/op`,
  `1.813 KiB/op` to `1.625 KiB/op`, and `1.089 us/op` to `1.004 us/op` in the
  saved `benchstat` run.

## [0.0.237.0] - 2026-05-18 - perf(raft): reduce heartbeat batch decode allocations

Raft heartbeat batch decoding now allocates less on the receiver hot path while
preserving owned decoded strings. The release also adds a measured Raft wire
benchmark matrix so future wire-format and transport allocation work starts from
saved before/after evidence instead of intuition.

### Added

- Added Raft wire microbenchmarks for `RaftConn` frame send/read,
  heartbeat batch encode/decode, and v2 QUIC AppendEntries encode/decode.
- Added saved benchmark artifacts under `benchmarks/raft-wire/` showing the
  baseline, selected heartbeat decode after run, and `benchstat` comparison.
- Added a regression test proving decoded heartbeat `groupID` and `LeaderID`
  strings remain valid after the input payload buffer is mutated.

### Changed

- `decodeHeartbeatBatch` now fills one batch-local `[]AppendEntriesArgs`
  backing store instead of allocating one `AppendEntriesArgs` per decoded item.
- Heartbeat AppendEntries decode now reuses repeated `LeaderID` string copies
  within a single decoded batch while keeping returned strings owned.

### Fixed

- `BenchmarkHeartbeatDecodeBatch` improved from `25 allocs/op` to
  `11 allocs/op`, `960 B/op` to `904 B/op`, and `563.2 ns/op` to
  `454.2 ns/op` in the saved `benchstat` run.

## [0.0.236.0] - 2026-05-18 - fix(cluster/s3auth): warp benchmark passes on a 4-node cluster (versioned + multipart + sigv4 botocore)

Operators running the warp benchmark suite against a 4-node, at-rest-encrypted
cluster can now exercise versioned, multipart, multipart-put, mixed, list, stat,
delete, put, get, and iceberg `catalog-mixed`/`catalog-commits` workloads
end-to-end. The previous release rejected versioned PUT/GET at signature time,
multipart at the capability gate, and multipart over 5 MiB parts at the
encrypted spool reader. Each is now traced to a concrete root cause and fixed
with a regression test. The `benchmarks/bench_s3_compat_compare.sh` helper
accepts the full warp op surface so a single sweep covers every supported
workload.

### Fixed

- `s3auth.buildCanonicalRequest` now rebuilds the canonical query
  string from `r.URL.Query()` instead of passing through
  `r.URL.RawQuery`. AWS SigV4 requires the canonical query to use
  `key=` for value-less parameters, AWS-strict URI encoding
  (`%20` for space, `~` left unencoded), and lexicographically
  sorted keys. botocore (the AWS CLI / Python SDK) signs against
  the strict form but transmits the wire form
  (`PUT /bucket?versioning`), so the previous comparison against
  `RawQuery` rejected every `PutBucketVersioning` and
  `GetBucketVersioning` call with `signature mismatch`. The new
  `awsURIEncode` helper percent-encodes anything outside the AWS
  unreserved set and is reused by `buildSortedQuery` (presigned URL
  signing).
- `ClusterCoordinator.SetBucketVersioning` now runs the
  cluster-aware `HeadBucket` (which understands meta-Raft bucket
  assignments) before invoking the backend. On a freshly
  bootstrapped cluster a follower may have the bucket assignment
  replicated through meta-Raft but not yet have applied the data-
  Raft `CmdCreateBucket` entry locally; the previous local-only
  pre-check inside `DistributedBackend.SetBucketVersioning`
  rejected the follower with `NoSuchBucket` and warp's `versioned`
  workload tripped at `PutBucketVersioning`.
- `DistributedBackend.SetBucketVersioningPropose` is the new
  coordinator-facing entrypoint. The coordinator calls it after the
  cluster-aware HeadBucket, so the propose path no longer
  duplicates the local pre-check. The original
  `SetBucketVersioning` keeps its local pre-check intact for direct
  callers (EC unit tests, single-node setups).
- `ClusterCoordinator.requireMultipartListingPeerCapability` now
  resolves `group.PeerIDs` (canonical node IDs such as
  `bench-node-2`) to raft addresses via `ResolveNodeAddresses` when
  the underlying `ShardGroupSource` also implements
  `NodeAddressBook`. The gossip receiver keys capability evidence by
  the resolved raft address (see `gossip.resolveGossipNodeID`), so
  without the resolve step `CreateMultipartUpload`,
  `ListMultipartUploads`, and `ListParts` were rejected on every
  freshly bootstrapped cluster with "capability multipart_listing_v1
  rejected for operation ...; finish the rolling upgrade before
  retrying" even though every node had advertised the capability
  and gossip had observed it. PUT/GET/DELETE were unaffected because
  those ops are not gated on `multipart_listing_v1`. Resolution
  falls back to the original peer slice when the meta source does
  not satisfy `NodeAddressBook` (existing test fakes) or when
  resolution fails, keeping prior unit-test behaviour intact.
- `DistributedBackend.UploadPart` previously copied the part body
  into the encrypted spool record stream with a bare `io.Copy`. When
  the caller-side reader implemented `WriteTo` (for example
  `*bytes.Reader`, which warp uses for 5 MiB parts), the writer
  received the entire part in a single `Write`, producing one sealed
  record larger than the `maxEncryptedSpoolBlobBytes = 2 MiB`
  receiver-side invariant. `CompleteMultipartUpload` then failed with
  `copy part 2: read encrypted spool record: blob too large` and the
  multipart workload could not run on an at-rest-encrypted cluster.
  `UploadPart` now copies through `copyToSpoolChunked`, which uses a
  pooled `spoolCopyBufferSize` buffer and hides any `WriteTo` fast
  path so every Write to the encrypted spool record writer stays
  within the chunk invariant. Reader-side reject behaviour is
  unchanged.

### Changed

- `benchmarks/bench_s3_compat_compare.sh` now accepts the full warp
  op surface (`put`, `get`, `delete`, `mixed`, `list`, `stat`,
  `versioned`, `retention`, `multipart`, `multipart-put`, `append`)
  in `WARP_OPS`. Multipart workloads use `--part.size` instead of
  `--obj.size`. `delete` auto-raises `--objects` to
  `concurrent × batch × 4` so warp's minimum-object guard does not
  reject the run. Buckets are now scoped per op
  (`warp-<target>-<op>`) so one run does not seed the next op with
  the previous op's data.
- The `warp analyze` parser accepts the obj/s-only `Average:` line
  used by `list` and `stat`, so those ops no longer trip the
  "missing Average line" fallback.

### Tests

- `TestVerifyAcceptsBareKeyQuery` signs `PUT /bucket?versioning`
  against the AWS-strict canonical `versioning=` and expects
  `Verify` to accept it.
- `TestVerifyAcceptsSpaceAsPercent20` exercises the `%20`
  encoding path used by AWS-strict canonical queries.
- `TestClusterCoordinatorSetBucketVersioningPassesClusterAwareHeadBucket`
  reproduces the follower scenario: base `HeadBucket` returns
  `ErrBucketNotFound`, meta has the assignment, coordinator must
  still propose successfully.
- `TestClusterCoordinatorSetBucketVersioningRejectsUnassignedBucket`
  pins the reverse: with no assignment the coordinator must
  surface `ErrBucketNotFound` without proposing.
- `TestRequireMultipartListingResolvesPeerIDsBeforeGate` registers
  capability evidence keyed by raft addresses (mimicking gossip),
  publishes `PeerIDs` as node IDs, and expects the gate to allow
  `CreateMultipartUpload`. Without the resolve step the gate marks
  every peer as `unknown`.
- `TestCopyToSpoolChunkedHandlesLargeReaders` writes a ~5 MiB
  payload through `copyToSpoolChunked` into the encrypted spool
  record writer and round-trips it through
  `openSpoolEncryptedRecordFile`. Without the helper the read would
  fail on the first record header with `blob too large`.

## [0.0.235.0] - 2026-05-18 - perf(server): pre-allocate buffered response body (-55% allocs, +137% throughput)

### Changed
- **`server.writeObjectBody`** buffered-response path (objects under
  the 128 KiB `bufferedObjectBodyLimit` threshold) now allocates the
  output buffer in one shot at `obj.Size` and reads with
  `io.ReadFull` instead of routing the reader through
  `newExactLengthReadCloser` and accumulating via `io.ReadAll`. The
  old path grew its buffer geometrically (~16 doublings to reach a
  64 KiB warp-sized object) while wrapping the upstream reader in a
  length-limiting closer, stacking ~16 throwaway allocations per
  buffered GET response. The new path is one `make`.

### Performance

`BenchmarkWriteObjectBody_WarpSizedObject` (64 KiB body, 3-run × 3s median):

| | before | after | Δ |
| --- | --- | --- | --- |
| allocs/op | 29 | 13 | **-55%** |
| B/op | 140169 | 67568 | **-52%** |
| ns/op | ~16800 | ~7142 | **-57%** |
| throughput | 3868 MB/s | 9176 MB/s | **+137%** |

The remaining 13 allocs/op are dominated by the Hertz header machinery
inside `SetBodyRaw` and the headers that precede it; the bench's prior
54% from `io.ReadAll` is gone.

`writeObjectBody` is on the S3 GET hot path for every response that
fits inside `bufferedObjectBodyLimit` (128 KiB). With encryption on
(production default) the underlying read also benefits from the
PR #401 reader-buffer reuse, so the full GET path improves end-to-end.

### Correctness note

The original `io.ReadAll` over `exactLengthReadCloser` silently
truncated when the backend reader ended before reaching `obj.Size` —
the response was emitted with the `Content-Length` header pointing
at a larger size than the body actually contained. The new
`io.ReadFull` returns `io.ErrUnexpectedEOF` in that case, which the
caller propagates as an error and the client observes as a 5xx
rather than a silently malformed response. This is a deliberate
behavior change.

The streaming path (objects ≥ 128 KiB and range requests) still
wraps the reader in `newExactLengthReadCloser` and is unchanged.

## [0.0.234.0] - 2026-05-18 - chore(encrypt): remove unused SealValue/OpenValue wrappers + encrypted packblob bench

### Added
- **`BenchmarkParallelGetSmallObjects_Encrypted`** in
  `internal/storage/packblob/get_parallel_bench_test.go` — measures the
  same parallel small-object GET workload as the existing
  `BenchmarkParallelGetSmallObjects` but with at-rest AES-256-GCM
  encryption enabled (the production-default per CLAUDE.md). This is
  the baseline future encryption-touching changes regress-check
  against. The shared `setupPackedBackend` helper was generalised to
  accept an `*encrypt.Encryptor` parameter.

### Measured

`BenchmarkParallelGetSmallObjects_Encrypted` (3 sizes × 3s, single
run):

| entries | allocs/op | B/op | ns/op |
| ------- | --------- | ---- | ----- |
| 1000    | 5         | 544  | ~1434 |
| 10000   | 5         | 544  | ~1372 |
| 100000  | 5         | 544  | ~1516 |

Compared with the unencrypted bench (4 allocs/op, ~449 B/op, ~1500
ns/op since PR #397), encryption costs **one extra allocation per
GetObject** and ~95 B/op. The extra alloc is `OpenValueAAD`'s
plaintext output buffer, sourced from `BlobStore.decodePayload`. The
encryption-on overhead is small enough that the previously-considered
"pool the plaintext buffer in BlobStore" refactor (which would have
added ~50 LOC of buffer lifecycle around `packedReader.Close`) was
not justified by the measured delta — this bench is what made that
clear.

### Removed
- **`encrypt.Encryptor.SealValue(domain string, plaintext []byte)`** — zero
  production callers after the encrypted-file refactors in PR #401 and
  PR #402. The wrapper converted its `domain` string to `[]byte` and
  delegated to `SealValueAADTo(nil, []byte(domain), plaintext)`. Callers
  with a `string` domain construct the `[]byte` themselves now (which
  is what `SealValueAADTo` was always documented to expect). The remaining
  `SealValueAADTo` is the canonical encrypt path.
- **`encrypt.Encryptor.OpenValue(domain string, blob []byte)`** — symmetric
  to the above. All in-tree callers already use `OpenValueAAD([]byte, []byte)`
  or `OpenValueAADTo(dst, []byte, []byte)`.

### Changed
- `encrypt_test.go` and `encrypt_bench_test.go` updated to call the
  canonical API directly. The two benchmarks that measured the removed
  wrappers are preserved under more accurate names:
  `BenchmarkSealValue` → `BenchmarkSealValue_NilDst` (measures the
  nil-dst allocating path) and `BenchmarkOpenValue` →
  `BenchmarkOpenValueAAD` (measures `OpenValueAAD`'s allocating-output
  path). Both call the same underlying code as before, so historical
  comparisons remain valid.

### Notes

A side effect surfaced by the rename: the bench `SealValue` / `OpenValue`
previously reported 2 allocs/op, while `SealValue_NilDst` /
`OpenValueAAD` now report 1 alloc/op. The missing alloc was the
wrapper's per-call `[]byte(domain)` conversion that ran inside the
timed loop. The wrappers had no production callers so this is
test-only, but it documents the cost of routing a string-domain
through the deprecated path. The canonical API has always taken
`[]byte` AAD precisely to let callers hoist the conversion outside
their hot loop.

## [0.0.233.0] - 2026-05-18 - perf(storage): finish encrypted-file buffer reuse across ReadAt/full-read/hash paths

### Changed
- The three remaining encrypted-file read paths in
  `internal/storage/encrypted_object_file.go` —
  `readAtEncryptedObjectFile` (range read),
  `readEncryptedObjectFile` (whole-object decrypt to `[]byte`), and
  `hashEncryptedObjectFile` (streaming hash) — now follow the same
  buffer-reuse pattern PR #401 introduced for `encryptedObjectReader`.
  Each function declares `aadBuf`, `sealedBuf`, and `plainBuf` at
  function scope, populated once on the first chunk and reused for
  the rest of the loop.
- `enc.OpenValue(encryptedChunkAAD(domain, chunk), sealed)` is
  replaced by `enc.OpenValueAADTo(plainBuf[:0], aadBuf, sealedBuf)`
  at all three sites. The AAD assembly uses the already-existing
  alloc-free `encryptedChunkAADBytes(aadBuf[:0], domain, chunk)`.
- `readEncryptedObjectFile` and `hashEncryptedObjectFile` switch from
  `readEncryptedObjectRecord(f)` to
  `readEncryptedObjectRecordInto(f, sealedBuf[:0])` so the sealed
  body is decoded into the reusable buffer rather than allocated
  fresh per chunk. `readAtEncryptedObjectFile` keeps its inline
  header parse (it needs the `Seek-past-this-chunk` skip branch),
  but the body-read now grows-or-reuses `sealedBuf`.
- Each of the three functions installs a `defer` that zero-fills
  every reusable buffer (up to capacity) on exit, so plaintext and
  sealed bytes never linger past the call. Matches the
  `Reader.Close()` security posture from PR #401.
- Dead code: the `encryptedChunkAAD(domain, chunk) string` helper
  (the `fmt.Sprintf`-based variant) was the last reason the
  Sprintf-allocating path was still loaded into the binary. After
  this PR it has zero production callers and zero test callers, so
  it is removed. `encryptedChunkAADBytes` (the alloc-free `dst []byte`
  variant) remains as the single source for chunk AAD assembly.

### Performance

`BenchmarkEncryptedObjectFileReadAt` (single-chunk range read,
3-run × 3s median):

| | before | after | Δ |
| --- | --- | --- | --- |
| allocs/op | 10 | 9 | -10% |
| B/op | 270749 | 270624 | -0.05% |
| ns/op | ~45000 | ~49959 | within noise |

ReadAt's savings are modest for one-chunk range reads because the
three reusable buffers all hit their first-grow on the only chunk
they process. The win materialises as the range spans more chunks
— each chunk past the first saves three allocations (AAD, sealed
body, plaintext). `BenchmarkEncryptedObjectFileRead` is unchanged
(already at the PR #401 floor of 138 allocs/op).

`readEncryptedObjectFile` and `hashEncryptedObjectFile` are not
covered by direct benchmarks, but they follow the same per-chunk
pattern as the now-optimised Reader path, so the savings scale the
same way: for an N-chunk decrypt of an 8 MiB object, ~3 × (N - 1)
fewer allocations compared to the prior code, plus 1 fewer per
chunk from the removed `fmt.Sprintf`. Hash recomputation
(`hashEncryptedObjectFile`) is on the ETag/integrity hot path; full
decrypt-to-`[]byte` (`readEncryptedObjectFile`) backs
read-modify-write at offset.

### Migration notes

Internal-only API changes. No external callers. The removal of the
`encryptedChunkAAD(domain, chunk) string` helper is safe — grep
across the tree shows zero remaining references; the same-named
function in `internal/storage/eccodec/shardio.go` has a different
signature (`func(base []byte, chunkIdx uint32) []byte`) and is
unrelated.

## [0.0.232.0] - 2026-05-18 - perf(storage): reuse buffers in encrypted object reader (-67% allocs)

### Changed
- **`storage.encryptedObjectReader`** now reuses three per-chunk
  buffers across reads instead of allocating fresh slices on every
  loop iteration:
  - `aadBuf` replaces the `fmt.Sprintf` AAD string with a reusable
    `[]byte` populated by the already-existing
    `encryptedChunkAADBytes(dst, domain, chunk)` helper.
  - `sealedBuf` is fed to a new `readEncryptedObjectRecordInto(r, dst)`
    helper that grows only when capacity is insufficient (i.e. once,
    on the first chunk).
  - `r.buf` (the plaintext output, drained by `Read()`) is reused as
    the destination passed to `Encryptor.OpenValueAADTo(r.buf[:0],
    ...)` instead of letting GCM allocate a fresh slice every chunk.
- `readEncryptedObjectRecord` is preserved as a thin wrapper around
  the new `readEncryptedObjectRecordInto` so the three call sites
  outside the hot Reader path (`ReadAt`, `decryptToWriter`,
  truncate) keep their current behavior unchanged.
- `encryptedObjectReader.Close()` now zero-fills the new `aadBuf`
  and `sealedBuf` (up to capacity) in addition to the plaintext
  buffer, so the security guarantee that no plaintext or sealed
  bytes linger past the reader's lifetime is preserved.

### Performance

`BenchmarkEncryptedObjectFileRead` (8 MiB sequential decrypt,
3-run × 3s median):

| | before | after | Δ |
| --- | --- | --- | --- |
| allocs/op | 415 | 138 | **-67%** |
| B/op | 17316388 | 8530305 | **-51%** |
| ns/op | ~2540 | ~2267 | **-11%** |
| throughput | 3311 MB/s | 3699 MB/s | +12% |

`BenchmarkEncryptedObjectFileReadAt` (range read, 1 chunk):
unchanged — that path is `readAtEncryptedObjectFile`, not the
reader. Touching it was out of scope for this PR.

### Remaining allocations

The post-refactor 138 allocs/op are dominated by stdlib internals
(`crypto/internal/fips140/aes/gcm.sliceForAppend` at ~65% of post
allocs). Those come from inside `aead.Open` and are not addressable
without bypassing the standard `cipher.AEAD` interface (security-
sensitive, explicitly out of scope).

### Migration notes

None. The reader API is unchanged; only internal buffer management
changes. Same `io.ReadCloser` contract, same security posture
(plaintext is cleared as it leaves `Read`, all scratch is zeroed on
`Close`). Tests including the race detector pass without
modification.

## [0.0.231.0] - 2026-05-18 - perf(storage): unmarshalObjectInto skips inner Object alloc, big Walk/List win

### Changed
- **`storage.unmarshalObject`** now delegates to a new `unmarshalObjectInto(data, dst *Object)` that decodes a flatbuffer directly into a caller-provided destination, eliminating the inner `&Object{...}` heap allocation it previously did on every call. The legacy `unmarshalObject(data) (*Object, error)` signature is preserved as a thin wrapper that allocates one Object and delegates.
- Six call sites in `internal/storage/local.go` (`HeadObject`, `SetObjectACL`, `Truncate`, `ListObjects`, `WalkObjects`, `ListAllObjects` snapshot path) now decode straight into a stack-declared `Object` they already had to allocate for their own use. The `decoded, err := unmarshalObject(...); obj = *decoded` pattern that copied a freshly heap-allocated Object onto a second location is gone.

### Performance

`BenchmarkWalkObjects` (1000 objects, 3-run × 3s median):

| | before | after | Δ |
| --- | --- | --- | --- |
| allocs/op | 8522 | 7522 | **-12%** |
| B/op | 530519 | 418508 | **-21%** |
| ns/op | ~398000 | ~337511 | **-15%** |

`BenchmarkListObjectsLoop` (same workload, bulk-load variant):

| | before | after | Δ |
| --- | --- | --- | --- |
| allocs/op | 8533 | 7533 | **-12%** |
| B/op | 548036 | 436036 | **-20%** |
| ns/op | ~397000 | ~355577 | **-10%** |

`BenchmarkHeadObject_NoCache` and `BenchmarkGetObject_NoCache` benefit
inversely-proportionally to their existing alloc count (Walk repeats
the decode 1000× per call, so an N=1 saving moves the per-object
fraction more):

| | before | after | Δ |
| --- | --- | --- | --- |
| HeadObject allocs/op | 16 | 15 | -6% |
| GetObject allocs/op | 21 | 19 | -10% |

Why this matters: S3 LIST is one of the most allocation-dense
operations a metadata service handles. A single LIST page over 1000
objects previously triggered ~8500 short-lived allocations from
GrainFS code alone, dominating GC pressure during bucket browsing.
Cutting one allocation per decoded object across the listing flow
trims 1000 allocations per page at zero behavior change. The B/op
reduction (−112KB per page) is a more direct lens on what GC will
see.

### Migration notes

`unmarshalObject(data []byte) (*Object, error)` keeps its signature
and behavior — external/test code calling it sees no change. The new
`unmarshalObjectInto(data []byte, dst *Object) error` is the canonical
form for hot paths that already own a destination.

## [0.0.230.0] - 2026-05-18 - perf(s3auth): replace two fmt.Sprintf with append in Verify hot path

### Changed
- **`verifyHeaderWithKey` and `verifyPresignedWithKey`** (the cache-hit
  path that fires on every authenticated S3 request) no longer build
  the SigV4 string-to-sign through two `fmt.Sprintf` calls plus
  `hex.EncodeToString` plus a `[]byte` conversion. Those four
  allocations are replaced by a single `stringToSignBytes` helper that
  pre-sizes one `[]byte`, hex-encodes the canonical-request hash into a
  stack array, and appends the fixed pieces in order. Output is
  byte-equivalent — the existing `SignRequest`/`Verify` round-trip
  tests verify the byte-for-byte signature conformance.

### Performance

`BenchmarkVerify_Hot` (5-run × 5s median, clean):

| | before | after | Δ |
| --- | --- | --- | --- |
| allocs/op | 33 | 23 | **-30%** |
| B/op | 1912 | 1496 | **-22%** |
| ns/op | ~1981 | ~1590 | **-20%** |

`BenchmarkVerify_Cold` (cache-miss path, dominated by DeriveSigningKey's
four `hmac.New` calls — only marginal gain available):

| | before | after | Δ |
| --- | --- | --- | --- |
| allocs/op | 92 | 82 | -11% |
| B/op | 6264 | 5848 | -7% |
| ns/op | ~4430 | ~4430 | flat |

This is intentionally a small surgical change. Earlier exploration
(pooled buffer + append-style canonical request helpers) cut allocs
to 11 but added 50+ lines of new code around security-sensitive
signature verification, and the latency improvement was masked by GC
noise. The risk/reward did not justify the broader refactor; this
change captures most of the practical alloc win with a single helper
function.

## [0.0.229.0] - 2026-05-18 - perf(local): fold bucket check into HeadObject, lazy readamp key

### Changed
- **`storage.LocalBackend.HeadObject`** no longer opens a separate
  `db.View` transaction for the bucket-existence pre-check. The bucket
  probe now runs only when the object meta lookup misses, inside the
  same transaction — happy path is one Badger View with one Get.
  Behavior is preserved: GET/HEAD on a missing bucket still returns
  `ErrBucketNotFound` (we fall back to the bucket probe when the
  object key misses); GET/HEAD on a missing key in an existing
  bucket still returns `ErrObjectNotFound`. The prior code paid
  Badger's per-View overhead (`getMemTables` allocation cluster,
  oracle read-mark, txn alloc) twice on every call, which was the
  single largest contributor to the HeadObject allocation profile.
- **`metrics/readamp.RecordBackendObject`** now takes `(bucket, key)`
  separately and concatenates them into the tracker key only when the
  simulator is globally enabled. The simulator is off in production by
  design (see package doc), so the `bucket + "/" + key` concat was
  pure waste on every backend GetObject. Single internal caller
  updated.

### Performance

`BenchmarkHeadObject_NoCache` (3-run × 5s median):

| | before | after | Δ |
| --- | --- | --- | --- |
| ns/op | 1279 | 776 | **-39%** |
| allocs/op | 24 | 16 | **-33%** |
| B/op | 1497 | 1088 | -27% |

`BenchmarkGetObject_NoCache` (3-run × 5s median):

| | before | after | Δ |
| --- | --- | --- | --- |
| ns/op | 15927 | 15023 | -5.7% |
| allocs/op | 29 | 20 | **-31%** |
| B/op | 1860 | 1435 | -23% |

GetObject's ns delta is small because file open dominates the path
(~15µs); the alloc win still falls through end-to-end since GetObject
delegates to HeadObject for metadata.

Why this matters: HeadObject runs on every S3 `HEAD` request and on
every `GET` cache miss. CachedBackend absorbs hits in steady state,
but a cold cache or a write-heavy workload that invalidates the cache
sees the full backend path on every read. A 39% latency reduction
there compounds into observable S3 p50 improvement for any workload
where the metadata cache is not saturated.

### Migration notes

`readamp.RecordBackendObject` signature changed from `(key string)` to
`(bucket, key string)`. Only one internal caller exists
(`internal/storage/local.go`), updated atomically. External callers
(none in tree) must pass bucket and key separately rather than
pre-concatenating.

The bucket-check fold preserves error semantics exactly and, as a
side benefit, eliminates a prior race: a concurrent
`ForceDeleteBucket` between the old two-View sequence could surface
`ErrObjectNotFound` from the second View when `ErrBucketNotFound` was
the correct answer (the bucket-and-its-objects were gone before the
second probe ran). Badger's single-View snapshot makes both Gets see
the same point-in-time state, so the caller now always gets the
consistent error.

## [0.0.228.0] - 2026-05-18 - perf(packblob): cut GetObject allocs from 6 to 4 via typed index key + pooled reader

### Changed
- **`packblob.PackedBackend` in-memory index** is now keyed by a typed
  `packedKey{bucket, key}` struct instead of the legacy
  `bucket + "/" + key` string concatenation. Every hot-path lookup
  (`GetObject`, `HeadObject`, `DeleteObject`, `PutObject`, `CopyObject`)
  previously allocated a fresh string just to form the index key — that
  allocation is now gone. Persistence (SaveIndex JSON, blob storage
  entries) still serialises the tuple back into the legacy string form
  at boundary crossings, so the on-disk format is unchanged and existing
  index.json files load without migration. `Range` callbacks now type-
  assert to `packedKey` so bucket filtering compares fields directly,
  replacing the previous `strings.HasPrefix(k.(string), bucket+"/")`
  scan.
- **`packblob.PackedBackend.GetObject` reader path** now returns a
  pooled `*packedReader` that embeds `bytes.Reader` and implements
  `io.Closer`. The prior `io.NopCloser(bytes.NewReader(data))` pair
  allocated two heap objects per packed read; the combined struct
  allocates at most one (and is reused across requests via
  `sync.Pool` so the steady-state count is zero). `Close()` resets the
  underlying byte slice reference before returning the reader to the
  pool, so callers that drop the reader on the floor cannot keep the
  decompressed payload alive.

### Performance

`BenchmarkParallelGetSmallObjects` mixed-load (3-run × 5s median):

| entries | before               | after                | Δ              |
| ------- | -------------------- | -------------------- | -------------- |
| 1000    | 1579 ns, 6 allocs/op | 1673 ns, 4 allocs/op | -33% allocs/op |
| 10000   | 1521 ns, 6 allocs/op | 1504 ns, 4 allocs/op | -33% allocs/op |
| 100000  | 1560 ns, 6 allocs/op | 1562 ns, 4 allocs/op | -33% allocs/op |

`BenchmarkParallelGetWithWriter` (concurrent writer pressure):

| entries | before               | after                | Δ              |
| ------- | -------------------- | -------------------- | -------------- |
| 10000   | 1913 ns, 6 allocs/op | 1929 ns, 4 allocs/op | -33% allocs/op |
| 100000  | 1923 ns, 6 allocs/op | 1914 ns, 4 allocs/op | -33% allocs/op |

ns/op sits inside the 5s-bench noise band; the measurable win is in
steady-state allocation churn (−33% allocs, −12% bytes per call). The
index-size invariance is preserved (1000 / 10000 / 100000 trace one
another), so the typed-key migration did not regress the sync.Map
lookup characteristic.

Why this matters: GetObject is the S3 GET hot path. With every packed
read previously allocating six objects (`indexKey` string, blob read
buffer, `&storage.Object{}`, `bytes.NewReader`, `io.NopCloser`, plus a
metadata map clone when present), every active connection drove GC
pressure on the small-object pool. Cutting the two cheapest-to-remove
allocations (the index key and the reader/closer pair) removes the
allocations that were _structurally_ avoidable — the remaining four
(blob read buffer, storage.Object, metadata clone, internal blob.Read
helper) are pinned by the public API and the encryption/CRC contract.

### Migration notes

None. The on-disk index format is unchanged and existing index.json
files load without conversion. `LoadIndex` rebuild-from-blobs and
JSON paths both parse the legacy "bucket/key" string back into
`packedKey` via a first-slash split — safe because S3 bucket names
cannot contain `/`. The parser now returns an error on a missing
separator (was previously a silent skip via `strings.Cut`), so a
corrupt index entry now fails LoadIndex loudly rather than silently
dropping the entry.

## [0.0.227.0] - 2026-05-17 - perf(pullthrough): lock-free IAMResolver cache via atomic.Pointer

### Changed
- **`pullthrough.IAMResolver`** no longer uses `sync.RWMutex`. The
  per-bucket upstream-client cache is published as an immutable
  `map[string]*resolverEntry` snapshot via `atomic.Pointer`. The
  cache-hit fast path (every pull-through S3 request) is a single
  atomic load + map lookup with no lock acquire/release. Cache fill,
  rotation rebuild, and eviction serialise on a small `writeMu` so
  `NewS3Upstream` is constructed at most once per rotation even under
  thundering-herd readers; the new entry is then published via
  clone-on-write.
- Eviction on "record disappeared" probes lock-free first and only
  acquires `writeMu` when there's actually something to clone out
  (avoids the prior unconditional `Lock` on every no-upstream call).
- Build-failure path still evicts any stale entry (preserved from the
  prior `delete(cache, bucket)` behavior, now expressed as a
  clone-without publish under the same `writeMu`) so a broken IAM
  record can't keep returning the prior cached client.

### Performance

Apple M3, `internal/storage/pullthrough`, `-benchtime=10s -count=2`,
100 buckets warmed, parallel readers across 8 cores:

| Bench | Before (median) | After (median) | Delta |
| --- | --- | --- | --- |
| `BenchmarkParallelResolve` | 112.9 ns/op | 34.9 ns/op | **-69% latency** |
| `BenchmarkParallelResolveWithRotation` | 126.8 ns/op | 27.0 ns/op | **-79% latency** |

Allocs per call unchanged (1 alloc/op — `sha256.Sum256` input escape,
not lock-related). Audit follow-up:
`docs/architecture/lock-free-audit.md` →
*"upstream client cache; hits take read lock, rotations rebuild under
write lock."* Every pull-through-bucket S3 request was paying that
`RLock` acquire/release on a shared cache line.

## [0.0.226.0] - 2026-05-17 - perf(policy): lock-free CompiledPolicyStore via atomic.Pointer

### Changed
- **`policy.CompiledPolicyStore`** no longer uses `sync.RWMutex`. The
  compiled-policy map and raw-JSON map are bundled into an immutable
  `policyState` struct published via `atomic.Pointer`. `Allow` (per-S3-
  request authorization hot path) and `GetRaw` are lock-free atomic
  loads. `Set` and `Delete` are serialised by a small `writeMu` so
  concurrent admin writers merge cleanly: each clones the current
  state, applies the mutation, and atomically publishes the new
  pointer. `Delete` on a non-present bucket short-circuits without
  cloning.
- Audit follow-up: `docs/architecture/lock-free-audit.md` →
  *"internal/policy/compiled.go - compiled policy map; request
  evaluation uses short read locks."* Every authorised S3 request was
  paying that RLock acquire/release on a shared cache line.

### Performance

Apple M3, `internal/policy`, `-benchtime=10s -count=2`, 100 buckets
preloaded, parallel readers across 8 cores:

| Bench | Before (median) | After (median) | Delta |
| --- | --- | --- | --- |
| `BenchmarkParallelAllow` | 174.7 ns/op | 17.7 ns/op | **-90% latency** |
| `BenchmarkParallelAllowWithWriter` | 99.4 ns/op | 18.7 ns/op | **-81% latency** |

Allocs per call unchanged (1 alloc/op — bench input copy escape, not
related to the lock). At 8 parallel readers the prior `RWMutex.RLock`
was paying cache-line bouncing on the shared mutex word; atomic load
pays a single read of an already-warm pointer. The reader-with-writer
bench shows the same speedup, confirming the writer no longer starves
readers (writer-priority RWMutex was forcing readers to wait for `Set`
calls even though the actual map mutation is a single pointer store).

## [0.0.225.0] - 2026-05-17 - fix(packblob): BlobStore.Close no longer leaks fds or directory lock on partial failure

### Fixed
- **`BlobStore.Close`** previously returned early on the first
  `f.Close()` error (active blob or any cached read fd), leaving the
  remaining read fds open and the directory `flock` held. A subsequent
  `NewBlobStore()` against the same directory would then fail with
  `blob dir already locked by another process`. Close now runs every
  cleanup step unconditionally and returns the joined set of failures
  via `errors.Join`. The directory lock is always released. Pre-existing
  bug — surfaced as a follow-up to PR #392 advisor review.

### Tests
- Two regression tests in `internal/storage/packblob/blob_close_leak_test.go`:
  one forces the active-blob close to fail (pre-closing the underlying fd),
  one seeds two pre-closed read fds in the cache. Both assert that the
  directory lock is released afterward by opening a second `BlobStore`
  on the same directory, and the multi-fd case asserts both fd errors
  appear in the joined error message. Verified that the tests fail on
  master (dir lock leak surfaces as `resource temporarily unavailable`)
  and pass on this branch.

## [0.0.224.0] - 2026-05-17 - perf(packblob): replace PackedBackend.mu with sync.Map index

### Changed
- **`PackedBackend.mu sync.RWMutex` + `index map[string]*indexEntry`**
  replaced with `index sync.Map`. `GetObject` / `HeadObject` /
  `DeleteObject` / `PutObject` are now lock-free on the index:
  - `PutObject` uses `index.Swap` and decrements the displaced entry's
    refcount atomically.
  - `DeleteObject` uses `LoadAndDelete`-equivalent via
    `Load` + `Refcount.Add(-1)` + `CompareAndDelete` to guard against
    a concurrent `Swap` publishing a new entry under the same key.
  - `CopyObject` preserves transactional semantics with a CAS-based
    refcount increment (rejects entries with `Refcount <= 0` or at
    `MaxInt64-1`) plus a re-validation `Load` that the source entry
    is still the canonical one. Lock-free.
- Range scans (`ListObjects`, `WalkObjects`, `bucketHasPackedObjects`,
  `deleteBucketIndex`, `ListAllObjects`, `SaveIndex`) use `sync.Map.Range`
  with documented weakly-consistent semantics — listing/scan operations
  tolerate concurrent inserts/deletes appearing or not.
- Audit follow-up: `docs/architecture/lock-free-audit.md` →
  "`PackedBackend.mu` protects the packed-object index. If packed small
  object reads become a hot-path bottleneck, convert this to the same
  immutable snapshot pattern used by `CachedBackend`." PR #392's mixed
  mutex profile attributed 91.7% of remaining delay (44.81s / 48.86s)
  to `PackedBackend.PutObject`'s `RWMutex.Unlock` — trigger condition
  hit. CoW with `atomic.Pointer[map]` was rejected because the
  isolated PutObject bench showed latency is index-size-invariant
  (11µs at N=1K through N=100K) — a CoW clone of N=100K would have
  pushed PutObject from 11µs to ~1ms (~100× regression).

### Performance

Apple M3, `internal/storage/packblob`, `-benchtime=10s -count=2`.

**Headline — mutex profile (`-mutexprofile`, mixed workload):**

| Metric | Before | After | Delta |
| --- | --- | --- | --- |
| Total mutex delay | 48.86s | 245.48ms | **-99.5%** |
| `PackedBackend.PutObject` (RWMutex.Unlock) | 44.81s (91.7%) | disappears | gone |

`PackedBackend.mu` is fully eliminated from the mutex profile.
Remaining 245ms is dominated by unrelated runtime / BadgerDB system
locks. PR #392 (BlobStore readFiles) cleared 445s → 51s of
contention on `bs.mu`; this PR clears the last 48.86s on `pb.mu`,
leaving the packblob hot path effectively lock-free for index access.

**Secondary — wall-clock bench (10s × 2; tight enough to read trend but
not a 15s × 3 measurement — treat the percentages as directional, not
load-bearing — see `feedback_bench_15s_min`):**

| Bench | Before | After | Direction |
| --- | --- | --- | --- |
| `BenchmarkParallelGetWithWriter/entries=10000` | 2045 ns/op | 1867 ns/op | reader latency down |
| `BenchmarkParallelGetWithWriter/entries=100000` | 1925 ns/op | 1858 ns/op | reader latency down |
| `BenchmarkPutObjectIsolated/preload=1000-100000` | ~11.0-11.4 µs, 18 allocs | ~11.3-11.5 µs, 20 allocs | +2-3% latency, +2 allocs |

The PutObject +2 allocs / +2-3% latency cost is sync.Map's
interface-boxing overhead for the string key + *indexEntry value;
the trade is justified by reads becoming completely lock-free and
PutObject no longer competing with readers under shared mutex.

### Concurrency Semantics

Delete-vs-Put races on the same key now resolve at `Load` granularity
rather than under a single lock. The final state — the live entry
visible via `index.Load(k)` — is identical to the prior lock-based
code in every realistic interleaving: the entry the last writer
publishes wins, and the **live** entry's refcount invariant is
preserved (the racing `DeleteObject` only decrements the displaced
entry it Load'd, leaving the fresh entry untouched). `DeleteObject`'s
`CompareAndDelete` may now fail when a concurrent `PutObject` Swap'd
in a fresher entry; in that case the **displaced** entry can take a
transient negative refcount because both `DeleteObject` (on its
Load'd pointer) and `PutObject`'s `Swap` (on the returned previous
value) decrement it — that entry is already unreachable from the
index, so the negative value is never observed and the entry is GC'd
once the racing goroutines drop their pointers. Callers needing
strict atomic delete-or-replace semantics must synchronize externally.

## [0.0.223.0] - 2026-05-17 - perf(packblob): split BlobStore.readFiles cache off bs.mu

### Changed
- **`BlobStore.readFiles`** is now published as an immutable
  `atomic.Pointer[map[uint64]*os.File]` snapshot. `getReadFile` no
  longer takes `bs.mu`: the hit path is a lock-free atomic load + map
  read; the miss path performs `os.Open` outside any lock and inserts
  via a CAS-retry CoW. Concurrent first-fillers race the syscall and
  the loser closes its duplicate fd — acceptable because fills happen
  at most once per blob file. `Close()` walks the published snapshot
  and stores an empty replacement.
- Audit follow-up: `docs/architecture/lock-free-audit.md` →
  "`BlobStore.getReadFile` shares `bs.mu` with `Append`; separate when
  mixed-workload mutex profile shows contention on the read path."
  PR #389 left this open after moving compression outside the lock;
  the mixed-workload mutex profile attributed 4.42% of delay to the
  read side (and a much larger share to writer self-blocking induced
  by reader contention on the same lock).

### Performance

Apple M3, `internal/storage/packblob`, `-benchtime=10s -count=2`:

| Bench | Before | After | Delta |
| --- | --- | --- | --- |
| `BenchmarkParallelGetWithWriter/entries=10000` | 4873 ns/op, 1051 B/op, 11 allocs | 2097 ns/op, 594 B/op, 6 allocs | **-57% latency, -45% allocs/op** |
| `BenchmarkParallelGetWithWriter/entries=100000` | 4457 ns/op, 988 B/op, 10 allocs | 1997 ns/op, 600 B/op, 6 allocs | **-55% latency, -40% allocs/op** |
| `BenchmarkParallelGetSmallObjects` | 1520-1606 ns/op, 6 allocs | 1539-1698 ns/op, 6 allocs | no regression (read-only) |

Mutex profile (`-mutexprofile`, same workload): **total delay
445.19s → 51.12s (-88.5%)**. `BlobStore.getReadFile` disappears from
the profile entirely (was 19.69s / 4.42%); `BlobStore.Append`'s
self-blocking also collapses because readers no longer hold the same
lock the writer is waiting on. Remaining 51s is dominated by
`PackedBackend.mu` (RWMutex protecting the small-object index) — a
separate lock, tracked as a follow-up audit item.

Writer throughput in the mixed bench drops from ~640K to ~230K writes
during the run. This is the correct trade-off: under the prior lock
the writer was monopolising the CPU because readers were sleeping on
contention; with reads decoupled, both sides progress and reader
latency wins by 2.3x.

## [0.0.222.0] - 2026-05-17 - perf(raft): drop redundant currentConfig defensive copy from actorState.snapshot

### Changed
- **`actorState.snapshot`** no longer deep-copies `currentConfig.voters`,
  `oldVoters`, and `learners` before publishing the readState. Every
  mutation site replaces `currentConfig` wholesale (via `newSingleConfig`
  / `newJointConfig` / `applyConfigEntry` / `configHistory` restore);
  none mutate the slices or learner map in place. `Configuration()`
  builds its own fresh `[]Server` via `allVoters()`, so external callers
  never receive the published slice header. Internal readers only use
  `len` + `range` on the published slices, which is safe under
  concurrent read.
- Documents the **wholesale-replacement invariant** on `currentConfig`
  in `snapshot()`'s comment. Future changes that mutate `voters` /
  `oldVoters` / `learners` in place break this contract and must
  reintroduce the defensive copy.

### Performance

Apple M3, `internal/raft/bench_test.go`, 15s × 3 runs (median):

| Bench | Before | After | Delta |
| --- | --- | --- | --- |
| `BenchmarkProposeWait_SingleNode_NoFsync` | 974 ns/op, 663 B/op, 5 allocs | 922 ns/op, 638 B/op, 4 allocs | **-5.3% latency, -20% allocs/op** |
| `BenchmarkProposeAndCommit_3Voter` | 8211 ns/op, 3325 B/op, 39 allocs | 7921 ns/op, 3002 B/op, 33 allocs | **-3.5% latency, -15% allocs/op** |

The earlier 3-second benchtime obscured this with noise — extending to
15 seconds × 3 runs reveals a consistent ~5% latency drop and an
integer-detectable allocs/op reduction (5→4 single-node, 39→33 3-voter).
The removed allocs are small (3-element string slices) but they fire
on every Raft publish and matter once you measure long enough to see
the signal.

## [0.0.221.0] - 2026-05-17 - perf(raft): reuse propose-batch scratch slice in the actor

### Changed
- **`Node.handleProposeBatch`** no longer allocates a fresh
  `make([]command, 0, maxProposeAppendBatch)` on every proposal. The
  64-capacity slice of the wide `command` struct dominated the raft
  benchmark's `alloc_space` profile at >95% of total bytes — most batches
  only contain one command, leaving the other 63 slots paid for and
  discarded.
- The actor goroutine is the sole reader / writer of the propose path, so
  a plain `proposeCmdScratch []command` field on `Node` beats a
  `sync.Pool` here. Written slots are zeroed with `clear()` before reuse
  so stale channel and pointer references do not survive across batches.

### Performance

Apple M3, `internal/raft/bench_test.go`:

| Bench | Before | After | Delta |
| --- | --- | --- | --- |
| `BenchmarkProposeWait_SingleNode_NoFsync` | 2180 ns/op, 33438 B/op, 6 allocs | 962 ns/op, 673 B/op, 5 allocs | **-56% latency, -98% bytes** |
| `BenchmarkProposeAndCommit_3Voter` | 9421 ns/op, 36025 B/op, 40 allocs | 8209 ns/op, 3196 B/op, 39 allocs | **-13% latency, -91% bytes** |

Total `alloc_space` across the bench dropped from 91.15 GB to 4.50 GB
(20× reduction). `handleProposeBatch` no longer appears in the
top-allocators list.

## [0.0.220.0] - 2026-05-17 - perf: move blob compression outside the BlobStore.Append critical section

### Changed
- **`BlobStore.Append`** now compresses input data *before* acquiring
  `BlobStore.mu`. The mutex profile of a mixed parallel read/write workload
  showed `Append` at 94% of total mutex delay, with zstd compression running
  inside the critical section. Compression depends only on the input bytes
  and the `bs.compress` setup flag (set once at construction); it does not
  need the lock. The file write and offset update remain inside the lock —
  those preserve append ordering and cannot be moved without a different
  schema (per-blob transactions or pre-allocated extents). Encryption stays
  inside the lock because its AAD depends on the in-lock `activeID` /
  `activeOff`.
- `BlobStore.EnableCompression` docstring now spells out the
  construction-only contract: callers must set `bs.compress` before the
  BlobStore is shared with any goroutine, because the new pre-lock
  compression path in `Append` reads the flag without the mutex. Future
  contributors cannot silently race that read by flipping compression on a
  live BlobStore.

### Internal
- Adds `get_parallel_bench_test.go` with `BenchmarkParallelGetSmallObjects`
  and `BenchmarkParallelGetWithWriter`. Together they document the original
  contention (`BlobStore.Append` at 94% of mutex delay during mixed
  read/write) and the post-fix profile (`Append` still dominant at 95.8% —
  the remaining cost is the file write itself, not compression).
- Negative finding recorded: a parallel `BenchmarkParallelGetSmallObjects`
  showed `PackedBackend.mu` RLock contention below the profiler's
  significance threshold (< 0.5% of mutex delay) even at 100k entries. The
  audit's conditional follow-up for `PackedBackend.mu` ("if packed small
  object reads become a hot-path bottleneck, convert to the immutable
  snapshot pattern") is **not** triggered by current workloads. The bench
  remains as a regression guard.
- `docs/architecture/lock-free-audit.md` "Changes In This Audit" section
  records the move; the `BlobStore.mu` inventory entry is updated to note
  that compression is now outside the critical section.

## [0.0.219.1] - 2026-05-17 - docs: ADR 0014 capability plan cache pattern

### Internal
- **ADR 0014** records the storage Operations capability plan cache decision
  (`atomic.Pointer` publication, single-Generation-source invariant, independent
  per-cache generation counters, per-wrapper long-lived `*Operations`).
  Establishes the third shape in the lock-free publication pattern family
  alongside IAM whole-state CoW (ADR 0007) and worker-pointer publication
  (ADR 0012, ADR 0013). Locks in `SwappableBackend` as the sole Generation()
  source so future contributors do not silently break cache invalidation by
  adding a second source.

## [0.0.219.0] - 2026-05-17 - refactor: lock-free Operations capability plan cache

### Changed
- **Storage decorator capability plan**: `Operations.planForCall` and the ACL
  capability plan now publish through `atomic.Pointer` and validate against a
  single-source `atomic.Uint64` generation counter
  (`SwappableBackend.Generation()`). Fast path is allocation-free and
  lock-free (7.7 ns/op, 0 B/op, 0 allocs/op on Apple M3).
- **Result-shape wrappers**: `SwappableBackend`, `CachedBackend`, `wal.Backend`,
  and `pullthrough.Backend` hold a long-lived `*Operations` over their inner
  backend instead of constructing a fresh `Operations` on every
  `PutObjectWith*Result` call. `SwappableBackend.Swap` resets the cached
  `*Operations` before swapping inner so post-swap calls rebuild against the
  new inner.

### Fixed
- **Hot-swap race in `SwappableBackend.cachedOps`**: a concurrent `Swap` could
  cause a reader to store an `*Operations` wrapping the previous inner,
  silently defeating the swap. The cache now uses a generation seqlock plus
  CAS publication so a racing build is discarded and rebuilt against the
  post-swap inner.
- **Cross-cache staleness between main plan and ACL plan**: a shared `planGen`
  meant that rebuilding the ACL cache made a stale main plan look fresh. Each
  cache now tracks its own generation (`planGen`, `aclPlanGen`) and
  invalidates independently while still observing the same upstream generation
  source.

### Internal
- `NewOperations` enforces the single-`Generation()`-source invariant at
  construction and panics if more than one source is discovered in the chain.
  Adds `TestNewOperationsPanicsOnMultipleGenerationSources`,
  `TestSwappableBackendCachedOpsInvalidatedOnSwap`,
  `TestSwappableBackendCachedOpsRaceWithSwap`, and
  `TestOperationsACLPlanRebuildDoesNotMaskStaleMainPlan` to guard the
  invariants.
- `docs/architecture/lock-free-audit.md` records the change and removes
  `internal/storage/operations.go` from the mutex inventory.
- `CONTEXT.md` extends the Storage Decorator Capability Plan section with the
  caching contract and single-source invariant.

## [0.0.218.0] - 2026-05-17 - feat: S3 production compatibility and warp benchmarks

### Added
- **SSE-S3 compatibility**: S3 PUT/COPY/HEAD/GET now accepts and returns
  `AES256` server-side encryption headers, persists SSE system metadata through
  object metadata codecs, and fails closed for unsupported KMS and SSE-C modes.
- **S3 DeleteObjects compatibility**: batch delete now supports the MinIO `mc`
  client path, including idempotent missing-key responses.
- **Real S3 client smoke coverage**: e2e coverage now exercises MinIO `mc` and
  conditionally runs `s3fs`/`goofys` through a Colima VM when the Linux client
  environment is available.
- **Iceberg warp compatibility**: the REST catalog exposes the `/_iceberg`
  alias and warehouse create/delete no-op endpoints needed by `warp iceberg`.
- **Lifecycle expiration days**: bucket lifecycle expiration rules now accept
  day-based expiration semantics.

### Changed
- **S3 benchmarks**: official single-node and cluster S3 benchmarks are
  consolidated on MinIO `warp` for PUT, GET, and DELETE runs.
- **Iceberg benchmarks**: Iceberg single-node and cluster benchmarks now use
  `warp iceberg`; the default mixed workload disables update distributions so
  the benchmark runs cleanly before the next optimization pass.
- **Compatibility documentation**: S3 production compatibility references now
  distinguish supported, partial, not supported, and not planned rows, keeping
  `s3fs` and `goofys` not supported until the Colima client smoke path passes.

### Fixed
- **SSE metadata persistence**: local, packed, and cluster object metadata paths
  preserve SSE-S3 system metadata, including copy-object metadata handling.
- **Iceberg metadata shape**: generated table metadata now includes valid UUID,
  timestamp, partition, and schema fields accepted by `warp iceberg`.

### Removed
- **k6 S3 benchmarks**: legacy k6-based S3 benchmark entry points were removed
  from the official benchmark surface.
- **Custom Iceberg Go runner**: the temporary `benchmarks/iceberg_table_bench`
  runner was removed after replacing it with `warp iceberg`.

### Verification
- `make test-unit`
- `GRAINFS_BINARY=$(pwd)/bin/grainfs go test ./tests/e2e -run 'TestS3|TestIceberg|TestMultipart|TestSmoke' -v -count=1 -timeout 10m`
- `PROFILE_ROOT=/tmp/grainfs-ship-iceberg-warp-single DURATION=3s VUS=2 ICEBERG_NAMESPACE_WIDTH=1 ICEBERG_NAMESPACE_DEPTH=1 ICEBERG_TABLES_PER_NS=1 ICEBERG_WARP_COMMAND=catalog-mixed NO_BUILD=1 make bench-iceberg-table`
- `PROFILE_ROOT=/tmp/grainfs-ship-s3-warp-single WARP_DURATION=8s WARP_CONCURRENT=2 WARP_OBJ_SIZE=1KiB WARP_OBJECTS=128 WARP_OPS=put,get NO_BUILD=1 make bench`
- `git diff --check -- ':!docs/superpowers/**'`

## [0.0.217.0] - 2026-05-17 - refactor(lifecycle): lock-free executor publication

### Changed
- `lifecycle.Service` removes `sync.Mutex`. The worker handle is published
  via `atomic.Pointer[Worker]` so admin `Status` callers acquire no lock,
  matching the migration service shape from v0.0.216.0. `cancelFn` and the
  wait group stay as plain fields because only the `Run()` goroutine
  touches them through `reconcile -> start/stop`.
- `Service.running` is removed; running state is derived from
  `worker.Load() != nil`. `workerRunningForTest` uses the same derivation.
- `lifecycle.Worker` removes `sync.Mutex`. `Stats.LastRun` is now published
  through `lastRunNano atomic.Int64` (unix nanoseconds, `0` means "never
  run"), mirroring `scrubber.liveSession.doneAt`. The three cycle counters
  (`ObjectsChecked`, `Expired`, `VersionsPruned`) move from raw `int64`
  with `atomic.AddInt64` to `atomic.Int64` fields with `Add(1)` for
  type-level consistency. `Stats()` translates `lastRunNano == 0` to a
  zero-value `time.Time{}` so the existing `IsZero` admin assertion still
  holds.

### Removed
- `lifecycle.Worker.Stop()` and `lifecycle.Worker.cancel` are removed.
  They had no production callers — executor shutdown is driven by
  `Service.stop` cancelling the workerCtx, which terminates `Worker.Run`
  via its existing `<-ctx.Done()` arm.

### Added
- `docs/adr/0013-lifecycle-service-lock-free-publication.md` closes the
  reservation in ADR 0012 about lifecycle. The conclusion is the same
  lock-free publication shape as migration, extended with a worker-side
  atomic stats surface; together ADRs 0012 and 0013 establish the
  lock-free publication pattern for leader-only executor services.
- `CONTEXT.md` gains a Bucket Lifecycle Executor domain entry.

## [0.0.216.0] - 2026-05-17 - refactor(migration): lock-free worker publication

### Changed
- `migration.Service` removes `sync.Mutex`. The worker handle is published
  via `atomic.Pointer[Worker]` so `SubmitJob` callers acquire no lock to
  signal a leader-side trigger. `cancelFn` and the wait group stay as plain
  fields because only the `Run()` goroutine touches them.
- `running` is no longer carried as a separate field; it is derived from
  `worker.Load() != nil`. `workerRunningForTest` uses the same derivation.
- `migration.Worker` is unchanged. `Trigger` keeps its non-blocking
  silent-drop semantics, and the `interval` ticker remains the multi-node
  safety net for triggers that land on followers.

### Added
- `docs/adr/0012-migration-service-lock-free-publication.md` records why
  the migration service is intentionally not folded into a controller-actor
  shape (its only cross-goroutine state is pointer publication, so atomic
  publication is the correct deepening rather than an actor pass-through).
- `CONTEXT.md` gains a Migration Worker domain entry. It also clarifies
  that job-state transitions are replicated through the meta-Raft FSM while
  `JobStore.SaveCursor` writes the per-bucket pagination cursor directly
  to the leader's local BadgerDB (an existing replication gap, unchanged
  by this release).

## [0.0.215.0] - 2026-05-16 - refactor(alerts): convert Dispatcher to fire-and-forget actor

### Changed
- `Dispatcher.Send` is now fire-and-forget (returns nothing). Acceptance and
  delivery results are observable only via `AlertDispatchDroppedTotal{reason}`
  counter and the optional `Options.OnResult` callback.
- `Dispatcher` is a controller-actor + ephemeral-worker-per-alert. Dedup state
  (lastSent, inFlight) and decrypt-warn rate-limit state are owned by the
  controller goroutine; the two prior `sync.Mutex` regions are removed.
- `cluster.AlertSender` and `resourceguard.AlertsSender` interfaces drop the
  `error` return.
- `server.AlertsState` removes `sync.Mutex`; counters are `atomic.Uint64` and
  `lastFailed` is `atomic.Pointer[alertFailureSnapshot]` COW snapshot.
- Six caller sites previously wrapping `Send` in `go func() { _ = ... }()`
  now call `Send(...)` directly.

### Added
- `AlertDispatchDroppedTotal{alert_kind, reason}` counter with bounded
  3-enum reason label (`inbox_full`, `not_started`, `stopped`).
- `Options.OnResult func(Alert, error)` — preferred callback. Legacy
  `FailureCallback` parameter is mapped to OnResult internally for
  backwards compatibility but should not be used in new code.
- `Dispatcher.Start(ctx)` / `Stop(ctx)` — graceful shutdown with ctx-aware
  retry/backoff and HTTP cancellation.
- `Dispatcher.DrainForTest()` — test-only synchronization barrier.

### Documentation
- `CONTEXT.md`: "Alerts Webhook Dispatcher" glossary with honest framing
  ("ergonomic deepening + minor locality, *not* scrubber-Director-style
  locality consolidation") to prevent future reviewers from re-proposing
  the same actor conversion for the wrong reason.

### Notes
- Race window closure is best-effort: a nanosecond window between caller's
  `stopping.Load()` and `inbox` send remains formally open. Operationally
  invisible (alert volume is low, Stop runs once per shutdown), but
  documented for future reviewers.

## [0.0.214.0] - 2026-05-16 - perf: tighten Iceberg catalog benchmark hot path

### Changed

- **Iceberg table metadata reads**: clustered Iceberg catalog loads now reuse the
  freshly read table metadata after create and commit operations, avoiding a
  repeated object read on the benchmark lifecycle hot path while preserving
  follower read-forwarding behavior.
- **Iceberg benchmark failure gate**: the Go Iceberg table benchmark now exits
  non-zero when any request fails, so low-rate request failures can no longer
  be hidden behind a successful benchmark exit.
- **Iceberg benchmark connection reuse**: the Go benchmark runner now sizes its
  HTTP transport for high-throughput local runs and records failure samples in
  the JSON report, preventing macOS ephemeral port exhaustion from masquerading
  as catalog failures.

### Verification

- `go test ./benchmarks/iceberg_table_bench ./internal/server ./internal/cluster ./internal/compat ./docs/reference -count=1`
- `go test ./internal/cluster -run '^$' -bench '^BenchmarkMetaCatalogLoadTableRepeated$' -benchmem -count=3`
- `VUS=4 DURATION=20s RAMP_UP=0s RAMP_DOWN=0s make bench-iceberg-table`
- `VUS=4 DURATION=20s RAMP_UP=0s RAMP_DOWN=0s CLUSTER_WARMUP_SLEEP=1 make bench-iceberg-table-cluster`
- `PROFILE=1 VUS=4 DURATION=20s RAMP_UP=0s RAMP_DOWN=0s make bench-iceberg-table`
- `git diff --check`

## [0.0.213.0] - 2026-05-16 - feat: support production S3 compatibility core

### Added

- **S3 production compatibility guardrails**: compatibility reference docs now
  use explicit supported, partial, not supported, or not planned status values,
  with tests preventing ambiguous `not tested` claims from returning.
- **Clustered multipart listing**: `ListMultipartUploads` and `ListParts` now
  work through the clustered forwarding path so single-node and cluster e2e
  tests exercise the same multipart listing feature set.
- **Iceberg benchmark Go runner**: `benchmarks/iceberg_table_bench` provides a
  native Go benchmark runner for Iceberg namespace/table lifecycle operations.

### Changed

- **Multipart listing performance**: clustered multipart upload scans filter
  FlatBuffers payloads before string allocation, reducing allocation pressure on
  the listing hot path.
- **Iceberg benchmark scripts**: single-node and cluster Iceberg benchmark entry
  points now run the Go runner instead of the legacy k6 script.
- **Benchmark documentation**: benchmark references document the new Iceberg
  runner and keep the S3 baseline policy aligned with `warp`.

### Fixed

- **Cluster multipart routing**: forwarded multipart listing/list-parts requests
  now encode, dispatch, and decode through the cluster transport correctly.
- **Multipart create gating**: local multipart creation is gated on required
  peer transport capabilities before accepting operations that require cluster
  forwarding support.
- **Iceberg metadata writes with IAM**: Iceberg table metadata writes avoid the
  ACL write path when IAM is enabled, preventing rollback failures on fresh
  metadata objects.

### Removed

- **Iceberg k6 workload**: the old `benchmarks/iceberg_table_bench.js` workload
  has been removed from the official Iceberg benchmark path.

### Verification

- `go test ./benchmarks/iceberg_table_bench ./internal/server ./internal/cluster ./internal/compat ./docs/reference -count=1`
- `GRAINFS_BINARY=$(pwd)/bin/grainfs go test ./tests/e2e -run 'TestMultipart_List|TestCluster_Multipart_List' -count=1`
- `bash -n benchmarks/bench_iceberg_table.sh benchmarks/bench_iceberg_table_cluster.sh`
- `VUS=2 DURATION=3s RAMP_UP=0s RAMP_DOWN=0s make bench-iceberg-table`
- `VUS=2 DURATION=3s RAMP_UP=0s RAMP_DOWN=0s make bench-iceberg-table-cluster`
- `git diff --check -- ':!docs/superpowers/**'`

## [0.0.212.0] - 2026-05-16 — refactor: convert scrubber Director to single-owner actor

### Changed

- **Scrubber Director registry ownership**: `internal/scrubber/Director`의
  `sources`/`verifiers`/`sessions`/`dedup` 4종 map을 `sync.Mutex` 보호에서
  단일 controller goroutine 단독 소유로 이전했다. 외부 API 시그니처와 의미
  (FSM drop semantics, dedup 영구성, 직렬 scrub 실행)는 모두 보존되며, 운영자
  관찰 가능한 동작 변화는 없다.
- **Worker dispatch**: controller가 `Trigger`/`ApplyFromFSM` 처리 시점에
  source/verifier를 resolve해 worker에 동봉 전달한다. worker→controller
  round-trip 제거.
- **Lifecycle 안전성**: `Stop()`이 idempotent (`sync.Once`) + `Start` 없이
  호출 시 즉시 반환. `done` chan으로 controller/worker 종료 완료 대기 가능.
  `Register`는 `Start` 이후 호출 시 panic으로 시점 제약 명시.

### Fixed

- **Pre-existing staticcheck 경고 3건**:
  `internal/audit/committer.go` deprecated `builder.NewRecord` 교체 (SA1019),
  `internal/storage/eccodec/shardio.go` 불필요한 for-loop 래퍼 제거 (SA4004),
  `internal/cluster/ec.go` `ecDataShardBufferPool`을 `*[]byte`로 변경해
  `sync.Pool` boxing alloc 회피 (SA6002).

### Documentation

- `docs/architecture/scrubber-director-actor.md`: actor 통합 설계 노트
  (topology, decisions, test strategy, out-of-scope).
- `TODOS.md`: 후속 task 3건 등록 (dedup 영구성 정책, 공통 JobActor 추상화,
  Register constructor 옵션화).

## [0.0.211.0] - 2026-05-16 — perf: improve small-object S3 throughput

### Added

- **Official S3 comparison benchmark**: `make bench-s3-compat-compare` now uses
  MinIO `warp` for comparable GrainFS, MinIO, and RustFS PUT/GET runs in
  single-node and 3-node cluster modes.
- **Cluster shard packing**: clustered EC shards below the default 65,537-byte
  threshold can now use node-local append-only shard packs, matching the
  small-object optimization used by single-node packed blobs.
- **Benchmark reporting**: README and the benchmark reference now show the
  latest same-host `warp` results for both single-node and 3-node cluster runs.

### Changed

- **Small-object defaults**: `grainfs serve` now enables `--pack-threshold` and
  `--shard-pack-threshold` by default for the 64 KiB workload class instead of
  requiring explicit tuning.
- **Cluster PUT hot path**: forwarded writes now use local data voters, sized
  FlatBuffer builders, batched Raft proposals, and shard-pack append batching to
  reduce CPU and syscall overhead.
- **Object GET path**: small object responses are buffered up to a bounded
  limit, while streamed responses are capped to the expected object length so
  clients do not observe trailing read errors as object data failures.
- **Benchmark policy**: MinIO/RustFS comparisons now keep only the latest
  comparable results and no longer use the old k6 mixed workload for official
  claims.

### Fixed

- **Cluster shard-pack durability errors**: shard-pack delete tombstone append
  failures now propagate instead of being silently ignored.
- **Shard-pack recovery**: startup scanning skips corrupt or truncated terminal
  records instead of failing the whole packed-shard store.
- **Packed copy metadata**: packed `CopyObject` preserves user metadata and
  object metadata across the copy path.
- **Spooled EC metadata**: memory-spooled EC writes preserve user metadata
  through clustered object reads and HEAD responses.
- **SigV4 compatibility**: canonical request handling now accepts the encoded
  path and payload-signing patterns used by `warp`.
- **Forwarded read EOF handling**: terminal EOF from forwarded streamed reads is
  treated as end-of-body instead of surfacing as an unexpected read failure.
- **S3 bucket compatibility**: bucket-level PUT/DELETE and location queries now
  match common S3 client expectations.

### Verification

- `make test-unit`
- `make build`
- `go test ./internal/cluster ./internal/storage/packblob ./internal/storage -count=1`
- `go test ./internal/cluster -run 'TestShardService_SharedPack(DefaultDoesNotSyncEveryAppend|DeleteReturnsTombstoneWriteError|RestartSkipsCorruptRecord|WriteReadRangeDelete)|TestShardPackScanSkipsOversizedRecord' -count=1`
- `git diff --check origin/master...HEAD`
- `PROFILE_ROOT=benchmarks/profiles/review-impact-single-grainfs-20260516-171005 TARGETS=grainfs-single WARP_DURATION=30s WARP_OBJ_SIZE=64KiB WARP_CONCURRENT=16 WARP_OBJECTS=4096 WARP_OPS=put,get WARP_NOCLEAR=1 WARP_HOST_SELECT=roundrobin make bench-s3-compat-compare`
- Manual 3-node `warp` PUT/GET run with 64 KiB objects, concurrency 16, and
  `--host-select roundrobin`, archived under
  `benchmarks/profiles/review-impact-cluster-grainfs-nosync-20260516-171937`

## [0.0.210.0] - 2026-05-15 — feat: route scrub through execution actors

### Added

- **Request execution contract**: admin scrub requests now pass through a typed
  `Operation` and `Result` contract that can choose single-node or cluster
  execution without changing the response shape.
- **Cluster scrub actor runtime**: cluster-mode scrub triggers now use a bounded
  mailbox executor with retry, timeout, cancellation, metrics, and cleanup
  wiring.
- **Execution observability**: queue depth, retry, timeout, worker failure,
  aggregation failure, and job duration metrics are available for the new actor
  path.

### Changed

- **Scrub trigger routing**: `/v1/scrub` keeps the existing
  `session_id`/`created` contract while routing through the execution seam when
  cluster execution is available, with legacy proposer fallback preserved.
- **Actor execution strategy docs**: the single/cluster request architecture now
  documents package boundaries, error mapping, capacity policy, performance
  gates, and the completed implementation checklist.

### Fixed

- **Admin error mapping**: bounded execution failures now map to stable admin
  codes and HTTP statuses, including retryable admission failures, timeouts,
  cancellations, job failures, and aggregation failures.
- **Production retry policy**: scrub actor boot wiring now uses the planned
  three-attempt retry policy with a 50 ms backoff.
- **Raft apply shutdown flush**: stopping a node no longer randomly drops ready
  apply entries during shutdown, removing a flaky replication ordering failure
  in the unit lane.

### Verification

- `go test ./internal/raft -run '^TestApplyLoopShutdownFlushesReadyEntries$' -count=50 -v`
- `go test ./internal/raft -count=5`
- `go test ./internal/server/execution ./internal/server/admin ./internal/serveruntime ./internal/serveruntime/executioncluster ./internal/metrics -count=1`
- `go test -race ./internal/serveruntime/executioncluster -count=1`
- `go test -count=1 -timeout 180s -v ./tests/e2e -run 'TestE2E_ECScrubTrigger'`
- `go test ./internal/server/admin -run '^$' -bench 'BenchmarkTriggerScrub(LegacyProposer|ExecutionSeam|ExecutionActor)$' -benchmem -count=5`
- `go list ./... | rg -v '^github.com/gritive/GrainFS/tests/e2e$' | xargs go test -count=1`
- `go test ./tests/e2e -count=1 -timeout 120s -run '^TestE2E_ECScrubTrigger'`
- `git diff --check`

## [0.0.209.2] - 2026-05-15 — docs: tighten compatibility and operator guides

### Changed

- **Operator examples**: `grainfs serve` examples now include `--cluster-key`, data paths, and ports where needed so copy-paste runs fail less often.
- **Credential setup**: S3, Iceberg, drill, and runbook docs now show the current IAM service-account flow and standard AWS CLI environment variables.
- **Protocol boundaries**: NFS, 9P, and NBD compatibility docs now state the actual network protocol expectations instead of implying unsupported client paths.

### Fixed

- **English-only docs**: remaining Korean prose in tracked docs was converted to English.
- **Stop-slop pass**: predictable AI-writing phrases in the touched docs were replaced with more direct wording.
- **Runbook deployment secret**: the Kubernetes example now creates the cluster-key secret consumed by the deployment.

### Verification

- `make test-unit`
- `git diff --check -- README.md ROADMAP.md docs`
- CJK text scan across `README.md`, `ROADMAP.md`, and `docs`
- local markdown link check
- fenced code block parity check

## [0.0.209.1] - 2026-05-15 — docs: refresh compatibility and benchmark guides

### Added

- **Compatibility references**: S3, NFSv4, 9P, NBD, and Iceberg now have focused compatibility matrices that separate supported, partial, not tested, and not supported surfaces.
- **Benchmark reference**: repository benchmark targets and current local performance snapshots now live in a dedicated reference page.
- **Documentation index and user guide**: readers can start from a role-based docs index and follow a user guide instead of scanning ad hoc files.

### Changed

- **README focus**: the README now summarizes product scope, compatibility, performance, and documentation links without listing every operator or CLI detail inline.
- **Docs layout**: user, operator, architecture, and reference material now live under consistent lowercase paths, with legacy runbooks moved into the operator section.
- **NFSv4 attribute audit**: the attribute matrix now reports status and relevant caveats without source-code columns or conformance-run bookkeeping.
- **English docs cleanup**: Korean and ad hoc planning prose in the tracked docs was converted to concise English.

### Fixed

- **Moved-document links**: code comments and admin error help links now point at the relocated operator and reference docs.
- **9P platform claims**: macOS native 9P is no longer described as something users should route through a Linux VM.

### Verification

- `git diff --check origin/master...HEAD`
- `go build ./cmd/grainfs`

## [0.0.209.0] - 2026-05-15 — perf: stabilize and shorten clustered PUTs

### Added

- **PUT trace shard attribution** — benchmark traces now identify remote shard open, buffer, RPC, local write, sync, meta-index, and forwarding stages so slow PUTs can be tied to the exact cluster phase.
- **PUT trace reports by object path** — the report now groups by ingress mode, size class, forwarding mode, and object key so local leader and forwarded non-leader paths can be compared directly.
- **PUT matrix warmup** — the cluster benchmark now warms each port and object-size path before measurement, then clears warmup trace data so startup leader election no longer pollutes p99 results.

### Changed

- **Forwarded PUT routing** — coordinators now resolve cached data-group leaders before forwarding writes, reducing avoidable peer sweeps on stable clusters.
- **Small EC shard writes** — small local shards now use buffered write paths with request-context tracing, cutting local shard write and sync overhead visible in the PUT matrix.
- **Object-index waits** — forwarded object-index local apply polling now reacts faster, reducing meta-index wait time on the receiver path.
- **Mutation preflight** — indexed PUTs now derive previous-object facts from the object index when possible, avoiding extra storage preflight work on hot PUT paths.

### Fixed

- **Bucket preflight on assigned buckets** — clustered PUTs now skip the base backend bucket existence check when the meta bucket assignment is already known.
- **Forwarded PUT p99 stability** — benchmark measurement now excludes data-group leader warmup retries, dropping the observed forwarded non-leader p99 outlier from roughly 183 ms to roughly 55 ms in the measured matrix.

### Verification

- `go test ./internal/storage/... -count=1`
- `go test ./internal/cluster -count=1`
- `go test ./internal/server -run 'TestPut' -count=1`
- `go test ./... -count=1` (all non-e2e packages completed; `tests/e2e` exceeded package timeout)
- `go test ./tests/nbd_interop -count=1 -timeout=5m`
- `go test ./tests/e2e -run '^TestIAM_E2E_PolicyBypassClosed$' -count=1 -timeout=3m`
- `go test ./tests/e2e -run '^TestE2E_DynamicGroupSeeding_1to5$' -count=1 -timeout=8m`
- `make build`
- Historical PUT matrix benchmark run before the S3 benchmark suite moved to `warp`.

## [0.0.208.0] - 2026-05-15 — refactor: split server route and runtime surfaces

### Added

- **Route surface manifests**: server and admin routes now have explicit path, availability, and auth surface tables with tests covering route visibility and anonymous/authenticated policy decisions.
- **Startup recovery package**: orphan tmp and multipart startup cleanup now lives in `internal/startuprecovery`, making server bootstrap thinner and independently testable.
- **NFS export e2e coverage**: multi-node NFS export tests now wait for rolling-upgrade capability gossip and mount explicit export paths.
- **PUT trace handoff**: HTTP PUT trace stages from `0.0.207.0` now flow through the split object-write runtime without restoring the old monolithic handler file.

### Changed

- **Server composition root**: the S3 server bootstrap, options, routes, middleware, and domain handlers are split into focused files instead of concentrating the system wiring in `server.go`.
- **Admin server modules**: admin route registration, Hertz adapters, bucket/NFS/scrub/volume handlers, and dependency wiring are separated by responsibility.
- **Object and Iceberg handlers**: object reads/writes, multipart, copy, post-policy, versioning, and Iceberg REST catalog flows are split into smaller modules while preserving existing API behavior.

### Fixed

- **Range authorization ordering**: range reads that use backend `ReadAt` now authorize private objects before writing object metadata headers.
- **Heal event persistence coverage**: heal emitter tests again cover event-store persistence and nil-hub enqueue behavior.
- **NFSv4 smoke flow**: the multi-raft NFSv4 smoke test now registers the bucket as an export before mounting and reads through the pseudo-root export directory.

### Verification

- `git diff --check origin/master`
- `go test -count=1 ./internal/server/... ./internal/startuprecovery ./internal/serveruntime`
- `go build -o bin/grainfs ./cmd/grainfs`
- `go test ./tests/e2e -run 'TestE2E_MultiRaftSharding_NFSv4Smoke|TestE2E_NFSMultiExportPropagation_MultiNode' -count=1 -timeout=4m -v`
- `go test ./tests/e2e -count=1 -timeout=25m`

## [0.0.207.0] - 2026-05-15: perf: attribute and tighten PUT forwarding

### Added

- **PUT trace attribution**: local and forwarded PUT paths can now emit benchmark-only JSONL trace events for routing, forwarding, receiver, shard-write, Raft metadata, and meta-index stages.
- **PUT matrix benchmark**: local cluster benchmarks can now compare small and large PUT latency across leader and follower ports, then generate a dominant-stage report with forward attempts, leader-hint retries, forwarded bytes, shard timing, and meta-index proposal counts.
- **PUT trace regression coverage**: trace sink behavior, coordinator forwarding, receiver forwarding, sender retry fields, report dominance, trace file permissions, and Raft dispatch timing now have targeted tests.

### Changed

- **Forwarded PUT index ownership**: forwarded PUTs now commit object-index entries on the receiving data-group leader instead of also committing from the forwarding coordinator.
- **Forwarding leader handling**: small forwarded PUTs avoid the extra preflight round trip and rely on NotLeader hint retry, while streamed PUTs keep the leader preflight before sending a non-rewindable body.
- **Raft replication wakeups**: leaders now dispatch pending entries after heartbeat replies and notify follower reads sooner when commit progress advances.
- **Follower read fallback budget**: follower local-read waits now use a shorter budget before forwarding, reducing long PUT-path waits observed during benchmark runs.

### Fixed

- **Forwarded mutation safety**: forward receivers now reject mutating object operations when object-index proposal is not wired, preventing successful writes that would be missing from the global object index.
- **Trace file privacy**: PUT trace JSONL files are created owner-only, reducing accidental exposure of raw bucket and key names during benchmark runs.
- **Benchmark artifact hygiene**: generated PUT matrix summaries, trace reports, and local planning files stay outside git.

### Verification

- `go test ./internal/cluster -count=1`
- `go test ./internal/raft -count=1`
- `go test ./internal/server -count=1`
- Historical PUT trace report verification before the S3 benchmark suite moved to `warp`.

## [0.0.206.1] - 2026-05-15: fix: NFS cluster benchmark reliability

### Fixed

- **NFS cluster benchmark startup**: localhost multi-node benchmark runs now use raft addresses as node IDs, so capability gossip accepts each node and export creation can proceed.
- **NFS cluster fio setup**: clustered NFS fio workloads now disable preallocation, matching the single-node NFS benchmark and avoiding long pre-layout stalls.

### Verification

- `bash -n benchmarks/bench_nfs_cluster_profile.sh`
- `git diff --check`
- `go test ./internal/storage ./internal/cluster -run 'TestInternalETag|TestVerifyETag|Test.*ETag|Test.*SingleLocal|TestGossipReceiverReportsCapabilityEvidenceUnderRaftMemberID|TestGossipReceiverPrefersAddressBookOverDirectNodeIDMatch|TestNodeIDMatchesFrom'`
- `NODE_COUNT=3 FIO_RUNTIME=3 FIO_STREAM_SIZE=4m FIO_STREAM_JOBS=1 FIO_RAND_SIZE=1m FIO_RAND_JOBS=1 CPU_PROFILE_SECONDS=8 CLUSTER_WARMUP_SLEEP=1 ./benchmarks/bench_nfs_cluster_profile.sh ./bin/grainfs`

## [0.0.206.0] - 2026-05-15: feat: write metadata snapshots with zstd

### Added

- **Snapshot zstd benchmark coverage**: snapshot compression benchmarks now compare gzip and zstd encode/decode behavior on representative snapshot payloads.

### Changed

- **Zstd metadata snapshots**: newly written metadata snapshots now keep the `GFSNAP01` envelope and store the JSON payload with zstd in `snapshot-<seq>.json.zst` files.
- **Snapshot compatibility policy**: legacy `.json.gz` snapshot archives are now intentionally unsupported by restore flows after the zstd cutover.
- **Rolling-upgrade docs**: compatibility docs now describe the zstd payload, `.json.zst` suffix, and older-binary suffix-level invisibility.

### Fixed

- **Legacy snapshot restore response**: direct restore of an existing `.json.gz` snapshot now returns an unsupported-format conflict instead of looking like a missing snapshot.
- **Snapshot sequence safety**: upgraded nodes seed new snapshot sequence numbers from legacy `.json.gz` filenames as well as current `.json.zst` files, avoiding sequence reuse after upgrade.

### Verification

- `make test-unit`
- `go test ./internal/snapshot -count=1`
- `go test ./internal/server -run 'TestRestore(SnapshotUnsupportedFormat|LegacyGzipSnapshot)ReturnsConflict' -count=1`
- `go test -tags compat ./tests/compat -run 'TestSnapshot(LegacyGzipRejectedByCurrent|HeadSnapshotInvisibleToOlderBinary)' -count=1`

## [0.0.205.1] - 2026-05-15: fix: encrypted benchmark allocation hotspots

### Added

- **9P benchmark coverage**: single-node and clustered 9P benchmark scripts now mount bucket exports in Colima, run fio workloads, and collect pprof profiles alongside the existing S3, NFS, NBD, and Iceberg benchmark lanes.
- **9P directory creation**: 9P bucket directories can now be created and removed through directory marker objects, with mode metadata and collision checks for files, sidecar namespaces, and existing directories.

### Changed

- **Encrypted shard reads**: full-shard and range reads now stream/decrypt from files with pooled chunk buffers instead of allocating full encrypted copies or MiB-scale buffers per range read.
- **NFS fallback writes**: non-`WriteAt` backends now rebuild partial writes as streams instead of reading the whole object into memory, while rejecting unsafe huge sparse offsets.
- **Encrypted spool reads**: cluster spool encryption now reuses plaintext and ciphertext buffers across records.
- **NBD request buffers**: 128 KiB NBD requests now use the buffer pool instead of allocating per request.

### Fixed

- **NFS cluster benchmark mount**: the clustered NFS benchmark now creates the target bucket/export and mounts the bucket path instead of the pseudo-root.
- **9P directory correctness**: file rename and child mutation paths now respect directory marker locks, directory mode metadata, and existing directory collisions.
- **9P server close race**: closing an already-stopped listener no longer reports a spurious `use of closed network connection` error.

### Verification

- `make test-unit`
- `git diff --check origin/master && bash -n benchmarks/bench_9p_profile.sh benchmarks/bench_9p_cluster_profile.sh benchmarks/bench_nfs_cluster_profile.sh benchmarks/bench_nbd_profile.sh benchmarks/bench_nbd_cluster_profile.sh benchmarks/bench_iceberg_table.sh benchmarks/bench_iceberg_table_cluster.sh benchmarks/bench_two_node_s3_profile.sh && make bin/grainfs`
- Benchmarks run across S3, NFS, NBD, Iceberg, and 9P single/cluster profiles under `benchmarks/profiles/`

## [0.0.205.0] - 2026-05-15: feat: searchable durable audit lake

### Added

- **Durable S3 audit outbox**: S3 request attempts and final outcomes are persisted locally before being committed to the Iceberg audit table.
- **Searchable audit schema**: audit rows now include request ID, service account, source IP, operation, auth status, error reason, version/upload/copy context, and day partition metadata for DuckDB queries.
- **Audit health and search APIs**: localhost dashboard endpoints expose outbox health and bounded S3 audit search backed by DuckDB/Iceberg.
- **Dashboard audit view**: the web UI now surfaces audit lake health and recent S3 audit events.

### Changed

- **Audit commit safety**: follower-shipped events are durably accepted by the leader, oversized wire fields are rejected/truncated before encoding, and stale provisional attempts can later be corrected by a final request outcome.
- **Internal audit bucket reads**: Iceberg artifacts remain blocked for normal S3 access except for the generated local audit reader credential or IAM-authorized artifact reads.
- **Audit docs**: `docs/users/audit-iceberg.md` now documents retention, query examples, dashboard behavior, and the operational guarantees.

### Verification

- `go test ./internal/audit ./internal/server ./internal/serveruntime ./internal/badgerrole -count=1`
- `make bin/grainfs && GRAINFS_BINARY=$(pwd)/bin/grainfs go test -tags duckdb_e2e ./tests/e2e -run TestAuditIcebergSingleDuckDB -count=1 -v -timeout 5m`

## [0.0.204.0] - 2026-05-15: feat: storage operations console

### Added

- **Storage operations console**: dashboard UI and `/ui/api/storage/*` routes now expose protocol status, safe bucket list/create, and NFS export state without mounting destructive storage mutations.
- **Capability-gated NFS export create**: NFS export registration now uses create-only meta-Raft commands gated by `nfs_export_create_v1` evidence across current meta-Raft members.
- **Protocol bind status**: NFSv4, NBD, and 9P service status now reflects actual listener bind success or failure for the admin/dashboard surface.

### Changed

- **Dashboard safety boundary**: the browser/volume/snapshot UI no longer exposes object delete, bucket delete, volume delete, or snapshot rollback/delete actions through `/ui/api`.

### Fixed

- **Rolling-upgrade forwarding**: gated meta-Raft forwarding preserves legacy raw migration cutovers while rejecting raw gated NFS create commands.
- **Capability gossip delivery**: capability evidence survives the QUIC stream catch-all path, records evidence under raft member addresses, and refreshes gate TTL from replayed cluster gossip settings.

## [0.0.203.0] - 2026-05-15: feat: snapshot format compatibility header

### Added

- **Snapshot format envelope**: newly written metadata snapshots now carry a `GFSNAP01` header with reader and writer format integers before the existing gzip JSON payload.
- **Forward-format restore guard**: restore rejects future snapshot envelopes before mutating backend state, and the admin restore API reports unsupported formats as `409 Conflict`.
- **Snapshot compatibility coverage**: tests cover header round-trips, legacy gzip-only snapshots, future-format rejection before backend mutation, API conflict responses, and the older-binary rejection compat scenario.

### Changed

- **Legacy snapshot reads**: existing gzip-only snapshots remain readable by detecting gzip magic before envelope parsing.
- **Rolling-upgrade compatibility docs**: `docs/reference/rolling-upgrade-compatibility.md` now documents the snapshot envelope and marks `TestHeadSnapshotReject` as live.

## [0.0.202.0] - 2026-05-15: feat: require local at-rest encryption

### Added

- **Mandatory local at-rest encryption**: local object files, multipart staging, cluster spool files, packed blobs, WAL mutation bodies, Badger metadata, and replicated FSM values are now written through the `GrainFS` encryption layer.
- **Encryption key bootstrap guardrails**: solo nodes can auto-create the local key, while cluster and join mode now require an explicit shared key file to avoid accidental split-key clusters.
- **Encrypted storage coverage**: tests now cover key bootstrap policy, hidden plaintext checks, wrong-key failures, metadata tampering, WAL tail handling, packblob downgrade resistance, and encrypted object `WriteAt`/`Truncate` atomic rewrites.

### Changed

- **Packblob and WAL compatibility**: encrypted records remain backward-compatible with existing plaintext records, while encrypted flags and metadata are authenticated to reject downgrade or tamper attempts.
- **Local object mutation safety**: encrypted random writes and truncates now rewrite through a temporary file with durable rename semantics instead of partially mutating ciphertext in place.
- **Smoke and benchmark bounds**: Colima/NFS smoke scripts and encryption benchmarks were adjusted for the encrypted storage path.

## [0.0.201.0] - 2026-05-15: feat: Badger startup recovery journal

### Added

- **Badger startup recovery journal**: startup-mode decisions that happen before the incident store is available are now written under `<data>/.recovery/entries/` with node, boot, binary version, role, group, path, status, action, and scrubbed reason metadata.
- **Incident import on next healthy boot**: once the incident store opens, pending recovery journal entries are imported as deterministic Badger startup incidents and marked imported without duplicating or regressing existing incident state.
- **Recovery journal coverage**: tests now cover relative journal paths, imported markers, reason scrubbing, pre-incident meta/group startup failures, idempotent import, and startup cleanup preserving `.recovery`.

### Changed

- **Quarantine manifest writes**: recovery journal entries and quarantine manifests now share the same atomic JSON write helper.
- **Runbook guidance**: Badger startup recovery documentation now calls out `.recovery` as the pre-incident journal that should be preserved for post-boot import.

## [0.0.200.1] - 2026-05-15: test: faster cluster unit test timing

### Changed

- **Cluster single-voter test setup**: backend and group backend helpers now poll leadership every 1ms while preserving the existing 2s cap, removing avoidable 10ms sleeps across many unit tests.
- **QUIC leadership transfer test**: reduced the special election timeout from 5s to 2.5s and added receiver-side TimeoutNow observation plus a 2s transfer deadline, keeping natural election outside the pass condition.

### Verification

- `go test -count=10 ./internal/cluster -run '^TestV2QUICCluster_ThreeNode_TransferLeadership$'`
- `go test -count=1 ./internal/cluster`

## [0.0.200.0] - 2026-05-15: perf: zero-alloc SigV4, storage cache, and NBD reply hot paths

### Changed

- **S3 SigV4 verification**: cached verification now parses auth fields and credential scopes without building per-request maps/slices, and compares expected HMAC hex without allocating the expected signature string.
- **Storage cache hits**: cached object reads now reuse reader state and struct cache keys, reducing cache-hit allocation churn while preserving lock-free snapshot reads.
- **NBD replies**: fixed and structured reply headers now reuse fixed buffers instead of allocating header slices on steady-state transmission paths.

### Fixed

- **Header auth query handling**: header-signed S3 requests whose query values contain `X-Amz-Algorithm=` or whose query includes an empty presign marker are no longer misclassified as presigned URLs; encoded presign keys remain recognized through a cold fallback.
- **Cached reader reuse safety**: stale double-close after cached reader reuse can no longer reset an active reader.
- **Coverage build compatibility**: NBD reply header pooling now avoids the generic fixed-array pattern that triggered a Go coverage compiler ICE while keeping the zero-allocation budget.

## [0.0.199.0] - 2026-05-15: feat: S3 audit log lake: Phase 2 (bootstrap + metrics + --audit-iceberg flag + e2e)

### Added

- **`--audit-iceberg` flag**: `cmd/grainfs serve` now exposes `--audit-iceberg` (bool, default `true`) and `--audit-commit-interval` (duration, default `60s`), wired to `serveruntime.Config.AuditIceberg` and `AuditCommitInterval`.
- **Idempotent bootstrap**: `internal/audit.Bootstrap(ctx, catalog, backend)` creates the `grainfs-audit` bucket, `audit` namespace, and `audit.s3` Iceberg table at startup when they do not exist.
- **Prometheus metrics**: `audit_drops_total{node}` (Counter), `audit_commit_lag_seconds{node}` (Histogram), and `audit_committer_state{node}` (Gauge) update during `Committer.Run` leader/follower state changes.
- **Subsystem wiring**: `boot_phases_srvopts.go` declares `metaCatalog` before the branch and wires Emitter, Bootstrap, Committer, and the `StreamAuditShip` QUIC handler when `cfg.AuditIceberg` is enabled. The leader ship function selects targets through `MetaProposalTargets`.
- **grainfs-audit access block**: `authzMiddleware` denies direct tenant S3 API access to the internal `grainfs-audit` bucket with `403`.
- **docs/users/audit-iceberg.md**: documents quick start, flags, storage layout, schema, DuckDB query examples, cluster behavior, and Prometheus metrics.
- **e2e tests**: adds `TestAuditIcebergSingleDuckDB`, `TestAuditIcebergClusterDuckDB`, `TestAuditIcebergClusterFollowerShipDuckDB`, and `TestAuditIcebergClusterLeaderFlap` under the `duckdb_e2e` build tag with `require.Eventually` polling.
- **`mrClusterOptions.ExtraArgs`**: adds `ExtraArgs []string` to the e2e harness so tests can pass per-node serve flags.

### Fixed

- **Audit drop counter**: leader-side `followerIn` channel overflow now increments `audit_drops_total`.
- **Batch cap**: follower drain loops now cap at 65,536 events, and `DecodeS3Batch` rejects counts above 65,536 to prevent OOM.
- **Commit failure log text**: commit failures now say "events in this batch are dropped" instead of "events retained in zerolog".
- **Bootstrap error level**: audit bootstrap errors now log at `Warn` instead of `Debug`.
- **Snapshot retain race**: `AutoSnapshotter.takeAndPrune()` now prunes again after snapshot creation, avoiding transient retain-limit overrun and creation-time retain reduction races.

## [0.0.198.0] - 2026-05-15: perf: xxhash3 ETag for internal buckets (~37× faster than MD5)

### Changed

- **Internal bucket write speed**: ETag computation on `__grainfs_*` write paths (WriteAt, PutObject, spool, cluster repair) now uses xxhash3 (~25 GB/s) instead of MD5 (~650 MB/s), a ~37× improvement. S3 user buckets are unaffected and continue using MD5.
- **Hash pool reuse**: `multipart.go` upload/complete/list paths now reuse a `sync.Pool`-backed MD5 hasher, eliminating per-operation allocations.
- **Algorithm-aware ETag verification**: `VerifyETag`, `ReplicationVerifier`, and `tryRepairFromPeer` detect the algorithm from ETag length (32 chars = MD5, 16 chars = xxhash3). Existing MD5 ETags verify correctly without migration.

### Fixed

- **Scrubber repair queue exhaustion**: `ReplicationVerifier` previously misreported objects with unrecognized ETag formats (e.g. multipart composite ETags) as `Corrupt`, which could exhaust the repair queue. These are now reported as `Skipped`.
- **Hasher pool lifetime**: `PutObjectWithUserMetadata` now returns the hash pool object immediately after computing the ETag rather than holding it for the duration of the rename + metadata write.

## [0.0.197.0] - 2026-05-14: fix: lock-free storage cache audit

### Changed

- **Storage read cache locking**: `CachedBackend` now publishes immutable cache snapshots with atomic compare-and-swap instead of protecting cache state with a mutex, keeping cache hits lock-free while preserving write invalidation.
- **Lock-free audit documentation**: added a production mutex inventory and review rule that explains which locks are justified, which should stay off read hot paths, and which storage locks remain acceptable.

### Fixed

- **Volume read/write serialization**: documented `Manager.mu` as a justified mutation boundary and added regression coverage proving `ReadAt` remains serialized with concurrent `WriteAt` for block-object consistency.

## [0.0.196.0] - 2026-05-14: feat: 9P read-write support

### Added

- **9P read-write objects**: Linux v9fs clients can create, overwrite, truncate, chmod/touch, rename, unlink, and fsync bucket objects through `grainfs serve --9p-port`.
- **9P metadata sidecars**: mode and mtime are stored under a protected `__meta/` namespace that is hidden from 9P directory listings and rejected for direct 9P access.
- **Colima read-write coverage**: `tests/9p_colima` verifies mounted 9P writes, signed HTTP visibility, stale-tail truncation, metadata operations, rename, unlink, and fsync.

### Changed

- **9P write safety**: object mutations now use per-object locks, recovery write-gate protection, backend capability preferences, bounded full-object fallbacks, same-path rename protection, and service shutdown cleanup.
- **9P fallback write performance**: user-bucket writes now coalesce per-fid `WriteAt` calls and flush once on `FSync`/`Close`, avoiding full-object read-modify-write on every 4 KiB write.
- **9P serving warning**: `--9p-port` now documents that the 9P endpoint is unauthenticated and should be kept behind a trusted network boundary.

### Fixed

- **9P user-bucket read fast path**: read capability preference is separate from write preference, preserving partial reads when partial writes are disabled for user buckets.

## [0.0.195.0] - 2026-05-14: feat: rolling upgrade capability gates

### Added

- **Capability gate framework**: `internal/compat` defines capability names, hard-gate errors, active feature helpers, and `grainfs_capability_reject_total{capability,scope,severity,operation,forced}` telemetry for version-skew rejections.
- **Config epoch-bound meta-Raft gates**: meta-Raft proposals can now be admitted through a `CapabilityGate` that verifies every current voter has fresh readiness evidence before new metadata commands are proposed or forwarded.
- **Gated migration cutover hook**: bucket upstream cutover state is persisted through IAM/meta-Raft, and `POST /v1/migration/cutover` is rejected until the cluster advertises the migration cutover capability.
- **Rolling upgrade compat coverage**: mixed-version compat tests now verify migration cutover fails closed before all nodes are capable, and the runbook documents capability gate rejection response.

## [0.0.194.0] - 2026-05-14: feat: S3 audit log lake: Phase 1 (Iceberg + Parquet)

### Added

- **S3 audit event schema**: `internal/audit` now defines `S3Event` with 13 fields, `BucketName = "grainfs-audit"`, `TableS3 = "s3"`, namespace constants, and the initial Iceberg metadata JSON template.
- **Lock-free ring buffer**: channel-backed bounded ring (cap 65,536) with non-blocking `Put`, `DrainInto`, and `Drops`/`Len`; `DrainInto(nil)` now drains all events correctly.
- **Emitter with recursion guard**: `audit.Emitter` writes S3 events to zerolog stdout and the ring, while skipping events for the `grainfs-audit` bucket and `system:audit` SA to prevent recursion.
- **S3 handler emit hooks**: PUT, GET, DELETE, and LIST paths can emit through the `WithAuditEmitter` server option; nil emitters are no-ops.
- **Follower-to-leader binary encoder**: `wire.go` adds LittleEndian `EncodeS3Batch` and `DecodeS3Batch` without JSON.
- **Cluster committer**: `audit.Committer` drains leader and follower events, encodes Parquet, commits Iceberg snapshots through `CommitTable` CAS, and accepts follower events through a non-blocking `followerIn chan []S3Event` with cap 256.
- **Parquet encoder**: Arrow-go v18 plus pqarrow writes Snappy-compressed Parquet with 13 Iceberg field IDs, verified with DuckDB `read_parquet()`.
- **Minimal Avro encoder**: writes Iceberg manifest and manifest-list files as Avro Object Container Files without another dependency.
- **StreamAuditShip = 0x13**: registers the follower-to-leader audit ship QUIC stream type in `internal/transport/transport.go`.

### Fixed

- **iceberg_api.go stale error text**: Iceberg REST Catalog access without `--audit-iceberg` now returns an error message that matches the real condition.

## [0.0.193.0] - 2026-05-14: feat: NFS multi-export DX and benchmarks

### Added

- **NFS export diagnostics**: `grainfs nfs debug <bucket>` reports registry state, backend bucket existence, recent pseudo-root LOOKUPs, and available client diagnostics in text or JSON.
- **NFS multi-export observability**: Prometheus now exposes export totals, propagation latency, unknown export LOOKUPs, and revoked stateid counters, with a sample Grafana dashboard in `docs/observability/nfs-multi-export.json`.
- **NFS profiling benchmarks**: `make bench-nfs-multi` runs a bounded multi-bucket Colima/fio workload with pprof capture, per-bucket throughput, and pseudo-root READDIR latency output.

### Changed

- **NFS export CLI JSON flags**: `grainfs nfs export` commands now use `--json`, matching bucket and IAM commands, and reject `--quiet --json`.
- **Benchmark defaults**: NFS profiling workloads now use bounded default sizes and `--fallocate=none` so local profiling completes and produces usable pprof data by default.
- **NFS runbooks**: README, RUNBOOK, `docs/operators/nfs-export-lifecycle.md`, and `docs/operators/nfs-debug.md` now document export lifecycle, debugging, and benchmark workflows.

### Fixed

- **NFS export admin errors**: `bucket_not_found` and `export_not_found` return 404, `export_already_exists` returns 409, and propagation timeouts return 504.
- **NFS write lock isolation**: writes and truncates for the same object key in different buckets no longer share one lock.
- **NFS debug truthfulness**: debug output no longer claims unavailable propagation/client state as healthy, applies admin timeouts, and keeps the NFS hint sweeper closed during runtime shutdown.

## [0.0.192.1] - 2026-05-14: feat: unknown MetaCmd telemetry

### Added

- **Unknown MetaCmd visibility**: operators now get `grainfs_unknown_metacmd_total{type}` when a node ignores a raft metadata command it does not recognize or handle.
- **Rolling-upgrade alerting**: Prometheus rule `GrainFSUnknownMetaCmdIgnored` warns on ignored MetaCmd events, including first-seen counter series, and the runbook explains the version-skew response path.

## [0.0.192.0] - 2026-05-14: feat: read-only 9P2000.L server

### Added

- **Read-only 9P2000.L server**: `grainfs serve --9p-port` can expose buckets and objects over 9P for Linux/Colima clients while remaining disabled by default.
- **9P directory and object coverage**: unit tests cover bucket listing, object reads, nested slash-containing object keys via synthetic directories, aname bucket roots, and paged Readdir behavior.
- **Colima 9P harness**: `make test-9p-colima` adds an opt-in Linux mount/read smoke test lane.

### Changed

- **Fast object-key walking**: local storage now provides `WalkObjectKeys` so 9P directory listing can iterate keys without unmarshalling object metadata.

## [0.0.190.1] - 2026-05-14: feat: rolling upgrade CI compat lane (Slice 1)

### Added

- **Rolling upgrade compat test lane**: `tests/compat/` package with 6 live cross-version scenarios and 1 stubbed placeholder for the snapshot version header (Slice 3). Run with `make test-compat`; tests skip gracefully when `COMPAT_PREV_BIN` is not set.
- **Compat policy document**: `docs/reference/rolling-upgrade-compatibility.md` defines the N → N+1 rolling upgrade policy, scenario table, and developer guide for adding new compat tests.
- **Slice 4 design document**: `docs/reference/upgrade-finalize-machinery-design.md` covers the `upgrade finalize` command, StateHash FSM divergence detection, snapshot version header, and drain/rollback procedure.

## [0.0.190.0] - 2026-05-14: feat: NFSv4.1 RFC 8881 audit

### Added

- **NFSv4.1 compliance matrix**: operators can now inspect RFC 8881 Section 5.8 attribute coverage in `docs/reference/nfsv4-compliance.md`, including Done, Partial, and Skipped rows with code citations and follow-up gaps.
- **pynfs conformance scaffold**: `tests/conformance/run_pynfs.sh`, `make test-pynfs-colima`, and the conformance README provide an advisory path for running external NFSv4.1 checks against a local `GrainFS` export.
- **NFS standards documentation**: README and runbook entries now point to the compliance matrix, conformance runner, and operational expectations for advisory pynfs results.

### Fixed

- **GETATTR attribute bitmaps**: NFSv4 GETATTR now supports the third attribute bitmap word, including RFC 8881 bit 75 `suppattr_exclcreat`.
- **NFS attribute truthfulness**: `cansettime` is advertised on bit 15 instead of the deprecated archive bit, and link/symlink support attributes now report unsupported operations accurately.
- **READDIR requested attrs**: real COMPOUND READDIR requests now preserve and honor requested entry attributes instead of dropping the bitmap during XDR argument decoding.
- **Colima conformance binary**: the pynfs Colima target now builds `grainfs` inside the Linux VM so macOS host binaries are not executed in Colima.

## [0.0.189.1] - 2026-05-14: fix: bucket policy/versioning handler correctness

### Fixed

- **Bucket existence pre-check**: policy and versioning admin endpoints (`GET/PUT/DELETE /v1/buckets/{name}/policy`, `GET/PUT /v1/buckets/{name}/versioning`) now return `404 not_found` instead of a storage-layer error when the bucket does not exist. A `checkBucketExists` helper is called after the internal-bucket guard and before the storage operation.
- **Policy `ErrBucketNotFound` → 404**: `GetBucketPolicy` now maps `storage.ErrBucketNotFound` (returned by `LocalBackend` when no policy key is present) to `404 not_found` instead of `500 internal`.
- **Policy structure validation**: `AdminSetBucketPolicy` now rejects non-JSON and structurally invalid policies (e.g., top-level string instead of object) at the handler layer via `policy.ParsePolicy`, before any storage write.
- **Effect case validation**: `policy.ParsePolicy` now rejects `Effect` values other than `"Allow"` or `"Deny"`, preventing silently inoperative policies caused by case typos (`"DENY"`, `"allow"`, etc.).
- **Ghost policy on bucket delete**: `LocalBackend.DeleteBucket` now also deletes the `policy:<bucket>` BadgerDB key, so a recreated bucket with the same name does not inherit the previous bucket's policy.
- **Backward-compatible policy cache warm-up**: `Operations.GetBucketPolicy` no longer propagates `CompiledPolicyStore.Set` errors to callers; a pre-existing policy with a non-conforming `Effect` is still returned as raw bytes via the admin API while being skipped for S3 authorization (default deny), allowing operators to read and fix it.

## [0.0.189.0] - 2026-05-14: fix: meta-Raft apply result delivery

### Transport

- **FIX**: Capability exchange now enforces strict 2-byte payload length;
  truncated frames are rejected with `payload_length` reason. (F1)
- **FIX**: CE failure modes now produce distinguishable peer-visible errors
  (`version_mismatch`, `wrong_first_stream`, `payload_length`,
  `feature_unsupported`, `timeout`, `io_error`). Replaces single generic
  "capability exchange failed" close message. (F3)
- **NEW**: Prometheus metric `grainfs_transport_ce_total{role,outcome,reason}`
  emitted on every CE attempt. (F7)
- **NEW**: CE features byte has an explicit reserved-bit policy: unknown bits
  in `features` reject with `feature_unsupported`. Registry at
  `docs/reference/transport-mux-versioning.md`. (F2)
- **TEST**: Concurrent mux dial dedup race coverage added. (F6)
- **DOC**: `docs/reference/transport-mux-versioning.md`: wire format, feature registry,
  version bump policy, v1 baseline rationale. (F5)

### Fixed

- **Meta-Raft apply errors**: proposals now return FSM apply failures after the committed index applies, so callers do not report success when the replicated metadata write failed.
- **Forwarded proposal visibility**: follower-forwarded writes now wait for bounded follower-local apply before returning, preserving local read-after-write behavior without tying latency to the full caller timeout.
- **Forwarded apply error types**: non-Iceberg FSM errors now cross the follower-to-leader forwarding boundary as `MetaForwardApplyError` instead of being collapsed into service-unavailable.
- **Raft-over-QUIC test setup**: raft QUIC cluster tests now retry connection setup with shorter per-attempt dial deadlines and a wider outer retry budget, reducing full-suite connection flakes without making each failed dial stall.

## [0.0.188.0] - 2026-05-14: feat: NFS export propagation follow-up

### Added

- **Multi-node NFS export propagation**: admin export add, update, remove, and bucket-delete cascade operations now wait for the committed meta-Raft index to apply before reporting success.
- **Bucket-delete cascade coverage**: process-level E2E coverage now verifies exported bucket deletion removes the export on success and preserves it when deletion or propagation fails.

### Fixed

- **Safe exported bucket deletion**: exported bucket deletion now records a durable cleanup marker and completes the NFS export cascade after the bucket delete succeeds, so crash or cascade failures can be retried without pre-removing a live bucket export.
- **User export partial-I/O fallback**: NFSv4 user-bucket exports now honor backend `PreferWriteAt`/`PreferReadAt` hints so writes, truncate, allocate, rename, and copy fall back to object-store paths instead of internal-bucket-only fast paths.
- **Cluster E2E UDP port race**: the five-node QUIC/static E2E now binds UDP listeners atomically instead of reserving free ports before parallel test startup.

## [0.0.187.0] - 2026-05-14: feat: NFSv4 multi-export registry and routing

### Added

- **NFS export registry**: cluster metadata now stores NFS export registrations with stable fsid/generation fields, and the admin API plus `grainfs nfs export` CLI can add, update, list, and remove exports.
- **NFSv4 pseudo-root multi-export routing**: NFS clients can browse registered buckets under the pseudo-root and route file operations to the selected bucket instead of the legacy fixed bucket.
- **Read-only export enforcement**: write, create, remove, rename, setattr, allocate, deallocate, and copy operations now reject mutations against read-only exports.
- **Export lifecycle E2E coverage**: CLI lifecycle tests cover export add/update/remove JSON output, missing-bucket rejection, and fsid/generation fields.
- **Fail-closed export lifecycle**: bucket deletion now rejects exported buckets instead of best-effort cascading the export first, and multi-node clusters reject NFS export mutations until a full propagation barrier is wired.
- **`GRAINFS_LOG_LEVEL` fallback**: `grainfs --log-level` still wins when explicitly provided, otherwise the CLI uses `GRAINFS_LOG_LEVEL` before falling back to `info`.

### Changed

- **NFSv4 legacy bucket hard removal**: `__grainfs_nfs4` is no longer an internal bucket and the NFSv4 server no longer auto-creates or routes through it.
- **E2E parallelism control**: `make test-e2e` now runs per-test invocations in parallel via `E2E_TEST_JOBS` (default `2`; set `E2E_TEST_JOBS=1` for serial execution).
- **NFS metadata cache keys**: NFSv4 metadata invalidation and file metadata cache entries are now bucket-aware.

### Fixed

- **Forwarded short reads**: cluster `ReadAt` forwarding now preserves short EOF reads instead of converting them to internal errors.
- **Empty EC objects**: EC-backed user buckets now accept zero-byte object writes, matching create/truncate flows used by NFS clients.
- **Deterministic export fsid allocation**: NFS export fsid minor and generation values are now assigned during meta-Raft apply, avoiding stale local-service allocation decisions.
- **Cross-export guards**: NFSv4 rename/copy across different exports now returns `NFS4ERR_XDEV`, and destination writes use the destination bucket.
- **Stale export handles**: filehandles bound to an older export generation now expire with `NFS4ERR_FHEXPIRED`; removed exports return `NFS4ERR_ADMIN_REVOKED`.
- **Live export refresh**: Raft-applied export registry changes now refresh the running NFSv4 server snapshot instead of requiring restart.

## [0.0.186.1] - 2026-05-14: docs: DX polish: NFS/NBD/Iceberg Quick Start

### Added

- **NFSv4 Quick Start**: README now includes a Phase 7 multi-export mount guide from `grainfs nfs export add` to pseudo-root mount and `/mnt/<bucket>/`.
- **NBD Quick Start (Linux)**: README now covers Linux `nbd-client` install, `mkfs.ext4`, and mount.
- **Iceberg IAM connection note**: `docs/users/iceberg-duckdb.md` now maps `grainfs iam sa create` output (`access_key`/`secret_key`) to DuckDB SECRET values.
- **`GRAINFS_ADMIN_SOCKET` environment variable**: Quick Start now exports `GRAINFS_ADMIN_SOCKET` so later commands can omit `--endpoint`.

### Fixed

- **`--nbd-port` default**: README now lists the actual default `10809` instead of the incorrect `0=disabled`.

## [0.0.186.0] - 2026-05-14: feat: QUIC mux capability exchange handshake

### Added

- **`ProtocolVersionMux = "grainfs-mux-v1"`**: `internal/transport/version.go` now owns the single protocol version constant, and `muxALPN()` returns it.
- **`StreamCapabilityExchange = 0x12`**: mux QUIC connections now use a Capability Exchange stream as the first stream.
- **Capability Exchange handshake**: mux QUIC connections exchange two bytes (`version=0x01, features=0x00`) during setup and close with `"capability exchange failed"` on version mismatch.
- **`ceRejectionCloseDelay = 200ms`**: rejected peers get time to read the error response before `CloseWithError`.
- **Five CE tests**: adds `TestMuxALPNConstant`, `TestVersionHandshakeSuccess`, `TestMixedVersionRejection`, `TestCapabilityExchangeTimeout`, and `TestCapabilityWrongFirstStream`.

### Fixed

- **`TestQUICTransport_MuxRejectedWithoutHandler`**: simplified the test for the CE failure path.

### Verification

- `go test ./internal/transport/... -run TestVersionHandshake` PASS
- `go test ./internal/transport/... -run TestMixedVersion` PASS
- `go test ./internal/transport/...` PASS (coverage: 73.4%)

## [0.0.185.0] - 2026-05-14: fix: Colima Linux tests and NBD/NFS fast paths

### Added

- **Colima Linux test integration**: Linux-dependent NBD/NFS/direct I/O coverage now runs through Colima without Docker and is wired into `make test`.
- **NBD/NFS profiling harness updates**: benchmark scripts run directly against the host binary, support pprof/direct fio options, and record NBD write-path trace data.
- **S3 user metadata persistence**: storage and cluster object metadata now carry user metadata through PutObject/CopyObject paths.

### Fixed

- **Docker removal**: deleted Docker-based e2e/benchmark scaffolding and updated docs/scripts to use direct host binary + Colima VM clients.
- **NFSv4 COPY/READDIR/rename behavior**: fixed offset-aware COPY, READDIR attr encoding, parent cache invalidation, and internal-bucket rename writes.
- **Internal bucket partial I/O routing**: internal buckets bypass user object-index paths and hard-delete internal metadata instead of creating S3 delete markers.
- **NBD fast path capability propagation**: pull-through now forwards `PartialIO`, `PreferWriteAt`, and async put capabilities so NBD volume writes can use `DistributedBackend.WriteAt`.
- **Single-node duplicate-self topology**: routing/backend write-at checks treat repeated local peer entries as one physical voter, preserving local pwrite fast paths in single-node EC-shaped topologies.

### Verification

- `go test ./internal/storage/pullthrough ./internal/cluster ./internal/volume -run 'TestPullThrough_ForwardsPartialIOCapabilities|TestOpRouter_RouteBucket_DuplicateSelfIsOnlyVoter|TestPreferWriteAt|TestClusterCoordinator_PreferWriteAt|TestClusterCoordinator_WALWriteAtReadAt'`
- `go test ./internal/nbd -run 'Test' -timeout 60s`
- `go test ./internal/volume/dedup -run '^$'`
- `make build`

## [0.0.184.0] - 2026-05-14: feat: bucket policy/versioning admin API + CLI

### Added

- **`grainfs bucket policy get/set/delete <bucket>`**: operators can read, set, and delete S3 bucket policies through admin UDS. `set` accepts JSON from `--file <path>` or stdin (`-`).
- **`grainfs bucket versioning get/enable/suspend <bucket>`**: operators can inspect, enable, and suspend bucket versioning.
- **`bucket list` + `bucket info`**: adds the `HAS_UPSTREAM` column; `bucket info` also adds `VERSIONING`.
- **`GET/PUT/DELETE /v1/buckets/:name/policy`**: admin HTTP API; PUT returns 400 for an empty body.
- **`GET/PUT /v1/buckets/:name/versioning`**: admin HTTP API; PUT accepts only `Enabled` or `Suspended`.
- **`AdminGetBucket` response**: includes `has_upstream` and `versioning`.
- **`AdminListBuckets` response**: includes `has_upstream`.

### Fixed

- **`bucket upstream list` parsing**: fixed a client bug that tried to unmarshal a raw server JSON array into a wrapped struct.
- **PUT `/v1/buckets/:name/policy` empty body**: policy PUT now returns 400 instead of accepting a body without a `policy` field.

### Verification

- `make test-e2e -run TestBucketUpstream_CLIRoundtrip` PASS
- `make test-e2e -run TestBucketUpstream_LegacyCLI_Removed` PASS

## [0.0.183.0] - 2026-05-14: test: dynamic MR cluster E2E + clusterpb fbs fix

### Added

- **`TestE2E_TwoNodeAvailabilityTrap`**: documents that writes fail with `context.DeadlineExceeded` after a two-node quorum loss.
- **`TestE2E_DynamicGroupSeeding_1to5`**: verifies that sequential 1-to-5 node expansion through `addNode` increases shard group count according to `seedGroupCountForClusterSize(n)=max(n*4,8)`.
- **`mrCluster.addNode`**: writes `.join-pending`, starts the node, waits for HTTP readiness, and refreshes `leaderIdx`.
- **`startMRCluster` / `tryStartMRCluster`**: start clusters with dynamic sequential join, beginning at node 0. `FastBootstrap` polls shard groups instead of sleeping 8 seconds.
- **`waitForShardGroupCount`**: polls admin UDS `/v1/cluster/status` until shard group count reaches the target.
- **`liveURLs()` helper**: skips unstarted node URLs in dynamic clusters where `MaxNodes > nodeCount`.

### Fixed

- **`clusterpb` fbs schema**: added missing `MigrationJobStart/Done/Failed` enum values so `make build`/`flatc` no longer removes constants from `MetaCmdType.go`. PR #340 carries the same fix.

### Verification

- `go test -count=1 ./tests/e2e/ -run TestE2E_MultiRaftSharding` (145s, PASS)
- `go test -count=1 -race ./tests/e2e/ -run TestE2E_DynamicGroupSeeding_1to5` (344s, PASS)

## [0.0.182.0] - 2026-05-14: feat: bucket & IAM CLI DX + security hardening

### Added

- **`grainfs bucket info <name>`**: reads bucket information, including object count, through admin UDS and supports `--json`.
- **`grainfs bucket upstream` subcommands**: manage per-bucket pull-through upstream credentials with `put`, `get`, `list`, and `delete`.
- **tabwriter table output**: `bucket list`, `bucket info`, `upstream get`, `upstream list`, and `iam sa list` now print aligned tables.
- **`--json` flag**: adds a persistent `--json` flag to `bucket` and `iam` commands for scripts.
- **`GRAINFS_ADMIN_SOCKET` environment variable**: commands fall back to this endpoint when `--endpoint` is omitted.
- **User feedback messages**: `bucket create/delete` and `upstream put/delete` print clear success messages.
- **`iam sa` create/get/delete output**: SA creation prints access key and secret key tables; get shows SA details.

### Fixed

- **`AdminGetBucket` UI exposure**: removed `registerBucket` from `RegisterUI`, preventing dashboard-token holders from triggering remote `CountObjects` full scans that could starve Badger writes. Bucket admin ops stay on admin UDS.
- **upstream routing collision**: moved `GET|PUT /v1/buckets/upstream` to `GET|PUT /v1/upstreams`, fixing the Hertz static-beats-param collision where `bucket info upstream` returned the upstream list.
- **CLI hang**: added a 30 second timeout to `iamHTTPClient` so unresponsive servers do not hang the CLI.

### Verification

- `go test -count=1 ./cmd/grainfs/... ./internal/server/admin/... ./internal/serveruntime/...`
- `go list ./... | grep -v '^github.com/gritive/GrainFS/tests/e2e$' | xargs go test -count=1`: all PASS

## [0.0.181.0] - 2026-05-14: fix: ForceDeleteBucket correctness bugs

### Fixed

- **ForceDeleteBucket Badger MVCC snapshot leak**: separated scan and propose (`View -> collect refs -> propose`) so Raft proposals no longer hold MVCC snapshots for N times RTT and block Badger GC.
- **ForceDeleteBucket context propagation**: internal loops now pass `ctx`, so cancellation is honored.
- **ForceDeleteBucket multi-version object cleanup**: scans the full `obj:<bucket>/` keyspace instead of relying on `WalkObjects`, which returns only the latest version per key.
- **ForceDeleteBucket ring refcount double-decRef**: processes versioned refs first so `applyDeleteObjectVersion` clears ObjectMetaKey before unversioned refs are handled.
- **`AdminDeleteBucket` force=true `ErrBucketNotEmpty`**: concurrent writes during forced delete now return 503 retry instead of a misleading `"use --force"` message.

## [0.0.180.2] - 2026-05-14: fix: cluster benchmark and e2e latency regressions

### Fixed

- **Cluster runtime topology publication**: runtime join paths now publish cluster node topology and EC config as immutable snapshots so writes do not stay pinned to boot-time placement after nodes join. Coordinator routing/execution state now refreshes atomically with EC config.
- **Cluster benchmark harnesses**: NFS, NBD, S3, and Iceberg cluster benchmarks now use dynamic join flow, shared encryption keys, admin socket readiness checks, node log archival, configurable node counts, and profile/runtime parameters.
- **Benchmark auth and partial I/O setup**: Iceberg benchmark setup signs bucket creation with IAM credentials; NFS/NBD benchmark scripts wait for admin socket/CPU profile completion and quote runtime parameters correctly.
- **Raft log reads**: badger raft log range reads now fetch contiguous indexes directly and fail on missing or mismatched entries instead of iterator-skipping metadata keys.
- **NFSv4 backend capability checks**: NFS operations now have explicit backend capability coverage for partial I/O behavior.
- **e2e harness latency**: static cluster startup removed fixed sleeps, process cleanup terminates signal-ignoring test children immediately, S3 e2e clients disable keep-alives, and expiring-key tests poll observed expiry instead of sleeping.
- **IAM plaintext secret test scope**: the no-plaintext-secret e2e check now scans the IAM control-plane `meta_raft` persistence path instead of unrelated data-plane directories.
- **Small Badger metadata DBs**: `SmallOptions` now caps value log files at 64 MiB to reduce test/runtime metadata store footprint.
- **Auto-snapshot hot reload**: disabled snapshot polling idle interval reduced from 5s to 1s, bounding cluster config hot-reload latency.

### Verification

- `go test -count=1 ./internal/badgerutil ./internal/iam ./internal/snapshot ./internal/cluster ./internal/nfs4server ./internal/serveruntime ./internal/raft`
- `go build -o bin/grainfs ./cmd/grainfs`
- `GRAINFS_BINARY=$PWD/bin/grainfs go test -json -short -count=1 -timeout 5m ./tests/e2e`: PASS, 50.658s
- `go list ./... | grep -v '^github.com/gritive/GrainFS/tests/e2e$' | xargs go test -count=1`

## [0.0.180.1] - 2026-05-13: fix: runbook bootstrap procedure and snapshot audit log

### Fixed

- **Bootstrap docs**: RUNBOOK deployment section now documents direct host binary startup and host-side `admin.sock` bootstrap.
- **K8s bootstrap**: RUNBOOK K8s section now documents admin SA creation after first deploy with `kubectl exec deploy/grainfs -n grainfs -- grainfs iam sa create admin`.
- **snapshot-interval / snapshot-retain audit log**: `ClusterConfigPatch` now includes `SnapshotInterval` and `SnapshotRetain` in the audit dict when the FSM applies them.

## [0.0.180.0] - 2026-05-13: feat: bucket and IAM admin API plus bucket CLI

### Added

- **Bucket admin API**: added admin UDS REST endpoints for bucket create (`POST /v1/buckets`), list (`GET /v1/buckets`), and delete (`DELETE /v1/buckets/:name?force=true`). `--force` can delete non-empty buckets.
- **`grainfs bucket create/list/delete` CLI commands**: operators can manage buckets directly through admin UDS. `grainfs bucket delete --force <name>` removes all objects before deleting the bucket.
- **IAM admin handlers use the volume pattern**: replaced `iam_admin.go` / `bucket_admin.go` with hertz adapter `registerIAM` / `registerBucket`, using pure handlers plus thin adapters for easier unit tests.
- **`IAMService` / `BucketOps` interfaces**: `admin.Deps` now references interfaces instead of concrete types so tests can use fakes.
- **`ForceDeleteBucket`**: added to the `Backend` interface and implemented by LocalBackend, Operations, DistributedBackend, SwappableBackend, PackedBackend, and RecoveryWriteGate.

### Fixed

- **S3 ListBuckets internal bucket filtering**: S3 ListBuckets no longer returns `__grainfs_*` internal buckets.
- **admin HTTP 403 restoration**: wildcard grant denial now returns 403 instead of 500 by restoring `statusForCode("forbidden")`.
- **`AdminCreateBucket` bucket-name validation**: rejects names with slashes, uppercase letters, or special characters, preventing Badger key collisions and LocalBackend path traversal. Adds `storage.ValidBucketName`.
- **`AdminDeleteBucket` internal bucket guard**: force-delete on `__grainfs_*` buckets now returns 403 Forbidden.

### Changed

- Creation endpoints such as `POST /v1/iam/sa` now return **201 Created** instead of 200, matching RFC 9110.

## [0.0.179.0] - 2026-05-13: chore: remove non-EC object write path

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

## [0.0.178.0] - 2026-05-13: fix: PromoteToVoter orphan recovery in Raft v2 becomeLeader

### Fixed

- **`recoverOrphanedPromote()`** added to `internal/raft/membership.go`, called from
  `becomeLeader()` after `recoverInFlightJoint()`. Handles the crash scenario where the
  prior leader committed Stage-1 (`ConfChangePromoteStage1`: drops target from learners)
  but crashed before appending Stage-2 (`LogEntryJointConfChange`). The orphaned target
  is left in neither voters nor learners, blocking it from participating in consensus.
- Recovery synthesises `pendingSingleConf` (pointing to the Stage-1 log index) and
  `pendingPromote` so the existing `advanceSingleConfPhase` machinery dispatches Stage-2
  on the new leader. Committed Stage-1 state at `becomeLeader` time drives the call
  inline; otherwise `applyCommitted → advanceSingleConfPhase` fires it.
- `matchIndex`/`nextIndex` for the orphaned target is seeded to
  `(0, lastLogIndex+1)` when absent: the normal path seeds these when the target joins
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

- `go test -race ./internal/raft/ -run TestPromoteToVoter_OrphanRecovery -count=20`: all PASS
- `go test ./internal/raft/ -timeout 120s -count=1`: all PASS (63 s, 63 tests)

## [0.0.177.0] - 2026-05-13: fix: RouteObjectWrite preserves forward peers when self is leader

### Fixed

- `OpRouter.RouteObjectWrite` now populates `RouteTarget.Peers` even when
  `SelfIsLeader` is true. Previously, `routeGroup` short-circuited and left
  `Peers` empty, so if leadership changed between routing and execution the
  write had no forward candidates. `RouteBucket` still uses the short-circuit
  path (peers empty on leader): only the object-write path resolves peers.

### Verification

- `go test -count=3 ./internal/cluster/ -run TestOpRouter_Route`: all PASS
- `go test -count=1 ./internal/cluster/`: all PASS

## [0.0.176.0] - 2026-05-13: feat: SendTimeoutNow QUIC RPC (leader transfer)

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

## [0.0.175.0] - 2026-05-13: fix: eliminate peerHealth race in ecObjectReader goroutine drain

### Fixed

- Moved `peerHealth.MarkHealthy`/`MarkUnhealthy` calls from spawned shard-fetch
  goroutines to the main goroutine in `ecObjectReader.readShards`. Previously,
  k-of-n early exit could leave a goroutine still executing `MarkUnhealthy` while
  the caller already read `health.unhealthy`, producing a DATA RACE under
  `-race`. The fix encodes peer state (`peer`, `peerOK`, `canceled`) in
  `shardResult` and processes it in `applyShardResult` and the drain loop -
  both running on the single main goroutine.

### Verification

- `go test -race -count=100 ./internal/cluster/ -run TestECObjectReader_ReadObject_MarksUnhealthyPeerOnFetchError`: 100/100 PASS, 0 DATA RACE
- `go test -race -count=100 ./internal/cluster/ -run TestECObjectReader_ReadObject_MarksHealthyPeerOnSuccess`: 100/100 PASS, 0 DATA RACE

## [0.0.174.0] - 2026-05-13: fix nbd cow snapshot cli flags

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
