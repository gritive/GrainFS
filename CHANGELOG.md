# Changelog

## [0.0.206.0] - 2026-05-15 — feat: write metadata snapshots with zstd

### Added

- **Snapshot zstd benchmark coverage** — snapshot compression benchmarks now compare gzip and zstd encode/decode behavior on representative snapshot payloads.

### Changed

- **Zstd metadata snapshots** — newly written metadata snapshots now keep the `GFSNAP01` envelope and store the JSON payload with zstd in `snapshot-<seq>.json.zst` files.
- **Snapshot compatibility policy** — legacy `.json.gz` snapshot archives are now intentionally unsupported by restore flows after the zstd cutover.
- **Rolling-upgrade docs** — compatibility docs now describe the zstd payload, `.json.zst` suffix, and older-binary suffix-level invisibility.

### Fixed

- **Legacy snapshot restore response** — direct restore of an existing `.json.gz` snapshot now returns an unsupported-format conflict instead of looking like a missing snapshot.
- **Snapshot sequence safety** — upgraded nodes seed new snapshot sequence numbers from legacy `.json.gz` filenames as well as current `.json.zst` files, avoiding sequence reuse after upgrade.

### Verification

- `make test-unit`
- `go test ./internal/snapshot -count=1`
- `go test ./internal/server -run 'TestRestore(SnapshotUnsupportedFormat|LegacyGzipSnapshot)ReturnsConflict' -count=1`
- `go test -tags compat ./tests/compat -run 'TestSnapshot(LegacyGzipRejectedByCurrent|HeadSnapshotInvisibleToOlderBinary)' -count=1`

## [0.0.205.1] - 2026-05-15 — fix: encrypted benchmark allocation hotspots

### Added

- **9P benchmark coverage** — single-node and clustered 9P benchmark scripts now mount bucket exports in Colima, run fio workloads, and collect pprof profiles alongside the existing S3, NFS, NBD, and Iceberg benchmark lanes.
- **9P directory creation** — 9P bucket directories can now be created and removed through directory marker objects, with mode metadata and collision checks for files, sidecar namespaces, and existing directories.

### Changed

- **Encrypted shard reads** — full-shard and range reads now stream/decrypt from files with pooled chunk buffers instead of allocating full encrypted copies or MiB-scale buffers per range read.
- **NFS fallback writes** — non-`WriteAt` backends now rebuild partial writes as streams instead of reading the whole object into memory, while rejecting unsafe huge sparse offsets.
- **Encrypted spool reads** — cluster spool encryption now reuses plaintext and ciphertext buffers across records.
- **NBD request buffers** — 128 KiB NBD requests now use the buffer pool instead of allocating per request.

### Fixed

- **NFS cluster benchmark mount** — the clustered NFS benchmark now creates the target bucket/export and mounts the bucket path instead of the pseudo-root.
- **9P directory correctness** — file rename and child mutation paths now respect directory marker locks, directory mode metadata, and existing directory collisions.
- **9P server close race** — closing an already-stopped listener no longer reports a spurious `use of closed network connection` error.

### Verification

- `make test-unit`
- `git diff --check origin/master && bash -n benchmarks/bench_9p_profile.sh benchmarks/bench_9p_cluster_profile.sh benchmarks/bench_nfs_cluster_profile.sh benchmarks/bench_nbd_profile.sh benchmarks/bench_nbd_cluster_profile.sh benchmarks/bench_iceberg_table.sh benchmarks/bench_iceberg_table_cluster.sh benchmarks/bench_two_node_s3_profile.sh && make bin/grainfs`
- Benchmarks run across S3, NFS, NBD, Iceberg, and 9P single/cluster profiles under `benchmarks/profiles/`

## [0.0.205.0] - 2026-05-15 — feat: searchable durable audit lake

### Added

- **Durable S3 audit outbox** — S3 request attempts and final outcomes are persisted locally before being committed to the Iceberg audit table.
- **Searchable audit schema** — audit rows now include request ID, service account, source IP, operation, auth status, error reason, version/upload/copy context, and day partition metadata for DuckDB queries.
- **Audit health and search APIs** — localhost dashboard endpoints expose outbox health and bounded S3 audit search backed by DuckDB/Iceberg.
- **Dashboard audit view** — the web UI now surfaces audit lake health and recent S3 audit events.

### Changed

- **Audit commit safety** — follower-shipped events are durably accepted by the leader, oversized wire fields are rejected/truncated before encoding, and stale provisional attempts can later be corrected by a final request outcome.
- **Internal audit bucket reads** — Iceberg artifacts remain blocked for normal S3 access except for the generated local audit reader credential or IAM-authorized artifact reads.
- **Audit docs** — `docs/audit-iceberg.md` now documents retention, query examples, dashboard behavior, and the operational guarantees.

### Verification

- `go test ./internal/audit ./internal/server ./internal/serveruntime ./internal/badgerrole -count=1`
- `make bin/grainfs && GRAINFS_BINARY=$(pwd)/bin/grainfs go test -tags duckdb_e2e ./tests/e2e -run TestAuditIcebergSingleDuckDB -count=1 -v -timeout 5m`

## [0.0.204.0] - 2026-05-15 — feat: storage operations console

### Added

- **Storage operations console** — dashboard UI and `/ui/api/storage/*` routes now expose protocol status, safe bucket list/create, and NFS export state without mounting destructive storage mutations.
- **Capability-gated NFS export create** — NFS export registration now uses create-only meta-Raft commands gated by `nfs_export_create_v1` evidence across current meta-Raft members.
- **Protocol bind status** — NFSv4, NBD, and 9P service status now reflects actual listener bind success or failure for the admin/dashboard surface.

### Changed

- **Dashboard safety boundary** — the browser/volume/snapshot UI no longer exposes object delete, bucket delete, volume delete, or snapshot rollback/delete actions through `/ui/api`.

### Fixed

- **Rolling-upgrade forwarding** — gated meta-Raft forwarding preserves legacy raw migration cutovers while rejecting raw gated NFS create commands.
- **Capability gossip delivery** — capability evidence survives the QUIC stream catch-all path, records evidence under raft member addresses, and refreshes gate TTL from replayed cluster gossip settings.

## [0.0.203.0] - 2026-05-15 — feat: snapshot format compatibility header

### Added

- **Snapshot format envelope** — newly written metadata snapshots now carry a `GFSNAP01` header with reader and writer format integers before the existing gzip JSON payload.
- **Forward-format restore guard** — restore rejects future snapshot envelopes before mutating backend state, and the admin restore API reports unsupported formats as `409 Conflict`.
- **Snapshot compatibility coverage** — tests cover header round-trips, legacy gzip-only snapshots, future-format rejection before backend mutation, API conflict responses, and the older-binary rejection compat scenario.

### Changed

- **Legacy snapshot reads** — existing gzip-only snapshots remain readable by detecting gzip magic before envelope parsing.
- **Rolling-upgrade compatibility docs** — `docs/COMPAT.md` now documents the snapshot envelope and marks `TestHeadSnapshotReject` as live.

## [0.0.202.0] - 2026-05-15 — feat: require local at-rest encryption

### Added

- **Mandatory local at-rest encryption** — local object files, multipart staging, cluster spool files, packed blobs, WAL mutation bodies, Badger metadata, and replicated FSM values are now written through the GrainFS encryption layer.
- **Encryption key bootstrap guardrails** — solo nodes can auto-create the local key, while cluster and join mode now require an explicit shared key file to avoid accidental split-key clusters.
- **Encrypted storage coverage** — tests now cover key bootstrap policy, hidden plaintext checks, wrong-key failures, metadata tampering, WAL tail handling, packblob downgrade resistance, and encrypted object `WriteAt`/`Truncate` atomic rewrites.

### Changed

- **Packblob and WAL compatibility** — encrypted records remain backward-compatible with existing plaintext records, while encrypted flags and metadata are authenticated to reject downgrade or tamper attempts.
- **Local object mutation safety** — encrypted random writes and truncates now rewrite through a temporary file with durable rename semantics instead of partially mutating ciphertext in place.
- **Smoke and benchmark bounds** — Colima/NFS smoke scripts and encryption benchmarks were adjusted for the encrypted storage path.

## [0.0.201.0] - 2026-05-15 — feat: Badger startup recovery journal

### Added

- **Badger startup recovery journal** — startup-mode decisions that happen before the incident store is available are now written under `<data>/.recovery/entries/` with node, boot, binary version, role, group, path, status, action, and scrubbed reason metadata.
- **Incident import on next healthy boot** — once the incident store opens, pending recovery journal entries are imported as deterministic Badger startup incidents and marked imported without duplicating or regressing existing incident state.
- **Recovery journal coverage** — tests now cover relative journal paths, imported markers, reason scrubbing, pre-incident meta/group startup failures, idempotent import, and startup cleanup preserving `.recovery`.

### Changed

- **Quarantine manifest writes** — recovery journal entries and quarantine manifests now share the same atomic JSON write helper.
- **Runbook guidance** — Badger startup recovery documentation now calls out `.recovery` as the pre-incident journal that should be preserved for post-boot import.

## [0.0.200.1] - 2026-05-15 — test: faster cluster unit test timing

### Changed

- **Cluster single-voter test setup** — backend and group backend helpers now poll leadership every 1ms while preserving the existing 2s cap, removing avoidable 10ms sleeps across many unit tests.
- **QUIC leadership transfer test** — reduced the special election timeout from 5s to 2.5s and added receiver-side TimeoutNow observation plus a 2s transfer deadline, keeping natural election outside the pass condition.

### Verification

- `go test -count=10 ./internal/cluster -run '^TestV2QUICCluster_ThreeNode_TransferLeadership$'`
- `go test -count=1 ./internal/cluster`

## [0.0.200.0] - 2026-05-15 — perf: zero-alloc SigV4, storage cache, and NBD reply hot paths

### Changed

- **S3 SigV4 verification** — cached verification now parses auth fields and credential scopes without building per-request maps/slices, and compares expected HMAC hex without allocating the expected signature string.
- **Storage cache hits** — cached object reads now reuse reader state and struct cache keys, reducing cache-hit allocation churn while preserving lock-free snapshot reads.
- **NBD replies** — fixed and structured reply headers now reuse fixed buffers instead of allocating header slices on steady-state transmission paths.

### Fixed

- **Header auth query handling** — header-signed S3 requests whose query values contain `X-Amz-Algorithm=` or whose query includes an empty presign marker are no longer misclassified as presigned URLs; encoded presign keys remain recognized through a cold fallback.
- **Cached reader reuse safety** — stale double-close after cached reader reuse can no longer reset an active reader.
- **Coverage build compatibility** — NBD reply header pooling now avoids the generic fixed-array pattern that triggered a Go coverage compiler ICE while keeping the zero-allocation budget.

## [0.0.199.0] - 2026-05-15 — feat: S3 audit log lake — Phase 2 (bootstrap + metrics + --audit-iceberg flag + e2e)

### Added

- **`--audit-iceberg` flag** — `cmd/grainfs serve` 에 `--audit-iceberg` (bool, default `true`) 및 `--audit-commit-interval` (duration, default `60s`) 추가. `serveruntime.Config.AuditIceberg` / `AuditCommitInterval` 필드 연동.
- **Idempotent bootstrap** — `internal/audit.Bootstrap(ctx, catalog, backend)`: startup 시 `grainfs-audit` 버킷, `audit` namespace, `audit.s3` Iceberg 테이블을 없으면 생성. 이미 존재하는 경우 all-OK.
- **Prometheus metrics** — `audit_drops_total{node}` (Counter), `audit_commit_lag_seconds{node}` (Histogram), `audit_committer_state{node}` (Gauge). `Committer.Run` 내에서 리더/팔로워 상태 전환 시 자동 업데이트.
- **Subsystem wiring** — `boot_phases_srvopts.go`: `metaCatalog` 스코프 수정 (if/else 전에 선언), `cfg.AuditIceberg` 시 Emitter + Bootstrap + Committer + `StreamAuditShip` QUIC handler 자동 연결. leader ship 함수는 `MetaProposalTargets`로 타겟 결정.
- **grainfs-audit 접근 차단** — `authzMiddleware`에 내부 버킷(`grainfs-audit`) 하드코딩 deny 추가; 모든 테넌트의 S3 API 접근 403으로 차단.
- **docs/audit-iceberg.md** — Quick Start, flag 레퍼런스, 스토리지 레이아웃, 스키마, DuckDB 쿼리 예제, 클러스터 동작 설명, Prometheus 메트릭 문서.
- **e2e 테스트** — `TestAuditIcebergSingleDuckDB`, `TestAuditIcebergClusterDuckDB`, `TestAuditIcebergClusterFollowerShipDuckDB`, `TestAuditIcebergClusterLeaderFlap`. 빌드 태그 `duckdb_e2e`. sleep 대신 `require.Eventually` 폴링.
- **`mrClusterOptions.ExtraArgs`** — e2e harness에 `ExtraArgs []string` 추가로 개별 serve 플래그 주입 가능.

### Fixed

- **audit drop 카운터** — 리더 측 followerIn 채널 오버플로 시 `audit_drops_total` 미증가 버그 수정.
- **배치 캡** — follower drain 루프 최대 65536 이벤트로 제한; `DecodeS3Batch` count > 65536 거부로 OOM 방어.
- **오해 유발 로그** — commit 실패 시 "events retained in zerolog" → "events in this batch are dropped" 수정.
- **부트스트랩 에러 레벨** — 감사 부트스트랩 에러 `Debug` → `Warn` 격상.
- **snapshot retain 경쟁 조건** — `AutoSnapshotter.takeAndPrune()`이 스냅샷 생성 전 (retain-1)개로 먼저 prune하여 순간적으로 retain 한도를 초과하는 문제 수정; 생성 후 재-prune으로 생성 중 retain 감소 케이스도 처리.

## [0.0.198.0] - 2026-05-15 — perf: xxhash3 ETag for internal buckets (~37× faster than MD5)

### Changed

- **Internal bucket write speed** — ETag computation on `__grainfs_*` write paths (WriteAt, PutObject, spool, cluster repair) now uses xxhash3 (~25 GB/s) instead of MD5 (~650 MB/s), a ~37× improvement. S3 user buckets are unaffected and continue using MD5.
- **Hash pool reuse** — `multipart.go` upload/complete/list paths now reuse a `sync.Pool`-backed MD5 hasher, eliminating per-operation allocations.
- **Algorithm-aware ETag verification** — `VerifyETag`, `ReplicationVerifier`, and `tryRepairFromPeer` detect the algorithm from ETag length (32 chars = MD5, 16 chars = xxhash3). Existing MD5 ETags verify correctly without migration.

### Fixed

- **Scrubber repair queue exhaustion** — `ReplicationVerifier` previously misreported objects with unrecognized ETag formats (e.g. multipart composite ETags) as `Corrupt`, which could exhaust the repair queue. These are now reported as `Skipped`.
- **Hasher pool lifetime** — `PutObjectWithUserMetadata` now returns the hash pool object immediately after computing the ETag rather than holding it for the duration of the rename + metadata write.

## [0.0.197.0] - 2026-05-14 — fix: lock-free storage cache audit

### Changed

- **Storage read cache locking** — `CachedBackend` now publishes immutable cache snapshots with atomic compare-and-swap instead of protecting cache state with a mutex, keeping cache hits lock-free while preserving write invalidation.
- **Lock-free audit documentation** — added a production mutex inventory and review rule that explains which locks are justified, which should stay off read hot paths, and which storage locks remain acceptable.

### Fixed

- **Volume read/write serialization** — documented `Manager.mu` as a justified mutation boundary and added regression coverage proving `ReadAt` remains serialized with concurrent `WriteAt` for block-object consistency.

## [0.0.196.0] - 2026-05-14 — feat: 9P read-write support

### Added

- **9P read-write objects** — Linux v9fs clients can create, overwrite, truncate, chmod/touch, rename, unlink, and fsync bucket objects through `grainfs serve --9p-port`.
- **9P metadata sidecars** — mode and mtime are stored under a protected `__meta/` namespace that is hidden from 9P directory listings and rejected for direct 9P access.
- **Colima read-write coverage** — `tests/9p_colima` verifies mounted 9P writes, signed HTTP visibility, stale-tail truncation, metadata operations, rename, unlink, and fsync.

### Changed

- **9P write safety** — object mutations now use per-object locks, recovery write-gate protection, backend capability preferences, bounded full-object fallbacks, same-path rename protection, and service shutdown cleanup.
- **9P fallback write performance** — user-bucket writes now coalesce per-fid `WriteAt` calls and flush once on `FSync`/`Close`, avoiding full-object read-modify-write on every 4 KiB write.
- **9P serving warning** — `--9p-port` now documents that the 9P endpoint is unauthenticated and should be kept behind a trusted network boundary.

### Fixed

- **9P user-bucket read fast path** — read capability preference is separate from write preference, preserving partial reads when partial writes are disabled for user buckets.

## [0.0.195.0] - 2026-05-14 — feat: rolling upgrade capability gates

### Added

- **Capability gate framework** — `internal/compat` defines capability names, hard-gate errors, active feature helpers, and `grainfs_capability_reject_total{capability,scope,severity,operation,forced}` telemetry for version-skew rejections.
- **Config epoch-bound meta-Raft gates** — meta-Raft proposals can now be admitted through a `CapabilityGate` that verifies every current voter has fresh readiness evidence before new metadata commands are proposed or forwarded.
- **Gated migration cutover hook** — bucket upstream cutover state is persisted through IAM/meta-Raft, and `POST /v1/migration/cutover` is rejected until the cluster advertises the migration cutover capability.
- **Rolling upgrade compat coverage** — mixed-version compat tests now verify migration cutover fails closed before all nodes are capable, and the runbook documents capability gate rejection response.

## [0.0.194.0] - 2026-05-14 — feat: S3 audit log lake — Phase 1 (Iceberg + Parquet)

### Added

- **S3 audit event schema** — `internal/audit` 패키지: `S3Event` struct (13개 필드: ts/node_id/request_id/sa_id/source_ip/method/bucket/key/http_status/bytes_in/bytes_out/latency_ms/err_class), `BucketName = "grainfs-audit"`, `TableS3 = "s3"`, Namespace 상수, Iceberg initial metadata JSON 템플릿.
- **Lock-free ring buffer** — channel 기반 bounded ring (cap=65536): `Put` (non-blocking, drop+count), `DrainInto` (zero-alloc preallocated path / allocating nil-dst path), `Drops`/`Len` 조회. Fix: `DrainInto(nil)`이 올바르게 모든 이벤트를 drain하도록 수정.
- **Emitter with recursion guard** — `audit.Emitter`: `EmitS3` 호출 시 zerolog stdout 이중 sink + ring.Put. `grainfs-audit` 버킷 또는 `system:audit` SA의 이벤트는 ring에 넣지 않아 무한 재귀 차단.
- **S3 핸들러 emit hooks** — PUT/GET/DELETE/LIST 4곳에 `WithAuditEmitter` 서버 옵션으로 활성화. `auditEmitter` nil 시 no-op (panic 없음).
- **Follower→Leader 바이너리 인코더** — `wire.go`: `EncodeS3Batch`/`DecodeS3Batch`; JSON 미사용, LittleEndian 바이너리 포맷.
- **Cluster committer** — `audit.Committer`: leader가 ring drain + follower 이벤트를 Parquet으로 인코딩 후 Iceberg snapshot 커밋 (`CommitTable` CAS). Follower는 `ShipToLeader` 콜백으로 이벤트 전달. `followerIn chan []S3Event`(cap=256) non-blocking.
- **Parquet encoder** — Arrow-go v18 + pqarrow: Snappy 압축, 13컬럼 Iceberg field ID 메타데이터 포함. DuckDB `read_parquet()` 직독 검증 완료.
- **Minimal Avro encoder** — Iceberg manifest + manifest list를 외부 라이브러리 없이 직접 Avro Object Container File 포맷으로 생성.
- **StreamAuditShip = 0x13** — `internal/transport/transport.go`에 follower→leader audit ship QUIC stream type 등록.

### Fixed

- **iceberg_api.go stale 에러 텍스트** — `--audit-iceberg` 플래그 없이 Iceberg REST Catalog 접근 시 반환되는 에러 메시지를 실제 조건에 맞게 수정.

## [0.0.193.0] - 2026-05-14 — feat: NFS multi-export DX and benchmarks

### Added

- **NFS export diagnostics** — `grainfs nfs debug <bucket>` reports registry state, backend bucket existence, recent pseudo-root LOOKUPs, and available client diagnostics in text or JSON.
- **NFS multi-export observability** — Prometheus now exposes export totals, propagation latency, unknown export LOOKUPs, and revoked stateid counters, with a sample Grafana dashboard in `docs/observability/nfs-multi-export.json`.
- **NFS profiling benchmarks** — `make bench-nfs-multi` runs a bounded multi-bucket Colima/fio workload with pprof capture, per-bucket throughput, and pseudo-root READDIR latency output.

### Changed

- **NFS export CLI JSON flags** — `grainfs nfs export` commands now use `--json`, matching bucket and IAM commands, and reject `--quiet --json`.
- **Benchmark defaults** — NFS profiling workloads now use bounded default sizes and `--fallocate=none` so local profiling completes and produces usable pprof data by default.
- **NFS runbooks** — README, RUNBOOK, `docs/nfs-export-lifecycle.md`, and `docs/nfs-debug.md` now document export lifecycle, debugging, and benchmark workflows.

### Fixed

- **NFS export admin errors** — `bucket_not_found` and `export_not_found` return 404, `export_already_exists` returns 409, and propagation timeouts return 504.
- **NFS write lock isolation** — writes and truncates for the same object key in different buckets no longer share one lock.
- **NFS debug truthfulness** — debug output no longer claims unavailable propagation/client state as healthy, applies admin timeouts, and keeps the NFS hint sweeper closed during runtime shutdown.

## [0.0.192.1] - 2026-05-14 — feat: unknown MetaCmd telemetry

### Added

- **Unknown MetaCmd visibility** — operators now get `grainfs_unknown_metacmd_total{type}` when a node ignores a raft metadata command it does not recognize or handle.
- **Rolling-upgrade alerting** — Prometheus rule `GrainFSUnknownMetaCmdIgnored` warns on ignored MetaCmd events, including first-seen counter series, and the runbook explains the version-skew response path.

## [0.0.192.0] - 2026-05-14 — feat: read-only 9P2000.L server

### Added

- **Read-only 9P2000.L server** — `grainfs serve --9p-port` can expose buckets and objects over 9P for Linux/Colima clients while remaining disabled by default.
- **9P directory and object coverage** — unit tests cover bucket listing, object reads, nested slash-containing object keys via synthetic directories, aname bucket roots, and paged Readdir behavior.
- **Colima 9P harness** — `make test-9p-colima` adds an opt-in Linux mount/read smoke test lane.

### Changed

- **Fast object-key walking** — local storage now provides `WalkObjectKeys` so 9P directory listing can iterate keys without unmarshalling object metadata.

## [0.0.190.1] - 2026-05-14 — feat: rolling upgrade CI compat lane (Slice 1)

### Added

- **Rolling upgrade compat test lane** — `tests/compat/` package with 6 live cross-version scenarios and 1 stubbed placeholder for the snapshot version header (Slice 3). Run with `make test-compat`; tests skip gracefully when `COMPAT_PREV_BIN` is not set.
- **Compat policy document** — `docs/COMPAT.md` defines the N → N+1 rolling upgrade policy, scenario table, and developer guide for adding new compat tests.
- **Slice 4 design document** — `docs/upgrade-finalize-machinery-design.md` covers the `upgrade finalize` command, StateHash FSM divergence detection, snapshot version header, and drain/rollback procedure.

## [0.0.190.0] - 2026-05-14 — feat: NFSv4.1 RFC 8881 audit

### Added

- **NFSv4.1 compliance matrix** — operators can now inspect RFC 8881 Section 5.8 attribute coverage in `docs/nfsv4-compliance.md`, including Done, Partial, and Skipped rows with code citations and follow-up gaps.
- **pynfs conformance scaffold** — `tests/conformance/run_pynfs.sh`, `make test-pynfs-colima`, and the conformance README provide an advisory path for running external NFSv4.1 checks against a local GrainFS export.
- **NFS standards documentation** — README and runbook entries now point to the compliance matrix, conformance runner, and operational expectations for advisory pynfs results.

### Fixed

- **GETATTR attribute bitmaps** — NFSv4 GETATTR now supports the third attribute bitmap word, including RFC 8881 bit 75 `suppattr_exclcreat`.
- **NFS attribute truthfulness** — `cansettime` is advertised on bit 15 instead of the deprecated archive bit, and link/symlink support attributes now report unsupported operations accurately.
- **READDIR requested attrs** — real COMPOUND READDIR requests now preserve and honor requested entry attributes instead of dropping the bitmap during XDR argument decoding.
- **Colima conformance binary** — the pynfs Colima target now builds `grainfs` inside the Linux VM so macOS host binaries are not executed in Colima.

## [0.0.189.1] - 2026-05-14 — fix: bucket policy/versioning handler correctness

### Fixed

- **Bucket existence pre-check** — policy and versioning admin endpoints (`GET/PUT/DELETE /v1/buckets/{name}/policy`, `GET/PUT /v1/buckets/{name}/versioning`) now return `404 not_found` instead of a storage-layer error when the bucket does not exist. A `checkBucketExists` helper is called after the internal-bucket guard and before the storage operation.
- **Policy `ErrBucketNotFound` → 404** — `GetBucketPolicy` now maps `storage.ErrBucketNotFound` (returned by `LocalBackend` when no policy key is present) to `404 not_found` instead of `500 internal`.
- **Policy structure validation** — `AdminSetBucketPolicy` now rejects non-JSON and structurally invalid policies (e.g., top-level string instead of object) at the handler layer via `policy.ParsePolicy`, before any storage write.
- **Effect case validation** — `policy.ParsePolicy` now rejects `Effect` values other than `"Allow"` or `"Deny"`, preventing silently inoperative policies caused by case typos (`"DENY"`, `"allow"`, etc.).
- **Ghost policy on bucket delete** — `LocalBackend.DeleteBucket` now also deletes the `policy:<bucket>` BadgerDB key, so a recreated bucket with the same name does not inherit the previous bucket's policy.
- **Backward-compatible policy cache warm-up** — `Operations.GetBucketPolicy` no longer propagates `CompiledPolicyStore.Set` errors to callers; a pre-existing policy with a non-conforming `Effect` is still returned as raw bytes via the admin API while being skipped for S3 authorization (default deny), allowing operators to read and fix it.

## [0.0.189.0] - 2026-05-14 — fix: meta-Raft apply result delivery

### Transport

- **FIX**: Capability exchange now enforces strict 2-byte payload length;
  truncated frames are rejected with `payload_length` reason. (F1)
- **FIX**: CE failure modes now produce distinguishable peer-visible errors
  (`version_mismatch`, `wrong_first_stream`, `payload_length`,
  `feature_unsupported`, `timeout`, `io_error`). Replaces single generic
  "capability exchange failed" close message. (F3)
- **NEW**: Prometheus metric `grainfs_transport_ce_total{role,outcome,reason}`
  emitted on every CE attempt. (F7)
- **NEW**: CE features byte has an explicit reserved-bit policy — unknown bits
  in `features` reject with `feature_unsupported`. Registry at
  `docs/transport-mux-versioning.md`. (F2)
- **TEST**: Concurrent mux dial dedup race coverage added. (F6)
- **DOC**: `docs/transport-mux-versioning.md` — wire format, feature registry,
  version bump policy, v1 baseline rationale. (F5)

### Fixed

- **Meta-Raft apply errors** — proposals now return FSM apply failures after the committed index applies, so callers do not report success when the replicated metadata write failed.
- **Forwarded proposal visibility** — follower-forwarded writes now wait for bounded follower-local apply before returning, preserving local read-after-write behavior without tying latency to the full caller timeout.
- **Forwarded apply error types** — non-Iceberg FSM errors now cross the follower-to-leader forwarding boundary as `MetaForwardApplyError` instead of being collapsed into service-unavailable.
- **Raft-over-QUIC test setup** — raft QUIC cluster tests now retry connection setup with shorter per-attempt dial deadlines and a wider outer retry budget, reducing full-suite connection flakes without making each failed dial stall.

## [0.0.188.0] - 2026-05-14 — feat: NFS export propagation follow-up

### Added

- **Multi-node NFS export propagation** — admin export add, update, remove, and bucket-delete cascade operations now wait for the committed meta-Raft index to apply before reporting success.
- **Bucket-delete cascade coverage** — process-level E2E coverage now verifies exported bucket deletion removes the export on success and preserves it when deletion or propagation fails.

### Fixed

- **Safe exported bucket deletion** — exported bucket deletion now records a durable cleanup marker and completes the NFS export cascade after the bucket delete succeeds, so crash or cascade failures can be retried without pre-removing a live bucket export.
- **User export partial-I/O fallback** — NFSv4 user-bucket exports now honor backend `PreferWriteAt`/`PreferReadAt` hints so writes, truncate, allocate, rename, and copy fall back to object-store paths instead of internal-bucket-only fast paths.
- **Cluster E2E UDP port race** — the five-node QUIC/static E2E now binds UDP listeners atomically instead of reserving free ports before parallel test startup.

## [0.0.187.0] - 2026-05-14 — feat: NFSv4 multi-export registry and routing

### Added

- **NFS export registry** — cluster metadata now stores NFS export registrations with stable fsid/generation fields, and the admin API plus `grainfs nfs export` CLI can add, update, list, and remove exports.
- **NFSv4 pseudo-root multi-export routing** — NFS clients can browse registered buckets under the pseudo-root and route file operations to the selected bucket instead of the legacy fixed bucket.
- **Read-only export enforcement** — write, create, remove, rename, setattr, allocate, deallocate, and copy operations now reject mutations against read-only exports.
- **Export lifecycle E2E coverage** — CLI lifecycle tests cover export add/update/remove JSON output, missing-bucket rejection, and fsid/generation fields.
- **Fail-closed export lifecycle** — bucket deletion now rejects exported buckets instead of best-effort cascading the export first, and multi-node clusters reject NFS export mutations until a full propagation barrier is wired.
- **`GRAINFS_LOG_LEVEL` fallback** — `grainfs --log-level` still wins when explicitly provided, otherwise the CLI uses `GRAINFS_LOG_LEVEL` before falling back to `info`.

### Changed

- **NFSv4 legacy bucket hard removal** — `__grainfs_nfs4` is no longer an internal bucket and the NFSv4 server no longer auto-creates or routes through it.
- **E2E parallelism control** — `make test-e2e` now runs per-test invocations in parallel via `E2E_TEST_JOBS` (default `2`; set `E2E_TEST_JOBS=1` for serial execution).
- **NFS metadata cache keys** — NFSv4 metadata invalidation and file metadata cache entries are now bucket-aware.

### Fixed

- **Forwarded short reads** — cluster `ReadAt` forwarding now preserves short EOF reads instead of converting them to internal errors.
- **Empty EC objects** — EC-backed user buckets now accept zero-byte object writes, matching create/truncate flows used by NFS clients.
- **Deterministic export fsid allocation** — NFS export fsid minor and generation values are now assigned during meta-Raft apply, avoiding stale local-service allocation decisions.
- **Cross-export guards** — NFSv4 rename/copy across different exports now returns `NFS4ERR_XDEV`, and destination writes use the destination bucket.
- **Stale export handles** — filehandles bound to an older export generation now expire with `NFS4ERR_FHEXPIRED`; removed exports return `NFS4ERR_ADMIN_REVOKED`.
- **Live export refresh** — Raft-applied export registry changes now refresh the running NFSv4 server snapshot instead of requiring restart.

## [0.0.186.1] - 2026-05-14 — docs: DX polish — NFS/NBD/Iceberg Quick Start

### Added

- **NFSv4 Quick Start** — README에 Phase 7 multi-export 기준 5-step 마운트 가이드 추가 (`grainfs nfs export add` → pseudo-root mount → `/mnt/<bucket>/`).
- **NBD Quick Start (Linux)** — README에 `nbd-client` 설치부터 `mkfs.ext4` + mount까지 Linux 전용 Quick Start 추가.
- **Iceberg IAM 연결 안내** — `docs/iceberg-duckdb.md`에 `grainfs iam sa create` 결과(`access_key`/`secret_key`)를 DuckDB SECRET에 매핑하는 1단락 안내 추가.
- **`GRAINFS_ADMIN_SOCKET` 환경변수** — Quick Start에 `export GRAINFS_ADMIN_SOCKET=...` 추가해 이후 명령에서 `--endpoint` 생략 가능.

### Fixed

- **`--nbd-port` 기본값 오류** — README에 `default 0=비활성`으로 잘못 기재된 기본값을 실제 코드 값인 `10809`로 수정.

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
