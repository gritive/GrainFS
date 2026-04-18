# Changelog

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
