# Changelog

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
