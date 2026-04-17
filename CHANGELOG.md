# Changelog

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
