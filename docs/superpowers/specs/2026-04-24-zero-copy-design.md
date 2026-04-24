# Zero-copy 최적화 설계 스펙

**날짜:** 2026-04-24  
**범위:** EC 핫패스 copy 최소화 + VFS 스트리밍 개선 (NFS→S3 경로)

---

## 목표

1. EC 클러스터 경로에서 발생하는 불필요한 메모리 복사 횟수를 줄여 처리량과 메모리 압력을 개선한다.
2. NFS 파일 읽기 요청 시 `grainFile`이 중간 `[]byte` 버퍼 없이 S3 GET 응답을 직접 스트리밍한다.

---

## 범위 밖

- `io_uring`, `SPDK` — Phase 19 TODOS의 별도 항목, 이 스펙에서 제외
- `Unified Buffer Cache` — 아키텍처 변경 규모가 커 별도 스펙으로 분리
- EC PUT의 `io.ReadAll` 제거 — EC split에 전체 데이터가 필요하므로 불가피, 유지
- `ZeroCopyBridge` in `internal/cluster/` — `cluster.Backend`가 존재하지 않음. NFS는 `storage.Backend`를 사용하므로 VFS 스트리밍으로 대체
- Peer fetch QUIC stream 직접 반환 — 별도 스펙 필요 (TODOS.md에 등록)
- `sendfile(2)` / `splice(2)` — Linux-only, darwin 개발환경 미지원
- NBD 경로 zero-copy — Linux-only, Docker 테스트 제약

---

## EC+VFS 이중 복사 경로 (현황)

```
EC 읽기:
  getObjectEC()
    → EC 복원 → []byte   ← 1st copy (EC shard 조합, 불가피)
    → io.NopCloser(bytes.NewReader(data)) 반환

VFS grainFile.loadExisting():
    → io.ReadAll(rc)     ← 2nd copy ([]byte → bytes.Buffer, 제거 대상)
    → bytes.NewBuffer(data)
```

**VFS 스트리밍 모드**는 순차 읽기에서 2nd copy를 제거한다.  
로컬 파일 경로(`*os.File`)는 이미 zero-copy이므로 개선 효과는 EC 백엔드 경로에서 나타난다.

---

## Sub-system 1: EC 핫패스 Copy 최소화

### 현황 분석

| 위치 | 현재 패턴 | 개선 |
|------|-----------|------|
| `ec.go:ECSplit` | `append` 2회 (header + shard) | `copy` 2회로 교체 (append 오버헤드 감소) |
| `backend.go:GetObject` | `io.NopCloser(bytes.NewReader(data))` | EC reconstruction이 `[]byte` 반환, 래핑 불가피 |
| `backend.go:PutObject` | `io.ReadAll(r)` | EC split 필수, 유지 |

### ECSplit 개선

**파일:** `internal/cluster/ec.go`

```go
// Before
payload := make([]byte, 0, shardHeaderSize+len(s))
payload = append(payload, header[:]...)
payload = append(payload, s...)

// After
payload := make([]byte, shardHeaderSize+len(s))
copy(payload, header[:])
copy(payload[shardHeaderSize:], s)
```

- `append` 2회 → `copy` 2회
- append 오버헤드 감소 (capacity는 이미 정확히 사전 할당됨)
- alloc 횟수 동일(1회)

---

## Sub-system 2: VFS 스트리밍 개선

### 목표

NFS 순차 읽기 요청이 들어올 때 S3 GET 응답 body를 중간 `[]byte` 버퍼 없이 NFS 소켓으로 직접 스트리밍한다.

### 현재 `grainFile` 구조

**파일:** `internal/vfs/vfs.go`

```go
type grainFile struct {
    fs     *GrainVFS
    path   string
    name   string
    flag   int
    perm   os.FileMode
    buf    *bytes.Buffer  // nil until loaded
    pos    int64
    closed bool
}
```

### 변경 후 `grainFile` 구조

```go
type grainFile struct {
    fs     *GrainVFS
    path   string
    name   string
    flag   int
    perm   os.FileMode
    buf    *bytes.Buffer    // nil until loadExisting() called
    pos    int64
    closed bool
    rc     io.ReadCloser    // 스트리밍 모드: 소비 완료 또는 Seek/ReadAt 전 사용
}
```

### 데이터 플로우 (스트리밍 모드)

```
NFS Read (순차):
[NFS client]
    → NFSHandler.Read()
    → grainFile.Read(buf)
        → rc != nil → rc.Read(buf)   ← 2nd copy 없음
        → pos 업데이트

NFS Read (Seek/ReadAt 필요 시):
    → grainFile.Seek() / ReadAt()
        → loadExisting(): io.ReadAll(rc) → bytes.Buffer
        → rc.Close(); rc = nil
        → 이후 buf 기반 접근

NFS Write:
    → grainFile.Write(data)
        → backend.PutObject(ctx, ..., reader)
            → 내부: io.ReadAll (EC split 필수, 불가피)
```

### 동작 규칙

| 상황 | 동작 |
|------|------|
| `Open(O_RDONLY)` | `backend.GetObject` 호출 → `rc` 저장, `buf = nil` |
| `Read()` (rc 모드) | `rc.Read()` 직접 호출, pos 업데이트 |
| `Seek()` 또는 `ReadAt()` | `loadExisting()` 호출: rc → io.ReadAll → buf; rc.Close(); rc = nil |
| `Close()` (rc 미소비) | `rc.Close()` 호출 (리소스 릵 방지) |
| 쓰기 경로 | 기존 동작 유지 |

### 에러 처리

| 상황 | 처리 |
|------|------|
| `GetObject` 실패 | `Open()` 에러 반환 |
| `rc.Read()` 중 네트워크 끊김 | 에러 반환 → NFS 계층 `NFS3ERR_IO` |
| `loadExisting()` 중 `GetObject` 실패 | 에러 반환 → NFS 에러 |
| `Close()` rc 미소비 | `rc.Close()` 호출, 에러 무시 |

---

## 테스트 전략

### Unit Tests

| 파일 | 테스트 내용 |
|------|-------------|
| `internal/cluster/ec_test.go` | ECSplit 기존 테스트 통과 확인 + `BenchmarkECSplit` allocs 측정 |
| `internal/vfs/vfs_test.go` | **신규: TestGrainFileStreamRead** — rc 스트리밍 순차 Read → Close, rc.Close() 확인 |
| `internal/vfs/vfs_test.go` | **신규: TestGrainFileSeekFallback** — Read 후 Seek → loadExisting() 트리거 확인 |
| `internal/vfs/vfs_test.go` | **신규: TestGrainFileCloseWithUnconsumedRC** — Read 없이 Close → rc.Close() 호출 확인 (리소스 릵 방지) |

### E2E Tests

- NFS mount → 순차 파일 읽기 → S3 GET 동일 내용 검증
- NFS mount → Seek 포함 읽기 → 정확성 검증

### 벤치마크

- `BenchmarkECSplit`: allocs/op가 기존 대비 동일하거나 감소
- `BenchmarkVFSRead`: EC 백엔드 기준 메모리 할당 횟수 측정

---

## 변경 파일 요약

| 파일 | 변경 유형 |
|------|-----------|
| `internal/cluster/ec.go` | 수정 — ECSplit double-append → copy 교체 |
| `internal/vfs/vfs.go` | 수정 — grainFile 스트리밍 모드 (rc 필드 추가, Read/Seek/Close 로직) |
| `internal/vfs/vfs_test.go` | 수정 — 스트리밍 유닛 테스트 3개 추가 |

---

## 구현 순서

1. ECSplit 최적화 + 벤치마크 테스트 (독립적, 위험 낮음)
2. VFS 스트리밍 구현 + 유닛 테스트 3개 (독립적)
3. E2E 테스트 (1, 2 완료 후)

> 단계 1과 2는 서로 다른 패키지로 병렬 작업 가능.

---

## GSTACK REVIEW REPORT

**리뷰 날짜:** 2026-04-24  
**스킬:** plan-eng-review  
**상태:** CLEAR (이슈 6개, 전부 해결)

### 해결된 이슈

| # | 섹션 | 내용 | 결정 |
|---|------|------|------|
| 1 | Architecture | ZeroCopyBridge 패키지 오류 (`cluster.Backend` 미존재) | Bridge 제거, VFS 스트리밍으로 대체 |
| 2 | Architecture | 스트리밍 모드 — Seek 전 read-only 스트리밍 설계 | Streaming read only mode 추가 |
| 3 | Code Quality | ECSplit "grow 제거" 표현 부정확 (실제로는 append 오버헤드 감소) | 스펙 표현 수정 |
| 4 | Code Quality | `ReadAt()`도 Seek와 동일한 fallback 필요 | ReadAt fallback 명시 |
| 5 | Tests | rc.Close() 리소스 릵 + ReadAt fallback 테스트 누락 | 유닛 테스트 3개 추가 |
| 6 | Performance | EC+VFS 이중 복사 경로 미명시 | 데이터 플로우 섹션 추가 |

### NOT in scope (확정)

io_uring, SPDK, Unified Buffer Cache, EC PUT io.ReadAll, ZeroCopyBridge in cluster/, Peer fetch QUIC stream, sendfile/splice, NBD 경로

### Critical gaps

- `grainFile.Close()` rc 미소비 시 `rc.Close()` 누락 → 테스트 #3으로 커버 필수
