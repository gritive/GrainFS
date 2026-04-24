# Zero-copy 최적화 설계 스펙

**날짜:** 2026-04-24  
**범위:** EC 핫패스 copy 최소화 + Zero-copy Protocol Bridge (NFS→S3)

---

## 목표

1. EC 클러스터 경로에서 발생하는 불필요한 메모리 복사 횟수를 줄여 처리량과 메모리 압력을 개선한다.
2. NFS ↔ S3 경로에 중간 버퍼 없이 데이터를 스트리밍하는 Protocol Bridge를 구현한다.

---

## 범위 밖

- `io_uring`, `SPDK` — Phase 19 TODOS의 별도 항목, 이 스펙에서 제외
- `Unified Buffer Cache` — 아키텍처 변경 규모가 커 별도 스펙으로 분리
- EC PUT의 `io.ReadAll` 제거 — EC split에 전체 데이터가 필요하므로 불가, 유지

---

## Sub-system 1: EC 핫패스 Copy 최소화

### 현황 분석

| 위치 | 현재 패턴 | 복사 횟수 |
|------|-----------|-----------|
| `ec.go:ECSplit` | double-append per shard | K+M + grow overhead |
| `backend.go:GetObject` | `io.NopCloser(bytes.NewReader(data))` x3 | +1 래핑 per call |
| `backend.go:PutObject` | `io.ReadAll(r)` | 불가피, 유지 |

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
- capacity 초과 시 내부 grow 로직 제거
- alloc 횟수 동일(1회), grow 가능성 제거

### GetObject 스트림 래핑 정리

**파일:** `internal/cluster/backend.go`

ECReconstruct 결과(`[]byte`)를 `bytes.NewReader`로 래핑해 반환하는 현재 방식은 유지하되, 호출 지점(`SetBodyStream` 등)에서 직접 소비하도록 경로를 정리한다. 래핑 자체는 인터페이스 계약상 필요하지만 중복 래핑(NopCloser + NewReader 이중)은 제거한다.

---

## Sub-system 2: Zero-copy Protocol Bridge

### 목표

NFS 파일 읽기 요청이 들어올 때 S3 GET 응답 body를 중간 `[]byte` 버퍼 없이 NFS 소켓으로 직접 스트리밍한다. NFS 파일 쓰기는 S3 PUT 스트림으로 직접 연결한다.

### 인터페이스

**신규 파일:** `internal/cluster/zero_copy_bridge.go`

```go
type ZeroCopyBridge struct {
    backend Backend
    bucket  string
}

func NewZeroCopyBridge(backend Backend, bucket string) *ZeroCopyBridge

// NFSReadToS3: NFS read 요청 → S3 GET → w로 직접 스트림
func (b *ZeroCopyBridge) NFSReadToS3(ctx context.Context, key string, w io.Writer) (int64, error)

// S3PutFromNFS: NFS write 데이터(r) → S3 PUT으로 직접 전달
func (b *ZeroCopyBridge) S3PutFromNFS(ctx context.Context, key string, size int64, r io.Reader) error
```

### 데이터 플로우

```
NFS Read:
[NFS client]
    → NFSHandler.Read()
    → ZeroCopyBridge.NFSReadToS3(ctx, key, nfsWriter)
        → backend.GetObject(ctx, bucket, key) → io.ReadCloser
        → io.Copy(nfsWriter, rc)         ← 중간 []byte 버퍼 없음
        → rc.Close()

NFS Write:
[NFS client]
    → NFSHandler.Write(data)
    → ZeroCopyBridge.S3PutFromNFS(ctx, key, size, nfsReader)
        → backend.PutObject(ctx, bucket, key, nfsReader)
            → 내부: io.ReadAll (EC split 필수, 불가피)
```

### NFS 핸들러 연동

**수정 파일:** `internal/nfsserver/` 내 파일 읽기/쓰기 핸들러

- NFS 파일 읽기 핸들러: `LocalBackend` 직접 호출 대신 `ZeroCopyBridge.NFSReadToS3` 사용
- NFS 파일 쓰기 핸들러: `ZeroCopyBridge.S3PutFromNFS` 사용
- `ZeroCopyBridge`는 서버 초기화 시 주입 (의존성 주입 패턴 유지)

### 에러 처리

| 상황 | 처리 |
|------|------|
| `GetObject` 실패 | `NFS3ERR_NOENT` 또는 `NFS3ERR_IO` 반환 |
| `io.Copy` 중 네트워크 끊김 | 에러 반환 → NFS 계층에서 `NFS3ERR_IO` |
| `PutObject` 실패 | `NFS3ERR_IO` 반환 |

---

## 테스트 전략

### Unit Tests

| 파일 | 테스트 내용 |
|------|-------------|
| `internal/cluster/ec_test.go` | ECSplit 기존 테스트 통과 확인 + `BenchmarkECSplit` allocs 측정 |
| `internal/cluster/backend_test.go` | GetObject 스트림 소비 정확성 |
| `internal/cluster/zero_copy_bridge_test.go` | NFSReadToS3, S3PutFromNFS (mock backend 사용) |

### E2E Tests

- NFS mount → 파일 읽기 → S3 GET 동일 내용 검증
- NFS 파일 쓰기 → S3 GET으로 내용 확인

### 벤치마크

- `BenchmarkECSplit`: allocs/op가 기존 대비 동일하거나 감소
- `BenchmarkGetObject`: 메모리 할당 횟수 측정

---

## 변경 파일 요약

| 파일 | 변경 유형 |
|------|-----------|
| `internal/cluster/ec.go` | 수정 — ECSplit double-append 제거 |
| `internal/cluster/backend.go` | 수정 — GetObject 중복 래핑 정리 |
| `internal/cluster/zero_copy_bridge.go` | 신규 — ZeroCopyBridge 타입 |
| `internal/cluster/zero_copy_bridge_test.go` | 신규 — Bridge 유닛 테스트 |
| `internal/nfsserver/` | 수정 — Bridge 연동 |

---

## 구현 순서

1. ECSplit 최적화 + 벤치마크 테스트 (독립적, 위험 낮음)
2. GetObject 래핑 정리 (기존 테스트로 회귀 검증)
3. `ZeroCopyBridge` 구현 + 유닛 테스트
4. NFS 핸들러 연동 + E2E 테스트
