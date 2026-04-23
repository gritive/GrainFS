# Zero-Allocation / Zero-Copy 최적화 설계

**날짜:** 2026-04-24  
**Phase:** 19 (Performance)  
**목표:** 예방적 최적화 — 관측된 병목 없이 Phase 19 전에 핫패스 메모리 할당 기반 다지기  
**전략:** 독립 최적화 (독립 PR 5개) + 위험도 낮은 순서 구현

---

## 범위

TODOS.md Phase 19에서 다음 5개 항목:

1. Reed-Solomon 버퍼 재사용 with sync.Pool
2. FlatBuffers codec zero-allocation (io.WriterTo)
3. hertz: Zero-copy Read/Write (sendfile 경로)
4. BinaryCodec io.WriterTo 통합
5. Zero-copy Protocol Bridge (NFS→S3)

---

## 설계 원칙

- **독립 최적화:** 각 항목은 별개 패키지, 별개 PR. 공유 풀 인프라 없음 (데이터 타입이 달라 통합 불가).
- **행동 불변:** 최적화는 alloc 감소만, 외부 API / 의미론 변경 없음.
- **회귀 방지:** 각 항목마다 `testing.AllocsPerRun` 기반 alloc 상한 테스트 추가.
- **구현 순서:** codec 계층 → 전송 계층 → 브리지 (위험도 오름차순).

---

## Layer 1: Codec 계층

### 1-A. Reed-Solomon encoder + shard 풀

**현황:**  
`ECSplit` / `ECReconstruct` 호출마다 `reedsolomon.New()` + `enc.Split()` 내부에서 `[][]byte` 샤드 슬라이스 신규 할당. 기본 설정(4+2)에서 호출당 추정 ~30 alloc (구현 전 `testing.AllocsPerRun`으로 실측 필수).

**변경:**  
`internal/cluster/ec_pool.go` 신규 파일:

```go
type encoderPool struct {
    pool sync.Pool  // *reedsolomon.Encoder 재사용 (설정 동일 시 stateless)
    cfg  ECConfig
}

type shardPool struct {
    pool     sync.Pool  // [][]byte (k+m 슬라이스) 재사용
    n        int        // k+m
    shardCap int        // 샤드당 byte capacity; Put 시 각 shard를 s[:0]으로 reset
}

// 조회: var encoderPools sync.Map // key: ECConfig, value: *encoderPool
```

`ECSplit` / `ECReconstruct`는 기존 시그니처 유지, 내부에서 풀 사용.  
ECConfig별로 풀 인스턴스 분리 (설정이 다른 버킷 혼용 가능).

**목표 alloc:** 호출당 ≤5 (현재 ~30)

---

### 1-B. FlatBuffers io.WriterTo

**현황:**  
`BinaryCodec.Encode`가 FlatBuffers 직렬화 후 `[]byte`를 중간 버퍼에 담아 스트림에 복사.

**변경:**  
`internal/transport/codec.go`:

```go
// Encode: msg가 io.WriterTo 이면 직접 쓰기, 아니면 기존 경로 (builder pool + copy)
func (c *BinaryCodec) Encode(w io.Writer, msg Message) error {
    if wt, ok := msg.(io.WriterTo); ok {
        _, err := wt.WriteTo(w)
        return err
    }
    // fallback: 기존 경로
}
```

FlatBuffers로 직렬화된 메시지 타입에 `WriteTo(w io.Writer) (int64, error)` 구현 추가.  
fallback 경로 유지로 마이그레이션 점진 가능.

**목표 alloc:** cluster 내부 통신 메시지 직렬화 복사 1회 제거

---

## Layer 2: 전송 계층

### 2-A. hertz Zero-copy (sendfile 경로)

**현황:**  
16KB 이상은 `SetBodyStream`으로 스트림 전달하지만 hertz 내부에서 response body 버퍼 복사 발생.

**변경:**  
`internal/server/handlers.go`:

```go
func getObject(s *Server, c *app.RequestContext) {
    rc, obj, err := s.storage.GetObject(bucket, key)

    // 스토리지가 io.WriterTo 반환 시 (예: *os.File) → sendfile 경로
    if wt, ok := rc.(io.WriterTo); ok {
        c.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
            if _, err := wt.WriteTo(w); err != nil {
                hlog.Errorf("sendfile WriteTo error: %v", err)
                // 연결 레벨에서 에러 처리됨; 여기서 추가 조치 불필요
            }
        })
        return
    }
    // fallback: 기존 SetBodyStream
    if obj.Size > 16*1024 {
        c.Response.SetBodyStream(rc, int(obj.Size))
    } else {
        data, _ := io.ReadAll(rc)
        c.Data(consts.StatusOK, obj.ContentType, data)
    }
}
```

스토리지 백엔드가 `io.WriterTo`를 구현하지 않으면 기존 경로로 자동 fallback.

---

### 2-B. BinaryCodec io.WriterTo 통합

**현황:**  
1-B의 `Encode` 개선과 짝으로, QUIC 스트림 핸들러에서 codec을 통해 응답을 쓸 때도 동일 경로 적용.

**변경:**  
`internal/transport/quic.go`의 `handleStream`에서 응답 인코딩 시 1-B의 `WriterTo` 경로가 자동 적용됨을 확인.  
QUIC 스트림(`*quic.Stream`)이 `io.Writer`이므로 `BinaryCodec.Encode`에 그대로 전달되면 1-B 경로 진입. `bufio.Writer` 래핑이 필요한지 실측 후 결정. 코드 변경 없이 검증만으로 완료될 수도 있음.

---

## Layer 3: 브리지

### 3. NFS → S3 Zero-copy Protocol Bridge

**현황:**  
NFS 읽기 결과를 `io.ReadAll`로 메모리에 전부 적재 후 S3 PUT. 파일 크기만큼 힙 사용.

**변경:**  
`internal/vfs/bridge.go` 신규:

```go
func copyNFStoS3(nfsReader io.ReadCloser, s3Put func(io.Reader) error) error {
    pr, pw := io.Pipe()

    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        _, err := io.Copy(pw, nfsReader)
        pw.CloseWithError(err)  // nil이면 정상 EOF, 에러면 에러 전파
    }()

    err := s3Put(pr)
    pr.CloseWithError(err)  // s3Put 실패 시 writer goroutine에 종료 신호
    wg.Wait()
    return err
}
```

goroutine 1개 추가되지만 메모리 상한이 OS 파이프 버퍼(~64KB)로 고정.  
에러 전파: `pw.CloseWithError`로 S3 PUT 측에 즉시 전달.

---

## 테스트 전략

### alloc 회귀 방지 (항목별 추가)

```go
func TestECSplit_AllocsBounded(t *testing.T) {
    cfg := ECConfig{DataShards: 4, ParityShards: 2}
    data := make([]byte, 1<<20)
    allocs := testing.AllocsPerRun(100, func() {
        shards, _ := ECSplit(cfg, data)
        _ = shards
    })
    assert.LessOrEqual(t, allocs, 5.0)
}
```

각 PR에 동일 패턴의 `AllocsPerRun` 테스트 필수.

### 정확성 검증

기존 E2E 테스트 (`tests/e2e/`) 전부 통과 필수. 최적화는 행동 변경 없음.

---

## 구현 순서

| PR | 항목 | 파일 | 위험도 |
|----|------|------|--------|
| 1 | RS encoder + shard 풀 | `internal/cluster/ec_pool.go`, `ec.go` | 낮음 |
| 2 | FlatBuffers io.WriterTo | `internal/transport/codec.go`, 메시지 타입들 | 낮음 |
| 3 | hertz sendfile 경로 | `internal/server/handlers.go` | 중간 |
| 4 | BinaryCodec WriterTo 통합 | `internal/transport/quic.go` | 중간 |
| 5 | NFS→S3 pipe bridge | `internal/vfs/bridge.go`, vfs 호출부 | 높음 |

PR 1, 2는 서로 다른 패키지이므로 병렬 구현 가능.  
PR 5는 vfs 아키텍처 변경이므로 마지막, PR 1-4 완료 후 진행.

---

## 비고

- PR 3 (hertz sendfile): hertz `SetBodyStreamWriter` API 확인 필요. 버전에 따라 API 이름 다를 수 있음.
- PR 2, 4: FlatBuffers 메시지 타입 수가 많으면 코드 생성기 활용 검토.
- 전 항목 완료 후 `make test-e2e`로 전체 E2E 통과 확인.
