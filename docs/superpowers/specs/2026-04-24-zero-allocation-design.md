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

**목표 alloc:** 구현 전 `testing.AllocsPerRun(5, ...)` 실측 후 baseline 1/4 이하로 설정 (현재 ~30 추정)

---

### 1-B. FlatBuffers io.WriterTo

**현황:**  
`fbFinish()`가 `make([]byte, n) + copy`로 FlatBuffers 빌더 내부 버퍼를 복사한 뒤 빌더를 풀로 반환.  
이 복사는 풀 재사용을 위한 의도적 설계이나, WriterTo 패턴으로 제거 가능.

**변경:**  
`internal/transport/codec.go` — `BinaryCodec.Encode` 시그니처 **유지**, 새 메서드 추가:

```go
// FlatBuffersWriter는 builder가 살아있는 동안 WriteTo로 쓰기 후 풀 반환.
type FlatBuffersWriter struct {
    typ     StreamType
    builder *flatbuffers.Builder  // WriteTo 완료 후 caller가 pool에 반환
}

// BinaryCodec.EncodeWriterTo는 FlatBuffers 직렬화 경로 전용.
// builder는 WriteTo 완료 후 pool로 반환되어야 함 (make+copy 제거).
func (c *BinaryCodec) EncodeWriterTo(w io.Writer, fw *FlatBuffersWriter) error {
    raw := fw.builder.FinishedBytes()
    header := [headerSize]byte{}
    header[0] = byte(fw.typ)
    binary.BigEndian.PutUint32(header[1:], uint32(len(raw)))
    if _, err := w.Write(header[:]); err != nil {
        return fmt.Errorf("write header: %w", err)
    }
    _, err := w.Write(raw)
    return err
    // caller: fw.builder.Reset(); pool.Put(fw.builder)
}
```

**빌더 생명주기:** `EncodeWriterTo` 호출 전 builder 보유, 호출 후 즉시 pool 반환.  
`fbFinish` 패턴은 cluster/codec.go (BadgerDB persist 경로)에서는 유지 — 쓰기가 비동기이므로 복사 필수.  
quic.go의 Send/Call 경로에서만 EncodeWriterTo 적용.

**목표 alloc:** QUIC 전송 경로에서 make+copy 1회 제거 (실측 후 수치 확정)

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

### 2-B. BinaryCodec EncodeWriterTo 통합

**현황:**  
quic.go의 `handleStream`, `Send`, `Call` 5개 호출부가 `t.codec.Encode(stream, msg)` 사용.

**변경:**  
`internal/transport/quic.go`에서 FlatBuffers 직렬화 경로(`marshalEnvelope`, `marshalShardRequest` 호출 직후)를  
`t.codec.EncodeWriterTo(stream, &FlatBuffersWriter{typ: ..., builder: b})` 로 교체.  
기존 `*Message` 경로(`Encode`)는 Raft 제어 메시지 등 non-FlatBuffers 경로에서 유지.

**검증:** `go test -race -count=1 ./internal/transport/...` 통과 확인.

---

## Layer 3: 브리지

### 3. NFS → S3 Zero-copy (vfs.go Rename 한정)

**현황:**  
`vfs.go:Rename()`:218 에서 `io.ReadAll(rc)` 후 `bytes.NewReader(data)`로 S3 PUT.  
파일 크기만큼 힙 사용. `grainFile.loadExisting()`:435의 io.ReadAll은 bytes.Buffer 랜덤 액세스 필요로 변경 불가.

**변경 대상:** `internal/vfs/vfs.go` — Rename 함수 인라인 교체 (bridge.go 별도 파일 불필요):

```go
func (fs *GrainVFS) Rename(oldpath, newpath string) error {
    oldFP := fs.fullPath(oldpath)
    newFP := fs.fullPath(newpath)

    rc, _, err := fs.backend.GetObject(fs.bucket, oldFP)
    if err != nil {
        return os.ErrNotExist
    }
    defer rc.Close()

    pr, pw := io.Pipe()
    defer pr.Close()  // s3Put panic 시 goroutine 보장 종료

    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        _, err := io.Copy(pw, rc)
        pw.CloseWithError(err)
    }()

    _, err = fs.backend.PutObject(fs.bucket, newFP, pr, "application/octet-stream")
    pr.CloseWithError(err)
    wg.Wait()
    if err != nil {
        return fmt.Errorf("write new file: %w", err)
    }

    if err := fs.backend.DeleteObject(fs.bucket, oldFP); err != nil {
        return err
    }
    fs.invalidateStatCache(oldFP)
    fs.invalidateStatCache(newFP)
    fs.invalidateParentDirCache(oldFP)
    fs.invalidateParentDirCache(newFP)
    return nil
}
```

메모리 상한 OS 파이프 버퍼(~64KB). `defer pr.Close()` 로 s3Put panic 시 goroutine 안전 종료.

---

## 테스트 전략

### alloc 회귀 방지 (항목별 추가)

각 PR에 동일 패턴의 `AllocsPerRun` 테스트 필수. **목표값은 구현 전 실측 후 코디파이.**

```go
// PR 1: ec_pool_test.go (또는 ec_test.go)
func TestECSplit_AllocsBounded(t *testing.T) {
    // 구현 전 baseline 측정: testing.AllocsPerRun(5, ...)
    // Pool 도입 후 목표: baseline의 1/4 이하
    cfg := ECConfig{DataShards: 4, ParityShards: 2}
    data := make([]byte, 1<<20)
    allocs := testing.AllocsPerRun(100, func() {
        shards, _ := ECSplit(cfg, data)
        _ = shards
    })
    // 실측 baseline 기반 상한 설정 (예: baseline=28 → assert ≤7)
    assert.LessOrEqual(t, allocs, /* 실측 후 설정 */ 7.0)
}

// PR 1-B: transport/codec_test.go
func TestBinaryCodec_EncodeWriterTo_AllocsBounded(t *testing.T) {
    b := flatbuffers.NewBuilder(64)
    // ... build some message ...
    codec := &BinaryCodec{}
    var buf bytes.Buffer
    allocs := testing.AllocsPerRun(100, func() {
        buf.Reset()
        _ = codec.EncodeWriterTo(&buf, &FlatBuffersWriter{typ: StreamData, builder: b})
    })
    assert.LessOrEqual(t, allocs, 1.0) // header array만 alloc
}

// PR 3: server/handlers_test.go
func TestGetObject_WriterToPath(t *testing.T) {
    // mock storage.Backend returning io.WriterTo
    // assert SetBodyStreamWriter 경로 진입 검증
}

// PR 5: vfs/vfs_test.go
func TestRename_LargeFile_MemoryBounded(t *testing.T) {
    // 100MB 파일 rename 시 메모리 ≤ 64KB (pipe buffer)
    // runtime.ReadMemStats 전/후 비교
}
```

### 정확성 검증

기존 E2E 테스트 (`tests/e2e/`) 전부 통과 필수. 최적화는 행동 변경 없음.

---

## 구현 순서

| PR | 항목 | 파일 | 위험도 | 전제조건 |
|----|------|------|--------|---------|
| 1 | RS encoder + shard 풀 | `internal/cluster/ec_pool.go`, `ec.go` | 낮음 | Phase 18 P1 해결 후 |
| 2 | FlatBuffers EncodeWriterTo | `internal/transport/codec.go`, `quic.go` | 낮음 | — |
| 3 | hertz sendfile 경로 | `internal/server/handlers.go` | 중간 | bufio 오버헤드 실측 후 |
| 4 | BinaryCodec EncodeWriterTo 통합 | `internal/transport/quic.go` | 중간 | PR 2 완료 후 |
| 5 | vfs.go Rename 스트리밍 | `internal/vfs/vfs.go` | 중간 | — |

PR 2+5는 서로 다른 패키지이므로 병렬 구현 가능.  
PR 1은 Phase 18 P1 해결 후, PR 4는 PR 2 완료 후.

---

## 비고

- PR 1-B: `fbFinish()` 패턴은 BadgerDB persist 경로에서 유지 (비동기 쓰기 → 복사 필수). quic.go 전송 경로에만 EncodeWriterTo 적용.
- PR 2/4: `BinaryCodec.Encode(w, *Message)` 시그니처 변경 없음. 새 `EncodeWriterTo` 메서드로 분리.
- PR 3: `SetBodyStreamWriter` API 먼저 확인 + bufio.Writer 오버헤드 실측 필수. 이득이 없으면 PR 3 드롭.
- PR 5: `internal/vfs/bridge.go` 미생성. vfs.go Rename 인라인 교체.
- 전 항목 완료 후 `make test-e2e`로 전체 E2E 통과 확인.

## Eng Review 결정사항 (2026-04-24)

| 항목 | 결정 |
|------|------|
| PR 1-B 빌더 생명주기 | WriteTo 완료 후 pool 반환 (zero-alloc) |
| BinaryCodec API | Encode 유지 + EncodeWriterTo 추가 |
| PR 5 범위 | vfs.go Rename 한정 (bridge.go 미생성) |
| Alloc 목표치 | 실측 baseline 기반 1/4 이하 (하드코딩 제거) |
| PR 5 panic 안전 | defer pr.Close() 추가 |
| AllocsPerRun 테스트 | 4개 추가 (PR 1, 1-B, 3, 5)
