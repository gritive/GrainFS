# Zero-Allocation / Zero-Copy 최적화 설계

**날짜:** 2026-04-24  
**Phase:** 19 (Performance)  
**목표:** 예방적 최적화 — 관측된 병목 없이 Phase 19 전에 핫패스 메모리 할당 기반 다지기  
**전략:** 독립 최적화 (독립 PR) + 위험도 낮은 순서 구현. PR2/PR4/PR5는 v0.0.4.19 완료.

---

## 범위 (미완료)

| PR | 항목 | 상태 |
|---|---|---|
| PR1 | Reed-Solomon encoder + shard pool | 미완료 |
| PR3 | hertz Zero-copy sendfile | 실측 후 결정 |
| PR6 | EncryptWithAAD 3→1 alloc | 미완료 |
| PR7 | FlatBuffers Builder Pool (Raft Cmd 경로) | 미완료 |
| PR8 | encodeShardHeader [8]byte stack | 미완료 |

---

## 설계 원칙

- **독립 최적화:** 각 항목은 별개 패키지, 별개 PR. 공유 풀 인프라 없음 (데이터 타입이 달라 통합 불가).
- **행동 불변:** 최적화는 alloc 감소만, 외부 API / 의미론 변경 없음.
- **회귀 방지:** 각 항목마다 `testing.AllocsPerRun` 기반 alloc 상한 테스트 추가.
- **구현 우선순위:** PR6 → PR7 → PR1 → PR8 (PR3은 실측 후).

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


## 테스트 전략

각 PR에 `testing.AllocsPerRun` 회귀 테스트 필수. 목표값은 구현 전 실측 후 설정.

```go
// PR1: internal/cluster/ec_pool_test.go
func TestECSplit_AllocsBounded(t *testing.T) {
    // 워밍업 후 testing.AllocsPerRun(100, ...)
    // 목표: baseline(실측) 1/4 이하
}

// PR6: internal/encrypt/encrypt_test.go
func TestEncryptWithAAD_AllocsBounded(t *testing.T) {
    allocs := testing.AllocsPerRun(100, func() { _, _ = e.EncryptWithAAD(plaintext, aad) })
    assert.LessOrEqual(t, allocs, 1.0)
}

// PR7: internal/cluster/codec_test.go
func TestEncodeObjectMeta_AllocsBounded(t *testing.T) {
    allocs := testing.AllocsPerRun(100, func() { _, _ = EncodeObjectMeta(meta) })
    assert.LessOrEqual(t, allocs, 2.0) // NewBuilder alloc 제거 후
}
```

### 정확성 검증

기존 E2E 테스트 (`tests/e2e/`) 전부 통과 필수. 최적화는 행동 변경 없음.

---

## 구현 순서

| PR | 항목 | 파일 | 위험도 | 상태 |
|----|------|------|--------|------|
| 1 | RS encoder + shard 풀 | `internal/cluster/ec_pool.go`, `ec.go` | 낮음 | 미완료 |
| 3 | hertz sendfile 경로 | `internal/server/handlers.go` | 중간 | bufio 실측 후 결정 |
| 6 | EncryptWithAAD 3→1 alloc | `internal/encrypt/encrypt.go` | 낮음 | 미완료 |
| 7 | FlatBuffers Builder Pool (Raft Cmd 경로) | `cluster/codec.go`, `storage/codec.go`, `raft/quic_rpc_codec.go`, `raft/store.go`, `volume/codec.go` | 낮음 | 미완료 |
| 8 | encodeShardHeader stack 배열 | `internal/cluster/ec.go` | 낮음 | 미완료 |

구현 우선순위: PR6 → PR7 → PR1 → PR8. PR1과 PR7은 서로 독립.

---

## Tier 1 (HIGH IMPACT) — 신규 추가

### 1-C. EncryptWithAAD 3→1 Alloc

**현황:**
`internal/encrypt/encrypt.go:EncryptWithAAD` — 3 allocs:

```go
nonce := make([]byte, e.aead.NonceSize())          // alloc 1 (12B heap)
inner := e.aead.Seal(nonce, nonce, plaintext, aad) // alloc 2
out := make([]byte, 2+len(inner))                  // alloc 3
```

**변경:**

```go
var nonce [12]byte                                    // stack (escape 없음)
if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
    return nil, fmt.Errorf("generate nonce: %w", err)
}
out := make([]byte, 2, 2+12+len(plaintext)+16)       // alloc 1 (정확한 크기)
out[0], out[1] = encMagic0, encMagic1
out = append(out, nonce[:]...)
out = e.aead.Seal(out, nonce[:], plaintext, aad)     // in-place
return out, nil
```

**영향 범위:** `shard_service.go`의 `WriteShard`, `ReadShard`, `writeShard` — EC PUT/GET 모든 샤드 암호화/복호화 경로.

**목표 alloc:** 3 → 1 (AllocsPerRun으로 검증)

---

## Tier 2 (MEDIUM IMPACT) — 신규 추가

### 2-C. FlatBuffers Builder Pool — Raft Cmd 경로

**현황:**
`cluster/codec.go`, `storage/codec.go`, `raft/quic_rpc_codec.go`, `raft/store.go`, `volume/codec.go` 모두 동일:

```go
b := flatbuffers.NewBuilder(128)  // 매 호출 alloc
// ... build ...
return fbFinish(b, ...), nil      // make+copy 후 b 버림
```

**변경:**
각 패키지에 package-level `sync.Pool` 추가, `fbFinish` / `fbFinishRPC` 내부에서 pool 반환:

```go
// cluster/codec.go
var clusterBuilderPool = sync.Pool{
    New: func() any { return flatbuffers.NewBuilder(256) },
}

func fbFinish(b *flatbuffers.Builder, root flatbuffers.UOffsetT) []byte {
    b.Finish(root)
    raw := b.FinishedBytes()
    out := make([]byte, len(raw))
    copy(out, raw)   // BadgerDB 소유권 이전 — copy 유지 필수
    b.Reset()
    clusterBuilderPool.Put(b)
    return out
}
```

각 encode 함수:
```go
b := clusterBuilderPool.Get().(*flatbuffers.Builder)
```

| 파일 | pool 변수 | 함수 수 |
|------|----------|--------|
| `cluster/codec.go` | `clusterBuilderPool` | ~20 |
| `storage/codec.go` | `storageBuilderPool` | 2 |
| `raft/quic_rpc_codec.go` + `raft/store.go` | `raftBuilderPool` | ~11 |
| `volume/codec.go` | `volumeBuilderPool` | 1 |

**make+copy는 유지** (Raft 로그/BadgerDB 소유권 이전 필요). `NewBuilder()` alloc만 제거.

---

## Tier 3 (LOW IMPACT) — 신규 추가

### 3-A. encodeShardHeader → [8]byte 반환

**현황** (`internal/cluster/ec.go:88`):

```go
func encodeShardHeader(origSize int64) []byte {
    h := make([]byte, shardHeaderSize)  // 8B heap alloc
    binary.BigEndian.PutUint64(h, uint64(origSize))
    return h
}
```

**변경:**

```go
func encodeShardHeader(origSize int64) [shardHeaderSize]byte {
    var h [shardHeaderSize]byte
    binary.BigEndian.PutUint64(h[:], uint64(origSize))
    return h
}
```

호출부: `append(payload, header[:]...)` — 스택 배열 slice, escape 없음.

---

## 비고

- PR 2/4/5: v0.0.4.19에서 완료. 플랜에서 체크 처리.
- PR 3: `SetBodyStreamWriter` bufio 오버헤드 실측 필수. 이득 없으면 드롭.
- PR 7: `make+copy`는 유지 (BadgerDB/Raft 소유권 이전). `NewBuilder()` alloc만 제거.
- Transport Decode payload pool: 크기 가변 + caller 소유권 이전 필요 → 드롭.
- 전 항목 완료 후 `make test-e2e`로 전체 E2E 통과 확인.

## 결정사항

| 항목 | 결정 |
|------|------|
| PR 1-B 빌더 생명주기 | WriteTo 완료 후 pool 반환 (zero-alloc) |
| BinaryCodec API | Encode 유지 + EncodeWriterTo 추가 |
| PR 5 범위 | vfs.go Rename 한정 |
| Alloc 목표치 | 실측 baseline 기반 1/4 이하 |
| PR 5 panic 안전 | defer pr.Close() 추가 |
| PR 6 nonce | [12]byte 스택 고정 (AES-GCM NonceSize=12 상수) |
| PR 7 make+copy 유지 | BadgerDB 소유권 이전 필수 |
| Transport Decode pool | 드롭 (가변 크기 + 소유권 문제) |
| AllocsPerRun 테스트 | PR1, PR6, PR7 각각 추가 |
