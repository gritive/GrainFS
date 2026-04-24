# Zero-Allocation Phase 3: NFS Compound Hot-Path Pool Exhaustion

## Goal

COMPOUND 요청 1회당 heap allocation을 ≤2로 낮춘다.
Phase 2(PR #47) 이후 PUTFH+READ 경로에 남은 ~11 allocs를 sync.Pool과
크기별 byte slice 풀로 제거한다.

## Background

Phase 2 결과 (`acd8a35`):
- XDR WriteUint32/64: stack array ✓
- XDR ReadUint32/64: `r.r.Read` direct ✓
- readOpArgs OpRead/OpWrite: `var buf [16]byte` ✓
- opReadBufPool: sync.Pool ✓
- vfs grainBufPool: sync.Pool ✓
- S3Auth Grow pre-alloc ✓

잔여 PUTFH+READ 경로 alloc (11개):

| # | 위치 | 패턴 |
|---|------|------|
| 1 | `NewXDRReader` | `&XDRReader{}` |
| 2 | `NewXDRReader → bytes.NewReader` | `&bytes.Reader{}` |
| 3 | `ParseCompound` | `make([]Op, N)` |
| 4 | `readOpArgs OpPutFH` | `ReadOpaque → make([]byte,N)` ← 불가피 |
| 5 | `readOpArgs OpRead` (+ GetAttr/ReadDir/Open) | `&XDRWriter{}` |
| 6 | `handleCompound` | `&Dispatcher{}` |
| 7 | `Dispatch` | `&CompoundResponse{}` |
| 8 | `Dispatch Results append` | backing slice alloc |
| 9 | `opRead` result writer | `&XDRWriter{}` |
| 10 | `EncodeCompoundResponse` | `&XDRWriter{}` |
| 11 | `BuildRPCReply` | `&XDRWriter{}` |

목표 잔여: #4 (가변 길이 ReadOpaque, 불가피) 1개 + 선택적 응답 copy 1개 = ≤2.

## Architecture

### 추가 풀 목록

```
풀 이름              타입                         선언 위치
─────────────────────────────────────────────────────────
xdrReaderPool        *xdrReaderEntry              xdr.go
xdrWriterPool        *XDRWriter                   xdr.go
opArgPool16          *[]byte  (cap=16)            xdr.go
opArgPool8           *[]byte  (cap=8)             xdr.go
dispatcherPool       *Dispatcher                  compound.go
compoundReqPool      *CompoundRequest             compound.go
compoundRespPool     *CompoundResponse            compound.go
```

### XDRReader 재구조화

`bytes.Reader`를 포인터(`*bytes.Reader`)에서 값(`bytes.Reader`)으로 변경해
풀링 시 struct 2개 → 1개로 줄인다.

```go
// Before
type XDRReader struct {
    r *bytes.Reader
}
func NewXDRReader(data []byte) *XDRReader {
    return &XDRReader{r: bytes.NewReader(data)}  // 2 allocs
}

// After
type XDRReader struct {
    r    bytes.Reader   // embed by value
    pool *sync.Pool     // 풀 참조 (non-nil이면 put 가능)
}
func NewXDRReader(data []byte) *XDRReader {
    r := &XDRReader{}
    r.r.Reset(data)
    return r  // 1 alloc (테스트·일회성 경로용)
}
func newXDRReaderFromPool(data []byte) *XDRReader {
    r := xdrReaderPool.Get().(*XDRReader)
    r.r.Reset(data)
    r.pool = &xdrReaderPool
    return r  // 0 allocs
}
func putXDRReader(r *XDRReader) {
    if r.pool != nil {
        r.pool.Put(r)
    }
}
```

### XDRWriter 풀링

```go
var xdrWriterPool = sync.Pool{New: func() any { return &XDRWriter{} }}

func getXDRWriter() *XDRWriter {
    return xdrWriterPool.Get().(*XDRWriter)
}
func putXDRWriter(w *XDRWriter) {
    w.buf.Reset()
    xdrWriterPool.Put(w)
}
```

**Lifetime 규칙:**
- `w.Bytes()` 결과를 사용 완료한 뒤 `putXDRWriter` 호출.
- `EncodeCompoundResponse` / `BuildRPCReply`: 결과를 `writeRPCFrame`에
  동기 전달 후 즉시 put → copy 불필요 (0 alloc).
- `readOpArgs` 내 small writer: `w.Bytes()` 결과를 `make([]byte, n); copy`로
  소유권 이전 후 put → 1 alloc (구조체 alloc 제거로 상쇄).

### opArgPool16 / opArgPool8

```go
var opArgPool16 = sync.Pool{New: func() any { b := make([]byte, 16); return &b }}
var opArgPool8  = sync.Pool{New: func() any { b := make([]byte, 8);  return &b }}

func getOpArg16() []byte { return *opArgPool16.Get().(*[]byte) }
func putOpArg16(b []byte) { opArgPool16.Put(&b) }

func getOpArg8() []byte { return *opArgPool8.Get().(*[]byte) }
func putOpArg8(b []byte) { opArgPool8.Put(&b) }
```

`Op.Data`는 `Dispatch()` 반환 전까지만 유효 → `dispatchOp` 완료 후 put.

### Dispatcher 풀링

```go
var dispatcherPool = sync.Pool{New: func() any { return &Dispatcher{} }}

func getDispatcher(backend storage.Backend, state *StateManager) *Dispatcher {
    d := dispatcherPool.Get().(*Dispatcher)
    d.backend = backend
    d.state   = state
    d.currentFH   = nil
    d.currentPath = ""
    return d
}
func putDispatcher(d *Dispatcher) {
    d.backend = nil
    d.state   = nil
    dispatcherPool.Put(d)
}
```

### CompoundRequest / CompoundResponse 풀링

```go
var compoundReqPool = sync.Pool{New: func() any {
    return &CompoundRequest{Ops: make([]Op, 0, maxCompoundOps)}
}}
var compoundRespPool = sync.Pool{New: func() any {
    return &CompoundResponse{Results: make([]OpResult, 0, maxCompoundOps)}
}}

// reset
req.Tag = ""; req.MinorVer = 0; req.Ops = req.Ops[:0]
resp.Status = 0; resp.Tag = ""; resp.Results = resp.Results[:0]
```

`maxCompoundOps` 용량 사전 확보 → `append` 시 재할당 없음.

### ParseCompound 시그니처 변경 (Amendment #3)

`handleCompound`가 pool Get/Put을 소유하도록 시그니처를 변경한다.
기존 `ParseCompound(data []byte) (*CompoundRequest, error)` 대신:

```go
func ParseCompound(data []byte, req *CompoundRequest) error
```

호출 패턴:
```go
// handleCompound 내부
req := compoundReqPool.Get().(*CompoundRequest)
req.Tag = ""; req.MinorVer = 0; req.Ops = req.Ops[:0]
defer compoundReqPool.Put(req)   // 에러 경로 포함 항상 반환

if err := ParseCompound(data, req); err != nil {
    return err
}
// ... Dispatch(req) ...
```

### Op.poolKey 필드 추가 (Amendment #2)

opArgPool 슬라이스를 `dispatchOp` 완료 후 안전하게 반환하기 위해
`Op` 구조체에 `poolKey int` 필드를 추가한다.

```go
type Op struct {
    OpCode  int
    Data    []byte
    poolKey int  // 0=no pool, 8=opArgPool8, 16=opArgPool16
}
```

`readOpArgs`에서 pool 소스 슬라이스에 poolKey 설정:
- `OpAccess`: `getOpArg8()` → `poolKey=8`  (4바이트만 사용, 8 슬라이스로 반환)
- `OpClose`, `OpSetAttr`, `OpSetClientIDConfirm`, `OpOpenConfirm`: `getOpArg16()` → `poolKey=16`
- `OpRenew`: `getOpArg8()` → `poolKey=8`
- 기타 (ReadOpaque, XDRWriter copy): `poolKey=0`

`Dispatch` 루프에서 `dispatchOp` 완료 후:
```go
for _, op := range req.Ops {
    result := d.dispatchOp(op)
    switch op.poolKey {
    case 8:
        putOpArg8(op.Data)
    case 16:
        putOpArg16(op.Data)
    }
    resp.Results = append(resp.Results, result)
    if result.Status != NFS4_OK {
        resp.Status = result.Status
        break
    }
}
```

### CompoundResponse pool put 위치 (Amendment #1)

`compoundRespPool.Put`은 `Dispatch` 내부의 defer가 아니라
`handleCompound`에서 `EncodeCompoundResponse` 완료 후 호출한다.

```go
// handleCompound 내부
resp := compoundRespPool.Get().(*CompoundResponse)
resp.Status = NFS4_OK; resp.Tag = ""; resp.Results = resp.Results[:0]
defer compoundRespPool.Put(resp)   // EncodeCompoundResponse 후에 실행됨

// ...
encoded := EncodeCompoundResponse(resp)
// defer fires here (function return 시), resp는 이미 인코딩됨
```

`EncodeCompoundResponse`는 현재 함수 내에서 동기적으로 완료되므로
defer가 안전하다. `Dispatch` 내부에서 defer하면 resp가 반환된 뒤
`EncodeCompoundResponse` 호출 중 pool에서 재사용될 수 있어 use-after-free.

### XDRWriter cap 상한선

대형 READ 응답 후 writer 내부 bytes.Buffer가 크게 grow될 수 있다.
pool에 반환 시 메모리를 과잉 보유하지 않도록 cap 체크를 추가한다:

```go
const maxXDRWriterCap = 64 * 1024

func putXDRWriter(w *XDRWriter) {
    if w.buf.Cap() > maxXDRWriterCap {
        w.buf = bytes.Buffer{}  // oversized buffer 폐기
    } else {
        w.buf.Reset()
    }
    xdrWriterPool.Put(w)
}
```

## File Changes

### `internal/nfs4server/xdr.go`
- `XDRReader.r` 타입: `*bytes.Reader` → `bytes.Reader`
- `XDRReader.pool *sync.Pool` 필드 추가
- `NewXDRReader`: 기존 서명 유지 (1 alloc, 테스트 경로)
- `newXDRReaderFromPool` / `putXDRReader` 추가
- `xdrWriterPool`, `getXDRWriter`, `putXDRWriter` 추가
- `opArgPool16`, `opArgPool8` 및 헬퍼 추가
- `readOpArgs` 수정:
  - `OpAccess`: `make([]byte,4)` → `opArgPool8` (4바이트 슬라이스로 반환, 앞 4바이트만 사용)
  - `OpClose`, `OpSetAttr`, `OpSetClientIDConfirm`, `OpOpenConfirm`: `opArgPool16`
  - `OpRenew`: `opArgPool8`
  - `OpGetAttr`, `OpReadDir`, `OpRead`, `OpWrite`, `OpOpen`: `xdrWriterPool`
- `ParseCompound`: `newXDRReaderFromPool` 사용, `compoundReqPool`과 연동
- `EncodeCompoundResponse`: `xdrWriterPool` 사용
- `BuildRPCReply`: `xdrWriterPool` 사용

### `internal/nfs4server/compound.go`
- `dispatcherPool`, `compoundRespPool`, `compoundReqPool` 선언
- `handleCompound`:
  - `compoundReqPool.Get()` + reset + `defer compoundReqPool.Put(req)` (에러 경로 포함)
  - `ParseCompound(data, req)` 호출 (새 시그니처)
  - `getDispatcher()` + `defer putDispatcher(d)`
  - `compoundRespPool.Get()` + reset
  - `EncodeCompoundResponse(resp)` 완료 후 `compoundRespPool.Put(resp)`
- `Dispatch`: `compoundRespPool.Get/Put` 제거; req/resp를 파라미터로 받는 형태로 변경
  - `resp *CompoundResponse` 파라미터 추가, 또는 handleCompound에서 inline 처리
- `dispatchOp` 완료 후: `op.poolKey` 기반으로 `putOpArg8` / `putOpArg16` 호출
- `Op` 구조체에 `poolKey int` 필드 추가
- `opXxx` 함수들: 14개 `&XDRWriter{}` 모두 `getXDRWriter()` + `make+copy` + `putXDRWriter(w)` 패턴으로 교체

### `internal/nfs4server/server.go`
- `handleCompound`: `ParseCompound` 호환 유지 (pool 경로는 내부에서 처리)

### `internal/nfs4server/xdr_test.go`
- 기존 Phase 2 회귀 테스트 유지

### `internal/nfs4server/pool_test.go` (신규)
- `TestCompound_PUTFH_READ_AllocsPerRun`: ≤2.0
- `TestCompound_PUTFH_READDIR_AllocsPerRun`: ≤3.0 (ReadDir에 추가 opaque 있음)
- `TestXDRReaderPool_ZeroAllocs`: `newXDRReaderFromPool` + `putXDRReader` = 0
- `TestXDRWriterPool_ZeroAllocs`: `getXDRWriter` + encode + `putXDRWriter` = 0
- `TestParseCompound_ErrorPath_NoLeak`: ParseCompound 에러 시 req가 pool로 반환됨 확인
- `TestDispatch_EarlyBreak_OpArgReturned`: dispatchOp error break 시 poolKey 슬라이스 leak 없음

## Version

`VERSION`: `0.0.4.24 → 0.0.4.25`
`CHANGELOG.md`: `## [0.0.4.25]` 섹션 추가

## Testing

```bash
go test ./internal/nfs4server/... -run TestCompound -count=1 -v
go test ./internal/nfs4server/... -bench=. -benchmem
```

목표: `PUTFH+READ` round-trip `AllocsPerRun ≤ 2.0`
