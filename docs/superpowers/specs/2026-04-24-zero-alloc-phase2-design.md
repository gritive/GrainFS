# Zero-Allocation Phase 2 설계 스펙

**날짜:** 2026-04-24  
**범위:** NFS XDR 스택 배열 + RPC fragment 사전할당 + sync.Pool 버퍼 재사용 (vfs/nfs4server/s3auth)

---

## 목표

Phase 19에서 완료한 FlatBuffers Builder pool, EncryptWithAAD 1-alloc, ECSplit copy 최적화에 이어,
NFS·VFS·S3Auth 경로에서 남아 있는 고빈도 heap allocation을 제거한다.

1. NFS XDR 필드 read/write에서 `make([]byte, 4/8)` → stack array 치환
2. NFS RPC frame 읽기에서 임시 `fragment` 버퍼 제거 (direct read into result)
3. vfs grainFile 쓰기 버퍼를 sync.Pool로 재사용
4. nfs4server compound 응답 버퍼를 sync.Pool로 재사용
5. s3auth chunked 바디 디코딩 시 `bytes.Buffer.Grow`로 re-alloc 방지

---

## 범위 밖

- `unsafe.StringData` / `unsafe.SliceData` — string↔[]byte zero-copy, 안전성 우려로 제외
- FlatBuffers Builder 버퍼 탈착(detach) — `b.Bytes` 직접 조작, 라이브러리 내부 의존성 우려로 제외
- Peer shard fetch QUIC stream 직접 반환 — RPC 시그니처 변경 필요, 별도 스펙
- `io_uring`, `sendfile(2)`, mmap — 시스템콜 레이어 변경, 별도 Phase

---

## 현황 분석

### 1. NFS XDR — make([]byte, N) 반복

`internal/nfs4server/xdr.go`의 ReadUint32/ReadUint64/ReadOpaque/WriteUint32/WriteUint64 등
12개 이상의 메서드가 4–8 바이트 고정 크기 버퍼를 매번 heap에 할당한다.
NFS compound 요청 하나에 수십 개의 XDR 필드가 직렬화/역직렬화되므로
누적 alloc 횟수가 크다.

```go
// 현재
func (r *XDRReader) ReadUint32() (uint32, error) {
    b := make([]byte, 4)                     // heap alloc
    if _, err := io.ReadFull(r.r, b); err != nil { return 0, err }
    return binary.BigEndian.Uint32(b), nil
}
```

### 2. NFS RPC frame — fragment 임시 버퍼

`internal/nfs4server/rpc.go` readRPCFrame이 각 fragment마다
`make([]byte, length)` 로 임시 버퍼를 만들고 `append`로 결합한다.
단일 fragment(일반 케이스)에서도 alloc 2회가 발생한다.

```go
// 현재
var result []byte
for {
    fragment := make([]byte, length)   // alloc 1
    io.ReadFull(r, fragment)
    result = append(result, fragment...) // alloc 2 (최초)
}
```

### 3. vfs grainFile 쓰기 버퍼

`internal/vfs/vfs.go` grainFile.Open(writable)이 매번 `new(bytes.Buffer)` 를 생성한다.
NFS 쓰기 요청이 파일당 open→write→close 사이클을 반복하므로
버퍼를 pooling하면 할당을 제거할 수 있다.

### 4. nfs4server compound 응답 버퍼

`internal/nfs4server/compound.go`의 opWrite 등 각 NFS4 op 핸들러가
`&bytes.Buffer{}` 를 개별 생성한다.

### 5. s3auth chunked 바디

`internal/s3auth/chunked.go` DecodeAWSChunkedBody가 `var result bytes.Buffer` 로
capacity 0에서 시작해 chunk를 append할 때마다 내부 re-alloc이 발생할 수 있다.
호출부에서 `Content-Length`를 알고 있으면 한 번의 Grow로 re-alloc을 방지할 수 있다.

---

## 설계

### PR1: NFS XDR 스택 배열

**파일**: `internal/nfs4server/xdr.go`

**패턴**: 고정 크기(4, 8 바이트) `make([]byte, N)` → `var b [N]byte`

```go
// 변경 후
func (r *XDRReader) ReadUint32() (uint32, error) {
    var b [4]byte                            // stack alloc
    if _, err := io.ReadFull(r.r, b[:]); err != nil { return 0, err }
    return binary.BigEndian.Uint32(b[:]), nil
}

func (w *XDRWriter) WriteUint32(v uint32) {
    var b [4]byte
    binary.BigEndian.PutUint32(b[:], v)
    w.buf.Write(b[:])
}
```

적용 대상 메서드: ReadUint32, ReadUint64, ReadOpaque(헤더 4바이트), WriteUint32, WriteUint64  
`internal/nfs4server/rpc.go` header read (line 27, 42) 도 동일 패턴 적용.

**회귀 테스트**: `TestXDRReadWrite_AllocsBounded` — ReadUint32/WriteUint32 각 0 alloc 보장

---

### PR2: NFS RPC fragment 사전할당

**파일**: `internal/nfs4server/rpc.go` readRPCFrame

**변경**:
```go
// 변경 후: fragment 임시 버퍼 제거, result에 직접 읽기
result := make([]byte, 0, length)    // 첫 fragment 크기로 사전할당
for {
    start := len(result)
    result = append(result, make([]byte, length)...)  // capacity 부족 시 grow
    if _, err := io.ReadFull(r, result[start:]); err != nil { return nil, err }
    if lastFragment { break }
    // 다음 fragment 헤더 읽기 (stack [4]byte)
    ...
}
```

단일 fragment 케이스(일반): alloc 2→1 (`result` 1회, `fragment` 임시 버퍼 제거).  
다중 fragment 케이스: fragment별 임시 버퍼 제거, `result` grow만 남음.  
`make([]byte, length)` in append는 컴파일러 최적화 대상 — 실제로는 슬라이스 extend로 처리됨.

**회귀 테스트**: 기존 RPC 테스트 통과 + `TestReadRPCFrame_AllocsBounded`

---

### PR3: vfs grainFile 쓰기 버퍼 — sync.Pool

**파일**: `internal/vfs/vfs.go`

```go
var grainFileBufPool = sync.Pool{
    New: func() any { return new(bytes.Buffer) },
}
```

- Open(writable): `grainFileBufPool.Get()` + `Reset()`
- Close: `grainFileBufPool.Put(f.buf); f.buf = nil`
- 에러 경로(panic 포함) 에서도 Put이 호출되도록 defer 사용

**회귀 테스트**: 기존 vfs 테스트 통과 + `TestGrainFileWrite_AllocsBounded`

---

### PR4: nfs4server compound 응답 버퍼 — sync.Pool

**파일**: `internal/nfs4server/compound.go`

```go
var compoundBufPool = sync.Pool{
    New: func() any { return new(bytes.Buffer) },
}
```

응답 버퍼 생성 위치마다 Get/defer Put 패턴 적용.  
Reset은 Put 직전에 호출해 다음 사용자가 빈 버퍼를 받도록 보장.

**회귀 테스트**: 기존 nfs4server 테스트 통과

---

### PR5: s3auth chunked — Grow 사전할당

**파일**: `internal/s3auth/chunked.go`

```go
func DecodeAWSChunkedBody(r io.Reader, contentLength int64) ([]byte, error) {
    var result bytes.Buffer
    if contentLength > 0 {
        result.Grow(int(contentLength))
    }
    ...
}
```

호출부 시그니처 변경: `Content-Length` 헤더값(없으면 0) 전달.  
0이면 현재와 동일 동작 → 하위 호환 유지.

**회귀 테스트**: 기존 s3auth 테스트 통과

---

## 검증 계획

| PR | 회귀 테스트 | AllocsPerRun 목표 |
|----|------------|-------------------|
| 1 | TestXDRReadWrite_AllocsBounded | ReadUint32/WriteUint32 = 0 alloc |
| 2 | TestReadRPCFrame_AllocsBounded | 단일 fragment = 1 alloc |
| 3 | TestGrainFileWrite_AllocsBounded | Open(writable) = 0 alloc (버퍼 기준) |
| 4 | 기존 nfs4server 테스트 | — |
| 5 | 기존 s3auth 테스트 | — |

전체 변경 후 `go test ./...` 통과 필수.

---

## 예상 효과

| 위치 | 변경 전 | 변경 후 |
|------|---------|---------|
| XDR 필드 read/write (12곳) | 1 alloc/필드 | 0 alloc/필드 |
| RPC frame (단일 fragment) | 2 alloc/frame | 1 alloc/frame |
| grainFile Open(writable) | 1 alloc/open | 0 alloc/open (pool warm) |
| compound 응답 버퍼 | 1 alloc/op | 0 alloc/op (pool warm) |
| chunked 바디 re-alloc | N회/큰 바디 | 0회 (Grow 시 1회) |

---

## 구현 순서

1. PR1 (XDR 스택 배열) — 가장 단순, 먼저 완료
2. PR2 (RPC fragment) — PR1 이후 동일 파일 근처
3. PR3 (vfs buffer pool) — 독립적
4. PR4 (compound buffer pool) — 독립적
5. PR5 (s3auth Grow) — 독립적, 호출부 시그니처 변경 포함

PR3–5는 병렬 진행 가능.
