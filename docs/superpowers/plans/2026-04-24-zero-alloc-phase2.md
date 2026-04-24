# Zero-Allocation Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** NFS·VFS·S3Auth 경로에서 고빈도 heap allocation을 제거해 GC 압력을 줄인다.

**Architecture:** (1) 고정 크기 버퍼를 stack array로 치환, (2) 반복 생성되는 bytes.Buffer를 sync.Pool로 재사용, (3) 청킹 디코딩 시 Grow로 re-alloc 방지. 모든 변경은 기존 인터페이스를 유지하며 AllocsPerRun 회귀 테스트로 검증한다.

**Tech Stack:** Go 1.26+, encoding/binary, sync.Pool, bytes.Buffer, testing.AllocsPerRun

**Spec:** `docs/superpowers/specs/2026-04-24-zero-alloc-phase2-design.md`

---

## File Map

| 파일 | 변경 내용 |
|------|-----------|
| `internal/nfs4server/xdr.go` | make([]byte,4/8/16) → var [N]byte (11곳) |
| `internal/nfs4server/rpc.go` | header make→stack, fragment pre-alloc |
| `internal/nfs4server/xdr_test.go` | 신규: AllocsPerRun 회귀 테스트 |
| `internal/nfs4server/rpc_test.go` | AllocsPerRun 회귀 테스트 추가 |
| `internal/nfs4server/compound.go` | compoundBufPool 추가, opRead buffer pooling |
| `internal/vfs/vfs.go` | grainFileBufPool 추가, grainFile buffer pooling |
| `internal/vfs/vfs_test.go` | buffer pool 재사용 회귀 테스트 추가 |
| `internal/s3auth/chunked.go` | bytes.Buffer.Grow 추가 |
| `internal/s3auth/chunked_test.go` | 사전할당 동작 테스트 추가 |

---

## Task 1: NFS XDR 메서드 — make→stack (xdr.go)

**Files:**
- Modify: `internal/nfs4server/xdr.go`

`WriteUint32`, `WriteUint64`, `ReadUint32`, `ReadUint64`, `ReadOpaque`(skip pad), `readOpArgs`(OpRead/OpWrite/OpSetAttr/OpOpenConfirm)에서 고정 크기 `make([]byte, N)` 을 stack array로 교체한다. 단, 반환값이 되는 `OpClose`/`OpSetClientIDConfirm`/`OpRenew`의 buf는 heap을 유지한다.

- [ ] **Step 1: XDRWriter 메서드 4/8바이트 stack 치환**

`internal/nfs4server/xdr.go` 의 WriteUint32, WriteUint64을 아래처럼 교체:

```go
func (w *XDRWriter) WriteUint32(v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	w.buf.Write(b[:])
}

func (w *XDRWriter) WriteUint64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	w.buf.Write(b[:])
}
```

- [ ] **Step 2: XDRReader 메서드 4/8바이트 stack 치환**

`internal/nfs4server/xdr.go` 의 ReadUint32, ReadUint64을 아래처럼 교체:

```go
func (r *XDRReader) ReadUint32() (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r.r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}

func (r *XDRReader) ReadUint64() (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r.r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}
```

- [ ] **Step 3: ReadOpaque skip pad stack 치환**

`ReadOpaque` 내 패딩 스킵 코드를 교체 (패딩은 최대 3바이트):

```go
// 변경 전
if pad > 0 {
    skip := make([]byte, pad)
    io.ReadFull(r.r, skip)
}

// 변경 후
if pad > 0 {
    var skip [3]byte
    io.ReadFull(r.r, skip[:pad])
}
```

- [ ] **Step 4: readOpArgs OpRead/OpWrite stateid stack 치환**

`readOpArgs` 내 OpRead, OpWrite case에서 stateid 읽기를 교체 (이 buf는 w.buf.Write에만 쓰이고 반환되지 않음):

```go
case OpRead:
    // stateid (seqid:4 + other:12) + offset:8 + count:4
    var buf [16]byte
    io.ReadFull(r.r, buf[:]) // stateid
    offset, _ := r.ReadUint64()
    count, _ := r.ReadUint32()
    w := &XDRWriter{}
    w.buf.Write(buf[:])
    w.WriteUint64(offset)
    w.WriteUint32(count)
    return w.Bytes(), nil

case OpWrite:
    var buf [16]byte
    io.ReadFull(r.r, buf[:]) // stateid
    offset, _ := r.ReadUint64()
    stable, _ := r.ReadUint32()
    data, _ := r.ReadOpaque()
    w := &XDRWriter{}
    w.buf.Write(buf[:])
    w.WriteUint64(offset)
    w.WriteUint32(stable)
    w.WriteOpaque(data)
    return w.Bytes(), nil
```

- [ ] **Step 5: readOpArgs OpSetAttr/OpOpenConfirm stateid stack 치환**

```go
case OpSetAttr:
    var buf [16]byte
    io.ReadFull(r.r, buf[:]) // stateid
    bitmapLen, _ := r.ReadUint32()
    for i := uint32(0); i < bitmapLen; i++ {
        r.ReadUint32()
    }
    r.ReadOpaque() // attrvals
    return buf[:], nil  // buf escapes to heap here — 반환되므로 stack→heap escape 불가피, 그러나 기존 make 대비 동일

case OpOpenConfirm:
    var buf [16]byte
    io.ReadFull(r.r, buf[:]) // stateid (open_stateid)
    r.ReadUint32()           // seqid
    return buf[:], nil
```

> 주의: `return buf[:], nil` 은 stack array의 슬라이스를 반환하므로 컴파일러가 heap으로 escape시킨다. 이는 `make([]byte, 16)` 과 동일한 1 alloc이다. 이 케이스는 alloc 감소 없음 — 4번 Step에서 w.buf.Write에만 사용되는 케이스만 실질적으로 0 alloc.

- [ ] **Step 6: 빌드 확인**

```bash
go build ./internal/nfs4server/...
```

Expected: 오류 없음

- [ ] **Step 7: 기존 테스트 통과 확인**

```bash
go test ./internal/nfs4server/... -count=1
```

Expected: PASS

- [ ] **Step 8: gofmt**

```bash
gofmt -w internal/nfs4server/xdr.go
```

- [ ] **Step 9: 커밋**

```bash
git add internal/nfs4server/xdr.go
git commit -m "perf(nfs4server): XDR read/write make([]byte,N) → stack array"
```

---

## Task 2: NFS RPC frame header — make→stack (rpc.go)

**Files:**
- Modify: `internal/nfs4server/rpc.go`

`writeRPCFrame`의 header 4바이트 및 `readRPCFrame`의 header 4바이트를 stack으로 치환.

- [ ] **Step 1: writeRPCFrame header stack 치환**

```go
func writeRPCFrame(w io.Writer, payload []byte) error {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload))|0x80000000)
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}
```

- [ ] **Step 2: readRPCFrame header stack + fragment pre-alloc**

```go
func readRPCFrame(r io.Reader) ([]byte, error) {
	var headerBuf [4]byte

	readHeader := func() (lastFrag bool, length uint32, err error) {
		if _, err = io.ReadFull(r, headerBuf[:]); err != nil {
			return
		}
		raw := binary.BigEndian.Uint32(headerBuf[:])
		lastFrag = (raw & 0x80000000) != 0
		length = raw & 0x7FFFFFFF
		if length > maxFrameSize {
			err = fmt.Errorf("RPC frame size %d exceeds max %d", length, maxFrameSize)
		}
		return
	}

	lastFragment, length, err := readHeader()
	if err != nil {
		return nil, err
	}

	// 첫 fragment 크기로 사전할당 → 단일 fragment(일반 케이스) alloc 2→1
	result := make([]byte, 0, length)

	for {
		start := len(result)
		result = append(result, make([]byte, length)...)
		if _, err := io.ReadFull(r, result[start:]); err != nil {
			return nil, err
		}
		if lastFragment {
			break
		}
		lastFragment, length, err = readHeader()
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}
```

- [ ] **Step 3: 기존 rpc_test.go 테스트 통과 확인**

```bash
go test ./internal/nfs4server/... -run TestRPCFrame -v -count=1
```

Expected:
```
=== RUN   TestRPCFrame_EncodeAndDecode
--- PASS: TestRPCFrame_EncodeAndDecode
=== RUN   TestRPCFrame_MaxSizeEnforced
--- PASS: TestRPCFrame_MaxSizeEnforced
```

- [ ] **Step 4: AllocsPerRun 회귀 테스트 추가**

`internal/nfs4server/rpc_test.go` 에 추가:

```go
func TestReadRPCFrame_AllocsBounded(t *testing.T) {
	payload := []byte("hello rpc payload for alloc test")

	// Encode a single-fragment frame
	var encoded bytes.Buffer
	err := writeRPCFrame(&encoded, payload)
	require.NoError(t, err)
	encoded_bytes := encoded.Bytes()

	allocs := testing.AllocsPerRun(100, func() {
		r := bytes.NewReader(encoded_bytes)
		_, _ = readRPCFrame(r)
	})
	// 단일 fragment: result 슬라이스 1회 + bytes.NewReader 1회 = 2 alloc 이하
	assert.LessOrEqual(t, allocs, 2.0, "readRPCFrame single fragment should allocate ≤2")
}
```

- [ ] **Step 5: 테스트 실행**

```bash
go test ./internal/nfs4server/... -run TestReadRPCFrame_AllocsBounded -v -count=1
```

Expected: `--- PASS: TestReadRPCFrame_AllocsBounded`

- [ ] **Step 6: gofmt**

```bash
gofmt -w internal/nfs4server/rpc.go internal/nfs4server/rpc_test.go
```

- [ ] **Step 7: 커밋**

```bash
git add internal/nfs4server/rpc.go internal/nfs4server/rpc_test.go
git commit -m "perf(nfs4server): RPC header make→stack + fragment pre-alloc"
```

---

## Task 3: XDR 회귀 테스트 추가 (xdr_test.go 신규)

**Files:**
- Create: `internal/nfs4server/xdr_test.go`

Task 1에서 변경한 메서드들에 대해 AllocsPerRun 회귀 테스트를 추가한다.

- [ ] **Step 1: 테스트 파일 생성**

`internal/nfs4server/xdr_test.go` 를 새로 생성:

```go
package nfs4server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestXDRReadWrite_AllocsBounded(t *testing.T) {
	t.Run("WriteUint32", func(t *testing.T) {
		w := &XDRWriter{}
		allocs := testing.AllocsPerRun(100, func() {
			w.buf.Reset()
			w.WriteUint32(0xDEADBEEF)
		})
		assert.Equal(t, 0.0, allocs, "WriteUint32 should allocate 0 (stack array)")
	})

	t.Run("WriteUint64", func(t *testing.T) {
		w := &XDRWriter{}
		allocs := testing.AllocsPerRun(100, func() {
			w.buf.Reset()
			w.WriteUint64(0xDEADBEEFCAFEBABE)
		})
		assert.Equal(t, 0.0, allocs, "WriteUint64 should allocate 0 (stack array)")
	})

	t.Run("ReadUint32", func(t *testing.T) {
		// 4바이트 고정 데이터로 XDRReader 반복 생성 비용 제외
		data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
		r := NewXDRReader(data)
		allocs := testing.AllocsPerRun(100, func() {
			r.r.Seek(0, 0) // rewind
			_, _ = r.ReadUint32()
		})
		assert.Equal(t, 0.0, allocs, "ReadUint32 should allocate 0 (stack array)")
	})

	t.Run("ReadUint64", func(t *testing.T) {
		data := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE}
		r := NewXDRReader(data)
		allocs := testing.AllocsPerRun(100, func() {
			r.r.Seek(0, 0)
			_, _ = r.ReadUint64()
		})
		assert.Equal(t, 0.0, allocs, "ReadUint64 should allocate 0 (stack array)")
	})
}
```

- [ ] **Step 2: 테스트 실행**

```bash
go test ./internal/nfs4server/... -run TestXDRReadWrite_AllocsBounded -v -count=1
```

Expected: 4개 서브테스트 모두 PASS

- [ ] **Step 3: gofmt**

```bash
gofmt -w internal/nfs4server/xdr_test.go
```

- [ ] **Step 4: 커밋**

```bash
git add internal/nfs4server/xdr_test.go
git commit -m "test(nfs4server): XDR AllocsPerRun 회귀 테스트 추가"
```

---

## Task 4: vfs grainFile 쓰기 버퍼 — sync.Pool

**Files:**
- Modify: `internal/vfs/vfs.go`
- Modify: `internal/vfs/vfs_test.go`

grainFile의 `buf *bytes.Buffer` 를 sync.Pool로 관리한다. writable open, Write, Truncate, Close 경로를 모두 수정한다.

- [ ] **Step 1: grainFileBufPool 추가**

`internal/vfs/vfs.go` 상단 import 아래(또는 패키지 변수 선언부)에 추가:

```go
var grainFileBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}
```

`sync` 패키지가 import에 없으면 추가한다.

- [ ] **Step 2: Open의 O_TRUNC 경로 pool 적용**

기존: `f.buf = &bytes.Buffer{}`  
(파일: `internal/vfs/vfs.go`, `flag&os.O_TRUNC != 0` 분기)

```go
if flag&os.O_TRUNC != 0 {
    b := grainFileBufPool.Get().(*bytes.Buffer)
    b.Reset()
    f.buf = b
} else {
```

- [ ] **Step 3: loadExisting pool 적용**

기존: `f.buf = bytes.NewBuffer(data)`

```go
func (f *grainFile) loadExisting() error {
	if f.rc != nil {
		f.rc.Close()
		f.rc = nil
	}
	rc, _, err := f.fs.backend.GetObject(f.fs.bucket, f.path)
	if err != nil {
		return os.ErrNotExist
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}
	b := grainFileBufPool.Get().(*bytes.Buffer)
	b.Reset()
	b.Write(data)
	if f.buf != nil {
		f.buf.Reset()
		grainFileBufPool.Put(f.buf)
	}
	f.buf = b
	return nil
}
```

- [ ] **Step 4: Write() nil buf 경로 pool 적용**

기존: `f.buf = &bytes.Buffer{}`

```go
func (f *grainFile) Write(p []byte) (int, error) {
	if f.buf == nil {
		b := grainFileBufPool.Get().(*bytes.Buffer)
		b.Reset()
		f.buf = b
	}
	return f.buf.Write(p)
}
```

- [ ] **Step 5: Truncate pool 적용**

기존 Truncate는 내부에서 `bytes.NewBuffer(data)` 로 버퍼를 교체한다. 풀 버퍼를 재사용하도록 변경:

```go
func (f *grainFile) Truncate(size int64) error {
	var current []byte
	if f.buf != nil {
		current = f.buf.Bytes()
	}

	newBuf := grainFileBufPool.Get().(*bytes.Buffer)
	newBuf.Reset()

	if int64(len(current)) >= size {
		newBuf.Write(current[:size])
	} else {
		newBuf.Write(current)
		padding := make([]byte, size-int64(len(current)))
		newBuf.Write(padding)
	}

	if f.buf != nil {
		f.buf.Reset()
		grainFileBufPool.Put(f.buf)
	}
	f.buf = newBuf
	return nil
}
```

- [ ] **Step 6: Close에서 buf 풀 반납**

기존 Close의 writable flush 경로 끝, 그리고 read-only 경로에서 buf 반납:

```go
func (f *grainFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true

	if f.rc != nil {
		f.rc.Close()
		f.rc = nil
	}

	if f.flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC) != 0 && f.buf != nil {
		data := f.buf.Bytes()
		_, err := f.fs.backend.PutObject(f.fs.bucket, f.path,
			bytes.NewReader(data), "application/octet-stream")
		// PutObject 완료 후 버퍼 반납 (data는 이미 소비됨)
		f.buf.Reset()
		grainFileBufPool.Put(f.buf)
		f.buf = nil
		if err != nil {
			return err
		}
		f.fs.invalidateStatCache(f.path)
		f.fs.invalidateParentDirCache(f.path)
		return nil
	}

	if f.buf != nil {
		f.buf.Reset()
		grainFileBufPool.Put(f.buf)
		f.buf = nil
	}
	return nil
}
```

- [ ] **Step 7: 빌드 확인**

```bash
go build ./internal/vfs/...
```

- [ ] **Step 8: 기존 vfs 테스트 통과 확인**

```bash
go test ./internal/vfs/... -count=1 -timeout 60s
```

Expected: PASS

- [ ] **Step 9: pool 재사용 회귀 테스트 추가**

`internal/vfs/vfs_test.go` 에 추가 (패키지 내부 접근 가능한 위치):

```go
func TestGrainFileBufPool_Reuse(t *testing.T) {
	// 풀에서 꺼내고 반납한 뒤 같은 포인터가 반환되는지 확인
	b1 := grainFileBufPool.Get().(*bytes.Buffer)
	b1.Reset()
	grainFileBufPool.Put(b1)

	b2 := grainFileBufPool.Get().(*bytes.Buffer)
	assert.Same(t, b1, b2, "pool should return the same buffer on warm reuse")
	grainFileBufPool.Put(b2)
}
```

- [ ] **Step 10: 풀 재사용 테스트 실행**

```bash
go test ./internal/vfs/... -run TestGrainFileBufPool_Reuse -v -count=1
```

Expected: `--- PASS: TestGrainFileBufPool_Reuse`

- [ ] **Step 11: gofmt**

```bash
gofmt -w internal/vfs/vfs.go internal/vfs/vfs_test.go
```

- [ ] **Step 12: 커밋**

```bash
git add internal/vfs/vfs.go internal/vfs/vfs_test.go
git commit -m "perf(vfs): grainFile bytes.Buffer → sync.Pool 재사용"
```

---

## Task 5: nfs4server compound opRead 버퍼 — sync.Pool

**Files:**
- Modify: `internal/nfs4server/compound.go`

opRead 핸들러에서 객체 데이터 읽기에 사용하는 `bytes.Buffer` 를 sync.Pool로 관리한다.

- [ ] **Step 1: compoundBufPool 추가**

`internal/nfs4server/compound.go` 상단 패키지 변수로 추가:

```go
var compoundBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}
```

`sync` 패키지가 import에 없으면 추가한다.

- [ ] **Step 2: opRead buffer pool 적용**

기존 `buffer := &bytes.Buffer{}` (line ~362) 를 아래로 교체:

```go
buffer := compoundBufPool.Get().(*bytes.Buffer)
buffer.Reset()
defer func() {
    buffer.Reset()
    compoundBufPool.Put(buffer)
}()
```

- [ ] **Step 3: 빌드 및 테스트 확인**

```bash
go build ./internal/nfs4server/...
go test ./internal/nfs4server/... -count=1 -timeout 120s
```

Expected: PASS

- [ ] **Step 4: gofmt**

```bash
gofmt -w internal/nfs4server/compound.go
```

- [ ] **Step 5: 커밋**

```bash
git add internal/nfs4server/compound.go
git commit -m "perf(nfs4server): compound opRead bytes.Buffer → sync.Pool"
```

---

## Task 6: s3auth chunked — bytes.Buffer.Grow 사전할당

**Files:**
- Modify: `internal/s3auth/chunked.go`
- Modify: `internal/s3auth/chunked_test.go`

`DecodeAWSChunkedBody`에 `Grow(len(data))`를 추가해 chunk append 시 re-alloc을 방지한다. 인코딩된 크기가 디코딩된 크기의 상한이므로 시그니처 변경 없이 적용 가능하다.

- [ ] **Step 1: Grow 추가**

`internal/s3auth/chunked.go` 의 `DecodeAWSChunkedBody` 함수 내 `var result bytes.Buffer` 직후에 추가:

```go
func DecodeAWSChunkedBody(data []byte) ([]byte, error) {
	r := bytes.NewReader(data)
	var result bytes.Buffer
	if len(data) > 0 {
		result.Grow(len(data)) // 디코딩 크기 ≤ 인코딩 크기이므로 상한으로 사전할당
	}
	// ... (이하 기존 코드 동일)
```

- [ ] **Step 2: 기존 테스트 통과 확인**

```bash
go test ./internal/s3auth/... -run TestDecodeAWSChunkedBody -v -count=1
```

Expected: 4개 테스트 모두 PASS

- [ ] **Step 3: Grow 동작 확인 테스트 추가**

`internal/s3auth/chunked_test.go` 에 추가:

```go
func TestDecodeAWSChunkedBody_PreAllocNoRealloc(t *testing.T) {
	// 큰 청크에서 Grow가 re-alloc을 방지하는지 AllocsPerRun으로 검증
	chunk := bytes.Repeat([]byte("x"), 1024)
	// aws-chunked 형식으로 인코딩
	input := fmt.Sprintf("400;chunk-signature=sig\r\n%s\r\n0;chunk-signature=end\r\n\r\n", chunk)
	data := []byte(input)

	allocs := testing.AllocsPerRun(50, func() {
		_, _ = DecodeAWSChunkedBody(data)
	})
	// result.Grow로 1회 할당 + bytes.NewReader 1회 = 2 alloc 이하
	assert.LessOrEqual(t, allocs, 3.0, "DecodeAWSChunkedBody should not re-alloc with Grow")
}
```

- [ ] **Step 4: 테스트 실행**

```bash
go test ./internal/s3auth/... -run TestDecodeAWSChunkedBody -v -count=1
```

Expected: 모든 테스트 PASS

- [ ] **Step 5: gofmt**

```bash
gofmt -w internal/s3auth/chunked.go internal/s3auth/chunked_test.go
```

- [ ] **Step 6: 커밋**

```bash
git add internal/s3auth/chunked.go internal/s3auth/chunked_test.go
git commit -m "perf(s3auth): DecodeAWSChunkedBody bytes.Buffer.Grow 사전할당"
```

---

## Task 7: 전체 테스트 통과 확인 및 최종 정리

**Files:** 없음 (검증 전용)

- [ ] **Step 1: 전체 테스트 실행**

```bash
go test ./... -count=1 -timeout 300s 2>&1 | tail -30
```

Expected: 모든 패키지 PASS

- [ ] **Step 2: AllocsPerRun 회귀 테스트 모두 통과 확인**

```bash
go test ./internal/nfs4server/... ./internal/vfs/... ./internal/s3auth/... \
  -run "AllocsBounded|PreAllocNoRealloc|BufPool" -v -count=1
```

Expected: 전부 PASS

- [ ] **Step 3: race detector 통과 확인**

```bash
go test -race ./internal/nfs4server/... ./internal/vfs/... ./internal/s3auth/... -count=1 -timeout 120s
```

Expected: PASS (race condition 없음)

- [ ] **Step 4: 최종 커밋 없음 (각 Task에서 이미 커밋됨)**

완료.
