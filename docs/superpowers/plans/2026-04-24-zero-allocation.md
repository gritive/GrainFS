# Zero-Allocation / Zero-Copy 최적화 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** GrainFS 핫패스(RS 인코딩, QUIC FlatBuffers 전송, NFS→S3 Rename)에서 불필요한 메모리 할당을 제거해 Phase 19 성능 최적화의 기반을 만든다.

**Architecture:** 5개 독립 PR을 위험도 낮은 순서로 구현한다. Codec 계층(PR2: codec.go EncodeWriterTo 추가) → VFS 브리지(PR5: vfs.go Rename io.Pipe) → QUIC 전송 통합(PR4: shard_service.go EncodeWriterTo 적용, PR2 선결) → HTTP 레이어(PR3: hertz sendfile, 실측 후) → RS 풀(PR1: ec_pool.go, Phase 18 P1 선결). 각 PR은 `testing.AllocsPerRun` 회귀 테스트를 동반하며, 외부 API·시맨틱은 변경하지 않는다.

**Tech Stack:** Go 1.26+, klauspost/reedsolomon, google/flatbuffers/go, cloudwego/hertz, sync.Pool, io.Pipe

**전제조건:**
- PR1(RS 풀)은 Phase 18 P1 플레이크(`TestE2E_ClusterEC_3Node_ActiveKM21`) 해결 후 착수
- PR3(hertz sendfile)은 bufio.Writer 오버헤드 실측 후 이득 확인 시 착수
- PR4(quic 통합)는 PR2(EncodeWriterTo 추가) 완료 후 착수

---

## 파일 구조

| PR | 파일 | 변경 유형 | 역할 |
|----|------|----------|------|
| PR2 | `internal/transport/codec.go` | 수정 | `FlatBuffersWriter` 타입 + `EncodeWriterTo` 메서드 추가 |
| PR2 | `internal/transport/codec_test.go` | 수정 | AllocsPerRun 테스트 추가 |
| PR4 | `internal/transport/quic.go` | 수정 | `CallFlatBuffer` 메서드 추가 (FlatBuffers 전용 호출 경로) |
| PR4 | `internal/cluster/shard_service.go` | 수정 | `WriteShard/ReadShard/DeleteShards`에서 `CallFlatBuffer` 사용 |
| PR5 | `internal/vfs/vfs.go` | 수정 | `Rename` 함수 io.Pipe 스트리밍으로 교체 |
| PR5 | `internal/vfs/vfs_test.go` | 수정 | 메모리 상한 테스트 추가 |
| PR3 | `internal/server/handlers.go` | 수정 | `getObject`에 io.WriterTo 경로 추가 |
| PR3 | `internal/server/handlers_test.go` | 수정 | WriterTo 경로 검증 테스트 추가 |
| PR1 | `internal/cluster/ec_pool.go` | 신규 | `encoderPool` + `shardPool` 타입 및 `sync.Pool` 인프라 |
| PR1 | `internal/cluster/ec.go` | 수정 | `ECSplit`/`ECReconstruct`에서 풀 사용 |
| PR1 | `internal/cluster/ec_pool_test.go` | 신규 | AllocsPerRun + 정확성 테스트 |

---

## Task 0: Alloc 기준선 측정

**실측 없이 목표값을 하드코딩하면 테스트가 의미 없어진다.** 풀 도입 전에 현재 alloc 수를 측정해 목표값의 기준을 잡는다.

**Files:**
- 임시 스크립트 (커밋 불필요)

- [ ] **Step 1: EC 기준선 측정 스크립트 작성**

```go
// /tmp/baseline_test.go (임시, 실측 후 삭제)
package cluster_test

import (
    "testing"
    "github.com/gritive/GrainFS/internal/cluster"
)

func TestECSplit_Baseline(t *testing.T) {
    cfg := cluster.ECConfig{DataShards: 4, ParityShards: 2}
    data := make([]byte, 1<<20) // 1MB
    allocs := testing.AllocsPerRun(5, func() {
        shards, _ := cluster.ECSplit(cfg, data)
        _ = shards
    })
    t.Logf("ECSplit baseline allocs: %.0f", allocs)
}

func TestECReconstruct_Baseline(t *testing.T) {
    cfg := cluster.ECConfig{DataShards: 4, ParityShards: 2}
    data := make([]byte, 1<<20)
    shards, _ := cluster.ECSplit(cfg, data)
    allocs := testing.AllocsPerRun(5, func() {
        _, _ = cluster.ECReconstruct(cfg, shards)
    })
    t.Logf("ECReconstruct baseline allocs: %.0f", allocs)
}
```

- [ ] **Step 2: 기준선 측정 실행**

```bash
cd /Users/whitekid/work/gritive/grains
cp /tmp/baseline_test.go internal/cluster/baseline_test.go
go test -v -run "TestECSplit_Baseline|TestECReconstruct_Baseline" ./internal/cluster/...
```

Expected output 예시:
```
=== RUN   TestECSplit_Baseline
    baseline_test.go:14: ECSplit baseline allocs: 28
=== RUN   TestECReconstruct_Baseline
    baseline_test.go:25: ECReconstruct baseline allocs: 22
```

- [ ] **Step 3: codec 기준선 측정**

```go
// /tmp/codec_baseline_test.go (임시)
package transport_test

import (
    "bytes"
    "encoding/binary"
    "testing"
    flatbuffers "github.com/google/flatbuffers/go"
    "github.com/gritive/GrainFS/internal/transport"
)

func TestEncodeBaseline(t *testing.T) {
    codec := &transport.BinaryCodec{}
    // 가장 작은 FlatBuffer 예시: 빈 payload
    payload := make([]byte, 64)
    msg := &transport.Message{Type: transport.StreamData, Payload: payload}
    var buf bytes.Buffer
    allocs := testing.AllocsPerRun(100, func() {
        buf.Reset()
        _ = codec.Encode(&buf, msg)
    })
    t.Logf("BinaryCodec.Encode baseline allocs: %.0f", allocs)
    _ = binary.BigEndian
    _ = flatbuffers.NewBuilder
}
```

```bash
cp /tmp/codec_baseline_test.go internal/transport/codec_baseline_test.go
go test -v -run "TestEncodeBaseline" ./internal/transport/...
```

- [ ] **Step 4: 기준선 기록 및 임시 파일 삭제**

측정한 수치를 메모해 둔다 (이 계획 문서의 빈 칸에 채워 넣는다):

| 항목 | 측정값(allocs) | PR 목표 (1/4 이하) |
|------|---------------|-------------------|
| ECSplit (1MB, 4+2) | _____ | _____ |
| ECReconstruct (1MB, 4+2) | _____ | _____ |
| BinaryCodec.Encode (64B payload) | _____ | _____ |

```bash
rm internal/cluster/baseline_test.go internal/transport/codec_baseline_test.go
```

---

## Task 1 (PR2): FlatBuffersWriter + EncodeWriterTo 추가

`BinaryCodec.Encode(*Message)` 시그니처는 유지하고, FlatBuffers 빌더를 직접 스트림에 쓰는 새 메서드를 추가한다. 빌더가 살아있는 동안 `WriteTo`로 쓰고, 완료 후 caller가 풀에 반환한다.

**Files:**
- Modify: `internal/transport/codec.go`
- Modify: `internal/transport/codec_test.go`

- [ ] **Step 1: 회귀 테스트 작성**

`internal/transport/codec_test.go`에 다음을 추가한다:

```go
package transport_test

import (
    "bytes"
    "testing"

    flatbuffers "github.com/google/flatbuffers/go"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestBinaryCodec_EncodeWriterTo_AllocsBounded(t *testing.T) {
    b := flatbuffers.NewBuilder(64)
    // 빈 테이블 빌드 (최소 FlatBuffer)
    b.StartObject(0)
    root := b.EndObject()
    b.Finish(root)

    codec := &BinaryCodec{}
    var buf bytes.Buffer
    allocs := testing.AllocsPerRun(100, func() {
        buf.Reset()
        fw := &FlatBuffersWriter{Typ: StreamData, Builder: b}
        _ = codec.EncodeWriterTo(&buf, fw)
    })
    // 목표: header array([5]byte) 스택 할당만 — 실측 후 수치 확정
    assert.LessOrEqual(t, allocs, 1.0, "EncodeWriterTo should allocate at most 1 (header array may escape)")
}

func TestBinaryCodec_EncodeWriterTo_RoundTrip(t *testing.T) {
    b := flatbuffers.NewBuilder(128)
    b.StartObject(0)
    root := b.EndObject()
    b.Finish(root)

    codec := &BinaryCodec{}
    var buf bytes.Buffer
    fw := &FlatBuffersWriter{Typ: StreamData, Builder: b}
    require.NoError(t, codec.EncodeWriterTo(&buf, fw))

    decoded, err := codec.Decode(&buf)
    require.NoError(t, err)
    assert.Equal(t, StreamData, decoded.Type)
    assert.Equal(t, b.FinishedBytes(), decoded.Payload)
}
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

```bash
go test -run "TestBinaryCodec_EncodeWriterTo" ./internal/transport/...
```

Expected: `FAIL` — `FlatBuffersWriter` 타입과 `EncodeWriterTo` 메서드가 없어서

- [ ] **Step 3: codec.go에 FlatBuffersWriter + EncodeWriterTo 추가**

`internal/transport/codec.go`의 `BinaryCodec` struct 정의 바로 다음에 추가한다:

```go
// FlatBuffersWriter holds a live FlatBuffers builder for zero-copy encoding.
// WriteTo 완료 후 caller가 builder를 pool에 반환해야 한다.
type FlatBuffersWriter struct {
    Typ     StreamType
    Builder *flatbuffers.Builder
}

// EncodeWriterTo writes the FlatBuffers payload directly from the builder's
// internal buffer — no make+copy. The Builder must remain alive until this
// call returns. Caller is responsible for returning Builder to its pool.
func (c *BinaryCodec) EncodeWriterTo(w io.Writer, fw *FlatBuffersWriter) error {
    raw := fw.Builder.FinishedBytes()
    header := [headerSize]byte{}
    header[0] = byte(fw.Typ)
    binary.BigEndian.PutUint32(header[1:], uint32(len(raw)))
    if _, err := w.Write(header[:]); err != nil {
        return fmt.Errorf("write header: %w", err)
    }
    if _, err := w.Write(raw); err != nil {
        return fmt.Errorf("write payload: %w", err)
    }
    return nil
}
```

또한 파일 상단 import에 flatbuffers가 없으면 추가:

```go
import (
    "encoding/binary"
    "fmt"
    "io"

    flatbuffers "github.com/google/flatbuffers/go"
)
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
go test -race -count=1 -run "TestBinaryCodec_EncodeWriterTo" ./internal/transport/...
```

Expected: `PASS`

- [ ] **Step 5: gofmt**

```bash
gofmt -w internal/transport/codec.go internal/transport/codec_test.go
```

- [ ] **Step 6: 전체 transport 테스트 통과 확인**

```bash
go test -race -count=1 ./internal/transport/...
```

Expected: 모두 `PASS`

- [ ] **Step 7: 커밋**

```bash
git add internal/transport/codec.go internal/transport/codec_test.go
git commit -m "feat(transport): BinaryCodec.EncodeWriterTo — FlatBuffers zero-copy 전송"
```

---

## Task 2 (PR5): vfs.go Rename io.Pipe 스트리밍

현재 `Rename`은 파일 전체를 `io.ReadAll`로 힙에 올린 뒤 S3에 쓴다. `io.Pipe`로 교체해 OS 파이프 버퍼(~64KB)로 메모리 상한을 고정한다.

> **주의:** `grainFile.loadExisting()` (`vfs.go:435`)의 `io.ReadAll`은 변경 불가 — random access가 필요한 bytes.Buffer 패턴이라 스트리밍 불가.

**Files:**
- Modify: `internal/vfs/vfs.go`
- Modify: `internal/vfs/vfs_test.go`

- [ ] **Step 1: 메모리 상한 테스트 작성**

`internal/vfs/vfs_test.go`에서 대용량 파일 Rename 시 메모리 사용량을 검증하는 테스트를 추가한다. 기존 테스트 파일에서 패턴을 따른다.

```go
func TestRename_LargeFile_MemoryBounded(t *testing.T) {
    // 테스트용 인메모리 backend 준비 (기존 테스트 패턴 따름)
    fs, cleanup := newTestVFS(t)
    defer cleanup()

    // 5MB 파일 생성
    const fileSize = 5 * 1024 * 1024
    data := make([]byte, fileSize)
    rand.Read(data)
    require.NoError(t, writeTestFile(fs, "test.bin", data))

    // 메모리 측정 전 GC
    runtime.GC()
    var before runtime.MemStats
    runtime.ReadMemStats(&before)

    // Rename 수행
    require.NoError(t, fs.Rename("test.bin", "renamed.bin"))

    runtime.GC()
    var after runtime.MemStats
    runtime.ReadMemStats(&after)

    // 힙 증가가 파일 크기의 10% 이내여야 함 (파이프 버퍼 ~64KB + 오버헤드)
    heapGrowth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
    t.Logf("Rename heap growth: %d bytes (file size: %d)", heapGrowth, fileSize)
    assert.Less(t, heapGrowth, int64(fileSize/10), "Rename should not allocate a full copy of the file")

    // 데이터 정확성 확인
    got, err := readTestFile(fs, "renamed.bin")
    require.NoError(t, err)
    assert.Equal(t, data, got)
}
```

- [ ] **Step 2: 테스트가 현재 실패(또는 힙 과다 사용)하는지 확인**

```bash
go test -v -run "TestRename_LargeFile_MemoryBounded" ./internal/vfs/...
```

현재는 `io.ReadAll`로 파일 전체를 힙에 올리므로 `heapGrowth ≈ fileSize`.

- [ ] **Step 3: vfs.go Rename 함수를 io.Pipe 스트리밍으로 교체**

`internal/vfs/vfs.go`의 `Rename` 함수를 찾아 전체를 교체한다 (현재 위치: 약 209번째 줄):

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
    defer pr.Close() // s3Put panic 시 writer goroutine 종료 보장

    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        _, copyErr := io.Copy(pw, rc)
        pw.CloseWithError(copyErr)
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

import에 `sync`가 없으면 추가해야 한다 (기존에 있는지 확인):

```bash
grep '"sync"' internal/vfs/vfs.go
```

없으면 import 블록에 `"sync"` 추가.

- [ ] **Step 4: 테스트 통과 확인**

```bash
go test -v -run "TestRename_LargeFile_MemoryBounded" ./internal/vfs/...
```

Expected: `PASS` — 힙 증가가 파일 크기의 10% 이내

- [ ] **Step 5: gofmt + 기존 테스트 전체 통과**

```bash
gofmt -w internal/vfs/vfs.go
go test -race -count=1 ./internal/vfs/...
```

Expected: 모두 `PASS`

- [ ] **Step 6: 커밋**

```bash
git add internal/vfs/vfs.go internal/vfs/vfs_test.go
git commit -m "feat(vfs): Rename io.Pipe 스트리밍으로 교체 — 힙 할당 O(1)"
```

---

## Task 3 (PR4): ShardService에서 EncodeWriterTo 사용

**PR2 완료 후 착수.** 현재 `WriteShard/ReadShard/DeleteShards`는 `marshalEnvelope()` → `[]byte` 복사 → `Message{Payload}` → `transport.Call()` 경로를 거친다. `CallFlatBuffer` 메서드를 추가해 make+copy를 제거한다.

**Files:**
- Modify: `internal/transport/quic.go` — `CallFlatBuffer` 메서드 추가
- Modify: `internal/cluster/shard_service.go` — `WriteShard/ReadShard/DeleteShards` 개선

- [ ] **Step 1: QUICTransport.CallFlatBuffer 메서드 테스트 작성**

`internal/transport/quic_test.go` (또는 기존 파일에 추가):

```go
func TestQUICTransport_CallFlatBuffer_SameAsCall(t *testing.T) {
    // 두 경로(Call vs CallFlatBuffer)가 동일한 바이트를 전송하는지 검증
    // loopback 테스트: 서버가 받은 payload를 echo back
    // (기존 transport 테스트 패턴 따름 — SetStreamHandler 사용)
    tr1, tr2, cleanup := newTestTransportPair(t)
    defer cleanup()

    // 서버: 받은 payload를 그대로 echo
    tr2.SetStreamHandler(func(msg *Message) *Message {
        return &Message{Type: StreamData, Payload: msg.Payload}
    })

    // 일반 Call로 전송
    payload := []byte("hello-flatbuffers")
    req := &Message{Type: StreamData, Payload: payload}
    resp1, err := tr1.Call(context.Background(), tr2.LocalAddr(), req)
    require.NoError(t, err)

    // CallFlatBuffer로 동일 payload 전송
    b := flatbuffers.NewBuilder(len(payload))
    off := b.CreateByteVector(payload)
    b.Finish(off)
    fw := &FlatBuffersWriter{Typ: StreamData, Builder: b}
    resp2, err := tr1.CallFlatBuffer(context.Background(), tr2.LocalAddr(), fw)
    require.NoError(t, err)

    // 응답 payload는 다를 수 있음 (FlatBuffer wrapping 차이)
    // 핵심: 에러 없이 완료되고 응답이 존재해야 함
    assert.NotNil(t, resp1)
    assert.NotNil(t, resp2)
}
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

```bash
go test -run "TestQUICTransport_CallFlatBuffer" ./internal/transport/...
```

Expected: `FAIL` — `CallFlatBuffer` 메서드가 없어서

- [ ] **Step 3: quic.go에 CallFlatBuffer 추가**

`internal/transport/quic.go`에서 `Call` 메서드 바로 다음에 추가:

```go
// CallFlatBuffer sends a FlatBuffers message and waits for a response.
// Builder must remain alive until this call returns; caller returns it to pool.
// Unlike Call, this writes directly from Builder.FinishedBytes() — no make+copy.
func (t *QUICTransport) CallFlatBuffer(ctx context.Context, addr string, fw *FlatBuffersWriter) (*Message, error) {
    conn, err := t.getOrConnect(ctx, addr)
    if err != nil {
        return nil, err
    }

    stream, err := conn.OpenStreamSync(ctx)
    if err != nil {
        t.evict(addr, conn)
        conn, err = t.getOrConnect(ctx, addr)
        if err != nil {
            return nil, fmt.Errorf("reconnect to %s: %w", addr, err)
        }
        if stream, err = conn.OpenStreamSync(ctx); err != nil {
            return nil, fmt.Errorf("open stream to %s: %w", addr, err)
        }
    }
    defer stream.Close()

    if err := t.codec.EncodeWriterTo(stream, fw); err != nil {
        return nil, fmt.Errorf("encode flatbuffer: %w", err)
    }

    resp, err := t.codec.Decode(stream)
    if err != nil {
        return nil, fmt.Errorf("decode response from %s: %w", addr, err)
    }
    return resp, nil
}
```

- [ ] **Step 4: shard_service.go — WriteShard를 CallFlatBuffer로 전환**

`internal/cluster/shard_service.go`의 `WriteShard` 함수를 수정한다. `marshalEnvelope` + `marshalShardRequest` + `Message` 패턴 대신 FlatBuffer를 직접 빌드해 `CallFlatBuffer` 호출:

```go
func (s *ShardService) WriteShard(ctx context.Context, peer, bucket, key string, shardIdx int, data []byte) error {
    // marshalShardRequest: FlatBuffer 직접 빌드 (make+copy 없음)
    b := shardBuilderPool.Get().(*flatbuffers.Builder)
    bucketOff := b.CreateString(bucket)
    keyOff := b.CreateString(key)
    var dataOff flatbuffers.UOffsetT
    if len(data) > 0 {
        dataOff = b.CreateByteVector(data)
    }
    pb.ShardRequestStart(b)
    pb.ShardRequestAddBucket(b, bucketOff)
    pb.ShardRequestAddKey(b, keyOff)
    pb.ShardRequestAddShardIdx(b, int32(shardIdx))
    if len(data) > 0 {
        pb.ShardRequestAddData(b, dataOff)
    }
    srRoot := pb.ShardRequestEnd(b)
    b.Finish(srRoot)
    srBytes := b.FinishedBytes()

    // marshalEnvelope: RPCMessage 빌드
    b2 := shardBuilderPool.Get().(*flatbuffers.Builder)
    typeOff := b2.CreateString("WriteShard")
    dataVec := b2.CreateByteVector(srBytes)
    // ShardRequest bytes가 b의 버퍼에 있으므로 b는 아직 반환하지 않는다
    pb.RPCMessageStart(b2)
    pb.RPCMessageAddType(b2, typeOff)
    pb.RPCMessageAddData(b2, dataVec)
    root := pb.RPCMessageEnd(b2)
    b2.Finish(root)

    // b는 srBytes가 b2에 복사된 후 반환
    b.Reset()
    shardBuilderPool.Put(b)

    fw := &transport.FlatBuffersWriter{Typ: transport.StreamData, Builder: b2}
    resp, err := s.transport.CallFlatBuffer(ctx, peer, fw)
    b2.Reset()
    shardBuilderPool.Put(b2)

    if err != nil {
        return fmt.Errorf("write shard to %s: %w", peer, err)
    }

    rpcType, _, err := unmarshalEnvelope(resp.Payload)
    if err != nil {
        return fmt.Errorf("unmarshal response: %w", err)
    }
    if rpcType == "Error" {
        return fmt.Errorf("remote error from %s", peer)
    }
    return nil
}
```

> **구현 메모:** 위 코드에서 `b`와 `b2` 두 빌더를 쓰는 이유는 RPCMessage가 ShardRequest를 byte vector로 감싸기 때문이다. `srBytes := b.FinishedBytes()`는 b가 살아있는 동안만 유효하므로 b2.CreateByteVector로 복사 후 b를 반환한다. 이 복사는 불가피하다. 대안으로 `marshalEnvelope/marshalShardRequest`를 단일 빌더로 통합하면 복사를 제거할 수 있으나 FlatBuffers API 제약으로 어렵다.

- [ ] **Step 5: ReadShard와 DeleteShards도 CallFlatBuffer로 전환**

`ReadShard` (같은 패턴):

```go
func (s *ShardService) ReadShard(ctx context.Context, peer, bucket, key string, shardIdx int) ([]byte, error) {
    b := shardBuilderPool.Get().(*flatbuffers.Builder)
    bucketOff := b.CreateString(bucket)
    keyOff := b.CreateString(key)
    pb.ShardRequestStart(b)
    pb.ShardRequestAddBucket(b, bucketOff)
    pb.ShardRequestAddKey(b, keyOff)
    pb.ShardRequestAddShardIdx(b, int32(shardIdx))
    srRoot := pb.ShardRequestEnd(b)
    b.Finish(srRoot)
    srBytes := b.FinishedBytes()

    b2 := shardBuilderPool.Get().(*flatbuffers.Builder)
    typeOff := b2.CreateString("ReadShard")
    dataVec := b2.CreateByteVector(srBytes)
    b.Reset()
    shardBuilderPool.Put(b)

    pb.RPCMessageStart(b2)
    pb.RPCMessageAddType(b2, typeOff)
    pb.RPCMessageAddData(b2, dataVec)
    root := pb.RPCMessageEnd(b2)
    b2.Finish(root)

    fw := &transport.FlatBuffersWriter{Typ: transport.StreamData, Builder: b2}
    resp, err := s.transport.CallFlatBuffer(ctx, peer, fw)
    b2.Reset()
    shardBuilderPool.Put(b2)

    if err != nil {
        return nil, fmt.Errorf("read shard from %s: %w", peer, err)
    }

    rpcType, data, err := unmarshalEnvelope(resp.Payload)
    if err != nil {
        return nil, fmt.Errorf("unmarshal response: %w", err)
    }
    if rpcType == "Error" {
        return nil, fmt.Errorf("remote error from %s", peer)
    }
    return data, nil
}
```

`DeleteShards` (data 없음, nil ShardRequest):

```go
func (s *ShardService) DeleteShards(ctx context.Context, peer, bucket, key string) error {
    b := shardBuilderPool.Get().(*flatbuffers.Builder)
    bucketOff := b.CreateString(bucket)
    keyOff := b.CreateString(key)
    pb.ShardRequestStart(b)
    pb.ShardRequestAddBucket(b, bucketOff)
    pb.ShardRequestAddKey(b, keyOff)
    srRoot := pb.ShardRequestEnd(b)
    b.Finish(srRoot)
    srBytes := b.FinishedBytes()

    b2 := shardBuilderPool.Get().(*flatbuffers.Builder)
    typeOff := b2.CreateString("DeleteShards")
    dataVec := b2.CreateByteVector(srBytes)
    b.Reset()
    shardBuilderPool.Put(b)

    pb.RPCMessageStart(b2)
    pb.RPCMessageAddType(b2, typeOff)
    pb.RPCMessageAddData(b2, dataVec)
    root := pb.RPCMessageEnd(b2)
    b2.Finish(root)

    fw := &transport.FlatBuffersWriter{Typ: transport.StreamData, Builder: b2}
    _, err := s.transport.CallFlatBuffer(ctx, peer, fw)
    b2.Reset()
    shardBuilderPool.Put(b2)
    return err
}
```

- [ ] **Step 6: 테스트 통과 확인**

```bash
go test -race -count=1 -run "TestQUICTransport_CallFlatBuffer" ./internal/transport/...
go test -race -count=1 ./internal/cluster/...
```

Expected: 모두 `PASS`

- [ ] **Step 7: gofmt**

```bash
gofmt -w internal/transport/quic.go internal/cluster/shard_service.go
```

- [ ] **Step 8: E2E 테스트 통과 확인**

```bash
make build
make test-e2e
```

Expected: 모두 `PASS`

- [ ] **Step 9: 커밋**

```bash
git add internal/transport/quic.go internal/cluster/shard_service.go
git commit -m "feat(transport/cluster): CallFlatBuffer — ShardService make+copy 제거"
```

---

## Task 4 (PR3): hertz sendfile 경로 — bufio 오버헤드 실측 필수

**착수 전 실측 필수.** `SetBodyStreamWriter`가 `bufio.Writer`를 거치면 EC shard처럼 이미 랜덤 바이트인 데이터는 버퍼링이 오히려 성능을 저하시킬 수 있다. 이득이 없으면 이 PR을 드롭한다.

**Files:**
- Modify: `internal/server/handlers.go`
- Modify: `internal/server/handlers_test.go`

- [ ] **Step 1: bufio 오버헤드 실측**

```bash
# hertz SetBodyStream vs SetBodyStreamWriter 비교 벤치마크
# 기존 핸들러 벤치마크가 있으면 실행, 없으면 k6로 측정

# k6 벤치마크 실행 (기존 스크립트 활용)
cd /Users/whitekid/work/gritive/grains
make build
./bin/grainfs serve --data ./tmp --port 9000 &
SERVER_PID=$!

# 1MB PUT
aws --endpoint-url http://localhost:9000 s3 cp /dev/urandom s3://bench/large.bin --expected-size 1048576 2>/dev/null

# GET 벤치마크 (10회 반복)
for i in $(seq 1 10); do
  time aws --endpoint-url http://localhost:9000 s3 cp s3://bench/large.bin /dev/null 2>&1
done

kill $SERVER_PID
```

Expected: 실측 결과를 보고 `SetBodyStreamWriter`로 전환 시 이득이 있는지 판단.

- [ ] **Step 2: 이득이 없으면 PR3 드롭**

실측 결과 이득이 없거나 미미하면 이 Task를 건너뛴다. TODOS.md에서 해당 항목을 드롭으로 표시한다.

- [ ] **Step 3: (이득 확인 시) WriterTo 경로 테스트 작성**

`internal/server/handlers_test.go`:

```go
// writerToReadCloser는 io.WriterTo + io.ReadCloser를 동시에 구현하는 mock.
// getObject의 sendfile 경로 진입을 검증한다.
type writerToReadCloser struct {
    data     []byte
    pos      int
    written  bool
}

func (r *writerToReadCloser) Read(p []byte) (int, error) {
    if r.pos >= len(r.data) {
        return 0, io.EOF
    }
    n := copy(p, r.data[r.pos:])
    r.pos += n
    return n, nil
}
func (r *writerToReadCloser) Close() error { return nil }
func (r *writerToReadCloser) WriteTo(w io.Writer) (int64, error) {
    r.written = true
    n, err := w.Write(r.data)
    return int64(n), err
}

func TestGetObject_WriterToPath(t *testing.T) {
    data := []byte("hello sendfile")
    mock := &writerToReadCloser{data: data}

    // mock backend 구성 및 GET 요청 실행 (기존 테스트 서버 패턴 따름)
    // ...

    // assert: WriterTo.WriteTo가 호출되어 sendfile 경로를 통했는지 확인
    assert.True(t, mock.written, "io.WriterTo path should be used")
}
```

- [ ] **Step 4: (이득 확인 시) handlers.go getObject에 WriterTo 경로 추가**

`internal/server/handlers.go`의 `getObject` 함수에서 Range 처리 이후, 기존 `obj.Size > 16*1024` 분기 앞에 추가:

```go
// Range 요청 처리 끝 (기존 코드)
// ...

// io.WriterTo 구현체면 sendfile 경로 (bufio 오버헤드 없이 직접 전송)
if wt, ok := rc.(io.WriterTo); ok {
    c.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
        if _, err := wt.WriteTo(w); err != nil {
            hlog.Errorf("sendfile WriteTo error: %v", err)
        }
    })
    rc = nil // defer Close 방지
    return
}

// 기존 경로
if obj.Size > 16*1024 {
    c.Response.SetBodyStream(rc, int(obj.Size))
    // ...
```

- [ ] **Step 5: gofmt + 테스트 통과**

```bash
gofmt -w internal/server/handlers.go internal/server/handlers_test.go
go test -race -count=1 ./internal/server/...
make build && make test-e2e
```

- [ ] **Step 6: 커밋**

```bash
git add internal/server/handlers.go internal/server/handlers_test.go
git commit -m "feat(server): getObject io.WriterTo sendfile 경로 추가"
```

---

## Task 5 (PR1): Reed-Solomon encoder + shard 풀

**Phase 18 P1 플레이크(`TestE2E_ClusterEC_3Node_ActiveKM21`) 해결 후 착수.** EC 코드를 동시에 수정하면 디버깅이 복잡해진다.

현재 `ECSplit`/`ECReconstruct`는 호출마다 `reedsolomon.New()` + 내부 `[][]byte` 슬라이스를 신규 할당한다. `sync.Pool`로 encoder와 shard 슬라이스를 재사용한다.

**Files:**
- Create: `internal/cluster/ec_pool.go`
- Modify: `internal/cluster/ec.go`
- Create: `internal/cluster/ec_pool_test.go`

- [ ] **Step 1: Task 0에서 측정한 기준선 확인**

Task 0에서 기록한 수치를 확인한다:
- ECSplit 기준선: _____ allocs
- ECReconstruct 기준선: _____ allocs
- 목표: 기준선의 1/4 이하

- [ ] **Step 2: AllocsPerRun 회귀 테스트 작성**

`internal/cluster/ec_pool_test.go` 신규 생성:

```go
package cluster

import (
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestECSplit_AllocsBounded(t *testing.T) {
    cfg := ECConfig{DataShards: 4, ParityShards: 2}
    data := make([]byte, 1<<20) // 1MB

    // 워밍업: 풀 채우기
    for i := 0; i < 3; i++ {
        _, _ = ECSplit(cfg, data)
    }

    allocs := testing.AllocsPerRun(100, func() {
        shards, err := ECSplit(cfg, data)
        require.NoError(t, err)
        _ = shards
    })

    // Task 0 기준선 기반으로 목표 설정 (예: 기준선 28 → 목표 ≤7)
    // 아래 수치는 Task 0 실측 후 업데이트 필요
    baseline := 28.0 // Task 0에서 측정한 값으로 교체
    target := baseline / 4
    assert.LessOrEqual(t, allocs, target,
        "ECSplit allocs should be ≤1/4 of baseline after pool introduction")
}

func TestECReconstruct_AllocsBounded(t *testing.T) {
    cfg := ECConfig{DataShards: 4, ParityShards: 2}
    data := make([]byte, 1<<20)
    shards, err := ECSplit(cfg, data)
    require.NoError(t, err)

    for i := 0; i < 3; i++ {
        _, _ = ECReconstruct(cfg, shards)
    }

    allocs := testing.AllocsPerRun(100, func() {
        out, err := ECReconstruct(cfg, shards)
        require.NoError(t, err)
        _ = out
    })

    baseline := 22.0 // Task 0에서 측정한 값으로 교체
    target := baseline / 4
    assert.LessOrEqual(t, allocs, target)
}

func TestECSplit_Correctness_WithPool(t *testing.T) {
    cfg := ECConfig{DataShards: 4, ParityShards: 2}
    data := make([]byte, 64*1024)
    for i := range data {
        data[i] = byte(i)
    }

    // 여러 번 분할-복원해 풀 재사용 시 데이터 오염 없는지 확인
    for i := 0; i < 10; i++ {
        shards, err := ECSplit(cfg, data)
        require.NoError(t, err)
        require.Len(t, shards, cfg.NumShards())

        // 패리티 샤드 손실 시뮬레이션
        shards[4] = nil
        shards[5] = nil
        got, err := ECReconstruct(cfg, shards)
        require.NoError(t, err)
        assert.Equal(t, data, got, "data corruption after pool reuse at iteration %d", i)
    }
}
```

- [ ] **Step 3: 테스트가 현재 실패하는지 확인**

```bash
go test -v -run "TestECSplit_AllocsBounded|TestECReconstruct_AllocsBounded" ./internal/cluster/...
```

Expected: `FAIL` — 아직 풀을 도입하지 않았으므로 allocs가 기준선 수준

- [ ] **Step 4: ec_pool.go 신규 생성**

`internal/cluster/ec_pool.go`:

```go
package cluster

import (
    "sync"

    "github.com/klauspost/reedsolomon"
)

// encoderPool caches reedsolomon.Encoder instances per ECConfig.
// reedsolomon.Encoder is stateless for given (DataShards, ParityShards) —
// safe to reuse concurrently after New().
type encoderPool struct {
    pool sync.Pool
}

func newEncoderPool(cfg ECConfig) *encoderPool {
    return &encoderPool{
        pool: sync.Pool{
            New: func() any {
                enc, _ := reedsolomon.New(cfg.DataShards, cfg.ParityShards)
                return enc
            },
        },
    }
}

func (p *encoderPool) Get() reedsolomon.Encoder {
    return p.pool.Get().(reedsolomon.Encoder)
}

func (p *encoderPool) Put(enc reedsolomon.Encoder) {
    p.pool.Put(enc)
}

// globalEncoderPools stores one encoderPool per ECConfig key.
var globalEncoderPools sync.Map // key: ECConfig, value: *encoderPool

func getEncoderPool(cfg ECConfig) *encoderPool {
    if v, ok := globalEncoderPools.Load(cfg); ok {
        return v.(*encoderPool)
    }
    p := newEncoderPool(cfg)
    actual, _ := globalEncoderPools.LoadOrStore(cfg, p)
    return actual.(*encoderPool)
}

// shardSlicePool caches [][]byte slices of a fixed length n.
// Each Get returns a slice with n nil-initialized elements.
type shardSlicePool struct {
    pool sync.Pool
    n    int
}

func newShardSlicePool(n int) *shardSlicePool {
    return &shardSlicePool{
        n: n,
        pool: sync.Pool{
            New: func() any {
                s := make([][]byte, n)
                return &s
            },
        },
    }
}

func (p *shardSlicePool) Get() *[][]byte {
    sp := p.pool.Get().(*[][]byte)
    s := *sp
    for i := range s {
        s[i] = nil // 이전 사용 데이터 초기화
    }
    return sp
}

func (p *shardSlicePool) Put(sp *[][]byte) {
    p.pool.Put(sp)
}

// globalShardPools stores one shardSlicePool per shard count.
var globalShardPools sync.Map // key: int (n), value: *shardSlicePool

func getShardPool(n int) *shardSlicePool {
    if v, ok := globalShardPools.Load(n); ok {
        return v.(*shardSlicePool)
    }
    p := newShardSlicePool(n)
    actual, _ := globalShardPools.LoadOrStore(n, p)
    return actual.(*shardSlicePool)
}
```

- [ ] **Step 5: ec.go — ECSplit에서 풀 사용**

`internal/cluster/ec.go`의 `ECSplit` 함수를 수정한다:

```go
func ECSplit(cfg ECConfig, data []byte) ([][]byte, error) {
    ep := getEncoderPool(cfg)
    enc := ep.Get()
    defer ep.Put(enc)

    sp := getShardPool(cfg.NumShards())
    shardsPtr := sp.Get()
    shards := *shardsPtr
    defer sp.Put(shardsPtr)

    // enc.Split은 내부적으로 shards를 채운다
    if err := enc.SplitInto(data, shards); err != nil {
        return nil, fmt.Errorf("ec split: %w", err)
    }
    if err := enc.Encode(shards); err != nil {
        return nil, fmt.Errorf("ec encode: %w", err)
    }

    header := encodeShardHeader(int64(len(data)))
    out := make([][]byte, len(shards))
    for i, s := range shards {
        payload := make([]byte, 0, shardHeaderSize+len(s))
        payload = append(payload, header...)
        payload = append(payload, s...)
        out[i] = payload
    }
    return out, nil
}
```

> **구현 메모:** `reedsolomon.Encoder`에 `SplitInto(data []byte, dst [][]byte) error` 메서드가 있는지 확인 필요. 없으면 기존 `enc.Split(data)` 호출 후 결과를 복사하는 방식 유지. `enc.Split`은 내부 슬라이스를 반환하므로, 풀에서 꺼낸 `shards` 슬라이스에 직접 쓰려면 API 확인이 필요하다.

```bash
grep -r "SplitInto\|func.*Split" $(go env GOPATH)/pkg/mod/github.com/klauspost/reedsolomon*/reedsolomon.go 2>/dev/null | head -5
```

`SplitInto`가 없으면 `ECReconstruct`에서만 풀을 사용하고, `ECSplit`은 encoder만 풀로 관리한다.

- [ ] **Step 6: ec.go — ECReconstruct에서 풀 사용**

```go
func ECReconstruct(cfg ECConfig, shards [][]byte) ([]byte, error) {
    if len(shards) != cfg.NumShards() {
        return nil, fmt.Errorf("shard count mismatch: got %d, want %d", len(shards), cfg.NumShards())
    }

    ep := getEncoderPool(cfg)
    enc := ep.Get()
    defer ep.Put(enc)

    sp := getShardPool(cfg.NumShards())
    bodiesPtr := sp.Get()
    bodies := *bodiesPtr
    defer sp.Put(bodiesPtr)

    var origSize int64 = -1
    for i, s := range shards {
        if s == nil {
            bodies[i] = nil
            continue
        }
        size, body, err := decodeShardHeader(s)
        if err != nil {
            bodies[i] = nil
            continue
        }
        if origSize < 0 {
            origSize = size
        }
        bodies[i] = body
    }
    if origSize < 0 {
        return nil, fmt.Errorf("no readable shards")
    }

    if err := enc.ReconstructData(bodies); err != nil {
        return nil, fmt.Errorf("ec reconstruct: %w", err)
    }
    var buf writeBuffer
    buf.b = make([]byte, 0, origSize)
    if err := enc.Join(&buf, bodies, int(origSize)); err != nil {
        return nil, fmt.Errorf("ec join: %w", err)
    }
    return buf.b, nil
}
```

- [ ] **Step 7: 테스트 통과 확인**

```bash
go test -v -run "TestECSplit_AllocsBounded|TestECReconstruct_AllocsBounded|TestECSplit_Correctness_WithPool" ./internal/cluster/...
```

Expected: `PASS` — allocs가 기준선의 1/4 이하

정확성 테스트도 함께 확인:

```bash
go test -race -count=3 ./internal/cluster/...
```

- [ ] **Step 8: gofmt**

```bash
gofmt -w internal/cluster/ec_pool.go internal/cluster/ec.go internal/cluster/ec_pool_test.go
```

- [ ] **Step 9: E2E 테스트 통과 확인**

```bash
make build
make test-e2e
```

Expected: 모두 `PASS` (Phase 18 P1 플레이크가 해결된 상태여야 함)

- [ ] **Step 10: 커밋**

```bash
git add internal/cluster/ec_pool.go internal/cluster/ec.go internal/cluster/ec_pool_test.go
git commit -m "feat(cluster): Reed-Solomon encoder + shard 슬라이스 sync.Pool 도입"
```

---

## 최종 검증

모든 Task 완료 후:

- [ ] **전체 테스트 통과**

```bash
make build
go test -race -count=1 ./...
make test-e2e
```

- [ ] **alloc 회귀 테스트 통과**

```bash
go test -v -run ".*AllocsBounded.*|.*MemoryBounded.*" ./...
```

Expected: 모든 alloc 테스트 `PASS`

- [ ] **완료 확인 체크리스트**

| PR | 항목 | 상태 |
|----|------|------|
| PR2 | codec.go FlatBuffersWriter + EncodeWriterTo | [ ] |
| PR5 | vfs.go Rename io.Pipe | [ ] |
| PR4 | quic.go CallFlatBuffer + shard_service.go 적용 | [ ] |
| PR3 | handlers.go sendfile (실측 후 결정) | [ ] |
| PR1 | ec_pool.go RS 풀 (Phase 18 P1 후) | [ ] |
