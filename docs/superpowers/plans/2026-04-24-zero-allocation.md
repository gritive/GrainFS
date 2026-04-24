# Zero-Allocation / Zero-Copy 최적화 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** GrainFS 핫패스(RS 인코딩, 암호화, QUIC FlatBuffers 전송, NFS→S3 Rename, Raft Cmd 인코딩)에서 불필요한 메모리 할당을 제거해 Phase 19 성능 최적화의 기반을 만든다.

**Architecture:** 8개 독립 PR을 위험도 낮은 순서로 구현한다. PR2/PR4/PR5는 v0.0.4.19에서 완료. 미완료 순서: PR6(EncryptWithAAD 1-alloc) → PR7(FlatBuffers Builder Pool for Raft Cmd) → PR1(EC encoder pool) → PR8(encodeShardHeader stack). 각 PR은 `testing.AllocsPerRun` 회귀 테스트를 동반하며, 외부 API·시맨틱은 변경하지 않는다.

**Tech Stack:** Go 1.26+, klauspost/reedsolomon, google/flatbuffers/go, cloudwego/hertz, sync.Pool, io.Pipe, AES-256-GCM

**완료된 PR (v0.0.4.19):**
- PR2: transport/codec.go FlatBuffersWriter + EncodeWriterTo ✅
- PR4: transport/quic.go CallFlatBuffer + shard_service.go 적용 ✅
- PR5: vfs/vfs.go Rename io.Pipe 스트리밍 ✅

**미완료 PR:**
- PR1: EC encoder + shard pool (전제조건 없음 — Phase 18 P1 해결됨)
- PR3: hertz sendfile (bufio 실측 후 결정)
- PR6: EncryptWithAAD 3→1 alloc
- PR7: FlatBuffers Builder Pool (Raft Cmd 경로 — cluster/storage/raft/volume)
- PR8: encodeShardHeader [8]byte stack 반환

---

## 파일 구조

| PR  | 파일                                | 변경 유형 | 역할                                                          | 상태 |
| --- | ----------------------------------- | --------- | ------------------------------------------------------------- | ---- |
| PR3 | `internal/server/handlers.go`       | 수정      | `getObject`에 io.WriterTo 경로 추가                           | 실측 후 결정 |
| PR3 | `internal/server/handlers_test.go`  | 수정      | WriterTo 경로 검증 테스트 추가                                | 실측 후 결정 |
| PR1 | `internal/cluster/ec_pool.go`       | 신규      | `encoderPool` + `shardPool` 타입 및 `sync.Pool` 인프라        | 미완료 |
| PR1 | `internal/cluster/ec.go`            | 수정      | `ECSplit`/`ECReconstruct`에서 풀 사용                         | 미완료 |
| PR1 | `internal/cluster/ec_pool_test.go`  | 신규      | AllocsPerRun + 정확성 테스트                                  | 미완료 |
| PR6 | `internal/encrypt/encrypt.go`       | 수정      | `EncryptWithAAD` 3→1 alloc                                    | 미완료 |
| PR6 | `internal/encrypt/encrypt_test.go`  | 수정      | AllocsPerRun 테스트 추가                                      | 미완료 |
| PR7 | `internal/cluster/codec.go`         | 수정      | `clusterBuilderPool` + `fbFinish` pool 반환                   | 미완료 |
| PR7 | `internal/storage/codec.go`         | 수정      | `storageBuilderPool` + `fbFinish` pool 반환                   | 미완료 |
| PR7 | `internal/raft/quic_rpc_codec.go`   | 수정      | `raftBuilderPool` + `fbFinishRPC` pool 반환                   | 미완료 |
| PR7 | `internal/raft/store.go`            | 수정      | `raftBuilderPool` 사용                                        | 미완료 |
| PR7 | `internal/volume/codec.go`          | 수정      | `volumeBuilderPool` + `fbFinish` pool 반환                    | 미완료 |
| PR8 | `internal/cluster/ec.go`            | 수정      | `encodeShardHeader` → `[8]byte` 반환                          | 미완료 |

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

**전제조건 없음 — Phase 18 P1 플레이크 v0.0.4.21에서 해결 완료.** EC 코드를 동시에 수정하면 디버깅이 복잡해진다.

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

---

## Task 6 (PR6): EncryptWithAAD 3→1 Alloc

`shard_service.go`의 `WriteShard`/`ReadShard`/`writeShard`가 매 샤드마다 `EncryptWithAAD`를 호출한다. 현재 3 allocs → 1 alloc으로 줄인다.

**Files:**
- Modify: `internal/encrypt/encrypt.go`
- Modify: `internal/encrypt/encrypt_test.go`

- [ ] **Step 1: AllocsPerRun 기준선 측정**

```bash
cd /Users/whitekid/work/gritive/grains
cat > /tmp/encrypt_baseline_test.go << 'EOF'
package encrypt_test

import (
    "testing"
    "github.com/gritive/GrainFS/internal/encrypt"
)

func TestEncryptWithAAD_Baseline(t *testing.T) {
    e, _ := encrypt.New(make([]byte, 32))
    plaintext := make([]byte, 128*1024) // 128KB shard
    aad := []byte("bucket/key/0")
    allocs := testing.AllocsPerRun(100, func() {
        _, _ = e.EncryptWithAAD(plaintext, aad)
    })
    t.Logf("EncryptWithAAD baseline allocs: %.0f", allocs)
}
EOF
cp /tmp/encrypt_baseline_test.go internal/encrypt/encrypt_baseline_test.go
go test -v -run "TestEncryptWithAAD_Baseline" ./internal/encrypt/...
rm internal/encrypt/encrypt_baseline_test.go
```

Expected 예시: `EncryptWithAAD baseline allocs: 3`

- [ ] **Step 2: AllocsPerRun 회귀 테스트 작성**

`internal/encrypt/encrypt_test.go`에 추가:

```go
func TestEncryptWithAAD_AllocsBounded(t *testing.T) {
    e, err := New(make([]byte, 32))
    require.NoError(t, err)
    plaintext := make([]byte, 128*1024)
    aad := []byte("bucket/key/0")

    // 워밍업
    _, _ = e.EncryptWithAAD(plaintext, aad)

    allocs := testing.AllocsPerRun(100, func() {
        _, _ = e.EncryptWithAAD(plaintext, aad)
    })
    assert.LessOrEqual(t, allocs, 1.0, "EncryptWithAAD should allocate exactly 1 (output slice)")
}
```

- [ ] **Step 3: 테스트 실패 확인**

```bash
go test -v -run "TestEncryptWithAAD_AllocsBounded" ./internal/encrypt/...
```

Expected: `FAIL` — 현재 3 allocs

- [ ] **Step 4: encrypt.go EncryptWithAAD 최적화**

`internal/encrypt/encrypt.go`의 `EncryptWithAAD` 함수를 교체한다. `NonceSize()`는 AES-256-GCM에서 항상 12이므로 배열 크기를 고정한다:

```go
func (e *Encryptor) EncryptWithAAD(plaintext, aad []byte) ([]byte, error) {
    var nonce [12]byte // AES-256-GCM NonceSize = 12, stack alloc
    if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
        return nil, fmt.Errorf("generate nonce: %w", err)
    }
    // 출력 형식: magic(2) + nonce(12) + ciphertext + tag(16)
    // make 한 번으로 전체 크기 예약 — aead.Seal이 in-place append
    out := make([]byte, 2, 2+12+len(plaintext)+16)
    out[0] = encMagic0
    out[1] = encMagic1
    out = append(out, nonce[:]...)
    out = e.aead.Seal(out, nonce[:], plaintext, aad)
    return out, nil
}
```

- [ ] **Step 5: 테스트 통과 확인**

```bash
go test -v -run "TestEncryptWithAAD_AllocsBounded" ./internal/encrypt/...
```

Expected: `PASS` — 1 alloc

- [ ] **Step 6: 기존 암복호화 정확성 테스트 통과 확인**

```bash
go test -race -count=1 ./internal/encrypt/...
```

Expected: 모두 `PASS`

- [ ] **Step 7: gofmt**

```bash
gofmt -w internal/encrypt/encrypt.go internal/encrypt/encrypt_test.go
```

- [ ] **Step 8: E2E 테스트 통과 확인**

```bash
make build && make test-e2e
```

Expected: 모두 `PASS`

- [ ] **Step 9: 커밋**

```bash
git add internal/encrypt/encrypt.go internal/encrypt/encrypt_test.go
git commit -m "perf(encrypt): EncryptWithAAD 3→1 alloc — nonce stack 배열 + 단일 make"
```

---

## Task 7 (PR7): FlatBuffers Builder Pool — Raft Cmd 경로

cluster/storage/raft/volume의 Raft Cmd 인코딩 함수 전체가 `flatbuffers.NewBuilder()` + `fbFinish` 패턴을 반복한다. 각 패키지에 `sync.Pool`을 추가해 Builder alloc을 제거한다. `make+copy`는 BadgerDB/Raft 소유권 이전 때문에 유지한다.

**Files:**
- Modify: `internal/cluster/codec.go`
- Modify: `internal/storage/codec.go`
- Modify: `internal/raft/quic_rpc_codec.go`
- Modify: `internal/raft/store.go`
- Modify: `internal/volume/codec.go`

- [ ] **Step 1: cluster/codec.go 기준선 측정**

```bash
cat > /tmp/cluster_codec_baseline_test.go << 'EOF'
package cluster_test

import (
    "testing"
    "github.com/gritive/GrainFS/internal/cluster"
)

func TestEncodeObjectMeta_Baseline(t *testing.T) {
    meta := &cluster.ObjectMeta{Key: "testkey", Size: 1024, ContentType: "application/octet-stream"}
    allocs := testing.AllocsPerRun(100, func() {
        _, _ = cluster.EncodeObjectMeta(meta)
    })
    t.Logf("EncodeObjectMeta baseline allocs: %.0f", allocs)
}
EOF
cp /tmp/cluster_codec_baseline_test.go internal/cluster/cluster_codec_baseline_test.go
go test -v -run "TestEncodeObjectMeta_Baseline" ./internal/cluster/...
rm internal/cluster/cluster_codec_baseline_test.go
```

- [ ] **Step 2: cluster/codec.go에 builderPool 추가**

`internal/cluster/codec.go` 파일 상단 (import 다음)에 추가:

```go
var clusterBuilderPool = sync.Pool{
    New: func() any { return flatbuffers.NewBuilder(256) },
}
```

`fbFinish` 함수를 수정해 builder를 pool에 반환:

```go
func fbFinish(b *flatbuffers.Builder, root flatbuffers.UOffsetT) []byte {
    b.Finish(root)
    raw := b.FinishedBytes()
    out := make([]byte, len(raw))
    copy(out, raw)
    b.Reset()
    clusterBuilderPool.Put(b)
    return out
}
```

모든 encode 함수에서 `flatbuffers.NewBuilder(N)` 호출을 `clusterBuilderPool.Get().(*flatbuffers.Builder)` 로 교체한다. 예시:

```go
// 변경 전
func EncodeCreateBucket(name string) ([]byte, error) {
    b := flatbuffers.NewBuilder(64)
    ...
}

// 변경 후
func EncodeCreateBucket(name string) ([]byte, error) {
    b := clusterBuilderPool.Get().(*flatbuffers.Builder)
    ...
}
```

import에 `"sync"`가 없으면 추가:

```bash
grep '"sync"' internal/cluster/codec.go
```

- [ ] **Step 3: cluster/codec.go AllocsPerRun 테스트 추가**

`internal/cluster/codec.go`가 있는 패키지의 테스트 파일(예: `internal/cluster/codec_test.go`, 없으면 신규 생성)에 추가:

```go
func TestEncodeObjectMeta_AllocsBounded(t *testing.T) {
    meta := &ObjectMeta{Key: "testkey", Size: 1024, ContentType: "application/octet-stream"}
    // 워밍업
    _, _ = EncodeObjectMeta(meta)

    allocs := testing.AllocsPerRun(100, func() {
        _, _ = EncodeObjectMeta(meta)
    })
    // NewBuilder alloc 제거 후 make+copy 1회만 남아야 함
    assert.LessOrEqual(t, allocs, 2.0, "EncodeObjectMeta should allocate ≤2 after builder pool")
}
```

- [ ] **Step 4: cluster/codec.go 테스트 통과 확인**

```bash
go test -race -count=1 -run "TestEncodeObjectMeta_AllocsBounded" ./internal/cluster/...
go test -race -count=1 ./internal/cluster/...
```

Expected: 모두 `PASS`

- [ ] **Step 5: storage/codec.go에 builderPool 추가**

`internal/storage/codec.go` 파일 상단에 추가:

```go
var storageBuilderPool = sync.Pool{
    New: func() any { return flatbuffers.NewBuilder(256) },
}
```

`storage/codec.go`의 모든 `flatbuffers.NewBuilder(N)` 호출을 `storageBuilderPool.Get().(*flatbuffers.Builder)` 로 교체.

storage의 `fbFinish`가 없으면 직접 패턴 확인 (`b.Finish(root); raw := b.FinishedBytes(); out := make...; copy...` 패턴을 찾아 pool 반환 추가):

```bash
grep -n "NewBuilder\|FinishedBytes\|Reset\(\)" internal/storage/codec.go
```

각 encode 함수 마지막에 `b.Reset(); storageBuilderPool.Put(b)` 추가.

- [ ] **Step 6: storage/codec.go 테스트 통과 확인**

```bash
go test -race -count=1 ./internal/storage/...
```

- [ ] **Step 7: raft/quic_rpc_codec.go + raft/store.go에 builderPool 추가**

`internal/raft/quic_rpc_codec.go` 상단에:

```go
var raftBuilderPool = sync.Pool{
    New: func() any { return flatbuffers.NewBuilder(256) },
}
```

`fbFinishRPC` 함수 수정:

```go
func fbFinishRPC(b *flatbuffers.Builder, root flatbuffers.UOffsetT) []byte {
    b.Finish(root)
    raw := b.FinishedBytes()
    out := make([]byte, len(raw))
    copy(out, raw)
    b.Reset()
    raftBuilderPool.Put(b)
    return out
}
```

`quic_rpc_codec.go`와 `store.go`의 모든 `flatbuffers.NewBuilder(N)` → `raftBuilderPool.Get().(*flatbuffers.Builder)` 교체.

- [ ] **Step 8: raft 테스트 통과 확인**

```bash
go test -race -count=1 ./internal/raft/...
```

- [ ] **Step 9: volume/codec.go에 builderPool 추가**

`internal/volume/codec.go` 상단에:

```go
var volumeBuilderPool = sync.Pool{
    New: func() any { return flatbuffers.NewBuilder(128) },
}
```

`flatbuffers.NewBuilder(N)` → `volumeBuilderPool.Get().(*flatbuffers.Builder)` 교체.  
함수 마지막에 `b.Reset(); volumeBuilderPool.Put(b)` 추가.

- [ ] **Step 10: volume 테스트 통과 확인**

```bash
go test -race -count=1 ./internal/volume/...
```

- [ ] **Step 11: gofmt**

```bash
gofmt -w internal/cluster/codec.go internal/storage/codec.go internal/raft/quic_rpc_codec.go internal/raft/store.go internal/volume/codec.go
```

- [ ] **Step 12: E2E 테스트 통과 확인**

```bash
make build && make test-e2e
```

Expected: 모두 `PASS`

- [ ] **Step 13: 커밋**

```bash
git add internal/cluster/codec.go internal/storage/codec.go internal/raft/quic_rpc_codec.go internal/raft/store.go internal/volume/codec.go
git commit -m "perf(cluster/raft/storage/volume): FlatBuffers Builder sync.Pool — NewBuilder alloc 제거"
```

---

## Task 8 (PR8): encodeShardHeader → [8]byte 반환

`internal/cluster/ec.go`의 `encodeShardHeader`가 `make([]byte, 8)` 를 매 ECSplit 샤드마다 호출한다. 배열 값 반환으로 heap alloc 제거.

**Files:**
- Modify: `internal/cluster/ec.go`

- [ ] **Step 1: encodeShardHeader 함수 반환 타입 변경**

`internal/cluster/ec.go:88` 의 `encodeShardHeader` 함수를 교체:

```go
func encodeShardHeader(origSize int64) [shardHeaderSize]byte {
    var h [shardHeaderSize]byte
    binary.BigEndian.PutUint64(h[:], uint64(origSize))
    return h
}
```

- [ ] **Step 2: 호출부 수정**

`ec.go:116` 부근의 호출부를 찾아 수정:

```bash
grep -n "encodeShardHeader" internal/cluster/ec.go
```

현재 패턴:
```go
header := encodeShardHeader(int64(len(data)))
// ...
payload = append(payload, header...)
```

변경 후:
```go
header := encodeShardHeader(int64(len(data)))
// header는 [8]byte 배열 — append에 slice 전달
payload = append(payload, header[:]...)
```

- [ ] **Step 3: 컴파일 확인**

```bash
go build ./internal/cluster/...
```

Expected: 에러 없음

- [ ] **Step 4: 기존 EC 테스트 통과 확인**

```bash
go test -race -count=1 ./internal/cluster/...
```

Expected: 모두 `PASS`

- [ ] **Step 5: gofmt**

```bash
gofmt -w internal/cluster/ec.go
```

- [ ] **Step 6: 커밋**

```bash
git add internal/cluster/ec.go
git commit -m "perf(cluster): encodeShardHeader [8]byte 반환 — heap alloc 제거"
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

| PR  | 항목                                           | 상태 |
| --- | ---------------------------------------------- | ---- |
| PR6 | encrypt.go EncryptWithAAD 3→1 alloc            | [ ] |
| PR7 | cluster/raft/storage/volume Builder pool       | [ ] |
| PR1 | ec_pool.go RS encoder + shard pool             | [ ] |
| PR8 | ec.go encodeShardHeader [8]byte                | [ ] |
| PR3 | handlers.go sendfile (실측 후 결정)            | [ ] |
