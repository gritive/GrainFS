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
| PR7 | `internal/cluster/codec.go`             | 수정  | `clusterBuilderPool` + `fbFinish` pool 반환 (~20함수)         | 미완료 |
| PR7 | `internal/storage/codec.go`             | 수정  | `storageBuilderPool` + `storageFbFinish` wrapper 추가 (2함수) | 미완료 |
| PR7 | `internal/storage/codec_test.go`        | 수정  | `marshalObject` AllocsPerRun 테스트                           | 미완료 |
| PR7 | `internal/raft/quic_rpc_codec.go`       | 수정  | `raftBuilderPool` + `fbFinishRPC` pool 반환 (~8함수)          | 미완료 |
| PR7 | `internal/raft/store.go`                | 수정  | `storeFbFinish` wrapper 추가 + `raftBuilderPool` 사용 (3함수) | 미완료 |
| PR7 | `internal/raft/quic_rpc_codec_test.go`  | 수정  | raft encode AllocsPerRun 테스트                               | 미완료 |
| PR7 | `internal/volume/codec.go`              | 수정  | `volumeBuilderPool` + `volumeFbFinish` wrapper 추가 (1함수)   | 미완료 |
| PR7 | `internal/volume/codec_test.go`         | 수정  | `marshalVolume` AllocsPerRun 테스트                           | 미완료 |
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

현재 `ECSplit`/`ECReconstruct`는 호출마다 `reedsolomon.New()` 를 새로 할당한다. `enc.Split(data)` API는 pre-allocated `[][]byte` 인자를 받지 않으므로 shard slice pool은 사용 불가. encoderPool만 도입해 `reedsolomon.New()` alloc을 제거한다.

**Files:**
- Create: `internal/cluster/ec_pool.go`
- Modify: `internal/cluster/ec.go`
- Create: `internal/cluster/ec_pool_test.go`

- [ ] **Step 1: ECSplit / ECReconstruct alloc 기준선 실측**

```bash
cat > /tmp/ec_baseline_test.go << 'EOF'
package cluster

import (
    "testing"
)

func TestECSplit_Baseline(t *testing.T) {
    cfg := ECConfig{DataShards: 4, ParityShards: 2}
    data := make([]byte, 1<<20)
    allocs := testing.AllocsPerRun(50, func() {
        _, _ = ECSplit(cfg, data)
    })
    t.Logf("ECSplit baseline allocs: %.0f", allocs)
}

func TestECReconstruct_Baseline(t *testing.T) {
    cfg := ECConfig{DataShards: 4, ParityShards: 2}
    data := make([]byte, 1<<20)
    shards, _ := ECSplit(cfg, data)
    allocs := testing.AllocsPerRun(50, func() {
        _, _ = ECReconstruct(cfg, shards)
    })
    t.Logf("ECReconstruct baseline allocs: %.0f", allocs)
}
EOF
cp /tmp/ec_baseline_test.go internal/cluster/ec_baseline_test.go
go test -v -run "TestECSplit_Baseline|TestECReconstruct_Baseline" ./internal/cluster/...
rm internal/cluster/ec_baseline_test.go
```

로그에 출력된 수치를 Step 2의 `ecsplitBaseline` / `ecreconBaseline` 에 기록한다.

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

    // Step 1에서 측정한 기준선 대비 감소 검증.
    // encoderPool은 reedsolomon.New() alloc(약 1-2)만 제거.
    // enc.Split 내부 shard/padding alloc은 제거 불가 (API 제약).
    // 목표: Step 1 기준선 − 2 이하 (encoder alloc 제거분)
    ecsplitBaseline := 0.0 // TODO: Step 1 실측값으로 교체
    assert.Less(t, allocs, ecsplitBaseline,
        "ECSplit allocs should decrease after encoderPool introduction")
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

    ecreconBaseline := 0.0 // TODO: Step 1 실측값으로 교체
    assert.Less(t, allocs, ecreconBaseline)
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
```

> **설계 메모:** `enc.Split(data []byte) ([][]byte, error)` 는 shard slice를 내부에서 할당한다. pre-allocated destination 인자(`SplitInto`)가 없어 shardSlicePool 적용 불가. encoderPool만으로 `reedsolomon.New()` alloc을 제거한다.

- [ ] **Step 5: ec.go — ECSplit에서 encoderPool 사용**

`internal/cluster/ec.go`의 `ECSplit` 함수를 수정한다. `enc.Split(data)` API는 그대로 사용하고, encoder만 풀에서 꺼낸다:

```go
func ECSplit(cfg ECConfig, data []byte) ([][]byte, error) {
    ep := getEncoderPool(cfg)
    enc := ep.Get()
    defer ep.Put(enc)

    shards, err := enc.Split(data)
    if err != nil {
        return nil, fmt.Errorf("ec split: %w", err)
    }
    if err := enc.Encode(shards); err != nil {
        return nil, fmt.Errorf("ec encode: %w", err)
    }

    header := encodeShardHeader(int64(len(data)))
    out := make([][]byte, len(shards))
    for i, s := range shards {
        payload := make([]byte, 0, shardHeaderSize+len(s))
        payload = append(payload, header[:]...)
        payload = append(payload, s...)
        out[i] = payload
    }
    return out, nil
}
```

- [ ] **Step 6: ec.go — ECReconstruct에서 encoderPool 사용**

```go
func ECReconstruct(cfg ECConfig, shards [][]byte) ([]byte, error) {
    if len(shards) != cfg.NumShards() {
        return nil, fmt.Errorf("shard count mismatch: got %d, want %d", len(shards), cfg.NumShards())
    }

    ep := getEncoderPool(cfg)
    enc := ep.Get()
    defer ep.Put(enc)

    bodies := make([][]byte, len(shards))
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

- [ ] **Step 5: nonce escape 분석 — [12]byte가 스택에 머무는지 확인**

```bash
go build -gcflags='-m=2' ./internal/encrypt/... 2>&1 | grep "nonce"
```

Expected: `nonce does not escape` 또는 출력 없음.  
만약 `nonce escapes to heap` 이면 nonce가 heap alloc되어 1 alloc 목표가 무의미해진다.  
그 경우 `var nonce [12]byte`를 `[]byte` 로 되돌리고 alloc target을 2로 조정한다.

- [ ] **Step 6: 테스트 통과 확인**

```bash
go test -v -run "TestEncryptWithAAD_AllocsBounded" ./internal/encrypt/...
```

Expected: `PASS` — 1 alloc (nonce가 스택에 머무는 경우)

- [ ] **Step 7: 기존 암복호화 정확성 테스트 통과 확인**

```bash
go test -race -count=1 ./internal/encrypt/...
```

Expected: 모두 `PASS`

- [ ] **Step 8: gofmt**

```bash
gofmt -w internal/encrypt/encrypt.go internal/encrypt/encrypt_test.go
```

- [ ] **Step 9: E2E 테스트 통과 확인**

```bash
make build && make test-e2e
```

Expected: 모두 `PASS`

- [ ] **Step 10: 커밋**

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

모든 encode 함수에서 `flatbuffers.NewBuilder(N)` 호출을 `clusterBuilderPool.Get().(*flatbuffers.Builder)` 로 교체한다. ~20개 함수 누락 방지를 위해 sed 사용:

```bash
# 교체 전 개수 확인
grep -c 'flatbuffers.NewBuilder' internal/cluster/codec.go

# 일괄 교체
sed -i '' 's/flatbuffers\.NewBuilder([0-9]*)/clusterBuilderPool.Get().(* flatbuffers.Builder)/g' internal/cluster/codec.go

# 공백 제거 (sed가 * 앞에 공백 삽입할 경우)
sed -i '' 's/\*[[:space:]]*flatbuffers\.Builder/\*flatbuffers.Builder/g' internal/cluster/codec.go

# 교체 후 확인 — 0개여야 함
grep -c 'flatbuffers.NewBuilder' internal/cluster/codec.go

# pool 호출 개수 확인
grep -c 'clusterBuilderPool.Get' internal/cluster/codec.go
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

`storage/codec.go`에는 `fbFinish` helper가 없다. 로컬 wrapper를 추가하고 inline 패턴을 교체한다:

```go
// storageBuilderPool 아래에 추가
func storageFbFinish(b *flatbuffers.Builder, root flatbuffers.UOffsetT) []byte {
    b.Finish(root)
    raw := b.FinishedBytes()
    out := make([]byte, len(raw))
    copy(out, raw)
    b.Reset()
    storageBuilderPool.Put(b)
    return out
}
```

기존 inline 패턴을 `storageFbFinish(b, root)` 호출로 교체한다:

```bash
# inline 패턴 위치 확인
grep -n "FinishedBytes\|NewBuilder" internal/storage/codec.go
```

- [ ] **Step 6: storage/codec.go AllocsPerRun 테스트 추가 + 통과 확인**

`internal/storage/codec_test.go` 에 추가 (없으면 신규 생성):

```go
func TestMarshalObject_AllocsBounded(t *testing.T) {
    obj := &ObjectInfo{Key: "testkey", Size: 1024, ContentType: "application/octet-stream"}
    _, _ = marshalObject(obj)

    allocs := testing.AllocsPerRun(100, func() {
        _, _ = marshalObject(obj)
    })
    assert.LessOrEqual(t, allocs, 2.0, "marshalObject should allocate ≤2 after builder pool")
}
```

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

`fbFinishRPC` 함수 수정 (기존 wrapper 교체):

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

`raft/store.go`는 `fbFinishRPC`가 없다. 로컬 wrapper 추가:

```go
// raft/store.go 상단 — raftBuilderPool은 quic_rpc_codec.go에 있으므로 해당 파일에서 참조 가능
// 동일 패키지이므로 pool 변수 공유됨
func storeFbFinish(b *flatbuffers.Builder, root flatbuffers.UOffsetT) []byte {
    b.Finish(root)
    raw := b.FinishedBytes()
    out := make([]byte, len(raw))
    copy(out, raw)
    b.Reset()
    raftBuilderPool.Put(b)
    return out
}
```

`quic_rpc_codec.go`와 `store.go`의 모든 `flatbuffers.NewBuilder(N)` → `raftBuilderPool.Get().(*flatbuffers.Builder)` 교체:

```bash
sed -i '' 's/flatbuffers\.NewBuilder([0-9]*)/raftBuilderPool.Get().(*flatbuffers.Builder)/g' internal/raft/quic_rpc_codec.go internal/raft/store.go
```

- [ ] **Step 8: raft AllocsPerRun 테스트 추가 + 통과 확인**

`internal/raft/quic_rpc_codec_test.go` 에 추가 (없으면 신규 생성):

```go
func TestEncodeRPCCommand_AllocsBounded(t *testing.T) {
    // quic_rpc_codec.go에서 대표 encode 함수 선택 (첫 번째 public 함수)
    // allocs 1회 워밍업 후 측정
    allocs := testing.AllocsPerRun(100, func() {
        // TODO: 실제 함수로 교체
        _ = EncodeXxx(...)
    })
    assert.LessOrEqual(t, allocs, 2.0)
}
```

```bash
go test -race -count=1 ./internal/raft/...
```

- [ ] **Step 9: volume/codec.go에 builderPool 추가**

`internal/volume/codec.go` 상단에:

```go
var volumeBuilderPool = sync.Pool{
    New: func() any { return flatbuffers.NewBuilder(128) },
}

func volumeFbFinish(b *flatbuffers.Builder, root flatbuffers.UOffsetT) []byte {
    b.Finish(root)
    raw := b.FinishedBytes()
    out := make([]byte, len(raw))
    copy(out, raw)
    b.Reset()
    volumeBuilderPool.Put(b)
    return out
}
```

`flatbuffers.NewBuilder(N)` → `volumeBuilderPool.Get().(*flatbuffers.Builder)` 교체.  
inline `b.Finish(); FinishedBytes(); make(); copy()` 패턴을 `volumeFbFinish(b, root)` 로 교체.

- [ ] **Step 10: volume AllocsPerRun 테스트 추가 + 통과 확인**

`internal/volume/codec_test.go` 에 추가 (없으면 신규 생성):

```go
func TestMarshalVolume_AllocsBounded(t *testing.T) {
    vol := &Volume{Name: "testvol", Size: 1 << 30}
    _, _ = marshalVolume(vol)

    allocs := testing.AllocsPerRun(100, func() {
        _, _ = marshalVolume(vol)
    })
    assert.LessOrEqual(t, allocs, 2.0, "marshalVolume should allocate ≤2 after builder pool")
}
```

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
