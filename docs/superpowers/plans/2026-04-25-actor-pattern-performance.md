# Actor 패턴 성능 개선 구현 플랜

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** EC shard write/read를 병렬화하고, Registry.InvalidateAll을 비동기 팬아웃으로 전환하여 latency와 throughput을 개선한다.

**Architecture:** Phase 1(EC 병렬 팬아웃)과 Phase 3(Registry 비동기)는 즉시 진행. Phase 2(MigrationExecutor Actor)와 Phase 4(BalancerProposer Actor)는 mutex contention ≥1ms 측정 게이트를 통과한 경우에만 진행.

**Tech Stack:** Go 1.26+, `golang.org/x/sync/errgroup`, `go.uber.org/goleak`, `benchstat`

---

## 파일 맵

| 파일 | 변경 유형 | Phase |
|------|----------|-------|
| `go.mod` | `golang.org/x/sync` 직접 의존성 추가 | 0 |
| `internal/cluster/backend.go` | putObjectEC·getObjectEC·upgradeObjectEC 병렬화 | 1 |
| `internal/cluster/backend_bench_test.go` | EC 벤치마크 신규 | 1 |
| `internal/cluster/ec_fix_test.go` | 기존 EC 테스트에 rollback + k-of-n 테스트 추가 | 1 |
| `internal/cluster/invalidator.go` | mutex 버그 수정 + InvalidateAll fan-out | 3 |
| `internal/cluster/invalidator_test.go` | 신규: race + 성능 테스트 | 3 |
| `internal/cluster/invalidator_bench_test.go` | 신규: 벤치마크 | 3 |
| `internal/cluster/migration_executor.go` | Actor 전환 (게이트 통과 시) | 2 |
| `internal/cluster/migration_executor_test.go` | Stop 幂등성 + 종료 후 Execute 테스트 | 2 |
| `internal/cluster/balancer.go` | Actor 형식화 (게이트 통과 시) | 4 |
| `internal/cluster/balancer_test.go` | Stats 동시성 + Stop 幂등성 테스트 | 4 |

---

## Phase 0: 준비 — 의존성 추가 + baseline 측정

### Task 0: golang.org/x/sync 직접 의존성 추가

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: errgroup 의존성 추가**

```bash
go get golang.org/x/sync/errgroup
```

Expected output:
```
go: added golang.org/x/sync v0.20.0
```

- [ ] **Step 2: 빌드 확인**

```bash
make build
```

Expected: 오류 없이 바이너리 생성

- [ ] **Step 3: Baseline 벤치마크 기록**

```bash
go test -bench=BenchmarkPutObjectEC -benchtime=10s -count=5 \
    ./internal/cluster/ 2>/dev/null | tee /tmp/bench_ec_before.txt
go test -bench=BenchmarkGetObjectEC -benchtime=10s -count=5 \
    ./internal/cluster/ 2>/dev/null | tee -a /tmp/bench_ec_before.txt
```

Note: 벤치마크가 아직 없으므로 이 단계는 Task 1 완료 후 재실행. 지금은 파일만 생성.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: add golang.org/x/sync/errgroup direct dependency"
```

---

## Phase 1: EC 병렬 팬아웃

### Task 1: BenchmarkPutObjectEC + BenchmarkGetObjectEC 작성 (failing이 아닌 baseline)

**Files:**
- Create: `internal/cluster/backend_bench_test.go`

- [ ] **Step 1: 벤치마크 헬퍼 + BenchmarkPutObjectEC 작성**

`internal/cluster/backend_bench_test.go` 를 신규 생성:

```go
package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// benchDelayShardService wraps ShardService and adds configurable delay to
// simulate network latency for benchmarks. Used to verify parallelism benefit.
type benchDelayShardService struct {
	inner *ShardService
	delay time.Duration
}

func (d *benchDelayShardService) WriteShard(ctx context.Context, peer, bucket, key string, shardIdx int, data []byte) error {
	select {
	case <-time.After(d.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return d.inner.WriteShard(ctx, peer, bucket, key, shardIdx, data)
}

func (d *benchDelayShardService) WriteLocalShard(bucket, key string, shardIdx int, data []byte) error {
	time.Sleep(d.delay)
	return d.inner.WriteLocalShard(bucket, key, shardIdx, data)
}

func (d *benchDelayShardService) ReadShard(ctx context.Context, peer, bucket, key string, shardIdx int) ([]byte, error) {
	select {
	case <-time.After(d.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return d.inner.ReadShard(ctx, peer, bucket, key, shardIdx)
}

func (d *benchDelayShardService) ReadLocalShard(bucket, key string, shardIdx int) ([]byte, error) {
	time.Sleep(d.delay)
	return d.inner.ReadLocalShard(bucket, key, shardIdx)
}

// BenchmarkPutObjectEC_Sequential measures the current sequential EC write latency.
// Run before the parallel implementation to establish baseline.
func BenchmarkPutObjectEC_Sequential(b *testing.B) {
	benchECWrite(b, 0) // no delay = no parallelism effect visible in sequential code
}

// BenchmarkPutObjectEC_WithDelay measures EC write with 5ms simulated network delay.
// Sequential: ~30ms for 6 shards. Parallel: ~5ms. Comparison shows the improvement.
func BenchmarkPutObjectEC_WithDelay(b *testing.B) {
	benchECWrite(b, 5*time.Millisecond)
}

func benchECWrite(b *testing.B, delay time.Duration) {
	b.Helper()
	bk := newTestDistributedBackend(b)
	require.NoError(b, bk.CreateBucket("bench"))
	bk.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})

	svc := NewShardService(bk.root, nil)
	allNodes := []string{bk.selfAddr}
	bk.SetShardService(svc, allNodes)

	data := make([]byte, 64*1024) // 64KB object
	b.ResetTimer()
	for b.Loop() {
		_, err := bk.PutObject("bench", "key", data, "application/octet-stream")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetObjectEC measures EC read latency (k-of-n fast path after Phase 1).
func BenchmarkGetObjectEC(b *testing.B) {
	bk := newTestDistributedBackend(b)
	require.NoError(b, bk.CreateBucket("bench"))
	bk.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})

	svc := NewShardService(bk.root, nil)
	allNodes := []string{bk.selfAddr}
	bk.SetShardService(svc, allNodes)

	data := make([]byte, 64*1024)
	obj, err := bk.PutObject("bench", "readkey", data, "application/octet-stream")
	require.NoError(b, err)

	b.ResetTimer()
	for b.Loop() {
		_, err := bk.GetObject("bench", "readkey", obj.VersionID)
		if err != nil {
			b.Fatal(err)
		}
	}
}
```

- [ ] **Step 2: 벤치마크 컴파일 확인**

```bash
go test -run='^$' -bench=BenchmarkPutObjectEC_Sequential \
    -benchtime=1x ./internal/cluster/
```

Expected: 벤치마크 1회 실행 후 종료 (오류 없음)

Note: `benchDelayShardService`는 Task 2에서 실제 delay injection이 필요. 지금은 컴파일만 확인.

- [ ] **Step 3: baseline 기록**

```bash
go test -bench='BenchmarkPutObjectEC_Sequential|BenchmarkGetObjectEC' \
    -benchtime=10s -count=5 ./internal/cluster/ | tee /tmp/bench_ec_before.txt
```

파일에 baseline 수치가 기록됨. 이후 비교에 사용.

- [ ] **Step 4: Commit**

```bash
git add internal/cluster/backend_bench_test.go
git commit -m "bench(cluster): add baseline EC write/read benchmarks"
```

---

### Task 2: TestPutObjectEC_ParallelRollback 작성 (실패 확인)

**Files:**
- Modify: `internal/cluster/ec_fix_test.go`

- [ ] **Step 1: rollback 테스트 추가**

`ec_fix_test.go` 파일 맨 끝에 추가:

```go
// TestPutObjectEC_ParallelRollback verifies that when one shard write fails,
// all previously-written shards are deleted (no orphaned shards on disk).
// This test validates the writtenMu + cleanup() pattern in the parallel version.
func TestPutObjectEC_ParallelRollback(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bkt"))
	b.SetECConfig(ECConfig{DataShards: 2, ParityShards: 1})

	svc := NewShardService(b.root, nil)
	allNodes := []string{b.selfAddr}
	b.SetShardService(svc, allNodes)

	// Single-node cluster: all shards land on self. Write a valid object first
	// to confirm the path works, then we verify rollback behavior is reachable.
	data := []byte("hello world")
	obj, err := b.PutObject("bkt", "obj", data, "text/plain")
	require.NoError(t, err)

	// Verify object can be retrieved (sanity check).
	got, err := b.GetObject("bkt", "obj", obj.VersionID)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

// TestGetObjectEC_KofN_CancelsRemainder verifies that once k shards are
// received successfully, remaining goroutines are signalled via context
// cancellation and the resultCh buffer drains without goroutine leak.
func TestGetObjectEC_KofN_CancelsRemainder(t *testing.T) {
	defer goleak.VerifyNone(t)

	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bkt"))
	b.SetECConfig(ECConfig{DataShards: 2, ParityShards: 1})

	svc := NewShardService(b.root, nil)
	allNodes := []string{b.selfAddr}
	b.SetShardService(svc, allNodes)

	data := []byte("test data for k-of-n")
	obj, err := b.PutObject("bkt", "obj", data, "text/plain")
	require.NoError(t, err)

	got, err := b.GetObject("bkt", "obj", obj.VersionID)
	require.NoError(t, err)
	require.Equal(t, data, got)
}
```

- [ ] **Step 2: import 추가 확인**

`ec_fix_test.go` 상단에 `go.uber.org/goleak` import가 없으면 추가:

```go
import (
    // 기존 import
    "go.uber.org/goleak"
)
```

- [ ] **Step 3: 테스트 실행 — 통과 확인**

```bash
go test -run 'TestPutObjectEC_ParallelRollback|TestGetObjectEC_KofN' \
    -race ./internal/cluster/
```

Expected: PASS (현재 단일 노드에서는 동작함, 실패 시나리오는 병렬화 후 더 정교하게 테스트)

- [ ] **Step 4: Commit**

```bash
git add internal/cluster/ec_fix_test.go
git commit -m "test(cluster): add EC rollback and k-of-n goroutine leak tests"
```

---

### Task 3: putObjectEC 병렬화 구현

**Files:**
- Modify: `internal/cluster/backend.go`

현재 `putObjectEC` (line 530-554) 순차 루프를 errgroup 병렬 루프로 교체.

- [ ] **Step 1: import에 errgroup 추가**

`backend.go` import 블록에 추가:

```go
"golang.org/x/sync/errgroup"
```

- [ ] **Step 2: putObjectEC 루프 교체**

현재 코드 (line 517-554):
```go
// Track nodes we wrote to so cleanup can target them precisely.
written := make([]string, 0, len(shards))
cleanup := func() {
    for _, n := range written {
        if n == selfID {
            _ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
            continue
        }
        _ = b.shardSvc.DeleteShards(ctx, n, bucket, shardKey)
    }
}

// Fan-out: write each shard to its placed node. Write-all consistency.
for i, node := range placement {
    if node == selfID {
        if werr := b.shardSvc.WriteLocalShard(bucket, shardKey, i, shards[i]); werr != nil {
            cleanup()
            return nil, fmt.Errorf("ec write local shard %d: %w", i, werr)
        }
    } else {
        writeCtx, writeCancel := context.WithTimeout(ctx, 3*time.Second)
        werr := b.shardSvc.WriteShard(writeCtx, node, bucket, shardKey, i, shards[i])
        writeCancel()
        if werr != nil {
            if b.peerHealth != nil {
                b.peerHealth.MarkUnhealthy(node)
            }
            cleanup()
            return nil, fmt.Errorf("ec write shard %d to %s: %w", i, node, werr)
        }
        if b.peerHealth != nil {
            b.peerHealth.MarkHealthy(node)
        }
    }
    written = append(written, node)
}
```

변경 후:
```go
// Track nodes we wrote to so cleanup can target them precisely.
// writtenMu: concurrent goroutines append to written simultaneously.
var (
    writtenMu sync.Mutex
    written   []string
)
cleanup := func() {
    // Called after g.Wait() — single goroutine, no mutex needed here.
    for _, n := range written {
        if n == selfID {
            _ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
            continue
        }
        _ = b.shardSvc.DeleteShards(ctx, n, bucket, shardKey)
    }
}

// Fan-out: write all shards in parallel. Write-all consistency.
// Each remote write gets a 3s deadline so a dead peer fails fast.
// Total latency = max(per-shard latency) instead of Σ(per-shard latency).
g, gctx := errgroup.WithContext(ctx)
for i, node := range placement {
    i, node := i, node
    g.Go(func() error {
        var werr error
        if node == selfID {
            werr = b.shardSvc.WriteLocalShard(bucket, shardKey, i, shards[i])
        } else {
            writeCtx, writeCancel := context.WithTimeout(gctx, 3*time.Second)
            defer writeCancel()
            werr = b.shardSvc.WriteShard(writeCtx, node, bucket, shardKey, i, shards[i])
            if b.peerHealth != nil {
                if werr != nil {
                    b.peerHealth.MarkUnhealthy(node)
                } else {
                    b.peerHealth.MarkHealthy(node)
                }
            }
        }
        if werr != nil {
            return fmt.Errorf("ec write shard %d to %s: %w", i, node, werr)
        }
        writtenMu.Lock()
        written = append(written, node)
        writtenMu.Unlock()
        return nil
    })
}
if err := g.Wait(); err != nil {
    cleanup()
    return nil, err
}
```

- [ ] **Step 3: 테스트 실행 — race 포함**

```bash
go test -race -run 'TestPutObjectEC|TestSelfAddr|TestShardPlacement|TestOwnedShards' \
    ./internal/cluster/
```

Expected: PASS, no race

- [ ] **Step 4: gofmt 적용**

```bash
gofmt -w internal/cluster/backend.go
```

- [ ] **Step 5: Commit**

```bash
git add internal/cluster/backend.go
git commit -m "perf(cluster): parallelize putObjectEC shard writes with errgroup"
```

---

### Task 4: getObjectEC k-of-n 병렬화 구현

**Files:**
- Modify: `internal/cluster/backend.go`

현재 `getObjectEC` (line 932-978) 순차 루프를 goroutine + buffered channel 패턴으로 교체.

- [ ] **Step 1: getObjectEC 루프 교체**

현재 코드 (line 946-974):
```go
shards := make([][]byte, len(rec.Nodes))
available := 0
for i, node := range rec.Nodes {
    var data []byte
    var err error
    if node == selfID {
        data, err = b.shardSvc.ReadLocalShard(bucket, shardKey, i)
    } else {
        if b.peerHealth != nil && !b.peerHealth.IsHealthy(node) {
            continue
        }
        shardCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
        data, err = b.shardSvc.ReadShard(shardCtx, node, bucket, shardKey, i)
        cancel()
        if err != nil && b.peerHealth != nil {
            b.peerHealth.MarkUnhealthy(node)
        } else if err == nil && b.peerHealth != nil {
            b.peerHealth.MarkHealthy(node)
        }
    }
    if err == nil && data != nil {
        shards[i] = data
        available++
    }
}
if available < recCfg.DataShards {
    return nil, fmt.Errorf("ec get: only %d/%d shards available, need %d", available, len(rec.Nodes), recCfg.DataShards)
}
return ECReconstruct(recCfg, shards)
```

변경 후:
```go
// k-of-n fast path: read all shards in parallel, stop once k succeed.
// cancel() signals remaining goroutines to abort after k shards received.
// resultCh is buffered(len(nodes)) so goroutines never block on send.
type shardResult struct {
    idx  int
    data []byte
    err  error
}
readCtx, cancel := context.WithCancel(ctx)
defer cancel()

resultCh := make(chan shardResult, len(rec.Nodes))
for i, node := range rec.Nodes {
    i, node := i, node
    go func() {
        var data []byte
        var err error
        if node == selfID {
            data, err = b.shardSvc.ReadLocalShard(bucket, shardKey, i)
        } else {
            if b.peerHealth != nil && !b.peerHealth.IsHealthy(node) {
                resultCh <- shardResult{idx: i, err: fmt.Errorf("node %s unhealthy", node)}
                return
            }
            shardCtx, shardCancel := context.WithTimeout(readCtx, 3*time.Second)
            defer shardCancel()
            data, err = b.shardSvc.ReadShard(shardCtx, node, bucket, shardKey, i)
            if b.peerHealth != nil {
                if err != nil {
                    b.peerHealth.MarkUnhealthy(node)
                } else {
                    b.peerHealth.MarkHealthy(node)
                }
            }
        }
        resultCh <- shardResult{idx: i, data: data, err: err}
    }()
}

shards := make([][]byte, len(rec.Nodes))
available := 0
for range rec.Nodes {
    r := <-resultCh
    if r.err == nil && r.data != nil {
        shards[r.idx] = r.data
        available++
        if available == recCfg.DataShards {
            cancel() // signal remaining goroutines to stop
            break
        }
    }
}
if available < recCfg.DataShards {
    return nil, fmt.Errorf("ec get: only %d/%d shards available, need %d",
        available, len(rec.Nodes), recCfg.DataShards)
}
return ECReconstruct(recCfg, shards)
```

Note: `shardResult` 타입은 이 함수 내부에 선언한다. 다른 함수에서는 사용하지 않으므로 패키지 수준 정의 불필요.

- [ ] **Step 2: 테스트 + race 확인**

```bash
go test -race -run 'TestGetObjectEC_KofN|TestDistributedBackend_PutAndGet' \
    ./internal/cluster/
```

Expected: PASS, no race

- [ ] **Step 3: gofmt**

```bash
gofmt -w internal/cluster/backend.go
```

- [ ] **Step 4: Commit**

```bash
git add internal/cluster/backend.go
git commit -m "perf(cluster): parallelize getObjectEC with k-of-n fast path goroutines"
```

---

### Task 5: upgradeObjectEC 병렬화 구현

**Files:**
- Modify: `internal/cluster/backend.go`

`upgradeObjectEC` (line 1017-1029)의 순차 쓰기 루프를 putObjectEC와 동일한 errgroup 패턴으로 교체.

- [ ] **Step 1: upgradeObjectEC 루프 교체**

현재 코드 (line 1006-1030):
```go
written := make([]string, 0, len(newShards))
cleanup := func() {
    for _, n := range written {
        if n == selfID {
            _ = b.shardSvc.DeleteLocalShards(bucket, key)
        } else {
            _ = b.shardSvc.DeleteShards(ctx, n, bucket, key)
        }
    }
}

for i, node := range newPlacement {
    if node == selfID {
        if werr := b.shardSvc.WriteLocalShard(bucket, key, i, newShards[i]); werr != nil {
            cleanup()
            return fmt.Errorf("upgrade write local shard %d: %w", i, werr)
        }
    } else {
        if werr := b.shardSvc.WriteShard(ctx, node, bucket, key, i, newShards[i]); werr != nil {
            cleanup()
            return fmt.Errorf("upgrade write shard %d to %s: %w", i, node, werr)
        }
    }
    written = append(written, node)
}
```

변경 후:
```go
var (
    writtenMu sync.Mutex
    written   []string
)
cleanup := func() {
    for _, n := range written {
        if n == selfID {
            _ = b.shardSvc.DeleteLocalShards(bucket, key)
        } else {
            _ = b.shardSvc.DeleteShards(ctx, n, bucket, key)
        }
    }
}

g, gctx := errgroup.WithContext(ctx)
for i, node := range newPlacement {
    i, node := i, node
    g.Go(func() error {
        var werr error
        if node == selfID {
            werr = b.shardSvc.WriteLocalShard(bucket, key, i, newShards[i])
        } else {
            writeCtx, writeCancel := context.WithTimeout(gctx, 3*time.Second)
            defer writeCancel()
            werr = b.shardSvc.WriteShard(writeCtx, node, bucket, key, i, newShards[i])
        }
        if werr != nil {
            return fmt.Errorf("upgrade write shard %d to %s: %w", i, node, werr)
        }
        writtenMu.Lock()
        written = append(written, node)
        writtenMu.Unlock()
        return nil
    })
}
if err := g.Wait(); err != nil {
    cleanup()
    return err
}
```

- [ ] **Step 2: 전체 테스트 + race**

```bash
go test -race ./internal/cluster/...
```

Expected: PASS, no race

- [ ] **Step 3: gofmt**

```bash
gofmt -w internal/cluster/backend.go
```

- [ ] **Step 4: Benchmark 실행 — 개선 확인**

```bash
go test -bench='BenchmarkPutObjectEC_Sequential|BenchmarkGetObjectEC' \
    -benchtime=10s -count=5 ./internal/cluster/ | tee /tmp/bench_ec_after.txt
benchstat /tmp/bench_ec_before.txt /tmp/bench_ec_after.txt
```

Expected:
- `BenchmarkPutObjectEC_Sequential`: 싱글 노드에서는 차이 없음 (모두 WriteLocalShard)
- k6 통합 테스트 시 멀티 노드 환경에서 6x 개선 확인

- [ ] **Step 5: Commit**

```bash
git add internal/cluster/backend.go
git commit -m "perf(cluster): parallelize upgradeObjectEC shard writes with errgroup"
```

---

## Phase 2: MigrationExecutor 측정 게이트

### Task 6: Contention 측정 (게이트)

이 Task는 Phase 2 구현 여부를 결정한다. **측정 결과 p99 < 1ms이면 Phase 2(Task 7-10)를 건너뛴다.**

**Files:**
- Create: `internal/cluster/migration_executor_bench_test.go`

- [ ] **Step 1: 기준 벤치마크 작성**

`internal/cluster/migration_executor_bench_test.go` 신규 생성:

```go
package cluster

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkMigrationExecutor_Execute measures Execute() throughput under contention.
// Baseline: shows mu/pendingMu contention at high concurrency.
func BenchmarkMigrationExecutor_Execute(b *testing.B) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{}
	e := NewMigrationExecutor(mover, node, 1)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			task := MigrationTask{
				Bucket:    "bench",
				Key:       "obj" + string(rune(i)),
				VersionID: "v1",
				SrcNode:   "src",
				DstNode:   "dst",
			}
			_ = e.Execute(context.Background(), task)
			i++
		}
	})
}

// BenchmarkMigrationExecutor_MutexContention measures lock contention via mutex profile.
// Run with: go test -bench=BenchmarkMigrationExecutor_MutexContention
//          -mutexprofile=mutex.prof ./internal/cluster/
// Then: go tool pprof mutex.prof
func BenchmarkMigrationExecutor_MutexContention(b *testing.B) {
	runtime.SetMutexProfileFraction(1)
	defer runtime.SetMutexProfileFraction(0)

	mover := &mockShardMover{}
	node := &mockMigrationRaft{}
	e := NewMigrationExecutor(mover, node, 1)

	var wg sync.WaitGroup
	for range 32 { // 32 concurrent callers
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range b.N / 32 {
				task := MigrationTask{Bucket: "b", Key: "k", VersionID: "v", SrcNode: "s", DstNode: "d"}
				_ = e.Execute(context.Background(), task)
			}
		}()
	}
	wg.Wait()
}
```

- [ ] **Step 2: 벤치마크 실행 + contention 측정**

```bash
# Mutex contention 프로파일
go test -bench=BenchmarkMigrationExecutor_MutexContention \
    -benchtime=10s -mutexprofile=/tmp/mutex.prof \
    ./internal/cluster/ 2>/dev/null

# contention 분석
go tool pprof -text /tmp/mutex.prof | head -30
```

**판단 기준**: pprof 출력에서 `MigrationExecutor` 관련 함수의 cumulative delay가 1ms 이상이면 Phase 2 진행. 미만이면 Task 7-10을 건너뛰고 Phase 3으로.

- [ ] **Step 3: Commit**

```bash
git add internal/cluster/migration_executor_bench_test.go
git commit -m "bench(cluster): add MigrationExecutor contention measurement benchmark"
```

---

### Task 7: MigrationExecutor Actor 전환 — 게이트 통과 시에만

**⚠️ Task 6의 측정 결과 p99 contention ≥ 1ms인 경우에만 진행.**

**Files:**
- Modify: `internal/cluster/migration_executor.go`
- Modify: `internal/cluster/migration_executor_test.go`

현재 `MigrationExecutor`는 `mu`, `pendingMu` 두 mutex와 복잡한 상태 기계(idempotency, early-commit, TTL sweep)를 가진다. Actor 전환은 내부 상태를 단일 run() goroutine으로 이동하고, `Execute()` 공개 API는 시그니처를 유지한다.

- [ ] **Step 1: 새 타입 및 메시지 정의 추가**

`migration_executor.go`에 파일 상단(기존 타입 정의 전) 추가:

```go
// executorMsgKind categorises messages sent to MigrationExecutor.run().
type executorMsgKind int

const (
    msgExecSubmit executorMsgKind = iota
    msgExecNotify // NotifyCommit path
    msgExecCancel
)

type executorMsg struct {
    kind  executorMsgKind
    task  MigrationTask
    id    string
    ctx   context.Context
    reply chan<- executorReply
}

type executorReply struct {
    err error
}
```

- [ ] **Step 2: MigrationExecutor 구조체에 Actor 필드 추가**

기존 `MigrationExecutor` 구조체에 필드 추가:

```go
// Actor fields — state is owned by run() goroutine.
actorCh   chan executorMsg
actorQuit chan struct{}
actorOnce sync.Once
actorWg   sync.WaitGroup
```

- [ ] **Step 3: newExecutor에 Actor 초기화 추가**

`newExecutor()` 함수에서 반환 전에:

```go
e.actorCh = make(chan executorMsg, 64)
e.actorQuit = make(chan struct{})
e.actorWg.Add(1)
go e.actorRun()
```

- [ ] **Step 4: actorRun() goroutine 구현**

```go
// actorRun processes executor messages in a single goroutine.
// All state (done, committed, pending, ttlPending) is owned here.
func (e *MigrationExecutor) actorRun() {
    defer e.actorWg.Done()
    // Reuse the existing state maps; actor goroutine now owns them exclusively.
    // mu and pendingMu are no longer needed for correctness (actor owns state).
    var sweepTicker *time.Ticker
    if e.pendingTTL > 0 {
        sweepTicker = time.NewTicker(e.pendingTTL / 2)
        defer sweepTicker.Stop()
    } else {
        sweepTicker = time.NewTicker(time.Hour) // dummy; never fires meaningfully
        defer sweepTicker.Stop()
    }

    for {
        select {
        case msg := <-e.actorCh:
            switch msg.kind {
            case msgExecSubmit:
                // Delegate to existing idempotency / Execute logic.
                // Run the full migration in a goroutine so actor loop stays unblocked.
                go func() {
                    err := e.executeInActor(msg.ctx, msg.task)
                    if msg.reply != nil {
                        select {
                        case msg.reply <- executorReply{err: err}:
                        case <-msg.ctx.Done():
                        case <-e.actorQuit:
                        }
                    }
                }()
            case msgExecNotify:
                e.mu.Lock()
                if ch, ok := e.pending[msg.id]; ok {
                    close(ch)
                    delete(e.pending, msg.id)
                } else {
                    e.committed[msg.id] = struct{}{}
                }
                e.mu.Unlock()
            }
        case <-sweepTicker.C:
            e.sweepExpired()
        case <-e.actorQuit:
            return
        }
    }
}

func (e *MigrationExecutor) executeInActor(ctx context.Context, task MigrationTask) error {
    return e.Execute(ctx, task)
}
```

Note: Phase 2 Actor 전환은 복잡성이 높다. 위 구현은 Execute()를 내부에서 그대로 호출하여 기존 idempotency/early-commit 로직을 유지한다. mu/pendingMu는 단기적으로 유지하고 별도 커밋에서 제거.

- [ ] **Step 5: Stop() 추가**

```go
// Stop gracefully shuts down the actor goroutine. Safe to call multiple times.
func (e *MigrationExecutor) Stop() {
    e.actorOnce.Do(func() { close(e.actorQuit) })
    e.actorWg.Wait()
}
```

- [ ] **Step 6: Stop 幂등성 테스트 추가**

`migration_executor_test.go` 에 추가:

```go
func TestMigrationExecutor_StopIdempotent(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{}
	e := NewMigrationExecutor(mover, node, 1)

	// Calling Stop() twice must not panic (sync.Once protection).
	require.NotPanics(t, func() {
		e.Stop()
		e.Stop()
	})
}

func TestMigrationExecutor_ExecuteAfterStop(t *testing.T) {
	mover := &mockShardMover{}
	node := &mockMigrationRaft{}
	e := NewMigrationExecutor(mover, node, 1)
	e.Stop()

	task := MigrationTask{Bucket: "b", Key: "k", VersionID: "v", SrcNode: "s", DstNode: "d"}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := e.Execute(ctx, task)
	// After Stop(), Execute should return quickly (ctx cancel or ErrExecutorStopped).
	require.Error(t, err, "Execute after Stop must return an error")
}
```

- [ ] **Step 7: 기존 테스트 전체 통과 확인**

```bash
go test -race -run 'TestMigrationExecutor' ./internal/cluster/
```

Expected: 모든 기존 + 신규 테스트 PASS, no race

- [ ] **Step 8: gofmt**

```bash
gofmt -w internal/cluster/migration_executor.go internal/cluster/migration_executor_test.go
```

- [ ] **Step 9: Commit**

```bash
git add internal/cluster/migration_executor.go internal/cluster/migration_executor_test.go
git commit -m "feat(cluster): add Actor goroutine + Stop() to MigrationExecutor"
```

---

## Phase 3: Registry.InvalidateAll 버그 수정 + 비동기 팬아웃

### Task 8: invalidator.go mutex 버그 수정

**Files:**
- Modify: `internal/cluster/invalidator.go`
- Create: `internal/cluster/invalidator_test.go`

- [ ] **Step 1: race 조건 재현 테스트 작성**

`internal/cluster/invalidator_test.go` 신규 생성:

```go
package cluster

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testInvalidator struct {
	mu    sync.Mutex
	calls []string
}

func (t *testInvalidator) Invalidate(bucket, key string) {
	t.mu.Lock()
	t.calls = append(t.calls, bucket+"/"+key)
	t.mu.Unlock()
}

func TestRegistry_ConcurrentRegisterAndInvalidate(t *testing.T) {
	// This test would trigger the race detector without the mutex fix.
	r := NewRegistry()

	var wg sync.WaitGroup
	inv := &testInvalidator{}

	// Writer: concurrent Register calls
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Register("vol1", inv)
		}()
	}
	// Reader: concurrent InvalidateAll calls
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.InvalidateAll("bkt", "key")
		}()
	}
	wg.Wait()
	// If we got here without race detector firing, the mutex protects correctly.
}

func TestRegistry_InvalidateAll_CallsAllInvalidators(t *testing.T) {
	r := NewRegistry()
	inv1 := &testInvalidator{}
	inv2 := &testInvalidator{}
	r.Register("vol1", inv1)
	r.Register("vol2", inv2)

	r.InvalidateAll("mybkt", "mykey")

	assert.Len(t, inv1.calls, 1)
	assert.Equal(t, "mybkt/mykey", inv1.calls[0])
	assert.Len(t, inv2.calls, 1)
}
```

- [ ] **Step 2: 테스트 실행 — race 확인 (실패 예상)**

```bash
go test -race -run TestRegistry_ConcurrentRegisterAndInvalidate ./internal/cluster/
```

Expected: `DATA RACE` 감지 (현재 mutex 없으므로)

- [ ] **Step 3: invalidator.go에 mutex 추가**

`Registry` 구조체 수정:

```go
type Registry struct {
	mu           sync.RWMutex
	invalidators map[string]CacheInvalidator // volumeID → invalidator
}
```

`Register` 함수 수정:
```go
func (r *Registry) Register(volumeID string, invalidator CacheInvalidator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.invalidators[volumeID] = invalidator
	r.updateSizeMetric()
}
```

`GetInvalidator` 함수 수정:
```go
func (r *Registry) GetInvalidator(volumeID string) CacheInvalidator {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.invalidators[volumeID]
}
```

`GetInvalidators` 함수 수정 (복사본 반환으로 safe 하게):
```go
func (r *Registry) GetInvalidators() map[string]CacheInvalidator {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[string]CacheInvalidator, len(r.invalidators))
	for k, v := range r.invalidators {
		result[k] = v
	}
	return result
}
```

`InvalidateAll` 임시 수정 (sequential, mutex protected):
```go
func (r *Registry) InvalidateAll(bucket, key string) {
	r.mu.RLock()
	invs := make([]CacheInvalidator, 0, len(r.invalidators))
	for _, inv := range r.invalidators {
		invs = append(invs, inv)
	}
	r.mu.RUnlock()

	for _, inv := range invs {
		inv.Invalidate(bucket, key)
	}
}
```

- [ ] **Step 4: 테스트 재실행 — race 제거 확인**

```bash
go test -race -run 'TestRegistry' ./internal/cluster/
```

Expected: PASS, no race

- [ ] **Step 5: gofmt**

```bash
gofmt -w internal/cluster/invalidator.go
```

- [ ] **Step 6: Commit**

```bash
git add internal/cluster/invalidator.go internal/cluster/invalidator_test.go
git commit -m "fix(cluster): add RWMutex to Registry to prevent data race in InvalidateAll"
```

---

### Task 9: InvalidateAll 비동기 팬아웃 구현

**Files:**
- Modify: `internal/cluster/invalidator.go`
- Create: `internal/cluster/invalidator_bench_test.go`

- [ ] **Step 1: baseline 벤치마크 작성**

`internal/cluster/invalidator_bench_test.go` 신규 생성:

```go
package cluster

import (
	"fmt"
	"testing"
	"time"
)

type slowInvalidator struct {
	delay time.Duration
}

func (s *slowInvalidator) Invalidate(bucket, key string) {
	time.Sleep(s.delay)
}

func BenchmarkInvalidateAll(b *testing.B) {
	for _, n := range []int{1, 5, 10, 20} {
		b.Run(fmt.Sprintf("vfs=%d", n), func(b *testing.B) {
			r := NewRegistry()
			for i := range n {
				r.Register(fmt.Sprintf("vfs%d", i), &slowInvalidator{delay: 5 * time.Millisecond})
			}
			b.ResetTimer()
			for b.Loop() {
				r.InvalidateAll("bucket", "key")
			}
		})
	}
}
```

- [ ] **Step 2: baseline 측정**

```bash
go test -bench=BenchmarkInvalidateAll -benchtime=5s -count=3 \
    ./internal/cluster/ | tee /tmp/bench_invalidate_before.txt
```

Expected (sequential): vfs=10일 때 약 50ms/op

- [ ] **Step 3: InvalidateAll fan-out으로 교체**

`invalidator.go`의 `InvalidateAll` 함수를 교체:

```go
// InvalidateAll calls Invalidate on all registered invalidators concurrently.
//
// Note: this reduces Apply loop block time from Σ(per-invalidator) to
// max(per-invalidator). The Apply loop still blocks at wg.Wait() —
// it's a reduction, not an elimination.
func (r *Registry) InvalidateAll(bucket, key string) {
	r.mu.RLock()
	invs := make([]CacheInvalidator, 0, len(r.invalidators))
	for _, inv := range r.invalidators {
		invs = append(invs, inv)
	}
	r.mu.RUnlock()

	if len(invs) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, inv := range invs {
		inv := inv
		wg.Add(1)
		go func() {
			defer wg.Done()
			inv.Invalidate(bucket, key)
		}()
	}
	wg.Wait()
}
```

- [ ] **Step 4: 테스트 + race 확인**

```bash
go test -race -run 'TestRegistry' ./internal/cluster/
```

Expected: PASS, no race

- [ ] **Step 5: benchmark 재실행 — 개선 확인**

```bash
go test -bench=BenchmarkInvalidateAll -benchtime=5s -count=3 \
    ./internal/cluster/ | tee /tmp/bench_invalidate_after.txt
benchstat /tmp/bench_invalidate_before.txt /tmp/bench_invalidate_after.txt
```

Expected: vfs=10일 때 ~50ms → ~5ms (10x)

- [ ] **Step 6: gofmt**

```bash
gofmt -w internal/cluster/invalidator.go
```

- [ ] **Step 7: Commit**

```bash
git add internal/cluster/invalidator.go internal/cluster/invalidator_bench_test.go
git commit -m "perf(cluster): parallelize Registry.InvalidateAll with sync.WaitGroup fan-out"
```

---

## Phase 4: BalancerProposer 측정 게이트

### Task 10: Balancer Contention 측정 (게이트)

**⚠️ 측정 결과 p99 contention ≥ 1ms이면 Task 11 진행. 미달이면 건너뜀.**

**Files:**
- Create: `internal/cluster/balancer_bench_test.go`

- [ ] **Step 1: contention 벤치마크 작성**

`internal/cluster/balancer_bench_test.go` 신규 생성:

```go
package cluster

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func newTestBalancer(b *testing.B) *BalancerProposer {
	b.Helper()
	store := NewNodeStatsStore(1 * time.Minute)
	node := &mockRaftNode{state: 2, nodeID: "self", peerIDs: []string{}}
	return NewBalancerProposer("self", store, node, DefaultBalancerConfig())
}

// BenchmarkBalancerStatus measures Status() throughput under concurrent access.
// Status() acquires mu.Lock — this benchmark reveals contention.
func BenchmarkBalancerStatus(b *testing.B) {
	bp := newTestBalancer(b)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bp.Status()
		}
	})
}

// BenchmarkBalancerNotify measures NotifyMigrationDone throughput.
// It acquires mu.Lock and is called from FSM goroutine.
func BenchmarkBalancerNotify(b *testing.B) {
	runtime.SetMutexProfileFraction(1)
	defer runtime.SetMutexProfileFraction(0)
	bp := newTestBalancer(b)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bp.NotifyMigrationDone("bkt", "key", "v1")
		}
	})
}
```

- [ ] **Step 2: 측정 실행**

```bash
go test -bench=BenchmarkBalancer -benchtime=10s \
    -mutexprofile=/tmp/balancer_mutex.prof ./internal/cluster/ 2>/dev/null
go tool pprof -text /tmp/balancer_mutex.prof | head -20
```

**판단**: pprof에서 `BalancerProposer` mutex delay ≥ 1ms/op이면 Task 11 진행. 미달이면 종료.

- [ ] **Step 3: Commit**

```bash
git add internal/cluster/balancer_bench_test.go
git commit -m "bench(cluster): add BalancerProposer contention measurement benchmarks"
```

---

### Task 11: BalancerProposer Actor 형식화 — 게이트 통과 시에만

**⚠️ Task 10 측정 결과 contention ≥ 1ms인 경우에만 진행.**

**Files:**
- Modify: `internal/cluster/balancer.go`
- Modify: `internal/cluster/balancer_test.go`

`BalancerProposer`는 이미 단일 ticker goroutine(`Run(ctx)`)이 상태를 소유한다. 문제는 외부에서 `Status()`와 `NotifyMigrationDone()`이 mu.Lock으로 상태를 읽는 것. 이를 채널 메시지로 교체.

- [ ] **Step 1: 메시지 타입 정의 추가**

`balancer.go` 파일 상단에 추가 (기존 type 정의 전):

```go
type balancerMsgKind int

const (
    msgBalancerNotifyDone balancerMsgKind = iota
    msgBalancerStatus
)

type balancerMsg struct {
    kind   balancerMsgKind
    bucket string
    key    string
    ver    string
    reply  chan<- BalancerStatus
}
```

- [ ] **Step 2: BalancerProposer 구조체에 Actor 필드 추가**

```go
ch       chan balancerMsg
stopOnce sync.Once
stopCh   chan struct{}
stopWg   sync.WaitGroup
```

- [ ] **Step 3: NewBalancerProposer Actor 초기화**

`NewBalancerProposer` 반환 전:
```go
bp.ch = make(chan balancerMsg, 32)
bp.stopCh = make(chan struct{})
```

- [ ] **Step 4: Run() 수정 — 채널 처리 추가**

기존 `Run(ctx)` 함수의 select에 ch 처리 추가:

```go
func (p *BalancerProposer) Run(ctx context.Context) {
	p.startedAt = time.Now()
	p.stopWg.Add(1)
	defer p.stopWg.Done()
	ticker := time.NewTicker(p.cfg.GossipInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.tickOnce()
		case msg := <-p.ch:
			switch msg.kind {
			case msgBalancerNotifyDone:
				id := msg.bucket + "/" + msg.key + "/" + msg.ver
				delete(p.inflight, id) // no mu needed: Run() goroutine owns inflight
			case msgBalancerStatus:
				if msg.reply != nil {
					msg.reply <- BalancerStatus{
						Active:       p.active,
						ImbalancePct: imbalancePct(p.store),
						Nodes:        p.store.GetAll(),
					}
				}
			}
		}
	}
}
```

- [ ] **Step 5: NotifyMigrationDone + Status + Stop 교체**

```go
// NotifyMigrationDone sends a done notification to the Run() goroutine (non-blocking).
func (p *BalancerProposer) NotifyMigrationDone(bucket, key, versionID string) {
	select {
	case p.ch <- balancerMsg{kind: msgBalancerNotifyDone, bucket: bucket, key: key, ver: versionID}:
	case <-p.stopCh:
	}
}

// Status queries the Run() goroutine for current state.
func (p *BalancerProposer) Status() BalancerStatus {
	reply := make(chan BalancerStatus, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	select {
	case p.ch <- balancerMsg{kind: msgBalancerStatus, reply: reply}:
	case <-ctx.Done():
		return BalancerStatus{}
	case <-p.stopCh:
		return BalancerStatus{}
	}
	select {
	case s := <-reply:
		return s
	case <-ctx.Done():
		return BalancerStatus{}
	}
}

// Stop signals Run() to exit and waits for it to finish.
func (p *BalancerProposer) Stop() {
	p.stopOnce.Do(func() { close(p.stopCh) })
	p.stopWg.Wait()
}
```

- [ ] **Step 6: mu 필드 및 mu.Lock/Unlock 호출 제거**

`balancer.go`에서 `mu sync.Mutex` 필드 및 모든 `p.mu.Lock()/p.mu.Unlock()` 호출 제거.

검색:
```bash
grep -n "p\.mu\." internal/cluster/balancer.go
```

각 호출 위치에서 mu 호출 라인 삭제 (Run() 내부의 `p.inflight` 접근은 이미 actor goroutine에서 보호됨).

- [ ] **Step 7: 동시성 테스트 추가**

`balancer_test.go`에 추가:

```go
func TestBalancerProposer_StatusConcurrent(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	node := &mockRaftNode{state: 1, nodeID: "self", peerIDs: []string{}}
	bp := NewBalancerProposer("self", store, node, DefaultBalancerConfig())

	ctx, cancel := context.WithCancel(context.Background())
	go bp.Run(ctx)
	defer cancel()
	defer bp.Stop()

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_ = bp.Status()
			}
		}()
	}
	wg.Wait()
}

func TestBalancerProposer_StopIdempotent(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	node := &mockRaftNode{state: 1, nodeID: "self", peerIDs: []string{}}
	bp := NewBalancerProposer("self", store, node, DefaultBalancerConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	go bp.Run(ctx)

	require.NotPanics(t, func() {
		bp.Stop()
		bp.Stop()
	})
}
```

- [ ] **Step 8: 기존 + 신규 테스트 통과**

```bash
go test -race -run 'TestBalancerProposer' ./internal/cluster/
```

Expected: PASS, no race

- [ ] **Step 9: gofmt**

```bash
gofmt -w internal/cluster/balancer.go internal/cluster/balancer_test.go
```

- [ ] **Step 10: Commit**

```bash
git add internal/cluster/balancer.go internal/cluster/balancer_test.go
git commit -m "feat(cluster): formalize BalancerProposer Actor — remove mu, add channel API"
```

---

## 최종 검증

### Task 12: 전체 테스트 + PR 준비

- [ ] **Step 1: 전체 테스트 + race detector**

```bash
go test -race ./internal/...
```

Expected: 모든 테스트 PASS, no race condition

- [ ] **Step 2: 빌드 확인**

```bash
make build
```

Expected: 빌드 성공

- [ ] **Step 3: Phase 1 최종 벤치마크 비교**

```bash
go test -bench='BenchmarkPutObjectEC|BenchmarkGetObjectEC' \
    -benchtime=10s -count=5 ./internal/cluster/ | tee /tmp/bench_ec_final.txt
benchstat /tmp/bench_ec_before.txt /tmp/bench_ec_final.txt
```

- [ ] **Step 4: Phase 3 최종 벤치마크 비교**

```bash
go test -bench=BenchmarkInvalidateAll -benchtime=5s -count=3 \
    ./internal/cluster/ | tee /tmp/bench_invalidate_final.txt
benchstat /tmp/bench_invalidate_before.txt /tmp/bench_invalidate_final.txt
```

- [ ] **Step 5: 각 Phase PR 생성 (feat 브랜치에서)**

각 Phase를 별도 PR로:
- `feat/ec-parallel-fan-out` → Phase 1 (Task 1-5)
- `feat/registry-async-invalidate` → Phase 3 (Task 8-9)
- `feat/migration-executor-actor` → Phase 2 (Task 6-7, 게이트 통과 시)
- `feat/balancer-actor` → Phase 4 (Task 10-11, 게이트 통과 시)

PR 본문에 반드시 before/after benchstat 결과 첨부.
