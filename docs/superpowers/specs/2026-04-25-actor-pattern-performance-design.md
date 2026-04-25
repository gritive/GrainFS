# Actor 패턴 성능 개선 설계

**날짜**: 2026-04-25  
**목표**: GrainFS 핵심 경로에서 mutex 기반 동시성을 Actor 패턴으로 전환하여 throughput 및 latency 개선

---

## 배경

GrainFS 코드베이스 분석 결과, 다음 병목이 확인됨:

1. `raft/Node.mu` — 70+ Lock 지점이 단일 mutex 직렬화. 디스크 I/O를 lock hold 중 실행.
2. `cluster/putObjectEC` — EC shard write/read가 순차 실행 (k+m개 peer 직렬).
3. `cluster/MigrationExecutor` — Mutex 2개 + lock-order 주석 = 유지보수 부채.
4. `cluster/Registry.InvalidateAll` — Raft Apply loop 내부에서 모든 VFS를 순차 호출.
5. `cluster/BalancerProposer` — 단일 ticker goroutine이 mutex 주석으로 invariant 관리.

기존 프로젝트에서 이미 Actor 패턴을 올바르게 사용하는 곳: `storage/wal/WAL`, `cluster/FSM.Apply` loop.  
이 패턴을 위 병목 지점으로 확산한다.

---

## 스코프

이 문서는 **Phase 1~4**를 다룬다. Phase 5 (Raft Full Actor)는 규모와 위험도가 크므로 별도 스펙 문서를 작성한다.

| Phase | 위치 | 유형 | 예상 기간 |
|-------|------|------|----------|
| 1 | EC 팬아웃 병렬화 | goroutine fan-out | 2일 |
| 2 | MigrationExecutor Actor 전환 | Actor | 3일 |
| 3 | Registry.InvalidateAll 비동기화 | fan-out + 버그 수정 | 1일 |
| 4 | BalancerProposer Actor 형식화 | Actor | 1일 |
| 5 | **Raft Node Full Actor** | **별도 스펙** | 2주 |

---

## Actor 패턴 정의 (이 프로젝트 맥락)

"단일 goroutine이 상태를 독점 소유하고, 외부는 채널 메시지로만 상호작용"

```go
type MyActor struct {
    ch   chan myMsg
    done chan struct{}
}

func (a *MyActor) run() {
    state := newState()  // 이 goroutine만 접근 — mutex 불필요
    for {
        select {
        case msg := <-a.ch:
            switch msg.kind {
            case msgGet:
                // reply chan이 nil이면 sender가 이미 떠난 것 — skip
                if msg.reply != nil {
                    select {
                    case msg.reply <- state.get(msg.key):
                    case <-msg.ctx.Done():
                    }
                }
            case msgPut:
                state.put(msg.key, msg.val)
            }
        case <-a.done:
            return
        }
    }
}
```

**공통 원칙**:
- 요청-응답: `reply chan<- Result` 패턴. caller context 취소 시 reply chan 전송을 `select + ctx.Done()` 으로 보호.
- Fire-and-forget: 버퍼드 채널. 가득 찼을 때는 error 반환 (drop 불허).
- Graceful shutdown: `close(done)` → run() 종료 → reply 없이 반환. in-flight msg의 reply chan 처리는 sender가 ctx timeout으로 처리.
- `Stop()` 반환 전에 run() goroutine이 종료됨을 보장 (sync.WaitGroup 또는 done chan close 확인).
- 읽기 전용 hot path: `atomic.Uint64` / `atomic.Pointer[T]`(Go 1.19+, 이 프로젝트 Go 1.26+이므로 가용)로 채널 왕복 없이 즉시 반환. atomic pointer 저장 시 항상 새 값의 복사본 저장 — actor goroutine 로컬 변수 주소 저장 금지.

---

## Phase 1: EC 병렬 팬아웃

**파일**: `internal/cluster/backend.go`  
**함수**: `putObjectEC`, `getObjectEC`, `upgradeObjectToEC`

### 현재 (순차)

```go
for i, node := range placement {
    err = shardSvc.WriteShard(ctx, node, shardID, data[i])
    if err != nil { return err }
}
// 총 latency = Σ(per-shard latency) ≈ 60ms (6 shard × 10ms)
```

### 변경 후 (병렬 write)

```go
g, gctx := errgroup.WithContext(ctx)
for i, node := range placement {
    i, node := i, node
    g.Go(func() error {
        return shardSvc.WriteShard(gctx, node, shardID, data[i])
    })
}
if err := g.Wait(); err != nil {
    return err
}
// 총 latency = max(per-shard latency) ≈ 10ms (최대 6x 개선)
```

### getObjectEC — k-of-n fast path

목표: k개 shard를 성공적으로 읽으면 나머지를 취소해 자원 절약.

구현: errgroup 대신 직접 goroutine + 결과 채널 패턴 사용.

```go
type shardResult struct {
    idx  int
    data []byte
    err  error
}

resultCh := make(chan shardResult, len(placement))
ctx, cancel := context.WithCancel(ctx)
defer cancel()

for i, node := range placement {
    i, node := i, node
    go func() {
        data, err := shardSvc.ReadShard(ctx, node, shardID)
        resultCh <- shardResult{idx: i, data: data, err: err}
    }()
}

shards := make([][]byte, len(placement))
success := 0
for range placement {
    r := <-resultCh
    if r.err == nil {
        shards[r.idx] = r.data
        success++
        if success == k {
            cancel()  // 나머지 goroutine 취소
            break
        }
    }
}
if success < k {
    return nil, fmt.Errorf("insufficient shards: got %d, need %d", success, k)
}
// reed-solomon reconstruct with shards...
```

**의미**: `cancel()` 후 in-flight RPC가 즉시 중단됨을 보장하지는 않지만 컨텍스트를 통해 조기 종료 유도. 남은 goroutine은 `resultCh`가 버퍼드(len=n)이므로 블록 없이 종료.

### upgradeObjectToEC

`putObjectEC`와 동일 패턴 적용.

### 검증

- 기존 EC 통합 테스트 전체 통과
- `go test -race ./internal/cluster/...` 클린
- `go test -bench=BenchmarkPutObjectEC` 전후 latency 비교

---

## Phase 2: MigrationExecutor Actor 전환

**파일**: `internal/cluster/migration_executor.go`

### 현재 문제

```go
type MigrationExecutor struct {
    mu        sync.Mutex  // Lock order: mu → pendingMu
    pendingMu sync.Mutex
    pending   map[string]chan struct{}
    running   map[string]MigrationTask
    done      map[string]MigrationResult
}
```

lock-order 주석이 필요한 시점 = Actor 패턴 적용 신호.

### Actor 구조

```go
type executorMsgKind int

const (
    msgExecSubmit executorMsgKind = iota
    msgExecCancel
    msgExecStatus
    msgExecTick
)

type executorMsg struct {
    kind  executorMsgKind
    task  MigrationTask
    id    string
    ctx   context.Context   // caller context (취소 전파용)
    reply chan<- executorReply
}

type executorReply struct {
    err    error
    status MigrationStatus
}

type MigrationExecutor struct {
    ch   chan executorMsg
    quit chan struct{}
    wg   sync.WaitGroup
}

func NewMigrationExecutor() *MigrationExecutor {
    e := &MigrationExecutor{
        ch:   make(chan executorMsg, 64),  // 64: 과부하 시 back-pressure
        quit: make(chan struct{}),
    }
    e.wg.Add(1)
    go e.run()
    return e
}

func (e *MigrationExecutor) Stop() {
    close(e.quit)
    e.wg.Wait()  // run() goroutine 완전 종료 대기
}

func (e *MigrationExecutor) run() {
    defer e.wg.Done()
    state := newExecutorState()
    ticker := time.NewTicker(sweepInterval)
    defer ticker.Stop()
    for {
        select {
        case msg := <-e.ch:
            switch msg.kind {
            case msgExecSubmit:
                reply := state.submit(msg.task)
                if msg.reply != nil {
                    select {
                    case msg.reply <- reply:
                    case <-msg.ctx.Done():
                        // caller가 이미 취소됨 — drop
                    }
                }
            case msgExecCancel:
                state.cancel(msg.id)
            case msgExecStatus:
                if msg.reply != nil {
                    select {
                    case msg.reply <- state.status(msg.id):
                    case <-msg.ctx.Done():
                    }
                }
            }
        case <-ticker.C:
            state.sweepExpired()
        case <-e.quit:
            // in-flight reply chan은 caller의 context timeout에 맡김
            return
        }
    }
}
```

### 공개 API (시그니처 불변)

```go
// Submit은 task를 admission queue에 즉시 추가 (비동기 시작).
// 반환값은 admission 성공 여부만 나타냄 (완료 여부 아님).
func (e *MigrationExecutor) Submit(ctx context.Context, task MigrationTask) error {
    reply := make(chan executorReply, 1)
    select {
    case e.ch <- executorMsg{kind: msgExecSubmit, task: task, ctx: ctx, reply: reply}:
    case <-ctx.Done():
        return ctx.Err()
    case <-e.quit:
        return ErrExecutorStopped
    }
    select {
    case r := <-reply:
        return r.err
    case <-ctx.Done():
        return ctx.Err()
    }
}

func (e *MigrationExecutor) Cancel(id string) {
    select {
    case e.ch <- executorMsg{kind: msgExecCancel, id: id}:
    case <-e.quit:
    }
}
```

### 검증

- `migration_executor_test.go` 기존 테스트 전체 통과
- `go test -race ./internal/cluster/...` 클린
- Stop() 호출 후 goroutine leak 없음 확인 (`goleak` 또는 `pprof`)

---

## Phase 3: Registry.InvalidateAll 비동기 팬아웃 + 버그 수정

**파일**: `internal/cluster/invalidator.go`

### 기존 버그 (독립 수정)

`invalidators` 맵에 mutex 없음 → `Register()`와 `InvalidateAll()` 간 data race 가능.  
이 버그는 Phase 3 구현과 별도 커밋으로 먼저 수정한다.

```go
type Registry struct {
    mu          sync.RWMutex  // 추가
    invalidators map[string]CacheInvalidator
}

func (r *Registry) Register(name string, inv CacheInvalidator) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.invalidators[name] = inv
}
```

### InvalidateAll 비동기 팬아웃

현재 Raft Apply loop 안에서 순차 실행됨:
```go
for _, inv := range r.invalidators {
    inv.Invalidate(ctx, ...)  // 각 VFS가 완료될 때까지 apply loop 블로킹
}
```

변경 후 — `sync.WaitGroup` 사용 (errgroup의 first-cancel이 다른 invalidator 취소하는 부작용 방지):

```go
func (r *Registry) InvalidateAll(ctx context.Context, key string) {
    r.mu.RLock()
    invs := make([]CacheInvalidator, 0, len(r.invalidators))
    for _, inv := range r.invalidators {
        invs = append(invs, inv)
    }
    r.mu.RUnlock()

    var wg sync.WaitGroup
    for _, inv := range invs {
        inv := inv
        wg.Add(1)
        go func() {
            defer wg.Done()
            inv.Invalidate(ctx, key)  // 에러는 cache invalidation이 non-fatal이므로 무시
        }()
    }
    wg.Wait()
}
```

**참고**: cache invalidation 실패는 stale read 위험이 있으나 Raft apply가 실패해야 하는 사유는 아님. 오류는 각 invalidator가 내부 로그로 기록.

### 검증

- `go test -race ./internal/cluster/...` 클린 (mutex 추가 후 race 제거 확인)
- Apply throughput 테스트: VFS 10개 등록 시 기존 대비 InvalidateAll 시간 감소

---

## Phase 4: BalancerProposer Actor 형식화

**파일**: `internal/cluster/balancer.go`

### 현재 상태

단일 ticker goroutine이 상태를 이미 소유하고 있지만 mutex 주석으로만 표현:

```go
// Note: mu must be held when accessing active/inflight
mu      sync.Mutex
active  int
inflight int
migQueue *MigrationPriorityQueue
```

외부에서 `active`/`inflight` 조회가 필요한 경우 (Prometheus metrics): mutex 취득. 이것이 goroutine safety를 깨는 유일한 경로.

### 형식화 후

```go
type balancerMsgKind int

const (
    msgBalancerTick   balancerMsgKind = iota
    msgBalancerNotify  // migration 완료 시 inflight 감소
    msgBalancerStats   // Prometheus scrape용
)

type balancerMsg struct {
    kind   balancerMsgKind
    taskID string
    reply  chan<- balancerStats
}

type balancerStats struct {
    active   int
    inflight int
}

// run() goroutine만이 active, inflight, migQueue, stickyDonor에 접근
func (b *BalancerProposer) run() {
    // ... active, inflight 등을 local 변수로 소유
}

// Prometheus collector 구현 시 Stats() 호출
func (b *BalancerProposer) Stats(ctx context.Context) (balancerStats, error) {
    reply := make(chan balancerStats, 1)
    select {
    case b.ch <- balancerMsg{kind: msgBalancerStats, reply: reply}:
    case <-ctx.Done():
        return balancerStats{}, ctx.Err()
    }
    select {
    case s := <-reply:
        return s, nil
    case <-ctx.Done():
        return balancerStats{}, ctx.Err()
    }
}
```

`mu sync.Mutex` 제거. 주석 불필요 — goroutine 경계가 컴파일러 수준 invariant.

**중요**: 기존에 mutex로 읽던 외부 코드(Prometheus handler 등)를 `Stats()` API로 마이그레이션.

### 검증

- balancer 통합 테스트 통과
- `go test -race ./internal/cluster/...` 클린
- Prometheus metrics 정상 수집 확인

---

## Phase 5: Raft Node Full Actor (별도 스펙)

Phase 5는 규모(1406 lines), Raft 안전성 요구사항(persistLogEntries commit 전 완료 보장), 마이그레이션 기간 동안의 hybrid state 관리 복잡도로 인해 **별도 설계 문서**로 분리한다.

별도 문서에서 다룰 핵심 항목:
- `persistLogEntries` 분리 시 "persist-before-commit-ack" invariant 보장 방법
- hybrid 마이그레이션 기간(mu + actor 공존) 동안의 safety 보장 (feature flag 또는 big-bang refactor 선택)
- `atomic.Pointer[string]` leaderID 업데이트 시 값 복사 패턴 (actor local 변수 주소 직접 저장 금지)
- raftMsgKind 열거형 및 complete raftMsg 구조체 정의
- 3-node, 5-node e2e 검증 계획
- Jepsen-style linearizability 테스트 도입 여부

---

## 공통 고려사항

### Go 버전 호환성

`atomic.Pointer[T]`: Go 1.19+. 이 프로젝트는 Go 1.26+ 사용으로 가용.

### Goroutine Leak 방지

각 Actor는 `Stop()` 반환 전 run() goroutine 종료 보장 (`sync.WaitGroup` 사용).  
테스트에서 `goleak.VerifyNone(t)` 또는 `defer goleak.VerifyNone(t)` 추가.

### Back-pressure 정책

| Actor | 채널 버퍼 | 가득 찼을 때 |
|-------|----------|-------------|
| MigrationExecutor | 64 | Submit이 ctx 만료까지 블록, 타임아웃 시 error |
| BalancerProposer | 32 | Stats/Notify가 ctx 만료 시 error |
| Registry.InvalidateAll | 해당 없음 (직접 goroutine) | - |

### Race Detector

모든 Phase의 PR에 `go test -race ./...` 통과 필수.

---

## 성능 측정 계획

각 Phase는 **구현 전 baseline 측정 → 구현 → 구현 후 측정 → 비교** 순서로 진행한다.  
결과는 PR 본문에 before/after 수치로 기록.

---

### Phase 1: EC 병렬 팬아웃

**측정 대상**: EC write/read latency, 전체 PUT/GET throughput

**Benchmark 추가** (`internal/cluster/backend_bench_test.go` 신규):

```go
// 6-shard(4+2) EC write latency 측정
func BenchmarkPutObjectEC(b *testing.B) { ... }

// 6-shard EC read latency (k-of-n fast path 포함)
func BenchmarkGetObjectEC(b *testing.B) { ... }
```

**실행 명령**:
```bash
# baseline (구현 전 master에서)
go test -bench=BenchmarkPutObjectEC -benchtime=10s -count=5 ./internal/cluster/ | tee bench_before.txt

# 구현 후
go test -bench=BenchmarkPutObjectEC -benchtime=10s -count=5 ./internal/cluster/ | tee bench_after.txt

# 비교
benchstat bench_before.txt bench_after.txt
```

**k6 S3 통합 측정** (단일 노드 클러스터, EC 활성화):
```bash
# 구현 전/후 동일 조건
k6 run --vus=32 --duration=60s tests/k6/s3_put_get.js
```

**기대 결과**:
- EC write latency p50: 직렬 Σ → 병렬 max (이론상 최대 6x 감소)
- EC write latency p99: 네트워크 지터 영향, 2-4x 범위 예상
- PUT throughput: 비례 증가

---

### Phase 2: MigrationExecutor Actor

**측정 대상**: mutex 경합 제거 여부, goroutine leak 없음

고부하 워크로드에서 mutex contention 자체는 migration이 드문 경로라 latency 영향이 작다.  
이 Phase의 측정 초점은 **정확성**(race detector, leak) + **Actor 채널 처리 속도**.

**Benchmark 추가** (`migration_executor_bench_test.go`):
```go
// Actor 채널 왕복 latency (Submit → admission reply)
func BenchmarkMigrationExecutorSubmit(b *testing.B) {
    e := NewMigrationExecutor()
    defer e.Stop()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            _ = e.Submit(context.Background(), MigrationTask{...})
        }
    })
}
```

**실행 명령**:
```bash
go test -bench=BenchmarkMigrationExecutorSubmit -benchtime=5s -count=3 ./internal/cluster/
go test -race -count=3 ./internal/cluster/
```

**기대 결과**:
- Submit latency: mutex 버전 대비 동등하거나 소폭 증가 (Actor 채널 왕복 오버헤드)
- race detector: 완전 클린
- goroutine leak: 없음 (`goleak` 확인)

---

### Phase 3: Registry.InvalidateAll

**측정 대상**: Raft Apply 경로에서 InvalidateAll 소요 시간, Apply throughput

**Benchmark 추가** (`invalidator_bench_test.go`):
```go
// N개 VFS 등록 시 InvalidateAll 소요 시간
func BenchmarkInvalidateAll(b *testing.B) {
    for _, n := range []int{1, 5, 10, 20} {
        b.Run(fmt.Sprintf("vfs=%d", n), func(b *testing.B) {
            r := NewRegistry()
            for i := range n {
                r.Register(fmt.Sprintf("vfs%d", i), &slowInvalidator{delay: 5 * time.Millisecond})
            }
            b.ResetTimer()
            for range b.N {
                r.InvalidateAll(context.Background(), "key")
            }
        })
    }
}
```

**실행 명령**:
```bash
go test -bench=BenchmarkInvalidateAll -benchtime=5s -count=3 ./internal/cluster/ | tee bench_invalidate_after.txt
benchstat bench_invalidate_before.txt bench_invalidate_after.txt
```

**기대 결과**:
- VFS 1개: 동등 (goroutine spawn 오버헤드로 미세 증가 가능)
- VFS 5개: 약 5x 빠름 (5 × 5ms → 5ms)
- VFS 10개: 약 10x 빠름 (Apply loop 해방)

---

### Phase 4: BalancerProposer

**측정 대상**: Stats() 채널 왕복 latency (Prometheus scrape 경로)

Prometheus는 기본 15초 간격으로 scrape하므로 실사용 영향은 미미.  
측정 목적은 Actor 전환 후 회귀가 없음을 확인하는 것.

**Benchmark 추가** (`balancer_bench_test.go`):
```go
func BenchmarkBalancerStats(b *testing.B) {
    bp := NewBalancerProposer(...)
    defer bp.Stop()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            _, _ = bp.Stats(context.Background())
        }
    })
}
```

**실행 명령**:
```bash
go test -bench=BenchmarkBalancerStats -benchtime=5s -count=3 ./internal/cluster/
go test -race -count=3 ./internal/cluster/
```

**기대 결과**:
- Stats() latency: < 100µs (채널 왕복 + goroutine schedule)
- mutex 버전 대비: 동등하거나 소폭 증가 (경합 없는 mutex는 매우 빠르므로)
- race detector: 클린

---

### 전체 측정 요약

| Phase | 핵심 지표 | 기대값 | 측정 도구 |
|-------|----------|--------|----------|
| 1 | EC write latency p50 | -50% ~ -83% | `benchstat` |
| 1 | EC write latency p99 | -50% ~ -75% | `benchstat` |
| 1 | PUT throughput (RPS) | +50% ~ +5x | k6 |
| 2 | Submit latency | 동등 ± 10% | `benchstat` |
| 2 | race detector | 클린 | `go test -race` |
| 3 | InvalidateAll (10 VFS) | -90% | `benchstat` |
| 3 | race detector | 클린 | `go test -race` |
| 4 | Stats() latency | < 100µs | `benchstat` |
| 4 | race detector | 클린 | `go test -race` |

**PR 형식**: 각 Phase PR에 아래 템플릿으로 측정 결과 첨부:

```
## 성능 측정 결과

| 지표 | Before | After | Delta |
|------|--------|-------|-------|
| EC write latency p50 | Xms | Xms | -X% |
| EC write latency p99 | Xms | Xms | -X% |
```

---

## 파일 목록

| Phase | 파일 | 변경 유형 |
|-------|------|----------|
| 1 | `internal/cluster/backend.go` | EC fan-out 병렬화 |
| 2 | `internal/cluster/migration_executor.go` | Actor 전환 |
| 2 | `internal/cluster/migration_executor_test.go` | 테스트 업데이트 |
| 3 | `internal/cluster/invalidator.go` | mutex 버그 수정 + fan-out |
| 4 | `internal/cluster/balancer.go` | Actor 형식화 |
