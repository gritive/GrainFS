# 요청 Single/Cluster 실행 전략

이 문서는 사용자 요청을 하나의 `Operation` contract로 정규화한 뒤,
`single` 또는 `cluster` 실행 전략으로 처리하는 canonical 설계다.
actor pattern은 모든 요청 path가 아니라 cluster executor 내부의 상태 소유와
실패 처리에 집중 적용한다.

Planning artifacts under `docs/superpowers/` are local-only working notes.
이 문서가 구현 source of truth다.

```text
서버 시작
  |
  v
Route Availability 정책
  |
  v
사용자 / 클라이언트
  |
  v
API Gateway
  |
  v
Route Surface Manifest
  |
  v
인증 / Rate Limit
  |
  v
요청 검증
  |
  v
Operation Handler
  |
  v
Execution Planner
  |
  +---------------------------+
  |                           |
  v                           v
SinglePlan                 ClusterPlan
  |                           |
  v                           v
SingleExecutor             ClusterExecutor
  |                           |
  |                           v
  |                         Admission Control
  |                           |
  |                    +------+------+
  |                    |             |
  |              용량 초과       수락
  |                    |             |
  |                    v             v
  |                Response       JobActor
  |                                  |
  |              +-------------------+-------------------+
  |              |                   |                   |
  |              v                   v                   v
  |     Operation 상태 저장소   Worker Actor Pool   Supervisor
  |                                  |                   ^
  |                                  v                   |
  |                          중간 결과 저장소           |
  |                                  |                   |
  |                                  v                   |
  |                           AggregatorActor <----------+
  |                                  |
  +------------------+---------------+
                     |
                     v
              공통 Result Contract
                     |
                     v
                응답 포매터
                     |
                     v
              사용자 / 클라이언트

관측 지점:
  Route Surface Manifest, Execution Planner, SingleExecutor,
  ClusterExecutor, JobActor, Worker Actor Pool, Supervisor, AggregatorActor
```

## 라우팅 기준

- `single`은 분산 조정이 필요 없는 작고 동기적인 단순 요청에 사용한다.
- `cluster`는 worker fan-out과 결과 집계가 이득인 long-running, 병렬,
  배치성 요청에 사용한다.
- route/auth/validation/result formatting은 single/cluster로 분기하지
  않는다. 분기는 `ExecutionPlan` 이후 executor strategy에서만 발생한다.

## 경계

- `Route Surface Manifest`는 요청 실행 전에 HTTP path를 S3, ops, admin,
  Iceberg, UI surface로 분류한다.
- `Route Availability Policy`는 선택적 subsystem이 없을 때 feature 기반
  route를 숨길지, 등록하되 unavailable 응답을 줄지 route 등록 시점에
  결정한다.
- `Operation Handler`는 handler 입력을 사용자-facing 의미의 `Operation`으로
  정규화한다.
- `Execution Planner`는 같은 operation에 대해 `SinglePlan` 또는
  `ClusterPlan`을 선택한다.
- `SingleExecutor`는 actor hop 없이 직접 실행해 hot path를 보호한다.
- `ClusterExecutor`는 actor 기반 orchestration을 담당한다.
- `JobActor`는 cluster orchestration, fan-out, fan-in, 취소 전파,
  타임아웃 처리를 소유한다. Operation-specific 상태의 source of truth는
  기존 도메인 저장소다. Scrub의 경우 `scrubber.Director`가 session,
  dedup, status, cancel 상태의 source of truth다.
- `WorkerActor` pool은 partition 단위 작업을 bounded concurrency로 처리한다.
- `Supervisor`는 retry, backoff, cancellation, failure policy를 적용한다.
- `AggregatorActor`는 worker 중간 결과를 공통 result contract로 병합한다.
- Actor 구현은 process 내 Go goroutine + bounded buffered channel mailbox로 한다.
  mutex 기반 큐를 도입하지 않는다.
- Worker fan-out이 노드 간으로 확장될 때는 wire format으로 FlatBuffers
  (기존 `internal/**/*.fbs` 패턴)를 사용하고 JSON을 쓰지 않는다.
  이번 scope의 cluster executor는 단일 노드 내 actor orchestration만 다룬다.

## 처리 흐름

1. 클라이언트가 API gateway로 요청을 보낸다.
2. `Route Surface Manifest`가 path를 분류하고 인증 정책을 선택한다.
3. 요청은 인증, rate limit, 검증을 통과한다.
4. `Operation Handler`가 요청을 공통 `Operation`으로 정규화한다.
5. `Execution Planner`가 `SinglePlan` 또는 `ClusterPlan`을 만든다.
6. `SinglePlan`은 `SingleExecutor`가 직접 실행한다.
7. `ClusterPlan`은 `ClusterExecutor`가 admission control을 먼저 적용한다.
8. 수락된 cluster job은 `JobActor`가 lifecycle을 소유한다.
9. `JobActor`가 worker pool로 partition 작업을 fan-out한다.
10. worker들은 partition 성공 또는 실패를 `JobActor`에 보고한다.
11. `Supervisor`는 재시도, 타임아웃, 취소, job 실패 정책을 처리한다.
12. 필수 partition이 완료되면 `AggregatorActor`가 중간 결과를 병합한다.
13. single/cluster 결과는 같은 `Result` contract로 응답 포매터에 전달된다.

## Job 상태

```text
[start]
  |
  v
Pending
  |
  v
Running
  |
  +--> Retrying ----+
  |                 |
  |                 v
  |              Running
  |
  +--> Aggregating --> Succeeded --> [done]
  |
  +--> Failed ---------------------> [done]
  |
  +--> TimedOut -------------------> [done]
  |
  +--> Cancelled ------------------> [done]
```

## 핵심 정책

- single/cluster는 별도 product path가 아니라 같은 operation의 executor
  strategy다.
- cluster job을 시작하기 전에 idempotency key 또는 request digest를 사용한다.
  새 operation의 idempotency key는 `google/uuid` `NewV7()` 기반 time-ordered ID로
  만든다. 기존 Scrub의 `session_id` 형식은 호환을 위해 유지한다.
- route-surface 분류는 actor layer 밖에 둔다. 그래야 S3, ops, admin,
  Iceberg, UI 인증 정책이 결정적이고 테스트 가능하게 유지된다.
- 선택적 subsystem의 노출 여부는 handler 곳곳의 nil check가 아니라 route
  availability manifest에 둔다.
- cluster 작업을 queue에 넣기 전에 admission control을 적용한다.
- cluster 응답 정책을 명확히 한다. Scrub migration은 기존
  `session_id`/`created` response contract와 현재 status code semantics를
  유지한다. 새 operation만 별도 API contract가 있을 때 `202 Accepted`와
  operation-specific status 조회를 사용할 수 있다.
- worker retry는 partition 단위로 제한하고, 전체 job 실패 정책은
  `JobActor`와 `Supervisor`가 소유한다.
- routing decision, queue depth, job duration, retry count, timeout count,
  worker failure rate, aggregation failure rate를 추적한다.

## Package 경계

Handler-facing contract만 `internal/server/execution`에 둔다.

```text
internal/server/execution/
  operation.go
  planner.go
  executor.go
  result.go
```

Cluster implementation과 runtime wiring은 server boundary 밖에 둔다.

```text
internal/serveruntime/executioncluster/
  executor.go
  job_actor.go
  worker_pool.go
  supervisor.go
  aggregator.go
  metrics.go
```

`admin` package는 slim interface만 본다. `internal/server/execution`은
meta-Raft, `ClusterCoordinator`, data-group runtime을 직접 import하지 않는다.

## Error Mapping

Executor error는 admin transport에 도달하기 전에 bounded error code로
변환한다. 원문 error string을 response code나 metric label로 쓰지 않는다.

| Executor error | Admin code | HTTP status | User-visible meaning |
|----------------|------------|-------------|----------------------|
| `ErrInvalidOperation` | `invalid` | 400 | 요청 의미가 operation contract를 만족하지 않음 |
| `ErrExecutionUnsupported` | `unsupported` | 422 | 현재 runtime에서 해당 operation 실행 불가 |
| `ErrAdmissionRejected` | `retry` | 503 | queue/worker capacity 초과, 나중에 재시도 가능 |
| `ErrJobTimedOut` | `job_timeout` | 504 | job deadline 초과 |
| `ErrJobCancelled` | `job_cancelled` | 409 | operator 또는 system cancellation으로 중단 |
| `ErrPartitionFailed` | `job_failed` | 500 | retry 소진 후 필수 partition 실패 |
| `ErrAggregationFailed` | `aggregation_failed` | 500 | worker 결과 병합 실패 |

Scrub status 조회에서 일부 peer/partition만 실패한 경우에는 error response로
바꾸지 않고 기존 `partial=true`와 `peer_failures` style의 partial result를
우선 사용한다.

## Capacity And Backpressure

초기 값은 보수적으로 둔다. 구현 후 metrics로 조정한다.

- Job mailbox: per node 64 jobs.
- Partition queue: per job 256 partitions.
- Worker count: `min(runtime.GOMAXPROCS(0), 8)` 기본값, operation별 override는
  실제 측정이 생긴 뒤 추가한다.
- Admission은 queue enqueue 전에 실행한다.
- User-triggered request가 capacity를 초과하면 enqueue하지 않고
  `ErrAdmissionRejected`로 명시적 reject한다.
- Raft-applied/FSM-triggered event는 silent drop하지 않는다. 실행 불가 상태를
  operation 상태 저장소에 failed/rejected reason으로 남겨 status 조회에서
  보여준다.
- Retry는 partition 단위 exponential backoff와 max attempt를 가진다.
  Queue pressure가 높으면 retry를 즉시 재queue하지 않고 backoff를 적용한다.

## Scrub Migration Contract

Scrub은 첫 migration 대상이지만 generic job API로 바꾸지 않는다.

- `POST /v1/scrub` response는 `session_id`와 `created`를 유지한다.
- `GET /v1/scrub/jobs/<session_id>`가 status source다.
- `DELETE /v1/scrub/jobs/<session_id>`는 기존 `Director.CancelSession`을
  통해 cancellation source of truth를 유지한다.
- `JobActor`는 `Director` session state를 대체하지 않는다. Worker fan-out과
  cluster orchestration만 담당한다.
- Duplicate request는 기존 dedup behavior처럼 같은 `session_id`로 수렴한다.

## Test Plan

Go test framework를 사용한다. 기본 검증 명령:

```bash
make test-unit
make test-race
go test ./internal/server/admin ./internal/scrubber ./internal/serveruntime
go test -count=1 -timeout 180s -v ./tests/e2e -run 'TestE2E_ECScrubTrigger'
```

Required regression and contract tests:

- `internal/server/execution/*_test.go`: same scrub operation against
  `SingleExecutor` and `ClusterExecutor` returns the same external result
  schema and mapped error categories.
- `internal/server/admin/handlers_scrub_test.go`: scrub response remains
  `session_id`/`created`; invalid scope and proposer failure keep existing
  admin error behavior.
- `internal/serveruntime/*execution*_test.go`: cluster executor uses
  `scrubber.Director` as scrub state source of truth; actor state cannot diverge
  from status/cancel/dedup.
- `tests/e2e/ec_scrub_trigger_e2e_test.go`: actor path preserves duplicate POST
  behavior and returns the same `session_id`.
- `internal/server/admin/hertz_response_test.go`: executor errors map to
  explicit admin codes and HTTP statuses.
- Actor tests cover worker success fan-in, worker failure retry, retry
  exhaustion, timeout, cancellation, mailbox full behavior, and shutdown without
  goroutine leaks.

## Coverage Diagram

```text
Code path / behavior                         Required verification
------------------------------------------   --------------------------------
Operation construction                       unit: invalid scope, missing input
Execution planning                           unit: single, cluster, unsupported
Executor contract                            contract: result schema parity
Executor error mapping                       unit: admin code and HTTP status
Scrub trigger response                       unit + E2E: session_id/created
Scrub duplicate trigger                      unit + E2E: same session_id
Scrub status and cancellation                unit: Director remains source
Cluster admission                            unit: full queue rejects visibly
JobActor worker success                      unit: fan-in and aggregate
JobActor worker retry                        unit: retry then success
JobActor retry exhaustion                    unit: failed state and code
JobActor timeout                             unit: deadline to timeout state
JobActor cancellation                        unit: cancel propagation
Aggregator partial result                    unit: partial=true with failures
Shutdown                                     race/unit: no goroutine leak
S3 hot path                                  benchmark: no actor hop regression
```

## Performance Acceptance

Performance checks are pass/fail, not best-effort.

This scrub actor migration slice uses the admin trigger microbenchmarks and
focused scrub E2E as its merge gate. The broader S3 p95/p99 gate and
partition worker utilization metrics apply when the cross-node worker fanout
path is wired.

- Single executor adapter overhead: before/after on the same machine, 5 runs,
  median p95 delta must be <= 10% versus the direct handler path.
- Admin `/v1/scrub` trigger latency: before/after on the same machine, 5 runs,
  median p95 delta must be <= 10%.
- S3 hot path must not route through execution actors. Run the existing S3
  benchmark path after wiring and confirm p95/p99 are within existing variance.
- Metrics required before merging cluster actor path:
  job queue depth, partition queue depth, worker utilization, retry count,
  timeout count, job duration, worker failure count, aggregation failure count.

## Failure Modes

| Codepath | Failure mode | Test required | Error handling | User-visible result |
|----------|--------------|---------------|----------------|---------------------|
| Operation construction | Missing bucket or invalid scope | Unit | `invalid` | Clear 400 response |
| Planner | Cluster unavailable | Unit | single fallback or `unsupported` | Clear unsupported/fallback behavior |
| Admission | Queue/worker capacity exceeded | Unit + metrics | `ErrAdmissionRejected` | 503 retry response |
| Scrub trigger | Duplicate request | Unit + E2E | Director dedup | Same `session_id`, `created=false` |
| Job actor | Worker fails then succeeds | Unit | partition retry | Status eventually succeeds |
| Job actor | Retry exhausted | Unit | `ErrPartitionFailed`/failed state | Status/error explains failed partition |
| Job actor | Deadline exceeded | Unit | `ErrJobTimedOut` | Timeout code/status |
| Job actor | Cancel requested | Unit + handler | Director cancellation source | Status `cancelled` |
| Aggregator | Some peer/partition unavailable | Unit | partial result when possible | `partial=true` with failures |
| Shutdown | Actor/worker goroutine leak | Unit/race | context cancellation and drain | Clean shutdown |

Critical silent-failure rule: no accepted job may be dropped from a full mailbox
without a persisted rejected/failed reason visible through status lookup.

## NOT In Scope

- Generic durable job database schema: defer until a second operation proves
  `Director`-style state adapters are insufficient.
- S3 object hot path migration: explicitly excluded to avoid latency regression.
- Tree/streaming aggregation: defer until single aggregator fan-in bottleneck is
  measured.
- External actor framework: Go channels, goroutines, context, and bounded worker
  pools are enough for this scope.
- New generic job API: scrub keeps existing operation-specific status surface.

## What Already Exists

- `internal/server/admin/handlers_scrub.go`: scrub trigger, status, list, cancel
  handlers already define the API contract.
- `internal/scrubber/director.go`: existing source of truth for sessions,
  dedup, queueing, status, and cancellation.
- `internal/serveruntime/adapters.go`: existing adapter shape for scrub proposer
  and aggregation wiring.
- `internal/server/admin/hertz_response.go`: existing admin error-code to HTTP
  status mapping.
- `tests/e2e/ec_scrub_trigger_e2e_test.go`: existing cluster scrub trigger and
  dedup E2E coverage.

## Worktree Parallelization

| Step | Modules touched | Depends on |
|------|-----------------|------------|
| Contract and planner | `internal/server/execution/` | — |
| Admin error mapping | `internal/server/admin/` | Contract and planner |
| Scrub adapter wiring | `internal/serveruntime/`, `internal/scrubber/` | Contract and planner |
| Cluster actor executor | `internal/serveruntime/executioncluster/` | Scrub adapter wiring |
| E2E and perf checks | `tests/e2e/`, `benchmarks/` | Cluster actor executor |

Lane A: contract and planner -> admin error mapping.
Lane B: scrub adapter wiring after contract interfaces settle.
Lane C: cluster actor executor after A + B merge.
Lane D: E2E/perf after C.

Execution order: launch A first. B can begin once interfaces are stable. C waits
for A+B. D waits for C.

## Implementation Tasks

Synthesized from /plan-eng-review findings.

- [x] **T1 (P1, human: ~1h / CC: ~10min)** — Scrub execution — Keep
  `scrubber.Director` as scrub state source of truth.
  - Surfaced by: Architecture — JobActor and Director both claimed lifecycle
    ownership.
  - Files: `docs/architecture/request-single-cluster-flow.md`,
    `internal/serveruntime/executioncluster/`, `internal/scrubber/`
  - Verify: `go test ./internal/scrubber ./internal/serveruntime`
- [x] **T2 (P1, human: ~45min / CC: ~10min)** — Scrub API — Preserve
  `session_id`/`created` response contract.
  - Surfaced by: Architecture — `202 Accepted + job_id` conflicted with current
    scrub API.
  - Files: `internal/server/admin/`, `internal/adminapi/`,
    `tests/e2e/ec_scrub_trigger_e2e_test.go`
  - Verify: `go test ./internal/server/admin` and focused E2E scrub trigger test.
- [x] **T3 (P2, human: ~1-2h / CC: ~20min)** — Package boundaries — Keep
  cluster executor implementation outside `internal/server`.
  - Surfaced by: Architecture — server package would otherwise import runtime
    cluster details.
  - Files: `internal/server/execution/`, `internal/serveruntime/executioncluster/`
  - Verify: `go test ./internal/server/... ./internal/serveruntime/...`
- [x] **T4 (P2, human: ~1h / CC: ~10min)** — Error mapping — Add executor
  error to admin code/status mapping.
  - Surfaced by: Code Quality — new executor errors lacked transport mapping.
  - Files: `internal/server/admin/hertz_response.go`,
    `internal/server/admin/hertz_response_test.go`
  - Verify: `go test ./internal/server/admin`
- [x] **T5 (P2, human: ~1h / CC: ~15min)** — Docs — Keep architecture doc as
  canonical and avoid duplicate actor design sources.
  - Surfaced by: Code Quality — two docs had diverging flow details.
  - Files: `docs/architecture/request-single-cluster-flow.md`,
    `docs/superpowers/specs/2026-05-15-actor-execution-strategy-design.md`
  - Verify: manual doc review.
- [x] **T6 (P1, human: ~2h / CC: ~20min)** — Tests — Add concrete
  regression/contract tests for scrub migration.
  - Surfaced by: Test Review — category-level test plan was not enough.
  - Files: `internal/server/execution/*_test.go`,
    `internal/server/admin/handlers_scrub_test.go`,
    `internal/serveruntime/*execution*_test.go`,
    `tests/e2e/ec_scrub_trigger_e2e_test.go`
  - Verify: `make test-unit` and focused scrub E2E.
- [x] **T7 (P2, human: ~1h / CC: ~15min)** — Backpressure — Define and enforce
  capacity/full policy.
  - Surfaced by: Performance — bounded mailboxes had no capacity or full policy.
  - Files: `internal/serveruntime/executioncluster/`
  - Verify: actor mailbox full unit test and metrics assertions.
- [x] **T8 (P2, human: ~1h / CC: ~15min)** — Performance — Add pass/fail
  latency threshold and measurement commands.
  - Surfaced by: Performance — “meaningfully worse” was not measurable.
  - Files: benchmark or focused test files selected during implementation.
  - Verify: actor trigger benchmark and focused E2E for this slice; broader
    p95/p99 gates apply to the later S3/cross-node worker path.
