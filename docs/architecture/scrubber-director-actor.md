# Scrub Director Actor 통합 설계

`internal/scrubber/Director`를 단일 owner actor 모델로 깊게 만들기 위한
설계 노트. `docs/architecture/request-single-cluster-flow.md`의 actor 전략을
`scrubber` 도메인에 구체 인스턴스로 적용하는 결과물이다.

이 문서는 구현 source of truth다. 구현 PR은 이 결정들을 따른다.
의미 변경(특히 dedup 정책)은 **out of scope** — 후속 task로 분리.

## 동기

현재 `Director`는 actor 모양이지만 완전하지 않다.
`workerLoop` goroutine + `queue chan triggerReq`는 actor pattern이지만,
`sources`/`verifiers`/`sessions`/`dedup` 4개 map은 `sync.Mutex`로 보호된다.
결과적으로 scrub session lifecycle의 source of truth가 mutex critical
section과 worker loop 두 군데에 흩어져 있다.

`liveSession` 카운터/상태는 이미 atomic으로 lock-free다. 이번 작업은
**registry 부분**만 actor 단독 소유로 옮긴다.

이점은 *rollback 로직 제거*가 아니다. 현재 코드도 `d.mu` 아래에서
sessions/dedup 추가와 queue push 실패 시 롤백을 atomic하게 수행하므로
호출자 관점 의미는 동일하다. 이점은 **locality** — 일관성 책임이 한
goroutine에 응집되고, 호출자가 별도로 본 mutex 임계영역과 worker loop
양쪽을 동시에 읽지 않아도 lifecycle을 추적할 수 있다는 데 있다.

## 참조 구현

이 설계는 코드베이스 내 다음 actor 사례를 참조한다. 구현 시 패턴
일관성 유지:

- `internal/raft/actor.go` — Raft Node가 cmd 채널에서 명령을 받아 처리.
  apply-style visitor 패턴의 정석 예.
- `internal/server/receipt_emitter_actor.go` — session map + inbox + reply
  chan 패턴. directorCmd 인터페이스 모델로 가장 가까움.
- `internal/serveruntime/executioncluster/executor.go` — execution actor의
  context-aware shutdown/done chan 패턴.
- `internal/raft/snapshot_actor.go` — atomic.Bool 가드를 통한 시점 제약
  표현 (Register guard와 유사).

## 제약

1. `Director.ApplyFromFSM`은 raft FSM apply loop에서 호출된다.
   **블록 절대 불가** — 큐가 가득 차면 drop + warning log가 현재 의미.
2. `Director.LookupDedup`은 cluster propose 경로에서 호출된다.
   같은 (bucket, prefix, scope, dryRun) 트리거가 raft entry를 burn하지
   않게 하는 short-circuit. ms 단위 latency 허용.
3. `Director.runSession`은 한 세션 전체(버킷 전체 scrub 반복)를
   수 분 동안 점유한다. 그동안 admin/FSM 호출은 즉시 응답해야 한다.
4. `Register`는 boot phase에서만 호출된다. `BlockSource`/`BlockVerifier`
   구현체가 다른 boot phase에서 생성되므로 `NewDirector` 한 번에
   주입하기는 불가 (별도 boot phase 재구성 필요 — out of scope).

## 토폴로지 결정: Controller + Worker (1:1)

```
Admin/FSM 호출자
       │
       ▼
   inbox chan directorCmd  ◄── controller goroutine 단독 소유
       │                       (sources, verifiers, sessions, dedup,
       │                        queue handle, logger, nodeID, incident)
       │ dispatch(sess, src, ver)  ◄── controller가 dispatch 시점에
       ▼                              source/verifier resolve해서 같이 전달
   queue chan triggerReq   ◄── 기존 채널, payload 확장
       │                       (sess + src + ver)
       ▼
   worker goroutine: runSession()
       │
       └── liveSession 카운터/상태 (atomic, 그대로 유지)
```

**Source/verifier resolve 시점**: controller가 dispatch 직전에 등록부에서
조회해 `triggerReq`에 동봉한다. worker는 등록부를 직접 만지지 않으므로
worker→controller round-trip이 발생하지 않는다.

**Queue full 시 의미 (현재 의미 보존)**: controller가 sessions/dedup
추가 후 `queue`에 push 시도. select default로 가득 검출 시:
- `triggerCmd`: sessions/dedup 롤백, reply `(sessionID="", created=false)`
- `applyFromFSMCmd`: sessions/dedup 롤백, warning 로그, 무응답

이는 현재 `Trigger`/`ApplyFromFSM`의 line 191-197/234-238 동작과 의미적
동등.

**셧다운**: `Stop()`은 `close(stop)`만 수행. controller goroutine과 worker
goroutine 둘 다 select에서 `stop` 감지 후 return. `Stop`은 추가로 `done
chan struct{}`을 통해 두 goroutine의 종료 완료를 대기한다 (테스트/부팅
호출자가 leak 검출 가능). 외부 ctx가 in-flight `runSession`의 즉시 중단을
담당.

검토한 대안과 탈락 이유:

- **단일 actor + fork-per-session**: `markDone` 등 worker→registry 경로가
  reply channel ping-pong이 되어 사실상 controller-worker 분리와 같아지지만
  비대칭으로 숨겨진다. fork goroutine 라이프사이클 추적 부담 추가.
- **Worker pool (N개 동시 세션)**: 운영 의미 변경(동시 scrub) 동반.
  CONTEXT/디자인 문서가 요구하지 않음. premature. controller만 바꾸면
  미래에 풀로 성장 가능 — 현재 인터페이스는 그 변화를 흡수한다.

## 세부 결정

### 1. inbox 메시지 형태: 단일 union channel

`type directorCmd interface { apply(*directorEnv) }`. 명령 struct 6개가
구체 구현. controller goroutine은 `stop` 채널 신호 기반으로 루프:

```go
for {
    select {
    case <-stop:
        close(done) // worker도 자기 done에 동일 처리
        return
    case cmd := <-inbox:
        cmd.apply(&env)
    }
}
```

`inbox`는 close하지 않는다 (외부 송신자가 close 후 send → panic 위험).
종료 신호는 오직 `stop` 채널 close.

`directorEnv`는 단순 데이터 컨테이너가 아니라 **controller environment**다.
다음을 포함:

- `sources map[string]BlockSource`, `verifiers map[string]BlockVerifier`
- `sessions map[string]*liveSession`, `dedup map[string]string`
- `queue chan triggerReq` — worker로 dispatch
- `nodeID string`, `incident IncidentRecorder` — runSession이 아닌
  controller가 메시지 처리 도중 incident 기록할 일은 없으나 future
  growth를 위해 env에 포함
- logger (drop 경고 등)

`apply`가 worker로 dispatch하거나 drop을 로깅하려면 이 env가 필요하다.

- 전역 FIFO 보장 (ApplyFromFSM → LookupDedup 일관성)
- 명령 카탈로그가 한 파일에 모임 — 곧 새 인터페이스의 test surface
- closure 채널 대안 탈락: 호출자 scope 캡쳐 → race 위험

### 2. ApplyFromFSM 비차단: 현재 의미 그대로 보존

```go
func (d *Director) ApplyFromFSM(entry ScrubTriggerEntry) {
    select {
    case d.inbox <- applyFromFSMCmd{entry: entry}:
    default:
        log.Warn(...).Msg("scrub director: inbox full, dropped FSM entry")
    }
}
```

- admin 호출(`Trigger`/`Sessions`/...)은 blocking send + reply chan
- FSM 호출은 non-blocking + drop (현재 동작과 동일)
- inbox size = 현재 `queue` size (64) 유지
- 부수적 개선: admin이 폭주해도 FSM과 별개로 backpressure 분리됨

우선순위 채널 대안 탈락: 현실에서 controller 명령은 모두 빠른 state
mutation이라 inbox가 가득찰 시나리오가 정상 운영에 없음. 진짜로 drop이
잦아지면 후속 개선.

### 3. Register는 Start 이전 직접 mutation

- `Director`에 `started atomic.Bool` 추가
- `Register`는 `started.Load()` 가드 — `true`면 panic
- mu는 완전히 제거됨

constructor 옵션으로 강제하는 대안은 boot phase 재구성 동반 — 별도
candidate로 분리.

### 4. 명령 struct + 내장 reply chan

명령 카탈로그:

| 명령 | reply 타입 | 호출 경로 |
|------|------------|-----------|
| `triggerCmd` | `(sessionID string, created bool)` | admin HTTP |
| `applyFromFSMCmd` | 없음 (fire-and-forget) | raft FSM apply |
| `lookupDedupCmd` | `(ScrubTriggerEntry, bool)` | cluster propose 경로 |
| `sessionsCmd` | `[]Session` | admin HTTP |
| `getSessionCmd` | `(Session, bool)` | admin HTTP |
| `cancelCmd` | `error` | admin HTTP |

`triggerReq` (controller → worker)는 source/verifier를 동봉한다:

```go
type triggerReq struct {
    sess *liveSession
    src  BlockSource
    ver  BlockVerifier
}
```

- reply는 명령 struct의 `reply chan T` 필드
- `applyFromFSMCmd`만 reply 없는 fire-and-forget struct
- callback closure 대안 탈락: caller scope 캡쳐 race 위험
- `any` payload 대안 탈락: 카탈로그 타입 안전성 소실

### 5. dedup 영구성은 이 PR scope 아님

현재 코드는 dedup 키를 **영구히 제거하지 않음**. 의도/버그 미정.
이번 PR은 *순수 refactor* — 현재 동작 그대로 보존한다.

후속 task로 분리:
- dedup 영구성 정책 결정 (ADR 또는 grill-with-docs 영역)
- 결정 결과에 따라 controller inbox에 `removeDedupCmd` 추가 (작은 변경)

### 6. 테스트 전략: black-box 기본 + 일부 internal

- `director_test.go` (기존): public API 시나리오 (Trigger → query → cancel)
  로 black-box 검증
- `director_internal_test.go` (신규, 같은 패키지): inbox에 명령 직접
  enqueue해 메시지 순서 의존 race 시나리오 결정적 검증. 6 시나리오:
  - FSM apply 직후 LookupDedup이 같은 sessionID를 본다
  - Trigger 직후 GetSession이 새 세션을 본다
  - Cancel 직후 worker가 다음 block 경계에서 정지한다 (현행 보존)
  - ApplyFromFSM이 같은 sessionID로 두 번 도착 — 두 번째는 no-op
    (raft 재전송/노드 간 중복 시나리오)
  - controller→worker queue full → sessions/dedup 롤백 + reply
    (sessionID="", created=false), drop 카운터 증가
  - `Stop()` 도중 inbox에 명령 도착 → drop 또는 정상 처리, panic 없음
    + `done` chan close 후 호출자 대기 가능
- worker mock 주입 대안 탈락: BlockSource/Verifier mock으로 충분히 제어됨

## Out of Scope

이 PR이 *건드리지 않는* 것들 — 별도 task로 분리한다.

- **dedup 영구성 정책**. 현행 동작 그대로 보존. 결정 5 참조.
- **동시 scrub 세션 (Worker pool)**. 토폴로지 결정 참조. 미래에 controller만
  바꾸면 성장 가능하지만 본 PR은 직렬 유지.
- **공통 JobActor 추상화**. `scrubber`/`lifecycle`/`migration` worker가
  공유 가능한 라이프사이클 패턴이지만, 본 PR은 scrubber만 다룬다.
- **Boot phase 재구성으로 Register 제거**. constructor 옵션화는 boot phase
  의존 그래프 재배치 필요 — 별도 candidate.

## 구현 단계 (TDD)

1. `directorCmd` interface + 6개 명령 struct 정의 — `apply(*directorEnv)`
   메서드 포함. `triggerReq` 구조체에 src/ver 필드 추가.
2. `directorEnv` struct로 sources/verifiers/sessions/dedup + queue handle +
   nodeID + incident recorder + logger 모음 (controller environment).
3. Director에 `inbox chan directorCmd`, `done chan struct{}`, `started
   atomic.Bool` 필드 추가. 기존 `queue` 유지.
4. `Director.run(ctx)` controller loop 추가. 기존 `workerLoop`는 worker
   goroutine으로 분리. controller가 dispatch 시점에 src/ver resolve해
   `queue`로 전달.
5. `Trigger`/`ApplyFromFSM`/`LookupDedup`/`Sessions`/`GetSession`/
   `CancelSession`을 inbox enqueue 패턴으로 재작성. queue full 시
   sessions/dedup 롤백.
6. `Register`에 `started` 가드 추가, mu 제거.
7. `Start`는 controller + worker 두 goroutine 기동, `Stop`은 `close(stop)`
   후 `<-done` 대기.
8. `director_internal_test.go` 신설 — race 시나리오 6개.
9. 기존 `director_test.go` green 확인.

## 검증

- `make test-unit` green
- 기존 `director_test.go` 의미적으로 동등 (의미 변경 없음)
- `go test -race ./internal/scrubber/...` green
- ADR 0011 등 raft FSM 일관성 보장과 무관 (FSM drop 의미 보존)

## 후속 task (TODOS.md에 추가)

- [ ] scrubber: dedup 영구성 정책 ADR 결정 후 cleanup 명령 추가
- [ ] scrubber/lifecycle/migration worker 공통 JobActor 추상화 검토
- [ ] scrubber boot phase에 Register 강제 시점 가드 (constructor 옵션화 검토)
