# ADR 0011: Bucket Lifecycle Policy via FSM Command

## Status
Accepted (2026-05-11).
Related: ADR 0007 (IAM Foundation — meta-Raft FSM command 패턴),
ADR 0001 (Storage operations facade — handlers ↔ domain seam discipline).

## Context

`internal/lifecycle.Store` 는 현재 `cluster.DistributedBackend.FSMDB()`(공유 BadgerDB)
에 raw `db.Update(...)` 로 직접 write 한다. meta-Raft FSM 을 거치지 않으므로 lifecycle
config 는 노드 간 **replicate 되지 않는다**. 결과적으로 다음 두 시나리오에서 정책이
조용히 사라진다.

1. 클라이언트가 **follower 노드에 PUT** 하면 그 노드의 로컬 BadgerDB 에만 들어가고
   leader 에서 도는 worker 는 영원히 그 정책을 보지 못한다.
2. **leader change** 가 일어나면 새 leader 는 이전 leader 의 로컬 BadgerDB 에만
   존재하던 정책을 모른 채 자기 worker 를 시작한다. operator 의 GET 도 노드별로
   다른 결과를 반환한다.

`internal/lifecycle.Service` 를 deep module 로 도입하면서, 이 모듈의 invariant
("operator 가 Apply 한 정책은 worker 가 실행한다") 를 유지하려면 정책 변경이 모든
voter 에 동기화되어야 한다. IAM 도메인이 이미 같은 문제를 meta-Raft FSM command
(`MetaCmdType` 21..33) 로 해결하고 있다.

## Decision

- Bucket lifecycle config 변경은 **meta-Raft FSM command** 로 replicate 한다.
- 새 `MetaCmdType` 값:
  - `MetaCmdTypeBucketLifecyclePut = 34`
  - `MetaCmdTypeBucketLifecycleDelete = 35`
- **Payload encoding 은 S3 wire XML 바이트 원형** (`PUT /{bucket}?lifecycle` body
  그대로). validation 은 propose 이전에 `lifecycle.Validate` 로 수행하고, FSM apply
  는 opaque 바이트를 `lifecycle.Store` 의 `"lifecycle:{bucket}"` 키에 그대로 쓴다.
  - 이유: lifecycle config 의 canonical form 이 곧 S3 wire XML 이고, operator 의
    GET 라운드트립이 byte-for-byte 가 되어야 한다. IAM payload 가 FlatBuffers 인
    것과는 다른 결정 — IAM 은 wire 와 내부 도메인 모델이 다른 반면 lifecycle 은
    동일하기 때문.
- Server (`PUT/GET/DELETE /{bucket}?lifecycle`) 핸들러는 XML body 만 받아
  `lifecycle.Service.Apply / Get / Delete` 를 호출한다. `lifecycle.Store` 에
  직접 접근하지 않는다.
- worker 는 leader 노드에서만 실행된다 (status quo). 그러나 worker 가 읽는 정책이
  이제 모든 노드에 replicate 된 정책이라는 점에서 invariant 가 닫힌다.

## Consequences

- (+) Follower 로 들어온 PUT 도 모든 voter 의 store 에 적용된다 — non-leader-aware
  client 가 lifecycle 정책을 안전하게 설정할 수 있다.
- (+) leader change 후 새 leader 의 worker 가 기존 정책을 그대로 본다.
- (+) raft snapshot / InstallSnapshot 에 lifecycle 키가 자연스럽게 포함된다 (FSM 이
  관리하는 BadgerDB 의 일부이므로).
- (−) PUT/DELETE 가 한 번의 Raft 왕복 비용을 추가로 낸다. lifecycle 변경은 운영
  이벤트 빈도이므로 실질적 영향 없음.
- (−) MetaCmdType 34/35 는 영구 예약. 향후 payload encoding 을 FlatBuffers 로
  바꾸려면 새 ID 를 할당해야 한다 (기존 ID 의 의미를 바꾸지 않는다).
- 업그레이드: 이 변경 이전에 follower 의 로컬 BadgerDB 에 단독으로 쓰여 있던
  lifecycle 키는 orphan 으로 남는다. 새 코드의 worker 는 leader 의 store 만 보므로
  실행에 영향 없음. 깔끔한 상태를 원하면 운영자가 정책을 한 번 재적용한다.

## Out of scope

- worker 실행 통계 (`Stats`) 의 admin/dashboard 표면화는 별도 follow-up.
- 다중 worker (leader 외 노드에서 read-only audit 등) 는 도메인 모델 변경이라
  본 ADR 의 범위가 아니다.
