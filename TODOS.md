# TODO

## Follow-ups
- [대형객체 chunking 통합 — 잔여] (a) 멀티파트-complete/EC-rewrap 단일화 여부(별 operation), (b) EC-memory·single-local-spooled 브랜치 완전제거(멀티파트/rewrap/unwired-test가 사용, shardGroup 보편화 필요), (c) [P3-perf] 작은객체 디스크 spool(메모리-티어 spool, perf deprioritized).

- [Slice 2 — EC orphan-shard 멀티그룹 fan-out] v0.0.589.0이 EC full-object orphan-shard scrubber를 **단일 그룹(group-0) 한정**으로 구현함(공유 dataDirs를 group-0 known으로만 안전 sweep). 멀티그룹 노드는 게이트 OFF라 누출 회수 안 됨(손실은 없음). Slice 2 = 모든 로컬 그룹에 걸친 cross-group known fan-out으로 게이트 해제. 이게 되면 multipart-complete phantom-commit 잔여 누출(propose timeout으로 보존됐으나 미commit인 rare² 샤드)도 멀티그룹에서 회수됨(단일그룹은 이미 v.589로 회수). segment GC도 같은 group-0-only 스코프라 함께 일반화 가능(TODOS: per-group segment GC).

- [P3-perf] WAL 제거 잔여 — `syncDirChain` 최적화: 같은 dir 반복 쓰기 시 이미 durable한 ancestor를 재-fsync함(신규 생성 레벨만 추적하면 최적화 가능). behavior-neutral 아님(perf 변경)이라 cosmetic rename과 분리. S5(segment/object WAL ops)는 LocalBackend test-fixture 전용·production caller 0(ADR-0015)이라 production 목표 무관. 로드맵/설계는 spec(docs/superpowers/specs/2026-06-14-wal-removal-design.md, git-untracked).
