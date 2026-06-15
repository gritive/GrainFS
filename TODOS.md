# TODO

## Follow-ups
- [대형객체 chunking 통합 — 잔여] (a) 멀티파트-complete/EC-rewrap 단일화 여부(별 operation), (b) EC-memory·single-local-spooled 브랜치 완전제거(멀티파트/rewrap/unwired-test가 사용, shardGroup 보편화 필요), (c) [P3-perf] 작은객체 디스크 spool(메모리-티어 spool, perf deprioritized).

- [P3-perf] WAL 제거 잔여 — `syncDirChain` 최적화: 같은 dir 반복 쓰기 시 이미 durable한 ancestor를 재-fsync함(신규 생성 레벨만 추적하면 최적화 가능). behavior-neutral 아님(perf 변경)이라 cosmetic rename과 분리. S5(segment/object WAL ops)는 LocalBackend test-fixture 전용·production caller 0(ADR-0015)이라 production 목표 무관. 로드맵/설계는 spec(docs/superpowers/specs/2026-06-14-wal-removal-design.md, git-untracked).
