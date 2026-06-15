# TODO

## Follow-ups
- [대형객체 chunking 통합 — 잔여] (a) RESOLVED-by-design: PUT/멀티파트는 production서 이미 청킹 단일화, convert/coalesce는 의도적 별 operation(writeSpooledShards 유지). (b) DONE: production-dead EC-memory·single-local fast-path 제거. (c) [P3-perf, deprioritized] 작은객체 디스크 spool → 메모리-티어 spool.
