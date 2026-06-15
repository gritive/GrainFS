# TODO

## Follow-ups
- [대형객체 chunking 통합 — 잔여] (a) RESOLVED-by-design: PUT/멀티파트는 production서 이미 청킹 단일화, convert/coalesce는 의도적 별 operation(writeSpooledShards 유지). (b) DONE: production-dead EC-memory·single-local fast-path 제거. (c) [P3-perf] 작은객체 디스크 spool(메모리-티어 spool, perf deprioritized).
- [test-only dead code cleanup — 별 PR] dead-fastpath 스윕서 발견, 0-ref 6개는 이미 제거. 잔여 test-only(production 정의·_test.go만 참조, 각 제거 시 test reroute 필요): `newECReconstructStreamReader`(ec.go), `ProposeIcebergCreateNamespace`(meta_raft.go, family 中 유일 잔존), `InflightBytes`(append_forward_buffer.go), `WithMaxForwardReadStreams`(forward_sender.go), `PutTraceStageMetaIndex*` 6 const(put_trace.go, 미배선), `bucketKey/multipartKey/bucketPolicyKey/bucketVerKey`(apply.go, keyspace.go 정본의 중복). 유지(절대 삭제 금지): codec decode halves(`//nolint:unused` wire-compat pin), `MetaCmdType*` IAM enum(스냅샷 wire-compat), `Less`(heap.Interface).
