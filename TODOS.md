# TODO

- [ ] production-dead `storage.LocalBackend` 제거 (별도 에픽, 접근 결정 대기)
  - 배경: single-node serve도 `NewDistributedBackendForGroup(group-0)`를 쓰고, `storage.LocalBackend`는
    production에서 구성되지 않음(test-only, ~3000 LoC + storagepb codec/Badger 영속화). storagepb
    `SegmentRef`가 `stored_size`를 흘리는 codec #2 gap도 이 백엔드에 속함(현재 라이브 read 미도달).
  - 걸림돌: ~87개 테스트 파일이 substrate로 사용. 특히 `internal/server/*_test.go` 29개는
    `setupTestServerWithBackend()`(server_test.go) 경유로 production server-layer(auth/ACL/versioning/
    lifecycle) 커버리지를 이 백엔드 위에서 검증 → 무작정 삭제 시 커버리지 손실.
  - 접근 미결(A/B/C): (A) LocalBackend 유지 + codec #2 gap은 note로 / (B) server 테스트를 production
    단일노드 `DistributedBackend`로 재홈 후 삭제 / (C) 얇은 in-memory test 백엔드 신규 후 삭제.
    → 별도 spec+plan gate에서 정확한 비용으로 확정 후 진행.

- [ ] Streaming-only unification follow-ups (PR #streaming-only, Tasks A/B/C/D)
  - [ ] **ReadShardRange RPC 정리**: `ReadShardRangeStream`으로 통합 후 `ReadShardRange` one-shot RPC는 size-fork에서 호출 없음. 의도적 프로토콜 정리 PR로 server handler(`shard_service.go:721`), server guards(`:444, :1071`), client test(`shard_service_test.go:429-451`), `maxShardRangeReplyBytes` const 삭제 여부 결정.
  - [ ] **ReadObject/readShards 은퇴**: 8+ test/bench 호출자(`ec_object_reader_test.go:218,234,253,303,446`, `ec_object_reader_characterization_test.go:171,259,304`, `backend_bench_test.go:244`)를 `OpenObject` 기반으로 포팅 후 `ReadObject`/`readShards` 삭제.
  - [ ] **In-heap full-shard cache 정리**: `openShardReaders` 스트리밍 경로는 `shardCacheKey` 캐시를 채우지 않음(`readShards`/`ReadObject` = test/bench 전용). Task D 벤치마크: B/op -6~22% 개선(소형 객체), wall-time +11~20% 증가 관측 — laptop 노이즈인지 warm-GET 실회귀인지 **미확정**(cold single-GET 벤치라 warm-cache 손실을 직접 재현 못 함). **결정 보류**: 캐시 machinery(`applyResult:387` Put, `cachePrepass:320-359` Get) 완전 제거 vs 스트리밍 경로에 Put 추가 — 클러스터 모드 repeated warm-GET 벤치로 실회귀 여부 측정 후 결정.
  - [ ] **appendable EC-backed segment 통합 테스트**: `appendable_ec_segment_test.go`의 `fakeSegmentECOpener`는 EC reconstruction을 바이패스 — `OpenObject` 스트리밍 경로(production: `append.go:397` → `segment_store.go:48`)를 real shard service로 실행하는 통합 테스트 추가. *(Fix 2: `fakeECObjectShardFetcher`+`buildFakeShards` 기반 단위 테스트 추가; real shard service 경로는 잔여 갭)*
  - [ ] **WriteLocalShardContext dead surface**: `WriteLocalShardContext` (kept for `localShardStore` interface) has zero production callers after Task A removed the buffered write branch. Fold its retirement into the `ReadObject`/`readShards` retirement follow-up (both need the interface to stabilise first).
  - [ ] **[pre-existing, separate PR] EC streaming reader close/cancel can hang**: `newAsyncPrefetchReader(dr, nil)` (`ec.go:310`, closeSrc=nil) — if the producer blocks in `io.ReadFull(remote shard)` (`async_prefetch_reader.go:88`), `Close`'s `<-r.done` (`:155`) waits for the producer, but the shard reader's context is not cancelled until `closeECShardReaders` runs AFTER `rc.Close()` (`ec_object_reader.go:117`). A client abort/partial read can hang until shardRPCTimeout. Pre-existing (large-object all-data path); the fork removal exposes it to small all-data reads too. Fix needs close-order/closeSrc rework — separate concurrency PR.
