# TODO

<!-- LocalBackend-removal follow-ups resolved: ListMultipartUploads bucket-existence
     and append-segment StoredSize codec parity fixed (this PR); EC ranged-GET
     compressed-segment re-decompress moved to GitHub Projects #2 (GrainFS). A
     systemic authz question surfaced (non-existent bucket list → 403 no_grant vs
     404 NoSuchBucket) is also tracked on GitHub Projects #2. -->

- [ ] Streaming-only unification follow-ups (PR #streaming-only, Tasks A/B/C/D)
  - [ ] **ReadShardRange RPC 정리**: `ReadShardRangeStream`으로 통합 후 `ReadShardRange` one-shot RPC는 size-fork에서 호출 없음. 의도적 프로토콜 정리 PR로 server handler(`shard_service.go:721`), server guards(`:444, :1071`), client test(`shard_service_test.go:429-451`), `maxShardRangeReplyBytes` const 삭제 여부 결정.
  - [ ] **ReadObject/readShards 은퇴**: 8+ test/bench 호출자(`ec_object_reader_test.go:218,234,253,303,446`, `ec_object_reader_characterization_test.go:171,259,304`, `backend_bench_test.go:244`)를 `OpenObject` 기반으로 포팅 후 `ReadObject`/`readShards` 삭제.
  - [ ] **In-heap full-shard cache 정리** (회귀 측정 완료, 수용): `openShardReaders` 스트리밍 경로는 `shardCacheKey` 캐시를 채우지 않음(`readShards`/`ReadObject` = test/bench 전용). **warm repeated-GET 벤치 측정(single-node, `BenchmarkGetObjectEC`, count=8, control clean p=0.505)**: 64KiB +20.3%, 1MiB +8.6% wall-time 회귀(p<0.01), B/op -6~21%·allocs -12% 개선. 완전 통일+메모리 우선으로 **회귀 수용, merge 결정**. 남은 옵션: (a) 캐시 machinery(`applyResult:387` Put, `cachePrepass:320-359` Get) 완전 제거(dead 정리), 또는 (b) 스트리밍 경로에 Put/Get 배선해 warm hit 복원. **cluster warm-GET은 remote shard라 page cache 미커버 → 회귀 더 클 수 있음, GCP 벤치로 확인 후 (a)/(b) 결정 권장.**
  - [ ] **appendable EC-backed segment 통합 테스트**: `appendable_ec_segment_test.go`의 `fakeSegmentECOpener`는 EC reconstruction을 바이패스 — `OpenObject` 스트리밍 경로(production: `append.go:397` → `segment_store.go:48`)를 real shard service로 실행하는 통합 테스트 추가. *(Fix 2: `fakeECObjectShardFetcher`+`buildFakeShards` 기반 단위 테스트 추가, 포함 degraded-shard RS 재건 검증; real shard service 경로는 잔여 갭)*
  - [ ] **WriteLocalShardContext dead surface**: `WriteLocalShardContext` (kept for `localShardStore` interface) has zero production callers after Task A removed the buffered write branch. Fold its retirement into the `ReadObject`/`readShards` retirement follow-up (both need the interface to stabilise first).
  - [ ] **[pre-existing, separate PR] EC streaming reader close/cancel can hang**: `newAsyncPrefetchReader(dr, nil)` (`ec.go:310`, closeSrc=nil) — if the producer blocks in `io.ReadFull(remote shard)` (`async_prefetch_reader.go:88`), `Close`'s `<-r.done` (`:155`) waits for the producer, but the shard reader's context is not cancelled until `closeECShardReaders` runs AFTER `rc.Close()` (`ec_object_reader.go:117`). A client abort/partial read can hang until shardRPCTimeout. Pre-existing (large-object all-data path); the fork removal exposes it to small all-data reads too. Fix needs close-order/closeSrc rework — separate concurrency PR.
