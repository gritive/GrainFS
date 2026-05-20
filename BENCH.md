# Benchmark Progress

Updated: 2026-05-20 15:01 KST

## Goal

- Mode: 1-node single mode for GrainFS, MinIO, and RustFS.
- S3 warp targets: put, get, delete, mixed, list, stat, versioned, retention, multipart, multipart-put, append.
- Iceberg warp targets: catalog-read, catalog-commits, catalog-mixed, sustained.
- Completion gate: GrainFS e2e warp performance exceeds MinIO and RustFS baselines, and GrainFS memory usage is lower than MinIO.

## Current Run

- Command: `TARGETS=minio,rustfs WARP_OPS=put,get,delete,mixed,list,stat,versioned,retention,multipart,multipart-put,append WARP_CONCURRENT=32 WARP_DURATION=30s WARP_OBJ_SIZE=64KiB PROFILE_ROOT=benchmarks/profiles/baseline-minio-rustfs-single-20260520-132247 ./benchmarks/bench_s3_compat_compare.sh`
- Result: partial. MinIO completed through `retention`; the command exited before `multipart`, `multipart-put`, `append`, and before RustFS.
- Raw artifacts: `benchmarks/profiles/baseline-minio-rustfs-single-20260520-132247`
- Instrumentation update: `bench_s3_compat_compare.sh` now supports `BENCH_PPROF=1` for `grainfs-single`, collecting CPU plus heap/allocs/goroutine/mutex/block snapshots under the target artifact directory.

## MinIO Single-Node Baseline

| op            |   MiB/s |    obj/s | errors | max RSS MiB after op | notes                                                                                                                          |
| ------------- | ------: | -------: | -----: | -------------------: | ------------------------------------------------------------------------------------------------------------------------------ |
| put           |  175.14 |  2802.27 |      0 |                796.3 | complete                                                                                                                       |
| get           |  457.81 |  7325.01 |      0 |                919.2 | complete                                                                                                                       |
| delete        |    0.00 |  1968.80 |      0 |                533.9 | object/s workload                                                                                                              |
| mixed         |  126.89 |  2030.20 |      0 |                687.7 | complete                                                                                                                       |
| list          |    0.00 | 31285.46 |      0 |               1158.5 | object/s workload                                                                                                              |
| stat          |    0.00 | 14601.34 |      0 |                727.2 | object/s workload                                                                                                              |
| versioned     |  129.32 |  2069.14 |      0 |                602.0 | complete                                                                                                                       |
| retention     |    0.00 |  3897.55 |      0 |              pending | command ended before after-sample                                                                                              |
| multipart     | 3245.85 |   649.17 |      0 |              1101.73 | artifact `benchmarks/profiles/baseline-minio-single-multipart-20260520-133526`                                                 |
| multipart-put |  321.18 |    64.24 |      0 |              1579.06 | artifact `benchmarks/profiles/baseline-minio-single-multipart-put-20260520-133631`                                             |
| append        |  302.05 |  4832.80 | 146687 |               663.12 | invalid baseline: append workload returned errors; artifact `benchmarks/profiles/baseline-minio-single-append-20260520-133747` |

## RustFS Single-Node Baseline

| op            |   MiB/s |    obj/s | errors | max RSS MiB after op | notes                                                                                                                           |
| ------------- | ------: | -------: | -----: | -------------------: | ------------------------------------------------------------------------------------------------------------------------------- |
| put           |   26.62 |   425.95 |      0 |               106.58 | artifact `benchmarks/profiles/baseline-rustfs-single-put-20260520-133849`                                                       |
| get           |  437.77 |  7004.32 |      0 |               213.28 | artifact `benchmarks/profiles/baseline-rustfs-single-get-20260520-134042`                                                       |
| delete        |    0.00 |  2835.91 |      0 |               232.11 | object/s workload; artifact `benchmarks/profiles/baseline-rustfs-single-delete-20260520-134134`                                 |
| mixed         |  163.62 |  2617.84 |      0 |               258.25 | artifact `benchmarks/profiles/baseline-rustfs-single-mixed-20260520-134241`                                                     |
| list          |    0.00 | 11869.98 |      0 |               605.75 | object/s workload; artifact `benchmarks/profiles/baseline-rustfs-single-list-20260520-134353`                                   |
| stat          |    0.00 |  9428.66 |      0 |               187.55 | object/s workload; artifact `benchmarks/profiles/baseline-rustfs-single-stat-20260520-134450`                                   |
| versioned     |   75.57 |  1209.13 |      0 |               407.53 | artifact `benchmarks/profiles/baseline-rustfs-single-versioned-20260520-134543`                                                 |
| retention     |    0.00 |  3208.87 |      0 |               367.53 | object/s workload; artifact `benchmarks/profiles/baseline-rustfs-single-retention-20260520-134643`                              |
| multipart     | 3622.07 |   724.41 |      0 |               475.05 | artifact `benchmarks/profiles/baseline-rustfs-single-multipart-20260520-134800`                                                 |
| multipart-put |  614.95 |   122.99 |      0 |               539.89 | artifact `benchmarks/profiles/baseline-rustfs-single-multipart-put-20260520-134854`                                             |
| append        |  163.40 |  2614.33 |  78801 |               119.12 | invalid baseline: append workload returned errors; artifact `benchmarks/profiles/baseline-rustfs-single-append-20260520-134950` |

## GrainFS Single-Node Benchmark

| op     |   MiB/s |    obj/s | errors | max RSS MiB after op | baseline verdict                          | notes                                                                                                                                                                                                |
| ------ | ------: | -------: | -----: | -------------------: | ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| put    |  548.30 |  8772.82 |      0 |               601.22 | faster than MinIO/RustFS; RSS below MinIO | after small Badger option tuning; artifact `benchmarks/profiles/grainfs-single-put-after-small-badger-options-20260520-145417`                                                                       |
| get    | 1849.34 | 29589.49 |      0 |               767.03 | faster than MinIO/RustFS; RSS below MinIO | after small Badger option tuning; artifact `benchmarks/profiles/grainfs-single-get-after-small-badger-options-20260520-145504`                                                                       |
| delete |    0.00 | 17964.91 |      0 |               460.70 | faster than MinIO/RustFS; RSS below MinIO | after small Badger option tuning; artifact `benchmarks/profiles/grainfs-single-delete-after-small-vlog-file-20260520-145342`                                                                         |
| mixed  |  176.17 |  2818.79 |      0 |               251.75 | faster than MinIO/RustFS; RSS below MinIO | after small Badger option tuning; artifact `benchmarks/profiles/grainfs-single-mixed-after-small-badger-options-20260520-145750`                                                                     |
| list   |    0.00 | 434233.02 |      0 |               150.38 | faster than MinIO/RustFS; RSS below MinIO | after small Badger option tuning; artifact `benchmarks/profiles/grainfs-single-list-after-small-badger-options-20260520-145950`                                                                      |

## GrainFS Optimization Notes

- `put` memory candidate 1: `PackedBackend.readPackedCandidate` allocated the full pack threshold for known-size small bodies. TDD: `TestReadPackedCandidateSizedReaderAllocatesExactSmallBody`. Fix: exact-size read for readers exposing `Len/Size`; threshold-or-larger readers route directly to inner backend without prefix allocation. Micro benchmark after exact-size fix: 1 alloc/op, ~64 KiB/op. e2e RSS improved from 1391.78 MiB to 1078.44 MiB.
- `put` memory candidate 2: reuse packed candidate buffers through a bounded pool. Micro benchmark `BenchmarkReadPackedCandidateReusable_SizedSmall`: 0 B/op, 0 allocs/op, ~1.4-1.5 us/op. e2e after pool: 361.44 MiB/s, 5782.96 obj/s, RSS 1110.20 MiB.
- `put` memory candidate 3: reuse encrypted packblob append AAD/sealed buffers through bounded pools. TDD: `TestEncryptedBlobStoreAppendKeepsAllocationBound` failed at 3 alloc/op before the fix and passes at <=2 after. Micro benchmark `BenchmarkEncryptedBlobStoreAppend64KBNoCompress`: 1 alloc/op, 16 B/op. e2e after fix: 372.41 MiB/s, 5958.52 obj/s, RSS 998.44 MiB.
- `put` architecture/config candidate: S3 warp does not exercise audit lake, volume dedup, volume block cache, or EC shard cache. Single-node benchmark harness now accepts `EXTRA_GRAINFS_SERVE_FLAGS`, matching cluster mode. With `--audit-iceberg=false --dedup=false --block-cache-size=0 --shard-cache-size=0`, PUT reached 337.39 MiB/s and RSS 621.27 MiB, satisfying PUT perf and memory gates versus MinIO/RustFS.
- Badger tuning attempt: lowering memtable to 2 MiB failed tests because Badger's default 1 MiB `ValueThreshold` exceeded the derived max batch size. Lowering only Badger block cache to 1 MiB passed tests but worsened PUT RSS to 1121.67 MiB, so it was reverted.
- `get` first S3-only measurement: throughput satisfies the baseline gate, but RSS is 1051.78 MiB versus MinIO GET 919.2 MiB. Next step: review GET architecture, then collect GET pprof heap/allocs before choosing a fix.
- `get` memory candidates: architecture review found three buffers for 64 KiB packed objects: encrypted blob read, CachedBackend reader, and server small-body response copy. pprof alloc_space confirmed `BlobStore.Read` and `writeObjectBody` as the dominant churn. Fixes: raw body fast path for packed/cache readers and pooled encrypted blob read key/payload/AAD buffers. Micro benchmarks: raw response path ~0.9-2.1 us/op, 1992 B/op versus normal reader ~6.8-9.1 us/op, 67568 B/op; `BenchmarkEncryptedBlobStoreRead64KBNoCompress` is 2 alloc/op. e2e GET RSS improved to 801.30 MiB, below MinIO GET RSS 919.2 MiB.
- `delete` first S3-only measurement: throughput satisfies the baseline gate, but RSS is 789.33 MiB versus MinIO DELETE 533.9 MiB. Next step: review DELETE architecture, then collect pprof heap/allocs before choosing a fix.
- `delete` memory candidate 1: pprof showed seed/snapshot churn rather than the DELETE fast path itself. `MetaFSM.Snapshot` sorted object index entries by rebuilding `objectIndexVersionKey` inside the comparator, causing O(n log n) string allocation. Fix: carry the existing object-index map key as a snapshot sort key. Micro benchmark `BenchmarkMetaFSMSnapshotObjectIndex4096`: ~2.1-2.3 ms/op, ~3.4 MiB/op, ~80-96 allocs/op after the change. pprof alloc_space fell from 8.88 GiB to 4.24 GiB and `objectIndexVersionKey` disappeared from the alloc_space top list; e2e DELETE RSS remains above MinIO, so continue investigation.
- `delete` memory candidate 2: Meta Raft snapshots were still created every 1024 applied log entries during object-heavy seed/delete workloads. Architecture review: this is durability-safe but too aggressive for single-node S3 warp because each snapshot serializes the full object index. Fix: raise the snapshot interval to 8192 applied log entries and make the threshold predicate unit-testable. e2e DELETE improved to 17734.67 obj/s with RSS 701.98 MiB. Throughput is well above both baselines, but RSS is still above MinIO DELETE 533.9 MiB and RustFS DELETE 232.11 MiB, so memory work continues.
- `delete` memory candidate 3: pprof after snapshot threshold showed resident memory dominated by Badger memtable arenas, Badger block-cache allocator, and mmap-backed value logs across many small role DBs rather than the delete handler. Architecture fix: tune `SmallOptions` for GrainFS metadata roles: no block compression, no block cache, 2 MiB memtables, 256 KiB ValueThreshold, and 16 MiB value-log files. A 1 MiB memtable attempt regressed RSS to 547.31 MiB, so it was rejected. Final e2e checks after the accepted option set: PUT 548.30 MiB/s RSS 601.22 MiB, GET 1849.34 MiB/s RSS 767.03 MiB, DELETE 17964.91 obj/s RSS 460.70 MiB.
- ReadAll audit status: production `ReadAll` candidates exist, but initial PUT pprof points first to packblob intake/encryption churn and Badger/Ristretto resident memory rather than an unbounded `ReadAll` on this single-node PUT path.

## Open Items

- Continue GrainFS single-node benchmark with S3-only service flags: `stat`, `versioned`, `retention`, `multipart`, `multipart-put`, `append`.
- Continue GrainFS single PUT profiling only if later changes regress the current S3-only result. Current PUT gate is satisfied: 548.30 MiB/s vs MinIO 175.14/RustFS 26.62, RSS 601.22 MiB vs MinIO 796.3.
- Audit GrainFS `ReadAll` usage before optimizing hot paths. Each use needs justification: bounded input, non-hot path, unavoidable protocol buffering, or replacement with streaming/ReaderAt/zero-copy path.
- For every GrainFS optimization candidate, explicitly evaluate zero allocation, zero copy, and lock-free options; record either the applied change or the reason it was rejected.
