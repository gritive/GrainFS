# `GrainFS` Benchmark Methodology

Use this document to produce and interpret benchmark results.
`benchmarks/README.md` covers script-specific usage.

## Principles

Only publish numbers that include enough context to reproduce them:

- `GrainFS` commit and binary build mode.
- Host CPU, memory, OS, disk type, and network topology.
- Command line, environment variables, object size, concurrency, and duration.
- Durability mode: single-node, replication, EC profile, and node count.
- Encryption state. `GrainFS` benchmarks should assume at-rest encryption is on.
- Result artifact path, including raw JSON, fio output, traces, or pprof files.
- Date of run.

Do not compare `GrainFS` with RustFS, MinIO, or any other S3-compatible store
unless all systems run on the same host class, with comparable durability,
object sizes, concurrency, and cold/warm-cache rules.

## Existing Benchmark Targets

| Target                                        | Scope                                              | Primary artifacts                                                   |
| --------------------------------------------- | -------------------------------------------------- | ------------------------------------------------------------------- |
| `make bench`                                  | Single-node S3 `warp` PUT/GET workload             | `benchmarks/profiles/s3-compat-compare-*`                           |
| `make bench-cluster`                          | 3-node S3 `warp` PUT/GET workload                  | `benchmarks/profiles/s3-compat-compare-*`, cluster logs             |
| `make bench-s3-compat-compare`                | `GrainFS` vs native MinIO/RustFS S3 `warp` workload | `benchmarks/profiles/s3-compat-compare-*`                           |
| `make bench-iceberg-table`                    | Single-node Iceberg REST Catalog via `warp iceberg` | `benchmarks/profiles/iceberg-table-*`                               |
| `make bench-iceberg-table-cluster`            | Multi-node Iceberg REST Catalog via `warp iceberg`  | `benchmarks/profiles/iceberg-table-*`, cluster logs                 |
| `make bench-nfs`                              | Single-node NFS fio profile via Colima             | `benchmarks/profiles/nfs-*/fio_output.txt`                          |
| `make bench-nfs-cluster`                      | Multi-node NFS fio profile                         | `benchmarks/profiles/nfs-*`                                         |
| `make bench-nfs-multi`                        | Multi-bucket NFS export profile                    | `benchmarks/profiles/nfs-multi-*`                                   |
| `make bench-nbd`                              | Single-node NBD fio profile via Colima             | `benchmarks/profiles/nbd-*`                                         |
| `make bench-nbd-cluster`                      | Multi-node NBD fio profile                         | `benchmarks/profiles/nbd-*`                                         |
| `make bench-9p`                               | Single-node 9P profile                             | `benchmarks/profiles/9p-*`                                          |
| `make bench-9p-cluster`                       | Multi-node 9P profile                              | `benchmarks/profiles/9p-*`                                          |
| `make bench-fuse-s3-colima`                   | rclone direct S3 vs rclone mount throughput        | Go benchmark output                                                 |

## Result Interpretation

Use these metrics consistently:

- Throughput: MiB/s or objects/s, with payload size stated.
- Latency: p50, p95, p99, and max when available.
- Resource use: CPU, RSS, heap, goroutine count, and file descriptors when available.
- Cluster behavior: ingress node, owner node, forwarded bytes, leader-hint retries,
  meta-index proposal count, and slowest shard stage when tracing is enabled.
- Error rate: all non-2xx protocol responses and transport/client errors.
  Benchmark runners should exit non-zero when any measured request fails.

For fio-based protocol benchmarks, record workload mode, block size, queue depth,
number of jobs, runtime, mount options, and client kernel/tool versions.

## Comparable S3 Protocol

Use this protocol before publishing `GrainFS` vs RustFS vs MinIO results.

| Step          | Requirement                                                          |
| ------------- | -------------------------------------------------------------------- |
| Build         | Use released or commit-pinned binaries/images for all systems.       |
| Host          | Run on the same host class, isolated from unrelated load.            |
| Storage       | Use equivalent disk layout and fresh data directories.               |
| Durability    | Match the closest durability profile possible; document mismatches.  |
| Auth          | Use signed S3 requests for all systems.                              |
| Workload      | Run the same object sizes, concurrency, duration, and operation mix. |
| Warmup        | State whether caches are cold, warm, or explicitly dropped.          |
| Observability | Capture CPU/RSS, process logs, and raw benchmark artifacts.          |
| Repetition    | Run at least three iterations and report median plus spread.         |

RustFS and MinIO are valid comparison anchors. Do not claim parity until this
document or an adjacent report links a reproducible run.

`benchmarks/bench_s3_compat_compare.sh` implements the local same-host version
of this protocol with MinIO `warp` as the official comparison tool. It prefers
native binaries or explicit external endpoints and skips unusable local builds,
such as license-gated MinIO AIStor binaries. Set
`MINIO_BIN=$HOME/go/bin/minio` or another explicit binary path when the default
`minio` on `PATH` is not benchmarkable. The script reports PUT and GET
separately from `warp analyze`, using the same signed S3 requests, object size,
concurrency, duration, bucket lookup mode, `roundrobin` host selection, and
warm-read rule for every target. Cluster endpoints are passed as comma-separated
host lists to `warp`. The script accepts the full warp op surface through
`WARP_OPS`: `put`, `get`, `delete`, `mixed`, `list`, `stat`, `versioned`,
`retention`, `multipart`, `multipart-put`, `append`. Multipart workloads pass
`--part.size` (warp does not accept `--obj.size` for those subcommands);
`delete` auto-raises `--objects` to `concurrent × batch × 4` so the warp
minimum-object guard is satisfied; and each op runs in its own bucket
(`warp-<target>-<op>`) so a later op does not seed against the prior op's
data. GrainFS cluster runs default to 4 nodes, and
`TARGETS=minio-cluster,rustfs-cluster` boots local 4-node distributed baselines
when native MinIO and RustFS binaries are available. Note that a freshly
bootstrapped GrainFS cluster needs roughly 30 to 45 seconds for the
multipart-listing capability evidence to propagate through gossip; set
`CLUSTER_WARMUP_SLEEP=45` before running multipart workloads (see TODOS for
the capability-ready probe follow-up). k6-based S3 benchmark scripts have
been removed; S3 performance claims should use `warp`.

Iceberg catalog benchmarks also use MinIO `warp` through
`make bench-iceberg-table` and `make bench-iceberg-table-cluster`. The default
Iceberg profile is `catalog-mixed` with views and update operations disabled so
the workload runs cleanly against the current table catalog surface; commit-heavy
update benchmarking is tracked separately.

## Latest Local Result

This section keeps only the latest comparable S3 results. Older benchmark runs
remain in their raw artifact directories and prior commits, not in this
reference page.

These snapshots were captured on the local Apple M3 loopback setup with signed
S3 requests, 64 KiB objects, concurrency 32, `warp`, `--host-select
roundrobin`, and `--noclear`. Each target ran as a local 4-node cluster. GET is
a warm-read pass over objects kept from the preceding PUT pass. MinIO and RustFS
were measured once in the final comparison. `GrainFS` ran with at-rest
encryption and default Iceberg audit enabled.

| Target    | Commit / build | PUT MiB/s | PUT obj/s | PUT errors | GET MiB/s | GET obj/s | GET errors | Raw artifacts |
| --------- | -------------- | --------: | --------: | ---------: | --------: | --------: | ---------: | ------------- |
| `GrainFS` | `b8d635e7`     |     65.32 |   1045.10 |          0 |    244.26 |   3908.11 |          0 | `benchmarks/profiles/fair4-c32-20260519-055104/grainfs-cluster` |
| MinIO     | local run      |     41.37 |    661.85 |          0 |     95.52 |   1528.30 |          0 | `benchmarks/profiles/fair4-c32-20260519-055104/minio-cluster` |
| RustFS    | local run      |     18.50 |    296.08 |          0 |     48.20 |    771.14 |          0 | `benchmarks/profiles/fair4-c32-20260519-055104/rustfs-cluster` |

Observed deltas:

- `GrainFS` PUT throughput was 1.58x the MinIO PUT baseline and 3.53x the
  RustFS PUT baseline.
- `GrainFS` GET throughput was 2.56x the MinIO GET baseline and 5.07x the
  RustFS GET baseline.
- Raw summary:
  `benchmarks/profiles/fair4-c32-20260519-055104/summary.md`.

The official comparison uses `warp`; the old k6 mixed workload has been removed.

## Updating Results

Replace the latest result above when publishing a new comparable benchmark.
Record:

```text
Date:
Commit:
Binary/image:
Host:
Command:
Environment:
Durability profile:
Raw artifacts:
Summary:
Known caveats:
```

Avoid keeping historical benchmark tables in this document. Summaries should
point back to the original JSON, fio output, trace, or pprof files.
