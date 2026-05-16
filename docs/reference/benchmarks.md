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
| `make bench`                                  | Single-node S3 object PUT/GET/DELETE               | `benchmarks/report.json`                                            |
| `make bench-cluster`                          | Multi-node S3 object benchmark                     | `benchmarks/report.json`, cluster logs                              |
| `make bench-s3-compat-compare`                | `GrainFS` vs native MinIO/RustFS S3 `warp` workload  | `benchmarks/profiles/s3-compat-compare-*`                           |
| `PUT_MATRIX=1 make bench-cluster`             | Cluster PUT matrix by ingress port and object size | `benchmarks/put-matrix-port<port>-<small\|large>.json`              |
| `PUT_MATRIX=1 PUT_TRACE=1 make bench-cluster` | PUT matrix plus per-node stage tracing             | owner-only JSONL traces and `benchmarks/put_trace_report.js` output |
| `make bench-profile`                          | Multi-node S3 benchmark with pprof                 | `/tmp/grainfs-bench-*.out`                                          |
| `make bench-topology-get`                     | Topology-aware GET profile                         | topology GET report and pprof files                                 |
| `make bench-topology-get-matrix`              | Topology-aware GET matrix                          | matrix reports and pprof files                                      |
| `make bench-iceberg-table`                    | Single-node Iceberg REST Catalog table API via Go runner | `benchmarks/iceberg_table_report.json`                              |
| `make bench-iceberg-table-cluster`            | Multi-node Iceberg table API via Go runner               | `benchmarks/iceberg_table_report.json`                              |
| `make bench-nfs`                              | Single-node NFS fio profile via Colima             | `benchmarks/profiles/nfs-*/fio_output.txt`                          |
| `make bench-nfs-cluster`                      | Multi-node NFS fio profile                         | `benchmarks/profiles/nfs-*`                                         |
| `make bench-nfs-multi`                        | Multi-bucket NFS export profile                    | `benchmarks/profiles/nfs-multi-*`                                   |
| `make bench-nbd`                              | Single-node NBD fio profile via Colima             | `benchmarks/profiles/nbd-*`                                         |
| `make bench-nbd-cluster`                      | Multi-node NBD fio profile                         | `benchmarks/profiles/nbd-*`                                         |
| `make bench-9p`                               | Single-node 9P profile                             | `benchmarks/profiles/9p-*`                                          |
| `make bench-9p-cluster`                       | Multi-node 9P profile                              | `benchmarks/profiles/9p-*`                                          |
| `make bench-fuse-s3-colima`                   | rclone direct S3 vs rclone mount throughput        | Go benchmark output                                                 |
| `make bench-directio-s3`                      | Direct I/O S3 benchmark                            | script output                                                       |

## Result Interpretation

Use these metrics consistently:

- Throughput: MiB/s or objects/s, with payload size stated.
- Latency: p50, p95, p99, and max when available.
- Resource use: CPU, RSS, heap, goroutine count, and file descriptors when available.
- Cluster behavior: ingress node, owner node, forwarded bytes, leader-hint retries,
  meta-index proposal count, and slowest shard stage when PUT tracing is enabled.
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
host lists to `warp`. The previous k6 mixed workload is not used for
GrainFS/MinIO/RustFS comparison claims because it is not the shared benchmark
surface.

## Latest Local Result

This section keeps only the latest comparable S3 results. Older benchmark runs
remain in their raw artifact directories and prior commits, not in this
reference page.

These snapshots were captured on the local Apple M3 loopback setup with signed
S3 requests, 64 KiB objects, concurrency 16, `warp`, `--host-select
roundrobin`, and `--noclear`. PUT and GET used the same bucket per target; GET
is therefore a warm-read pass over the objects kept from the preceding PUT
pass. MinIO and RustFS were measured once per mode in the final comparison.
`GrainFS` ran with at-rest encryption and default Iceberg audit enabled.
The 3-node `GrainFS` run below used the tree later committed as `24245071`;
the raw artifact summary records the pre-commit dirty build as
`da36ef39-dirty`.

### Single-Node

| Target    | Commit / build | PUT MiB/s | PUT obj/s | PUT errors | GET MiB/s | GET obj/s | GET errors | Raw artifacts |
| --------- | -------------- | --------: | --------: | ---------: | --------: | --------: | ---------: | ------------- |
| `GrainFS` | `da36ef39`     |    517.36 |   8277.82 |          0 |   1212.67 |  19402.70 |          0 | `benchmarks/profiles/review-impact-single-grainfs-20260516-171005/grainfs-single` |
| MinIO     | local run      |    252.88 |   4046.10 |          0 |   1074.01 |  17184.13 |          0 | `benchmarks/profiles/s3-compat-warp-single-official-20260516-163404/minio` |
| RustFS    | local run      |    225.43 |   3606.93 |          0 |    500.35 |   8005.57 |          0 | `benchmarks/profiles/s3-compat-warp-single-official-20260516-163404/rustfs` |

Observed deltas:

- `GrainFS` PUT throughput was 2.05x the MinIO PUT baseline and 2.29x the
  RustFS PUT baseline.
- `GrainFS` GET throughput was 1.13x the MinIO GET baseline and 2.42x the
  RustFS GET baseline.
- Raw summary:
  `benchmarks/profiles/review-impact-single-grainfs-20260516-171005/summary.md`.

### 3-Node Cluster

| Target    | Commit / build | PUT MiB/s | PUT obj/s | PUT errors | GET MiB/s | GET obj/s | GET errors | Raw artifacts |
| --------- | -------------- | --------: | --------: | ---------: | --------: | --------: | ---------: | ------------- |
| `GrainFS` | `24245071`     |    103.22 |   1651.44 |          0 |    325.85 |   5213.56 |          0 | `benchmarks/profiles/review-impact-cluster-grainfs-nosync-20260516-171937` |
| MinIO     | local run      |     47.05 |    752.77 |          0 |    296.84 |   4749.39 |          0 | `benchmarks/profiles/s3-compat-cluster-warp-minio-rebaseline-20260516-155338` |
| RustFS    | local run      |     36.31 |    580.88 |          0 |    105.88 |   1694.14 |          0 | `benchmarks/profiles/s3-compat-cluster-warp-rustfs-rebaseline-20260516-155512` |

Observed deltas:

- `GrainFS` PUT throughput was 2.19x the MinIO PUT baseline and 2.84x the
  RustFS PUT baseline.
- `GrainFS` GET throughput was 1.10x the MinIO GET baseline and 3.08x the
  RustFS GET baseline.
- Raw summary:
  `benchmarks/profiles/review-impact-cluster-grainfs-nosync-20260516-171937/summary.md`.

The official comparison no longer uses the old k6 mixed workload.

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
