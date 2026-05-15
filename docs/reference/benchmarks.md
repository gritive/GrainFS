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

| Target | Scope | Primary artifacts |
| --- | --- | --- |
| `make bench` | Single-node S3 object PUT/GET/DELETE | `benchmarks/report.json` |
| `make bench-cluster` | Multi-node S3 object benchmark | `benchmarks/report.json`, cluster logs |
| `PUT_MATRIX=1 make bench-cluster` | Cluster PUT matrix by ingress port and object size | `benchmarks/put-matrix-port<port>-<small\|large>.json` |
| `PUT_MATRIX=1 PUT_TRACE=1 make bench-cluster` | PUT matrix plus per-node stage tracing | owner-only JSONL traces and `benchmarks/put_trace_report.js` output |
| `make bench-profile` | Multi-node S3 benchmark with pprof | `/tmp/grainfs-bench-*.out` |
| `make bench-topology-get` | Topology-aware GET profile | topology GET report and pprof files |
| `make bench-topology-get-matrix` | Topology-aware GET matrix | matrix reports and pprof files |
| `make bench-iceberg-table` | Single-node Iceberg REST Catalog table API | `benchmarks/iceberg_table_report.json` |
| `make bench-iceberg-table-cluster` | Multi-node Iceberg table API | `benchmarks/iceberg_table_report.json` |
| `make bench-nfs` | Single-node NFS fio profile via Colima | `benchmarks/profiles/nfs-*/fio_output.txt` |
| `make bench-nfs-cluster` | Multi-node NFS fio profile | `benchmarks/profiles/nfs-*` |
| `make bench-nfs-multi` | Multi-bucket NFS export profile | `benchmarks/profiles/nfs-multi-*` |
| `make bench-nbd` | Single-node NBD fio profile via Colima | `benchmarks/profiles/nbd-*` |
| `make bench-nbd-cluster` | Multi-node NBD fio profile | `benchmarks/profiles/nbd-*` |
| `make bench-9p` | Single-node 9P profile | `benchmarks/profiles/9p-*` |
| `make bench-9p-cluster` | Multi-node 9P profile | `benchmarks/profiles/9p-*` |
| `make bench-fuse-s3-colima` | rclone direct S3 vs rclone mount throughput | Go benchmark output |
| `make bench-directio-s3` | Direct I/O S3 benchmark | script output |

## Result Interpretation

Use these metrics consistently:

- Throughput: MiB/s or objects/s, with payload size stated.
- Latency: p50, p95, p99, and max when available.
- Resource use: CPU, RSS, heap, goroutine count, and file descriptors when available.
- Cluster behavior: ingress node, owner node, forwarded bytes, leader-hint retries,
  meta-index proposal count, and slowest shard stage when PUT tracing is enabled.
- Error rate: all non-2xx S3 responses and transport/client errors.

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

## Current Local Snapshots

This table lists values already documented in the repository.

| Scenario                                                               | Result                                                                                                            |
| ---------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| FUSE-over-S3, 64 MiB payload, Apple M3, Colima loopback, 3-run average | Direct S3 write 96.8 MB/s, direct S3 read 108.0 MB/s, rclone mount write 106.7 MB/s, rclone mount read 107.3 MB/s |
| `GrainFS` vs RustFS vs MinIO S3 object benchmark                         | Pending reproducible run                                                                                          |
| CI regression threshold                                                | Not yet enforced                                                                                                  |

## Adding Results

Add a dated subsection or report file with:

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

Avoid replacing raw artifacts with prose summaries. Summaries should point back
to the original JSON, fio output, trace, or pprof files.
