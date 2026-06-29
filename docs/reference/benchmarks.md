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
- Archived result artifact reference when one exists; do not rely on ignored
  local-only paths as publishable evidence.
- Date of run.

Do not compare `GrainFS` with RustFS, MinIO, or any other S3-compatible store
unless all systems run on the same host class, with comparable durability,
object sizes, concurrency, and cold/warm-cache rules.

## Current Publishable Path

Use the GCP encrypted benchmark paths for GrainFS vs MinIO performance claims.
They build a committed GrainFS `NEW_REF` on a Linux client VM, run GrainFS and
MinIO on the same GCP VM class, and drive both from the in-network client with
signed MinIO `warp` requests.

The single-node path starts GrainFS on `node-0` with at-rest encryption and
single-node MinIO with SSE-S3 auto-encryption:

Keep `RESULT_DIR` exported across subcommands so all runs land in one artifact
directory:

```bash
export PROJECT=grainfs
export ZONE=asia-northeast3-a
export PREFIX=gr-single
export NODE_COUNT=1
export NEW_REF=HEAD
export RUNS=3
export RESULT_DIR="$PWD/benchmarks/profiles/gcp-single-$(date +%Y%m%d-%H%M%S)"
export WARP_OPS=put,get,stat
export WARP_OBJ_SIZE=10MiB
export WARP_CONCURRENT=32
export WARP_DURATION=1m
export WARP_OBJECTS=4096

./benchmarks/gcp/bench_gcp_cluster.sh up
./benchmarks/gcp/bench_gcp_cluster.sh build
for i in $(seq 1 "$RUNS"); do
  ./benchmarks/gcp/bench_gcp_cluster.sh single "$i"
  ./benchmarks/gcp/bench_gcp_cluster.sh minio "$i"
done
./benchmarks/gcp/bench_gcp_cluster.sh single-verdict | tee "$RESULT_DIR/single-verdict.txt"
./benchmarks/gcp/bench_gcp_cluster.sh down
```

Artifacts:

- `single/run<N>/warp-results.tsv`: GrainFS throughput and latency rows.
- `minio/run<N>/warp-results.tsv`: MinIO throughput and latency rows.
- `single/run<N>/pprof/`: GrainFS CPU, heap, allocs, goroutine, mutex, and block profiles.
- `single-verdict.txt`: side-by-side summary captured from `single-verdict`.

### Latest GCP Single-Node Encrypted Result

Captured on 2026-06-30 KST in `asia-northeast3-a` with `n2-standard-4` VMs,
10 MiB object size, 2048 total objects, concurrency 32, 1 minute per operation,
signed S3 requests, and 0 errors. `GrainFS` ran with XAES-256-GCM at-rest
encryption; MinIO ran with SSE-S3 auto-encryption.

| Target    | PUT MiB/s | GET MiB/s | vs MinIO PUT | vs MinIO GET |
| --------- | --------: | --------: | -----------: | -----------: |
| `GrainFS` |    215.50 |    437.02 |        1.03x |        0.92x |
| MinIO     |    209.77 |    472.65 |        1.00x |        1.00x |

The same measurement attempt produced two additional valid MinIO runs
(`PUT=212.37/210.74 MiB/s`, `GET=669.94/716.38 MiB/s`), but GrainFS repeat
runs 2 and 3 failed during `warp` preparation with `Access Denied` and empty
TSV files. Those failed GrainFS arms are excluded from the table above.

### GCP Cluster Encrypted Path

The cluster path starts a 4-node GrainFS cluster with at-rest encryption and a
4-node distributed MinIO cluster with SSE-S3 auto-encryption. Use the same
workload shape for both targets:

```bash
export PROJECT=grainfs
export ZONE=asia-northeast3-a
export PREFIX=gr-cluster
export NODE_COUNT=4
export NEW_REF=HEAD
export RUNS=1
export RESULT_DIR="$PWD/benchmarks/profiles/gcp-cluster-$(date +%Y%m%d-%H%M%S)"
export WARP_OPS=put,get
export WARP_OBJ_SIZE=10MiB
export WARP_CONCURRENT=32
export WARP_DURATION=1m
export WARP_OBJECTS=2048

./benchmarks/gcp/bench_gcp_cluster.sh up
./benchmarks/gcp/bench_gcp_cluster.sh build
for i in $(seq 1 "$RUNS"); do
  ./benchmarks/gcp/bench_gcp_cluster.sh grainfs-cluster "$i"
  ./benchmarks/gcp/bench_gcp_cluster.sh minio-cluster "$i"
done
./benchmarks/gcp/bench_gcp_cluster.sh cluster-minio-verdict | tee "$RESULT_DIR/cluster-minio-verdict.txt"
./benchmarks/gcp/bench_gcp_cluster.sh down
```

Artifacts:

- `grainfs-cluster/run<N>/warp-results.tsv`: GrainFS cluster throughput rows.
- `minio-cluster/run<N>/warp-results.tsv`: distributed MinIO throughput rows.
- `grainfs-cluster/run<N>/pprof/`: per-node GrainFS profiles when `CLUSTER_PPROF=1`.
- `cluster-minio-verdict.txt`: side-by-side summary captured from `cluster-minio-verdict`.

### Latest GCP Cluster Encrypted Result

Captured on 2026-06-30 KST in `asia-northeast3-a` with one `n2-standard-4`
client VM and four `n2-standard-4` storage VMs, 10 MiB object size, 2048 total
objects, concurrency 32, 1 minute per operation, signed S3 requests,
round-robin host selection, warm GET over the preceding PUT objects, and
0 errors.

| Target            | PUT MiB/s | PUT p50 ms | PUT p99 ms | GET MiB/s | GET p50 ms | GET p99 ms | vs MinIO PUT | vs MinIO GET |
| ----------------- | --------: | ---------: | ---------: | --------: | ---------: | ---------: | -----------: | -----------: |
| `GrainFS` cluster |    341.17 |      995.5 |     1411.0 |   2380.79 |      118.7 |      430.5 |        0.73x |        1.07x |
| MinIO distributed |    468.56 |      690.2 |      881.2 |   2216.49 |      131.3 |      383.0 |        1.00x |        1.00x |

Interpretation: GrainFS cluster write throughput is 0.73x of distributed
MinIO under this workload; read throughput is 1.07x of MinIO. GrainFS read TTFB
was higher than MinIO in the raw `warp analyze` output (`median 40 ms` vs
`25 ms`), so the GET win is throughput, not first-byte latency.

## Existing Benchmark Targets

| Target                                       | Scope                                                     | Primary artifacts                                       |
| -------------------------------------------- | --------------------------------------------------------- | ------------------------------------------------------- |
| `benchmarks/gcp/bench_gcp_cluster.sh single` | GCP single-node GrainFS `warp` workload with pprof        | `benchmarks/profiles/gcp-single-*/single/run<N>/`       |
| `benchmarks/gcp/bench_gcp_cluster.sh minio`  | GCP single-node MinIO SSE-S3 `warp` workload              | `benchmarks/profiles/gcp-single-*/minio/run<N>/`        |
| `benchmarks/gcp/bench_gcp_cluster.sh grainfs-cluster` | GCP cluster GrainFS `warp` workload               | `benchmarks/profiles/gcp-cluster-*/grainfs-cluster/run<N>/` |
| `benchmarks/gcp/bench_gcp_cluster.sh minio-cluster` | GCP distributed MinIO SSE-S3 `warp` workload       | `benchmarks/profiles/gcp-cluster-*/minio-cluster/run<N>/` |
| `make bench`                                 | Local single-node S3 `warp` smoke/regression workload     | `benchmarks/profiles/s3-compat-compare-*`               |
| `make bench-cluster`                         | Local cluster S3 `warp` smoke/regression workload         | `benchmarks/profiles/s3-compat-compare-*`, cluster logs |
| `make bench-s3-compat-compare`               | Local same-host GrainFS vs native MinIO/RustFS comparison | `benchmarks/profiles/s3-compat-compare-*`               |

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

For local smoke/regression results, run benchmark scripts with
`BENCH_STRICT_HOST=1` when host contamination matters. The S3 warp scripts write
`host-preflight.txt` into the raw artifact directory and fail before starting
benchmark backends when the host already has `grainfs serve` processes or the
benchmark filesystem is at least 90 percent full. The same preflight records
`load1`, `cpu_count`, `load_per_cpu`, and `max_load_per_cpu`; strict mode also
fails when `load_per_cpu` exceeds `BENCH_MAX_LOAD_PER_CPU` (default `1.0`).
Without strict mode, those conditions are warning-only and the resulting
throughput/RSS rows must be treated as contaminated unless the extra load is
intentional and documented.

RustFS and MinIO are valid comparison anchors. Do not claim parity until this
document or an adjacent report links a reproducible run.

`benchmarks/bench_s3_compat_compare.sh` implements the local same-host smoke
version of this protocol with MinIO `warp` as the official comparison tool. It
prefers native binaries or explicit external endpoints and skips unusable local
builds, such as license-gated MinIO AIStor binaries. Set
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

## Historical Local Result

This section keeps the last local Apple M3 loopback comparison for historical
context. Do not use these same-host local numbers as the current publishable
GrainFS vs MinIO benchmark; use the GCP single-node encrypted path above.
Older benchmark runs remain in their raw artifact directories and prior
commits, not in this reference page.

These snapshots were captured on the local Apple M3 loopback setup with signed
S3 requests, 64 KiB objects, concurrency 32, `warp`, `--host-select
roundrobin`, and `--noclear`. Each target ran in local single-node mode. GET is
a warm-read pass over objects kept from the preceding PUT pass. `GrainFS` ran
with at-rest encryption and S3-only benchmark flags:
`--block-cache-size=0 --shard-cache-size=0`.

| S3 op         | MinIO MiB/s | MinIO obj/s | MinIO errors | MinIO RSS MiB | RustFS MiB/s | RustFS obj/s | RustFS errors | RustFS RSS MiB | GrainFS MiB/s | GrainFS obj/s | GrainFS errors | GrainFS RSS MiB | GrainFS artifact                                                                                 |
| ------------- | ----------: | ----------: | -----------: | ------------: | -----------: | -----------: | ------------: | -------------: | ------------: | ------------: | -------------: | --------------: | ------------------------------------------------------------------------------------------------ |
| put           |      175.14 |     2802.27 |            0 |        796.30 |        26.62 |       425.95 |             0 |         106.58 |        548.30 |       8772.82 |              0 |          601.22 | `benchmarks/profiles/grainfs-single-put-after-small-badger-options-20260520-145417`              |
| get           |      457.81 |     7325.01 |            0 |        919.20 |       437.77 |      7004.32 |             0 |         213.28 |       1849.34 |      29589.49 |              0 |          767.03 | `benchmarks/profiles/grainfs-single-get-after-small-badger-options-20260520-145504`              |
| delete        |        0.00 |     1968.80 |            0 |        533.90 |         0.00 |      2835.91 |             0 |         232.11 |          0.00 |      17964.91 |              0 |          460.70 | `benchmarks/profiles/grainfs-single-delete-after-small-vlog-file-20260520-145342`                |
| mixed         |      126.89 |     2030.20 |            0 |        687.70 |       163.62 |      2617.84 |             0 |         258.25 |        176.17 |       2818.79 |              0 |          251.75 | `benchmarks/profiles/grainfs-single-mixed-after-small-badger-options-20260520-145750`            |
| list          |        0.00 |    31285.46 |            0 |       1158.50 |         0.00 |     11869.98 |             0 |         605.75 |          0.00 |     434233.02 |              0 |          150.38 | `benchmarks/profiles/grainfs-single-list-after-small-badger-options-20260520-145950`             |
| stat          |        0.00 |    14601.34 |            0 |        727.20 |         0.00 |      9428.66 |             0 |         187.55 |          0.00 |      58557.94 |              0 |          126.72 | `benchmarks/profiles/grainfs-single-stat-after-small-badger-options-20260520-150109`             |
| versioned     |      129.32 |     2069.14 |            0 |        602.00 |        75.57 |      1209.13 |             0 |         407.53 |        182.82 |       2925.17 |              0 |          486.23 | `benchmarks/profiles/grainfs-single-versioned-after-stream-shard-pack-20260520-150840`           |
| retention     |        0.00 |     6546.51 |            0 |        646.83 |         0.00 |      3208.87 |             0 |         367.53 |          0.00 |      19336.57 |              0 |          280.86 | `benchmarks/profiles/grainfs-single-retention-after-stream-shard-pack-20260520-151345`           |
| multipart     |     3245.85 |      649.17 |            0 |       1101.73 |      3622.07 |       724.41 |             0 |         475.05 |       3986.73 |        797.35 |              0 |          675.98 | `benchmarks/profiles/grainfs-single-multipart-after-head-metadata-cache-bounded-20260520-163449` |
| multipart-put |      321.18 |       64.24 |            0 |       1579.06 |       614.95 |       122.99 |             0 |         539.89 |        804.95 |        160.99 |              0 |          879.59 | `benchmarks/profiles/grainfs-single-multipart-put-after-complete-8m-limit24-20260520-170108`     |
| append        |         n/a |         n/a |       146687 |        663.12 |          n/a |          n/a |         78801 |         119.12 |         78.39 |       1254.28 |              0 |          326.50 | `benchmarks/profiles/grainfs-single-append-initial-20260520-170436`                              |

MinIO and RustFS append runs returned errors, so they are not valid correctness
baselines for append throughput. `warp append` exercises S3 Express append
semantics; the local OSS MinIO binary rejects `x-amz-write-offset-bytes` outside
S3 Express mode and cannot provide a valid append baseline without an AIStor/S3
Express endpoint. Their raw artifact directories still contain the failed run
output, but the throughput cells are intentionally not published as comparable
baseline numbers. GrainFS append is reported as a best-effort 0-error result.

Observed S3 deltas:

- `GrainFS` PUT throughput was 3.13x the MinIO PUT baseline and 20.60x the
  RustFS PUT baseline, with lower RSS than MinIO.
- `GrainFS` GET throughput was 4.04x the MinIO GET baseline and 4.22x the
  RustFS GET baseline, with lower RSS than MinIO.
- `GrainFS` passed the measured non-append S3 throughput gates with 0 errors
  and lower RSS than MinIO.

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
