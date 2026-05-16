# GrainFS Benchmarks

For benchmark principles, result interpretation, and RustFS/MinIO comparison
rules, see [`docs/reference/benchmarks.md`](../docs/reference/benchmarks.md).

## S3 Cluster PUT Matrix

`make bench-cluster` starts a local 3-node cluster and runs the standard S3
object benchmark. Set `PUT_MATRIX=1` to run a PUT-only matrix across all three
S3 ports with small and large objects:

```bash
PUT_MATRIX=1 make bench-cluster
# results: benchmarks/put-matrix-port<port>-<small|large>.json
```

Tune the matrix with:

```bash
PUT_SMALL_KB=64 PUT_LARGE_KB=8192 PUT_MATRIX_ITERATIONS=25 PUT_MATRIX=1 make bench-cluster
```

The matrix warms each port and object-size path before measurement by default,
then clears PUT traces so the report excludes warmup traffic. Disable that with
`PUT_MATRIX_WARMUP=0`, or tune it with `PUT_MATRIX_WARMUP_ITERATIONS` and
`PUT_MATRIX_WARMUP_ROUNDS`.

Set `PUT_TRACE=1` with the matrix to write per-node PUT trace JSONL files under
the benchmark temp directory and print a dominant-stage report:

```bash
PUT_MATRIX=1 PUT_TRACE=1 make bench-cluster
```

The scripts create trace files with mode `0600` because they include raw bucket
and object keys. The report groups requests by ingress, size class, and forwarding mode,
then summarizes forwarded bytes, leader-hint retries, meta-index proposal
counts, and the slowest shard stage.

## S3-Compatible Comparison

`make bench-s3-compat-compare` runs the official local S3-compatible comparison
with MinIO `warp` against GrainFS single-node and any local native MinIO/RustFS
binaries available on `PATH`. Set `MINIO_BIN`, `RUSTFS_BIN`, `MINIO_URL`, or
`RUSTFS_URL` to point at specific released builds or already-running endpoints.

```bash
make bench-s3-compat-compare
# results: benchmarks/profiles/s3-compat-compare-<timestamp>/summary.md
```

The macOS `minio` command may resolve to MinIO AIStor builds that deny S3
operations without a license. The comparison script detects that case and skips
MinIO rather than recording unusable 403 results. Use a benchmarkable native
MinIO binary through `MINIO_BIN=/path/to/minio`.

```bash
MINIO_BIN=$HOME/go/bin/minio make bench-s3-compat-compare
```

The comparison reports PUT and GET as separate rows, using the same signed S3
requests, object size, concurrency, duration, and lookup mode for every target.
The default is a short local baseline: 64 KiB objects, concurrency 16, 30s per
operation, `WARP_HOST_SELECT=roundrobin`, and `WARP_NOCLEAR=1` so GET measures
a warm-read pass over the objects written by the preceding PUT pass.

```bash
WARP_OPS=put,get WARP_OBJ_SIZE=20MiB WARP_CONCURRENT=32 WARP_DURATION=1m make bench-s3-compat-compare
```

For cluster endpoints, pass a comma-separated host list through the matching
`*_URL` variable; the script strips URL schemes before passing the host list to
`warp`.

Failures are recorded as raw `warp.out` and `analyze.out` artifacts under the
profile directory. The old k6 mixed workload is no longer used for
GrainFS/MinIO/RustFS comparison claims because its write-heavy operation mix and
client implementation are not the shared public benchmark surface.

## Iceberg Table API

`make bench-iceberg-table` and `make bench-iceberg-table-cluster` run the
Iceberg REST Catalog table lifecycle benchmark with a Go runner, not k6. The
shell scripts still start GrainFS, bootstrap IAM credentials, optionally collect
pprof profiles, and write `benchmarks/iceberg_table_report.json`.

Tune the workload with:

```bash
VUS=16 DURATION=1m make bench-iceberg-table
VUS=16 DURATION=1m make bench-iceberg-table-cluster
```

## NFS Multi-Bucket Export Baseline

The NFSv4 server now uses explicit bucket exports. Single-bucket benchmark runs
therefore create a bucket, register it as an export, and mount `host:/<bucket>`.

Prerequisites:

- Colima running with at least 4 vCPUs for multi-export runs
- `fio` installed inside the Colima VM
- `jq` installed on the macOS host for summary parsing

```bash
make bench-nfs
# results: benchmarks/profiles/nfs-<workload>-<timestamp>/fio_output.txt
```

Compare single-bucket throughput with the pre-multi-export baseline manually.
The target is within +/-10 percent for equivalent workload settings; this PR
does not add an automatic baseline gate.

```bash
make bench-nfs-multi
# results: benchmarks/profiles/nfs-multi-<timestamp>/
```

The multi-export run creates four buckets by default and runs one fio worker
per bucket, for four total fio workers. It records:

- `summary.txt`
- `fio_b<i>.log`
- `cpu.pprof`
- `heap.pprof`
- `pseudo_root_lat_ms.txt`

Tune the workload with:

```bash
NUM_BUCKETS=8 FIO_WORKERS_PER_BUCKET=8 READDIR_SAMPLES=1000 make bench-nfs-multi
```

Use `go tool pprof -top -focus='LockPath' cpu.pprof` to inspect export-level
contention. Measure pseudo-root latency through a separate `host:/` mount and
bucket throughput through `host:/bench-<i>` mounts.
