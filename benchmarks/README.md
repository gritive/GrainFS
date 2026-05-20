# GrainFS Benchmarks

For benchmark principles, result interpretation, and RustFS/MinIO comparison
rules, see [`docs/reference/benchmarks.md`](../docs/reference/benchmarks.md).

## S3-Compatible Comparison

`make bench`, `make bench-cluster`, and `make bench-s3-compat-compare` run the
official S3 workload with MinIO `warp`. `make bench` targets a local GrainFS
single-node server, `make bench-cluster` targets a local GrainFS 4-node cluster,
and `make bench-s3-compat-compare` compares GrainFS single-node with any native
MinIO/RustFS binaries available on `PATH`. The comparison script can also boot
local 4-node MinIO and RustFS clusters with `TARGETS=minio-cluster` and
`TARGETS=rustfs-cluster`. Set `MINIO_BIN`, `RUSTFS_BIN`, `MINIO_URL`,
`RUSTFS_URL`, `MINIO_CLUSTER_URL`, or `RUSTFS_CLUSTER_URL` to point at specific
released builds or already-running endpoints.

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

Include batch delete measurements with:

```bash
WARP_OPS=put,get,delete WARP_OBJECTS=4096 WARP_DELETE_BATCH=100 make bench-cluster
```

`WARP_OPS` accepts the full warp op surface: `put`, `get`, `delete`, `mixed`,
`list`, `stat`, `versioned`, `retention`, `multipart`, `multipart-put`,
`append`. Each op runs in its own bucket (`warp-<target>-<op>`) so the previous
op does not seed the next one. Multipart workloads use `--part.size`
automatically. To run a 4-node GrainFS cluster sweep with multipart support, allow
the gossip-propagated `multipart_listing_v1` capability evidence enough time
to settle:

```bash
CLUSTER_WARMUP_SLEEP=45 \
WARP_OPS=put,get,delete,mixed,list,stat,versioned,multipart,multipart-put \
  make bench-cluster
```

Run local 4-node comparison baselines with:

```bash
TARGETS=minio-cluster,rustfs-cluster WARP_OPS=put,get make bench-s3-compat-compare
```

For external cluster endpoints, pass a comma-separated host list through
`GRAINFS_CLUSTER_URL` or the matching `*_URL` variable; the script strips URL
schemes before passing the host list to `warp`.

Failures are recorded as raw `warp.out` and `analyze.out` artifacts under the
profile directory. k6-based S3 benchmark scripts have been removed; S3
performance claims should use `warp`.

Every comparison run writes `host-preflight.txt` in the profile directory and
adds summary warnings when the host already has `grainfs serve` processes or
the benchmark filesystem is at least 90 percent full. The same file records
`load1`, `cpu_count`, `load_per_cpu`, and `max_load_per_cpu`; strict mode fails
when `load_per_cpu` exceeds `BENCH_MAX_LOAD_PER_CPU` (default `1.0`). Use
strict mode for publishable runs so contaminated host state fails before any
benchmark server starts:

```bash
BENCH_STRICT_HOST=1 WARP_OPS=put,get make bench-s3-compat-compare
```

## Iceberg Table API

`make bench-iceberg-table` and `make bench-iceberg-table-cluster` run MinIO
`warp iceberg` against GrainFS. The shell scripts start GrainFS, bootstrap IAM
credentials, optionally collect pprof profiles, and write raw `warp` artifacts
under `benchmarks/profiles/iceberg-table-*`.

The default workload is `ICEBERG_WARP_COMMAND=catalog-mixed` with views and
update operations disabled (`ICEBERG_VIEWS_PER_NS=0`,
`ICEBERG_TABLE_UPDATE_DISTRIB=0`) so the benchmark targets read/list/head
table-catalog behavior first. Use `ICEBERG_WARP_COMMAND=catalog-read` or
`catalog-commits` to focus the workload, and opt into update distribution after
commit-conflict behavior has been tuned.

Tune the workload with:

```bash
VUS=16 DURATION=1m make bench-iceberg-table
VUS=16 DURATION=1m make bench-iceberg-table-cluster
```

Iceberg benchmark scripts use the same host preflight and `BENCH_STRICT_HOST=1`
fail-fast behavior as the S3 comparison script.

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
