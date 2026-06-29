# GrainFS Benchmarks

For benchmark principles, result interpretation, and RustFS/MinIO comparison
rules, see [`docs/reference/benchmarks.md`](../docs/reference/benchmarks.md).

## GCP Single-Node Encrypted Comparison

Use `gcp/bench_gcp_cluster.sh` for publishable single-node GrainFS vs MinIO
performance comparisons. The script provisions a GCP client VM plus storage
VMs, builds `NEW_REF` on Linux, runs GrainFS single-node on `node-0` with
at-rest encryption, runs MinIO single-node on the same VM class with SSE-S3
auto-encryption, and drives both from the in-network client with MinIO `warp`.

Keep `RESULT_DIR` exported across subcommands; otherwise each invocation creates
a new timestamped artifact directory. Commit the ref you want to measure before
running the script because `build` uses `git archive`.

```bash
export PROJECT=grainfs
export ZONE=asia-northeast3-a
export PREFIX=gr-single
export NODE_COUNT=1
export NEW_REF=HEAD
export OLD_REF=master
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
# results: benchmarks/profiles/gcp-single-<timestamp>/
```

The default GCP workload is signed S3 `warp` with 10 MiB objects, concurrency
32, 1 minute per operation, `put,get,stat`, 4096 objects, and `WARP_NOCLEAR=1`
inside the script so GET and stat measure the objects created by PUT. GrainFS
pprof snapshots are saved under `single/run<N>/pprof/`; raw warp artifacts are
saved under `single/run<N>/raw/` and `minio/run<N>/raw/`.

Always run `down` after preserving results; `full` intentionally does not
auto-teardown VMs.

## GCP Cross-Binary A/B

Use the same GCP wrapper for paired GrainFS binary comparisons. This path boots
`NEW_REF` and `OLD_REF`, runs the same warp workload for each arm, and aggregates
`run<N>/{new,old}` results with `lib/ab_verdict.py`.

```bash
export RESULT_DIR="$PWD/benchmarks/profiles/gcp-ab-$(date +%Y%m%d-%H%M%S)"
RUNS=3 WARP_OBJ_SIZE=10MiB WARP_CONCURRENT=32 WARP_DURATION=1m \
  ./benchmarks/gcp/bench_gcp_cluster.sh full
# results: benchmarks/profiles/gcp-ab-<timestamp>/verdict.md
```

## Local S3-Compatible Comparison

`make bench`, `make bench-cluster`, and `make bench-s3-compat-compare` run the
official S3 workload with MinIO `warp` on the local machine. Use these targets
for smoke checks and local regressions, not for publishable GrainFS vs MinIO
claims. `make bench` targets a local GrainFS single-node server,
`make bench-cluster` targets a local GrainFS 4-node cluster, and
`make bench-s3-compat-compare` compares GrainFS single-node with any native
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
operation, `WARP_HOST_SELECT=roundrobin`, and `WARP_NOCLEAR=0` so each op clears
the objects it created, bounding peak disk to a single op's working set (important
for large object sizes). Set `WARP_NOCLEAR=1` to retain objects across ops so GET
measures a warm-read pass over the preceding PUT, at the cost of unbounded
accumulation across ops.

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
strict mode for local comparison runs so contaminated host state fails before
any benchmark server starts:

```bash
BENCH_STRICT_HOST=1 WARP_OPS=put,get make bench-s3-compat-compare
```
