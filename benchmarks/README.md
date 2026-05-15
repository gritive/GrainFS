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

Set `PUT_TRACE=1` with the matrix to write per-node PUT trace JSONL files under
the benchmark temp directory and print a dominant-stage report:

```bash
PUT_MATRIX=1 PUT_TRACE=1 make bench-cluster
```

The scripts create trace files with mode `0600` because they include raw bucket
and object keys. The report groups requests by ingress, size class, and forwarding mode,
then summarizes forwarded bytes, leader-hint retries, meta-index proposal
counts, and the slowest shard stage.

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
