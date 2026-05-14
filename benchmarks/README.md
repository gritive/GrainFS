# GrainFS Benchmarks

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

The multi-export run creates four buckets by default and runs four fio workers
per bucket, for 16 total fio workers. It records:

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
contention. Pseudo-root latency is measured through a separate `host:/` mount;
bucket throughput is measured through `host:/bench-<i>` mounts.
