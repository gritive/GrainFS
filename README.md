# `GrainFS`

`GrainFS` is a single-binary distributed storage server. It runs as one local
node or as a Raft-backed cluster.

It exposes object, file, and block interfaces over one storage layer:

- **Object storage:** S3-compatible HTTP API
- **File storage:** NFSv4 and 9P2000.L
- **Block storage:** NBD for Linux clients
- **Table/catalog integration:** Iceberg REST Catalog for DuckDB-oriented lake workflows

## Quick Start

```bash
make build

CLUSTER_KEY=$(openssl rand -hex 32)

./bin/grainfs serve --data ./storage --port 9000 --cluster-key "$CLUSTER_KEY" &

# Bootstrap the first admin service account through the local admin socket.
./bin/grainfs iam sa create admin --endpoint ./storage/admin.sock
# {"sa_id":"sa-default","access_key":"GRAIN...","secret_key":"<one-time>",...}

export GRAINFS_ADMIN_SOCKET=./storage/admin.sock
```

Use the returned `access_key` and `secret_key` with any SigV4 S3 client:

```bash
export AWS_ACCESS_KEY_ID=<access_key>
export AWS_SECRET_ACCESS_KEY=<secret_key>
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://localhost:9000 s3 mb s3://test
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://test/
aws --endpoint-url http://localhost:9000 s3 ls s3://test/
```

The server creates a `default` bucket at startup and exposes the object browser
at `http://localhost:9000/ui/`. S3 requests return `401` until an operator
creates the first service account.

For all serve flags, use:

```bash
./bin/grainfs serve --help
```

## What It Supports

| Area | Summary | Details |
| --- | --- | --- |
| S3 API | Bucket/object basics, multipart upload/listing, SigV4, presigned URL, form upload | [S3 compatibility](docs/reference/s3-compatibility.md) |
| File protocols | NFSv4 explicit bucket exports, 9P2000.L | [NFSv4 compatibility](docs/reference/nfs-compatibility.md), [9P compatibility](docs/reference/9p-compatibility.md) |
| Block protocol | Linux NBD protocol surface | [NBD compatibility](docs/reference/nbd-compatibility.md) |
| Iceberg | DuckDB-compatible REST Catalog | [Iceberg compatibility](docs/reference/iceberg-compatibility.md) |
| Cluster durability | Custom Raft, zero-config EC profile, shard integrity envelope | [Runbook](docs/operators/runbook.md) |
| Operations | Object browser, metrics, balancer status, incidents, recovery drills | [Documentation](#documentation) |

The compatibility tables use `Supported` only for features covered by e2e,
conformance, or real client integration tests. Unit tests alone do not qualify.

## Performance

Latest same-host `warp` runs, 64 KiB objects, concurrency 16, signed S3
requests, 0 errors:

| Mode | Target    | PUT MiB/s | GET MiB/s | vs MinIO PUT | vs MinIO GET |
| ---- | --------- | --------: | --------: | -----------: | -----------: |
| Single-node | `GrainFS` |    517.36 |   1212.67 |        2.05x |        1.13x |
| Single-node | MinIO     |    252.88 |   1074.01 |        1.00x |        1.00x |
| Single-node | RustFS    |    225.43 |    500.35 |        0.89x |        0.47x |
| 3-node cluster | `GrainFS` |    103.22 |    325.85 |        2.19x |        1.10x |
| 3-node cluster | MinIO     |     47.05 |    296.84 |        1.00x |        1.00x |
| 3-node cluster | RustFS    |     36.31 |    105.88 |        0.77x |        0.36x |

Methodology and raw artifacts:
[benchmark reference](docs/reference/benchmarks.md#latest-local-result).

## Core Concepts

**Admin socket first.** Mutating admin operations use the local Unix domain
socket by default (`<data>/admin.sock`). Set `GRAINFS_ADMIN_SOCKET` to avoid
passing `--endpoint` on every command.

**Secure bootstrap.** A fresh cluster has no S3 credentials. Create the first
service account through the admin socket; the secret key is shown once.

**Zero-config EC.** Operators do not choose `k/m` at startup. `GrainFS` derives
the desired erasure-coding profile from cluster size and placement group voters.
Temporary target loss does not silently lower durability; writes fail until the
required targets are writable.

**Same data, multiple protocols.** S3, NFSv4, 9P, NBD, and Iceberg use the same
storage backend contracts. Use the compatibility docs for protocol-specific
limits.

**Protocol network boundary.** S3 uses IAM. NFSv4, 9P, and NBD do not use S3
IAM; expose those listeners only on loopback, private networks, or
firewall-restricted addresses.

## Common Workflows

| Workflow | Command or entry point |
| --- | --- |
| Create/list service accounts and grants | `grainfs iam --endpoint <data>/admin.sock ...` |
| Inspect cluster peers | `grainfs cluster --endpoint <data>/admin.sock peers` |
| Inspect object placement | `grainfs cluster --endpoint <data>/admin.sock placement [bucket] [key]` |
| Configure cluster policy | `grainfs cluster config --endpoint <data>/admin.sock ...` |
| Export a bucket over NFSv4 | `grainfs nfs export add <bucket>` |
| Create an NBD volume | `grainfs volume create <name> --size 10Gi` |
| Run recovery planning | `grainfs recover cluster plan --source-data <dir> --target-data <dir>` |
| Check balancer status | `curl http://localhost:9000/api/cluster/balancer/status` |
| Check incidents | `curl http://localhost:9000/api/incidents` |

Operational details live in [docs/index.md](docs/index.md#operators).

## Development

Requirements:

- Go 1.26+
- `golangci-lint` (run by `make lint`, which `make build` depends on)
- `warp` for S3-compatible comparison benchmarks
- Linux client tooling for NFS, NBD, 9P, and FUSE-over-S3 integration tests

Common commands:

```bash
make build
make test
make test-race
make test-e2e
make lint
```

Benchmark targets:

```bash
make bench
make bench-cluster
make bench-s3-compat-compare
make bench-iceberg-table
make bench-iceberg-table-cluster
make bench-nfs
make bench-nbd
make bench-nfs-cluster
make bench-nbd-cluster
```

Use [docs/reference/benchmarks.md](docs/reference/benchmarks.md) for benchmark
methodology and result interpretation. Use
[benchmarks/README.md](benchmarks/README.md) for script flags.

## Documentation

| Topic | Document |
| --- | --- |
| Documentation hub | [docs/index.md](docs/index.md) |
| Users | [docs/users/guide.md](docs/users/guide.md) |
| Operators | [docs/index.md#operators](docs/index.md#operators) |
| Reference | [docs/index.md#reference](docs/index.md#reference) |
| Explanation | [docs/index.md#explanation](docs/index.md#explanation) |

## License

Apache 2.0
