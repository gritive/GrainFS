# `GrainFS`

`GrainFS` is a single-binary distributed storage server. It runs as one local
node or as a Raft-backed cluster.

It exposes object, file, and block interfaces over one storage layer:

- **Object storage:** S3-compatible HTTP API
- **File storage:** NFSv4 and 9P2000.L
- **Block storage:** NBD for Linux clients
- **Table/catalog integration:** Iceberg REST Catalog for DuckDB-oriented lake workflows

## Quick Start (2-5 minutes)

```bash
DATA_DIR=./tmp
make bin/grainfs
CLUSTER_KEY=$(openssl rand -hex 32)
./bin/grainfs serve --data "$DATA_DIR" --port 9000 --cluster-key "$CLUSTER_KEY"
```

In another terminal:

```bash
echo "hello grainfs" > file.txt
aws --no-sign-request --endpoint-url http://localhost:9000 s3 cp file.txt s3://default/
aws --no-sign-request --endpoint-url http://localhost:9000 s3 ls s3://default/
```

Expected output:

```text
upload: ./file.txt to s3://default/file.txt
... file.txt
```

That's it. You have a working local S3 server. To verify the same data through
NFS on Linux, continue with [`docs/users/nfs-mount-quickstart.md`](docs/users/nfs-mount-quickstart.md).
That guide also covers 9P mounts, authenticated Mount SAs, and read-only exports.

> ⚠ **Anonymous local mode**: any client on this port can read/write `s3://default`. To require authentication, create the first service account through the admin socket under the data directory (`<data-dir>/admin.sock`); the Auth + Iceberg block below shows the Quick Start command. See [`docs/operators/deploy-production-cluster.md`](docs/operators/deploy-production-cluster.md).

### Optional: cluster / production setup

<details>
<summary>Cluster</summary>

Phase A stages two files on each joining node: `<data>/keys/0.key` (active KEK) and `<data>/cluster.id` (cluster identity). Both must be copied from a healthy peer before running `cluster join`:

```bash
DATA_DIR=./dataB
mkdir -p "$DATA_DIR/keys"
scp <nodeA>:<nodeA-data-dir>/keys/0.key "$DATA_DIR/keys/0.key"
scp <nodeA>:<nodeA-data-dir>/cluster.id "$DATA_DIR/cluster.id"
chmod 0600 "$DATA_DIR/keys/0.key" "$DATA_DIR/cluster.id"
./bin/grainfs cluster join <nodeA>:7001 \
  --data "$DATA_DIR" \
  --node-id node-b \
  --bind-addr <nodeB>:7001 \
  --cluster-key "$CLUSTER_KEY"
```
</details>

<details>
<summary>Auth + Iceberg</summary>

```bash
DATA_DIR=./tmp
./bin/grainfs iam sa create admin --endpoint "$DATA_DIR/admin.sock"
./bin/grainfs iam policy attach readwrite --sa <id> --endpoint "$DATA_DIR/admin.sock" --i-know
./bin/grainfs iam bucket create analytics --attach-sa <id> --attach-policy readwrite --endpoint "$DATA_DIR/admin.sock"
```

For Iceberg client config (DuckDB / Trino / Spark / PyIceberg / warp):

```bash
./bin/grainfs iceberg config --warehouse analytics --sa <id> --endpoint "$DATA_DIR/admin.sock"
```

See [`docs/users/oauth2-iceberg-quickstart.md`](docs/users/oauth2-iceberg-quickstart.md).
</details>

<details>
<summary>Production hardening</summary>

TLS / encryption-key / audit / proxy-trust — all hot-applyable via `config set`.
See [`docs/operators/deploy-production-cluster.md`](docs/operators/deploy-production-cluster.md).
</details>

## What It Supports

| Area | Summary | Details |
| --- | --- | --- |
| S3 API | Bucket/object basics, AppendObject (S3 Express), multipart upload/listing, SigV4, presigned URL, form upload | [S3 compatibility](docs/reference/s3-compatibility.md) |
| File protocols | NFSv4 explicit bucket exports, 9P2000.L | [NFSv4 compatibility](docs/reference/nfs-compatibility.md), [9P compatibility](docs/reference/9p-compatibility.md) |
| Block protocol | Linux NBD protocol surface | [NBD compatibility](docs/reference/nbd-compatibility.md) |
| Iceberg | DuckDB-compatible REST Catalog | [Iceberg compatibility](docs/reference/iceberg-compatibility.md) |
| Cluster durability | Custom Raft, zero-config EC profile, shard integrity envelope | [Runbook](docs/operators/runbook.md) |
| Operations | Object browser, metrics, balancer status, incidents, recovery drills | [Documentation](#documentation) |

The compatibility tables use `Supported` only for features covered by e2e,
conformance, or real client integration tests. Unit tests alone do not qualify.

## Performance

Latest same-host single-node `warp` runs, 64 KiB objects, concurrency 32,
signed S3 requests, 0 errors:

| Target    | PUT MiB/s | GET MiB/s | vs MinIO PUT | vs MinIO GET |
| --------- | --------: | --------: | -----------: | -----------: |
| `GrainFS` |    548.30 |   1849.34 |        3.13x |        4.04x |
| MinIO     |    175.14 |    457.81 |        1.00x |        1.00x |
| RustFS    |     26.62 |    437.77 |        0.15x |        0.96x |

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
| Create/list service accounts, keys, and policies | `grainfs iam --endpoint <data>/admin.sock ...` |
| Inspect cluster peers | `grainfs cluster --endpoint <data>/admin.sock peers` |
| Inspect object placement | `grainfs cluster --endpoint <data>/admin.sock placement [bucket] [key]` |
| Configure cluster policy | `grainfs cluster config --endpoint <data>/admin.sock ...` |
| Export a bucket over NFSv4 | `grainfs nfs export add <bucket> --endpoint <data>/admin.sock` |
| Rotate / inspect the cluster encryption key (KEK) | `grainfs encrypt kek status\|rotate\|retire\|prune --endpoint <data>/admin.sock` |
| Create an NBD volume | `grainfs volume create <name> --size 10Gi --endpoint <data>/admin.sock` |
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
