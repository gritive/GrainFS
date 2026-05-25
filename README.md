# `GrainFS`

`GrainFS` is a single-binary distributed storage server. It runs as one local node or as a Raft-backed cluster.

It exposes one storage layer through multiple interfaces:

- **Object storage:** S3-compatible HTTP API
- **File storage:** NFSv4 and 9P2000.L
- **Block storage:** NBD for Linux clients
- **Table/catalog integration:** Iceberg REST Catalog for DuckDB-oriented lake workflows

## Quick Start

```bash
./grainfs serve --data ./tmp --port 9000
```

In another terminal:

```bash
echo "hello grainfs" > file.txt
aws --no-sign-request --endpoint-url http://localhost:9000 s3 cp file.txt s3://default/
aws --no-sign-request --endpoint-url http://localhost:9000 s3 ls s3://default/
```

For NFS:

```bash
grainfs nfs export add default --endpoint ./tmp/admin.sock
sudo mount -t nfs4 -o nolock,nfsvers=4.0 localhost:/default /mnt/data
```

A fresh local node starts in Phase 0 anonymous mode. Any client that can reach the S3 listener can read and write `s3://default`. Create a service account through the admin socket before exposing it beyond local development.

Start with [the documentation hub](docs/index.md), or go directly to:

- [Quickstart](docs/start-here/quickstart.md)
- [First S3 object](docs/start-here/first-s3-object.md)
- [First NFS mount](docs/start-here/first-nfs-mount.md)
- [Identity and admin socket](docs/concepts/identity-and-admin-socket.md)
- [Cluster lifecycle](docs/operations/cluster-lifecycle.md)

## What It Supports

| Area | Summary | Details |
| --- | --- | --- |
| S3 API | Bucket/object basics, AppendObject, multipart upload/listing, SigV4, presigned URL, form upload | [S3 compatibility](docs/reference/s3-compatibility.md) |
| File protocols | NFSv4 explicit bucket exports, 9P2000.L | [NFSv4 compatibility](docs/reference/nfs-compatibility.md), [9P compatibility](docs/reference/9p-compatibility.md) |
| Block protocol | Linux NBD protocol surface | [NBD compatibility](docs/reference/nbd-compatibility.md) |
| Iceberg | DuckDB-compatible REST Catalog | [Iceberg compatibility](docs/reference/iceberg-compatibility.md) |
| Cluster durability | Raft-backed clustering, zero-config EC profile, shard integrity envelope | [Durability and placement](docs/concepts/durability-and-placement.md) |
| Operations | Metrics, balancer status, incidents, recovery, rolling upgrades | [Operations docs](docs/index.md#operations) |

The compatibility tables use `Supported` only for features covered by e2e, conformance, or real client integration tests. Unit tests alone do not qualify.

## Performance

Latest same-host single-node `warp` runs, 64 KiB objects, concurrency 32, signed S3 requests, 0 errors:

| Target    | PUT MiB/s | GET MiB/s | vs MinIO PUT | vs MinIO GET |
| --------- | --------: | --------: | -----------: | -----------: |
| `GrainFS` |    548.30 |   1849.34 |        3.13x |        4.04x |
| MinIO     |    175.14 |    457.81 |        1.00x |        1.00x |
| RustFS    |     26.62 |    437.77 |        0.15x |        0.96x |

Methodology and raw artifacts: [benchmark reference](docs/reference/benchmarks.md#latest-local-result).

## Development

Requirements:

- Go 1.26+
- `golangci-lint` for `make lint` and `make build`
- `warp` for S3-compatible comparison benchmarks
- Linux client tooling for NFS, NBD, 9P, and FUSE-over-S3 integration tests

Common commands:

```bash
make build
make test-unit
make test
make test-race
make test-e2e
make lint
```

More development docs:

- [Build and test](docs/development/build-and-test.md)
- [Repository layout](docs/development/repository-layout.md)
- [Adding CLI commands](docs/development/adding-cli-commands.md)
- [Compatibility claims](docs/development/compatibility-claims.md)
- [Release process](docs/development/release-process.md)

## Documentation

The public documentation starts at [docs/index.md](docs/index.md). Internal planning artifacts, ADR drafts, and agent work logs are not part of the customer documentation set.
