# `GrainFS`

`GrainFS` is a high-performance distributed S3-compatible storage server with
zero-config cluster management and at-rest encryption on by default.

> **Technical preview:** `GrainFS` is currently positioned for local-first S3
> evaluation, compatibility testing, and operator feedback — not as a GA
> production storage replacement. The strongest launch wedge is a working local
> S3-compatible object store with documented encryption, IAM, cluster, metrics,
> and recovery surfaces. Broader production, protocol-parity, and competitive
> claims should be checked against the compatibility docs, benchmark methodology,
> `CHANGELOG.md`, and [launch claims checklist](docs/reference/launch-claims-checklist.md).

## Install / Try It

`GrainFS` does not publish release binaries, container images, or Homebrew
packages yet. For this technical preview, the supported evaluator path is a
source checkout with Go 1.26+; it does not require the full contributor toolchain
or `golangci-lint`.

```bash
git clone https://github.com/gritive/GrainFS.git
cd GrainFS
go build -o bin/grainfs ./cmd/grainfs/
```

Contributor builds should use [`make build`](#development), which also runs the
repository lint gates.

## Quick Start (2-5 minutes)

After installing `bin/grainfs` from the source-build step above:

```bash
DATA_DIR=./tmp
./bin/grainfs serve --data "$DATA_DIR" --port 9000
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

That's it. You have a working local S3 server.

> ⚠ **Anonymous default bucket**: any client on this port can read/write `s3://default` until you install an explicit bucket policy for `default`. Create service accounts through the admin socket under the data directory (`<data-dir>/admin.sock`); the Auth block below shows the Quick Start command. See [`docs/operators/deploy-production-cluster.md`](docs/operators/deploy-production-cluster.md).

### Optional: cluster / production preview setup

<details>
<summary>Cluster (zero-CA join)</summary>

GrainFS uses Zero-CA invite join for new peers. There is no separate `cluster
join` command and no `--cluster-key` flag in the current CLI. A fresh leader
self-generates and seals the cluster transport key; a joining node receives the
sealed bootstrap material through a single-use invite bundle.

Current node-to-node cluster traffic uses streaming HTTP over TCP with
SPKI-pinned mTLS. Older design records and changelog entries may mention QUIC,
TCP mux carriers, `--transport`, or `--quic-*` flags as migration history; those
are not current operator controls.

Start or restart the leader with a stable Raft address and, for production, a
stable join-listener address:

```bash
LEADER_DATA=./dataA
./bin/grainfs serve \
  --data "$LEADER_DATA" \
  --node-id node-a \
  --raft-addr node-a:7001 \
  --join-listen-addr node-a:7443
```

On the leader, mint an invite and copy the printed bundle token out-of-band:

```bash
./bin/grainfs cluster invite create \
  --endpoint "$LEADER_DATA/admin.sock" \
  --ttl 1h
```

On the joining node, set the token and start `serve` with no pre-copied cluster
secrets:

```bash
GRAINFS_INVITE_BUNDLE='<bundle-token>' ./bin/grainfs serve \
  --data ./dataB \
  --node-id node-b \
  --raft-addr node-b:7001 \
  --port 9001
```

For production steps, verification, cutover, and revocation, see
[`docs/operators/zero-ca-cluster-join.md`](docs/operators/zero-ca-cluster-join.md).

</details>

<details>
<summary>Auth</summary>

```bash
DATA_DIR=./tmp
./bin/grainfs iam sa create admin --endpoint "$DATA_DIR/admin.sock"
./bin/grainfs iam policy attach readwrite --sa <id> --endpoint "$DATA_DIR/admin.sock" --i-know
./bin/grainfs iam bucket create analytics --attach-sa <id> --attach-policy readwrite --endpoint "$DATA_DIR/admin.sock"
```

</details>

<details>
<summary>Production hardening</summary>

TLS / encryption-key / audit / proxy-trust — all hot-applyable via `config set`.
See [`docs/operators/deploy-production-cluster.md`](docs/operators/deploy-production-cluster.md).

</details>

## What It Supports

| Area               | Summary                                                                                                      | Details                                                                     |
| ------------------ | ------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------- |
| S3 API             | Bucket/object basics, AppendObject (S3 Express), multipart upload/listing, SigV4, presigned URL, form upload | [S3 compatibility](docs/reference/s3-compatibility.md)                      |
| Cluster durability | Custom Raft, zero-config topology (EC profile, join, key hierarchy), shard integrity envelope                | [Runbook](docs/operators/runbook.md)                                        |
| Encryption         | XAES-256-GCM at rest by default, KEK/DEK rotation, hot-applyable via `config set`                            | [Production deploy](docs/operators/deploy-production-cluster.md)            |
| Auth               | Service accounts, access keys, bucket policies, SigV4 request signing                                        | [Admin and identity](docs/architecture/admin-identity-and-bucket-config.md) |
| Operations         | Object browser, metrics, balancer status, incidents, recovery drills                                         | [Documentation](#documentation)                                             |

The compatibility tables use `Supported` only for features covered by e2e,
conformance, or real client integration tests. Unit tests alone do not qualify.
Use the [launch claims checklist](docs/reference/launch-claims-checklist.md)
before publishing public copy or competitive comparisons.

## Performance

Both runs used `warp`, 10 MiB object size. `GrainFS` ran with XAES-256-GCM
at-rest encryption; MinIO ran with SSE-S3 auto-encryption.

Latest GCP single-node `warp` run (v0.0.783.0; the MinIO row is the 2026-06-30
baseline — the single-node MinIO `warp` arm again returned empty benchdata, a
known harness issue):

| Target    | PUT MiB/s | GET MiB/s | vs MinIO PUT | vs MinIO GET |
| --------- | --------: | --------: | -----------: | -----------: |
| `GrainFS` |    221.98 |    361.39 |        1.06x |        0.76x |
| MinIO     |    209.77 |    472.65 |        1.00x |        1.00x |

Latest GCP 4-node cluster `warp` run (v0.0.783.0, PUT/GET):

| Target            | PUT MiB/s | GET MiB/s | vs MinIO PUT | vs MinIO GET |
| ----------------- | --------: | --------: | -----------: | -----------: |
| `GrainFS` cluster |    439.51 |   2267.93 |        0.94x |        0.99x |
| MinIO distributed |    467.42 |   2291.82 |        1.00x |        1.00x |

## Core Concepts

**Zero-config cluster.** Operators set no topology parameters. `GrainFS` derives
the erasure-coding profile from cluster size, manages join ceremonies without
pre-shared secrets, and handles the full key hierarchy (KEK/DEK) automatically.

**At-rest encryption.** All data is encrypted with XAES-256-GCM by default, no
configuration needed. The cluster manages its own key hierarchy (KEK/DEK); rotate
keys live via `encrypt kek rotate`.

**Admin socket first.** Mutating admin operations use the local Unix domain
socket by default (`<data>/admin.sock`). Set `GRAINFS_ADMIN_SOCKET` to avoid
passing `--endpoint` on every command.

**Secure bootstrap.** A fresh cluster has no S3 credentials. Create the first
service account through the admin socket; the secret key is shown once.

## Common Workflows

| Workflow                                          | Command or entry point                                                                 |
| ------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Create/list service accounts, keys, and policies  | `grainfs iam --endpoint <data>/admin.sock ...`                                         |
| Inspect cluster peers                             | `grainfs cluster --endpoint <data>/admin.sock peers`                                   |
| Add a peer with zero-CA invite join               | [`docs/operators/zero-ca-cluster-join.md`](docs/operators/zero-ca-cluster-join.md)     |
| Mint a zero-CA join invite (leader)               | `grainfs cluster invite create --endpoint <data>/admin.sock --ttl 1h`                  |
| Drop the shared cluster-key accept path           | `grainfs cluster --endpoint <data>/admin.sock complete-cutover`                        |
| Revoke a zero-CA node identity                    | `grainfs cluster --endpoint <data>/admin.sock revoke-node <node-id>`                   |
| Inspect object placement                          | `grainfs cluster --endpoint <data>/admin.sock placement [bucket] [key]`                |
| Grow placement groups on a running cluster        | `grainfs cluster expand-placement --endpoint <data>/admin.sock`                        |
| Retire a drained placement generation             | `grainfs cluster retire-placement-generation --endpoint <data>/admin.sock --epoch <n>` |
| Configure cluster policy                          | `grainfs cluster config --endpoint <data>/admin.sock ...`                              |
| Rotate / inspect the cluster encryption key (KEK) | `grainfs encrypt kek status\|rotate\|retire\|prune --endpoint <data>/admin.sock`       |
| Check balancer status                             | `curl http://localhost:9000/api/cluster/balancer/status`                               |
| Check incidents                                   | `curl http://localhost:9000/api/incidents`                                             |

Operational details live in [docs/index.md](docs/index.md#operators).

## Development

Requirements:

- Go 1.26+
- `golangci-lint` (run by `make lint`, which `make build` depends on)
- `warp` for S3-compatible comparison benchmarks
- `gcloud` CLI and project access for GCP performance benchmarks
- Linux client tooling for FUSE-over-S3 integration tests

Common commands:

```bash
make build
make test
make test-race
make test-e2e
make lint
```

GCP benchmark entry point:

```bash
./benchmarks/gcp/bench_gcp_cluster.sh {up|build|single|grainfs-cluster|minio|minio-cluster|single-verdict|cluster-minio-verdict|down}
```

Local benchmark targets:

```bash
make bench
make bench-cluster
make bench-s3-compat-compare
```

Use [docs/reference/benchmarks.md](docs/reference/benchmarks.md) for benchmark
methodology and result interpretation. Use
[benchmarks/README.md](benchmarks/README.md) for script flags.

## Documentation

| Topic             | Document                                               |
| ----------------- | ------------------------------------------------------ |
| Documentation hub | [docs/index.md](docs/index.md)                         |
| Users             | [docs/users/guide.md](docs/users/guide.md)             |
| Operators         | [docs/index.md#operators](docs/index.md#operators)     |
| Reference         | [docs/index.md#reference](docs/index.md#reference)     |
| Explanation       | [docs/index.md#explanation](docs/index.md#explanation) |

## License

Apache 2.0
