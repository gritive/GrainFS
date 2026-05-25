# `GrainFS` User Guide

This guide covers common user-facing commands and protocol workflows. The
README is intentionally short; use this document when you need operational
examples beyond the first S3 quickstart.

Always check the running binary for authoritative CLI flags:

```bash
grainfs serve --help
grainfs iam --help
grainfs cluster --help
grainfs nfs --help
grainfs volume --help
```

## Serve

Minimal single-node startup:

```bash
CLUSTER_KEY=$(openssl rand -hex 32)
grainfs serve --data ./data --port 9000 --cluster-key "$CLUSTER_KEY"
```

Common serve options:

| Option | Purpose |
| --- | --- |
| `--data` | Data directory. |
| `--port` | HTTP/S3 port. |
| `--cluster-key` | Pre-shared key for peer authentication. Required, including single-node mode. |
| `--admin-socket` | Local Unix socket for admin commands. Defaults under the data directory. |
| `--admin-group` | Group owner for the admin socket. |
| `--public-url` | Dashboard base URL shown by `grainfs dashboard`. |
| `--node-id` | Explicit node identity. |
| `--raft-addr` | Raft listen address. |
| `--nfs4-port` | NFSv4 port; use `0` to disable. |
| `--nbd-port` | NBD port; use `0` to disable. |
| `--9p-bind`, `--9p-port` | 9P2000.L bind address and port. |
| `--encryption-key-file` | 32-byte at-rest encryption key file. Required for cluster/join mode. |

## Bootstrap IAM

Fresh data directories have no S3 credentials. Create the first service account
through the local admin socket:

```bash
grainfs iam sa create admin --endpoint ./data/admin.sock
export GRAINFS_ADMIN_SOCKET=./data/admin.sock
```

The first response includes an `access_key` and `secret_key`. Store the secret
immediately; it is shown once.

Create a normal service account and grant bucket access:

```bash
grainfs iam sa create alice --description "data team"
grainfs iam policy attach readwrite --sa <sa_id> --i-know
grainfs iam bucket create mybucket --attach-sa <sa_id> --attach-policy readwrite
```

Rotate an access key with a client rollover window:

```bash
grainfs iam key create <sa_id>
grainfs iam key revoke <sa_id> <old_access_key>
```

## S3

Use path-style S3 clients against the configured HTTP port:

```bash
export AWS_ACCESS_KEY_ID=<access_key>
export AWS_SECRET_ACCESS_KEY=<secret_key>
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://mybucket/file.txt
```

AppendObject (S3 Express semantics) lets you grow an object incrementally without
multipart upload. Each write supplies the expected current size via
`x-amz-write-offset-bytes`; the server stitches segments and EC-coalesced blobs on
read. Use it for log streams, telemetry, or any append-only workload:

```bash
# First append creates the object.
curl -X PUT "http://localhost:9000/mybucket/log.bin" \
  -H "x-amz-write-offset-bytes: 0" \
  --data-binary @chunk-0.bin

# Subsequent appends supply the expected current size.
SIZE=$(aws --endpoint-url http://localhost:9000 s3api head-object \
  --bucket mybucket --key log.bin --query ContentLength --output text)
curl -X PUT "http://localhost:9000/mybucket/log.bin" \
  -H "x-amz-write-offset-bytes: $SIZE" \
  --data-binary @chunk-1.bin
```

Per-request body cap defaults to 64 MiB; per-object size cap defaults to 5 TiB.
Tune via `--cluster-append-forward-buffer-max-per-request` and
`--append-size-cap-bytes`. Saturation of the forward buffer surfaces as HTTP
`503 SlowDown` with `Retry-After: 1` — retry with exponential backoff.

Configure per-bucket pull-through upstreams through the admin surface:

```bash
grainfs bucket upstream put legacy-data \
  --endpoint-url http://upstream-minio:9000 \
  --access-key legacy-ak \
  --secret-key legacy-sk

grainfs bucket upstream get legacy-data
grainfs bucket upstream delete legacy-data
```

See `../reference/s3-compatibility.md` for the supported S3 surface.

## NFSv4

NFSv4 exports are explicit. Create an S3 bucket, register it as an export, then
mount the pseudo-root. This example assumes the `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION` variables from the S3 section
are still set.

```bash
aws --endpoint-url http://localhost:9000 s3 mb s3://mydata

grainfs nfs export add mydata

sudo mkdir -p /mnt/grainfs
sudo mount -t nfs4 -o "vers=4.0,port=2049,rw,hard,intr" localhost:/ /mnt/grainfs

ls /mnt/grainfs/
echo "hello" | sudo tee /mnt/grainfs/mydata/test.txt

sudo umount /mnt/grainfs
grainfs nfs export remove mydata
```

Manage exports:

```bash
grainfs nfs export list
grainfs nfs export add mydata --ro
grainfs nfs export update mydata --rw
grainfs nfs debug mydata --json
```

See `../reference/nfs-compatibility.md`, `../operators/nfs-debug.md`, and
`../operators/nfs-export-lifecycle.md`.

## NBD

NBD requires a Linux client.

```bash
grainfs volume create v1 --size 10Gi

sudo modprobe nbd
sudo nbd-client localhost 10809 /dev/nbd0 -N v1

sudo mkfs.ext4 /dev/nbd0
sudo mkdir -p /mnt/nbd-v1
sudo mount /dev/nbd0 /mnt/nbd-v1

sudo umount /mnt/nbd-v1
sudo nbd-client -d /dev/nbd0
```

See `../reference/nbd-compatibility.md`.

## 9P

Enable the 9P server with `--9p-port` and `--9p-bind`. The server is
unauthenticated, so keep the default loopback bind unless the network is
trusted.

```bash
grainfs serve \
  --data ./data \
  --port 9000 \
  --cluster-key "$CLUSTER_KEY" \
  --9p-bind 127.0.0.1 \
  --9p-port 5640
```

From a Linux client:

```bash
sudo modprobe 9p 9pnet 9pnet_virtio
sudo mkdir -p /mnt/grainfs-9p
sudo mount -t 9p \
  -o trans=tcp,port=5640,version=9p2000.L,msize=262144,aname=/mybucket \
  127.0.0.1 /mnt/grainfs-9p
```

See `../reference/9p-compatibility.md` for the supported 9P surface.

## Cluster Operations

Inspect peers:

```bash
grainfs cluster peers
grainfs cluster events --since 24h
```

Inspect object placement:

```bash
grainfs cluster placement
grainfs cluster placement <bucket> <key> --format json
```

Remove a dead peer:

```bash
grainfs cluster remove-peer <node-id> --yes
```

Force removal is a disaster-recovery step:

```bash
grainfs cluster remove-peer <node-id> --force --yes
```

Update cluster-wide policy values:

```bash
grainfs cluster config --help
```

See `../operators/runbook.md` for production procedures.

## Recovery

Plan recovery into a fresh target directory:

```bash
grainfs recover cluster plan \
  --source-data /var/lib/grainfs \
  --target-data /var/lib/grainfs-recovered \
  --new-node-id node-recovered \
  --new-raft-addr 10.0.0.10:19100
```

Execute and verify:

```bash
grainfs recover cluster execute \
  --source-data /var/lib/grainfs \
  --target-data /var/lib/grainfs-recovered \
  --new-node-id node-recovered \
  --new-raft-addr 10.0.0.10:19100

grainfs recover cluster verify \
  --target-data /var/lib/grainfs-recovered \
  --mark-writable
```

`recover --auto` is intentionally non-mutating and fails closed. See
`docs/operators/recover-cluster.md`.

## Operations APIs

Useful local endpoints:

```bash
curl http://localhost:9000/api/cluster/balancer/status | jq .
curl http://localhost:9000/api/incidents | jq .
curl http://localhost:9000/api/audit/health | jq .
```

The object browser is available at:

```text
http://localhost:9000/ui/
```
