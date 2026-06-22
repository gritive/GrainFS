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
grainfs serve --data ./data --port 9000
```

Common serve options:

| Option | Purpose |
| --- | --- |
| `--data` | Data directory. |
| `--port` | HTTP/S3 port. |
| (cluster transport key) | Generated at genesis, pulled by invite-join, or read from `keys.d/current.key`; no flag. |
| `--admin-socket` | Local Unix socket for admin commands. Defaults under the data directory. |
| `--admin-group` | Group owner for the admin socket. |
| `--public-url` | Dashboard base URL shown by `grainfs dashboard`. |
| `--node-id` | Explicit node identity. |
| `--raft-addr` | Raft listen address. |
| `--nfs4-port` | NFSv4 port; use `0` to disable. |

At-rest encryption is always enabled and bootstraps from the KEK/DEK metadata
under the data directory; there is no separate static encryption-key flag.

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

Explain a request-shaped S3 decision before testing with a real client:

```bash
grainfs iam explain --sa <sa_id> --s3 put s3://mybucket/path/to/object
grainfs iam explain --sa <sa_id> --s3 ls s3://mybucket
```

The command prints the derived IAM action and resource, then runs the same
server-side simulator used by `grainfs iam policy simulate`.

Rotate an access key with a client rollover window:

```bash
grainfs iam key create <sa_id>
grainfs iam key revoke <sa_id> <old_access_key>
```

## Protocol credentials

`grainfs credential` is the shared admin surface for protocol-scoped
credentials. It creates a credential for one service account, protocol,
resource, and access mode:

```bash
grainfs credential create \
  --sa <sa_id> \
  --protocol nbd \
  --resource volume/v1 \
  --mode rw
```

The response includes a one-time `secret` and a protocol-specific
`connection_hint`. Credentials do not expire by default; pass
`--expires-at <RFC3339>` when you want a planned rotation deadline. Use
`grainfs credential rotate <id>` to issue a replacement secret and
`grainfs credential revoke <id>` to disable a credential.

Protocol credential metadata is stored through Meta Raft so create, rotate, and
revoke state survives node restart and snapshot restore. Create, rotate, and
revoke now require the target service account to be allowed for
`grainfs:CredentialCreate`, `grainfs:CredentialRotate`, or
`grainfs:CredentialRevoke` on `protocol-credential/<protocol>/<resource>`:

```json
{
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "grainfs:CredentialCreate",
        "grainfs:CredentialList",
        "grainfs:CredentialRead",
        "grainfs:CredentialRotate",
        "grainfs:CredentialRevoke"
      ],
      "Resource": "protocol-credential/nbd/volume/v1"
    }
  ]
}
```

S3 and Iceberg accept protocol credentials as SigV4 access keys. S3 credentials
are scoped to one bucket (`--protocol s3 --resource bucket/<bucket>`); Iceberg
credentials are scoped to one catalog (`--protocol iceberg --resource
catalog/<warehouse>`). Use the returned `id` as the SigV4 access key and the
one-time `secret` as the SigV4 secret key. Read-only credentials allow read
operations only. S3 `CopyObject` is limited to same-bucket copies for protocol
credentials.

NFS attach paths enforce protocol credentials when credential storage is wired.
NFS uses `connection_hint.mount_path` (`bucket/credential-id:secret`). Read-only
credentials mount successfully but reject mutation operations.

List inventory can be narrowed to the same resource scope:

```bash
grainfs credential list --protocol nbd --resource volume/v1 --endpoint ./tmp/admin.sock
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

## Encryption Key Rotation

GrainFS encrypts bulk data at rest with XAES-256-GCM. The active Key Encryption Key (KEK) wraps
per-node Data Encryption Keys (DEKs). On a cluster, DEK material is leader-driven and
Raft-replicated so every node shares the same active DEK. Use `grainfs encrypt kek` to
manage the KEK lifecycle.

Check current status:

```bash
grainfs encrypt kek status --endpoint ./data/admin.sock
grainfs encrypt kek status --endpoint ./data/admin.sock --format json
```

Rotate to a new KEK version (generates a fresh KEK and re-wraps the active DEK under it):

```bash
# --i-know confirms you understand this triggers a cluster-wide re-wrap.
grainfs encrypt kek rotate --endpoint ./data/admin.sock --i-know
```

After confirming the new version is healthy (all nodes report the new active version via
`kek status`), retire the old version. Retire marks the version inactive and begins
draining any in-flight leases:

```bash
grainfs encrypt kek retire --version <old-version> --endpoint ./data/admin.sock
```

Once leases are fully drained (confirmed by `kek status` showing zero lease count for
that version), prune removes it permanently. `--confirm-name` must match the
`cluster_name` / node-id shown by `kek status` to prevent accidental cross-cluster
prune:

```bash
grainfs encrypt kek prune \
  --version <old-version> \
  --confirm-name <node-id> \
  --endpoint ./data/admin.sock
```

> **Multi-node note:** all four subcommands require every cluster voter to advertise the
> `kek_envelope_v1` capability (shipped in 0.0.353.0). In a mixed-version rolling upgrade
> the commands return 503 until the upgrade is complete.

See `../operators/runbook.md` for keystore disk-full and DEK rotation cadence procedures.

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

Revoke a Zero-CA node identity so the same node ID and transport certificate
cannot rejoin:

```bash
grainfs cluster revoke-node <node-id>
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
