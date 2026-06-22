# NFS Quick Start

GrainFS exposes the same data via S3 and NFSv4. Mount once, read/write either way.

## Anonymous mount on the default bucket

Start GrainFS and register the default bucket as an NFS export:

```bash
./grainfs serve --data ./tmp --port 9000 &

# Register the export (bucket creation is automatic on first S3 write,
# but NFS requires an explicit export registration).
grainfs nfs export add default --endpoint ./tmp/admin.sock
```

**NFSv4 mount (Linux):**

```bash
sudo mount -t nfs4 -o nolock,nfsvers=4.0 localhost:/default /mnt/data
```

That's it. Read/write under `/mnt/data` lands in the `default` bucket and is visible via the S3 endpoint at the same time:

```bash
ls /mnt/data                                                      # via NFS
aws --no-sign-request --endpoint-url http://localhost:9000 s3 ls s3://default/  # via S3
```

> Anonymous mount access is decided by the bucket/export authorization path.
> The bucket must be registered as an export; S3 bucket creation alone does not
> expose the bucket over NFS.

## Authenticated mount with a Mount SA

S3 auth is active after you bootstrap the first admin SA
(`grainfs iam sa create`). Mounts require a Mount SA with an attached policy.

`grainfs credential` is the new shared admin surface for protocol credentials
across S3, Iceberg, and NFS. NFS accepts either the Mount SA path below or a
protocol credential connection hint. Mount SA remains useful when you want a
stable operator-named principal with explicit uid metadata; protocol credentials
are the rotation-friendly path.

### 1. Create a Mount SA

```bash
grainfs iam mount-sa create alice-mount --uid 1000 --endpoint ./tmp/admin.sock
```

The `--uid` value is the numeric UID that the NFS principal reports. It has
no effect on S3 policy evaluation; it is advisory metadata for the client host.

### 2. Attach a builtin policy

For NFSv4 access attach `NFSMountOnly`:

```bash
grainfs iam mount-sa policy attach alice-mount NFSMountOnly --endpoint ./tmp/admin.sock
```

This builtin policy grants the mount SA read/write access to any exported
bucket it is authorized on. To restrict to a specific bucket, write a custom
policy and attach it instead.

### 3. Register the export

```bash
grainfs nfs export add my-bucket --endpoint ./tmp/admin.sock
```

### 4. Mount

**NFSv4** — the mount path encodes `<bucket>/<mount-sa>`:

```bash
sudo mount -t nfs4 -o nolock,nfsvers=4.0 localhost:/my-bucket/alice-mount /mnt/data
```

## Protocol credential mount

Create protocol credentials against the target bucket and use the returned
connection hints directly:

```bash
# NFSv4
grainfs credential create --sa <sa_id> --protocol nfs --resource bucket/my-bucket --mode rw --endpoint ./tmp/admin.sock
sudo mount -t nfs4 -o nolock,nfsvers=4.0 localhost:/<connection_hint.mount_path> /mnt/data
```

The NFS mount path is `bucket/credential-id:secret`. Read-only credentials mount
successfully and return `NFS4ERR_ROFS` for mutations.

## Read-only export

Use `--ro` when registering the export to enforce server-side read-only for all
mounts on that bucket:

```bash
grainfs nfs export add my-bucket --ro --endpoint ./tmp/admin.sock
```

To flip an existing export to read-only with a quiesce window:

```bash
grainfs nfs export update my-bucket --ro --quiesce-wait 30s --endpoint ./tmp/admin.sock
```

Clients that already have the export mounted will receive `NFS4ERR_ROFS` on any
write attempt after the update propagates.

## Audit NFS access

The `audit.s3` Iceberg table records all NFS accesses with the `source` column
set to `nfs4`:

```bash
grainfs audit query "
  SELECT sa_id, source, source_ip, operation, bucket, ts
  FROM grainfs_iceberg.audit.s3
  WHERE source = 'nfs4'
  ORDER BY ts DESC
  LIMIT 20
" --endpoint ./tmp/admin.sock
```

## Known limits

- `AUTH_SYS` uid is trusted on NFSv4. Restrict NFS port access to LAN or
  trusted hosts via firewall or IP ACL; any host that can reach the port can
  present any uid.
- macOS NFS client buffers writes aggressively in the page cache. Use Linux
  clients for write-after-read verification in tests.
- Mount SA pool miss (authenticated path with a Mount SA name that does not
  exist) returns `NFS4ERR_NOENT`. There is no anonymous fallback once a Mount
  SA name is supplied.
