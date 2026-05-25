# NFS / 9P Quick Start

GrainFS exposes the same data via S3, NFSv4, and 9P. Mount once, read/write either way.

## Phase 0 — Anonymous mount on the default bucket

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

**9P mount (Linux):**

```bash
sudo mount -t 9p -o trans=tcp,port=5640,aname=default localhost /mnt/data
```

That's it. Read/write under `/mnt/data` lands in the `default` bucket and is visible via the S3 endpoint at the same time:

```bash
ls /mnt/data                                                      # via NFS/9P
aws --no-sign-request --endpoint-url http://localhost:9000 s3 ls s3://default/  # via S3
```

> Phase 0 anonymous mount works only when `iam.anon-enabled=true` (the default
> for a fresh cluster). The bucket must also be registered as an export;
> S3 bucket creation alone does not expose the bucket over NFS or 9P.

## Phase 2 — Authenticated mount with a Mount SA

Phase 2 is active after you bootstrap the first admin SA (`grainfs iam sa create`).
Mounts require a Mount SA with an attached policy.

### 1. Create a Mount SA

```bash
grainfs iam mount-sa create alice-mount --uid 1000 --endpoint ./tmp/admin.sock
```

The `--uid` value is the numeric UID that the NFS/9P principal reports. It has
no effect on S3 policy evaluation; it is advisory metadata for the client host.

### 2. Attach a builtin policy

For NFSv4 access attach `NFSMountOnly`; for 9P attach `9PAttachOnly`:

```bash
# NFSv4
grainfs iam mount-sa policy attach alice-mount NFSMountOnly --endpoint ./tmp/admin.sock

# 9P
grainfs iam mount-sa policy attach alice-mount 9PAttachOnly --endpoint ./tmp/admin.sock
```

Both builtin policies grant the mount SA read/write access to any exported
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

**9P** — the `aname` parameter encodes `<mount-sa>@<bucket>`:

```bash
sudo mount -t 9p -o trans=tcp,port=5640,aname=alice-mount@my-bucket localhost /mnt/data
```

> The `aname=<mount-sa>@<bucket>` convention is required by the hugelgupf/p9
> library: `aname` is consumed as the first Walk component rather than a
> separate Tattach field. The format `uname=<mount-sa>, aname=<bucket>` shown
> in some plan drafts does not match the implementation.

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

Clients that already have the export mounted will receive `NFS4ERR_ROFS` / 9P
`EROFS` on any write attempt after the update propagates.

## Audit NFS/9P access

The `audit.s3` Iceberg table records all NFS and 9P accesses with the
`source` column set to `nfs4` or `9p`:

```bash
grainfs audit query "
  SELECT sa_id, source, source_ip, operation, bucket, ts
  FROM grainfs_iceberg.audit.s3
  WHERE source IN ('nfs4', '9p')
  ORDER BY ts DESC
  LIMIT 20
" --endpoint ./tmp/admin.sock
```

## Known limits

- `AUTH_SYS` uid is trusted on NFSv4. Restrict NFS/9P port access to LAN or
  trusted hosts via firewall or IP ACL; any host that can reach the port can
  present any uid.
- macOS NFS client buffers writes aggressively in the page cache. Use Linux
  clients for write-after-read verification in tests.
- Mount SA pool miss (authenticated path with an `@` in `aname` but the SA
  does not exist) returns `NFS4ERR_NOENT` or 9P `ENOENT`. There is no
  anonymous fallback once a Mount SA name is supplied.
- 9P uses `aname=<mount-sa>@<bucket>` due to hugelgupf/p9 lib convention.
  The `uname` field is ignored by GrainFS.
