# NFS Debug Runbook

Use `grainfs nfs debug <bucket>` when a mount cannot find a bucket, a client sees
stale handles, or operators need to verify the admin registry and backend bucket
agree.

```bash
export GRAINFS_ADMIN_SOCKET=<data>/admin.sock
grainfs nfs debug mydata
grainfs nfs debug mydata --json
```

The command reports:

- export registry status, mode, fsid, and generation
- backend bucket existence and object count
- recent pseudo-root LOOKUPs observed by the NFS server
- best-effort propagation and active-client diagnostics

## Common Scenarios

### Client sees `No such file or directory` at the pseudo-root

Run `grainfs nfs export list`. If the bucket is missing, register it:

```bash
grainfs nfs export add <bucket>
```

Server logs include a rate-limited hint for unknown pseudo-root LOOKUPs.

### Client sees stale file handle

Run `grainfs nfs debug <bucket>` and compare the reported generation with recent
export changes. Remount the client if the handle was created before an export
remove/re-add or mode transition.

### Backend bucket exists but export is missing

The bucket is reachable through S3 but not NFS. Register the export, then remount
or browse the pseudo-root again.

## Log Mapping

- Linux client `dmesg`: `NFS4ERR_NOENT` near mount or `ls /mnt/grainfs/<bucket>`
- Server log: `lookup unknown export ... grainfs nfs export add <bucket>`
- Admin CLI: `grainfs nfs debug <bucket>` shows `registered=false`
