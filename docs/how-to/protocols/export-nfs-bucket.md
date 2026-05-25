# NFS Export Lifecycle

`GrainFS` exports S3 buckets over NFSv4 only after an explicit registration:

```bash
grainfs bucket create mydata
grainfs nfs export add mydata
```

The admin API stores export state in the cluster metadata registry. The running
NFSv4 server refreshes its local snapshot after registry changes and binds file
handles to the export generation so stale handles fail closed.

## bucket-not-found

Create the bucket before exporting it:

```bash
grainfs bucket create <bucket>
grainfs nfs export add <bucket>
```

## export-already-exists

The export registry is idempotent by bucket name. Use `grainfs nfs export update`
to change read-only/read-write mode instead of adding a duplicate export.

## export-not-found

The requested bucket is not registered as an NFS export. Inspect active exports:

```bash
grainfs nfs export list
```

Then register or correct the bucket name.

## export-propagation-timeout

In clustered deployments, export changes must apply on NFS-serving nodes before
the admin command returns. Retry the command after checking node health and raft
apply lag.

## bucket-locked-by-export

`GrainFS` may protect an exported bucket from destructive bucket operations. Remove
the export first, wait for clients to unmount, then retry the bucket operation:

```bash
grainfs nfs export remove <bucket>
```
