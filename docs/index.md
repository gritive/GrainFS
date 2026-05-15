# `GrainFS` Documentation

Start from the role that matches the work in front of you. The README stays
short; this index keeps the detailed docs reachable without turning the README
into a file list.

## Users

Use these when you are integrating `GrainFS` with clients, protocols, or table
workflows.

| Need | Document | Type |
| --- | --- | --- |
| Run `GrainFS` locally and use common commands | [User guide](users/guide.md) | Tutorial / how-to |
| Use DuckDB with the Iceberg REST Catalog | [Iceberg DuckDB guide](users/iceberg-duckdb.md) | How-to |
| Query S3 audit data as an Iceberg table | [Audit Iceberg guide](users/audit-iceberg.md) | How-to / explanation |
| Check S3 compatibility before choosing a client feature | [S3 compatibility](reference/s3-compatibility.md) | Reference |
| Check file protocol behavior | [NFSv4 compatibility](reference/nfs-compatibility.md), [9P compatibility](reference/9p-compatibility.md) | Reference |
| Check block protocol behavior | [NBD compatibility](reference/nbd-compatibility.md) | Reference |
| Check Iceberg client support | [Iceberg compatibility](reference/iceberg-compatibility.md) | Reference |

## Operators

Use these when you are deploying, monitoring, recovering, or running drills.

| Need | Document | Type |
| --- | --- | --- |
| Deploy and operate a production node or cluster | [Production runbook](operators/runbook.md) | How-to / runbook |
| Back up and restore data | [Backup and restore](operators/backup-restore.md) | How-to |
| Plan disaster recovery | [Disaster recovery](operators/disaster-recovery.md) | How-to |
| Run failure drills | [Drill manual](operators/drill-manual.md) | Tutorial / runbook |
| Recover a cluster offline | [Cluster recovery](operators/recover-cluster.md) | How-to |
| Track service objectives | [SLI/SLO](operators/sli-slo.md) | Reference |
| Operate NFS exports | [NFS export lifecycle](operators/nfs-export-lifecycle.md), [NFS debug](operators/nfs-debug.md) | How-to |
| Operate the balancer | [Balancer operations](operators/balancer.md) | How-to |
| Roll back managed Badger paths | [Badger managed-mode rollback](operators/badger-managed-mode-rollback.md) | How-to |

## Reference

Use these when you need exact behavior, test-backed support claims, or benchmark
rules.

| Need | Document |
| --- | --- |
| S3 API and client compatibility | [S3 compatibility](reference/s3-compatibility.md) |
| NFSv4 compatibility and attribute audit | [NFSv4 compatibility](reference/nfs-compatibility.md), [NFSv4 attribute audit](reference/nfsv4-attribute-audit.md) |
| 9P compatibility | [9P compatibility](reference/9p-compatibility.md) |
| NBD compatibility | [NBD compatibility](reference/nbd-compatibility.md) |
| Iceberg REST Catalog compatibility | [Iceberg compatibility](reference/iceberg-compatibility.md) |
| Benchmark methodology | [Benchmarks](reference/benchmarks.md) |
| Rolling upgrade compatibility | [Rolling upgrade compatibility](reference/rolling-upgrade-compatibility.md) |
| Iceberg request trace | [Iceberg DuckDB request trace](reference/iceberg-duckdb-request-trace.md) |
| Transport versioning | [Transport mux versioning](reference/transport-mux-versioning.md) |
| Upgrade finalization design | [Upgrade finalize machinery](reference/upgrade-finalize-machinery-design.md) |

## Explanation

Use these when you need the design rationale behind the system.

| Need | Document |
| --- | --- |
| Protocol layering | [Protocol layering](architecture/protocol-layering.md) |
| Admin, identity, and bucket configuration | [Admin and identity architecture](architecture/admin-identity-and-bucket-config.md) |
| BadgerDB consolidation history | [BadgerDB consolidation](architecture/badger-consolidation.md) |
| Durability and object placement | [Durability and placement](architecture/durability-and-placement.md) |
| Storage operation boundaries | [Storage operations facade](architecture/storage-operations-facade.md) |
| Cache invalidation | [Cache invalidation flow](architecture/cache-invalidation-flow.md) |
| NFS client testing approach | [NFS client spike](architecture/nfs-client-spike.md) |
| Raft RPC stream multiplexing | [QUIC stream multiplexing](architecture/quic-stream-multiplex.md) |
