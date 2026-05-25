# GrainFS Documentation

GrainFS is a single-binary storage server that exposes one storage layer through S3, NFSv4, 9P, NBD, and Iceberg REST Catalog interfaces.

Use this index by task. Internal planning artifacts, ADR drafts, and agent work logs are not part of the customer documentation set.

## Start here

| Need | Document |
| --- | --- |
| Run GrainFS locally and write the first object | [Quickstart](start-here/quickstart.md) |
| Install or build the binary | [Install](start-here/install.md) |
| Put and list an object with the AWS CLI | [First S3 object](start-here/first-s3-object.md) |
| Mount a bucket with NFSv4 | [First NFS mount](start-here/first-nfs-mount.md) |
| Query Iceberg metadata with DuckDB | [First Iceberg query](start-here/first-iceberg-query.md) |

## Understand GrainFS

| Need | Document |
| --- | --- |
| Product overview and mental model | [Concepts overview](concepts/overview.md) |
| Buckets, object keys, paths, and volumes | [Namespace and buckets](concepts/namespace-and-buckets.md) |
| How S3, NFSv4, 9P, NBD, and Iceberg share one backend | [Same data, multiple protocols](concepts/same-data-multiple-protocols.md) |
| Admin socket, service accounts, access keys, and mount identities | [Identity and admin socket](concepts/identity-and-admin-socket.md) |
| Phase 0 to Phase 3 lifecycle | [Cluster phases](concepts/cluster-phases.md) |
| Placement, Raft, erasure coding, and shard integrity | [Durability and placement](concepts/durability-and-placement.md) |
| What a compatibility claim means | [Consistency and compatibility](concepts/consistency-and-compatibility.md) |
| Network and authentication boundaries | [Security boundaries](concepts/security-boundaries.md) |

## Tutorials

| Goal | Tutorial |
| --- | --- |
| Learn the local single-node workflow | [Local single-node tutorial](tutorials/local-single-node.md) |
| Use S3 with the AWS CLI | [AWS CLI S3 tutorial](tutorials/aws-cli-s3.md) |
| Mount buckets with NFSv4 and 9P | [NFS and 9P mounts](tutorials/nfs-and-9p-mounts.md) |
| Create and attach an NBD volume | [NBD volume tutorial](tutorials/nbd-volume.md) |
| Use DuckDB with the Iceberg REST Catalog | [DuckDB Iceberg tutorial](tutorials/duckdb-iceberg.md) |
| Bring up a small cluster | [Three-node cluster tutorial](tutorials/three-node-cluster.md) |

## How-to guides

| Task area | Documents |
| --- | --- |
| Buckets | [Create and manage buckets](how-to/buckets/create-and-manage-buckets.md), [Configure an upstream](how-to/buckets/configure-upstream.md), [Migrate and cut over](how-to/buckets/migrate-and-cutover.md) |
| IAM | [Bootstrap a service account](how-to/iam/bootstrap-service-account.md), [Import AWS IAM policy JSON](how-to/iam/import-aws-policy.md), [Rotate keys](how-to/iam/rotate-keys.md) |
| Protocols | [Export an NFS bucket](how-to/protocols/export-nfs-bucket.md), [Mount 9P](how-to/protocols/mount-9p.md), [Create an NBD volume](how-to/protocols/create-nbd-volume.md) |
| Iceberg | [Configure OAuth2 Iceberg](how-to/iceberg/configure-oauth2-iceberg.md), [Query audit data as Iceberg](how-to/iceberg/query-audit-table.md) |
| Operations tasks | [Run doctor](how-to/operations/run-doctor.md), [Inspect status and dashboard](how-to/operations/inspect-status-dashboard.md), [Run benchmarks](how-to/operations/run-benchmarks.md) |

## Operations

| Need | Document |
| --- | --- |
| Operate production GrainFS | [Production runbook](operations/production-runbook.md) |
| Check a deployment before exposing it | [Deployment checklist](operations/deployment-checklist.md) |
| Move through Phase 0, 1, 2, and 3 | [Cluster lifecycle](operations/cluster-lifecycle.md) |
| Monitor service health | [Monitoring and SLO](operations/monitoring-and-slo.md) |
| Handle incidents | [Incidents](operations/incidents.md) |
| Plan disaster recovery | [Disaster recovery](operations/disaster-recovery.md) |
| Recover a cluster offline | [Recover cluster](operations/recover-cluster.md) |
| Upgrade safely | [Rolling upgrade](operations/rolling-upgrade.md) |
| Operate the balancer | [Balancer](operations/balancer.md) |
| Roll back Badger managed mode | [Badger rollback](operations/badger-rollback.md) |
| Debug auth | [Troubleshooting auth](operations/troubleshooting-auth.md) |
| Debug NFS | [Troubleshooting NFS](operations/troubleshooting-nfs.md) |
| Debug slow workloads | [Troubleshooting performance](operations/troubleshooting-performance.md) |

## Reference

| Need | Document |
| --- | --- |
| CLI command surface | [CLI reference](reference/cli.md) |
| `grainfs serve` flags | [Serve flags](reference/serve-flags.md) |
| Runtime configuration keys | [Config reference](reference/config.md) |
| Admin API surface | [Admin API reference](reference/admin-api.md) |
| Prometheus metrics | [Metrics reference](reference/metrics.md) |
| S3 support | [S3 compatibility](reference/s3-compatibility.md) |
| NFSv4 support | [NFSv4 compatibility](reference/nfs-compatibility.md), [NFSv4 attribute audit](reference/nfsv4-attribute-audit.md) |
| 9P support | [9P compatibility](reference/9p-compatibility.md) |
| NBD support | [NBD compatibility](reference/nbd-compatibility.md) |
| Iceberg support | [Iceberg compatibility](reference/iceberg-compatibility.md) |
| Rolling upgrade policy | [Rolling upgrade compatibility](reference/rolling-upgrade-compatibility.md) |
| Benchmarks and methodology | [Benchmarks](reference/benchmarks.md) |
| Terms | [Glossary](reference/glossary.md) |

## Architecture

| Need | Document |
| --- | --- |
| System map | [Architecture overview](architecture/overview.md) |
| Protocol layering | [Protocol layering](architecture/protocol-layering.md) |
| Single-node and cluster request flow | [Request execution actor flow](architecture/request-single-cluster-flow.md) |
| Admin, identity, and bucket configuration | [Admin and identity architecture](architecture/admin-identity-and-bucket-config.md) |
| Durability and object placement | [Durability and placement](architecture/durability-and-placement.md) |
| Storage operation boundaries | [Storage operations facade](architecture/storage-operations-facade.md) |
| Cache invalidation | [Cache invalidation flow](architecture/cache-invalidation-flow.md) |
| QUIC stream multiplexing | [QUIC stream multiplexing](architecture/quic-stream-multiplex.md) |
| Scrubber actor model | [Scrubber director actor](architecture/scrubber-director-actor.md) |
| BadgerDB consolidation | [BadgerDB consolidation](architecture/badger-consolidation.md) |
| Upgrade finalization | [Upgrade finalize machinery](architecture/upgrade-finalize-machinery.md) |

## Development

| Need | Document |
| --- | --- |
| Build and test GrainFS | [Build and test](development/build-and-test.md) |
| Understand repository layout | [Repository layout](development/repository-layout.md) |
| Add CLI commands without bloating `cmd/` | [Adding CLI commands](development/adding-cli-commands.md) |
| Make or update compatibility claims | [Compatibility claims](development/compatibility-claims.md) |
| Regenerate FlatBuffers bindings | [FlatBuffers](development/flatbuffers.md) |
| Prepare releases | [Release process](development/release-process.md) |
