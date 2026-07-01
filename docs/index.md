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
| Check S3 compatibility before choosing a client feature | [S3 compatibility](reference/s3-compatibility.md) | Reference |

## Operators

Use these when you are deploying, monitoring, recovering, or running drills.
Current cluster traffic is streaming HTTP over TCP with SPKI-pinned mTLS; QUIC
and mux-carrier documents listed below are historical design records, not live
operator setup instructions.

| Need | Document | Type |
| --- | --- | --- |
| Deploy and operate a production node or cluster | [Production runbook](operators/runbook.md) | How-to / runbook |
| Track service objectives | [SLI/SLO](operators/sli-slo.md) | Reference |
| Operate federated IAM authorization | [OIDC federated IAM](operators/oidc-federated-iam.md) | How-to / explanation |
| Operate the balancer | [Balancer operations](operators/balancer.md) | How-to |
| Roll back managed Badger paths | [Badger managed-mode rollback](operators/badger-managed-mode-rollback.md) | How-to |

## Reference

Use these when you need exact behavior, test-backed support claims, or benchmark
rules.

| Need | Document |
| --- | --- |
| S3 API and client compatibility | [S3 compatibility](reference/s3-compatibility.md) |
| Public launch wording and evidence mapping | [Launch claims checklist](reference/launch-claims-checklist.md) |
| Benchmark methodology | [Benchmarks](reference/benchmarks.md) |
| Rolling upgrade compatibility | [Rolling upgrade compatibility](reference/rolling-upgrade-compatibility.md) |
| Retired transport mux rationale | [Historical transport mux versioning](reference/transport-mux-versioning.md) |
| Upgrade finalization design | [Upgrade finalize machinery](reference/upgrade-finalize-machinery-design.md) |

## Explanation

Use these when you need the design rationale behind the system.

| Need | Document |
| --- | --- |
| Protocol layering | [Protocol layering](architecture/protocol-layering.md) |
| Admin, identity, and bucket configuration | [Admin and identity architecture](architecture/admin-identity-and-bucket-config.md) |
| BadgerDB consolidation history | [BadgerDB consolidation](architecture/badger-consolidation.md) |
| Single/cluster request execution | [Request execution actor flow](architecture/request-single-cluster-flow.md) |
| Durability and object placement | [Durability and placement](architecture/durability-and-placement.md) |
| Storage operation boundaries | [Storage operations facade](architecture/storage-operations-facade.md) |
| Retired Raft RPC stream multiplexing | [Historical stream multiplexing + heartbeat coalescing](architecture/quic-stream-multiplex.md) |
