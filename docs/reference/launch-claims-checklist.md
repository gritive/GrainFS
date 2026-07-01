# `GrainFS` Launch Claims Checklist

Use this checklist before publishing website copy, launch posts, sales notes, or
competitive comparisons. Every public claim should map to repo evidence and use
wording that does not outrun the compatibility matrix, benchmark methodology, or
removal history.

## Review rules

- Treat [`s3-compatibility.md`](s3-compatibility.md) as the source of truth for
  S3 API and S3 client compatibility. `Supported` requires e2e, conformance, or
  real-client integration coverage; unit tests alone do not qualify.
- Treat [`benchmarks.md`](benchmarks.md) as the source of truth for publishable
  performance numbers. Include the environment, object size, concurrency,
  durability mode, encryption mode, date, and artifact reference.
- Do not market retired protocol surfaces as shipped behavior. NFSv4, 9P, NBD /
  volume devices, and Iceberg catalog/log-lake support are removal-history
  references only.
- Use `not supported` or `not planned` wording for surfaces marked that way in
  the current docs. Use `roadmap` or `future` only when an accepted roadmap item
  or owner-approved plan exists.
- Get owner approval before using any comparison against MinIO, Ceph, SeaweedFS,
  JuiceFS, Garage, cloud S3-compatible storage, or other named products.

## Claim map

| Public claim area | Current evidence | Safe launch wording | Caveat / not-a-claim | Owner approval needed? |
| --- | --- | --- | --- | --- |
| S3 API basics | `README.md` "What It Supports"; [`s3-compatibility.md`](s3-compatibility.md) bucket/object/multipart/auth rows | "GrainFS is an S3-compatible object store for covered bucket, object, multipart, SigV4, presigned URL, and browser form-upload flows." | Do not imply full AWS S3 parity. The matrix lists unsupported S3 Select, object tagging, CORS, website hosting, bucket replication, KMS/SSE-C, and other gaps. | Yes for broad launch copy; no for internal release notes that quote the matrix. |
| S3 clients | [`s3-compatibility.md`](s3-compatibility.md) client compatibility rows | "AWS CLI, boto3, rclone direct S3, and MinIO `mc` are covered; rclone mount / FUSE-over-S3 is partial because it inherits S3 semantics." | Do not claim POSIX filesystem behavior over FUSE-over-S3; rename is copy+delete and chmod/chown/locking are not supported. Do not claim s3fs/goofys support yet. | Yes for public client-support lists. |
| S3 access control and ACLs | [`s3-compatibility.md`](s3-compatibility.md) Access control rows | "GrainFS supports bucket policies and a documented subset of canned ACL write/copy headers, with unsupported ACL forms rejected fail-closed." | Public-read anonymous authorization and multipart-complete ACL stamping are not yet covered; explicit grant headers and unsupported canned ACLs are rejected. | Yes. |
| Encryption / security posture | `README.md` encryption row and core concept; [`../operators/deploy-production-cluster.md`](../operators/deploy-production-cluster.md); [`../users/guide.md`](../users/guide.md) encryption section | "GrainFS encrypts data at rest with XAES-256-GCM by default and manages the KEK/DEK hierarchy with admin operations for KEK lifecycle." | Do not claim external KMS or SSE-KMS/SSE-C support; S3 compatibility marks those as not supported. Avoid compliance claims unless separately reviewed. | Yes. |
| Cluster durability | `README.md` cluster durability row; [`../architecture/durability-and-placement.md`](../architecture/durability-and-placement.md); [`../operators/runbook.md`](../operators/runbook.md) | "GrainFS provides zero-config cluster placement with Raft-managed control plane, erasure-coded object data, and quorum metadata." | Do not equate GrainFS internal replication with S3 bucket replication; the S3 matrix explicitly does not claim bucket replication compatibility. | Yes. |
| Operations | `README.md` operations row; [`../operators/runbook.md`](../operators/runbook.md); [`../operators/sli-slo.md`](../operators/sli-slo.md) | "Operators get documented runbooks, metrics/SLO guidance, balancer status, incident views, and recovery-drill procedures." | This repository has no configured production URL or automatic post-deploy canary target. Avoid claiming managed service operations. | Yes for launch copy. |
| Performance | `README.md` performance tables; [`benchmarks.md`](benchmarks.md); `benchmarks/README.md` | "In the latest documented encrypted GCP `warp` runs, GrainFS measured 215.50 MiB/s PUT and 437.02 MiB/s GET single-node, and 434.79 MiB/s PUT and 1834.58 MiB/s GET on a 4-node cluster, under the stated benchmark conditions." | Always include benchmark conditions. Do not generalize to all workloads, all object sizes, or full product superiority. Do not cite local-only or incomplete benchmark artifacts. | Yes, always. |
| NFSv4 | `CHANGELOG.md` entries for `0.0.637.0` and `0.0.638.0` | "NFSv4 is not currently shipped by GrainFS." | Not a launch claim. Do not list NFSv4 as supported, partial, or roadmap without a new accepted plan. | Yes if mentioned publicly. |
| 9P | `CHANGELOG.md` entries for `0.0.636.0` and `0.0.638.0` | "9P is not currently shipped by GrainFS." | Not a launch claim. Do not list 9P as supported, partial, or roadmap without a new accepted plan. | Yes if mentioned publicly. |
| NBD / volume devices | `CHANGELOG.md` entries noting the removed volume/NBD subsystem and removed leftover NBD references | "NBD / volume-device support is not currently shipped by GrainFS." | Not a launch claim. Do not imply block-device or volume service support. | Yes if mentioned publicly. |
| Iceberg catalog / log lake | `CHANGELOG.md` entries for `0.0.663.0` and `0.0.664.0`; `internal/protocred/iceberg_removal_test.go` | "GrainFS is currently a pure S3 plus Web UI server; the Iceberg catalog and audit log lake were removed." | Not a launch claim. Do not claim DuckDB/Trino/Spark Iceberg catalog integration or Iceberg-backed audit analytics. | Yes if mentioned publicly. |
| Product comparisons | [`benchmarks.md`](benchmarks.md) comparison principles; current compatibility matrix | "Compare only the specific tested surface: e.g. a particular S3 API/client row or a benchmark run with matching durability, encryption, object size, concurrency, and hardware." | Do not compare against MinIO, Ceph, SeaweedFS, JuiceFS, Garage, or cloud object stores without a row-by-row evidence map and owner approval. | Yes, always. |

## Pre-publication checklist

Before publishing, paste the draft into the PR or release notes and tick each item:

- [ ] Every product/protocol claim has a row in the claim map above or a linked
      compatibility/benchmark document.
- [ ] `Supported`, `Partial`, `Not supported`, and `Not planned` wording matches
      the current compatibility matrix.
- [ ] Retired surfaces (NFSv4, 9P, NBD / volume devices, Iceberg catalog/log
      lake) are either omitted or explicitly described as not currently shipped.
- [ ] Performance numbers include date, environment, object size, concurrency,
      durability mode, encryption mode, and artifact/source document.
- [ ] Named-competitor comparisons have owner approval.
- [ ] Marketing reviewed the final wording against this checklist.
