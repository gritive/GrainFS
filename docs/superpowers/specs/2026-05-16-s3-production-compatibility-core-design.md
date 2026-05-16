# S3 Production Compatibility Core Design

## Context

The compatibility matrix no longer uses `Not tested`. For S3-first production,
the remaining production-relevant unsupported surfaces are:

- Multipart listing APIs in cluster/default mode.
- S3 server-side-encryption request and response header compatibility.
- Lifecycle expiration effects.

This design intentionally excludes NFS/9P authentication, POSIX compatibility
gaps, Iceberg Spark/Trino/PyIceberg clients, S3 Object Lock, S3 Select, object
tagging, CORS, static website hosting, bucket notifications, and full external
KMS integration.

## Goals

1. Make multipart listing APIs supportable under the default clustered backend.
2. Add narrow SSE header compatibility without changing the existing at-rest
   encryption model.
3. Execute lifecycle expiration rules through existing lifecycle machinery.
4. Promote only rows that have e2e, conformance, or real-client integration
   coverage.

## Non-Goals

- Implement SSE-C customer-key encryption.
- Implement a real KMS envelope encryption service.
- Implement lifecycle transition to storage classes.
- Implement noncurrent-version lifecycle actions.
- Implement multipart abort lifecycle rules.
- Broaden S3 compatibility beyond the three production-core areas above.

## Design

### Multipart Listing

`DistributedBackend.ListMultipartUploads` currently returns an empty list in
cluster mode because the replicated multipart metadata does not preserve enough
bucket/key information for bucket-scoped listing.

Extend the cluster multipart metadata record to include:

- bucket
- key
- upload ID
- created timestamp
- content type
- placement group ID

Decoders must continue to read old records. Old records without bucket/key are
not listable, but upload-ID based operations such as complete and abort must
continue to work.

Expose listing only when every relevant cluster member advertises the new
multipart listing metadata capability. During rolling upgrade, reject listing
with an explicit unsupported response instead of returning partial truth.

`ClusterCoordinator.ListParts` should route to the owning placement group. If
the group is remote, add or extend a forward operation so `ListParts` observes
the same parts that complete uses.

### SSE Header Compatibility

Parse and validate SSE headers in the S3 handler layer and store compatibility
metadata with object facts.

Supported in this slice:

- `x-amz-server-side-encryption: AES256` (`SSE-S3`)
- `x-amz-server-side-encryption: aws:kms` with optional KMS key ID round-trip

The implementation does not claim external KMS envelope encryption. It records
the requested compatibility metadata and returns AWS-compatible response
headers on PUT, COPY, HEAD, and GET.

Unsupported in this slice:

- SSE-C headers

The compatibility matrix should split the current single SSE row into separate
rows so SSE-S3/SSE-KMS can become supported while SSE-C remains unsupported.

### Lifecycle Expiration

Reuse the existing `internal/lifecycle` store/service/worker instead of adding a
parallel subsystem.

Supported in this slice:

- Enabled `Expiration.Days`

The lifecycle worker should run leader-only and execute deletes through the
existing object delete path so routing, mutation accounting, audit emission, and
cluster consistency stay aligned with normal deletes.

`Expiration.Date`, transition rules, noncurrent-version rules, and
multipart-abort lifecycle rules remain unsupported rows.

## Data Compatibility

Multipart metadata changes require careful rolling-upgrade behavior. New fields
must be additive, old records must remain decodable, and list APIs must be
capability-gated until all nodes can produce and consume the richer metadata.

SSE metadata should be stored as small string fields on object metadata/facts,
not as a new encryption layer. Existing encrypted-at-rest behavior remains
unchanged.

Lifecycle config replication remains unchanged. The new behavior is the worker
acting on already-replicated config.

## Testing

Add focused tests for each compatibility claim:

- Cluster e2e: create multipart upload, upload a part, list uploads, list
  parts, abort, then verify listing removal.
- Rolling/capability test: multipart listing is rejected while any peer lacks
  the new metadata capability.
- SSE e2e: PUT with SSE-S3 and SSE-KMS headers, then verify PUT/HEAD/GET/COPY
  response header round-trip.
- SSE negative test: SSE-C request returns a clear unsupported or invalid
  request error without storing customer key material.
- Lifecycle e2e: set short expiration, run lifecycle interval, verify expired
  object is deleted and delete behavior survives leader/follower access.
- Documentation test: compatibility matrix rows match supported/unsupported
  status and do not reintroduce ambiguous status labels.

## Rollout

Implement in three independent slices:

1. Multipart listing metadata and routing.
2. SSE header compatibility and matrix split.
3. Lifecycle expiration execution.

Each slice should update the compatibility matrix only after its e2e coverage is
passing.

## Fixed Decisions

- Lifecycle expiration support starts with `Expiration.Days` only.
- SSE-KMS accepts and round-trips any non-empty key ID while documenting that no
  external KMS is contacted.
