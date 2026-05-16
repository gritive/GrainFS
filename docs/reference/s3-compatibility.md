# `GrainFS` S3 Compatibility Matrix

This matrix tracks externally visible compatibility. Mark a feature `Supported`
only when `GrainFS` has e2e, conformance, or real client integration coverage for
it. Unit tests alone do not qualify.

Use [`rolling-upgrade-compatibility.md`](rolling-upgrade-compatibility.md) for
binary and data compatibility across rolling upgrades.

## Status Definitions

| Status        | Meaning                                                                                              |
| ------------- | ---------------------------------------------------------------------------------------------------- |
| Supported     | Covered by e2e, conformance, or real client integration tests.                                       |
| Partial       | Integration-tested, but with known semantic or scope limits.                                         |
| Not supported | `GrainFS` does not implement or claim this compatibility surface.                                      |
| Not planned   | Intentionally outside the product scope.                                                             |

## S3 API Compatibility

| Area              | Operation or surface                    | Status        | Notes                                                                            |
| ----------------- | --------------------------------------- | ------------- | -------------------------------------------------------------------------------- |
| Bucket basics     | CreateBucket                            | Supported     |                                                                                  |
| Bucket basics     | HeadBucket                              | Supported     |                                                                                  |
| Bucket basics     | ListBuckets                             | Supported     | Internal buckets are excluded from user listing.                                 |
| Bucket basics     | DeleteBucket                            | Supported     |                                                                                  |
| Bucket basics     | GetBucketLocation                       | Supported     | Returns `us-east-1`.                                                             |
| Object basics     | PutObject                               | Supported     |                                                                                  |
| Object basics     | GetObject                               | Supported     |                                                                                  |
| Object basics     | HeadObject                              | Supported     |                                                                                  |
| Object basics     | DeleteObject                            | Supported     |                                                                                  |
| Object basics     | ListObjects with prefix                 | Supported     |                                                                                  |
| Object basics     | Nested keys                             | Supported     |                                                                                  |
| Object basics     | Overwrite object                        | Supported     |                                                                                  |
| Object reads      | Range GET                               | Supported     |                                                                                  |
| Object reads      | Conditional headers                     | Supported     |                                                                                  |
| Multipart         | Create/upload/complete multipart upload | Supported     |                                                                                  |
| Multipart         | Abort multipart upload                  | Supported     |                                                                                  |
| Multipart         | Multipart listing APIs                  | Supported     | Single-node e2e and cluster e2e cover ListMultipartUploads and ListParts for incomplete uploads; legacy uploads created before bucket/key metadata are complete/abort capable but omitted from listing. |
| Auth              | AWS Signature Version 4                 | Supported     |                                                                                  |
| Auth              | Presigned GET/PUT URL                   | Supported     |                                                                                  |
| Auth              | Browser POST policy/form upload         | Supported     |                                                                                  |
| Access control    | Bucket policy set/get/deny              | Supported     |                                                                                  |
| Access control    | ACL header on object write/copy         | Supported     |                                                                                  |
| Bucket controls   | Versioning                              | Supported     |                                                                                  |
| Bucket controls   | Lifecycle config replication            | Supported     | Replicates lifecycle configuration through cluster metadata.                     |
| Bucket controls   | Lifecycle expiration/transition effects | Not supported | Configuration replication exists; S3 lifecycle action semantics are not claimed. |
| Bucket controls   | Object tagging                          | Not supported |                                                                                  |
| Bucket controls   | CORS                                    | Not supported |                                                                                  |
| Bucket controls   | Static website hosting                  | Not supported |                                                                                  |
| Bucket controls   | Bucket notification configuration       | Not supported | Internal events exist, but S3 bucket notification compatibility is not claimed.  |
| Bucket controls   | Bucket replication                      | Not supported | `GrainFS` has Raft/EC replication; S3 bucket replication is not claimed.           |
| Object governance | Object Lock, retention, legal hold      | Not supported |                                                                                  |
| Query             | S3 Select                               | Not supported |                                                                                  |
| Encryption        | SSE-S3/SSE-KMS/SSE-C S3 headers         | Not supported | Stored bytes are encrypted; S3 SSE headers, KMS keys, and SSE-C keys are not claimed. |

## Client Compatibility

| Client or integration       | Status     | Notes                                                                                              |
| --------------------------- | ---------- | -------------------------------------------------------------------------------------------------- |
| AWS CLI                     | Supported  |                                                                                                    |
| boto3                       | Supported  |                                                                                                    |
| rclone direct S3            | Supported  |                                                                                                    |
| rclone mount / FUSE-over-S3 | Partial    | S3 semantics mean rename is copy+delete and POSIX chmod/chown/locking are not supported over FUSE. |
| s3fs/goofys                 | Not supported | No compatibility claim without real-client coverage.                                               |
| MinIO client (`mc`)         | Not supported | No compatibility claim without real-client coverage.                                               |
