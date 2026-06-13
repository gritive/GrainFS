# TODO

## Follow-ups

- **[P2] Single-node packblob PUT skips `Content-MD5` (BadDigest) validation.**
  The small-object packed-blob fast path (`internal/storage/packblob/packed_backend.go`,
  the non-versioned branch around the `blobStore.Append` + `md5.Sum(data)` →
  `etag`) computes the body MD5 but never compares it to `req.ContentMD5Hex`, so
  a single-node PUT with a wrong `Content-MD5` commits instead of returning 400
  `BadDigest`. The cluster EC path now validates uniformly (this PR); packblob
  is the remaining gap. Pre-existing and single-node-only (cluster mode keeps EC
  shard storage). Fix: validate `hex(md5(data)) == req.ContentMD5Hex` **before**
  `blobStore.Append` (validating after Append would orphan a blob entry); return
  `storage.ErrContentMD5Mismatch`. Versioned-bucket PUTs already delegate to the
  inner backend, so only the small non-versioned pack path needs the guard.

- **[P3] Malformed `Content-MD5` header is silently ignored (should be `InvalidDigest`).**
  `internal/server/object_write_request.go` (`putObjectContentMD5Hex`) returns
  `""` on a non-base64 or non-16-byte `Content-MD5`, and every validator no-ops
  on empty — so `Content-MD5: not-base64` commits instead of returning 400
  `InvalidDigest` per the S3 contract. Server-wide pre-existing. Fix: distinguish
  "absent" from "present-but-malformed"; map the latter to `InvalidDigest`.

- **[P3] Cluster PUT-with-`x-amz-acl` does not persist the ACL on any node.**
  `ClusterCoordinator.PutObjectWithRequest` (and the forward path) ignore
  `req.ACL`; an object ACL is only stored via the separate `SetObjectACL`
  path (`cmd.ACL`). A `PUT` carrying `x-amz-acl` therefore silently drops the
  ACL whether it lands on a voter or a non-voter — the two paths are
  result-identical, which is why the S3 single-path #1 slice could leave this
  alone. True ACL persistence is a separate change: wire `req.ACL` into
  `PutObjectMetaCmd.ACL`, or apply the ACL via a `PUT`-then-`SetObjectACL`
  step. Add a test asserting `HeadObject`/`GetObjectACL` reflects the
  `x-amz-acl` sent on `PutObject`.
