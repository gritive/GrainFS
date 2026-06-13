# TODO

## Follow-ups

- **[P2] Forwarded PUTs skip `Content-MD5` (BadDigest) validation.**
  `Content-MD5` is only validated inside the put pipeline, which the storage
  layer enters only when `req.SizeHint != nil` (`object_put.go:49`). A
  direct-to-voter PUT always carries `SizeHint` (set from Content-Length in
  `object_write_api.go`), so a bad digest is rejected with `BadDigest`. The
  forward path does **not** carry `SizeHint`, so the receiver rebuilds the
  request without it and takes the non-pipeline fallback that never reads
  `ContentMD5Hex` — a bad digest is silently accepted. This divergence is
  **pre-existing** (it already applied to every forwarded PUT with a
  `Content-MD5` header, with or without user metadata) and is independent of
  the S3 single-path #1 change, which only extended forwarding to metadata
  PUTs. Carrying `content_md5_hex` over the wire alone does **not** fix it —
  the receiver's fallback path ignores it; the real fix is to forward
  `SizeHint` so forwarded PUTs enter the validating pipeline like direct ones.
  Add a test: a forwarded PUT with a mismatched `Content-MD5` must return
  `BadDigest`, matching the direct-to-voter path.

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
