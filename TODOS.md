# TODO

## Follow-ups

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
