# TODO

## Follow-ups

- **[P3] `UploadPart` ignores `Content-MD5` entirely.** Multipart part PUTs route
  through `uploadPart` → `uploadMultipartPart` before the normal PUT parsing, and
  the multipart storage API (`operations_multipart.go`) has no per-part digest
  parameter — so a malformed or mismatched `Content-MD5` on an `UploadPart` is
  silently accepted. Closing this needs a digest parameter threaded through the
  multipart upload-part storage API + handler. Pre-existing; surfaced by the
  Content-MD5-completeness plan-gate (codex). Out of scope for object PUT.

- **[P3] packblob large-object PUT can drop `ContentMD5Hex` on the direct storage API.**
  `PackedBackend.PutObjectWithRequest` passes large objects to the inner backend
  via `putInnerWithRequest`, which falls back to `PutObjectWithUserMetadata`
  (dropping `ContentMD5Hex`) when `ACL`/`SSE`/`SizeHint` are all empty. The S3
  HTTP handler always sets `SizeHint`, so the real S3 path is covered; only a
  direct `PutObjectWithRequest` large write without a `SizeHint` loses the digest.
  Fix: include `req.ContentMD5Hex != ""` in that helper's full-request
  preservation condition. Pre-existing; surfaced by the plan-gate.
