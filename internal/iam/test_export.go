package iam

// ApplyBucketUpstreamForTest exposes the package-private Store apply for
// cross-package tests (e.g., pullthrough.Resolver). Production callers go
// through the FSM applier path.
func (s *Store) ApplyBucketUpstreamForTest(u BucketUpstream) {
	s.applyBucketUpstreamPut(u)
}

// ApplyBucketUpstreamDeleteForTest exposes the package-private delete.
func (s *Store) ApplyBucketUpstreamDeleteForTest(bucket string) {
	s.applyBucketUpstreamDelete(bucket)
}
