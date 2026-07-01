package iam

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestApplyBucketUpstream_GenerationCAS exercises the full per-record
// generation + CAS-on-delete lifecycle: monotonic bump on put, stale-gen delete
// no-op, matching-gen delete, and the unconditional sentinel.
func TestApplyBucketUpstream_GenerationCAS(t *testing.T) {
	s := NewStore()
	enc := newTestEncryptor(t)
	ap := NewApplier(s, enc)

	wrapped, _, err := WrapSecret(enc, "bucket-upstream:buc", "AK", "sk")
	require.NoError(t, err)
	put := buildBucketUpstreamPutPayload(BucketUpstream{
		Bucket: "buc", Endpoint: "http://x", AccessKey: "AK", SecretKeyEnc: wrapped,
	})

	// 1. First put → generation 1.
	require.NoError(t, ap.ApplyBucketUpstreamPut(put))
	u, ok := s.LookupBucketUpstream("buc")
	require.True(t, ok)
	require.Equal(t, uint64(1), u.Generation, "first put → gen 1")

	// 2. Second put (same bucket) → generation 2.
	require.NoError(t, ap.ApplyBucketUpstreamPut(put))
	u, ok = s.LookupBucketUpstream("buc")
	require.True(t, ok)
	require.Equal(t, uint64(2), u.Generation, "second put → gen 2")

	// 3. Delete with stale observedGen (1) → CAS mismatch, record stays.
	require.NoError(t, ap.ApplyBucketUpstreamDelete(buildBucketUpstreamDeletePayload("buc", 1)))
	_, ok = s.LookupBucketUpstream("buc")
	require.True(t, ok, "stale-gen delete must be a no-op (CAS mismatch)")

	// 4. Delete with matching observedGen (2) → record removed.
	require.NoError(t, ap.ApplyBucketUpstreamDelete(buildBucketUpstreamDeletePayload("buc", 2)))
	_, ok = s.LookupBucketUpstream("buc")
	require.False(t, ok, "matching-gen delete removes the record")

	// 5. Re-put → generation resets to 1 (record was absent); then an
	//    unconditional-sentinel delete removes it regardless of generation.
	require.NoError(t, ap.ApplyBucketUpstreamPut(put))
	u, ok = s.LookupBucketUpstream("buc")
	require.True(t, ok)
	require.Equal(t, uint64(1), u.Generation, "re-put after delete → gen 1 again")

	require.NoError(t, ap.ApplyBucketUpstreamDelete(buildBucketUpstreamDeletePayload("buc", UnconditionalDeleteGen)))
	_, ok = s.LookupBucketUpstream("buc")
	require.False(t, ok, "unconditional sentinel deletes regardless of generation")
}
