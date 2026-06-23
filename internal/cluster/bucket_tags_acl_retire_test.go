package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// putTestObjectForRetire is a minimal PUT helper that writes a small blob
// through the full quorum-meta path (shardSvc wired) so readQuorumMetaCmd works.
func putTestObjectForRetire(t *testing.T, b *DistributedBackend, bucket, key string, body []byte) {
	t.Helper()
	ctx := context.Background()
	size := int64(len(body))
	_, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:      bucket,
		Key:         key,
		Body:        bytes.NewReader(body),
		ContentType: "application/octet-stream",
		SizeHint:    &size,
	})
	require.NoError(t, err)
}

// TestSetObjectTags_BlobObject_NoRaftFallback locks the behaviour that
// SetObjectTags on a greenfield (quorum-meta) object mutates the blob and
// advances MetaSeq — the blob RMW is the sole authority, no raft fallback.
func TestSetObjectTags_BlobObject_NoRaftFallback(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	putTestObjectForRetire(t, b, "b", "k", []byte("hello"))

	require.NoError(t, b.SetObjectTags("b", "k", "", []storage.Tag{{Key: "env", Value: "prod"}}))

	m, err := b.readQuorumMetaCmd("b", "k")
	require.NoError(t, err)
	require.Len(t, m.Tags, 1)
	require.Equal(t, "env", m.Tags[0].Key)
	require.Equal(t, "prod", m.Tags[0].Value)
	require.Greater(t, m.MetaSeq, uint64(0), "MetaSeq must advance on blob RMW")
}

// TestSetObjectACL_BlobObject_NoRaftFallback locks the behaviour that
// SetObjectACL on a greenfield (quorum-meta) object mutates the blob and
// advances MetaSeq — the blob RMW is the sole authority, no raft fallback.
func TestSetObjectACL_BlobObject_NoRaftFallback(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	putTestObjectForRetire(t, b, "b", "k", []byte("hello"))

	require.NoError(t, b.SetObjectACL("b", "k", 1))

	m, err := b.readQuorumMetaCmd("b", "k")
	require.NoError(t, err)
	require.Equal(t, uint8(1), m.ACL)
	require.Greater(t, m.MetaSeq, uint64(0), "MetaSeq must advance on blob RMW")
}
