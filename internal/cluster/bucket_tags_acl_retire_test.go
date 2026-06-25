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
// advances MetaSeq — the blob RMW is the blob authority, no raft fallback.
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
// advances MetaSeq — the blob RMW is the blob authority, no raft fallback.
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

func TestSetObjectTags_VersionIDMutatesPerVersionBlob(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")
	seedVersionBlob(t, b, "b", "k", "v1", PutObjectMetaCmd{
		ETag: "v1", Tags: []storage.Tag{{Key: "old", Value: "v1"}},
		NodeIDs: []string{b.currentSelfAddr()}, ECData: 1,
	})
	seedVersionBlob(t, b, "b", "k", "v2", PutObjectMetaCmd{
		ETag: "v2", Tags: []storage.Tag{{Key: "old", Value: "v2"}},
		NodeIDs: []string{b.currentSelfAddr()}, ECData: 1,
	})

	want := []storage.Tag{{Key: "target", Value: "v1"}}
	require.NoError(t, b.SetObjectTags("b", "k", "v1", want))

	v1, ok, err := b.readQuorumMetaVersion("b", "k", "v1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, v1.Tags)
	require.Greater(t, v1.MetaSeq, uint64(0), "targeted tag RMW must advance MetaSeq")

	v2, ok, err := b.readQuorumMetaVersion("b", "k", "v2")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []storage.Tag{{Key: "old", Value: "v2"}}, v2.Tags, "sibling version must be untouched")
}

func TestSetObjectACL_VersionedLatestMutatesPerVersionBlob(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	setVersioningForTest(t, b, "b", "Enabled")
	seedVersionBlob(t, b, "b", "k", "v1", PutObjectMetaCmd{
		ETag: "v1", ACL: 0,
		NodeIDs: []string{b.currentSelfAddr()}, ECData: 1,
	})
	seedVersionBlob(t, b, "b", "k", "v2", PutObjectMetaCmd{
		ETag: "v2", ACL: 0,
		NodeIDs: []string{b.currentSelfAddr()}, ECData: 1,
	})

	require.NoError(t, b.SetObjectACL("b", "k", 1))

	v1, ok, err := b.readQuorumMetaVersion("b", "k", "v1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint8(0), v1.ACL, "older sibling ACL must be untouched")

	v2, ok, err := b.readQuorumMetaVersion("b", "k", "v2")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint8(1), v2.ACL)
	require.Greater(t, v2.MetaSeq, uint64(0), "latest ACL RMW must advance MetaSeq")
}
