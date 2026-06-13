package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestPutObject_PersistsACL(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	acl := uint8(s3auth.ACLPublicRead)
	_, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket: "bucket", Key: "k", Body: bytes.NewReader([]byte("hi")), ACL: &acl,
	})
	require.NoError(t, err)
	head, err := b.HeadObject(ctx, "bucket", "k")
	require.NoError(t, err)
	require.Equal(t, acl, head.ACL)
}
