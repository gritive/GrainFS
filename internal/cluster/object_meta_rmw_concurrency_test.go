package cluster

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestSetObjectTags_ConcurrentRMW_NoLostUpdate(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	_, err := b.PutObject(ctx, "bucket", "obj", bytes.NewReader([]byte("data")), "text/plain")
	require.NoError(t, err)

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tag := storage.Tag{Key: "k", Value: fmt.Sprintf("v%02d", i)}
			require.NoError(t, b.SetObjectTags("bucket", "obj", "", []storage.Tag{tag}))
		}(i)
	}
	wg.Wait()

	cmd, err := b.readQuorumMetaCmd("bucket", "obj")
	require.NoError(t, err)
	// Each serialized RMW bumps MetaSeq by 1 from the prior winner; n writers => MetaSeq == n.
	require.Equal(t, uint64(n), cmd.MetaSeq, "every RMW must serialize and advance MetaSeq; a lost update would leave a gap")
	require.Len(t, cmd.Tags, 1, "exactly one tag key survives")
}

func TestSetObjectACL_ConcurrentRMW_NoLostUpdate(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	_, err := b.PutObject(ctx, "bucket", "obj", bytes.NewReader([]byte("data")), "text/plain")
	require.NoError(t, err)

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Alternate between ACLPrivate and ACLPublicRead so each write is meaningful.
			acl := uint8(s3auth.ACLPrivate)
			if i%2 == 0 {
				acl = uint8(s3auth.ACLPublicRead)
			}
			require.NoError(t, b.SetObjectACL("bucket", "obj", acl))
		}(i)
	}
	wg.Wait()

	cmd, err := b.readQuorumMetaCmd("bucket", "obj")
	require.NoError(t, err)
	// Each serialized RMW bumps MetaSeq by 1 from the prior winner; n writers => MetaSeq == n.
	require.Equal(t, uint64(n), cmd.MetaSeq, "every ACL RMW must serialize and advance MetaSeq; a lost update would leave a gap")
}
