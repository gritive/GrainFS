package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

// TestColdCacheEnforcesCommittedDenyPolicy is the headline proof: a node whose
// in-memory compiled policy cache was never populated for a bucket (a fresh
// follower, or a single-node restart) must still enforce a committed Deny
// bucket-policy. The server's policy store is wired with the pull-on-miss loader
// via NewOperations, so Allow loads the committed policy from the local backend.
func TestColdCacheEnforcesCommittedDenyPolicy(t *testing.T) {
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "b"))
	deny := []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":["*"]},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::b/*"]}]}`)
	require.NoError(t, backend.SetBucketPolicy("b", deny))

	srv := New("127.0.0.1:0", backend, withoutReadAfterWriteRetry())

	// Cold-cache / restart-equivalent: drop any compiled entry for "b" so the
	// next Allow must pull from the committed replica.
	srv.PolicyStore().Invalidate("b")

	in := s3auth.PermCheckInput{
		Action:    s3auth.PutObject,
		Principal: s3auth.Principal{AccessKey: "AKIA"},
		Resource:  s3auth.ResourceRef{Bucket: "b", Key: "k"},
	}
	require.False(t, srv.PolicyStore().Allow(ctx, in)) // committed Deny enforced on cold cache

	in.Resource.Bucket = "free"
	require.True(t, srv.PolicyStore().Allow(ctx, in)) // no committed policy ⇒ allow (negative-cache)
}
