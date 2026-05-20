package storage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestLocalBackend_PutObjectThenSetTags_Regression guards against R1
// (Phase 2 unblock fixes): PutObject 직후 SetObjectTags 호출이
// ErrObjectNotFound로 실패하지 않아야 한다.
//
// At v0.0.274.0 baseline this test PASSES at the LocalBackend layer,
// confirming the R1 regression lives in a wrapper / ops facade / server
// handler above LocalBackend (Case B in Task 1 plan). Keep this test as
// the LocalBackend-layer guard so any future regression at this layer
// is caught immediately.
func TestLocalBackend_PutObjectThenSetTags_Regression(t *testing.T) {
	b := newBackend(t)

	const bucket, key = "b", "k"
	require.NoError(t, b.CreateBucket(ctx(), bucket))

	_, err := b.PutObject(ctx(), bucket, key, body("body"), "text/plain")
	require.NoError(t, err, "PutObject must succeed")

	tags := []storage.Tag{{Key: "expire", Value: "yes"}}
	err = b.SetObjectTags(bucket, key, "", tags)
	require.NoError(t, err, "SetObjectTags must find the object just written by PutObject (R1 regression guard)")

	got, err := b.GetObjectTags(bucket, key, "")
	require.NoError(t, err)
	require.Equal(t, tags, got)
}
