package cluster

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestDistributedBackend_ListObjectVersions_IncludesTags verifies that
// objectMeta.Tags propagate through ListObjectVersions into the
// storage.ObjectVersion projection. Cluster replication semantics are
// covered by e2e (Task 21); this is the local-projection unit test.
func TestDistributedBackend_ListObjectVersions_IncludesTags(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	const bucket = "tagbucket"
	require.NoError(t, b.CreateBucket(ctx, bucket))

	// PutObject always generates a VersionID internally and stores both the
	// legacy key (obj:{bucket}/{key}) and the versioned key
	// (obj:{bucket}/{key}/{versionID}). ListObjectVersions reads the latter.
	obj, err := b.PutObject(ctx, bucket, "tagged.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID, "PutObject must assign a VersionID")

	want := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}}

	// SetObjectTags (VersionID="") dual-writes to both the legacy key and the
	// latest versioned key via applySetObjectTags semantics.
	require.NoError(t, b.SetObjectTags(bucket, "tagged.txt", "", want))

	versions, err := b.ListObjectVersions(bucket, "", 100)
	require.NoError(t, err)
	require.Len(t, versions, 1, "expected exactly one version")

	v := versions[0]
	require.Equal(t, "tagged.txt", v.Key)
	require.Equal(t, want, v.Tags, "Tags must be projected into ObjectVersion")
}

// TestDistributedBackend_ListObjects_PreservesTags is the regression guard
// for adversarial review pass #3 finding #3: ListObjects / ListObjectsPage /
// WalkObjects previously built storage.Object from objectMeta but omitted
// Tags, breaking single/cluster parity (HeadObject + ListObjectVersions
// returned Tags but list paths returned nil).
func TestDistributedBackend_ListObjects_PreservesTags(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	const bucket = "listtagbucket"
	require.NoError(t, b.CreateBucket(ctx, bucket))

	_, err := b.PutObject(ctx, bucket, "a.txt", strings.NewReader("hi"), "text/plain")
	require.NoError(t, err)
	_, err = b.PutObject(ctx, bucket, "b.txt", strings.NewReader("hi"), "text/plain")
	require.NoError(t, err)

	tagsA := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}}
	require.NoError(t, b.SetObjectTags(bucket, "a.txt", "", tagsA))
	// b.txt deliberately left untagged — list must return Tags=nil for it.

	t.Run("ListObjects", func(t *testing.T) {
		objs, err := b.ListObjects(ctx, bucket, "", 100)
		require.NoError(t, err)
		got := tagsByKey(objs)
		require.Equal(t, tagsA, got["a.txt"])
		require.Nil(t, got["b.txt"])
	})

	t.Run("ListObjectsPage", func(t *testing.T) {
		objs, _, err := b.ListObjectsPage(ctx, bucket, "", "", 100)
		require.NoError(t, err)
		got := tagsByKey(objs)
		require.Equal(t, tagsA, got["a.txt"])
		require.Nil(t, got["b.txt"])
	})

	t.Run("WalkObjects", func(t *testing.T) {
		var walked []*storage.Object
		require.NoError(t, b.WalkObjects(ctx, bucket, "", func(o *storage.Object) error {
			walked = append(walked, o)
			return nil
		}))
		got := tagsByKey(walked)
		require.Equal(t, tagsA, got["a.txt"])
		require.Nil(t, got["b.txt"])
	})
}

// tagsByKey collapses []*storage.Object into key→Tags so tests can assert
// per-key regardless of iterator order.
func tagsByKey(objs []*storage.Object) map[string][]storage.Tag {
	out := make(map[string][]storage.Tag, len(objs))
	for _, o := range objs {
		out[o.Key] = o.Tags
	}
	return out
}
