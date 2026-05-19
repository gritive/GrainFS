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
