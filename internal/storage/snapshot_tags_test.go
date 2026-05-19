package storage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestSnapshotRestore_PreservesTags verifies that the ListAllObjects → RestoreObjects
// round-trip preserves object Tags. The test uses a single backend: tags are set,
// snapshotted, wiped, then restored — ensuring both projection gaps are exercised.
func TestSnapshotRestore_PreservesTags(t *testing.T) {
	b := newBackend(t)
	require.NoError(t, b.CreateBucket(ctx(), "b"))
	_, err := b.PutObject(ctx(), "b", "k", body("body"), "text/plain")
	require.NoError(t, err)

	want := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}}
	require.NoError(t, b.SetObjectTags("b", "k", "", want))

	// Snapshot while tags are set.
	objs, err := b.ListAllObjects()
	require.NoError(t, err)

	// Wipe tags so the restore has something meaningful to do.
	require.NoError(t, b.SetObjectTags("b", "k", "", nil))
	wiped, err := b.GetObjectTags("b", "k", "")
	require.NoError(t, err)
	require.Empty(t, wiped)

	// Restore and verify tags come back.
	_, _, err = b.RestoreObjects(objs)
	require.NoError(t, err)

	got, err := b.GetObjectTags("b", "k", "")
	require.NoError(t, err)
	require.Equal(t, want, got)
}
