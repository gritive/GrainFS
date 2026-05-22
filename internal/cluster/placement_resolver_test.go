package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newResolverTestBackend(t *testing.T) *DistributedBackend {
	t.Helper()
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	return &DistributedBackend{db: db, fsm: fsm, ecConfig: ECConfig{DataShards: 2, ParityShards: 1}}
}

func TestResolvePlacement_UsesMetadataNodeIDs(t *testing.T) {
	b := newResolverTestBackend(t)

	got, err := b.ResolvePlacement(context.Background(), "bkt", "obj", PlacementMeta{
		VersionID: "v1",
		ECData:    2,
		ECParity:  1,
		NodeIDs:   []string{"n0", "n1", "n2"},
	})
	require.NoError(t, err)

	assert.Equal(t, "obj/v1", got.ShardKey)
	assert.Equal(t, PlacementRecord{Nodes: []string{"n0", "n1", "n2"}, K: 2, M: 1}, got.Record)
}

func TestResolvePlacement_ReturnsErrNotECWhenNoPlacementExists(t *testing.T) {
	b := newResolverTestBackend(t)

	_, err := b.ResolvePlacement(context.Background(), "bkt", "obj", PlacementMeta{VersionID: "v1"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotEC))
}

func TestResolvePlacement_ReturnsErrPlacementCorruptForBadMetadata(t *testing.T) {
	b := newResolverTestBackend(t)

	_, err := b.ResolvePlacement(context.Background(), "bkt", "obj", PlacementMeta{
		VersionID: "v1",
		ECData:    2,
		ECParity:  1,
		NodeIDs:   []string{"only-one"},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPlacementCorrupt))
}

func TestIterObjectMetas_YieldsVersionIDAndNodeIDs(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      "bkt",
		Key:         "obj/with/slash",
		VersionID:   "v1",
		Size:        10,
		ContentType: "application/octet-stream",
		ETag:        "etag",
		ModTime:     1,
		ECData:      2,
		ECParity:    1,
		NodeIDs:     []string{"n0", "n1", "n2"},
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))

	var refs []ObjectMetaRef
	require.NoError(t, fsm.IterObjectMetas(func(ref ObjectMetaRef) error {
		refs = append(refs, ref)
		return nil
	}))

	require.Len(t, refs, 1)
	assert.Equal(t, "bkt", refs[0].Bucket)
	assert.Equal(t, "obj/with/slash", refs[0].Key)
	assert.Equal(t, "v1", refs[0].VersionID)
	assert.Equal(t, []string{"n0", "n1", "n2"}, refs[0].NodeIDs)
}

func TestHeadObjectMeta_ReturnsObjectAndPlacementMeta(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	b := &DistributedBackend{db: db, fsm: fsm}

	raw, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "bkt"})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))
	raw, err = EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      "bkt",
		Key:         "obj",
		VersionID:   "v1",
		Size:        123,
		ContentType: "application/octet-stream",
		ETag:        "etag",
		ModTime:     1,
		ECData:      2,
		ECParity:    1,
		NodeIDs:     []string{"n0", "n1", "n2"},
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))

	obj, meta, err := b.headObjectMeta(context.Background(), "bkt", "obj")
	require.NoError(t, err)
	assert.Equal(t, "obj", obj.Key)
	assert.Equal(t, int64(123), obj.Size)
	assert.Equal(t, "v1", obj.VersionID)
	assert.Equal(t, PlacementMeta{
		VersionID: "v1",
		ECData:    2,
		ECParity:  1,
		NodeIDs:   []string{"n0", "n1", "n2"},
	}, meta)
}
