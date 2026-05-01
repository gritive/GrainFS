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
	fsm := NewFSM(db)
	return &DistributedBackend{db: db, fsm: fsm, ecConfig: ECConfig{DataShards: 2, ParityShards: 1}}
}

func TestResolvePlacement_UsesRingWhenRingVersionPresent(t *testing.T) {
	b := newResolverTestBackend(t)
	ring := NewRing(7, []string{"n0", "n1", "n2", "n3"}, 10)
	b.fsm.GetRingStore().putRing(ring)

	meta := PlacementMeta{
		VersionID:   "v1",
		RingVersion: 7,
		ECData:      2,
		ECParity:    1,
		NodeIDs:     []string{"metadata-a", "metadata-b", "metadata-c"},
	}

	got, err := b.ResolvePlacement(context.Background(), "bkt", "obj", meta)
	require.NoError(t, err)

	wantNodes := ring.PlacementForKey(ECConfig{DataShards: 2, ParityShards: 1}, "obj/v1")
	assert.Equal(t, PlacementSourceRing, got.Source)
	assert.Equal(t, "obj/v1", got.ShardKey)
	assert.Equal(t, PlacementRecord{Nodes: wantNodes, K: 2, M: 1}, got.Record)
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

	assert.Equal(t, PlacementSourceMetadata, got.Source)
	assert.Equal(t, "obj/v1", got.ShardKey)
	assert.Equal(t, PlacementRecord{Nodes: []string{"n0", "n1", "n2"}, K: 2, M: 1}, got.Record)
}

func TestResolvePlacement_MetadataWinsOverConflictingLegacy(t *testing.T) {
	b := newResolverTestBackend(t)
	writePlacement(t, b, "bkt", "obj/v1", []string{"legacy-a", "legacy-b", "legacy-c", "legacy-d", "legacy-e", "legacy-f"})

	got, err := b.ResolvePlacement(context.Background(), "bkt", "obj", PlacementMeta{
		VersionID: "v1",
		ECData:    2,
		ECParity:  1,
		NodeIDs:   []string{"meta-a", "meta-b", "meta-c"},
	})
	require.NoError(t, err)

	assert.Equal(t, PlacementSourceMetadata, got.Source)
	assert.Equal(t, []string{"meta-a", "meta-b", "meta-c"}, got.Record.Nodes)
}

func TestResolvePlacement_FallsBackToLegacyPlacement(t *testing.T) {
	b := newResolverTestBackend(t)
	writePlacement(t, b, "bkt", "obj/v1", []string{"legacy-a", "legacy-b", "legacy-c", "legacy-d", "legacy-e", "legacy-f"})

	got, err := b.ResolvePlacement(context.Background(), "bkt", "obj", PlacementMeta{VersionID: "v1"})
	require.NoError(t, err)

	assert.Equal(t, PlacementSourceLegacy, got.Source)
	assert.Equal(t, "obj/v1", got.ShardKey)
	assert.Equal(t, []string{"legacy-a", "legacy-b", "legacy-c", "legacy-d", "legacy-e", "legacy-f"}, got.Record.Nodes)
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

func TestResolvePlacement_ReturnsErrPlacementCorruptForMissingRing(t *testing.T) {
	b := newResolverTestBackend(t)

	_, err := b.ResolvePlacement(context.Background(), "bkt", "obj", PlacementMeta{
		VersionID:   "v1",
		RingVersion: 99,
		ECData:      2,
		ECParity:    1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPlacementCorrupt))
}

func TestIterObjectMetas_YieldsVersionIDAndNodeIDs(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
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
	fsm := NewFSM(db)
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
		RingVersion: 7,
		ECData:      2,
		ECParity:    1,
		NodeIDs:     []string{"n0", "n1", "n2"},
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))

	obj, meta, err := b.headObjectMeta("bkt", "obj")
	require.NoError(t, err)
	assert.Equal(t, "obj", obj.Key)
	assert.Equal(t, int64(123), obj.Size)
	assert.Equal(t, "v1", obj.VersionID)
	assert.Equal(t, PlacementMeta{
		VersionID:   "v1",
		RingVersion: 7,
		ECData:      2,
		ECParity:    1,
		NodeIDs:     []string{"n0", "n1", "n2"},
	}, meta)
}
