package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/badgermeta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newResolverTestBackend(t *testing.T) *DistributedBackend {
	t.Helper()
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
	return &DistributedBackend{store: badgermeta.Wrap(db), fsm: fsm, ecConfig: ECConfig{DataShards: 2, ParityShards: 1}}
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
