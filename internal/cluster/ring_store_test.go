package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRingStore_GetRing(t *testing.T) {
	store := newRingStore()
	ring := NewRing(1, []string{"n1", "n2", "n3"}, 10)
	store.putRing(ring)

	got, err := store.GetRing(1)
	require.NoError(t, err)
	assert.Equal(t, RingVersion(1), got.Version)
}

func TestRingStore_GetRing_NotFound(t *testing.T) {
	store := newRingStore()
	_, err := store.GetRing(99)
	assert.ErrorIs(t, err, ErrRingNotFound)
}

func TestRingStore_GetCurrentRing(t *testing.T) {
	store := newRingStore()
	ring1 := NewRing(1, []string{"n1"}, 10)
	ring2 := NewRing(2, []string{"n1", "n2"}, 10)
	store.putRing(ring1)
	store.putRing(ring2)

	got, err := store.GetCurrentRing()
	require.NoError(t, err)
	assert.Equal(t, RingVersion(2), got.Version)
}

func TestRingStore_RefCount_GCEligible(t *testing.T) {
	store := newRingStore()
	ring1 := NewRing(1, []string{"n1"}, 10)
	ring2 := NewRing(2, []string{"n1", "n2"}, 10)
	store.putRing(ring1)
	store.putRing(ring2)
	store.incRef(1)
	store.decRef(1)
	// refCount[1]==0, current==2, current>1 → eligible
	assert.True(t, store.gcEligible(1))
	assert.False(t, store.gcEligible(2)) // current ring never GC
}

func TestRingStore_EncodeDecodeRingForDB(t *testing.T) {
	ring := NewRing(3, []string{"node-a", "node-b", "node-c"}, 10)
	data, err := encodeRingForDB(ring)
	require.NoError(t, err)

	got, err := decodeRingFromDB(data)
	require.NoError(t, err)
	assert.Equal(t, ring.Version, got.Version)
	assert.Equal(t, ring.VPerNode, got.VPerNode)
	assert.Equal(t, len(ring.VNodes), len(got.VNodes))
	for i := range ring.VNodes {
		assert.Equal(t, ring.VNodes[i].Token, got.VNodes[i].Token)
		assert.Equal(t, ring.VNodes[i].NodeID, got.VNodes[i].NodeID)
	}
}
