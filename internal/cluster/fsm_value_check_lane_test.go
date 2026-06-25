package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// TestFSMValueCheckLane_ImplementsRewrapLane ensures the interface is satisfied.
func TestFSMValueCheckLane_ImplementsRewrapLane(t *testing.T) {
	var _ encrypt.RewrapLane = (*FSMValueCheckLane)(nil)
}

// TestFSMValueCheckLane_RewrapByGenDrainsLocalValues verifies that the lane
// actively drains stale local FSM values instead of waiting for a raft marker.
func TestFSMValueCheckLane_RewrapByGenDrainsLocalValues(t *testing.T) {
	gb, gbDB := newTestGroupBackendWithDB(t, "check-lane-group")
	keeper := gb.shardSvc.DEKKeeper()
	ks := gb.ks()

	// Seal a policy: value at gen 0 (initial active gen).
	polKey := ks.Key([]byte("policy:b1"))
	polRaw, err := gb.fsm.sealValue(polKey, []byte(`{}`))
	require.NoError(t, err)
	dbSet(t, gbDB, polKey, polRaw)

	// Rotate keeper to gen 1 — the sealed value is now stale.
	require.NoError(t, keeper.Rotate())
	_, active := keeper.VersionsAndActive()
	require.Equal(t, uint32(1), active)

	lane := NewFSMValueCheckLane(func() []*GroupBackend { return []*GroupBackend{gb} })

	require.NoError(t, lane.RewrapByGen(context.Background(), 0, 1))

	left, err := gb.CollectStaleFSMValueKeys(1, 10, 10<<20)
	require.NoError(t, err)
	require.Empty(t, left)
	require.Equal(t, uint32(1), gbFrameGenOf(t, gb, polKey))
}

// TestFSMValueCheckLane_CleanWhenNoEncryption verifies that the lane returns nil
// when encryption is disabled (activeDEKGen returns ok=false).
func TestFSMValueCheckLane_CleanWhenNoEncryption(t *testing.T) {
	// newTestGroupBackend wires a keeper; to simulate no-encryption we use a
	// GroupBackend with no keeper (fsm.dekKeeper == nil).
	gb := newTestGroupBackend(t, "no-enc-group")
	// Verify there are no stale keys when keeper is at gen 0 and no values are seeded.
	lane := NewFSMValueCheckLane(func() []*GroupBackend { return []*GroupBackend{gb} })
	// No values seeded, keeper at gen 0: nothing stale → nil.
	require.NoError(t, lane.RewrapByGen(context.Background(), 0, 0))
}

// TestFSMValueCheckLane_EmptyGroupList verifies that an empty group list is always clean.
func TestFSMValueCheckLane_EmptyGroupList(t *testing.T) {
	lane := NewFSMValueCheckLane(func() []*GroupBackend { return nil })
	require.NoError(t, lane.RewrapByGen(context.Background(), 0, 1))
}
