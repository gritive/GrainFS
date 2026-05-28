package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/raft"

	"github.com/stretchr/testify/require"
)

func TestPresentFlipBegun_SnapshotRoundTrip_FiresCallback(t *testing.T) {
	src := NewMetaFSM()
	wireSnapshotKEK(t, src)
	src.SetPresentFlipBegunForTest(true) // test-only setter added by this task

	snap, err := src.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	dst := NewMetaFSM()
	wireSnapshotKEK(t, dst)
	var fired bool
	dst.SetOnPresentFlip(func() { fired = true })

	err = dst.Restore(raft.SnapshotMeta{}, snap)
	require.NoError(t, err)
	require.True(t, fired, "Restore of present_flip_begun=true must fire onPresentFlip")
	require.True(t, dst.PresentFlipBegun(), "PresentFlipBegun() reflects restored state")
}

func TestPresentFlipBegun_SnapshotRoundTrip_NoFireWhenFalse(t *testing.T) {
	src := NewMetaFSM()
	wireSnapshotKEK(t, src)

	snap, err := src.Snapshot()
	require.NoError(t, err)

	dst := NewMetaFSM()
	wireSnapshotKEK(t, dst)
	var fired bool
	dst.SetOnPresentFlip(func() { fired = true })

	err = dst.Restore(raft.SnapshotMeta{}, snap)
	require.NoError(t, err)
	require.False(t, fired, "Restore of present_flip_begun=false must NOT fire onPresentFlip")
	require.False(t, dst.PresentFlipBegun())
}

func TestBeginPresentFlip_Apply_FiresCallback(t *testing.T) {
	f := NewMetaFSM()
	wireSnapshotKEK(t, f)
	var fired int
	f.SetOnPresentFlip(func() { fired++ })

	stamp := PresentFlipStamp{Voters: []string{"A"}, ConfigIndex: 10}
	payload, err := encodePresentFlipStampCmd(stamp)
	require.NoError(t, err)

	require.NoError(t, applyMetaCmdForTest(f, MetaCmdTypeBeginPresentFlip, payload))
	require.Equal(t, 1, fired, "first Begin Apply fires callback exactly once")
	require.True(t, f.PresentFlipBegun())

	// Idempotent: re-applying Begin must NOT fire again.
	require.NoError(t, applyMetaCmdForTest(f, MetaCmdTypeBeginPresentFlip, payload))
	require.Equal(t, 1, fired, "second Begin Apply is idempotent")
}

func TestPreparePresentFlip_Apply_NoCallback(t *testing.T) {
	f := NewMetaFSM()
	wireSnapshotKEK(t, f)
	var fired int
	f.SetOnPresentFlip(func() { fired++ })

	stamp := PresentFlipStamp{Voters: []string{"A"}, ConfigIndex: 10}
	payload, err := encodePresentFlipStampCmd(stamp)
	require.NoError(t, err)

	require.NoError(t, applyMetaCmdForTest(f, MetaCmdTypePreparePresentFlip, payload))
	require.Equal(t, 0, fired, "Prepare Apply has no transport side effect")
	require.False(t, f.PresentFlipBegun())
}
