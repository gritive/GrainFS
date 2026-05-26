package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAllFrozenSegmentPaths_NilSourceFailsClosed(t *testing.T) {
	b := &DistributedBackend{}
	got, err := b.AllFrozenSegmentPaths()
	require.Error(t, err)
	require.Nil(t, got)
}

func TestAllFrozenSegmentPaths_DelegatesToSource(t *testing.T) {
	want := map[string][]string{
		"bucket-a": {"seg/aaa", "seg/bbb"},
		"bucket-b": {"seg/ccc"},
	}
	b := &DistributedBackend{}
	b.SetFrozenSegmentPathSource(func() (map[string][]string, error) {
		return want, nil
	})
	got, err := b.AllFrozenSegmentPaths()
	require.NoError(t, err)
	require.Equal(t, want, got)

	// Source error propagates.
	b.SetFrozenSegmentPathSource(func() (map[string][]string, error) {
		return nil, fmt.Errorf("boom")
	})
	_, err = b.AllFrozenSegmentPaths()
	require.Error(t, err)
}

// TestCaughtUp_NilNode covers the single-node / unit-test path where no raft
// node is wired: the backend is trivially current so GC is never starved.
func TestCaughtUp_NilNode(t *testing.T) {
	b := &DistributedBackend{}
	require.True(t, b.CaughtUp(context.Background()))
}

// TestCaughtUp_SingleNodeLeader exercises the ReadIndex barrier against a
// single-node leader (self-quorum confirms commitIndex inline). Once the
// bootstrap no-op applies, CaughtUp must report true so GC can run.
func TestCaughtUp_SingleNodeLeader(t *testing.T) {
	b := newTestDistributedBackend(t)
	caughtUp := false
	for range 2000 {
		if b.CaughtUp(context.Background()) {
			caughtUp = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.True(t, caughtUp, "single-node leader must become caught up")
}
