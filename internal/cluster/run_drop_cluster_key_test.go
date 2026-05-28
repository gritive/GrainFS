package cluster

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunDropClusterKey_EmptyVoterSet(t *testing.T) {
	deps := DropClusterKeyDeps{
		SelfID:  "node-A",
		Voters:  func() ([]string, uint64) { return nil, 0 },
		AllVPN:  func([]string) bool { t.Fatal("AllVPN must not be called"); return false },
		Propose: func(context.Context, MetaCmdType, []byte) error { t.Fatal("must not propose"); return nil },
	}
	err := RunDropClusterKey(context.Background(), deps, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty voter set")
}

func TestRunDropClusterKey_SingleNode(t *testing.T) {
	deps := DropClusterKeyDeps{
		SelfID:  "node-A",
		Voters:  func() ([]string, uint64) { return []string{"node-A"}, 1 },
		AllVPN:  func([]string) bool { t.Fatal("AllVPN must not be called"); return false },
		Propose: func(context.Context, MetaCmdType, []byte) error { t.Fatal("must not propose"); return nil },
	}
	err := RunDropClusterKey(context.Background(), deps, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "single-node")
}

func TestRunDropClusterKey_DCut4GateNotMet(t *testing.T) {
	var proposed atomic.Bool
	deps := DropClusterKeyDeps{
		SelfID:  "node-A",
		Voters:  func() ([]string, uint64) { return []string{"node-A", "node-B"}, 2 },
		AllVPN:  func([]string) bool { return false },
		Propose: func(context.Context, MetaCmdType, []byte) error { proposed.Store(true); return nil },
	}
	err := RunDropClusterKey(context.Background(), deps, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "D-cut4")
	require.False(t, proposed.Load())
}

func TestRunDropClusterKey_VoterSetChangedBeforePropose(t *testing.T) {
	var calls atomic.Int32
	var proposed atomic.Bool
	deps := DropClusterKeyDeps{
		SelfID: "node-A",
		Voters: func() ([]string, uint64) {
			if calls.Add(1) == 1 {
				return []string{"node-A", "node-B"}, 2
			}
			return []string{"node-A", "node-B", "node-C"}, 3
		},
		AllVPN:  func([]string) bool { return true },
		Propose: func(context.Context, MetaCmdType, []byte) error { proposed.Store(true); return nil },
	}
	err := RunDropClusterKey(context.Background(), deps, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "voter set changed")
	require.False(t, proposed.Load())
}

func TestRunDropClusterKey_HappyPath(t *testing.T) {
	var dropProposed atomic.Bool
	deps := DropClusterKeyDeps{
		SelfID: "node-A",
		Voters: func() ([]string, uint64) {
			return []string{"node-A", "node-B", "node-C"}, 7
		},
		AllVPN: func(voters []string) bool {
			require.Equal(t, []string{"node-A", "node-B", "node-C"}, voters)
			return true
		},
		Propose: func(_ context.Context, typ MetaCmdType, _ []byte) error {
			require.Equal(t, MetaCmdTypeDropClusterKeyAccept, typ)
			dropProposed.Store(true)
			return nil
		},
	}
	require.NoError(t, RunDropClusterKey(context.Background(), deps, time.Second))
	require.True(t, dropProposed.Load())
}

func TestRunDropClusterKey_ProposeErrorPropagates(t *testing.T) {
	deps := DropClusterKeyDeps{
		SelfID:  "node-A",
		Voters:  func() ([]string, uint64) { return []string{"node-A", "node-B"}, 2 },
		AllVPN:  func([]string) bool { return true },
		Propose: func(context.Context, MetaCmdType, []byte) error { return fmt.Errorf("leader not available") },
	}
	err := RunDropClusterKey(context.Background(), deps, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "leader not available")
}
