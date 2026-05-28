package cluster

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunPresentFlip_SingleNodeRefuse(t *testing.T) {
	deps := PresentFlipDeps{
		SelfID:        "node-A",
		Voters:        func() ([]string, uint64) { return []string{"node-A"}, 7 },
		RegistrySPKIs: func() map[string][32]byte { return map[string][32]byte{"node-A": {0xAA}} },
		ProposeWithIndex: func(_ context.Context, _ MetaCmdType, _ []byte) (uint64, error) {
			t.Fatal("ProposeWithIndex must NOT be called on single-node")
			return 0, nil
		},
		WaitVoters: func(_ context.Context, _ uint64, _ []string) error {
			t.Fatal("WaitVoters must NOT be called on single-node")
			return nil
		},
	}
	err := RunPresentFlip(context.Background(), deps, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "single-node")
}

func TestRunPresentFlip_RefusesWhenVoterNotInRegistry(t *testing.T) {
	var proposed atomic.Bool
	deps := PresentFlipDeps{
		SelfID:        "node-A",
		Voters:        func() ([]string, uint64) { return []string{"node-A", "node-B", "node-C"}, 7 },
		RegistrySPKIs: func() map[string][32]byte { return map[string][32]byte{"node-A": {0xAA}, "node-B": {0xBB}} },
		ProposeWithIndex: func(_ context.Context, _ MetaCmdType, _ []byte) (uint64, error) {
			proposed.Store(true)
			return 0, nil
		},
		WaitVoters: func(_ context.Context, _ uint64, _ []string) error { return nil },
	}
	err := RunPresentFlip(context.Background(), deps, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "node-C")
	require.Contains(t, err.Error(), "not in peer registry")
	require.False(t, proposed.Load(), "ProposeWithIndex must NOT be called on registry-precondition fail")
}

func TestRunPresentFlip_HappyPath(t *testing.T) {
	var prepareProposed, beginProposed, barrierCalled atomic.Bool
	deps := PresentFlipDeps{
		SelfID: "node-A",
		Voters: func() ([]string, uint64) { return []string{"node-A", "node-B", "node-C"}, 7 },
		RegistrySPKIs: func() map[string][32]byte {
			return map[string][32]byte{"node-A": {0xAA}, "node-B": {0xBB}, "node-C": {0xCC}}
		},
		ProposeWithIndex: func(_ context.Context, typ MetaCmdType, _ []byte) (uint64, error) {
			switch typ {
			case MetaCmdTypePreparePresentFlip:
				prepareProposed.Store(true)
				return 42, nil
			case MetaCmdTypeBeginPresentFlip:
				require.True(t, prepareProposed.Load())
				require.True(t, barrierCalled.Load())
				beginProposed.Store(true)
				return 43, nil
			}
			return 0, fmt.Errorf("unexpected cmd type %d", typ)
		},
		WaitVoters: func(_ context.Context, target uint64, voters []string) error {
			require.True(t, prepareProposed.Load())
			require.Equal(t, uint64(42), target)
			require.ElementsMatch(t, []string{"node-A", "node-B", "node-C"}, voters)
			barrierCalled.Store(true)
			return nil
		},
	}
	err := RunPresentFlip(context.Background(), deps, time.Second)
	require.NoError(t, err)
	require.True(t, prepareProposed.Load() && barrierCalled.Load() && beginProposed.Load())
}

func TestRunPresentFlip_BarrierTimeout_NoBegin(t *testing.T) {
	var beginProposed atomic.Bool
	deps := PresentFlipDeps{
		SelfID:        "node-A",
		Voters:        func() ([]string, uint64) { return []string{"node-A", "node-B"}, 7 },
		RegistrySPKIs: func() map[string][32]byte { return map[string][32]byte{"node-A": {0xAA}, "node-B": {0xBB}} },
		ProposeWithIndex: func(_ context.Context, typ MetaCmdType, _ []byte) (uint64, error) {
			if typ == MetaCmdTypeBeginPresentFlip {
				beginProposed.Store(true)
			}
			return 42, nil
		},
		WaitVoters: func(_ context.Context, _ uint64, _ []string) error {
			return fmt.Errorf("applied_index_probe: timeout waiting for voters [node-B]")
		},
	}
	err := RunPresentFlip(context.Background(), deps, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout")
	require.False(t, beginProposed.Load(), "Begin MUST NOT be proposed on barrier failure")
}

func TestRunPresentFlip_VoterChangedSincePrepare_AbortBegin(t *testing.T) {
	var calls atomic.Int32
	var beginProposed atomic.Bool
	deps := PresentFlipDeps{
		SelfID: "node-A",
		Voters: func() ([]string, uint64) {
			n := calls.Add(1)
			if n == 1 {
				return []string{"node-A", "node-B"}, 7
			}
			return []string{"node-A", "node-B", "node-C"}, 8
		},
		RegistrySPKIs: func() map[string][32]byte {
			return map[string][32]byte{"node-A": {0xAA}, "node-B": {0xBB}, "node-C": {0xCC}}
		},
		ProposeWithIndex: func(_ context.Context, typ MetaCmdType, _ []byte) (uint64, error) {
			if typ == MetaCmdTypeBeginPresentFlip {
				beginProposed.Store(true)
			}
			return 42, nil
		},
		WaitVoters: func(_ context.Context, _ uint64, _ []string) error { return nil },
	}
	err := RunPresentFlip(context.Background(), deps, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "voter set changed")
	require.False(t, beginProposed.Load(), "Begin MUST NOT be proposed on config-stamp mismatch")
}
