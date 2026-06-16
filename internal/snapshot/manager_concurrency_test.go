package snapshot

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManager_ConcurrentOpsAreSerialized(t *testing.T) {
	m := NewTestManager(t, t.TempDir(), &testBackend{}, "")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = m.Create("c") }()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = m.List() }()
	}
	wg.Wait()
	snaps, err := m.List()
	require.NoError(t, err)
	seen := map[uint64]bool{}
	for _, s := range snaps {
		require.False(t, seen[s.Seq], "duplicate seq %d", s.Seq)
		seen[s.Seq] = true
	}
	require.Len(t, snaps, 8)
}
