package snapshot

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestManager_PITRRestoreSerializesWithCreate(t *testing.T) {
	m := NewTestManager(t, t.TempDir(), &testBackend{}, "")
	// Seed one snapshot so PITRRestore has a base.
	_, err := m.Create("seed")
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = m.Create("c") }()
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = m.PITRRestore(time.Now()) }()
	}
	wg.Wait()
	// No assertion needed beyond -race cleanliness + no panic; some PITRRestore
	// calls may error (no WAL, etc.) — that is fine. The point is no data race.
}

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
