package scrubber

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeRedundancyBackend implements the minimal Scrubbable surface plus
// RedundancyUpgrader, counting RunRedundancyUpgradeSweep invocations.
type fakeRedundancyBackend struct {
	sweepCalls   int
	lastMaxPerCy int
	lastMinAge   time.Duration
}

func (f *fakeRedundancyBackend) ListBuckets(context.Context) ([]string, error) { return nil, nil }
func (f *fakeRedundancyBackend) ScanObjects(string) (<-chan ObjectRecord, error) {
	ch := make(chan ObjectRecord)
	close(ch)
	return ch, nil
}
func (f *fakeRedundancyBackend) ObjectExists(string, string) (bool, error)       { return false, nil }
func (f *fakeRedundancyBackend) ShardPaths(string, string, string, int) []string { return nil }
func (f *fakeRedundancyBackend) ReadShard(string, string, string, int, string) ([]byte, error) {
	return nil, nil
}
func (f *fakeRedundancyBackend) WriteShard(string, string, string, int, string, []byte) error {
	return nil
}

func (f *fakeRedundancyBackend) RunRedundancyUpgradeSweep(_ context.Context, maxPerCycle int, minAge time.Duration) (int, error) {
	f.sweepCalls++
	f.lastMaxPerCy = maxPerCycle
	f.lastMinAge = minAge
	return 0, nil
}

// TestRunOnce_RedundancyUpgradeGating verifies the sweep runs only when enabled.
func TestRunOnce_RedundancyUpgradeGating(t *testing.T) {
	t.Run("disabled by default", func(t *testing.T) {
		be := &fakeRedundancyBackend{}
		s := New(be, 0)
		s.runOnce(context.Background())
		require.Equal(t, 0, be.sweepCalls)
	})

	t.Run("enabled invokes sweep with configured cap", func(t *testing.T) {
		be := &fakeRedundancyBackend{}
		s := New(be, 0)
		s.EnableRedundancyUpgrade(7, 30*time.Second)
		s.runOnce(context.Background())
		require.Equal(t, 1, be.sweepCalls)
		require.Equal(t, 7, be.lastMaxPerCy)
		require.Equal(t, 30*time.Second, be.lastMinAge)
	})
}
