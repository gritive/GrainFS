package scrubber

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type countingSource struct {
	name      string
	calls     atomic.Int32
	lastScope atomic.Int32 // ScrubScope encoded as int32 for atomic access
}

func (c *countingSource) Name() string { return c.name }
func (c *countingSource) Iter(ctx context.Context, scope ScrubScope, keyPrefix string) (<-chan Block, error) {
	c.calls.Add(1)
	c.lastScope.Store(int32(scope))
	ch := make(chan Block)
	close(ch)
	return ch, nil
}

type noopVerifier struct{}

func (noopVerifier) Verify(ctx context.Context, b Block) (BlockStatus, error) {
	return BlockStatus{Healthy: true}, nil
}
func (noopVerifier) Repair(ctx context.Context, b Block) error { return nil }

type noopScrubbable struct{}

func (noopScrubbable) ListBuckets(ctx context.Context) ([]string, error) { return nil, nil }
func (noopScrubbable) ScanObjects(bucket string) (<-chan ObjectRecord, error) {
	ch := make(chan ObjectRecord)
	close(ch)
	return ch, nil
}
func (noopScrubbable) ObjectExists(bucket, key string) (bool, error)          { return false, nil }
func (noopScrubbable) ShardPaths(bucket, key, vid string, n int) []string     { return nil }
func (noopScrubbable) ReadShard(bucket, key, path string) ([]byte, error)     { return nil, nil }
func (noopScrubbable) WriteShard(bucket, key, path string, data []byte) error { return nil }

func TestBackgroundScrubber_VolumeDualTicker(t *testing.T) {
	bg := New(noopScrubbable{}, 50*time.Millisecond)
	vol := &countingSource{name: "volume"}
	bg.RegisterSource("volume", vol, noopVerifier{})
	bg.SetVolumeFullInterval(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	bg.Start(ctx)
	<-ctx.Done()
	// Drain a moment in case the last tick is in flight.
	time.Sleep(20 * time.Millisecond)

	// Live ticker fires ~6× in 350ms (50ms interval).
	// Full ticker fires ~1× at 200ms.
	// Both feed the volume source — call count should clearly exceed live-only.
	require.GreaterOrEqual(t, int(vol.calls.Load()), 4,
		"volume source should be hit by both live and full tickers")
}

func TestBackgroundScrubber_VolumeFullDisabled(t *testing.T) {
	bg := New(noopScrubbable{}, 50*time.Millisecond)
	vol := &countingSource{name: "volume"}
	bg.RegisterSource("volume", vol, noopVerifier{})
	bg.SetVolumeFullInterval(0) // disabled

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	bg.Start(ctx)
	<-ctx.Done()
	time.Sleep(20 * time.Millisecond)

	// Only live ticker fires; lastScope must always be ScopeLive.
	require.Equal(t, int32(ScopeLive), vol.lastScope.Load())
	require.Greater(t, int(vol.calls.Load()), 0)
}
