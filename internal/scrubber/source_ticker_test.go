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

func TestBackgroundScrubber_SourceTickerFires(t *testing.T) {
	bg := New(noopScrubbable{}, 50*time.Millisecond)
	src := &countingSource{name: "replication"}
	bg.RegisterSource("replication", src, noopVerifier{})

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	bg.Start(ctx)
	<-ctx.Done()
	time.Sleep(20 * time.Millisecond)

	require.Greater(t, int(src.calls.Load()), 0,
		"replication source should be invoked by the background ticker")
	require.Equal(t, int32(ScopeFull), src.lastScope.Load(),
		"background ticker calls source with ScopeFull")
}

func TestBackgroundScrubber_NoSourceRegistered(t *testing.T) {
	bg := New(noopScrubbable{}, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	// No RegisterSource call — Start must not panic and the EC ticker must
	// run on its own.
	bg.Start(ctx)
	<-ctx.Done()
}
