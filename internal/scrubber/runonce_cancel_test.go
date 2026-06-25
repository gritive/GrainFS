package scrubber_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// cancelMockBackend is a mockBackend that also implements OrphanWalkable so a
// test can observe whether the downstream orphan-shard sweep ran. It optionally
// invokes a stored cancel() on the first ObjectExists call so a scrub cycle can
// be cancelled deterministically mid-scan (no sleeps / no timing races).
type cancelMockBackend struct {
	*mockBackend

	mu sync.Mutex
	// cancel, when non-nil, is invoked exactly once on the first ObjectExists
	// call to deterministically abort the in-progress scan loop.
	cancel       func()
	cancelFired  bool
	walkOrphansN int // number of WalkOrphanShards calls (downstream-phase witness)
}

func newCancelMockBackend() *cancelMockBackend {
	return &cancelMockBackend{mockBackend: newMockBackend()}
}

func (m *cancelMockBackend) ObjectExists(bucket, key string) (bool, error) {
	m.mu.Lock()
	if m.cancel != nil && !m.cancelFired {
		m.cancelFired = true
		m.cancel()
	}
	m.mu.Unlock()
	return m.mockBackend.ObjectExists(bucket, key)
}

// WalkOrphanShards / DeleteOrphanDir satisfy OrphanWalkable. WalkOrphanShards
// records that the downstream orphan-shard sweep was reached.
func (m *cancelMockBackend) WalkOrphanShards(known map[string]bool, fn func(dir string) error) error {
	m.mu.Lock()
	m.walkOrphansN++
	m.mu.Unlock()
	return nil
}

func (m *cancelMockBackend) DeleteOrphanDir(dir string) (int, error) { return 0, nil }

func (m *cancelMockBackend) walkOrphanCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.walkOrphansN
}

// TestRunOnce_NormalRun_ReachesDownstreamAndStats is the positive control for
// the cancel characterization below: with the same mock and no cancellation,
// a full cycle must reach the downstream orphan-shard sweep and finalize stats
// (LastRun set). Without this control the cancel test's negative assertions
// could pass vacuously on a broken refactor.
func TestRunOnce_NormalRun_ReachesDownstreamAndStats(t *testing.T) {
	m := newCancelMockBackend()
	m.records["b"] = []scrubber.ObjectRecord{
		{Bucket: "b", Key: "k0", DataShards: 2, ParityShards: 1},
	}
	// Healthy shards so the object processes cleanly without repair.
	m.storeShards("b", "k0", [][]byte{[]byte("s0"), []byte("s1"), []byte("s2")})

	s := scrubber.New(m, time.Hour)
	s.RunOnce(context.Background())

	assert.GreaterOrEqual(t, m.walkOrphanCalls(), 1, "downstream orphan-shard sweep must run on a non-cancelled cycle")
	assert.False(t, s.Stats().LastRun.IsZero(), "LastRun must be set after a full cycle completes")
}

// TestRunOnce_CancelMidScan_SkipsDownstreamAndStats characterizes the abort-
// everything cancellation contract: when ctx is cancelled mid-scan, the cycle
// must skip ALL downstream phases (orphan sweeps, redundancy) and must NOT
// finalize stats. This is the load-bearing regression guard for the runOnce
// decomposition — folding cancellation into a plain return would let the cycle
// flow into downstream sweeps with a cancelled ctx (observable behavior change).
//
// LastRun (set only at the very end, after every downstream phase) is the
// discriminator: it stays zero iff the abort-everything path held.
func TestRunOnce_CancelMidScan_SkipsDownstreamAndStats(t *testing.T) {
	m := newCancelMockBackend()
	// Two objects: the first ObjectExists call fires cancel(); the gate at the
	// top of the next object's loop iteration observes ctx.Done() and aborts.
	m.records["b"] = []scrubber.ObjectRecord{
		{Bucket: "b", Key: "k0", DataShards: 2, ParityShards: 1},
		{Bucket: "b", Key: "k1", DataShards: 2, ParityShards: 1},
	}
	m.storeShards("b", "k0", [][]byte{[]byte("s0"), []byte("s1"), []byte("s2")})
	m.storeShards("b", "k1", [][]byte{[]byte("s0"), []byte("s1"), []byte("s2")})

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	s := scrubber.New(m, time.Hour)
	s.RunOnce(ctx)

	require.True(t, m.cancelFired, "test precondition: cancel must have fired during the scan")
	assert.Equal(t, 0, m.walkOrphanCalls(), "downstream orphan-shard sweep must NOT run after mid-scan cancellation")
	assert.True(t, s.Stats().LastRun.IsZero(), "stats finalization (LastRun) must be skipped after mid-scan cancellation")
}
