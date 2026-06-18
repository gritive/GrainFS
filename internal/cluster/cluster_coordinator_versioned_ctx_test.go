package cluster

import (
	"context"
	"sync"
	"testing"
)

// ctxSpyCoordinator wraps a ClusterCoordinator and exposes the last ctx seen
// by the per-group ListObjectVersions call via lastVersionedListCtx().
type ctxSpyCoordinator struct {
	*ClusterCoordinator
	mu      sync.Mutex
	lastCtx context.Context
}

func (s *ctxSpyCoordinator) lastVersionedListCtx() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastCtx
}

// newSingleGroupTestCoordinator builds a single-group ClusterCoordinator backed
// by a real GroupBackend with a ctx-recording hook wired into ListObjectVersions.
// The bucket "vbucket" is pre-created and assigned to the sole group.
func newSingleGroupTestCoordinator(t *testing.T) *ctxSpyCoordinator {
	t.Helper()
	// newTestGroupBackend already starts the node + apply loop; do not double-start.
	gb := newTestGroupBackend(t, "g1")

	// Create the bucket so ListObjectVersions' HeadBucket succeeds.
	if err := gb.CreateBucket(context.Background(), "vbucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	spy := &ctxSpyCoordinator{}

	// Wire the test hook on the underlying DistributedBackend so we can record
	// the ctx that arrives at ListObjectVersions.
	gb.DistributedBackend.testOnListObjectVersionsCtx = func(ctx context.Context) {
		spy.mu.Lock()
		spy.lastCtx = ctx
		spy.mu.Unlock()
	}

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("g1", []string{"test-node"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("vbucket", "g1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g1": {ID: "g1", PeerIDs: []string{"test-node"}},
	}}
	ec := ECConfig{DataShards: 1, ParityShards: 0}
	spy.ClusterCoordinator = NewClusterCoordinator(&fakeBackend{}, mgr, router, meta, "test-node").WithECConfig(ec)
	return spy
}

// TestListObjectVersions_ThreadsCallerContext proves the coordinator no longer
// drops the caller ctx for a fresh context.Background(): a stamped versioning
// decision survives into the single-group read path. Behavior-neutral: the
// returned versions are unchanged; only ctx propagation is asserted.
func TestListObjectVersions_ThreadsCallerContext(t *testing.T) {
	c := newSingleGroupTestCoordinator(t)
	ctx := ContextWithBucketVersioning(context.Background(), true)

	// Spy installed by the helper records the ctx the per-group backend saw.
	if _, err := c.ListObjectVersions(ctx, "vbucket", "", 0); err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	enabled, resolved := bucketVersioningFromContext(c.lastVersionedListCtx())
	if !resolved || !enabled {
		t.Fatalf("ctx not threaded: resolved=%v enabled=%v", resolved, enabled)
	}
}
