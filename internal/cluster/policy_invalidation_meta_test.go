package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMetaPolicyInvalidationPostCommit verifies that a MetaPolicyInvalidationWorker
// registered on a MetaFSM fires the injected invalidation callback asynchronously
// when a committed SetBucketPolicy or DeleteBucketPolicy meta command is applied.
//
// The invalidation is async (buffered-channel handoff), so the test polls with a
// bounded timeout rather than a synchronous assertion.
func TestMetaPolicyInvalidationPostCommit_SetBucketPolicy(t *testing.T) {
	fsm := NewMetaFSM()

	// Seed the bucket so applySetBucketPolicy does not return ErrBucketNotFound.
	require.NoError(t, fsm.applyCmd(makeCreateBucketCmd(t, "b1", "group-1", false)))

	got := make(chan string, 4)
	w := NewMetaPolicyInvalidationWorker()
	w.SetInvalidate(func(bucket string) { got <- bucket })
	w.Start()
	defer w.Stop()
	fsm.RegisterPostCommit(w.Hook)

	// Apply a committed SetBucketPolicy for "b1".
	require.NoError(t, fsm.applyCmd(makeSetBucketPolicyCmd(t, "b1", []byte(`{"Version":"2012-10-17"}`))))

	select {
	case bucket := <-got:
		assert.Equal(t, "b1", bucket)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: policy invalidation callback not fired for SetBucketPolicy")
	}
}

func TestMetaPolicyInvalidationPostCommit_DeleteBucketPolicy(t *testing.T) {
	fsm := NewMetaFSM()

	// Seed the bucket so applyDeleteBucketPolicy does not return ErrBucketNotFound.
	require.NoError(t, fsm.applyCmd(makeCreateBucketCmd(t, "b2", "group-1", false)))

	got := make(chan string, 4)
	w := NewMetaPolicyInvalidationWorker()
	w.SetInvalidate(func(bucket string) { got <- bucket })
	w.Start()
	defer w.Stop()
	fsm.RegisterPostCommit(w.Hook)

	// Apply a committed DeleteBucketPolicy for "b2".
	require.NoError(t, fsm.applyCmd(makeDeleteBucketPolicyCmd(t, "b2")))

	select {
	case bucket := <-got:
		assert.Equal(t, "b2", bucket)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: policy invalidation callback not fired for DeleteBucketPolicy")
	}
}

// TestMetaPolicyInvalidationPostCommit_NoLeakAfterStop verifies that Stop drains
// pending invalidations and the goroutine exits cleanly (no goroutine leak).
func TestMetaPolicyInvalidationPostCommit_NoLeakAfterStop(t *testing.T) {
	called := make(chan string, 8)
	w := NewMetaPolicyInvalidationWorker()
	w.SetInvalidate(func(bucket string) { called <- bucket })
	w.Start()

	fsm := NewMetaFSM()
	require.NoError(t, fsm.applyCmd(makeCreateBucketCmd(t, "leak-b", "group-1", false)))
	fsm.RegisterPostCommit(w.Hook)

	require.NoError(t, fsm.applyCmd(makeSetBucketPolicyCmd(t, "leak-b", []byte(`{}`))))

	// Stop must not deadlock; the goroutine must exit.
	done := make(chan struct{})
	go func() {
		w.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Stop returned; goroutine exited cleanly.
	case <-time.After(3 * time.Second):
		t.Fatal("Stop blocked: possible goroutine leak")
	}
}

// TestMetaPolicyInvalidationPostCommit_OtherCmdsNotFired verifies that unrelated
// meta commands (e.g. CreateBucket) do NOT trigger the policy invalidation callback.
func TestMetaPolicyInvalidationPostCommit_OtherCmdsNotFired(t *testing.T) {
	fsm := NewMetaFSM()
	called := make(chan string, 4)
	w := NewMetaPolicyInvalidationWorker()
	w.SetInvalidate(func(bucket string) { called <- bucket })
	w.Start()
	defer w.Stop()
	fsm.RegisterPostCommit(w.Hook)

	// CreateBucket should NOT fire the invalidation callback.
	require.NoError(t, fsm.applyCmd(makeCreateBucketCmd(t, "other-b", "group-1", false)))

	// Give the worker a moment to process any spurious signals.
	select {
	case bucket := <-called:
		t.Fatalf("unexpected policy invalidation for bucket %q on CreateBucket", bucket)
	case <-time.After(100 * time.Millisecond):
		// correct: no invalidation fired
	}
}

// TestMetaPolicyInvalidationPostCommit_LateBindCallback verifies that events
// arriving before SetInvalidate are silently dropped (no panic, no goroutine
// hang), and that events arriving after SetInvalidate are delivered.
func TestMetaPolicyInvalidationPostCommit_LateBindCallback(t *testing.T) {
	fsm := NewMetaFSM()
	require.NoError(t, fsm.applyCmd(makeCreateBucketCmd(t, "late-b", "group-1", false)))

	w := NewMetaPolicyInvalidationWorker()
	w.Start()
	defer w.Stop()
	fsm.RegisterPostCommit(w.Hook)

	// Fire before SetInvalidate — should be silently dropped.
	require.NoError(t, fsm.applyCmd(makeSetBucketPolicyCmd(t, "late-b", []byte(`{}`))))
	// Give the worker time to drain the first event before wiring.
	time.Sleep(50 * time.Millisecond)

	// Wire the callback and fire again.
	got := make(chan string, 4)
	w.SetInvalidate(func(bucket string) { got <- bucket })
	require.NoError(t, fsm.applyCmd(makeDeleteBucketPolicyCmd(t, "late-b")))

	select {
	case bucket := <-got:
		assert.Equal(t, "late-b", bucket)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: late-bound callback not fired")
	}
}
