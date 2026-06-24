package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestBucketRecord_ApplyPutBucketAssignment_ReplayCompat verifies that
// applyPutBucketAssignment (the existing command) populates BucketRecord.GroupID
// and that BucketAssignments() still returns the bucket→groupID view.
func TestBucketRecord_ApplyPutBucketAssignment_ReplayCompat(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "b1", "group-2")))

	rec, ok := f.BucketRecord("b1")
	require.True(t, ok)
	assert.Equal(t, "group-2", rec.GroupID)
	assert.Equal(t, "", rec.Versioning)
	assert.Empty(t, rec.Policy)

	assignments := f.BucketAssignments()
	assert.Equal(t, map[string]string{"b1": "group-2"}, assignments)
}

// TestBucketRecord_DeepCopy verifies that mutating the returned Policy slice
// does NOT change FSM state.
func TestBucketRecord_DeepCopy(t *testing.T) {
	f := NewMetaFSM()
	f.mu.Lock()
	f.bucketRecords["b1"] = BucketRecord{
		GroupID:    "group-1",
		Versioning: "Enabled",
		Policy:     []byte(`{"Version":"2012-10-17"}`),
	}
	f.mu.Unlock()

	rec1, ok := f.BucketRecord("b1")
	require.True(t, ok)

	// Mutate the returned slice.
	rec1.Policy[0] = 'X'

	// FSM state must be unaffected.
	rec2, ok2 := f.BucketRecord("b1")
	require.True(t, ok2)
	assert.Equal(t, byte('{'), rec2.Policy[0], "FSM Policy must not be aliased via BucketRecord return")
}

// TestBucketRecord_SnapshotRoundTrip verifies that Versioning and Policy survive
// a Snapshot/Restore cycle.
func TestBucketRecord_SnapshotRoundTrip(t *testing.T) {
	f := NewMetaFSM()
	wireTestKEK(t, f)

	f.mu.Lock()
	f.bucketRecords["b1"] = BucketRecord{
		GroupID:    "group-3",
		Versioning: "Enabled",
		Policy:     []byte(`{"Version":"2012-10-17","Statement":[]}`),
	}
	f.bucketRecords["b2"] = BucketRecord{
		GroupID: "group-4",
	}
	f.mu.Unlock()

	snap, err := f.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	f2 := NewMetaFSM()
	wireTestKEK(t, f2)
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))

	r1, ok1 := f2.BucketRecord("b1")
	require.True(t, ok1)
	assert.Equal(t, "group-3", r1.GroupID)
	assert.Equal(t, "Enabled", r1.Versioning)
	assert.Equal(t, []byte(`{"Version":"2012-10-17","Statement":[]}`), r1.Policy)

	r2, ok2 := f2.BucketRecord("b2")
	require.True(t, ok2)
	assert.Equal(t, "group-4", r2.GroupID)
	assert.Equal(t, "", r2.Versioning)
	assert.Empty(t, r2.Policy)

	// BucketAssignments view must also be correct.
	assignments := f2.BucketAssignments()
	assert.Equal(t, "group-3", assignments["b1"])
	assert.Equal(t, "group-4", assignments["b2"])
}

// TestBucketRecordExists checks the boolean existence helper.
func TestBucketRecordExists(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0")))

	assert.True(t, f.BucketRecordExists("photos"))
	assert.False(t, f.BucketRecordExists("missing"))
}

// TestAllBucketRecords verifies the deep-copied bulk accessor.
func TestAllBucketRecords(t *testing.T) {
	f := NewMetaFSM()
	f.mu.Lock()
	f.bucketRecords["a"] = BucketRecord{GroupID: "g1", Policy: []byte("p1")}
	f.bucketRecords["b"] = BucketRecord{GroupID: "g2"}
	f.mu.Unlock()

	all := f.AllBucketRecords()
	require.Len(t, all, 2)
	assert.Equal(t, "g1", all["a"].GroupID)
	assert.Equal(t, []byte("p1"), all["a"].Policy)
	assert.Equal(t, "g2", all["b"].GroupID)

	// Mutating returned map/slice must not affect FSM.
	all["a"].Policy[0] = 'X'
	inner, _ := f.BucketRecord("a")
	assert.Equal(t, byte('p'), inner.Policy[0], "AllBucketRecords must deep-copy Policy")
}
