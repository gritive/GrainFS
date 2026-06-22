package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestFSMForBucketReuse(t *testing.T) *FSM {
	t.Helper()
	return NewFSM(newTestStore(t), newStateKeyspaceEmpty())
}

func applyCmdErr(t *testing.T, fsm *FSM, cmdType CommandType, payload interface{}) error {
	t.Helper()
	data, err := EncodeCommand(cmdType, payload)
	require.NoError(t, err)
	return fsm.Apply(data)
}

// TestDeleteBucket_ClearsVersioning proves a recreated same-name bucket does not
// inherit the prior incarnation's versioning state (live S3-correctness: a new
// bucket is unversioned).
func TestDeleteBucket_ClearsVersioning(t *testing.T) {
	fsm := newTestFSMForBucketReuse(t)
	const b = "b"
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: b})
	applyCmd(t, fsm, CmdSetBucketVersioning, SetBucketVersioningCmd{Bucket: b, State: "Enabled"})
	require.True(t, dbHasKey(t, fsm.db, fsm.keys.BucketVerKey(b)))

	applyCmd(t, fsm, CmdDeleteBucket, DeleteBucketCmd{Bucket: b})
	require.False(t, dbHasKey(t, fsm.db, fsm.keys.BucketVerKey(b)),
		"versioning state must be cleared on delete (recreated bucket is unversioned)")
}

// TestDeleteBucket_ClearsPolicy proves a recreated same-name bucket does not
// inherit the prior incarnation's bucket policy.
func TestDeleteBucket_ClearsPolicy(t *testing.T) {
	fsm := newTestFSMForBucketReuse(t)
	const b = "b"
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: b})
	applyCmd(t, fsm, CmdSetBucketPolicy, SetBucketPolicyCmd{Bucket: b, PolicyJSON: []byte(`{"Version":"2012-10-17"}`)})
	require.True(t, dbHasKey(t, fsm.db, fsm.keys.BucketPolicyKey(b)))

	applyCmd(t, fsm, CmdDeleteBucket, DeleteBucketCmd{Bucket: b})
	require.False(t, dbHasKey(t, fsm.db, fsm.keys.BucketPolicyKey(b)),
		"bucket policy must be cleared on delete")
}

// TestDeleteBucket_AbsentStateKeys_NoError proves the extra state-key deletes
// tolerate absent keys: a plain bucket (no policy/versioning ever set) deletes
// cleanly.
func TestDeleteBucket_AbsentStateKeys_NoError(t *testing.T) {
	fsm := newTestFSMForBucketReuse(t)
	const b = "b"
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: b})
	require.NoError(t, applyCmdErr(t, fsm, CmdDeleteBucket, DeleteBucketCmd{Bucket: b}),
		"delete must tolerate absent per-bucket state keys")
	require.False(t, dbHasKey(t, fsm.db, fsm.keys.BucketKey(b)))
}
