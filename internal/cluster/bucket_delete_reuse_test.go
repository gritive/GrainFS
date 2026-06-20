package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDeleteBucket_ClearsSoleAuthState_PreservesEpochFloor is the crux of the
// bucket-name-reuse fix. Deleting a bucket must clear its soleauth STATE (so a
// recreated same-name bucket is not stuck in terminal `on`) while PRESERVING the
// monotonic soleauth epoch — a cross-incarnation floor. Resetting the epoch would
// let a dead incarnation's stale forwarded write pass the soleauth fence on the
// recreated bucket.
func TestDeleteBucket_ClearsSoleAuthState_PreservesEpochFloor(t *testing.T) {
	fsm := newTestFSMForSoleAuth(t)
	const b = "b"
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: b})
	applyCmd(t, fsm, CmdSetBucketSoleAuthority, SetBucketSoleAuthorityCmd{Bucket: b, State: soleAuthPending}) // epoch 1
	applyCmd(t, fsm, CmdSetBucketSoleAuthority, SetBucketSoleAuthorityCmd{Bucket: b, State: soleAuthOn})      // epoch 2, state on
	require.Equal(t, uint32(2), fsmSoleAuthEpoch(t, fsm, b))

	applyCmd(t, fsm, CmdDeleteBucket, DeleteBucketCmd{Bucket: b})

	// State cleared (recreated bucket starts off, not stuck terminal-on)...
	require.False(t, dbHasKey(t, fsm.db, fsm.keys.BucketSoleAuthKey(b)),
		"soleauth STATE must be cleared on delete")
	// ...epoch PRESERVED as the monotonic floor.
	require.True(t, dbHasKey(t, fsm.db, fsm.keys.BucketSoleAuthEpochKey(b)),
		"soleauth epoch must be PRESERVED across delete (cross-incarnation floor)")
	require.Equal(t, uint32(2), fsmSoleAuthEpoch(t, fsm, b), "preserved epoch floor == dead incarnation max")

	// Recreate + reflip: the epoch climbs from the floor (2+1=3), never restarts at
	// 1 — so a dead incarnation's epoch-2 stale write can never be admitted.
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: b})
	require.NoError(t, applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
		SetBucketSoleAuthorityCmd{Bucket: b, State: soleAuthPending}),
		"recreated bucket reads off (state cleared) so the flip is allowed")
	require.Equal(t, uint32(3), fsmSoleAuthEpoch(t, fsm, b),
		"reflip climbs from the preserved floor (2+1), not from 1")
}

// TestDeleteBucket_ClearsVersioning proves a recreated same-name bucket does not
// inherit the prior incarnation's versioning state (live S3-correctness: a new
// bucket is unversioned).
func TestDeleteBucket_ClearsVersioning(t *testing.T) {
	fsm := newTestFSMForSoleAuth(t)
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
	fsm := newTestFSMForSoleAuth(t)
	const b = "b"
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: b})
	applyCmd(t, fsm, CmdSetBucketPolicy, SetBucketPolicyCmd{Bucket: b, PolicyJSON: []byte(`{"Version":"2012-10-17"}`)})
	require.True(t, dbHasKey(t, fsm.db, fsm.keys.BucketPolicyKey(b)))

	applyCmd(t, fsm, CmdDeleteBucket, DeleteBucketCmd{Bucket: b})
	require.False(t, dbHasKey(t, fsm.db, fsm.keys.BucketPolicyKey(b)),
		"bucket policy must be cleared on delete")
}

// TestDeleteBucket_AbsentStateKeys_NoError proves the extra state-key deletes
// tolerate absent keys: a plain bucket (no policy/versioning/soleauth ever set)
// deletes cleanly.
func TestDeleteBucket_AbsentStateKeys_NoError(t *testing.T) {
	fsm := newTestFSMForSoleAuth(t)
	const b = "b"
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: b})
	require.NoError(t, applyCmdErr(t, fsm, CmdDeleteBucket, DeleteBucketCmd{Bucket: b}),
		"delete must tolerate absent per-bucket state keys")
	require.False(t, dbHasKey(t, fsm.db, fsm.keys.BucketKey(b)))
}
