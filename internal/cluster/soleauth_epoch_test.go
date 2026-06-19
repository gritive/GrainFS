package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBucketSoleAuthEpochKey asserts the raw key equals "soleauthepoch:{b}",
// mirroring TestSoleAuthKeyAndConsts' use of the empty (identity) keyspace.
func TestBucketSoleAuthEpochKey(t *testing.T) {
	ks := newStateKeyspaceEmpty()
	require.Equal(t, "soleauthepoch:b", string(ks.BucketSoleAuthEpochKey("b")))
}

// TestSoleAuthEpochCodec round-trips the BigEndian uint32 codec and verifies
// malformed/absent inputs decode to 0.
func TestSoleAuthEpochCodec(t *testing.T) {
	for _, n := range []uint32{0, 1, 7, 4294967295} {
		require.Equal(t, n, decodeSoleAuthEpoch(encodeSoleAuthEpoch(n)), "round-trip %d", n)
	}
	require.Equal(t, uint32(0), decodeSoleAuthEpoch(nil))
	require.Equal(t, uint32(0), decodeSoleAuthEpoch([]byte{1}))
}

// fsmSoleAuthEpoch reads the epoch directly from the FSM's db, mirroring how
// TestFSM_SetBucketSoleAuthority_DefaultOff reads the soleauth key.
func fsmSoleAuthEpoch(t *testing.T, fsm *FSM, bucket string) uint32 {
	t.Helper()
	var epoch uint32
	require.NoError(t, fsm.db.View(func(txn MetadataTxn) error {
		item, gerr := txn.Get(fsm.keys.BucketSoleAuthEpochKey(bucket))
		if gerr == ErrMetaKeyNotFound {
			return nil
		}
		require.NoError(t, gerr)
		raw, gerr := item.ValueCopy(nil)
		require.NoError(t, gerr)
		epoch = decodeSoleAuthEpoch(raw)
		return nil
	}))
	return epoch
}

func TestFSM_SoleAuthEpoch_BumpsOnRealTransition(t *testing.T) {
	fsm := newTestFSMForSoleAuth(t)
	const bucket = "b"
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: bucket})

	// Epoch starts at 0 (key absent).
	require.Equal(t, uint32(0), fsmSoleAuthEpoch(t, fsm, bucket))

	// off -> pending: real transition, epoch -> 1.
	require.NoError(t, applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
		SetBucketSoleAuthorityCmd{Bucket: bucket, State: soleAuthPending}))
	require.Equal(t, uint32(1), fsmSoleAuthEpoch(t, fsm, bucket))

	// pending -> on: real transition, epoch -> 2.
	require.NoError(t, applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
		SetBucketSoleAuthorityCmd{Bucket: bucket, State: soleAuthOn}))
	require.Equal(t, uint32(2), fsmSoleAuthEpoch(t, fsm, bucket))

	// on -> on: idempotent same-state write, epoch stays 2.
	require.NoError(t, applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
		SetBucketSoleAuthorityCmd{Bucket: bucket, State: soleAuthOn}))
	require.Equal(t, uint32(2), fsmSoleAuthEpoch(t, fsm, bucket))
}

func TestFSM_SoleAuthEpoch_AbortBumps(t *testing.T) {
	fsm := newTestFSMForSoleAuth(t)
	const bucket = "b"
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: bucket})

	// off -> pending: epoch -> 1.
	require.NoError(t, applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
		SetBucketSoleAuthorityCmd{Bucket: bucket, State: soleAuthPending}))
	require.Equal(t, uint32(1), fsmSoleAuthEpoch(t, fsm, bucket))

	// pending -> off (abort): real transition, epoch -> 2.
	require.NoError(t, applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
		SetBucketSoleAuthorityCmd{Bucket: bucket, State: soleAuthOff}))
	require.Equal(t, uint32(2), fsmSoleAuthEpoch(t, fsm, bucket))
}

func TestFSM_SoleAuthEpoch_DefaultZero(t *testing.T) {
	fsm := newTestFSMForSoleAuth(t)
	const bucket = "b"
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: bucket})

	// Never flipped: epoch key absent (ErrMetaKeyNotFound) and decodes to 0.
	require.NoError(t, fsm.db.View(func(txn MetadataTxn) error {
		_, gerr := txn.Get(fsm.keys.BucketSoleAuthEpochKey(bucket))
		assert.Equal(t, ErrMetaKeyNotFound, gerr, "absent epoch key means 0")
		return nil
	}))
	require.Equal(t, uint32(0), fsmSoleAuthEpoch(t, fsm, bucket))
}

// TestDistributedBackend_SoleAuthEpoch_EndToEnd exercises the backend-level
// reader: a fresh bucket reads epoch 0, and the epoch advances as soleauth
// transitions are applied through the backend.
func TestDistributedBackend_SoleAuthEpoch_EndToEnd(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	// Fresh bucket: epoch 0.
	ep, err := b.GetBucketSoleAuthEpoch("bucket")
	require.NoError(t, err)
	require.Equal(t, uint32(0), ep)

	// off -> pending: epoch 1.
	require.NoError(t, b.SetBucketSoleAuthority("bucket", soleAuthPending))
	ep, err = b.GetBucketSoleAuthEpoch("bucket")
	require.NoError(t, err)
	require.Equal(t, uint32(1), ep)

	// pending -> on: epoch 2.
	require.NoError(t, b.SetBucketSoleAuthority("bucket", soleAuthOn))
	ep, err = b.GetBucketSoleAuthEpoch("bucket")
	require.NoError(t, err)
	require.Equal(t, uint32(2), ep)
}
