package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

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

// TestShardService_BucketSoleAuthLock_StableIdentity asserts the per-bucket
// fence RWMutex accessor returns the SAME pointer for a bucket name on repeat
// calls (LoadOrStore convergence) and a DISTINCT pointer for different buckets.
// A zero-value ShardService suffices: bucketSoleAuthLock only touches the map.
func TestShardService_BucketSoleAuthLock_StableIdentity(t *testing.T) {
	s := &ShardService{}
	a1 := s.bucketSoleAuthLock("a")
	a2 := s.bucketSoleAuthLock("a")
	b1 := s.bucketSoleAuthLock("b")
	require.NotNil(t, a1)
	require.True(t, a1 == a2, "same bucket must return identical *sync.RWMutex")
	require.True(t, a1 != b1, "different buckets must return distinct *sync.RWMutex")
}

// TestFSM_Flip_HoldsFenceWriteLock proves applySetBucketSoleAuthority takes the
// per-bucket fence WRITE-lock: while a goroutine holds the same mutex's RLock,
// a flip cannot complete; once the RLock is released, the flip completes.
// Deterministic — channels + bounded waits, no sleep-as-assertion.
func TestFSM_Flip_HoldsFenceWriteLock(t *testing.T) {
	fsm := newTestFSMForSoleAuth(t)
	const bucket = "b"
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: bucket})

	fence := &sync.RWMutex{}
	fsm.SetFenceLock(func(string) *sync.RWMutex { return fence })

	// Reader goroutine holds the fence RLock until told to release.
	rHeld := make(chan struct{})
	rRelease := make(chan struct{})
	var rWG sync.WaitGroup
	rWG.Add(1)
	go func() {
		defer rWG.Done()
		fence.RLock()
		close(rHeld)
		<-rRelease
		fence.RUnlock()
	}()
	<-rHeld // RLock is definitely held now.

	// Flip in another goroutine; it must BLOCK on the fence WLock.
	flipDone := make(chan error, 1)
	go func() {
		flipDone <- applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
			SetBucketSoleAuthorityCmd{Bucket: bucket, State: soleAuthPending})
	}()

	select {
	case <-flipDone:
		t.Fatal("flip completed while fence RLock was held — WLock not taken")
	case <-time.After(100 * time.Millisecond):
		// Expected: flip is blocked on the WLock.
	}

	// Release the RLock; the flip must now complete.
	close(rRelease)
	select {
	case err := <-flipDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("flip did not complete after fence RLock released")
	}
	rWG.Wait()

	// The transition actually landed.
	require.Equal(t, uint32(1), fsmSoleAuthEpoch(t, fsm, bucket))
}
