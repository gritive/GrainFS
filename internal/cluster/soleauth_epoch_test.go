package cluster

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
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

// TestShardEnvelope_EpochRoundTrip asserts the additive admitted_soleauth_epoch
// scalar survives the build→unwrap→decode path on the shard wire envelope
// (S4c-a2-A T3). It also pins the FlatBuffers default-0 behavior for both an
// envelope built with epoch 0 and a bare ShardRequest from a hypothetical old
// peer that omits the field entirely.
func TestShardEnvelope_EpochRoundTrip(t *testing.T) {
	t.Run("nonzero epoch round-trips", func(t *testing.T) {
		envb := buildShardEnvelope("WriteQuorumMeta", "b", "k", 0, []byte("data"), 7)
		defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()

		_, srData, err := unmarshalEnvelope(envb.FinishedBytes())
		require.NoError(t, err)
		sr, err := unmarshalShardRequest(srData)
		require.NoError(t, err)
		assert.Equal(t, uint32(7), sr.AdmittedSoleAuthEpoch)
	})

	t.Run("epoch 0 decodes 0", func(t *testing.T) {
		envb := buildShardEnvelope("WriteQuorumMeta", "b", "k", 0, []byte("data"), 0)
		defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()

		_, srData, err := unmarshalEnvelope(envb.FinishedBytes())
		require.NoError(t, err)
		sr, err := unmarshalShardRequest(srData)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), sr.AdmittedSoleAuthEpoch)
	})

	t.Run("old peer omitting the field decodes default 0", func(t *testing.T) {
		// Simulate an old peer: build a bare ShardRequest WITHOUT calling
		// ShardRequestAddAdmittedSoleauthEpoch, so the field is absent on the wire.
		b := flatbuffers.NewBuilder(64)
		bucketOff := b.CreateString("b")
		keyOff := b.CreateString("k")
		pb.ShardRequestStart(b)
		pb.ShardRequestAddBucket(b, bucketOff)
		pb.ShardRequestAddKey(b, keyOff)
		b.Finish(pb.ShardRequestEnd(b))

		sr, err := unmarshalShardRequest(b.FinishedBytes())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), sr.AdmittedSoleAuthEpoch)
	})
}

// TestSoleAuthEpochStale exercises the leaf reject predicate. It fires only
// once the bucket has been flipped (local > 0) AND the wire epoch is older.
func TestSoleAuthEpochStale(t *testing.T) {
	cases := []struct {
		local, wire uint32
		want        bool
	}{
		{0, 0, false}, // dormant: never flipped
		{0, 5, false}, // dormant: never flipped (wire ahead is irrelevant at 0)
		{3, 5, false}, // wire newer than local: accept
		{5, 5, false}, // equal: accept
		{5, 3, true},  // wire older: reject
		{5, 0, true},  // wire is the default-0 (un-threaded caller) under a flipped bucket: reject
	}
	for _, c := range cases {
		require.Equalf(t, c.want, soleAuthEpochStale(c.local, c.wire),
			"soleAuthEpochStale(%d,%d)", c.local, c.wire)
	}
}

// validQuorumMetaBlob encodes a minimal decodable PutObjectMetaCmd blob so the
// leaf's write-time LWW guard (which decodes the blob) is satisfied.
func validQuorumMetaBlob(t *testing.T, bucket, key string) []byte {
	t.Helper()
	blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: bucket, Key: key, ETag: "e", Size: 1, ModTime: 1,
	})
	require.NoError(t, err)
	return blob
}

// TestLeaf_RejectsStaleEpoch verifies a per-version leaf rejects a stale wire
// epoch BEFORE touching the filesystem (no file is created on reject), and
// accepts an equal or newer wire epoch.
func TestLeaf_RejectsStaleEpoch(t *testing.T) {
	svc, _ := newTestShardService(t)
	svc.SetSoleAuthEpochFn(func(string) uint32 { return 5 })

	const bucket, key, vid = "b", "k", "v"
	sub := filepath.Join(key, vid)
	blob := validQuorumMetaBlob(t, bucket, key)
	target := filepath.Join(svc.dataDirs[0], quorumMetaVersionsSubDir, bucket, key, vid)

	// Stale wire epoch (3 < 5) → reject, and the reject precedes the FS op.
	err := svc.writeQuorumMetaVersionLocal(bucket, sub, blob, 3)
	require.ErrorIs(t, err, errStaleSoleAuthEpoch)
	_, statErr := os.Stat(target)
	require.True(t, os.IsNotExist(statErr), "reject must precede the FS write (no file)")

	// Equal epoch (5) → accept.
	require.NoError(t, svc.writeQuorumMetaVersionLocal(bucket, sub, blob, 5))
	_, statErr = os.Stat(target)
	require.NoError(t, statErr, "accepted write must create the file")

	// Newer epoch (6) → accept (idempotent re-write).
	require.NoError(t, svc.writeQuorumMetaVersionLocal(bucket, sub, blob, 6))
}

// TestLeaf_DormantAcceptsAll verifies that with the epoch source READY and the
// callback returning 0 (local epoch 0 — never flipped, every bucket today) all
// four leaves accept regardless of the admitted epoch. (The nil/unwired callback
// is now the boot-window fail-closed case — see TestLeaf_BootWindowFailsClosed.)
func TestLeaf_DormantAcceptsAll(t *testing.T) {
	svc, _ := newTestShardService(t) // helper marks ready
	svc.SetSoleAuthEpochFn(func(string) uint32 { return 0 })

	const bucket, key, vid = "b", "k", "v"
	wblob := validQuorumMetaBlob(t, bucket, key)
	// Writers accept any admitted epoch while dormant (local epoch 0, not stale).
	require.NoError(t, svc.writeQuorumMetaLocal(bucket, key, wblob, 99))
	require.NoError(t, svc.writeQuorumMetaVersionLocal(bucket, filepath.Join(key, vid), wblob, 99))
	// Deleters: absent file is idempotent; any admitted epoch accepted.
	require.NoError(t, svc.deleteQuorumMetaLocal(bucket, key, 99))
	require.NoError(t, svc.deleteQuorumMetaVersionLocal(bucket, key, vid, 99))
}

// TestLeaf_BootWindowFailsClosed covers the boot-window fence: a NOT-ready
// ShardService (the shard RPC route can be live before the group-0 FSM is
// confirmed caught up) fails closed for any non-zero admitted epoch on all four
// leaves, while still admitting epoch-0 (legit boot-time replication). It also
// covers that not-ready dominates a wired callback and that SetSoleAuthEpochFn(nil)
// clears without a nil-func-call panic.
func TestLeaf_BootWindowFailsClosed(t *testing.T) {
	newNotReady := func(t *testing.T) *ShardService {
		t.Helper()
		keeper, clusterID := testDEKKeeper(t)
		// NewShardService directly (NOT the helper) → soleAuthEpochReady defaults
		// false: the boot window.
		return NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	}
	const bucket, key, vid = "b", "k", "v"

	t.Run("not ready: wire>0 fail-closed, wire=0 admitted (all four leaves)", func(t *testing.T) {
		svc := newNotReady(t)
		blob := validQuorumMetaBlob(t, bucket, key)
		require.ErrorIs(t, svc.writeQuorumMetaLocal(bucket, key, blob, 5), errStaleSoleAuthEpoch)
		require.NoError(t, svc.writeQuorumMetaLocal(bucket, key, blob, 0))
		require.ErrorIs(t, svc.writeQuorumMetaVersionLocal(bucket, filepath.Join(key, vid), blob, 5), errStaleSoleAuthEpoch)
		require.NoError(t, svc.writeQuorumMetaVersionLocal(bucket, filepath.Join(key, vid), blob, 0))
		require.ErrorIs(t, svc.deleteQuorumMetaLocal(bucket, key, 5), errStaleSoleAuthEpoch)
		require.NoError(t, svc.deleteQuorumMetaLocal(bucket, key, 0))
		require.ErrorIs(t, svc.deleteQuorumMetaVersionLocal(bucket, key, vid, 5), errStaleSoleAuthEpoch)
		require.NoError(t, svc.deleteQuorumMetaVersionLocal(bucket, key, vid, 0))
	})

	t.Run("not ready dominates a wired callback", func(t *testing.T) {
		svc := newNotReady(t)
		svc.SetSoleAuthEpochFn(func(string) uint32 { return 5 }) // wired but NOT ready
		blob := validQuorumMetaBlob(t, bucket, key)
		require.ErrorIs(t, svc.writeQuorumMetaLocal(bucket, key, blob, 5), errStaleSoleAuthEpoch)
		require.NoError(t, svc.writeQuorumMetaLocal(bucket, key, blob, 0))
	})

	t.Run("ready + wired callback → live stale semantics", func(t *testing.T) {
		svc := newNotReady(t)
		svc.MarkSoleAuthEpochReady()
		svc.SetSoleAuthEpochFn(func(string) uint32 { return 3 })
		blob := validQuorumMetaBlob(t, bucket, key)
		require.ErrorIs(t, svc.writeQuorumMetaLocal(bucket, key, blob, 2), errStaleSoleAuthEpoch) // stale: 2<3
		require.NoError(t, svc.writeQuorumMetaLocal(bucket, key, blob, 3))                        // equal
		require.NoError(t, svc.writeQuorumMetaLocal(bucket, key, blob, 4))                        // newer
		require.ErrorIs(t, svc.writeQuorumMetaLocal(bucket, key, blob, 0), errStaleSoleAuthEpoch) // 0<3 is stale
	})

	t.Run("SetSoleAuthEpochFn(nil) clears without panic, re-fails-closed", func(t *testing.T) {
		svc := newNotReady(t)
		svc.MarkSoleAuthEpochReady()
		svc.SetSoleAuthEpochFn(func(string) uint32 { return 3 })
		svc.SetSoleAuthEpochFn(nil) // must store a nil pointer, not a non-nil ptr to a nil func
		blob := validQuorumMetaBlob(t, bucket, key)
		require.ErrorIs(t, svc.writeQuorumMetaLocal(bucket, key, blob, 5), errStaleSoleAuthEpoch)
		require.NoError(t, svc.writeQuorumMetaLocal(bucket, key, blob, 0))
	})
}
