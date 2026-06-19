package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestSoleAuthKeyAndConsts(t *testing.T) {
	// stateKeyspace{} is the empty (identity) keyspace — Key(raw) returns raw unchanged.
	// Mirror keyspace_test.go usage of newStateKeyspaceEmpty().
	ks := newStateKeyspaceEmpty()
	require.Equal(t, "soleauth:b", string(ks.BucketSoleAuthKey("b")))
	require.Equal(t, "off", soleAuthOff)
	require.Equal(t, "pending", soleAuthPending)
	require.Equal(t, "on", soleAuthOn)
	_ = SetBucketSoleAuthorityCmd{Bucket: "b", State: soleAuthOn}
}

func TestSetBucketSoleAuthorityCmd_RoundTrip(t *testing.T) {
	raw, err := EncodeCommand(CmdSetBucketSoleAuthority, SetBucketSoleAuthorityCmd{Bucket: "b", State: soleAuthPending})
	require.NoError(t, err)
	cmd, err := DecodeCommand(raw) // returns {Type, Data} only — NO typed Payload (fsm.go:355)
	require.NoError(t, err)
	require.Equal(t, CmdSetBucketSoleAuthority, cmd.Type)
	got, err := decodeSetBucketSoleAuthorityCmd(cmd.Data) // decode the payload bytes
	require.NoError(t, err)
	require.Equal(t, "b", got.Bucket)
	require.Equal(t, soleAuthPending, got.State)
}

// TestSoleAuthTransitionAllowed covers the full transition matrix for the pure guard helper.
func TestSoleAuthTransitionAllowed(t *testing.T) {
	cases := []struct {
		from, to string
		want     bool
	}{
		// idempotent
		{soleAuthOff, soleAuthOff, true},
		{soleAuthPending, soleAuthPending, true},
		{soleAuthOn, soleAuthOn, true},
		// allowed forward transitions
		{soleAuthOff, soleAuthPending, true},
		{soleAuthPending, soleAuthOn, true},
		// allowed abort
		{soleAuthPending, soleAuthOff, true},
		// refused: must go through pending
		{soleAuthOff, soleAuthOn, false},
		// refused: on is terminal
		{soleAuthOn, soleAuthPending, false},
		{soleAuthOn, soleAuthOff, false},
		// refused: unknown states
		{"off", "bogus", false},
		{"bogus", "off", false},
		{"", "on", false},
	}
	for _, tc := range cases {
		t.Run(tc.from+"->"+tc.to, func(t *testing.T) {
			assert.Equal(t, tc.want, soleAuthTransitionAllowed(tc.from, tc.to))
		})
	}
}

// bucketSoleAuthKey is a test-only helper mirroring bucketVerKey.
func bucketSoleAuthKey(bucket string) []byte { return []byte("soleauth:" + bucket) }

// newTestFSMForSoleAuth builds a fresh FSM with an empty keyspace for soleauth tests.
func newTestFSMForSoleAuth(t *testing.T) *FSM {
	t.Helper()
	return NewFSM(newTestStore(t), newStateKeyspaceEmpty())
}

// applyCmdErr is a small helper to encode+apply a command and return the error.
func applyCmdErr(t *testing.T, fsm *FSM, cmdType CommandType, payload interface{}) error {
	t.Helper()
	data, err := EncodeCommand(cmdType, payload)
	require.NoError(t, err)
	return fsm.Apply(data)
}

// seedSoleAuth sets the stored soleauth key by walking valid transitions.
// It is the canonical way to reach a specific `from` state before testing
// a target transition. The bucket must already exist.
func seedSoleAuth(t *testing.T, fsm *FSM, bucket, target string) {
	t.Helper()
	switch target {
	case soleAuthOff:
		// default — do nothing
	case soleAuthPending:
		require.NoError(t, applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
			SetBucketSoleAuthorityCmd{Bucket: bucket, State: soleAuthPending}))
	case soleAuthOn:
		require.NoError(t, applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
			SetBucketSoleAuthorityCmd{Bucket: bucket, State: soleAuthPending}))
		require.NoError(t, applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
			SetBucketSoleAuthorityCmd{Bucket: bucket, State: soleAuthOn}))
	default:
		t.Fatalf("seedSoleAuth: unknown target %q", target)
	}
}

func TestFSM_SetBucketSoleAuthority_OneWayGuard(t *testing.T) {
	cases := []struct {
		from   string
		to     string
		wantOK bool
	}{
		// idempotent
		{soleAuthOff, soleAuthOff, true},
		{soleAuthPending, soleAuthPending, true},
		{soleAuthOn, soleAuthOn, true},
		// allowed forward
		{soleAuthOff, soleAuthPending, true},
		{soleAuthPending, soleAuthOn, true},
		// allowed abort
		{soleAuthPending, soleAuthOff, true},
		// refused: skip pending
		{soleAuthOff, soleAuthOn, false},
		// refused: on is terminal
		{soleAuthOn, soleAuthPending, false},
		{soleAuthOn, soleAuthOff, false},
		// invalid state
		{soleAuthOff, "bogus", false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.from+"->"+tc.to, func(t *testing.T) {
			fsm := newTestFSMForSoleAuth(t)
			const bucket = "b"
			applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: bucket})

			// Seed the from state via valid transitions.
			seedSoleAuth(t, fsm, bucket, tc.from)

			// Now attempt the transition under test.
			err := applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
				SetBucketSoleAuthorityCmd{Bucket: bucket, State: tc.to})

			if tc.wantOK {
				require.NoError(t, err, "transition %s->%s should be allowed", tc.from, tc.to)
				// Verify the stored state equals tc.to.
				fsm.db.View(func(txn MetadataTxn) error {
					item, gerr := txn.Get([]byte("soleauth:" + bucket))
					require.NoError(t, gerr)
					raw, gerr := item.ValueCopy(nil)
					require.NoError(t, gerr)
					assert.Equal(t, tc.to, string(raw))
					return nil
				})
			} else {
				require.Error(t, err, "transition %s->%s should be refused", tc.from, tc.to)
				// Verify the stored state is unchanged (still tc.from).
				fsm.db.View(func(txn MetadataTxn) error {
					item, gerr := txn.Get([]byte("soleauth:" + bucket))
					if gerr == ErrMetaKeyNotFound {
						// absent means off
						assert.Equal(t, soleAuthOff, tc.from,
							"stored absent but expected from=%s", tc.from)
						return nil
					}
					require.NoError(t, gerr)
					raw, gerr := item.ValueCopy(nil)
					require.NoError(t, gerr)
					assert.Equal(t, tc.from, string(raw))
					return nil
				})
			}
		})
	}
}

func TestFSM_SetBucketSoleAuthority_BucketNotFound(t *testing.T) {
	fsm := newTestFSMForSoleAuth(t)
	err := applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
		SetBucketSoleAuthorityCmd{Bucket: "ghost", State: soleAuthPending})
	require.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestFSM_SetBucketSoleAuthority_InvalidState(t *testing.T) {
	fsm := newTestFSMForSoleAuth(t)
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: "b"})
	err := applyCmdErr(t, fsm, CmdSetBucketSoleAuthority,
		SetBucketSoleAuthorityCmd{Bucket: "b", State: "bogus"})
	require.Error(t, err)
}

func TestFSM_SetBucketSoleAuthority_DefaultOff(t *testing.T) {
	fsm := newTestFSMForSoleAuth(t)
	applyCmd(t, fsm, CmdCreateBucket, CreateBucketCmd{Bucket: "b"})
	// No soleauth apply — key must be absent (= "off" by convention).
	fsm.db.View(func(txn MetadataTxn) error {
		_, gerr := txn.Get([]byte("soleauth:b"))
		assert.Equal(t, ErrMetaKeyNotFound, gerr, "absent key means soleAuthOff")
		return nil
	})
}

func TestDistributedBackend_SoleAuth_EndToEnd(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	// Absent key defaults to "off".
	st, err := b.GetBucketSoleAuthority("bucket")
	require.NoError(t, err)
	require.Equal(t, soleAuthOff, st)

	// off -> pending
	require.NoError(t, b.SetBucketSoleAuthority("bucket", soleAuthPending))
	st, err = b.GetBucketSoleAuthority("bucket")
	require.NoError(t, err)
	require.Equal(t, soleAuthPending, st)

	// pending -> on
	require.NoError(t, b.SetBucketSoleAuthority("bucket", soleAuthOn))
	st, err = b.GetBucketSoleAuthority("bucket")
	require.NoError(t, err)
	require.Equal(t, soleAuthOn, st)

	// on -> off: refused (on is terminal)
	err = b.SetBucketSoleAuthority("bucket", soleAuthOff)
	require.Error(t, err, "transition on->off must be refused")

	// State must remain "on" after the refused transition.
	st, err = b.GetBucketSoleAuthority("bucket")
	require.NoError(t, err)
	require.Equal(t, soleAuthOn, st)
}

// TestSnapshot_SoleAuthRoundTrip verifies that a bucket set to "pending" is
// captured by ListAllBuckets (SoleAuthState=="pending") and that RestoreBuckets
// onto a fresh backend reproduces the "pending" state.
func TestSnapshot_SoleAuthRoundTrip(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	require.NoError(t, b.SetBucketSoleAuthority("bucket", soleAuthPending))

	bks, err := b.ListAllBuckets()
	require.NoError(t, err)

	var found *storage.SnapshotBucket
	for i := range bks {
		if bks[i].Name == "bucket" {
			found = &bks[i]
		}
	}
	require.NotNil(t, found)
	require.Equal(t, soleAuthPending, found.SoleAuthState)

	// Restore onto a fresh backend.
	b2 := newTestDistributedBackend(t)
	require.NoError(t, b2.RestoreBuckets(bks))
	st, err := b2.GetBucketSoleAuthority("bucket")
	require.NoError(t, err)
	require.Equal(t, soleAuthPending, st)
}

// TestSnapshot_SoleAuthRestoreOn verifies that a bucket set all the way to "on"
// is captured and restored correctly. The one-way guard means a fresh restored
// bucket (starting at off) must walk off->pending->on to reach the snapshot's
// "on" state.
func TestSnapshot_SoleAuthRestoreOn(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	require.NoError(t, b.SetBucketSoleAuthority("bucket", soleAuthPending))
	require.NoError(t, b.SetBucketSoleAuthority("bucket", soleAuthOn))

	bks, err := b.ListAllBuckets()
	require.NoError(t, err)

	var found *storage.SnapshotBucket
	for i := range bks {
		if bks[i].Name == "bucket" {
			found = &bks[i]
		}
	}
	require.NotNil(t, found)
	require.Equal(t, soleAuthOn, found.SoleAuthState)

	// Restore onto a fresh backend; RestoreBuckets must walk off->pending->on.
	b2 := newTestDistributedBackend(t)
	require.NoError(t, b2.RestoreBuckets(bks))
	st, err := b2.GetBucketSoleAuthority("bucket")
	require.NoError(t, err)
	require.Equal(t, soleAuthOn, st)
}

// TestSnapshot_SoleAuthRestoreIdempotent verifies that RestoreBuckets is
// idempotent for the soleauth state: re-restoring a snapshot onto a backend
// where the bucket is ALREADY at the snapshot's state must not propose a guard-
// refused transition (e.g. on->pending) and must not error. RestoreBuckets
// supports restore-onto-existing (CmdCreateBucket is guarded by ErrBucketNotFound).
func TestSnapshot_SoleAuthRestoreIdempotent(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	require.NoError(t, b.SetBucketSoleAuthority("bucket", soleAuthPending))
	require.NoError(t, b.SetBucketSoleAuthority("bucket", soleAuthOn))

	bks, err := b.ListAllBuckets()
	require.NoError(t, err)

	// Restore TWICE onto the SAME backend (bucket already "on"). The second
	// restore must be a no-op for soleauth, not an on->pending refusal.
	require.NoError(t, b.RestoreBuckets(bks))
	require.NoError(t, b.RestoreBuckets(bks))
	st, err := b.GetBucketSoleAuthority("bucket")
	require.NoError(t, err)
	require.Equal(t, soleAuthOn, st)
}

// TestSnapshot_SoleAuthRestorePendingIdempotent verifies re-restore of a
// "pending" bucket is also a no-op (pending==pending idempotent).
func TestSnapshot_SoleAuthRestorePendingIdempotent(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))
	require.NoError(t, b.SetBucketSoleAuthority("bucket", soleAuthPending))

	bks, err := b.ListAllBuckets()
	require.NoError(t, err)

	require.NoError(t, b.RestoreBuckets(bks))
	require.NoError(t, b.RestoreBuckets(bks))
	st, err := b.GetBucketSoleAuthority("bucket")
	require.NoError(t, err)
	require.Equal(t, soleAuthPending, st)
}

// TestSnapshot_SoleAuthDefaultOffOmitted verifies that a bucket that was never
// given a soleauth state has SoleAuthState=="" in the snapshot (omitempty) and
// that RestoreBuckets is a no-op for it (resulting state is still "off").
func TestSnapshot_SoleAuthDefaultOffOmitted(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "bucket"))

	bks, err := b.ListAllBuckets()
	require.NoError(t, err)

	var found *storage.SnapshotBucket
	for i := range bks {
		if bks[i].Name == "bucket" {
			found = &bks[i]
		}
	}
	require.NotNil(t, found)
	// "off" is the default; omitempty means the field is empty string in the struct
	// (the JSON tag omits it, but the in-memory value is "" = treated as off).
	require.Equal(t, "", found.SoleAuthState, "default off is stored as empty (omitempty)")

	// Restore: no soleauth propose needed; result is still off.
	b2 := newTestDistributedBackend(t)
	require.NoError(t, b2.RestoreBuckets(bks))
	st, err := b2.GetBucketSoleAuthority("bucket")
	require.NoError(t, err)
	require.Equal(t, soleAuthOff, st)
}
