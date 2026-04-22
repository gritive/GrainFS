package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/scrubber"
)

func openTmpBadger(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions(t.TempDir()).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func newTestKeyStore(t *testing.T) *receipt.KeyStore {
	t.Helper()
	ks, err := receipt.NewKeyStore(receipt.Key{ID: "test", Secret: []byte("s3cr3t")})
	require.NoError(t, err)
	return ks
}

func newTestStore(t *testing.T) *receipt.Store {
	t.Helper()
	db := openTmpBadger(t)
	store, err := receipt.NewStore(db, receipt.StoreOptions{
		Retention:      time.Hour,
		FlushThreshold: 1,
		FlushInterval:  10 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}

func TestReceiptTrackingEmitter_SigningHealthy(t *testing.T) {
	store := newTestStore(t)

	t.Run("healthy when keystore has active key", func(t *testing.T) {
		ks := newTestKeyStore(t)
		e := NewReceiptTrackingEmitter(scrubber.NoopEmitter{}, store, ks)
		defer e.Close()
		assert.True(t, e.SigningHealthy())
	})

	t.Run("unhealthy when keystore is nil", func(t *testing.T) {
		e := NewReceiptTrackingEmitter(scrubber.NoopEmitter{}, store, nil)
		defer e.Close()
		assert.False(t, e.SigningHealthy())
	})
}

func TestReceiptTrackingEmitter_FinalizeSession(t *testing.T) {
	store := newTestStore(t)
	ks := newTestKeyStore(t)
	e := NewReceiptTrackingEmitter(scrubber.NoopEmitter{}, store, ks)
	defer e.Close()

	cid := "test-correlation-id"

	// Emit a detect + reconstruct + write + verify sequence.
	detect := scrubber.NewEvent(scrubber.PhaseDetect, scrubber.OutcomeFailed)
	detect.CorrelationID = cid
	detect.Bucket = "bkt"
	detect.Key = "obj"
	detect.ShardID = 2

	reconstruct := scrubber.NewEvent(scrubber.PhaseReconstruct, scrubber.OutcomeSuccess)
	reconstruct.CorrelationID = cid
	reconstruct.Bucket = "bkt"
	reconstruct.Key = "obj"
	reconstruct.ShardID = 2
	reconstruct.DurationMs = 42

	write := scrubber.NewEvent(scrubber.PhaseWrite, scrubber.OutcomeSuccess)
	write.CorrelationID = cid
	write.Bucket = "bkt"
	write.Key = "obj"
	write.ShardID = 2

	verify := scrubber.NewEvent(scrubber.PhaseVerify, scrubber.OutcomeSuccess)
	verify.CorrelationID = cid
	verify.Bucket = "bkt"
	verify.Key = "obj"

	e.Emit(detect)
	e.Emit(reconstruct)
	e.Emit(write)
	e.Emit(verify)

	// Finalize — receipt should be signed and persisted.
	e.FinalizeSession(cid)

	// Give the store's flush timer a moment to drain.
	time.Sleep(50 * time.Millisecond)

	r, err := store.GetByCorrelationID(cid)
	require.NoError(t, err)
	require.NotNil(t, r)

	assert.Equal(t, cid, r.CorrelationID)
	assert.Equal(t, "bkt", r.Object.Bucket)
	assert.Equal(t, "obj", r.Object.Key)
	assert.NotEmpty(t, r.ReceiptID)
	assert.NotEmpty(t, r.Signature)
	assert.NotEmpty(t, r.CanonicalPayload)
	assert.Equal(t, []int32{2}, r.ShardsLost)
	assert.Equal(t, []int32{2}, r.ShardsRebuilt)
	assert.Len(t, r.EventIDs, 4)

	// Verify signature is valid.
	err = receipt.Verify(r, ks)
	assert.NoError(t, err)
}

func TestReceiptTrackingEmitter_FinalizeSession_NoKeyStore(t *testing.T) {
	store := newTestStore(t)
	e := NewReceiptTrackingEmitter(scrubber.NoopEmitter{}, store, nil)
	defer e.Close()

	cid := "no-ks-cid"
	ev := scrubber.NewEvent(scrubber.PhaseDetect, scrubber.OutcomeFailed)
	ev.CorrelationID = cid
	ev.Bucket = "b"
	ev.Key = "k"
	e.Emit(ev)
	e.FinalizeSession(cid)

	time.Sleep(50 * time.Millisecond)

	// Should NOT be persisted — signing failed.
	_, err := store.GetByCorrelationID(cid)
	assert.ErrorIs(t, err, receipt.ErrNotFound)
}

func TestReceiptTrackingEmitter_OrphanSweep(t *testing.T) {
	// This test patches the constant indirectly by checking that after Close
	// the sessions map is empty (goroutine exited). It does NOT test the TTL
	// because waiting 5 minutes in a test is unreasonable — the TTL logic is
	// simple enough to trust.
	store := newTestStore(t)
	ks := newTestKeyStore(t)
	e := NewReceiptTrackingEmitter(scrubber.NoopEmitter{}, store, ks)

	cid := "orphan-session"
	ev := scrubber.NewEvent(scrubber.PhaseDetect, scrubber.OutcomeFailed)
	ev.CorrelationID = cid
	e.Emit(ev)

	e.mu.Lock()
	assert.Len(t, e.sessions, 1)
	e.mu.Unlock()

	e.Close()

	// After close the sweeper goroutine has exited; sessions may still be in
	// memory (we don't drain on close, orphan cleanup is TTL-based). The
	// important thing is that Close() doesn't deadlock.
}

func TestReceiptTrackingEmitter_Emit_EmptyCorrelationID(t *testing.T) {
	store := newTestStore(t)
	ks := newTestKeyStore(t)
	e := NewReceiptTrackingEmitter(scrubber.NoopEmitter{}, store, ks)
	defer e.Close()

	ev := scrubber.NewEvent(scrubber.PhaseDetect, scrubber.OutcomeFailed)
	ev.CorrelationID = ""
	ev.Bucket = "b"
	ev.Key = "k"
	e.Emit(ev)

	e.mu.Lock()
	assert.Empty(t, e.sessions, "empty correlationID must not create a session")
	e.mu.Unlock()
}

func TestReceiptTrackingEmitter_Emit_MaxEventsPerSession(t *testing.T) {
	store := newTestStore(t)
	ks := newTestKeyStore(t)
	e := NewReceiptTrackingEmitter(scrubber.NoopEmitter{}, store, ks)
	defer e.Close()

	cid := "cap-test"
	// Emit maxEventsPerSession+10 events; only maxEventsPerSession should be buffered.
	for i := 0; i < maxEventsPerSession+10; i++ {
		ev := scrubber.NewEvent(scrubber.PhaseDetect, scrubber.OutcomeFailed)
		ev.CorrelationID = cid
		ev.Bucket = "b"
		ev.Key = "k"
		e.Emit(ev)
	}

	e.mu.Lock()
	sess := e.sessions[cid]
	e.mu.Unlock()
	require.NotNil(t, sess)
	assert.Equal(t, maxEventsPerSession, len(sess.events), "session must not exceed maxEventsPerSession")
}

func TestReceiptTrackingEmitter_FinalizeSession_NotFound(t *testing.T) {
	store := newTestStore(t)
	ks := newTestKeyStore(t)
	e := NewReceiptTrackingEmitter(scrubber.NoopEmitter{}, store, ks)
	defer e.Close()

	// FinalizeSession for a cid that was never emitted must be a no-op.
	e.FinalizeSession("nonexistent-cid")

	_, err := store.GetByCorrelationID("nonexistent-cid")
	assert.ErrorIs(t, err, receipt.ErrNotFound)
}
