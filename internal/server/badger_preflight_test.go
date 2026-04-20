package server

import (
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreflightBadger_HealthyDB(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	cap := &captureSrvEmitter{}
	require.NoError(t, PreflightBadger(db, dir, cap))

	require.Len(t, cap.events, 1)
	assert.Equal(t, "preflight_ok", cap.events[0].ErrCode)

	// Sentinel must be cleaned up on success.
	err = db.View(func(txn *badger.Txn) error {
		_, e := txn.Get(preflightSentinel)
		return e
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestPreflightBadger_NilDB(t *testing.T) {
	err := PreflightBadger(nil, "/tmp/x", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil badger DB")
}

func TestPreflightBadger_RecoveryGuideOnFailure(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Operating on a closed DB returns ErrDBClosed; the wrapper must surface
	// the recovery guide so the operator knows what to do next.
	err = PreflightBadger(db, dir, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Recovery guide")
	assert.Contains(t, err.Error(), dir)
	assert.True(t,
		strings.Contains(err.Error(), "preflight write failed") ||
			strings.Contains(err.Error(), "preflight read failed"),
		"error should name the failed operation, got %q", err.Error())
}
