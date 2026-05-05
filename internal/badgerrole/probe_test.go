package badgerrole

import (
	"errors"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyProbeDoesNotCreateSentinel(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("existing"), []byte("ok"))
	}))
	require.NoError(t, db.Close())

	decision := ProbeReadOnly(RoleMeta, "", dir, func(readOnly bool) (*badger.DB, error) {
		return badger.Open(badger.DefaultOptions(dir).WithLogger(nil).WithReadOnly(readOnly))
	})
	require.Equal(t, DecisionOK, decision.Status)
	assert.True(t, decision.OpenedReadOnly)

	db, err = badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	defer db.Close()
	err = db.View(func(txn *badger.Txn) error {
		_, e := txn.Get(PreflightSentinelKey())
		return e
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestWritableProbeWritesReadsAndDeletesSentinel(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	defer db.Close()

	decision := ProbeWritable(db, RoleMeta, "", dir)
	require.Equal(t, DecisionOK, decision.Status)

	err = db.View(func(txn *badger.Txn) error {
		_, e := txn.Get(PreflightSentinelKey())
		return e
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestReadOnlyProbeFailureMapsDecision(t *testing.T) {
	decision := ProbeReadOnly(RoleMeta, "", "/bad/path", func(bool) (*badger.DB, error) {
		return nil, errors.New("open failed")
	})

	require.Equal(t, DecisionOpenFailed, decision.Status)
	assert.Equal(t, RecoveryActionBlockStart, decision.Action)
	assert.Contains(t, decision.Reason, "open failed")
}
