package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerutil"
)

// LocalBackend badger meta is plaintext after the R3 static meta-encryptor
// retirement (the data-at-rest seam is b.segEnc, protecting object/segment files,
// not badger meta). The badger-value helpers now only pass meta through and keep
// the pre-XAES loud-fail boundary. The previous round-trip/wrong-domain/wrong-key
// encryption tests were removed with the encryptor seam (they tested deleted code).

func openBadgerDB(t *testing.T) *badger.DB {
	t.Helper()
	db, err := badger.Open(badgerutil.SmallOptions(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestBadgerValueRoundTripPlaintext(t *testing.T) {
	db := openBadgerDB(t)
	key := []byte("obj:bkt/key")
	plain := []byte(`{"key":"object"}`)

	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return setBadgerValue(txn, key, plain)
	}))

	var raw []byte
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			raw = append([]byte(nil), val...)
			return nil
		})
	}))
	require.Equal(t, plain, raw, "badger meta is stored as plaintext")

	require.NoError(t, db.View(func(txn *badger.Txn) error {
		got, err := getBadgerValue(txn, key)
		require.NoError(t, err)
		require.Equal(t, plain, got)
		return nil
	}))
}

// TestBadgerValueRejectsPreXAESEncryptedFormat: a value carrying the exact
// pre-XAES envelope (magic 0xAE 0xE2 + version 0x01) must loud-fail rather than
// be served as plaintext.
func TestBadgerValueRejectsPreXAESEncryptedFormat(t *testing.T) {
	db := openBadgerDB(t)
	key := []byte("obj:bkt/old-format")
	oldFormatVal := []byte{0xAE, 0xE2, 0x01, 0xDE, 0xAD, 0xBE, 0xEF}
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, oldFormatVal)
	}))

	require.NoError(t, db.View(func(txn *badger.Txn) error {
		_, err := getBadgerValue(txn, key)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported/old encrypted-value format")
		return nil
	}))
}

// TestBadgerValueReadsPlaintextWithValueMagicNonLegacyVersion: a value with the
// value magic but a non-legacy version byte (not 0x01) passes through unchanged.
func TestBadgerValueReadsPlaintextWithValueMagicNonLegacyVersion(t *testing.T) {
	db := openBadgerDB(t)
	key := []byte("obj:bkt/non-legacy-magic")
	val := []byte{0xAE, 0xE2, 0x05, 'd', 'a', 't', 'a'}
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	}))

	require.NoError(t, db.View(func(txn *badger.Txn) error {
		got, err := getBadgerValue(txn, key)
		require.NoError(t, err)
		require.Equal(t, val, got)
		return nil
	}))
}

func TestBadgerValueReadsGenuinePlaintext(t *testing.T) {
	db := openBadgerDB(t)
	key := []byte("obj:bkt/legacy")
	plain := []byte(`{"key":"legacy"}`)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, plain)
	}))

	require.NoError(t, db.View(func(txn *badger.Txn) error {
		got, err := getBadgerValue(txn, key)
		require.NoError(t, err)
		require.Equal(t, plain, got)
		return nil
	}))
}
