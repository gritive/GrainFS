package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestEncryptedBadgerValueRoundTripAndNoPlaintext(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	defer db.Close()

	enc := testEncryptor(t)
	key := []byte("obj:bkt/key")
	plain := []byte(`{"key":"secret-object"}`)

	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return setBadgerValue(txn, enc, "badger:meta:object", key, plain)
	}))

	var raw []byte
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			raw = append([]byte(nil), val...)
			return nil
		})
	}))
	require.NotContains(t, string(raw), "secret-object")

	require.NoError(t, db.View(func(txn *badger.Txn) error {
		got, err := getBadgerValue(txn, enc, "badger:meta:object", key)
		require.NoError(t, err)
		require.Equal(t, plain, got)
		return nil
	}))
}

func TestEncryptedBadgerValueRejectsWrongDomain(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	defer db.Close()

	enc := testEncryptor(t)
	key := []byte("mpu:id")
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return setBadgerValue(txn, enc, "badger:multipart", key, []byte("secret"))
	}))
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		_, err := getBadgerValue(txn, enc, "badger:object", key)
		require.Error(t, err)
		return nil
	}))
}

func TestEncryptedBadgerValueRejectsWrongKey(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	defer db.Close()

	enc := testEncryptor(t)
	keyA := []byte("obj:bkt/a")
	keyB := []byte("obj:bkt/b")
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		if err := setBadgerValue(txn, enc, "badger:meta:object", keyA, []byte("secret-a")); err != nil {
			return err
		}
		return setBadgerValue(txn, enc, "badger:meta:object", keyB, []byte("secret-b"))
	}))

	var rawA []byte
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyA)
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			rawA = append([]byte(nil), val...)
			return nil
		})
	}))
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyB, rawA)
	}))
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		_, err := getBadgerValue(txn, enc, "badger:meta:object", keyB)
		require.Error(t, err)
		return nil
	}))
}

func TestEncryptedBadgerValueReadsLegacyPlaintext(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	defer db.Close()

	enc := testEncryptor(t)
	key := []byte("obj:bkt/legacy")
	plain := []byte(`{"key":"legacy"}`)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, plain)
	}))

	require.NoError(t, db.View(func(txn *badger.Txn) error {
		got, err := getBadgerValue(txn, enc, "badger:meta:object", key)
		require.NoError(t, err)
		require.Equal(t, plain, got)
		return nil
	}))
}
