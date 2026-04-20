package receipt

import (
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newReceiptWithID(id string) *HealReceipt {
	r := sampleReceipt()
	r.ReceiptID = id
	return r
}

func TestStore_PutGet_RoundTrip(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ks := newTestKeyStore(t)
	r := newReceiptWithID("rcpt-0001")
	require.NoError(t, Sign(r, ks))

	require.NoError(t, s.Put(r))
	require.NoError(t, s.Flush())

	got, err := s.Get("rcpt-0001")
	require.NoError(t, err)
	require.Equal(t, "rcpt-0001", got.ReceiptID)
	require.Equal(t, r.Signature, got.Signature)
	require.Equal(t, r.CanonicalPayload, got.CanonicalPayload)
	require.NoError(t, Verify(got, ks))
}

func TestStore_Get_NotFound(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	_, err = s.Get("nonexistent")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestStore_Put_RejectsUnsigned(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	r := sampleReceipt() // not signed
	err = s.Put(r)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnsigned)
}

func TestStore_BatchFlush_ByThreshold(t *testing.T) {
	db := openTestDB(t)
	// High interval, low threshold → threshold triggers first.
	s, err := NewStore(db, StoreOptions{
		Retention:      time.Hour,
		FlushThreshold: 3,
		FlushInterval:  time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ks := newTestKeyStore(t)
	for i := 0; i < 3; i++ {
		r := newReceiptWithID(uid(i))
		require.NoError(t, Sign(r, ks))
		require.NoError(t, s.Put(r))
	}

	// Poll briefly — threshold flush happens async on the background loop.
	require.Eventually(t, func() bool {
		_, err := s.Get(uid(2))
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "threshold flush should persist receipts")
}

func TestStore_BatchFlush_ByInterval(t *testing.T) {
	db := openTestDB(t)
	// High threshold, low interval → interval triggers first.
	s, err := NewStore(db, StoreOptions{
		Retention:      time.Hour,
		FlushThreshold: 1000,
		FlushInterval:  20 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ks := newTestKeyStore(t)
	r := newReceiptWithID("rcpt-timer")
	require.NoError(t, Sign(r, ks))
	require.NoError(t, s.Put(r))

	require.Eventually(t, func() bool {
		_, err := s.Get("rcpt-timer")
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "interval flush should persist receipts")
}

func TestStore_Close_FlushesPending(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{
		Retention:      time.Hour,
		FlushThreshold: 1000,
		FlushInterval:  time.Hour,
	})
	require.NoError(t, err)

	ks := newTestKeyStore(t)
	r := newReceiptWithID("rcpt-close")
	require.NoError(t, Sign(r, ks))
	require.NoError(t, s.Put(r))

	// Close must drain the buffer before returning.
	require.NoError(t, s.Close())

	// Reopen through a bare Get bypassing the Store machinery.
	// Since the DB is shared, open a new read-only Store to query.
	s2, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	got, err := s2.Get("rcpt-close")
	require.NoError(t, err)
	require.Equal(t, "rcpt-close", got.ReceiptID)
}

func TestStore_BurstPersistsAll(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{
		Retention:      time.Hour,
		FlushThreshold: 100,
		FlushInterval:  50 * time.Millisecond,
	})
	require.NoError(t, err)

	ks := newTestKeyStore(t)
	const n = 1000
	for i := 0; i < n; i++ {
		r := newReceiptWithID(uid(i))
		require.NoError(t, Sign(r, ks))
		require.NoError(t, s.Put(r))
	}
	require.NoError(t, s.Close()) // drains pending

	s2, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	for i := 0; i < n; i++ {
		got, err := s2.Get(uid(i))
		require.NoError(t, err, "receipt %s missing after burst", uid(i))
		require.Equal(t, uid(i), got.ReceiptID)
	}
}

func uid(i int) string {
	return "rcpt-" + itoa4(i)
}

func itoa4(i int) string {
	const digits = "0123456789"
	buf := []byte{'0', '0', '0', '0'}
	for pos := 3; pos >= 0 && i > 0; pos-- {
		buf[pos] = digits[i%10]
		i /= 10
	}
	return string(buf)
}
