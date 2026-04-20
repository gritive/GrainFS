package receipt

import (
	"encoding/json"
	"fmt"
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
	return fmt.Sprintf("rcpt-%04d", i)
}

func newReceiptWithIDAndTime(id string, ts time.Time) *HealReceipt {
	r := sampleReceipt()
	r.ReceiptID = id
	r.Timestamp = ts
	return r
}

func TestStore_List_EmptyStore(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	from := time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	got, err := s.List(from, to, 100)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestStore_List_ReturnsReceiptsInRange(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ks := newTestKeyStore(t)
	base := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)

	// 3 in range (t+10s, t+20s, t+30s), 2 out of range (t-1h, t+2h)
	inRange := []*HealReceipt{
		newReceiptWithIDAndTime("rcpt-in-1", base.Add(10*time.Second)),
		newReceiptWithIDAndTime("rcpt-in-2", base.Add(20*time.Second)),
		newReceiptWithIDAndTime("rcpt-in-3", base.Add(30*time.Second)),
	}
	outOfRange := []*HealReceipt{
		newReceiptWithIDAndTime("rcpt-before", base.Add(-time.Hour)),
		newReceiptWithIDAndTime("rcpt-after", base.Add(2*time.Hour)),
	}
	for _, r := range append(inRange, outOfRange...) {
		require.NoError(t, Sign(r, ks))
		require.NoError(t, s.Put(r))
	}
	require.NoError(t, s.Flush())

	got, err := s.List(base, base.Add(time.Minute), 100)
	require.NoError(t, err)
	require.Len(t, got, 3)

	// Verify ascending timestamp order
	require.Equal(t, "rcpt-in-1", got[0].ReceiptID)
	require.Equal(t, "rcpt-in-2", got[1].ReceiptID)
	require.Equal(t, "rcpt-in-3", got[2].ReceiptID)
}

func TestStore_List_EmptyRange(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ks := newTestKeyStore(t)
	r := newReceiptWithIDAndTime("rcpt-1", time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC))
	require.NoError(t, Sign(r, ks))
	require.NoError(t, s.Put(r))
	require.NoError(t, s.Flush())

	// from == to: half-open interval [from, to) is empty
	t0 := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	got, err := s.List(t0, t0, 100)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestStore_List_RespectsLimit(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ks := newTestKeyStore(t)
	base := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		r := newReceiptWithIDAndTime(uid(i), base.Add(time.Duration(i)*time.Second))
		require.NoError(t, Sign(r, ks))
		require.NoError(t, s.Put(r))
	}
	require.NoError(t, s.Flush())

	got, err := s.List(base, base.Add(time.Minute), 3)
	require.NoError(t, err)
	require.Len(t, got, 3)
	// Earliest 3 by timestamp
	require.Equal(t, uid(0), got[0].ReceiptID)
	require.Equal(t, uid(1), got[1].ReceiptID)
	require.Equal(t, uid(2), got[2].ReceiptID)
}

func TestStore_List_HalfOpenInterval(t *testing.T) {
	// Verify [from, to) semantics: from is inclusive, to is exclusive.
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ks := newTestKeyStore(t)
	t0 := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)

	rFrom := newReceiptWithIDAndTime("rcpt-at-from", t0)
	rTo := newReceiptWithIDAndTime("rcpt-at-to", t1)
	for _, r := range []*HealReceipt{rFrom, rTo} {
		require.NoError(t, Sign(r, ks))
		require.NoError(t, s.Put(r))
	}
	require.NoError(t, s.Flush())

	got, err := s.List(t0, t1, 100)
	require.NoError(t, err)
	require.Len(t, got, 1, "[from, to) should include from, exclude to")
	require.Equal(t, "rcpt-at-from", got[0].ReceiptID)
}

func TestStore_LookupReceiptJSON_Found(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ks := newTestKeyStore(t)
	r := newReceiptWithID("rcpt-json-1")
	require.NoError(t, Sign(r, ks))
	require.NoError(t, s.Put(r))
	require.NoError(t, s.Flush())

	raw, ok := s.LookupReceiptJSON("rcpt-json-1")
	require.True(t, ok)
	require.NotEmpty(t, raw)
	// The returned bytes must decode back to the same receipt.
	var decoded HealReceipt
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, "rcpt-json-1", decoded.ReceiptID)
}

func TestStore_LookupReceiptJSON_NotFound(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	raw, ok := s.LookupReceiptJSON("nonexistent")
	require.False(t, ok)
	require.Nil(t, raw)
}

func TestStore_RecentReceiptIDs_NewestFirst(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ks := newTestKeyStore(t)
	base := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	// Write 5 receipts with increasing timestamps.
	for i := 0; i < 5; i++ {
		r := newReceiptWithIDAndTime(uid(i), base.Add(time.Duration(i)*time.Second))
		require.NoError(t, Sign(r, ks))
		require.NoError(t, s.Put(r))
	}
	require.NoError(t, s.Flush())

	got := s.RecentReceiptIDs(3)
	require.Equal(t, []string{uid(4), uid(3), uid(2)}, got, "newest first")
}

func TestStore_RecentReceiptIDs_EmptyStore(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	require.Empty(t, s.RecentReceiptIDs(50))
}

func TestStore_RecentReceiptIDs_ZeroMax(t *testing.T) {
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	require.Nil(t, s.RecentReceiptIDs(0))
}

func TestStore_Put_WritesTimeIndex(t *testing.T) {
	// Verify secondary index is written alongside primary.
	db := openTestDB(t)
	s, err := NewStore(db, StoreOptions{Retention: time.Hour, FlushThreshold: 1, FlushInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	ks := newTestKeyStore(t)
	r := newReceiptWithIDAndTime("rcpt-idx-1", time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC))
	require.NoError(t, Sign(r, ks))
	require.NoError(t, s.Put(r))
	require.NoError(t, s.Flush())

	// Scan for time index keys directly.
	var tsKeyCount int
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek([]byte(tsIndexPrefix)); it.ValidForPrefix([]byte(tsIndexPrefix)); it.Next() {
			tsKeyCount++
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, tsKeyCount, "exactly one ts:* key should exist after one Put")
}
