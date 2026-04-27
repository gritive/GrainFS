package dedup_test

import (
	"fmt"
	"sync"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/volume/dedup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func hash32(seed byte) [32]byte {
	var h [32]byte
	h[0] = seed
	return h
}

func TestLookupOrRegister_NewBlock(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))

	canonical, isNew, err := idx.LookupOrRegister(hash32(1), "key-a")
	require.NoError(t, err)
	assert.True(t, isNew)
	assert.Equal(t, "key-a", canonical)
}

func TestLookupOrRegister_Duplicate(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))

	_, _, err := idx.LookupOrRegister(hash32(1), "key-a")
	require.NoError(t, err)

	canonical, isNew, err := idx.LookupOrRegister(hash32(1), "key-b")
	require.NoError(t, err)
	assert.False(t, isNew)
	assert.Equal(t, "key-a", canonical)
}

func TestRelease_Shared(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))

	_, _, _ = idx.LookupOrRegister(hash32(1), "key-a")
	_, _, _ = idx.LookupOrRegister(hash32(1), "key-b") // duplicate: refcount → 2

	shouldDelete, err := idx.Release("key-a")
	require.NoError(t, err)
	assert.False(t, shouldDelete, "still has references, should not delete")
}

func TestRelease_Last(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))

	_, _, _ = idx.LookupOrRegister(hash32(1), "key-a")

	shouldDelete, err := idx.Release("key-a")
	require.NoError(t, err)
	assert.True(t, shouldDelete, "last reference, should delete S3 object")
}

func TestReleaseNotFound(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))

	// objectKey without vd:r entry — not a dedup object, delegate to existing GC
	shouldDelete, err := idx.Release("unknown-key")
	require.NoError(t, err)
	assert.False(t, shouldDelete, "not tracked by dedup — caller uses existing GC path")
}

func TestRelease_DeleteCleansHashIndex(t *testing.T) {
	db := openTestDB(t)
	idx := dedup.NewBadgerIndex(db)

	h := hash32(1)
	_, _, _ = idx.LookupOrRegister(h, "key-a")

	shouldDelete, err := idx.Release("key-a")
	require.NoError(t, err)
	assert.True(t, shouldDelete)

	// After last release, same hash should be treated as new block again
	canonical, isNew, err := idx.LookupOrRegister(h, "key-b")
	require.NoError(t, err)
	assert.True(t, isNew, "hash index should be cleaned up, so this is a new block")
	assert.Equal(t, "key-b", canonical)
}

func TestConcurrentWrites(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))

	// 20 goroutines is sufficient to verify concurrency safety without
	// saturating BadgerDB MVCC retry budget (3 retries × 100 concurrent = unrealistic load)
	const goroutines = 20
	h := hash32(42)

	var wg sync.WaitGroup
	canonicals := make([]string, goroutines)
	errs := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			canonical, _, err := idx.LookupOrRegister(h, key)
			canonicals[i] = canonical
			errs[i] = err
		}(i)
	}
	wg.Wait()

	// All goroutines must succeed
	for i, err := range errs {
		require.NoError(t, err, "goroutine %d failed", i)
	}

	// All must agree on the same canonical key
	first := canonicals[0]
	for i, c := range canonicals {
		assert.Equal(t, first, c, "goroutine %d returned different canonical key", i)
	}

	// Verify refcount via repeated Release: only the last should return shouldDelete=true
	for i := 0; i < goroutines-1; i++ {
		shouldDelete, err := idx.Release(first)
		require.NoError(t, err)
		assert.False(t, shouldDelete, "release %d should not delete yet", i+1)
	}
	shouldDelete, err := idx.Release(first)
	require.NoError(t, err)
	assert.True(t, shouldDelete, "last release should signal delete")
}
