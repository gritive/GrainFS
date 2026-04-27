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

func TestWriteBlock_New(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))
	res, err := idx.WriteBlock("vol", 0, hash32(1), "key-a")
	require.NoError(t, err)
	assert.True(t, res.IsNew)
	assert.Equal(t, "key-a", res.Canonical)
	assert.Empty(t, res.ToDelete)
}

func TestWriteBlock_DedupHit(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))
	h := hash32(1)

	// Block 0 first
	_, err := idx.WriteBlock("vol", 0, h, "key-a")
	require.NoError(t, err)

	// Block 1 with same hash → reuses canonical from block 0
	res, err := idx.WriteBlock("vol", 1, h, "key-b")
	require.NoError(t, err)
	assert.False(t, res.IsNew)
	assert.Equal(t, "key-a", res.Canonical)
	assert.Empty(t, res.ToDelete)
}

func TestWriteBlock_Overwrite_DifferentContent(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))

	_, err := idx.WriteBlock("vol", 0, hash32(1), "key-a")
	require.NoError(t, err)

	// Overwrite block 0 with different content
	res, err := idx.WriteBlock("vol", 0, hash32(2), "key-b")
	require.NoError(t, err)
	assert.True(t, res.IsNew)
	assert.Equal(t, "key-b", res.Canonical)
	assert.Equal(t, "key-a", res.ToDelete, "old object must be scheduled for deletion")
}

func TestWriteBlock_Overwrite_SameContent(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))
	h := hash32(1)

	_, err := idx.WriteBlock("vol", 0, h, "key-a")
	require.NoError(t, err)

	// Overwrite block 0 with same content (no-op)
	res, err := idx.WriteBlock("vol", 0, h, "key-a2")
	require.NoError(t, err)
	assert.False(t, res.IsNew, "same content must not create a new object")
	assert.Equal(t, "key-a", res.Canonical, "canonical must remain the first key")
	assert.Empty(t, res.ToDelete, "no deletion when content is unchanged")
}

func TestReadBlock_NotFound(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))
	canonical, found, err := idx.ReadBlock("vol", 0)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, canonical)
}

func TestReadBlock_Found(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))
	res, err := idx.WriteBlock("vol", 0, hash32(1), "key-a")
	require.NoError(t, err)

	canonical, found, err := idx.ReadBlock("vol", 0)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, res.Canonical, canonical)
}

func TestFreeBlock_Last(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))
	_, err := idx.WriteBlock("vol", 0, hash32(1), "key-a")
	require.NoError(t, err)

	objectKey, shouldDelete, err := idx.FreeBlock("vol", 0)
	require.NoError(t, err)
	assert.Equal(t, "key-a", objectKey)
	assert.True(t, shouldDelete, "last reference: caller must delete the S3 object")
}

func TestFreeBlock_Shared(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))
	h := hash32(1)

	_, _ = idx.WriteBlock("vol", 0, h, "key-a")
	_, _ = idx.WriteBlock("vol", 1, h, "key-b") // duplicate: refcount → 2

	// Free block 0 → refcount → 1, must NOT delete
	objectKey, shouldDelete, err := idx.FreeBlock("vol", 0)
	require.NoError(t, err)
	assert.Equal(t, "key-a", objectKey)
	assert.False(t, shouldDelete, "still has references, should not delete")
}

func TestFreeBlock_NotFound(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))
	objectKey, shouldDelete, err := idx.FreeBlock("vol", 99)
	require.NoError(t, err)
	assert.Empty(t, objectKey)
	assert.False(t, shouldDelete)
}

func TestFreeBlock_CleansHashIndex(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))
	h := hash32(1)

	_, _ = idx.WriteBlock("vol", 0, h, "key-a")
	_, _, _ = idx.FreeBlock("vol", 0)

	// After last free, same hash should be treated as new block again
	res, err := idx.WriteBlock("vol", 0, h, "key-b")
	require.NoError(t, err)
	assert.True(t, res.IsNew, "hash index should be cleaned up after last free")
	assert.Equal(t, "key-b", res.Canonical)
}

func TestDeleteVolume(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))

	_, _ = idx.WriteBlock("vol", 0, hash32(1), "key-a") // refcount=1
	_, _ = idx.WriteBlock("vol", 1, hash32(2), "key-b") // refcount=1
	// Block 2 shares content with "vol2" block 0 → refcount will be 2
	_, _ = idx.WriteBlock("vol", 2, hash32(3), "key-c")
	_, _ = idx.WriteBlock("vol2", 0, hash32(3), "key-c2") // dedup hit: canonical=key-c, refcount=2

	toDelete, err := idx.DeleteVolume("vol")
	require.NoError(t, err)

	// key-a and key-b: refcount reaches 0 → must delete
	// key-c: refcount 2→1 (vol2 still references) → must NOT delete
	assert.ElementsMatch(t, []string{"key-a", "key-b"}, toDelete)

	// vol2's block 0 should still be accessible
	canonical, found, err := idx.ReadBlock("vol2", 0)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "key-c", canonical)
}

func TestConcurrentWrites(t *testing.T) {
	idx := dedup.NewBadgerIndex(openTestDB(t))

	// 20 goroutines, each writing block 0 of their own volume with the same hash.
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
			res, err := idx.WriteBlock(fmt.Sprintf("vol%d", i), 0, h, key)
			canonicals[i] = res.Canonical
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "goroutine %d failed", i)
	}

	// All goroutines must agree on the same canonical key
	first := canonicals[0]
	for i, c := range canonicals {
		assert.Equal(t, first, c, "goroutine %d returned different canonical key", i)
	}

	// Verify refcount via sequential FreeBlock: only the last should shouldDelete=true
	for i := 0; i < goroutines-1; i++ {
		_, shouldDelete, err := idx.FreeBlock(fmt.Sprintf("vol%d", i), 0)
		require.NoError(t, err)
		assert.False(t, shouldDelete, "release %d should not delete yet", i+1)
	}
	_, shouldDelete, err := idx.FreeBlock(fmt.Sprintf("vol%d", goroutines-1), 0)
	require.NoError(t, err)
	assert.True(t, shouldDelete, "last release should signal delete")
}
