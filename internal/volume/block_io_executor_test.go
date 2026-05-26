package volume

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func executorForTest(store *fakeBlockStore, cache blockCache) blockIOExecutor {
	return blockIOExecutor{
		objects:   store,
		cache:     cache,
		getBlkBuf: func(size int) []byte { return make([]byte, size) },
		putBlkBuf: func([]byte) {},
	}
}

func TestExecuteWrite_DirectNewBlockPutsObject(t *testing.T) {
	store := newFakeBlockStore()
	ex := executorForTest(store, newFakeBlockCache())
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := bytes.Repeat([]byte("a"), DefaultBlockSize)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: blockKey("v", 0), OldKey: blockKey("v", 0), IsNew: true},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, actions)

	require.NoError(t, err)
	require.Equal(t, DefaultBlockSize, result.Bytes)
	require.Equal(t, int64(DefaultBlockSize), result.AllocationBytesDelta)
	require.Equal(t, p, store.objects[blockKey("v", 0)])
}

func TestExecuteWrite_DirectExistingBlockNoAllocationDelta(t *testing.T) {
	store := newFakeBlockStore()
	store.objects[blockKey("v", 0)] = make([]byte, DefaultBlockSize)
	ex := executorForTest(store, newFakeBlockCache())
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := bytes.Repeat([]byte("b"), DefaultBlockSize)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: blockKey("v", 0), OldKey: blockKey("v", 0), IsNew: false},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, actions)

	require.NoError(t, err)
	require.Equal(t, int64(0), result.AllocationBytesDelta)
}

func TestExecuteWrite_DirectInvalidatesCache(t *testing.T) {
	store := newFakeBlockStore()
	cache := newFakeBlockCache()
	ex := executorForTest(store, cache)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)
	key := blockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: key, OldKey: key, IsNew: true},
	}

	_, err := ex.executeWrite(context.Background(), "v", vol, p, actions)

	require.NoError(t, err)
	require.Contains(t, cache.invalidations, key)
}

func TestExecuteWrite_AsyncDirectCollectsCommitFn(t *testing.T) {
	store := newFakeBlockStore()
	deferred := &fakeAsyncPutter{}
	ex := executorForTest(store, newFakeBlockCache())
	ex.deferred = deferred
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)
	key := blockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: key, OldKey: key, IsNew: true, Async: true},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, actions)

	require.NoError(t, err)
	require.Len(t, result.CommitFns, 1)
}

func TestExecuteWrite_DirectWriteAtPath(t *testing.T) {
	store := newFakeBlockStore()
	store.writeAtSupport = true
	ex := executorForTest(store, newFakeBlockCache())
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := bytes.Repeat([]byte("a"), DefaultBlockSize)
	key := blockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: key, OldKey: key, IsNew: true},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, actions)

	require.NoError(t, err)
	require.Equal(t, DefaultBlockSize, result.Bytes)
	require.Equal(t, int64(DefaultBlockSize), result.AllocationBytesDelta)
	require.Equal(t, p, store.objects[key])
}

func TestExecuteWrite_DirectPartialBlock(t *testing.T) {
	store := newFakeBlockStore()
	store.objects[blockKey("v", 0)] = make([]byte, DefaultBlockSize)
	ex := executorForTest(store, newFakeBlockCache())
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := []byte("hello")
	key := blockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 10, DataStart: 0, CanWrite: 5,
			Key: key, OldKey: key, IsNew: false},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, actions)

	require.NoError(t, err)
	require.Equal(t, 5, result.Bytes)
	require.Equal(t, []byte("hello"), store.objects[key][10:15])
}

func TestExecuteWrite_AsyncDirectPartialBlock(t *testing.T) {
	store := newFakeBlockStore()
	store.objects[blockKey("v", 0)] = make([]byte, DefaultBlockSize)
	deferred := &fakeAsyncPutter{}
	ex := executorForTest(store, newFakeBlockCache())
	ex.deferred = deferred
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := []byte("hello")
	key := blockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 10, DataStart: 0, CanWrite: 5,
			Key: key, OldKey: key, IsNew: false, Async: true},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, actions)

	require.NoError(t, err)
	require.Len(t, result.CommitFns, 1)
	require.Contains(t, deferred.puts, key)
}

func TestExecuteWrite_PartialDirectNewBlockCountsAllocation(t *testing.T) {
	store := newFakeBlockStore() // block does not exist
	ex := executorForTest(store, newFakeBlockCache())
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := []byte("hello")
	key := blockKey("v", 0)
	// planner sets IsNew=false for partial blocks (skips HeadObject)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 10, DataStart: 0, CanWrite: 5,
			Key: key, OldKey: key, IsNew: false},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, actions)

	require.NoError(t, err)
	require.Equal(t, int64(DefaultBlockSize), result.AllocationBytesDelta,
		"partial write to non-existent block must count as new allocation")
}

func TestExecuteWrite_InvalidateAllNilCache(t *testing.T) {
	store := newFakeBlockStore()
	ex := executorForTest(store, nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := bytes.Repeat([]byte("a"), DefaultBlockSize)
	key := blockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: key, OldKey: key, IsNew: true},
	}

	_, err := ex.executeWrite(context.Background(), "v", vol, p, actions)
	require.NoError(t, err) // must not panic with nil cache
}

// fakeAsyncPutter is a test double for blockDeferredWriter.
type fakeAsyncPutter struct {
	puts []string
}

func (f *fakeAsyncPutter) PutObjectAsync(_ context.Context, _, key string, r io.Reader, _ string) (*storage.Object, func() error, error) {
	data, _ := io.ReadAll(r)
	f.puts = append(f.puts, key)
	return &storage.Object{Key: key, Size: int64(len(data))}, func() error { return nil }, nil
}
