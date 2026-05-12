package volume

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume/dedup"
	"github.com/stretchr/testify/require"
)

func executorForTest(store *fakeBlockStore, cache blockCache, dedup blockDedupIndex) blockIOExecutor {
	return blockIOExecutor{
		objects:   store,
		dedup:     dedup,
		cache:     cache,
		getBlkBuf: func(size int) []byte { return make([]byte, size) },
		putBlkBuf: func([]byte) {},
	}
}

func TestExecuteWrite_DirectNewBlockPutsObject(t *testing.T) {
	store := newFakeBlockStore()
	ex := executorForTest(store, newFakeBlockCache(), nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := bytes.Repeat([]byte("a"), DefaultBlockSize)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: blockKey("v", 0), OldKey: blockKey("v", 0), IsNew: true},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, 0, nil, actions)

	require.NoError(t, err)
	require.Equal(t, DefaultBlockSize, result.Bytes)
	require.Equal(t, int64(DefaultBlockSize), result.AllocationBytesDelta)
	require.Equal(t, p, store.objects[blockKey("v", 0)])
}

func TestExecuteWrite_DirectExistingBlockNoAllocationDelta(t *testing.T) {
	store := newFakeBlockStore()
	store.objects[blockKey("v", 0)] = make([]byte, DefaultBlockSize)
	ex := executorForTest(store, newFakeBlockCache(), nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := bytes.Repeat([]byte("b"), DefaultBlockSize)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: blockKey("v", 0), OldKey: blockKey("v", 0), IsNew: false},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, 0, nil, actions)

	require.NoError(t, err)
	require.Equal(t, int64(0), result.AllocationBytesDelta)
}

func TestExecuteWrite_DirectInvalidatesCache(t *testing.T) {
	store := newFakeBlockStore()
	cache := newFakeBlockCache()
	ex := executorForTest(store, cache, nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)
	key := blockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: key, OldKey: key, IsNew: true},
	}

	_, err := ex.executeWrite(context.Background(), "v", vol, p, 0, nil, actions)

	require.NoError(t, err)
	require.Contains(t, cache.invalidations, key)
}

func TestExecuteWrite_AsyncDirectCollectsCommitFn(t *testing.T) {
	store := newFakeBlockStore()
	deferred := &fakeAsyncPutter{}
	ex := executorForTest(store, newFakeBlockCache(), nil)
	ex.deferred = deferred
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := make([]byte, DefaultBlockSize)
	key := blockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: key, OldKey: key, IsNew: true, Async: true},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, 0, nil, actions)

	require.NoError(t, err)
	require.Len(t, result.CommitFns, 1)
}

func TestExecuteWrite_DedupNewBlockPutsObject(t *testing.T) {
	store := newFakeBlockStore()
	di := &fakeDedupIndex{blocks: map[int64]string{}}
	ex := executorForTest(store, newFakeBlockCache(), di)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := bytes.Repeat([]byte("c"), DefaultBlockSize)
	proposedKey := "proposed-key-0"
	actions := []BlockAction{
		{Kind: ActionDedup, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: proposedKey, OldKey: "", IsNew: true},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, 0, nil, actions)

	require.NoError(t, err)
	require.Equal(t, int64(DefaultBlockSize), result.AllocationBytesDelta)
	require.Contains(t, store.objects, proposedKey)
}

func TestExecuteWrite_DedupDuplicateSkipsPutObject(t *testing.T) {
	store := newFakeBlockStore()
	existingKey := "existing-canonical"
	store.objects[existingKey] = bytes.Repeat([]byte("x"), DefaultBlockSize)
	di := &fakeDedupIndex{blocks: map[int64]string{0: existingKey}}
	ex := executorForTest(store, newFakeBlockCache(), di)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := bytes.Repeat([]byte("x"), DefaultBlockSize) // same data → dedup hit
	proposedKey := "proposed-key-0"
	actions := []BlockAction{
		{Kind: ActionDedup, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: proposedKey, OldKey: existingKey, IsNew: false},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, 0, nil, actions)

	require.NoError(t, err)
	// dedup hit: no new allocation
	require.Equal(t, int64(0), result.AllocationBytesDelta)
}

func TestExecuteWrite_CowWritesToCowKey(t *testing.T) {
	store := newFakeBlockStore()
	oldKey := physicalKey("v", 0, nil)
	store.objects[oldKey] = bytes.Repeat([]byte("z"), DefaultBlockSize)
	ex := executorForTest(store, newFakeBlockCache(), nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	liveMap := map[int64]string{0: oldKey}
	p := bytes.Repeat([]byte("w"), DefaultBlockSize)
	cowKey := cowBlockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionCow, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: cowKey, OldKey: oldKey, IsNew: false},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, 0, liveMap, actions)

	require.NoError(t, err)
	require.True(t, result.LiveMapDirty)
	require.Equal(t, cowKey, liveMap[0])
	require.Equal(t, p, store.objects[cowKey])
}

func TestExecuteWrite_DirectWriteAtPath(t *testing.T) {
	store := newFakeBlockStore()
	store.writeAtSupport = true
	ex := executorForTest(store, newFakeBlockCache(), nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := bytes.Repeat([]byte("a"), DefaultBlockSize)
	key := blockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: key, OldKey: key, IsNew: true},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, 0, nil, actions)

	require.NoError(t, err)
	require.Equal(t, DefaultBlockSize, result.Bytes)
	require.Equal(t, int64(DefaultBlockSize), result.AllocationBytesDelta)
	require.Equal(t, p, store.objects[key])
}

func TestExecuteWrite_DirectPartialBlock(t *testing.T) {
	store := newFakeBlockStore()
	store.objects[blockKey("v", 0)] = make([]byte, DefaultBlockSize)
	ex := executorForTest(store, newFakeBlockCache(), nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := []byte("hello")
	key := blockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 10, DataStart: 0, CanWrite: 5,
			Key: key, OldKey: key, IsNew: false},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, 10, nil, actions)

	require.NoError(t, err)
	require.Equal(t, 5, result.Bytes)
	require.Equal(t, []byte("hello"), store.objects[key][10:15])
}

func TestExecuteWrite_AsyncDirectPartialBlock(t *testing.T) {
	store := newFakeBlockStore()
	store.objects[blockKey("v", 0)] = make([]byte, DefaultBlockSize)
	deferred := &fakeAsyncPutter{}
	ex := executorForTest(store, newFakeBlockCache(), nil)
	ex.deferred = deferred
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := []byte("hello")
	key := blockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 10, DataStart: 0, CanWrite: 5,
			Key: key, OldKey: key, IsNew: false, Async: true},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, 10, nil, actions)

	require.NoError(t, err)
	require.Len(t, result.CommitFns, 1)
	require.Contains(t, deferred.puts, key)
}

func TestExecuteWrite_DedupToDeleteDecrementsBlocks(t *testing.T) {
	store := newFakeBlockStore()
	toDeleteKey := "to-delete-key"
	store.objects[toDeleteKey] = make([]byte, DefaultBlockSize)
	existingKey := "canonical-key"
	store.objects[existingKey] = bytes.Repeat([]byte("x"), DefaultBlockSize)
	di := &fakeDedupIndexWithDelete{
		blocks:   map[int64]string{0: existingKey},
		toDelete: toDeleteKey,
	}
	ex := executorForTest(store, newFakeBlockCache(), di)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := bytes.Repeat([]byte("x"), DefaultBlockSize)
	actions := []BlockAction{
		{Kind: ActionDedup, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: "proposed-key", OldKey: existingKey, IsNew: false},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, 0, nil, actions)

	require.NoError(t, err)
	require.Equal(t, -int64(DefaultBlockSize), result.AllocationBytesDelta)
	require.NotContains(t, store.objects, toDeleteKey)
}

func TestExecuteWrite_CowNewBlockSkipsRead(t *testing.T) {
	store := newFakeBlockStore()
	ex := executorForTest(store, newFakeBlockCache(), nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	liveMap := map[int64]string{}
	p := bytes.Repeat([]byte("w"), DefaultBlockSize)
	cowKey := cowBlockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionCow, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: cowKey, OldKey: "", IsNew: true},
	}

	result, err := ex.executeWrite(context.Background(), "v", vol, p, 0, liveMap, actions)

	require.NoError(t, err)
	require.Equal(t, int64(DefaultBlockSize), result.AllocationBytesDelta)
	require.Equal(t, p, store.objects[cowKey])
	require.Empty(t, store.gets) // no GetObject called for new CoW block
}

func TestExecuteWrite_InvalidateAllNilCache(t *testing.T) {
	store := newFakeBlockStore()
	ex := executorForTest(store, nil, nil)
	vol := &Volume{Name: "v", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	p := bytes.Repeat([]byte("a"), DefaultBlockSize)
	key := blockKey("v", 0)
	actions := []BlockAction{
		{Kind: ActionDirect, BlkNum: 0, BlkOff: 0, DataStart: 0, CanWrite: DefaultBlockSize,
			Key: key, OldKey: key, IsNew: true},
	}

	_, err := ex.executeWrite(context.Background(), "v", vol, p, 0, nil, actions)
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

// fakeDedupIndexWithDelete simulates dedup WriteBlock returning a ToDelete key.
type fakeDedupIndexWithDelete struct {
	blocks   map[int64]string
	toDelete string
}

func (d *fakeDedupIndexWithDelete) ReadBlock(_ string, blkNum int64) (string, bool, error) {
	k, ok := d.blocks[blkNum]
	return k, ok, nil
}

func (d *fakeDedupIndexWithDelete) WriteBlock(_ string, blkNum int64, _ [32]byte, _ string) (dedup.WriteResult, error) {
	canonical := d.blocks[blkNum]
	return dedup.WriteResult{Canonical: canonical, IsNew: false, ToDelete: d.toDelete}, nil
}

func (d *fakeDedupIndexWithDelete) FreeBlock(_ string, blkNum int64) (string, bool, error) {
	key, ok := d.blocks[blkNum]
	if ok {
		delete(d.blocks, blkNum)
	}
	return key, ok, nil
}
