package volume

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestBlockIOReadCacheHitRecordsMeterAndAvoidsStore(t *testing.T) {
	store := newFakeBlockStore()
	cache := newFakeBlockCache()
	meter := &fakeBlockReadMeter{}
	cached := bytes.Repeat([]byte("x"), DefaultBlockSize)
	copy(cached, []byte("abcd"))
	cache.puts["phys-0"] = cached

	engine := testBlockIOEngine(store, cache, meter)
	vol := &Volume{Name: "vol", Size: int64(DefaultBlockSize), BlockSize: DefaultBlockSize}
	liveMap := map[int64]string{0: "phys-0"}
	dst := make([]byte, 3)

	result, err := engine.read("vol", vol, dst, 1, liveMap)
	require.NoError(t, err)
	require.Equal(t, 3, result.Bytes)
	require.Equal(t, []byte("bcd"), dst)
	require.Empty(t, store.gets)
	require.Equal(t, []string{"phys-0"}, meter.keys)
}

func TestBlockIOWriteFullBlockSelectsDirectKeyAndReportsAllocation(t *testing.T) {
	store := newFakeBlockStore()
	cache := newFakeBlockCache()
	engine := testBlockIOEngine(store, cache, &fakeBlockReadMeter{})
	vol := &Volume{Name: "vol", Size: int64(DefaultBlockSize * 2), BlockSize: DefaultBlockSize}
	src := bytes.Repeat([]byte("a"), DefaultBlockSize)

	result, err := engine.write("vol", vol, src, 0, nil, 0, 0)
	require.NoError(t, err)
	require.Equal(t, DefaultBlockSize, result.Bytes)
	require.Equal(t, int64(DefaultBlockSize), result.AllocationBytesDelta)
	require.False(t, result.LiveMapDirty)
	require.Equal(t, src, store.objects[blockKey("vol", 0)])
	require.Equal(t, []string{blockKey("vol", 0), blockKey("vol", 0)}, result.InvalidatedKeys)
	require.Equal(t, []string{blockKey("vol", 0), blockKey("vol", 0)}, cache.invalidations)
}

func TestBlockIODiscardDeletesLiveMapBlockAndReportsFreedBytes(t *testing.T) {
	store := newFakeBlockStore()
	store.objects["cow-key"] = bytes.Repeat([]byte("z"), DefaultBlockSize)
	cache := newFakeBlockCache()
	engine := testBlockIOEngine(store, cache, &fakeBlockReadMeter{})
	vol := &Volume{Name: "vol", Size: int64(DefaultBlockSize * 3), BlockSize: DefaultBlockSize}
	liveMap := map[int64]string{1: "cow-key"}

	result, err := engine.discard("vol", vol, int64(DefaultBlockSize), int64(DefaultBlockSize), liveMap)
	require.NoError(t, err)
	require.Equal(t, -int64(DefaultBlockSize), result.AllocationBytesDelta)
	require.True(t, result.LiveMapDirty)
	require.NotContains(t, liveMap, int64(1))
	require.NotContains(t, store.objects, "cow-key")
	require.Equal(t, []string{"cow-key"}, cache.invalidations)
}

func testBlockIOEngine(store *fakeBlockStore, cache blockCache, meter blockReadMeter) blockIOEngine {
	return blockIOEngine{
		objects:   store,
		cache:     cache,
		meter:     meter,
		getBlkBuf: func(size int) []byte { return make([]byte, size) },
		putBlkBuf: func([]byte) {},
	}
}

type fakeBlockStore struct {
	objects         map[string][]byte
	gets            []string
	deletes         []string
	preferReadAt    bool
	preferWriteAt   bool
	readAtSupported bool
	writeAtSupport  bool
}

func newFakeBlockStore() *fakeBlockStore {
	return &fakeBlockStore{objects: make(map[string][]byte)}
}

func (s *fakeBlockStore) GetObject(_ context.Context, _, key string) (io.ReadCloser, *storage.Object, error) {
	s.gets = append(s.gets, key)
	data, ok := s.objects[key]
	if !ok {
		return nil, nil, fmt.Errorf("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), &storage.Object{Key: key, Size: int64(len(data))}, nil
}

func (s *fakeBlockStore) PutObject(_ context.Context, _, key string, r io.Reader, _ string) (*storage.Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	s.objects[key] = append([]byte(nil), data...)
	return &storage.Object{Key: key, Size: int64(len(data))}, nil
}

func (s *fakeBlockStore) DeleteObject(_ context.Context, _, key string) error {
	s.deletes = append(s.deletes, key)
	if _, ok := s.objects[key]; !ok {
		return fmt.Errorf("not found")
	}
	delete(s.objects, key)
	return nil
}

func (s *fakeBlockStore) HeadObject(_ context.Context, _, key string) (*storage.Object, error) {
	data, ok := s.objects[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return &storage.Object{Key: key, Size: int64(len(data))}, nil
}

func (s *fakeBlockStore) PreferReadAt(string) bool {
	return s.preferReadAt
}

func (s *fakeBlockStore) PreferWriteAt(string) bool {
	return s.preferWriteAt
}

func (s *fakeBlockStore) ReadAt(_ context.Context, _, key string, offset int64, buf []byte) (int, bool) {
	if !s.readAtSupported {
		return 0, false
	}
	data, ok := s.objects[key]
	if !ok || offset >= int64(len(data)) {
		return 0, true
	}
	return copy(buf, data[offset:]), true
}

func (s *fakeBlockStore) WriteAt(_ context.Context, _, key string, offset uint64, data []byte) (*storage.Object, bool, error) {
	if !s.writeAtSupport {
		return nil, false, nil
	}
	existing := s.objects[key]
	end := int(offset) + len(data)
	if len(existing) < end {
		grown := make([]byte, end)
		copy(grown, existing)
		existing = grown
	}
	copy(existing[offset:], data)
	s.objects[key] = existing
	return &storage.Object{Key: key, Size: int64(len(existing))}, true, nil
}

type fakeBlockCache struct {
	puts          map[string][]byte
	invalidations []string
}

func newFakeBlockCache() *fakeBlockCache {
	return &fakeBlockCache{puts: make(map[string][]byte)}
}

func (c *fakeBlockCache) Get(key string) ([]byte, bool) {
	data, ok := c.puts[key]
	return data, ok
}

func (c *fakeBlockCache) Put(key string, data []byte) {
	c.puts[key] = append([]byte(nil), data...)
}

func (c *fakeBlockCache) Invalidate(key string) {
	c.invalidations = append(c.invalidations, key)
}

type fakeBlockReadMeter struct {
	keys []string
}

func (m *fakeBlockReadMeter) RecordVolumeBlock(key string) {
	m.keys = append(m.keys, key)
}
