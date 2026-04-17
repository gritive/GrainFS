package scrubber_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// ----------------------------------------------------------------------------
// mock Scrubbable backend
// ----------------------------------------------------------------------------

type mockBackend struct {
	shards   map[string][]byte  // path → shard data
	shardErr map[string]error   // path → forced read error
	records  map[string][]scrubber.ObjectRecord // bucket → records
}

func newMockBackend() *mockBackend {
	return &mockBackend{
		shards:  make(map[string][]byte),
		shardErr: make(map[string]error),
		records: make(map[string][]scrubber.ObjectRecord),
	}
}

func (m *mockBackend) ListBuckets() ([]string, error) {
	buckets := make([]string, 0, len(m.records))
	for b := range m.records {
		buckets = append(buckets, b)
	}
	return buckets, nil
}

func (m *mockBackend) ObjectExists(bucket, key string) (bool, error) {
	for _, recs := range m.records {
		for _, r := range recs {
			if r.Bucket == bucket && r.Key == key {
				return true, nil
			}
		}
	}
	prefix := fmt.Sprintf("%s/%s/", bucket, key)
	for path := range m.shards {
		if len(path) > len(prefix) && path[:len(prefix)] == prefix {
			return true, nil
		}
	}
	return false, nil
}

func (m *mockBackend) ScanObjects(bucket string) (<-chan scrubber.ObjectRecord, error) {
	recs := m.records[bucket]
	ch := make(chan scrubber.ObjectRecord, len(recs))
	for _, r := range recs {
		ch <- r
	}
	close(ch)
	return ch, nil
}

func (m *mockBackend) ShardPaths(bucket, key string, total int) []string {
	paths := make([]string, total)
	for i := range paths {
		paths[i] = fmt.Sprintf("%s/%s/%d", bucket, key, i)
	}
	return paths
}

func (m *mockBackend) ReadShard(bucket, key, path string) ([]byte, error) {
	if err, ok := m.shardErr[path]; ok {
		return nil, err
	}
	data, ok := m.shards[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	return data, nil
}

func (m *mockBackend) WriteShard(bucket, key, path string, data []byte) error {
	m.shards[path] = data
	return nil
}

// storeShards stores pre-encoded RS shards in the mock backend.
func (m *mockBackend) storeShards(bucket, key string, shards [][]byte) {
	for i, s := range shards {
		m.shards[fmt.Sprintf("%s/%s/%d", bucket, key, i)] = s
	}
}

// encodeShards creates real RS-encoded shards from data.
func encodeShards(t *testing.T, data []byte, dataShards, parityShards int) [][]byte {
	t.Helper()
	enc, err := reedsolomon.New(dataShards, parityShards)
	require.NoError(t, err)
	shards, err := enc.Split(data)
	require.NoError(t, err)
	require.NoError(t, enc.Encode(shards))
	return shards
}

// ----------------------------------------------------------------------------
// ShardVerifier tests
// ----------------------------------------------------------------------------

func TestShardVerifier_Healthy(t *testing.T) {
	m := newMockBackend()
	m.storeShards("b", "k", [][]byte{
		[]byte("d0"), []byte("d1"), []byte("d2"), []byte("d3"),
		[]byte("p0"), []byte("p1"),
	})

	v := scrubber.NewShardVerifier(m)
	status := v.Verify(scrubber.ObjectRecord{
		Bucket: "b", Key: "k", DataShards: 4, ParityShards: 2,
	})
	assert.True(t, status.IsHealthy())
	assert.Empty(t, status.Missing)
	assert.Empty(t, status.Corrupt)
}

func TestShardVerifier_MissingShard(t *testing.T) {
	m := newMockBackend()
	m.storeShards("b", "k", [][]byte{
		[]byte("d0"), []byte("d1"), nil, // index 2 absent
		[]byte("d3"), []byte("p0"), []byte("p1"),
	})
	// Don't store index 2
	delete(m.shards, "b/k/2")

	v := scrubber.NewShardVerifier(m)
	status := v.Verify(scrubber.ObjectRecord{
		Bucket: "b", Key: "k", DataShards: 4, ParityShards: 2,
	})
	assert.False(t, status.IsHealthy())
	assert.Contains(t, status.Missing, 2)
	assert.Empty(t, status.Corrupt)
}

func TestShardVerifier_CorruptShard(t *testing.T) {
	m := newMockBackend()
	m.storeShards("b", "k", [][]byte{
		[]byte("d0"), []byte("d1"), []byte("d2"), []byte("d3"),
		[]byte("p0"), []byte("p1"),
	})
	// Force a CRC error on shard 3
	m.shardErr["b/k/3"] = fmt.Errorf("CRC mismatch")

	// Use zero retry delay for speed
	v := scrubber.NewShardVerifier(m, scrubber.WithVerifyRetryDelay(0))
	status := v.Verify(scrubber.ObjectRecord{
		Bucket: "b", Key: "k", DataShards: 4, ParityShards: 2,
	})
	assert.False(t, status.IsHealthy())
	assert.Empty(t, status.Missing)
	assert.Contains(t, status.Corrupt, 3)
}

// ----------------------------------------------------------------------------
// RepairEngine tests
// ----------------------------------------------------------------------------

func TestRepairEngine_Reconstruct(t *testing.T) {
	const (dataShards, parityShards = 4, 2)
	data := []byte("hello world this is test data for ec scrubber repair engine")
	// Pad to multiple of dataShards
	pad := (dataShards - len(data)%dataShards) % dataShards
	padded := append(data, make([]byte, pad)...)

	m := newMockBackend()
	shards := encodeShards(t, padded, dataShards, parityShards)
	m.storeShards("b", "k", shards)

	// Remove shard 1 to simulate data loss
	origShard1 := shards[1]
	delete(m.shards, "b/k/1")

	r := scrubber.NewRepairEngine(m)
	err := r.Repair(
		scrubber.ObjectRecord{Bucket: "b", Key: "k", DataShards: dataShards, ParityShards: parityShards},
		scrubber.ShardStatus{Bucket: "b", Key: "k", Missing: []int{1}},
	)
	require.NoError(t, err)

	assert.Equal(t, origShard1, m.shards["b/k/1"], "repaired shard must match original")
}

func TestRepairEngine_TooManyLost(t *testing.T) {
	const (dataShards, parityShards = 4, 2)
	data := []byte("test data for ec scrubber repair engine too many lost shards")
	pad := (dataShards - len(data)%dataShards) % dataShards
	padded := append(data, make([]byte, pad)...)

	m := newMockBackend()
	shards := encodeShards(t, padded, dataShards, parityShards)
	m.storeShards("b", "k", shards)

	// Lose 3 shards — more than parityShards (2)
	delete(m.shards, "b/k/0")
	delete(m.shards, "b/k/1")
	delete(m.shards, "b/k/2")

	r := scrubber.NewRepairEngine(m)
	err := r.Repair(
		scrubber.ObjectRecord{Bucket: "b", Key: "k", DataShards: dataShards, ParityShards: parityShards},
		scrubber.ShardStatus{Bucket: "b", Key: "k", Missing: []int{0, 1, 2}},
	)
	assert.Error(t, err, "should fail when too many shards lost")
}

// ----------------------------------------------------------------------------
// BackgroundScrubber tests
// ----------------------------------------------------------------------------

func TestBackgroundScrubber_RunOnce(t *testing.T) {
	const (dataShards, parityShards = 4, 2)
	data := []byte("background scrubber runonce test data for verification and repair")
	pad := (dataShards - len(data)%dataShards) % dataShards
	padded := append(data, make([]byte, pad)...)

	m := newMockBackend()
	shards := encodeShards(t, padded, dataShards, parityShards)
	m.storeShards("b", "k", shards)

	rec := scrubber.ObjectRecord{Bucket: "b", Key: "k", DataShards: dataShards, ParityShards: parityShards}
	m.records["b"] = []scrubber.ObjectRecord{rec}

	// Delete shard 0 to trigger repair
	origShard0 := make([]byte, len(shards[0]))
	copy(origShard0, shards[0])
	delete(m.shards, "b/k/0")

	s := scrubber.New(m, time.Hour) // long interval, won't auto-fire
	s.RunOnce(context.Background())

	assert.Equal(t, origShard0, m.shards["b/k/0"], "scrubber must repair the missing shard")

	stats := s.Stats()
	assert.EqualValues(t, 1, stats.ObjectsChecked)
	assert.EqualValues(t, 1, stats.ShardErrors)
	assert.EqualValues(t, 1, stats.Repaired)
	assert.EqualValues(t, 0, stats.Unrepairable)
}

func TestBackgroundScrubber_StopsOnContextCancel(t *testing.T) {
	m := newMockBackend()
	s := scrubber.New(m, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()

	// Should not panic or deadlock after cancel
	time.Sleep(20 * time.Millisecond)
}
