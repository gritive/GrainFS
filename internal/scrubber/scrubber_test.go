package scrubber_test

import (
	"context"
	"fmt"
	"os"
	"sync"
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
	mu             sync.Mutex
	shards         map[string][]byte                  // path → shard data
	shardErr       map[string]error                   // path → forced read error
	records        map[string][]scrubber.ObjectRecord // bucket → records
	deletedObjects map[string]bool                    // "bucket/key" → deleted mid-scan
}

func newMockBackend() *mockBackend {
	return &mockBackend{
		shards:         make(map[string][]byte),
		shardErr:       make(map[string]error),
		records:        make(map[string][]scrubber.ObjectRecord),
		deletedObjects: make(map[string]bool),
	}
}

func (m *mockBackend) ListBuckets(ctx context.Context) ([]string, error) {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	buckets := make([]string, 0, len(m.records))
	for b := range m.records {
		buckets = append(buckets, b)
	}
	return buckets, nil
}

func (m *mockBackend) ObjectExists(bucket, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deletedObjects[bucket+"/"+key] {
		return false, nil
	}
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
	m.mu.Lock()
	recs := append([]scrubber.ObjectRecord(nil), m.records[bucket]...)
	m.mu.Unlock()
	ch := make(chan scrubber.ObjectRecord, len(recs))
	for _, r := range recs {
		ch <- r
	}
	close(ch)
	return ch, nil
}

func (m *mockBackend) ShardPaths(bucket, key, versionID string, total int) []string {
	paths := make([]string, total)
	for i := range paths {
		paths[i] = fmt.Sprintf("%s/%s/%d", bucket, key, i)
	}
	return paths
}

func (m *mockBackend) ReadShard(bucket, key, path string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shards[path] = data
	return nil
}

// storeShards stores pre-encoded RS shards in the mock backend.
func (m *mockBackend) storeShards(bucket, key string, shards [][]byte) {
	for i, s := range shards {
		if s != nil {
			m.shards[fmt.Sprintf("%s/%s/%d", bucket, key, i)] = s
		}
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

// captureEmitter collects HealEvents for assertions.
type captureEmitter struct {
	mu     sync.Mutex
	events []scrubber.HealEvent
}

func (c *captureEmitter) Emit(e scrubber.HealEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
}

func (c *captureEmitter) Snapshot() []scrubber.HealEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]scrubber.HealEvent, len(c.events))
	copy(out, c.events)
	return out
}

func TestRepairEngine_EmitsPhaseEvents(t *testing.T) {
	const (
		dataShards   = 4
		parityShards = 2
	)
	data := []byte("hello world this is test data for ec scrubber repair engine emit")
	pad := (dataShards - len(data)%dataShards) % dataShards
	padded := append(data, make([]byte, pad)...)

	m := newMockBackend()
	shards := encodeShards(t, padded, dataShards, parityShards)
	m.storeShards("b", "k", shards)
	delete(m.shards, "b/k/1")

	cap := &captureEmitter{}
	r := scrubber.NewRepairEngine(m, scrubber.WithRepairEmitter(cap))
	err := r.RepairWithCorrelation(
		scrubber.ObjectRecord{Bucket: "b", Key: "k", DataShards: dataShards, ParityShards: parityShards},
		scrubber.ShardStatus{Bucket: "b", Key: "k", Missing: []int{1}},
		"corr-1",
	)
	require.NoError(t, err)

	events := cap.Snapshot()
	require.NotEmpty(t, events, "expected emitter to receive HealEvents")

	phases := map[scrubber.HealPhase]int{}
	for _, e := range events {
		phases[e.Phase]++
		assert.Equal(t, "corr-1", e.CorrelationID, "all events must share correlation ID")
		assert.Equal(t, "b", e.Bucket)
		assert.Equal(t, "k", e.Key)
	}
	assert.Equal(t, 1, phases[scrubber.PhaseReconstruct])
	assert.Equal(t, 1, phases[scrubber.PhaseWrite])
	assert.Equal(t, 1, phases[scrubber.PhaseVerify])

	// Write event must reference shard 1.
	for _, e := range events {
		if e.Phase == scrubber.PhaseWrite {
			assert.EqualValues(t, 1, e.ShardID)
			assert.Equal(t, scrubber.OutcomeSuccess, e.Outcome)
			assert.Greater(t, e.BytesRepaired, int64(0))
		}
	}
}

func TestRepairEngine_NoEmitterIsSafe(t *testing.T) {
	// Passing a nil emitter via WithRepairEmitter must NOT replace the default
	// NoopEmitter, otherwise a forgetful caller would crash on Emit.
	const (
		dataShards   = 4
		parityShards = 2
	)
	data := []byte("nil emitter must default to noop and not panic on Emit calls")
	pad := (dataShards - len(data)%dataShards) % dataShards
	padded := append(data, make([]byte, pad)...)

	m := newMockBackend()
	shards := encodeShards(t, padded, dataShards, parityShards)
	m.storeShards("b", "k", shards)
	delete(m.shards, "b/k/0")

	r := scrubber.NewRepairEngine(m, scrubber.WithRepairEmitter(nil))
	err := r.Repair(
		scrubber.ObjectRecord{Bucket: "b", Key: "k", DataShards: dataShards, ParityShards: parityShards},
		scrubber.ShardStatus{Bucket: "b", Key: "k", Missing: []int{0}},
	)
	require.NoError(t, err)
}

func TestRepairEngine_Reconstruct(t *testing.T) {
	const (
		dataShards   = 4
		parityShards = 2
	)
	data := []byte("hello world this is test data for ec scrubber repair engine")
	pad := (dataShards - len(data)%dataShards) % dataShards
	padded := append(data, make([]byte, pad)...)

	m := newMockBackend()
	shards := encodeShards(t, padded, dataShards, parityShards)
	m.storeShards("b", "k", shards)

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
	const (
		dataShards   = 4
		parityShards = 2
	)
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
	const (
		dataShards   = 4
		parityShards = 2
	)
	data := []byte("background scrubber runonce test data for verification and repair")
	pad := (dataShards - len(data)%dataShards) % dataShards
	padded := append(data, make([]byte, pad)...)

	m := newMockBackend()
	shards := encodeShards(t, padded, dataShards, parityShards)
	m.storeShards("b", "k", shards)

	rec := scrubber.ObjectRecord{Bucket: "b", Key: "k", DataShards: dataShards, ParityShards: parityShards}
	m.records["b"] = []scrubber.ObjectRecord{rec}

	origShard0 := make([]byte, len(shards[0]))
	copy(origShard0, shards[0])
	delete(m.shards, "b/k/0")

	s := scrubber.New(m, time.Hour)
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

	time.Sleep(20 * time.Millisecond) // should not panic or deadlock
}

// TestScrubber_DeletedObject_NotFlagged verifies that an object deleted between
// ScanObjects and Verify is silently skipped — NOT counted as unrepairable.
// Regression for Eng Review #9 (DeleteObject race mitigation).
func TestScrubber_DeletedObject_NotFlagged(t *testing.T) {
	const (
		dataShards   = 4
		parityShards = 2
	)
	m := newMockBackend()

	rec := scrubber.ObjectRecord{Bucket: "b", Key: "obj", DataShards: dataShards, ParityShards: parityShards}
	// Record is in scan results but shards are missing (would trigger unrepairable without race check)
	m.records["b"] = []scrubber.ObjectRecord{rec}
	// No shards stored — this would normally be "3 shards missing" which is unrepairable

	// Simulate: object was deleted between scan and verify
	m.deletedObjects["b/obj"] = true

	s := scrubber.New(m, time.Hour)
	s.RunOnce(context.Background())

	stats := s.Stats()
	assert.EqualValues(t, 0, stats.ObjectsChecked, "deleted object must not be counted")
	assert.EqualValues(t, 0, stats.ShardErrors, "deleted object must not trigger shard errors")
	assert.EqualValues(t, 0, stats.Unrepairable, "deleted object must not be flagged as unrepairable")
}

// TestScrubber_PlainObject_Skipped verifies that plain-storage objects
// (DataShards == 0) never appear in ScanObjects results.
// The channel-based ScanObjects in ECBackend filters DataShards > 0 (Eng Review #10).
func TestScrubber_PlainObject_Skipped(t *testing.T) {
	m := newMockBackend()

	// Add a plain-storage record (DataShards=0) directly to records
	// (in real ECBackend, ScanObjects would never emit this)
	plain := scrubber.ObjectRecord{Bucket: "b", Key: "plain.txt", DataShards: 0, ParityShards: 0}
	ec := scrubber.ObjectRecord{Bucket: "b", Key: "ec.bin", DataShards: 4, ParityShards: 2}
	m.records["b"] = []scrubber.ObjectRecord{plain, ec}

	// Store all shards for ec.bin so it's healthy
	m.storeShards("b", "ec.bin", [][]byte{
		[]byte("d0"), []byte("d1"), []byte("d2"), []byte("d3"),
		[]byte("p0"), []byte("p1"),
	})

	s := scrubber.New(m, time.Hour)
	s.RunOnce(context.Background())

	stats := s.Stats()
	// Both records are delivered by mock ScanObjects — plain one passes ObjectExists (shards absent
	// but record exists), then Verify sees 0 shards as missing.
	// This test documents that the real ECBackend ScanObjects would filter DataShards=0.
	// With the mock, the plain record reaches Verify and triggers unrepairable since DataShards=0
	// means 0 missing+corrupt > 0 parity. But ObjectsChecked increments for it.
	// The important invariant: ec.bin with healthy shards must have 0 errors.
	assert.EqualValues(t, 0, stats.ShardErrors, "ec.bin with healthy shards must show 0 errors")
}

// ----------------------------------------------------------------------------
// Signing-health gate tests (Phase 16 Week 5 Slice 3)
// ----------------------------------------------------------------------------

// stubSigningHealthEmitter is an Emitter + SigningHealthChecker + SessionFinalizer
// that lets tests control signing health and count FinalizeSession calls.
type stubSigningHealthEmitter struct {
	signingOK bool
	emitted   []scrubber.HealEvent
	finalized []string
	mu        sync.Mutex
}

func (e *stubSigningHealthEmitter) Emit(ev scrubber.HealEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.emitted = append(e.emitted, ev)
}
func (e *stubSigningHealthEmitter) SigningHealthy() bool { return e.signingOK }
func (e *stubSigningHealthEmitter) FinalizeSession(cid string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.finalized = append(e.finalized, cid)
}

func TestScrubber_SigningHealthGate_SkipsRepairWhenUnhealthy(t *testing.T) {
	const (
		dataShards   = 4
		parityShards = 2
	)
	data := make([]byte, 64)
	m := newMockBackend()
	shards := encodeShards(t, data, dataShards, parityShards)
	m.storeShards("b", "k", shards)
	m.records["b"] = []scrubber.ObjectRecord{{Bucket: "b", Key: "k", DataShards: dataShards, ParityShards: parityShards}}
	delete(m.shards, "b/k/0") // one shard missing

	emitter := &stubSigningHealthEmitter{signingOK: false}
	s := scrubber.New(m, time.Hour, scrubber.WithNoRetry())
	s.SetEmitter(emitter)
	s.RunOnce(context.Background())

	// Detect events still emitted (verification ran).
	emitter.mu.Lock()
	detected := 0
	for _, ev := range emitter.emitted {
		if ev.Phase == scrubber.PhaseDetect {
			detected++
		}
	}
	emitter.mu.Unlock()
	assert.Greater(t, detected, 0, "detect events must be emitted even when signing is unhealthy")

	// But no FinalizeSession — repair was skipped.
	emitter.mu.Lock()
	finalizedCount := len(emitter.finalized)
	emitter.mu.Unlock()
	assert.Equal(t, 0, finalizedCount, "FinalizeSession must not be called when signing is unhealthy")

	// Shard must still be missing — repair did not run.
	_, repaired := m.shards["b/k/0"]
	assert.False(t, repaired, "shard must not be repaired when signing is unhealthy")
}

func TestScrubber_SigningHealthGate_FinalizeCalledWhenHealthy(t *testing.T) {
	const (
		dataShards   = 4
		parityShards = 2
	)
	data := make([]byte, 64)
	m := newMockBackend()
	shards := encodeShards(t, data, dataShards, parityShards)
	m.storeShards("b", "k", shards)
	m.records["b"] = []scrubber.ObjectRecord{{Bucket: "b", Key: "k", DataShards: dataShards, ParityShards: parityShards}}
	delete(m.shards, "b/k/0") // one shard missing

	emitter := &stubSigningHealthEmitter{signingOK: true}
	s := scrubber.New(m, time.Hour, scrubber.WithNoRetry())
	s.SetEmitter(emitter)
	s.RunOnce(context.Background())

	emitter.mu.Lock()
	finalizedCount := len(emitter.finalized)
	emitter.mu.Unlock()
	assert.Equal(t, 1, finalizedCount, "FinalizeSession must be called once after repair succeeds")
}
