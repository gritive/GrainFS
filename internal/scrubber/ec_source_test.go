package scrubber_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// recorderEmitter records every HealEvent for verification in tests.
type recorderEmitter struct {
	mu     sync.Mutex
	events []scrubber.HealEvent
}

func (r *recorderEmitter) Emit(e scrubber.HealEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorderEmitter) countByPhase(p scrubber.HealPhase) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if e.Phase == p {
			n++
		}
	}
	return n
}

func (r *recorderEmitter) snapshot() []scrubber.HealEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]scrubber.HealEvent, len(r.events))
	copy(out, r.events)
	return out
}

// signingFlagEmitter implements SigningHealthChecker so we can simulate signing-down.
type signingFlagEmitter struct {
	mu         sync.Mutex
	healthy    bool
	eventCount int
}

func (s *signingFlagEmitter) Emit(e scrubber.HealEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eventCount++
	_ = e
}
func (s *signingFlagEmitter) SigningHealthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.healthy
}

// finalizingEmitter implements both Emitter and SessionFinalizer.
type finalizingEmitter struct {
	mu        sync.Mutex
	events    []scrubber.HealEvent
	finalized []string
}

func (f *finalizingEmitter) Emit(e scrubber.HealEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
}
func (f *finalizingEmitter) FinalizeSession(correlationID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finalized = append(f.finalized, correlationID)
}
func (f *finalizingEmitter) countByPhase(p scrubber.HealPhase) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, e := range f.events {
		if e.Phase == p {
			n++
		}
	}
	return n
}

// --- Iter tests ---

func TestECScrubSource_Iter_HappyPath(t *testing.T) {
	m := newMockBackend()
	m.records["b"] = []scrubber.ObjectRecord{
		{Bucket: "b", Key: "k1", DataShards: 2, ParityShards: 1, VersionID: "v1", ETag: "e1"},
		{Bucket: "b", Key: "k2", DataShards: 2, ParityShards: 1, VersionID: "v2", ETag: "e2"},
	}
	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(m), "node-A")
	require.Equal(t, "ec", src.Name())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := src.Iter(ctx, scrubber.ScopeFull, "b", "")
	require.NoError(t, err)

	var got []scrubber.Block
	for blk := range ch {
		got = append(got, blk)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].Key < got[j].Key })
	require.Len(t, got, 2)
	assert.Equal(t, "b", got[0].Bucket)
	assert.Equal(t, "k1", got[0].Key)
	assert.Equal(t, "v1", got[0].VersionID)
	assert.Equal(t, "e1", got[0].ExpectedETag)
}

func TestECScrubSource_Iter_KeyPrefixFilter(t *testing.T) {
	m := newMockBackend()
	m.records["b"] = []scrubber.ObjectRecord{
		{Bucket: "b", Key: "match/a", DataShards: 2, ParityShards: 1, VersionID: "v1"},
		{Bucket: "b", Key: "match/b", DataShards: 2, ParityShards: 1, VersionID: "v2"},
		{Bucket: "b", Key: "other/c", DataShards: 2, ParityShards: 1, VersionID: "v3"},
	}
	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(m), "node-A")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := src.Iter(ctx, scrubber.ScopeFull, "b", "match/")
	require.NoError(t, err)

	var keys []string
	for blk := range ch {
		keys = append(keys, blk.Key)
	}
	sort.Strings(keys)
	assert.Equal(t, []string{"match/a", "match/b"}, keys)
}

func TestECScrubSource_Iter_ScopeLiveEqualsFull(t *testing.T) {
	m := newMockBackend()
	m.records["b"] = []scrubber.ObjectRecord{
		{Bucket: "b", Key: "k1", DataShards: 2, ParityShards: 1, VersionID: "v1"},
	}
	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(m), "node-A")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	chFull, err := src.Iter(ctx, scrubber.ScopeFull, "b", "")
	require.NoError(t, err)
	var fullKeys []string
	for b := range chFull {
		fullKeys = append(fullKeys, b.Key)
	}
	chLive, err := src.Iter(ctx, scrubber.ScopeLive, "b", "")
	require.NoError(t, err)
	var liveKeys []string
	for b := range chLive {
		liveKeys = append(liveKeys, b.Key)
	}
	assert.Equal(t, fullKeys, liveKeys)
}

func TestECScrubSource_Iter_EmptyBucket(t *testing.T) {
	m := newMockBackend()
	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(m), "node-A")
	ch, err := src.Iter(context.Background(), scrubber.ScopeFull, "", "")
	require.NoError(t, err)
	var got []scrubber.Block
	for b := range ch {
		got = append(got, b)
	}
	assert.Empty(t, got)
}

func TestECScrubSource_Iter_DroppedObjectsSkipped(t *testing.T) {
	m := newMockBackend()
	m.records["b"] = []scrubber.ObjectRecord{
		{Bucket: "b", Key: "alive", DataShards: 2, ParityShards: 1, VersionID: "v1"},
		{Bucket: "b", Key: "dead", DataShards: 2, ParityShards: 1, VersionID: "v2"},
	}
	m.deletedObjects["b/dead"] = true
	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(m), "node-A")
	ch, err := src.Iter(context.Background(), scrubber.ScopeFull, "b", "")
	require.NoError(t, err)
	var keys []string
	for blk := range ch {
		keys = append(keys, blk.Key)
	}
	assert.Equal(t, []string{"alive"}, keys)
}

func TestECScrubSource_Iter_ContextCancelMidWalk(t *testing.T) {
	m := newMockBackend()
	for i := 0; i < 50; i++ {
		m.records["b"] = append(m.records["b"], scrubber.ObjectRecord{
			Bucket: "b", Key: fmt.Sprintf("k%02d", i), DataShards: 2, ParityShards: 1, VersionID: "v",
		})
	}
	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(m), "node-A")
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := src.Iter(ctx, scrubber.ScopeFull, "b", "")
	require.NoError(t, err)
	<-ch
	cancel()
	done := make(chan struct{})
	go func() {
		for range ch {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Iter goroutine leaked after context cancel")
	}
}

func TestECScrubSource_Iter_ShardOwnerFilter(t *testing.T) {
	o := newOwnerBackend("node-A")
	o.records["b"] = []scrubber.ObjectRecord{
		{Bucket: "b", Key: "owned", DataShards: 2, ParityShards: 1, VersionID: "v1"},
		{Bucket: "b", Key: "peer-only", DataShards: 2, ParityShards: 1, VersionID: "v2"},
	}
	o.ownedByKey["b/owned"] = []int{0}

	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(o), "node-A")
	ch, err := src.Iter(context.Background(), scrubber.ScopeFull, "b", "")
	require.NoError(t, err)
	var keys []string
	for blk := range ch {
		keys = append(keys, blk.Key)
	}
	assert.Equal(t, []string{"owned"}, keys, "peer-only object must be filtered")
}

// --- Verify tests ---

func TestECScrubVerifier_Verify_Healthy(t *testing.T) {
	m := newMockBackend()
	rec := scrubber.ObjectRecord{Bucket: "b", Key: "k", DataShards: 2, ParityShards: 1, VersionID: "v"}
	m.records["b"] = []scrubber.ObjectRecord{rec}
	m.storeShards("b", "k", [][]byte{[]byte("s0"), []byte("s1"), []byte("s2")})

	bs := scrubber.New(m, time.Hour)
	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(m), "node-A")
	src.PrimeForTest("b", "k", "v", rec)
	ver := scrubber.NewECScrubVerifier(m, bs.Verifier(), bs.Limiter(), bs.Emitter(), "node-A", src)

	st, err := ver.Verify(context.Background(), scrubber.Block{Bucket: "b", Key: "k", VersionID: "v"})
	require.NoError(t, err)
	assert.True(t, st.IsHealthy())
}

func TestECScrubVerifier_Verify_MissingShard(t *testing.T) {
	m := newMockBackend()
	rec := scrubber.ObjectRecord{Bucket: "b", Key: "k", DataShards: 2, ParityShards: 1, VersionID: "v"}
	m.records["b"] = []scrubber.ObjectRecord{rec}
	m.storeShards("b", "k", [][]byte{[]byte("s0"), nil, []byte("s2")})

	bs := scrubber.New(m, time.Hour, scrubber.WithNoRetry())
	em := &recorderEmitter{}
	bs.SetEmitter(em)
	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(m), "node-A")
	src.PrimeForTest("b", "k", "v", rec)
	ver := scrubber.NewECScrubVerifier(m, bs.Verifier(), bs.Limiter(), bs.Emitter(), "node-A", src)

	st, err := ver.Verify(context.Background(), scrubber.Block{Bucket: "b", Key: "k", VersionID: "v"})
	require.NoError(t, err)
	assert.False(t, st.IsHealthy())
	assert.True(t, st.Missing)
	assert.GreaterOrEqual(t, em.countByPhase(scrubber.PhaseDetect), 1, "Detect emitted")
}

func TestECScrubVerifier_Verify_UnverifiedLegacyShardSkipped(t *testing.T) {
	m := newIntegrityMockBackend()
	rec := scrubber.ObjectRecord{Bucket: "b", Key: "k", DataShards: 2, ParityShards: 1, VersionID: "v"}
	m.records["b"] = []scrubber.ObjectRecord{rec}
	m.storeShards("b", "k", [][]byte{[]byte("s0"), []byte("s1"), []byte("s2")})
	m.status["b/k/1"] = scrubber.ShardIntegrityUnverifiedLegacy

	bs := scrubber.New(m, time.Hour, scrubber.WithNoRetry())
	em := &recorderEmitter{}
	bs.SetEmitter(em)
	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(m), "node-A")
	src.PrimeForTest("b", "k", "v", rec)
	ver := scrubber.NewECScrubVerifier(m, bs.Verifier(), bs.Limiter(), bs.Emitter(), "node-A", src)

	st, err := ver.Verify(context.Background(), scrubber.Block{Bucket: "b", Key: "k", VersionID: "v"})
	require.NoError(t, err)
	require.True(t, st.Skipped)
	assert.False(t, st.Healthy)
	assert.Contains(t, st.Detail, "legacy shard without CRC oracle")
	assert.Contains(t, st.Detail, "[1]")

	events := em.snapshot()
	require.Len(t, events, 1)
	assert.Equal(t, scrubber.PhaseDetect, events[0].Phase)
	assert.Equal(t, scrubber.OutcomeSkipped, events[0].Outcome)
	assert.Equal(t, "legacy_no_crc", events[0].ErrCode)
	assert.EqualValues(t, 1, events[0].ShardID)
}

func TestECScrubVerifier_Verify_SigningDownSkips(t *testing.T) {
	m := newMockBackend()
	rec := scrubber.ObjectRecord{Bucket: "b", Key: "k", DataShards: 2, ParityShards: 1, VersionID: "v"}
	m.records["b"] = []scrubber.ObjectRecord{rec}
	m.storeShards("b", "k", [][]byte{[]byte("s0"), nil, []byte("s2")})

	bs := scrubber.New(m, time.Hour)
	em := &signingFlagEmitter{healthy: false}
	bs.SetEmitter(em)
	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(m), "node-A")
	src.PrimeForTest("b", "k", "v", rec)
	ver := scrubber.NewECScrubVerifier(m, bs.Verifier(), bs.Limiter(), bs.Emitter(), "node-A", src)

	st, err := ver.Verify(context.Background(), scrubber.Block{Bucket: "b", Key: "k", VersionID: "v"})
	require.NoError(t, err)
	assert.True(t, st.Skipped)
	assert.Equal(t, 0, em.eventCount, "no events when signing down")
}

func TestECScrubVerifier_Repair_FullPipeline(t *testing.T) {
	o := newOwnerBackend("node-A")
	rec := scrubber.ObjectRecord{Bucket: "b", Key: "k", DataShards: 2, ParityShards: 1, VersionID: "v"}
	o.records["b"] = []scrubber.ObjectRecord{rec}
	o.ownedByKey["b/k"] = []int{0, 1, 2}
	o.storeShards("b", "k", [][]byte{nil, []byte("s1"), []byte("s2")}) // shard 0 missing

	bs := scrubber.New(o, time.Hour, scrubber.WithNoRetry())
	em := &finalizingEmitter{}
	bs.SetEmitter(em)
	src := scrubber.NewECScrubSource(scrubber.SingleBackendResolver(o), "node-A")
	src.PrimeForTest("b", "k", "v", rec)
	ver := scrubber.NewECScrubVerifier(o, bs.Verifier(), bs.Limiter(), bs.Emitter(), "node-A", src)

	blk := scrubber.Block{Bucket: "b", Key: "k", VersionID: "v"}
	st, err := ver.Verify(context.Background(), blk)
	require.NoError(t, err)
	require.True(t, st.Missing, "missing shard detected")

	require.NoError(t, ver.Repair(context.Background(), blk))

	calls := o.repairedCalls()
	require.Len(t, calls, 1, "one RepairShardLocal call for missing shard 0")
	assert.Equal(t, 0, calls[0].shardIdx)

	assert.GreaterOrEqual(t, em.countByPhase(scrubber.PhaseReconstruct), 1)
	assert.GreaterOrEqual(t, em.countByPhase(scrubber.PhaseWrite), 1)
	assert.GreaterOrEqual(t, em.countByPhase(scrubber.PhaseVerify), 1)
	assert.NotEmpty(t, em.finalized, "FinalizeSession invoked with correlation IDs")
}

func TestECScrubSource_Iter_ResolverReturnsFalse_EmptyChannel(t *testing.T) {
	src := scrubber.NewECScrubSource(func(string) (scrubber.Scrubbable, bool) { return nil, false }, "node-A")
	ch, err := src.Iter(context.Background(), scrubber.ScopeFull, "any-bucket", "")
	require.NoError(t, err)
	_, open := <-ch
	require.False(t, open, "channel must close immediately when resolver yields false")
}

func TestECScrubSource_Iter_DifferentBucketsResolveToDifferentBackends(t *testing.T) {
	backendA := newMockBackend()
	backendA.records["a"] = []scrubber.ObjectRecord{
		{Bucket: "a", Key: "ka", DataShards: 2, ParityShards: 1, VersionID: "va", ETag: "ea"},
	}
	backendB := newMockBackend()
	backendB.records["b"] = []scrubber.ObjectRecord{
		{Bucket: "b", Key: "kb", DataShards: 2, ParityShards: 1, VersionID: "vb", ETag: "eb"},
	}
	resolve := func(bucket string) (scrubber.Scrubbable, bool) {
		switch bucket {
		case "a":
			return backendA, true
		case "b":
			return backendB, true
		}
		return nil, false
	}
	src := scrubber.NewECScrubSource(resolve, "node-A")

	ch, err := src.Iter(context.Background(), scrubber.ScopeFull, "a", "")
	require.NoError(t, err)
	var got []scrubber.Block
	for blk := range ch {
		got = append(got, blk)
	}
	require.Len(t, got, 1)
	assert.Equal(t, "a", got[0].Bucket)
	assert.Equal(t, "ka", got[0].Key)

	ch2, err := src.Iter(context.Background(), scrubber.ScopeFull, "c", "")
	require.NoError(t, err)
	_, open := <-ch2
	require.False(t, open, "unmapped bucket must yield empty channel")
}
