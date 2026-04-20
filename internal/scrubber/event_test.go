package scrubber_test

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

func TestNewEvent_PopulatesIDAndTimestamp(t *testing.T) {
	before := time.Now().UTC()
	e := scrubber.NewEvent(scrubber.PhaseDetect, scrubber.OutcomeSuccess)
	after := time.Now().UTC()

	assert.NotEmpty(t, e.ID, "ID should be set")
	assert.Equal(t, scrubber.PhaseDetect, e.Phase)
	assert.Equal(t, scrubber.OutcomeSuccess, e.Outcome)
	assert.EqualValues(t, -1, e.ShardID, "ShardID defaults to -1")
	assert.False(t, e.Timestamp.Before(before), "Timestamp >= before")
	assert.False(t, e.Timestamp.After(after.Add(time.Second)), "Timestamp <= after+1s")
}

func TestNewEvent_UniqueIDs(t *testing.T) {
	const n = 200
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		e := scrubber.NewEvent(scrubber.PhaseWrite, scrubber.OutcomeSuccess)
		_, dup := seen[e.ID]
		require.Falsef(t, dup, "duplicate ID: %s", e.ID)
		seen[e.ID] = struct{}{}
	}
}

func TestHealEvent_JSONRoundTrip(t *testing.T) {
	original := scrubber.HealEvent{
		ID:            "019daa5b-0000-7000-8000-000000000000",
		Timestamp:     time.Date(2026, 4, 20, 18, 0, 0, 0, time.UTC),
		Phase:         scrubber.PhaseReconstruct,
		Bucket:        "photos",
		Key:           "holiday.jpg",
		VersionID:     "v1",
		ShardID:       2,
		PeerID:        "node-4",
		BytesRepaired: 4 * 1024 * 1024,
		DurationMs:    1234,
		Outcome:       scrubber.OutcomeSuccess,
		CorrelationID: "session-42",
	}
	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded scrubber.HealEvent
	require.NoError(t, json.Unmarshal(data, &decoded))

	assert.Equal(t, original, decoded)
}

func TestHealEvent_FailureOmitsOptionalFields(t *testing.T) {
	e := scrubber.NewEvent(scrubber.PhaseWrite, scrubber.OutcomeFailed)
	e.ErrCode = "disk_full"

	data, err := json.Marshal(e)
	require.NoError(t, err)
	s := string(data)

	assert.Contains(t, s, `"err_code":"disk_full"`)
	assert.NotContains(t, s, `"bucket"`, "empty bucket should be omitted")
	assert.NotContains(t, s, `"peer_id"`, "empty peer_id should be omitted")
	assert.NotContains(t, s, `"bytes_repaired"`, "zero bytes_repaired should be omitted")
}

func TestPhaseConstants_String(t *testing.T) {
	cases := []struct {
		name string
		p    scrubber.HealPhase
		want string
	}{
		{"detect", scrubber.PhaseDetect, "detect"},
		{"reconstruct", scrubber.PhaseReconstruct, "reconstruct"},
		{"write", scrubber.PhaseWrite, "write"},
		{"verify", scrubber.PhaseVerify, "verify"},
		{"startup", scrubber.PhaseStartup, "startup"},
		{"degraded", scrubber.PhaseDegraded, "degraded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, string(tc.p))
		})
	}
}

func TestOutcomeConstants_String(t *testing.T) {
	cases := []struct {
		name string
		o    scrubber.HealOutcome
		want string
	}{
		{"success", scrubber.OutcomeSuccess, "success"},
		{"failed", scrubber.OutcomeFailed, "failed"},
		{"skipped", scrubber.OutcomeSkipped, "skipped"},
		{"isolated", scrubber.OutcomeIsolated, "isolated"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, string(tc.o))
		})
	}
}

func TestNoopEmitter_DoesNotPanic(t *testing.T) {
	var em scrubber.Emitter = scrubber.NoopEmitter{}
	em.Emit(scrubber.NewEvent(scrubber.PhaseDetect, scrubber.OutcomeSuccess))
}

// recordingEmitter is a minimal test Emitter used to verify the interface
// contract. Production implementations will live in other packages (SSE hub,
// eventstore adapter) and are tested there.
type recordingEmitter struct {
	mu     sync.Mutex
	events []scrubber.HealEvent
}

func (r *recordingEmitter) Emit(e scrubber.HealEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recordingEmitter) snapshot() []scrubber.HealEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]scrubber.HealEvent, len(r.events))
	copy(out, r.events)
	return out
}

func TestEmitter_Concurrent(t *testing.T) {
	rec := &recordingEmitter{}
	const goroutines = 32
	const perGoroutine = 25
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				rec.Emit(scrubber.NewEvent(scrubber.PhaseDetect, scrubber.OutcomeSuccess))
			}
		}()
	}
	wg.Wait()

	got := rec.snapshot()
	assert.Len(t, got, goroutines*perGoroutine)
	ids := make(map[string]struct{}, len(got))
	for _, e := range got {
		ids[e.ID] = struct{}{}
	}
	assert.Len(t, ids, goroutines*perGoroutine, "all IDs unique across concurrent emits")
}
