package scrubber

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/incident"
)

type recordingIncident struct {
	mu    sync.Mutex
	facts [][]incident.Fact
}

func (r *recordingIncident) Record(ctx context.Context, facts []incident.Fact) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.facts = append(r.facts, facts)
	return nil
}

func (r *recordingIncident) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.facts)
}

func TestDirector_TriggerDedupSameRequest(t *testing.T) {
	d := NewDirector(DirectorOpts{Incident: &recordingIncident{}, QueueSize: 8})
	d.Register("replication", &countingSource{name: "replication"}, noopVerifier{})
	req := TriggerReq{Bucket: "__grainfs_volumes", KeyPrefix: "__vol/v/blk_", Scope: ScopeFull}
	id1, created1 := d.Trigger(req)
	require.NotEmpty(t, id1)
	require.True(t, created1)
	id2, created2 := d.Trigger(req)
	require.Equal(t, id1, id2, "same request should reuse session id")
	require.False(t, created2)
}

func TestDirector_ApplyFromFSM_Nonblocking(t *testing.T) {
	src := &countingSource{name: "replication"}
	d := NewDirector(DirectorOpts{Incident: &recordingIncident{}, QueueSize: 1})
	d.Register("replication", src, noopVerifier{})
	d.Start(context.Background())
	defer d.Stop()
	for i := 0; i < 5; i++ {
		d.ApplyFromFSM(ScrubTriggerEntry{
			SessionID: "sess",
			Bucket:    "__grainfs_volumes",
			KeyPrefix: "__vol/v/blk_",
			Scope:     ScopeFull,
		})
	}
	require.Eventually(t, func() bool {
		return src.calls.Load() >= 1
	}, 2*time.Second, 50*time.Millisecond)
}
