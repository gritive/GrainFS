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
	d.Start(context.Background())
	defer d.Stop()
	req := TriggerReq{Bucket: "__grainfs_volumes", KeyPrefix: "__vol/v/blk_", Scope: ScopeFull}
	id1, created1 := d.Trigger(req)
	require.NotEmpty(t, id1)
	require.True(t, created1)
	id2, created2 := d.Trigger(req)
	require.Equal(t, id1, id2, "same request should reuse session id")
	require.False(t, created2)
}

func TestDirector_ApplyFromFSM_Nonblocking(t *testing.T) {
	src := &countingSource{name: "ec"}
	d := NewDirector(DirectorOpts{Incident: &recordingIncident{}, QueueSize: 1})
	d.Register("ec", src, noopVerifier{})
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

func TestDirector_RoutesVolumeBlocksToECSource(t *testing.T) {
	repl := &countingSource{name: "replication"}
	ec := &countingSource{name: "ec"}
	d := NewDirector(DirectorOpts{Incident: &recordingIncident{}, QueueSize: 8})
	d.Register("replication", repl, noopVerifier{})
	d.Register("ec", ec, noopVerifier{})
	d.Start(context.Background())
	defer d.Stop()

	id, created := d.Trigger(TriggerReq{
		Bucket:    "__grainfs_volumes",
		KeyPrefix: "__vol/v/blk_",
		Scope:     ScopeFull,
	})
	require.NotEmpty(t, id)
	require.True(t, created)

	require.Eventually(t, func() bool {
		return ec.calls.Load() == 1
	}, 2*time.Second, 50*time.Millisecond)
	require.Equal(t, int32(0), repl.calls.Load(), "volume blocks are EC objects, not full-object replicas")
}

func TestDirector_LookupDedup_Hit(t *testing.T) {
	d := NewDirector(DirectorOpts{NodeID: "n1"})
	d.Start(context.Background())
	defer d.Stop()
	id, created := d.Trigger(TriggerReq{Bucket: "b1", KeyPrefix: "p", Scope: ScopeFull})
	require.NotEmpty(t, id)
	require.True(t, created)

	got, ok := d.LookupDedup(TriggerReq{Bucket: "b1", KeyPrefix: "p", Scope: ScopeFull})
	require.True(t, ok)
	require.Equal(t, id, got.SessionID)
	require.Equal(t, "b1", got.Bucket)
}

func TestDirector_LookupDedup_Miss(t *testing.T) {
	d := NewDirector(DirectorOpts{NodeID: "n1"})
	d.Start(context.Background())
	defer d.Stop()
	_, ok := d.LookupDedup(TriggerReq{Bucket: "ghost", Scope: ScopeFull})
	require.False(t, ok)
}
