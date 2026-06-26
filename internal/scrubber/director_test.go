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

func TestDirector_TriggerDedupSameRequest(t *testing.T) {
	d := NewDirector(DirectorOpts{Incident: &recordingIncident{}, QueueSize: 8})
	d.Register("ec", &countingSource{name: "ec"}, noopVerifier{})
	d.Start(context.Background())
	defer d.Stop()
	req := TriggerReq{Bucket: "__grainfs_volumes", KeyPrefix: "__vol/v/blk_"}
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
		})
	}
	require.Eventually(t, func() bool {
		return src.calls.Load() >= 1
	}, 2*time.Second, 50*time.Millisecond)
}

func TestRouteSourceFor(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		want   string
	}{
		{"internal buckets use the registered production source", "__grainfs_volumes", "ec"},
		{"S3-exposed buckets ride the EC data path", "user-bucket", "ec"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, routeSourceFor(tt.bucket))
		})
	}
}

func TestDirector_LookupDedup_Hit(t *testing.T) {
	d := NewDirector(DirectorOpts{NodeID: "n1"})
	d.Start(context.Background())
	defer d.Stop()
	id, created := d.Trigger(TriggerReq{Bucket: "b1", KeyPrefix: "p"})
	require.NotEmpty(t, id)
	require.True(t, created)

	got, ok := d.LookupDedup(TriggerReq{Bucket: "b1", KeyPrefix: "p"})
	require.True(t, ok)
	require.Equal(t, id, got.SessionID)
	require.Equal(t, "b1", got.Bucket)
}

func TestDirector_LookupDedup_Miss(t *testing.T) {
	d := NewDirector(DirectorOpts{NodeID: "n1"})
	d.Start(context.Background())
	defer d.Stop()
	_, ok := d.LookupDedup(TriggerReq{Bucket: "ghost"})
	require.False(t, ok)
}
