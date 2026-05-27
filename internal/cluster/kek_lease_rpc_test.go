package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

func TestKEKLeaseRPC_RoundTrip_ZeroCount(t *testing.T) {
	tracker := encrypt.NewKEKLeaseTracker()
	h := NewKEKLeaseSnapshotHandler("node-A", tracker, func() uint64 { return 42 })

	req := &transport.Message{
		Type:    transport.StreamKEKLeaseSnapshotProbe,
		Payload: encodeKEKLeaseSnapshotReq(KEKLeaseSnapshotReq{Version: 5}),
	}
	resp := h.Handle(req)
	if resp.Status != transport.StatusOK {
		t.Fatalf("handler returned status=%v payload=%q", resp.Status, string(resp.Payload))
	}
	decoded, err := decodeKEKLeaseSnapshotResp(resp.Payload)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if decoded.LeaseCount != 0 {
		t.Errorf("lease count = %d, want 0", decoded.LeaseCount)
	}
	if decoded.ObservedAtRaftCommitIndex != 42 {
		t.Errorf("observed_at_raft_commit_index = %d, want 42", decoded.ObservedAtRaftCommitIndex)
	}
	if decoded.NodeID != "node-A" {
		t.Errorf("node_id = %q, want node-A", decoded.NodeID)
	}
}

func TestKEKLeaseSnapshotHandler_RejectsBadMagic(t *testing.T) {
	h := NewKEKLeaseSnapshotHandler("node-X", encrypt.NewKEKLeaseTracker(), func() uint64 { return 0 })
	resp := h.Handle(&transport.Message{
		Type:    transport.StreamKEKLeaseSnapshotProbe,
		Payload: []byte("not the right magic"),
	})
	if resp.Status != transport.StatusError {
		t.Fatalf("expected StatusError on bad magic, got %v", resp.Status)
	}
}

func TestGetKEKLeaseSnapshot_RoundTrip(t *testing.T) {
	tracker := encrypt.NewKEKLeaseTracker()
	rel := tracker.Acquire(7)
	defer rel()

	h := NewKEKLeaseSnapshotHandler("node-B", tracker, func() uint64 { return 99 })

	dialer := func(_ context.Context, _ string, payload []byte) ([]byte, error) {
		reqMsg := &transport.Message{
			Type:    transport.StreamKEKLeaseSnapshotProbe,
			Payload: payload,
		}
		respMsg := h.Handle(reqMsg)
		return respMsg.Payload, nil
	}

	got, err := GetKEKLeaseSnapshot(context.Background(), "node-B", 7, dialer)
	if err != nil {
		t.Fatalf("GetKEKLeaseSnapshot: %v", err)
	}
	if got.LeaseCount != 1 {
		t.Errorf("lease count = %d, want 1", got.LeaseCount)
	}
	if got.ObservedAtRaftCommitIndex != 99 {
		t.Errorf("observed_at_raft_commit_index = %d, want 99", got.ObservedAtRaftCommitIndex)
	}
	if got.NodeID != "node-B" {
		t.Errorf("node_id = %q, want node-B", got.NodeID)
	}
}
