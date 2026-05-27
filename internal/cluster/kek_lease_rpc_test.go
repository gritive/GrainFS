package cluster

import (
	"context"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// noopSnapRefCount is a zero-returning snapshotRefCountFn for tests that do
// not exercise the snapshot-ref path.
func noopSnapRefCount(_ uint32) (uint64, error) { return 0, nil }

func TestKEKLeaseRPC_RoundTrip_ZeroCount(t *testing.T) {
	tracker := encrypt.NewKEKLeaseTracker()
	h := NewKEKLeaseSnapshotHandler("node-A", tracker, func() uint64 { return 42 }, noopSnapRefCount)

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
	h := NewKEKLeaseSnapshotHandler("node-X", encrypt.NewKEKLeaseTracker(), func() uint64 { return 0 }, noopSnapRefCount)
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

	h := NewKEKLeaseSnapshotHandler("node-B", tracker, func() uint64 { return 99 }, noopSnapRefCount)

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

// TestKEKLeaseRPC_SnapshotRefCount_RoundTrip verifies that SnapshotRefCount
// survives encode → decode.
func TestKEKLeaseRPC_SnapshotRefCount_RoundTrip(t *testing.T) {
	tracker := encrypt.NewKEKLeaseTracker()
	h := NewKEKLeaseSnapshotHandler("node-C", tracker, func() uint64 { return 7 }, func(_ uint32) (uint64, error) {
		return 3, nil // 3 retained snapshots under this KEK version
	})

	req := &transport.Message{
		Type:    transport.StreamKEKLeaseSnapshotProbe,
		Payload: encodeKEKLeaseSnapshotReq(KEKLeaseSnapshotReq{Version: 2}),
	}
	resp := h.Handle(req)
	if resp.Status != transport.StatusOK {
		t.Fatalf("handler returned status=%v payload=%q", resp.Status, string(resp.Payload))
	}
	decoded, err := decodeKEKLeaseSnapshotResp(resp.Payload)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if decoded.SnapshotRefCount != 3 {
		t.Errorf("snapshot_ref_count = %d, want 3", decoded.SnapshotRefCount)
	}
	if decoded.NodeID != "node-C" {
		t.Errorf("node_id = %q, want node-C", decoded.NodeID)
	}
}

// TestKEKLeaseRPC_OldMagicRejected verifies that decoding a payload with the
// old \x01 magic returns the upgrade-required error.
func TestKEKLeaseRPC_OldMagicRejected(t *testing.T) {
	// Build a payload using the old \x01 magic.
	oldMagic := []byte("KLSREP\x01")
	var payload []byte
	payload = append(payload, oldMagic...)
	payload = append(payload, make([]byte, 8+8+2)...) // lease_count + commit_index + id_len

	_, err := decodeKEKLeaseSnapshotResp(payload)
	if err == nil {
		t.Fatal("expected error for old magic, got nil")
	}
	if !strings.Contains(err.Error(), "upgrade all nodes") {
		t.Errorf("error message should mention upgrade: %v", err)
	}
}
