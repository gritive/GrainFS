// Package cluster: handler-wire integration test for KEKLeaseSnapshotHandler.
//
// It calls the real KEKLeaseSnapshotHandler with a real wire-encoded request and
// decodes the real wire-encoded response, proving that:
//
//  1. encodeKEKLeaseSnapshotReq → decodeKEKLeaseSnapshotReq round-trips.
//  2. KEKLeaseSnapshotHandler.Handle calls snapshotRefCountFn.
//  3. The SnapshotRefCount field survives the encode → decode path.
//
// With the object-metadata snapshot feature removed, production always reports a
// count of 0; this test injects a stub count fn to exercise the surviving
// KEK-lease wire codec without depending on the deleted snapshot package.
package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func TestKEKLeaseSnapshotHandler_WireRoundTrip(t *testing.T) {
	h := NewKEKLeaseSnapshotHandler(
		"node-remote",
		encrypt.NewKEKLeaseTracker(),
		func() uint64 { return 100 },
		func(v uint32) (uint64, error) {
			if v == 1 {
				return 1, nil
			}
			return 0, nil
		},
	)

	// --- v1 probe: expect SnapshotRefCount == 1 ---
	respV1, herr := h.Handle(encodeKEKLeaseSnapshotReq(KEKLeaseSnapshotReq{Version: 1}))
	if herr != nil {
		t.Fatalf("v1 probe: handler error: %v", herr)
	}
	gotV1, err := decodeKEKLeaseSnapshotResp(respV1)
	if err != nil {
		t.Fatalf("v1 probe: decode response: %v", err)
	}
	if gotV1.SnapshotRefCount != 1 {
		t.Errorf("v1 probe: SnapshotRefCount = %d, want 1", gotV1.SnapshotRefCount)
	}
	if gotV1.ObservedAtRaftCommitIndex != 100 {
		t.Errorf("v1 probe: ObservedAtRaftCommitIndex = %d, want 100", gotV1.ObservedAtRaftCommitIndex)
	}
	if gotV1.NodeID != "node-remote" {
		t.Errorf("v1 probe: NodeID = %q, want node-remote", gotV1.NodeID)
	}

	// --- v2 probe: count must be 0 ---
	respV2, herr := h.Handle(encodeKEKLeaseSnapshotReq(KEKLeaseSnapshotReq{Version: 2}))
	if herr != nil {
		t.Fatalf("v2 probe: handler error: %v", herr)
	}
	gotV2, err := decodeKEKLeaseSnapshotResp(respV2)
	if err != nil {
		t.Fatalf("v2 probe: decode response: %v", err)
	}
	if gotV2.SnapshotRefCount != 0 {
		t.Errorf("v2 probe: SnapshotRefCount = %d, want 0", gotV2.SnapshotRefCount)
	}
}
