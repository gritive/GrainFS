package cluster

import (
	"context"
	"errors"
	"testing"
)

// fakeProbeDialer captures the peer + payload of each call and returns the
// next canned response (encoded with the production codec).
type fakeProbeDialer struct {
	diskFn  func(ctx context.Context, peer string, payload []byte) ([]byte, error)
	leaseFn func(ctx context.Context, peer string, payload []byte) ([]byte, error)
}

func (d *fakeProbeDialer) CallKEKDiskSpace(ctx context.Context, peer string, payload []byte) ([]byte, error) {
	return d.diskFn(ctx, peer, payload)
}

func (d *fakeProbeDialer) CallKEKLeaseSnapshot(ctx context.Context, peer string, payload []byte) ([]byte, error) {
	return d.leaseFn(ctx, peer, payload)
}

func TestProductionPeerKEKProbe_DiskSpaceFanOut_LocalSelfShortcuts(t *testing.T) {
	rcr := &fakeRaftConfigReader{voters: []string{"node-a", "node-self", "node-b"}, configIndex: 5}
	dialer := &fakeProbeDialer{
		diskFn: func(_ context.Context, peer string, _ []byte) ([]byte, error) {
			resp := KEKDiskSpaceResp{NodeID: peer, FreeBytes: 1 << 30, KeystorePath: "/keys"}
			return encodeKEKDiskSpaceResp(resp), nil
		},
	}
	localCalls := 0
	probe := NewPeerKEKProbe(dialer, rcr, "node-self",
		func() (KEKDiskSpaceResp, error) {
			localCalls++
			return KEKDiskSpaceResp{NodeID: "node-self", FreeBytes: 99}, nil
		},
		func(uint32) (KEKLeaseSnapshotResp, error) {
			return KEKLeaseSnapshotResp{}, nil
		},
	)

	resp, err := probe.ProbeAllKEKDiskSpace(context.Background())
	if err != nil {
		t.Fatalf("ProbeAllKEKDiskSpace: %v", err)
	}
	if len(resp) != 3 {
		t.Fatalf("got %d responses, want 3", len(resp))
	}
	if localCalls != 1 {
		t.Errorf("localDiskFn called %d times, want 1", localCalls)
	}
	// Self entry carries the local 99-byte sentinel.
	var sawSelf bool
	for _, r := range resp {
		if r.NodeID == "node-self" {
			if r.FreeBytes != 99 {
				t.Errorf("self FreeBytes = %d, want 99", r.FreeBytes)
			}
			sawSelf = true
		}
	}
	if !sawSelf {
		t.Errorf("response missing self entry")
	}
}

func TestProductionPeerKEKProbe_LeaseSnapshotFanOut_LocalSelfShortcuts(t *testing.T) {
	rcr := &fakeRaftConfigReader{voters: []string{"node-self", "node-b"}, configIndex: 5}
	dialer := &fakeProbeDialer{
		leaseFn: func(_ context.Context, peer string, _ []byte) ([]byte, error) {
			resp := KEKLeaseSnapshotResp{NodeID: peer, LeaseCount: 0, ObservedAtRaftCommitIndex: 200}
			return encodeKEKLeaseSnapshotResp(resp), nil
		},
	}
	localCalls := 0
	probe := NewPeerKEKProbe(dialer, rcr, "node-self",
		func() (KEKDiskSpaceResp, error) {
			return KEKDiskSpaceResp{}, nil
		},
		func(version uint32) (KEKLeaseSnapshotResp, error) {
			localCalls++
			return KEKLeaseSnapshotResp{NodeID: "node-self", LeaseCount: 7, ObservedAtRaftCommitIndex: 50}, nil
		},
	)

	samples, err := probe.ProbeKEKLeaseSnapshot(context.Background(), []string{"node-self", "node-b"}, 3)
	if err != nil {
		t.Fatalf("ProbeKEKLeaseSnapshot: %v", err)
	}
	if len(samples) != 2 {
		t.Fatalf("got %d samples, want 2", len(samples))
	}
	if localCalls != 1 {
		t.Errorf("localLeaseFn called %d times, want 1", localCalls)
	}
	if samples[0].NodeID != "node-self" || samples[0].LeaseCount != 7 {
		t.Errorf("self sample = %+v, want NodeID=node-self LeaseCount=7", samples[0])
	}
	if samples[1].NodeID != "node-b" || samples[1].ObservedAtIndex != 200 {
		t.Errorf("remote sample = %+v, want NodeID=node-b ObservedAtIndex=200", samples[1])
	}
}

func TestProductionPeerKEKProbe_DiskSpaceFanOut_PropagatesDialerError(t *testing.T) {
	rcr := &fakeRaftConfigReader{voters: []string{"node-a"}, configIndex: 5}
	dialer := &fakeProbeDialer{
		diskFn: func(context.Context, string, []byte) ([]byte, error) {
			return nil, errors.New("conn refused")
		},
	}
	probe := NewPeerKEKProbe(dialer, rcr, "node-self",
		func() (KEKDiskSpaceResp, error) { return KEKDiskSpaceResp{}, nil },
		func(uint32) (KEKLeaseSnapshotResp, error) { return KEKLeaseSnapshotResp{}, nil },
	)
	_, err := probe.ProbeAllKEKDiskSpace(context.Background())
	if err == nil {
		t.Fatalf("expected dialer error, got nil")
	}
}
