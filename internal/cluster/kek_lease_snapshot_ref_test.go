// Package cluster: handler-wire integration test for KEKLeaseSnapshotHandler
// with a real on-disk snapshot file (Task 3, Medium-4).
//
// This test exercises the path the Task-2 leader-refuse unit test could NOT:
// it calls the real KEKLeaseSnapshotHandler (server-side) with a real wire-
// encoded request and decodes the real wire-encoded response, proving that:
//
//  1. encodeKEKLeaseSnapshotReq → decodeKEKLeaseSnapshotReq round-trips.
//  2. KEKLeaseSnapshotHandler.Handle calls snapshotRefCountFn (here wired to
//     the real snapshot.CountSnapshotsSealedUnderKEK).
//  3. The SnapshotRefCount field survives the encode → decode path.
//  4. Version mismatches (v1 snapshot probed at v2) correctly return 0.
//
// No real QUIC transport is needed: the test directly invokes h.Handle with a
// *transport.Message carrying the wire-encoded payload, then decodes the
// response. This is the same path a remote caller traverses; the only skipped
// layer is the TCP/QUIC framing, which is codec-independent.
package cluster

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/transport"
)

// makeTestSnapshotFile writes a snapshot envelope file sealed under the given
// KEK version into dir using a randomly-generated KEK. The body need not be
// valid zstd — CountSnapshotsSealedUnderKEK only peeks the envelope header.
func makeTestSnapshotFile(t *testing.T, dir string, kekVer uint32, seq uint64) {
	t.Helper()
	kek := make([]byte, encrypt.KEKSize)
	for i := range kek {
		kek[i] = byte(kekVer*7 + 0x11) // deterministic, non-zero
	}
	var cid [16]byte
	cid[0] = 0xAB
	var sid [16]byte
	sid[0] = byte(seq)
	// body need not be valid zstd; the scan only reads the plaintext header.
	body := []byte("not-zstd-but-header-only-scan")
	sealed, err := encrypt.SealSnapshotEnvelope(kek, cid[:], sid, kekVer, body)
	if err != nil {
		t.Fatalf("SealSnapshotEnvelope: %v", err)
	}
	name := filepath.Join(dir, "snapshot-"+uint64str(seq)+".json.zst")
	if err := os.WriteFile(name, sealed, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

// uint64str converts a uint64 to its decimal string representation.
func uint64str(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

// TestKEKLeaseSnapshotHandler_WireWithRealScan is the handler-wire integration
// test required by Task 3 (plan Medium-4). It proves that:
//
//   - A snapshot file sealed under KEK v1 is detected by the real
//     snapshot.CountSnapshotsSealedUnderKEK.
//   - The handler encodes the count into the wire response.
//   - The decoded SnapshotRefCount is 1 for v1 and 0 for v2.
func TestKEKLeaseSnapshotHandler_WireWithRealScan(t *testing.T) {
	dir := t.TempDir()

	// Write one object snapshot sealed under KEK version 1.
	makeTestSnapshotFile(t, dir, 1, 1)

	h := NewKEKLeaseSnapshotHandler(
		"node-remote",
		encrypt.NewKEKLeaseTracker(),
		func() uint64 { return 100 },
		func(v uint32) (uint64, error) {
			return snapshot.CountSnapshotsSealedUnderKEK(dir, v)
		},
	)

	// --- v1 probe: expect SnapshotRefCount == 1 ---
	reqV1 := &transport.Message{
		Type:    transport.StreamKEKLeaseSnapshotProbe,
		Payload: encodeKEKLeaseSnapshotReq(KEKLeaseSnapshotReq{Version: 1}),
	}
	respV1 := h.Handle(reqV1)
	if respV1.Status != transport.StatusOK {
		t.Fatalf("v1 probe: handler status=%v payload=%q", respV1.Status, string(respV1.Payload))
	}
	gotV1, err := decodeKEKLeaseSnapshotResp(respV1.Payload)
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

	// --- v2 probe: snapshot is sealed under v1, so count must be 0 ---
	reqV2 := &transport.Message{
		Type:    transport.StreamKEKLeaseSnapshotProbe,
		Payload: encodeKEKLeaseSnapshotReq(KEKLeaseSnapshotReq{Version: 2}),
	}
	respV2 := h.Handle(reqV2)
	if respV2.Status != transport.StatusOK {
		t.Fatalf("v2 probe: handler status=%v payload=%q", respV2.Status, string(respV2.Payload))
	}
	gotV2, err := decodeKEKLeaseSnapshotResp(respV2.Payload)
	if err != nil {
		t.Fatalf("v2 probe: decode response: %v", err)
	}
	if gotV2.SnapshotRefCount != 0 {
		t.Errorf("v2 probe: SnapshotRefCount = %d, want 0 (snapshot sealed under v1)", gotV2.SnapshotRefCount)
	}
}
