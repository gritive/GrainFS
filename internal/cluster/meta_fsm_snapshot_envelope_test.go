package cluster

import (
	"bytes"
	"testing"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestMetaFSMSnapshotRestoreRoundTripEnveloped seals a snapshot on one FSM and
// restores it on a fresh FSM sharing the same KEK store + cluster id, asserting
// (a) the on-disk bytes are enveloped (GSNE magic), (b) the restored FSM has the
// seeded state, and (c) a recognizable plaintext key does NOT leak into the
// sealed bytes (confidentiality).
func TestMetaFSMSnapshotRestoreRoundTripEnveloped(t *testing.T) {
	const secretBucket = "top-secret-bucket-zzz"

	src := NewMetaFSM()
	wireTestKEK(t, src)
	if err := src.applyCmd(makeAddNodeCmd(t, "node-1", "addr-1:7001", 0)); err != nil {
		t.Fatalf("add node: %v", err)
	}
	if err := src.applyCmd(makePutShardGroupCmd(t, "group-0", []string{"node-1"})); err != nil {
		t.Fatalf("put shard group: %v", err)
	}
	if err := src.applyCmd(makePutBucketAssignmentCmd(t, secretBucket, "group-0")); err != nil {
		t.Fatalf("put bucket assignment: %v", err)
	}

	snapBytes, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// (a) on-disk bytes are enveloped.
	if !bytes.HasPrefix(snapBytes, []byte("GSNE")) {
		t.Fatalf("snapshot is not GSNE-enveloped; prefix=%q", snapBytes[:4])
	}
	// (c) the recognizable plaintext must not appear in the sealed bytes.
	if bytes.Contains(snapBytes, []byte(secretBucket)) {
		t.Fatalf("plaintext bucket name leaked into sealed snapshot")
	}

	// (b) restore on a fresh FSM sharing the same KEK + cluster id.
	dst := NewMetaFSM()
	wireTestKEK(t, dst)
	if err := dst.Restore(raft.SnapshotMeta{}, snapBytes); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := dst.BucketAssignments()[secretBucket]; got != "group-0" {
		t.Fatalf("restored FSM bucket assignment %q = %q, want group-0", secretBucket, got)
	}
	nodes := dst.Nodes()
	if len(nodes) != 1 || nodes[0].ID != "node-1" {
		t.Fatalf("restored FSM nodes = %+v, want one node-1", nodes)
	}
}

func TestFSMSealOpenSnapshotEnvelopeRoundTrip(t *testing.T) {
	fsm, _ := newTestMetaFSMWithKEKAndDEK(t)
	body := []byte("plaintext fsm blob")
	sealed, err := fsm.sealSnapshotEnvelope(body)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	plain, err := fsm.openSnapshotEnvelope(sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if string(plain) != string(body) {
		t.Fatalf("round-trip mismatch: got %q", plain)
	}
}
