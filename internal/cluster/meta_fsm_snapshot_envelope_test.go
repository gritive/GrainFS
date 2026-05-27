package cluster

import (
	"bytes"
	"crypto/rand"
	"errors"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
)

// newTestMetaFSMSharingKEK builds a MetaFSM wired with the given KEKStore and
// the canonical byte(i+1) test cluster id (matching wireTestKEK), so multiple
// FSMs can seal/open one another's snapshot envelopes.
func newTestMetaFSMSharingKEK(t *testing.T, store *encrypt.KEKStore) *MetaFSM {
	t.Helper()
	var clusterID [16]byte
	for i := range clusterID {
		clusterID[i] = byte(i + 1)
	}
	f := NewMetaFSM()
	f.SetClusterID(clusterID[:])
	f.SetKEKStore(store)
	return f
}

// TestMetaFSMSnapshotRestoreAcrossKEKRotation pins openSnapshotEnvelope's
// by-version KEK resolution: a snapshot sealed under KEK version v must restore
// after the active version advances to v+1 (v retained), and must fail loudly
// when version v is absent from the restore-target's KEK store.
func TestMetaFSMSnapshotRestoreAcrossKEKRotation(t *testing.T) {
	t.Run("active KEK advanced, old version retained", func(t *testing.T) {
		k0 := bytes.Repeat([]byte{0xA0}, encrypt.KEKSize)
		store := encrypt.NewKEKStore()
		if err := store.Add(0, k0); err != nil {
			t.Fatalf("seed KEKStore v0: %v", err)
		}

		src := newTestMetaFSMSharingKEK(t, store)
		if err := src.applyCmd(makeAddNodeCmd(t, "node-1", "addr-1:7001", 0)); err != nil {
			t.Fatalf("add node: %v", err)
		}
		// Seal under the active KEK (version 0).
		sealed, err := src.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}

		// Advance the shared KEK store to version 1, retaining version 0.
		k1 := make([]byte, encrypt.KEKSize)
		if _, err := rand.Read(k1); err != nil {
			t.Fatalf("rand k1: %v", err)
		}
		if err := store.Add(1, k1); err != nil {
			t.Fatalf("add KEK v1: %v", err)
		}
		if err := store.SetActiveVersion(1); err != nil {
			t.Fatalf("SetActiveVersion(1): %v", err)
		}

		// Restore the ORIGINAL (v0-sealed) bytes on a fresh FSM sharing the same
		// (now v1-active, v0-retained) store + cluster id.
		dst := newTestMetaFSMSharingKEK(t, store)
		if err := dst.Restore(raft.SnapshotMeta{}, sealed); err != nil {
			t.Fatalf("Restore across rotation must succeed (open resolves v0): %v", err)
		}
		nodes := dst.Nodes()
		if len(nodes) != 1 || nodes[0].ID != "node-1" {
			t.Fatalf("restored FSM nodes = %+v, want one node-1", nodes)
		}
	})

	t.Run("sealing KEK version absent", func(t *testing.T) {
		k0 := bytes.Repeat([]byte{0xA0}, encrypt.KEKSize)
		srcStore := encrypt.NewKEKStore()
		if err := srcStore.Add(0, k0); err != nil {
			t.Fatalf("seed src KEKStore v0: %v", err)
		}
		src := newTestMetaFSMSharingKEK(t, srcStore)
		sealed, err := src.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}

		// Restore-target store has only version 1 — the sealing version 0 is absent.
		k1 := make([]byte, encrypt.KEKSize)
		if _, err := rand.Read(k1); err != nil {
			t.Fatalf("rand k1: %v", err)
		}
		dstStore := encrypt.NewKEKStore()
		if err := dstStore.Add(1, k1); err != nil {
			t.Fatalf("seed dst KEKStore v1: %v", err)
		}
		if err := dstStore.SetActiveVersion(1); err != nil {
			t.Fatalf("SetActiveVersion(1): %v", err)
		}
		dst := newTestMetaFSMSharingKEK(t, dstStore)

		err = dst.Restore(raft.SnapshotMeta{}, sealed)
		if err == nil {
			t.Fatal("Restore must fail when the sealing KEK version is absent")
		}
		if !strings.Contains(err.Error(), "resolve KEK") {
			t.Fatalf("error %q must mention \"resolve KEK\"", err)
		}
		if !errors.Is(err, encrypt.ErrKEKVersionUnknown) {
			t.Fatalf("error must wrap ErrKEKVersionUnknown, got %v", err)
		}
	})
}

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

// TestMetaFSMSnapshotOpensWithKEKOnly locks the spec §D11 boot-order invariant:
// the snapshot envelope MUST be openable with the KEK store alone, BEFORE the
// data-path DEKKeeper is rebuilt from the DKVS trailer (that trailer lives
// INSIDE the encrypted body and is consumed only AFTER the envelope is opened).
// At boot, wireDEKKeeper wires the KEK store BEFORE metaRaft.Start runs FSM
// Restore; this test proves the envelope does not secretly depend on the
// data-path keeper.
func TestMetaFSMSnapshotOpensWithKEKOnly(t *testing.T) {
	const secretBucket = "kek-only-secret-bucket"

	// Fully-wired source: KEK store + DEKKeeper + clusterID byte(i+1). This
	// produces a production-shaped snapshot whose encrypted body carries a DKVS
	// trailer. Seed OBJECT/bucket/node state only — NO JWT signing keys, so the
	// snapshot has no JKEY trailer (Restore refuses JKEY without a wired keeper).
	src, _ := newTestMetaFSMWithKEKAndDEK(t)
	if err := src.applyCmd(makeAddNodeCmd(t, "node-1", "addr-1:7001", 0)); err != nil {
		t.Fatalf("add node: %v", err)
	}
	if err := src.applyCmd(makePutShardGroupCmd(t, "group-0", []string{"node-1"})); err != nil {
		t.Fatalf("put shard group: %v", err)
	}
	if err := src.applyCmd(makePutBucketAssignmentCmd(t, secretBucket, "group-0")); err != nil {
		t.Fatalf("put bucket assignment: %v", err)
	}

	data, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Target shares the SAME KEK store + cluster id as src but has NO data-path
	// DEKKeeper (mirrors the boot moment after wireDEKKeeper has set the KEK store
	// but before Restore rebuilds the keeper from the DKVS trailer).
	dst := newTestMetaFSMSharingKEK(t, src.KEKStore())
	if dst.KEKStore() == nil {
		t.Fatal("precondition: dst must have a KEK store wired")
	}
	if dst.dekKeeper != nil {
		t.Fatal("precondition: dst must NOT have a data-path DEKKeeper wired")
	}

	// PRIMARY: the envelope opens with KEK-only. This is the precise boot-order
	// invariant — no DEKKeeper needed to reach the plaintext body.
	if !bytes.HasPrefix(data, []byte("GSNE")) {
		t.Fatalf("snapshot is not GSNE-enveloped; prefix=%q", data[:4])
	}
	if _, err := dst.openSnapshotEnvelope(data); err != nil {
		t.Fatalf("openSnapshotEnvelope must succeed with KEK-only (no DEKKeeper): %v", err)
	}

	// SECONDARY: the full Restore path also succeeds with KEK-only, because the
	// snapshot carries no JKEY trailer (the only keeper-dependent Restore branch).
	// The DKVS trailer is decoded into pending fields without touching the keeper.
	if err := dst.Restore(raft.SnapshotMeta{Index: 5, Term: 1}, data); err != nil {
		t.Fatalf("Restore must succeed with KEK-only: %v", err)
	}
	if got := dst.BucketAssignments()[secretBucket]; got != "group-0" {
		t.Fatalf("restored FSM bucket assignment %q = %q, want group-0", secretBucket, got)
	}
	nodes := dst.Nodes()
	if len(nodes) != 1 || nodes[0].ID != "node-1" {
		t.Fatalf("restored FSM nodes = %+v, want one node-1", nodes)
	}
}

// TestMetaFSMRestoreReadsLegacyPlaintextSnapshot proves the Phase D-snap
// read-compat shim: a non-enveloped (plaintext) meta-FSM snapshot — as written
// by a pre-Slice-1 binary on an existing data dir — restores successfully so a
// rolling upgrade does not brick the cluster.
func TestMetaFSMRestoreReadsLegacyPlaintextSnapshot(t *testing.T) {
	const legacyBucket = "legacy-plaintext-bucket-yyy"

	src := NewMetaFSM()
	wireTestKEK(t, src)
	if err := src.applyCmd(makeAddNodeCmd(t, "node-9", "addr-9:7001", 0)); err != nil {
		t.Fatalf("add node: %v", err)
	}
	if err := src.applyCmd(makePutShardGroupCmd(t, "group-9", []string{"node-9"})); err != nil {
		t.Fatalf("put shard group: %v", err)
	}
	if err := src.applyCmd(makePutBucketAssignmentCmd(t, legacyBucket, "group-9")); err != nil {
		t.Fatalf("put bucket assignment: %v", err)
	}

	// Recover the exact plaintext snapshot blob (the inner body) by sealing then
	// opening — this is byte-for-byte what a pre-envelope binary would write.
	sealed, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	plain, err := src.openSnapshotEnvelope(sealed)
	if err != nil {
		t.Fatalf("openSnapshotEnvelope: %v", err)
	}
	if encrypt.IsSnapshotEnvelope(plain) {
		t.Fatal("legacy test blob must NOT carry envelope magic")
	}

	// Restore the PLAINTEXT blob on a fresh FSM. The shim must read it directly.
	dst := NewMetaFSM()
	wireTestKEK(t, dst)
	if err := dst.Restore(raft.SnapshotMeta{Index: 3, Term: 1}, plain); err != nil {
		t.Fatalf("Restore legacy plaintext snapshot must succeed: %v", err)
	}
	if got := dst.BucketAssignments()[legacyBucket]; got != "group-9" {
		t.Fatalf("restored FSM bucket assignment %q = %q, want group-9", legacyBucket, got)
	}
	nodes := dst.Nodes()
	if len(nodes) != 1 || nodes[0].ID != "node-9" {
		t.Fatalf("restored FSM nodes = %+v, want one node-9", nodes)
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
