package cluster

import (
	"crypto/rand"
	"encoding/binary"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
)

// buildPutObjectIndexCmd returns a FlatBuffers-encoded MetaCmd for PutObjectIndex
// with the given DekGen stamped on the entry.
func buildPutObjectIndexCmdWithDekGen(t *testing.T, bucket, key, versionID, placementGroupID string, dekGen uint32) []byte {
	t.Helper()
	entry := ObjectIndexEntry{
		Bucket:           bucket,
		Key:              key,
		VersionID:        versionID,
		PlacementGroupID: placementGroupID,
		Size:             1024,
		ETag:             "abc123",
		DekGen:           dekGen,
	}
	data, err := encodeMetaPutObjectIndexCmd(entry, false)
	if err != nil {
		t.Fatalf("encodeMetaPutObjectIndexCmd: %v", err)
	}
	return buildMetaCmd(t, clusterpb.MetaCmdTypePutObjectIndex, data)
}

func buildDeleteObjectIndexCmd(t *testing.T, bucket, key, versionID string) []byte {
	t.Helper()
	data, err := encodeMetaDeleteObjectIndexCmd(bucket, key, versionID)
	if err != nil {
		t.Fatalf("encodeMetaDeleteObjectIndexCmd: %v", err)
	}
	return buildMetaCmd(t, clusterpb.MetaCmdTypeDeleteObjectIndex, data)
}

func TestDEKRefCount_WriteIncrements(t *testing.T) {
	fsm := NewMetaFSM()
	cmd := buildPutObjectIndexCmdWithDekGen(t, "bkt", "k1", "v1", "pg1", 3)
	if err := fsm.applyCmd(cmd); err != nil {
		t.Fatalf("applyCmd PutObjectIndex: %v", err)
	}
	if got := fsm.dekRefCount(3); got != 1 {
		t.Fatalf("dekRefCount(3) = %d, want 1", got)
	}
}

func TestDEKRefCount_DeleteDecrements(t *testing.T) {
	fsm := NewMetaFSM()
	// First write
	cmd := buildPutObjectIndexCmdWithDekGen(t, "bkt", "k1", "v1", "pg1", 2)
	if err := fsm.applyCmd(cmd); err != nil {
		t.Fatalf("applyCmd PutObjectIndex: %v", err)
	}
	if got := fsm.dekRefCount(2); got != 1 {
		t.Fatalf("after write, dekRefCount(2) = %d, want 1", got)
	}
	// Delete
	delCmd := buildDeleteObjectIndexCmd(t, "bkt", "k1", "v1")
	if err := fsm.applyCmd(delCmd); err != nil {
		t.Fatalf("applyCmd DeleteObjectIndex: %v", err)
	}
	if got := fsm.dekRefCount(2); got != 0 {
		t.Fatalf("after delete, dekRefCount(2) = %d, want 0", got)
	}
}

func TestDEKRefCount_PersistsInSnapshot(t *testing.T) {
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	keeper, err := encrypt.NewDEKKeeper(kek)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}

	fsm1 := newTestMetaFSMWithDEKKeeper(t, keeper)
	// Write two entries with different gens
	cmd1 := buildPutObjectIndexCmdWithDekGen(t, "bkt", "k1", "v1", "pg1", 0)
	cmd2 := buildPutObjectIndexCmdWithDekGen(t, "bkt", "k2", "v2", "pg1", 0)
	cmd3 := buildPutObjectIndexCmdWithDekGen(t, "bkt", "k3", "v3", "pg1", 0)
	for _, cmd := range [][]byte{cmd1, cmd2, cmd3} {
		if err := fsm1.applyCmd(cmd); err != nil {
			t.Fatalf("applyCmd: %v", err)
		}
	}
	if got := fsm1.dekRefCount(0); got != 3 {
		t.Fatalf("before snapshot, dekRefCount(0) = %d, want 3", got)
	}

	snapBytes, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restore into a fresh FSM with the same keeper
	kek2 := make([]byte, 32)
	copy(kek2, kek)
	keeper2, err := encrypt.LoadFromFSM(kek2, keeper.Versions())
	if err != nil {
		t.Fatalf("LoadFromFSM: %v", err)
	}
	fsm2 := newTestMetaFSMWithDEKKeeper(t, keeper2)
	if err := fsm2.Restore(raft.SnapshotMeta{}, snapBytes); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if got := fsm2.dekRefCount(0); got != 3 {
		t.Fatalf("after restore, dekRefCount(0) = %d, want 3", got)
	}
}

// TestDEKRefCount_RebuildsFromObjectIndexWhenTrailerMissing verifies that when a
// pre-Task-12 snapshot is restored (DKVS trailer present but no ref_counts field),
// dekRefCounts is rebuilt from the restored objectIndex rather than left empty.
// An empty dekRefCounts would allow DEKVersionPrune(0) to silently corrupt objects.
func TestDEKRefCount_RebuildsFromObjectIndexWhenTrailerMissing(t *testing.T) {
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	keeper, err := encrypt.NewDEKKeeper(kek)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}

	// Build an FSM with some objects (all dekGen=0 — legacy).
	fsm1 := newTestMetaFSMWithDEKKeeper(t, keeper)
	for i, key := range []string{"k1", "k2", "k3"} {
		cmd := buildPutObjectIndexCmdWithDekGen(t, "bkt", key, "v1", "pg1", 0)
		if err := fsm1.applyCmd(cmd); err != nil {
			t.Fatalf("applyCmd [%d]: %v", i, err)
		}
	}

	// Take a real snapshot (which includes ref_counts).
	snapBytes, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Strip the real DKVS trailer and replace it with a legacy one (nil refCounts).
	// This mimics what a pre-Task-12 node would have written.
	if len(snapBytes) < dekSnapshotTrailerLen {
		t.Fatal("snapshot too short to contain DKVS trailer")
	}
	dekFooter := snapBytes[len(snapBytes)-dekSnapshotTrailerLen:]
	if binary.LittleEndian.Uint32(dekFooter[4:8]) != dekSnapshotTrailerMagic {
		t.Fatal("snapshot does not end with DKVS magic")
	}
	dekLen := binary.LittleEndian.Uint32(dekFooter[0:4])
	dekEnd := len(snapBytes) - dekSnapshotTrailerLen
	dekStart := dekEnd - int(dekLen)
	base := snapBytes[:dekStart] // snapshot without any DKVS trailer

	// Re-encode DKVS without ref_counts (nil → pre-Task-12 format).
	versions := keeper.Versions()
	active, _ := keeper.Active()
	legacyDEKPayload, err := encodeMetaDEKVersionSnapshot(versions, active, nil)
	if err != nil {
		t.Fatalf("encodeMetaDEKVersionSnapshot (legacy): %v", err)
	}
	var legacyFooter [dekSnapshotTrailerLen]byte
	binary.LittleEndian.PutUint32(legacyFooter[0:4], uint32(len(legacyDEKPayload)))
	binary.LittleEndian.PutUint32(legacyFooter[4:8], dekSnapshotTrailerMagic)
	legacySnap := append(base, legacyDEKPayload...)
	legacySnap = append(legacySnap, legacyFooter[:]...)

	// Restore from the legacy snapshot.
	keeper2, err := encrypt.LoadFromFSM(kek, versions)
	if err != nil {
		t.Fatalf("LoadFromFSM: %v", err)
	}
	fsm2 := newTestMetaFSMWithDEKKeeper(t, keeper2)
	if err := fsm2.Restore(raft.SnapshotMeta{}, legacySnap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// dekRefCount(0) must equal the number of objects (3), not 0.
	if got := fsm2.dekRefCount(0); got != 3 {
		t.Fatalf("after legacy restore, dekRefCount(0) = %d, want 3 (rebuilt from objectIndex)", got)
	}
}

func TestEncryptedRecord_MigratePreRewriteAssignsGenZero(t *testing.T) {
	// Build a MetaObjectIndexEntry WITHOUT setting dek_gen (pre-Task-12 format).
	// FlatBuffers default for missing uint32 field is 0.
	entry := ObjectIndexEntry{
		Bucket:           "bkt",
		Key:              "k1",
		VersionID:        "v1",
		PlacementGroupID: "pg1",
		Size:             512,
		ETag:             "deadbeef",
		// DekGen deliberately omitted (zero-value = gen 0 = legacy)
	}
	data, err := encodeMetaPutObjectIndexCmd(entry, false)
	if err != nil {
		t.Fatalf("encodeMetaPutObjectIndexCmd: %v", err)
	}
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypePutObjectIndex, data)

	fsm := NewMetaFSM()
	if err := fsm.applyCmd(cmd); err != nil {
		t.Fatalf("applyCmd: %v", err)
	}

	e, ok := fsm.objectIndex[objectIndexVersionKey("bkt", "k1", "v1")]
	if !ok {
		t.Fatal("entry not found in objectIndex")
	}
	if e.DekGen != 0 {
		t.Fatalf("DekGen = %d, want 0 (legacy/pre-rewrite)", e.DekGen)
	}
}
