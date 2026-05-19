package cluster

import (
	"crypto/rand"
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
