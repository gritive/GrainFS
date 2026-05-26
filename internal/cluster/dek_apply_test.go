package cluster

import (
	"crypto/rand"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
)

// newTestMetaFSMWithDEKKeeper builds a minimal MetaFSM with the given DEKKeeper wired.
func newTestMetaFSMWithDEKKeeper(t *testing.T, keeper *encrypt.DEKKeeper) *MetaFSM {
	t.Helper()
	f := NewMetaFSM()
	f.SetDEKKeeper(keeper)
	return f
}

// setDEKReferenceCount is a test-only helper to inject ref counts into the FSM.
// Real per-record ref counting is wired in Task 12.
func (f *MetaFSM) setDEKReferenceCount(gen uint32, count uint64) {
	if f.dekRefCounts == nil {
		f.dekRefCounts = make(map[uint32]uint64)
	}
	f.dekRefCounts[gen] = count
}

// buildMetaDEKVersionPruneCmd builds the FlatBuffers inner payload for DEKVersionPrune.
func buildMetaDEKVersionPruneCmd(t *testing.T, gen uint32) []byte {
	t.Helper()
	data, err := encodeMetaDEKVersionPruneCmd(gen)
	if err != nil {
		t.Fatalf("encodeMetaDEKVersionPruneCmd: %v", err)
	}
	return data
}

func TestApply_DEKRotate_BumpsActiveGen(t *testing.T) {
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	keeper, err := encrypt.NewDEKKeeper(kek)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	fsm := newTestMetaFSMWithDEKKeeper(t, keeper)

	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeDEKRotate, nil)
	if err := fsm.applyCmd(cmd); err != nil {
		t.Fatalf("applyCmd DEKRotate: %v", err)
	}

	gen, _ := keeper.Active()
	if gen != 1 {
		t.Fatalf("active gen = %d, want 1", gen)
	}
}

func TestApply_DEKRotate_NilKeeper_IsNoOp(t *testing.T) {
	fsm := NewMetaFSM() // no DEKKeeper wired
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeDEKRotate, nil)
	if err := fsm.applyCmd(cmd); err != nil {
		t.Fatalf("expected no error with nil DEKKeeper, got: %v", err)
	}
}

func TestApply_DEKVersionPrune_RefusesIfReferenced(t *testing.T) {
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	keeper, err := encrypt.NewDEKKeeper(kek)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	if err := keeper.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	// gen 0 and gen 1 now exist; active is gen 1.
	fsm := newTestMetaFSMWithDEKKeeper(t, keeper)
	fsm.setDEKReferenceCount(0, 5) // injected; real impl in Task 12

	payload := buildMetaDEKVersionPruneCmd(t, 0)
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeDEKVersionPrune, payload)
	if err := fsm.applyCmd(cmd); err == nil {
		t.Fatal("expected refusal while ref > 0, got nil")
	}
}

func TestApply_DEKVersionPrune_SucceedsWhenUnreferenced(t *testing.T) {
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	keeper, err := encrypt.NewDEKKeeper(kek)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	if err := keeper.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	// gen 0 exists and active is gen 1; no ref count injection → ref = 0.
	fsm := newTestMetaFSMWithDEKKeeper(t, keeper)

	payload := buildMetaDEKVersionPruneCmd(t, 0)
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeDEKVersionPrune, payload)
	if err := fsm.applyCmd(cmd); err != nil {
		t.Fatalf("applyCmd DEKVersionPrune: %v", err)
	}

	// gen 0 should be pruned; only gen 1 remains.
	versions := keeper.Versions()
	if _, ok := versions[0]; ok {
		t.Fatal("expected gen 0 pruned, but still present")
	}
	if _, ok := versions[1]; !ok {
		t.Fatal("expected gen 1 still present")
	}
}

func TestSnapshot_DEKVersionTrailerRoundTrip(t *testing.T) {
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	keeper, err := encrypt.NewDEKKeeper(kek)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	// Create gen 0 and gen 1.
	if err := keeper.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	origVersions := keeper.Versions()
	origActive, _ := keeper.Active()

	fsm1 := newTestMetaFSMWithDEKKeeper(t, keeper)
	snapBytes, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restore into a fresh FSM (no keeper wired yet).
	fsm2 := NewMetaFSM()
	if err := fsm2.Restore(raft.SnapshotMeta{}, snapBytes); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	pendingVersions, pendingActive := fsm2.PendingDEKVersions()
	if pendingActive != origActive {
		t.Fatalf("pending active gen = %d, want %d", pendingActive, origActive)
	}
	if len(pendingVersions) != len(origVersions) {
		t.Fatalf("pending versions len = %d, want %d", len(pendingVersions), len(origVersions))
	}
	for gen, origWrapped := range origVersions {
		gotWrapped, ok := pendingVersions[gen]
		if !ok {
			t.Fatalf("gen %d missing from pending versions", gen)
		}
		if string(gotWrapped) != string(origWrapped) {
			t.Fatalf("gen %d wrapped bytes differ", gen)
		}
	}
}

func TestMetaFSM_ActiveKEKVersion_DefaultZero(t *testing.T) {
	fsm := NewMetaFSM()
	if got := fsm.ActiveKEKVersion(); got != 0 {
		t.Errorf("fresh FSM ActiveKEKVersion = %d, want 0", got)
	}
}

func TestMetaFSM_Snapshot_PreservesActiveKEKVersion(t *testing.T) {
	// Round-trip a non-zero active_kek_version through the DKVS trailer.
	// Requires a wired DEKKeeper with ≥1 version; otherwise the trailer is
	// skipped (documented on SetActiveKEKVersion).
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	keeper, err := encrypt.NewDEKKeeper(kek)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}

	fsm1 := newTestMetaFSMWithDEKKeeper(t, keeper)
	fsm1.SetActiveKEKVersion(7)

	snapBytes, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	fsm2 := NewMetaFSM()
	if err := fsm2.Restore(raft.SnapshotMeta{}, snapBytes); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := fsm2.ActiveKEKVersion(); got != 7 {
		t.Errorf("after Restore, ActiveKEKVersion = %d, want 7", got)
	}
}

func TestSnapshot_DEKVersionTrailer_AbsentWhenNoKeeper(t *testing.T) {
	// Snapshot with no DEKKeeper wired should not contain a DKVS trailer.
	// Restore into a fresh FSM should leave PendingDEKVersions empty.
	fsm1 := NewMetaFSM()
	snapBytes, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	fsm2 := NewMetaFSM()
	if err := fsm2.Restore(raft.SnapshotMeta{}, snapBytes); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	versions, _ := fsm2.PendingDEKVersions()
	if len(versions) != 0 {
		t.Fatalf("expected empty pending versions, got %d", len(versions))
	}
}
