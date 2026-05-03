package cluster

import (
	"testing"
)

func TestRotationFSM_BeginFromSteady(t *testing.T) {
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1, 2, 3})

	rotID := [16]byte{0xa, 0xb, 0xc}
	newSPKI := [32]byte{4, 5, 6}
	cmd := RotateKeyBegin{
		RotationID:      rotID,
		ExpectedNewSPKI: newSPKI,
		StartedBy:       "leader-node",
		Capabilities:    CapRotationV1,
	}
	if err := fsm.Apply(cmd); err != nil {
		t.Fatal(err)
	}
	st := fsm.State()
	if st.Phase != PhaseBegun {
		t.Fatalf("phase: want %d, got %d", PhaseBegun, st.Phase)
	}
	if st.NewSPKI != newSPKI {
		t.Fatal("NewSPKI not stored")
	}
	if st.RotationID != rotID {
		t.Fatal("RotationID not stored")
	}
}

func TestRotationFSM_DuplicateBeginSameRotID_NoOp(t *testing.T) {
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	rotID := [16]byte{0xa}
	cmd := RotateKeyBegin{RotationID: rotID, ExpectedNewSPKI: [32]byte{2}, Capabilities: CapRotationV1}

	if err := fsm.Apply(cmd); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(cmd); err != nil {
		t.Fatalf("duplicate same-rotID should be no-op, got %v", err)
	}
}

func TestRotationFSM_DifferentRotIDDuringActive_Rejected(t *testing.T) {
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	cmd1 := RotateKeyBegin{RotationID: [16]byte{0xa}, ExpectedNewSPKI: [32]byte{2}, Capabilities: CapRotationV1}
	if err := fsm.Apply(cmd1); err != nil {
		t.Fatal(err)
	}
	cmd2 := RotateKeyBegin{RotationID: [16]byte{0xb}, ExpectedNewSPKI: [32]byte{3}, Capabilities: CapRotationV1}
	if err := fsm.Apply(cmd2); err == nil {
		t.Fatal("second rotation while active should be rejected")
	}
}

func TestRotationFSM_SwitchAndDrop(t *testing.T) {
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	rotID := [16]byte{0xa}
	if err := fsm.Apply(RotateKeyBegin{RotationID: rotID, ExpectedNewSPKI: [32]byte{2}, Capabilities: CapRotationV1}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(RotateKeySwitch{RotationID: rotID}); err != nil {
		t.Fatal(err)
	}
	if fsm.State().Phase != PhaseSwitched {
		t.Fatal("not switched")
	}
	if err := fsm.Apply(RotateKeyDrop{RotationID: rotID, GraceUntil: 99}); err != nil {
		t.Fatal(err)
	}
	st := fsm.State()
	if st.Phase != PhaseSteady {
		t.Fatalf("post-drop should be steady, got phase %d", st.Phase)
	}
	if st.OldSPKI != ([32]byte{2}) {
		t.Fatalf("OldSPKI after drop should be the new key, got %v", st.OldSPKI)
	}
	if st.GraceUntil != 99 {
		t.Fatalf("GraceUntil not recorded, got %d", st.GraceUntil)
	}
}

func TestRotationFSM_AbortPhase2_RollsBack(t *testing.T) {
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	rotID := [16]byte{0xa}
	if err := fsm.Apply(RotateKeyBegin{RotationID: rotID, ExpectedNewSPKI: [32]byte{2}, Capabilities: CapRotationV1}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(RotateKeyAbort{RotationID: rotID, Reason: "operator"}); err != nil {
		t.Fatal(err)
	}
	st := fsm.State()
	if st.Phase != PhaseSteady {
		t.Fatal("abort from phase 2 should return to steady on OLD")
	}
	if st.OldSPKI != ([32]byte{1}) {
		t.Fatal("OldSPKI should remain the original (1)")
	}
	if st.NewSPKI != ([32]byte{}) {
		t.Fatal("NewSPKI should be cleared")
	}
}

func TestRotationFSM_AbortPhase3_ForwardRolls(t *testing.T) {
	// D18: phase 3 abort = forward-roll (continue to phase 4 NEW).
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	rotID := [16]byte{0xa}
	if err := fsm.Apply(RotateKeyBegin{RotationID: rotID, ExpectedNewSPKI: [32]byte{2}, Capabilities: CapRotationV1}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(RotateKeySwitch{RotationID: rotID}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(RotateKeyAbort{RotationID: rotID, Reason: "operator"}); err != nil {
		t.Fatal(err)
	}
	st := fsm.State()
	if st.Phase != PhaseSteady {
		t.Fatalf("phase 3 abort should forward-roll to steady, got %d", st.Phase)
	}
	if st.OldSPKI != ([32]byte{2}) {
		t.Fatalf("forward-roll: active should be NEW, got %v", st.OldSPKI)
	}
}

func TestRotationFSM_BeginRejectedWithoutCapability(t *testing.T) {
	// D9 revised: rotation requires CapRotationV1.
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	cmd := RotateKeyBegin{RotationID: [16]byte{0xa}, ExpectedNewSPKI: [32]byte{2}, Capabilities: 0}
	if err := fsm.Apply(cmd); err == nil {
		t.Fatal("Begin without capability should be rejected")
	}
}

func TestRotationFSM_AppliedByTrackedPerRotation(t *testing.T) {
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	rotID := [16]byte{0xa}
	fsm.Apply(RotateKeyBegin{RotationID: rotID, ExpectedNewSPKI: [32]byte{2}, Capabilities: CapRotationV1})

	fsm.RecordAck("peer-1", RotationApplyRecord{RotationID: rotID, Phase: PhaseBegun})
	fsm.RecordAck("peer-2", RotationApplyRecord{RotationID: rotID, Phase: PhaseBegun})

	st := fsm.State()
	if len(st.AppliedBy) != 2 {
		t.Fatalf("want 2 acks, got %d", len(st.AppliedBy))
	}
	if st.AppliedBy["peer-1"].Phase != PhaseBegun {
		t.Fatal("peer-1 ack not recorded")
	}

	// New rotation resets AppliedBy.
	fsm.Apply(RotateKeyAbort{RotationID: rotID, Reason: "operator"})
	rotID2 := [16]byte{0xb}
	fsm.Apply(RotateKeyBegin{RotationID: rotID2, ExpectedNewSPKI: [32]byte{3}, Capabilities: CapRotationV1})
	st = fsm.State()
	if len(st.AppliedBy) != 0 {
		t.Fatalf("new rotation should reset AppliedBy, got %d entries", len(st.AppliedBy))
	}
}

func TestRotationFSM_SwitchRotIDMismatch_Rejected(t *testing.T) {
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	rotID := [16]byte{0xa}
	fsm.Apply(RotateKeyBegin{RotationID: rotID, ExpectedNewSPKI: [32]byte{2}, Capabilities: CapRotationV1})

	wrongID := [16]byte{0xc}
	if err := fsm.Apply(RotateKeySwitch{RotationID: wrongID}); err == nil {
		t.Fatal("Switch with wrong RotationID should be rejected (D15 idempotency)")
	}
}
