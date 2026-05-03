package cluster

import (
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
)

// fakeIdentitySwapper captures SwapIdentity calls so we can assert on them
// without spinning a real QUIC transport.
type fakeIdentitySwapper struct {
	calls []*transport.IdentitySnapshot
}

func (f *fakeIdentitySwapper) SwapIdentity(snap *transport.IdentitySnapshot) {
	f.calls = append(f.calls, snap)
}

func TestRotationWorker_PhaseBegun_AddsNewToAccept(t *testing.T) {
	dir := t.TempDir()
	ks := transport.NewKeystore(dir)
	oldKey := strings.Repeat("a", 64)
	newKey := strings.Repeat("b", 64)
	if err := ks.WriteCurrent(oldKey); err != nil {
		t.Fatal(err)
	}
	if err := ks.WriteNext(newKey); err != nil {
		t.Fatal(err)
	}

	swapper := &fakeIdentitySwapper{}
	w := NewRotationWorker(ks, swapper, "node-1")

	_, oldSPKI, _ := transport.DeriveClusterIdentity(oldKey)
	_, newSPKI, _ := transport.DeriveClusterIdentity(newKey)

	rotID := [16]byte{0xa}
	ack := w.OnPhaseChange(RotationState{
		RotationID: rotID,
		Phase:      PhaseBegun,
		OldSPKI:    oldSPKI,
		NewSPKI:    newSPKI,
	})

	if ack.Error != "" {
		t.Fatalf("expected clean ack, got error: %s", ack.Error)
	}
	if ack.Phase != PhaseBegun {
		t.Fatalf("ack phase: want %d, got %d", PhaseBegun, ack.Phase)
	}
	if len(swapper.calls) != 1 {
		t.Fatalf("expected 1 SwapIdentity call, got %d", len(swapper.calls))
	}
	snap := swapper.calls[0]
	if len(snap.AcceptSPKIs) != 2 {
		t.Fatalf("expected 2 accept SPKIs in phase 2, got %d", len(snap.AcceptSPKIs))
	}
	if snap.PresentSPKI != oldSPKI {
		t.Fatal("phase 2 should still present OLD")
	}
}

func TestRotationWorker_NextKeyMissing_AcksWithError(t *testing.T) {
	dir := t.TempDir()
	ks := transport.NewKeystore(dir)
	oldKey := strings.Repeat("a", 64)
	if err := ks.WriteCurrent(oldKey); err != nil {
		t.Fatal(err)
	}

	swapper := &fakeIdentitySwapper{}
	w := NewRotationWorker(ks, swapper, "node-1")

	_, oldSPKI, _ := transport.DeriveClusterIdentity(oldKey)
	rotID := [16]byte{0xa}
	ack := w.OnPhaseChange(RotationState{
		RotationID: rotID,
		Phase:      PhaseBegun,
		OldSPKI:    oldSPKI,
		NewSPKI:    [32]byte{0x99},
	})

	if ack.Error == "" {
		t.Fatal("expected error ack when next.key missing")
	}
	if ack.Phase != -1 {
		t.Fatalf("error ack should set Phase=-1, got %d", ack.Phase)
	}
	if len(swapper.calls) != 0 {
		t.Fatal("no transport swap should occur on disk error")
	}
}

func TestRotationWorker_NextKeySPKIMismatch_AcksWithError(t *testing.T) {
	dir := t.TempDir()
	ks := transport.NewKeystore(dir)
	oldKey := strings.Repeat("a", 64)
	wrongKey := strings.Repeat("c", 64)
	ks.WriteCurrent(oldKey)
	ks.WriteNext(wrongKey)

	swapper := &fakeIdentitySwapper{}
	w := NewRotationWorker(ks, swapper, "node-1")

	_, oldSPKI, _ := transport.DeriveClusterIdentity(oldKey)
	expectedNew := [32]byte{0x99}
	rotID := [16]byte{0xa}
	ack := w.OnPhaseChange(RotationState{
		RotationID: rotID,
		Phase:      PhaseBegun,
		OldSPKI:    oldSPKI,
		NewSPKI:    expectedNew,
	})

	if ack.Error == "" {
		t.Fatal("expected error ack on SPKI mismatch")
	}
	if !strings.Contains(ack.Error, "SPKI mismatch") {
		t.Fatalf("error should mention SPKI mismatch, got %q", ack.Error)
	}
}

func TestRotationWorker_PhaseSwitched_PresentsNew(t *testing.T) {
	dir := t.TempDir()
	ks := transport.NewKeystore(dir)
	oldKey := strings.Repeat("a", 64)
	newKey := strings.Repeat("b", 64)
	ks.WriteCurrent(oldKey)
	ks.WriteNext(newKey)

	swapper := &fakeIdentitySwapper{}
	w := NewRotationWorker(ks, swapper, "node-1")

	_, oldSPKI, _ := transport.DeriveClusterIdentity(oldKey)
	_, newSPKI, _ := transport.DeriveClusterIdentity(newKey)

	rotID := [16]byte{0xa}
	ack := w.OnPhaseChange(RotationState{
		RotationID: rotID,
		Phase:      PhaseSwitched,
		OldSPKI:    oldSPKI,
		NewSPKI:    newSPKI,
	})
	if ack.Error != "" {
		t.Fatalf("clean ack expected, got %q", ack.Error)
	}
	if len(swapper.calls) != 1 {
		t.Fatalf("expected 1 SwapIdentity, got %d", len(swapper.calls))
	}
	snap := swapper.calls[0]
	if snap.PresentSPKI != newSPKI {
		t.Fatal("phase 3 should present NEW")
	}
}

func TestRotationWorker_Steady_PromotesNextToCurrent(t *testing.T) {
	dir := t.TempDir()
	ks := transport.NewKeystore(dir)
	oldKey := strings.Repeat("a", 64)
	newKey := strings.Repeat("b", 64)
	ks.WriteCurrent(oldKey)
	ks.WriteNext(newKey)

	swapper := &fakeIdentitySwapper{}
	w := NewRotationWorker(ks, swapper, "node-1")

	_, newSPKI, _ := transport.DeriveClusterIdentity(newKey)

	// Simulate post-Drop: FSM phase is Steady, OldSPKI is now NEW.
	w.OnPhaseChange(RotationState{
		Phase:   PhaseSteady,
		OldSPKI: newSPKI,
	})

	cur, err := ks.ReadCurrent()
	if err != nil || cur != newKey {
		t.Fatalf("after drop, current.key should be NEW: cur=%q err=%v", cur, err)
	}
	prev, err := ks.ReadPrevious()
	if err != nil || prev != oldKey {
		t.Fatalf("after drop, previous.key should be OLD: prev=%q err=%v", prev, err)
	}
}

func TestRotationWorker_Steady_Phase2AbortDeletesNext(t *testing.T) {
	dir := t.TempDir()
	ks := transport.NewKeystore(dir)
	oldKey := strings.Repeat("a", 64)
	newKey := strings.Repeat("b", 64)
	ks.WriteCurrent(oldKey)
	ks.WriteNext(newKey)

	swapper := &fakeIdentitySwapper{}
	w := NewRotationWorker(ks, swapper, "node-1")

	_, oldSPKI, _ := transport.DeriveClusterIdentity(oldKey)

	// Phase-2 abort: FSM Steady, OldSPKI still OLD (didn't change).
	w.OnPhaseChange(RotationState{
		Phase:   PhaseSteady,
		OldSPKI: oldSPKI,
	})

	cur, _ := ks.ReadCurrent()
	if cur != oldKey {
		t.Fatalf("after phase-2 abort, current.key should still be OLD: cur=%q", cur)
	}
	if _, err := ks.ReadNext(); err == nil {
		t.Fatal("after abort, next.key should be deleted")
	}
}
