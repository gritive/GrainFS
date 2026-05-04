package cluster

import (
	"errors"
	"testing"
	"time"
)

// fakePrevDeleter records DeletePrevious calls so we can assert they happened
// (or didn't) without a real keystore.
type fakePrevDeleter struct {
	calls int
	err   error
}

func (f *fakePrevDeleter) DeletePrevious() error {
	f.calls++
	return f.err
}

// runOneCleanupTick mirrors the goroutine body for ONE iteration so we don't
// have to spin up timers. Tests this is the actual decision logic the
// goroutine runs each tick.
func runOneCleanupTick(fsm *RotationFSM, ks PreviousKeyDeleter, lastDeletedFor *int64) {
	st := fsm.State()
	if st.GraceUntil == 0 || st.GraceUntil == *lastDeletedFor {
		return
	}
	if time.Now().UnixNano() < st.GraceUntil {
		return
	}
	if err := ks.DeletePrevious(); err != nil {
		return
	}
	*lastDeletedFor = st.GraceUntil
}

func TestPrevKeyCleanup_NoActionWhenGraceNotSet(t *testing.T) {
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	ks := &fakePrevDeleter{}
	var last int64
	runOneCleanupTick(fsm, ks, &last)
	if ks.calls != 0 {
		t.Fatalf("no GraceUntil set → no DeletePrevious; got %d calls", ks.calls)
	}
}

func TestPrevKeyCleanup_NoActionBeforeGrace(t *testing.T) {
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	// Future GraceUntil — not yet expired.
	rotID := [16]byte{0xa}
	fsm.Apply(RotateKeyBegin{RotationID: rotID, ExpectedNewSPKI: [32]byte{2}, Capabilities: CapRotationV1})
	fsm.Apply(RotateKeySwitch{RotationID: rotID})
	future := time.Now().Add(time.Hour).UnixNano()
	fsm.Apply(RotateKeyDrop{RotationID: rotID, GraceUntil: future})

	ks := &fakePrevDeleter{}
	var last int64
	runOneCleanupTick(fsm, ks, &last)
	if ks.calls != 0 {
		t.Fatalf("grace not yet expired → no DeletePrevious; got %d calls", ks.calls)
	}
}

func TestPrevKeyCleanup_DeletesAfterGraceExpires(t *testing.T) {
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	rotID := [16]byte{0xa}
	fsm.Apply(RotateKeyBegin{RotationID: rotID, ExpectedNewSPKI: [32]byte{2}, Capabilities: CapRotationV1})
	fsm.Apply(RotateKeySwitch{RotationID: rotID})
	// Past GraceUntil — already expired.
	past := time.Now().Add(-time.Minute).UnixNano()
	fsm.Apply(RotateKeyDrop{RotationID: rotID, GraceUntil: past})

	ks := &fakePrevDeleter{}
	var last int64
	runOneCleanupTick(fsm, ks, &last)
	if ks.calls != 1 {
		t.Fatalf("grace expired → exactly one DeletePrevious; got %d", ks.calls)
	}
	if last != past {
		t.Fatalf("lastDeletedFor should record the GraceUntil we acted on")
	}
}

func TestPrevKeyCleanup_IdempotentOnRepeatedTicks(t *testing.T) {
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	rotID := [16]byte{0xa}
	fsm.Apply(RotateKeyBegin{RotationID: rotID, ExpectedNewSPKI: [32]byte{2}, Capabilities: CapRotationV1})
	fsm.Apply(RotateKeySwitch{RotationID: rotID})
	past := time.Now().Add(-time.Minute).UnixNano()
	fsm.Apply(RotateKeyDrop{RotationID: rotID, GraceUntil: past})

	ks := &fakePrevDeleter{}
	var last int64
	runOneCleanupTick(fsm, ks, &last)
	runOneCleanupTick(fsm, ks, &last)
	runOneCleanupTick(fsm, ks, &last)
	if ks.calls != 1 {
		t.Fatalf("multiple ticks for same GraceUntil → still one DeletePrevious; got %d", ks.calls)
	}
}

func TestPrevKeyCleanup_RetriesOnError(t *testing.T) {
	fsm := NewRotationFSM()
	fsm.SetSteady([32]byte{1})
	rotID := [16]byte{0xa}
	fsm.Apply(RotateKeyBegin{RotationID: rotID, ExpectedNewSPKI: [32]byte{2}, Capabilities: CapRotationV1})
	fsm.Apply(RotateKeySwitch{RotationID: rotID})
	past := time.Now().Add(-time.Minute).UnixNano()
	fsm.Apply(RotateKeyDrop{RotationID: rotID, GraceUntil: past})

	ks := &fakePrevDeleter{err: errors.New("ENOSPC")}
	var last int64
	runOneCleanupTick(fsm, ks, &last)
	runOneCleanupTick(fsm, ks, &last)
	// Errors don't update lastDeletedFor, so we should retry.
	if ks.calls != 2 {
		t.Fatalf("error → retry next tick; got %d calls", ks.calls)
	}
	if last != 0 {
		t.Fatalf("error → lastDeletedFor stays 0; got %d", last)
	}

	// Now succeed; should advance lastDeletedFor and stop retrying.
	ks.err = nil
	runOneCleanupTick(fsm, ks, &last)
	if ks.calls != 3 {
		t.Fatalf("recovery success → one more call; got %d total", ks.calls)
	}
	runOneCleanupTick(fsm, ks, &last)
	if ks.calls != 3 {
		t.Fatal("after success no more retries")
	}
}
