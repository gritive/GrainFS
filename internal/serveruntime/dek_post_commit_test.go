package serveruntime

import (
	"context"
	"crypto/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// fakeDEKProposer records calls for assertion.
type fakeDEKProposer struct {
	rotateCalls atomic.Int32
	pruneCalls  atomic.Int32
	pruneGen    atomic.Uint32
}

func (p *fakeDEKProposer) ProposeDEKRotate(_ context.Context) error {
	p.rotateCalls.Add(1)
	return nil
}

func (p *fakeDEKProposer) ProposeDEKVersionPrune(_ context.Context, gen uint32) error {
	p.pruneCalls.Add(1)
	p.pruneGen.Store(gen)
	return nil
}

// waitFor polls cond until it returns true or the deadline is exceeded.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDispatcher_RotateConfigTriggersPropose(t *testing.T) {
	p := &fakeDEKProposer{}
	d := &dekPostCommitDispatcher{proposer: p}

	payload, err := cluster.EncodeConfigPutPayload("encryption.rotate-dek", "now")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	d.Handle(clusterpb.MetaCmdTypeConfigPut, payload)

	waitFor(t, 500*time.Millisecond, func() bool { return p.rotateCalls.Load() == 1 })
}

func TestDispatcher_PruneConfigTriggersPropose(t *testing.T) {
	p := &fakeDEKProposer{}
	d := &dekPostCommitDispatcher{proposer: p}

	payload, err := cluster.EncodeConfigPutPayload("encryption.prune-dek-version", "3")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	d.Handle(clusterpb.MetaCmdTypeConfigPut, payload)

	waitFor(t, 500*time.Millisecond, func() bool { return p.pruneCalls.Load() == 1 })
	if g := p.pruneGen.Load(); g != 3 {
		t.Fatalf("pruneGen = %d, want 3", g)
	}
}

func TestDispatcher_DEKRotate_KicksScrubber(t *testing.T) {
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	keeper, err := encrypt.NewDEKKeeper(kek)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	// Rotate so active gen=1; oldGen = 0.
	if err := keeper.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	var called atomic.Int32
	d := &dekPostCommitDispatcher{
		keeper: keeper,
		scrubberKick: func(_ context.Context, oldGen uint32) {
			called.Add(1)
		},
	}

	d.Handle(clusterpb.MetaCmdTypeDEKRotate, nil)

	waitFor(t, 500*time.Millisecond, func() bool { return called.Load() == 1 })
}

func TestDispatcher_UnrelatedConfigKey_NoOp(t *testing.T) {
	p := &fakeDEKProposer{}
	d := &dekPostCommitDispatcher{proposer: p}

	payload, err := cluster.EncodeConfigPutPayload("audit.deny-only", "true")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	d.Handle(clusterpb.MetaCmdTypeConfigPut, payload)

	time.Sleep(100 * time.Millisecond)
	if p.rotateCalls.Load() != 0 || p.pruneCalls.Load() != 0 {
		t.Fatal("unrelated key triggered propose")
	}
}

func TestDispatcher_NilScrubberKick_NoOp(t *testing.T) {
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

	// scrubberKick is nil — must not panic.
	d := &dekPostCommitDispatcher{keeper: keeper}
	d.Handle(clusterpb.MetaCmdTypeDEKRotate, nil)
	time.Sleep(50 * time.Millisecond)
}
