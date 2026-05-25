package serveruntime

import (
	"context"
	"errors"
	"testing"
)

func TestProductionBootPhasesExposeCriticalOrdering(t *testing.T) {
	phases := productionBootPhases(bootRuntimeDeps{})
	names := make([]string, 0, len(phases))
	for _, phase := range phases {
		names = append(names, phase.name)
	}

	assertBeforePhase(t, names, "group-raft-mux", "data-raft-node")
	assertBeforePhase(t, names, "data-raft-node", "meta-raft-wiring")
	assertBeforePhase(t, names, "meta-raft-start", "shard-service")
	assertBeforePhase(t, names, "http-server-admin", "tls-posture-gate")
	assertBeforePhase(t, names, "tls-posture-gate", "phase0-banner")
	assertBeforePhase(t, names, "node-services", "join-complete-cleanup")
}

func TestRunBootPhasesStopsAtFirstError(t *testing.T) {
	wantErr := errors.New("stop here")
	var ran []string

	err := runBootPhases(context.Background(), &bootState{}, []bootPhase{
		{name: "first", run: func(context.Context, *bootState) error {
			ran = append(ran, "first")
			return nil
		}},
		{name: "second", run: func(context.Context, *bootState) error {
			ran = append(ran, "second")
			return wantErr
		}},
		{name: "third", run: func(context.Context, *bootState) error {
			ran = append(ran, "third")
			return nil
		}},
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("runBootPhases error = %v, want %v", err, wantErr)
	}
	if got, want := len(ran), 2; got != want {
		t.Fatalf("ran %d phases (%v), want %d", got, ran, want)
	}
}

func assertBeforePhase(t *testing.T, names []string, before, after string) {
	t.Helper()
	beforeIdx := phaseIndex(names, before)
	afterIdx := phaseIndex(names, after)
	if beforeIdx < 0 {
		t.Fatalf("phase %q missing from production boot phases: %v", before, names)
	}
	if afterIdx < 0 {
		t.Fatalf("phase %q missing from production boot phases: %v", after, names)
	}
	if beforeIdx >= afterIdx {
		t.Fatalf("phase %q must run before %q: %v", before, after, names)
	}
}

func phaseIndex(names []string, name string) int {
	for i, got := range names {
		if got == name {
			return i
		}
	}
	return -1
}
