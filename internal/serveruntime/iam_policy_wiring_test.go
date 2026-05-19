package serveruntime

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
)

func TestWireIAMPolicyStores_SeedsBuiltinsAndInjects(t *testing.T) {
	fsm := cluster.NewMetaFSM()
	s, err := WireIAMPolicyStores(context.Background(), fsm, 0 /* default TTL */)
	if err != nil {
		t.Fatalf("WireIAMPolicyStores: %v", err)
	}
	if s == nil || s.Policies == nil || s.Resolver == nil || s.Adapter == nil {
		t.Fatal("bundle has nil fields")
	}
	// All four built-ins seeded with builtin=true.
	for _, name := range []string{"readonly", "readwrite", "writeonly", "bucket-admin"} {
		if !s.Policies.IsBuiltin(name) {
			t.Errorf("builtin %q not seeded as builtin", name)
		}
	}
}

func TestWireIAMPolicyStores_NilFSM(t *testing.T) {
	// Nil FSM is allowed (e.g. unit tests that only need the bundle).
	s, err := WireIAMPolicyStores(context.Background(), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if s == nil {
		t.Fatal("bundle is nil")
	}
}
