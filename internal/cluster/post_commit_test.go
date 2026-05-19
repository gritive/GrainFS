package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/config"
)

func TestApply_FiresPostCommitHook_OnSuccess(t *testing.T) {
	store := config.NewStore()
	config.RegisterClusterKeys(store, config.ReloadHooks{})
	fsm := newTestMetaFSMWithConfigStore(t, store)

	var calls []clusterpb.MetaCmdType
	fsm.RegisterPostCommit(func(ct clusterpb.MetaCmdType, _ []byte) {
		calls = append(calls, ct)
	})

	payload := buildConfigPutPayload(t, "audit.deny-only", "true")
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeConfigPut, payload)
	if err := fsm.applyCmd(cmd); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(calls) != 1 || calls[0] != clusterpb.MetaCmdTypeConfigPut {
		t.Fatalf("hook calls = %v", calls)
	}
}

func TestApply_DoesNotFirePostCommit_OnError(t *testing.T) {
	// config.NewStore() with no registered keys → Set returns ErrUnknownKey
	fsm := newTestMetaFSMWithConfigStore(t, config.NewStore())

	called := false
	fsm.RegisterPostCommit(func(ct clusterpb.MetaCmdType, _ []byte) {
		called = true
	})

	payload := buildConfigPutPayload(t, "no.such.key", "x")
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeConfigPut, payload)
	if err := fsm.applyCmd(cmd); err == nil {
		t.Fatal("expected apply error for unregistered key")
	}
	if called {
		t.Fatal("hook fired despite apply error")
	}
}

func TestApply_MultiplePostCommitHooks_AllFired(t *testing.T) {
	store := config.NewStore()
	config.RegisterClusterKeys(store, config.ReloadHooks{})
	fsm := newTestMetaFSMWithConfigStore(t, store)

	var a, b int
	fsm.RegisterPostCommit(func(_ clusterpb.MetaCmdType, _ []byte) { a++ })
	fsm.RegisterPostCommit(func(_ clusterpb.MetaCmdType, _ []byte) { b++ })

	payload := buildConfigPutPayload(t, "audit.deny-only", "true")
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeConfigPut, payload)
	if err := fsm.applyCmd(cmd); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if a != 1 || b != 1 {
		t.Fatalf("hook calls: a=%d b=%d, want both 1", a, b)
	}
}
