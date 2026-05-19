package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/config"
)

// newTestMetaFSMWithConfigStore builds a minimal MetaFSM with the config store
// wired, mirroring the pattern used in meta_fsm_cluster_config_test.go.
func newTestMetaFSMWithConfigStore(t *testing.T, store *config.Store) *MetaFSM {
	t.Helper()
	f := NewMetaFSM()
	f.SetConfigStore(store)
	return f
}

// buildConfigPutPayload builds the FlatBuffers inner payload for ConfigPut.
func buildConfigPutPayload(t *testing.T, key, value string) []byte {
	t.Helper()
	data, err := encodeMetaConfigPutCmd(key, value)
	if err != nil {
		t.Fatalf("encodeMetaConfigPutCmd: %v", err)
	}
	return data
}

// buildConfigDeletePayload builds the FlatBuffers inner payload for ConfigDelete.
func buildConfigDeletePayload(t *testing.T, key string) []byte {
	t.Helper()
	data, err := encodeMetaConfigDeleteCmd(key)
	if err != nil {
		t.Fatalf("encodeMetaConfigDeleteCmd: %v", err)
	}
	return data
}

// buildMetaCmd wraps an inner payload in a MetaCmd envelope.
func buildMetaCmd(t *testing.T, mtype MetaCmdType, payload []byte) []byte {
	t.Helper()
	data, err := encodeMetaCmd(mtype, payload)
	if err != nil {
		t.Fatalf("encodeMetaCmd: %v", err)
	}
	return data
}

func TestApply_ConfigPut_UpdatesStore(t *testing.T) {
	store := config.NewStore()
	config.RegisterClusterKeys(store, config.ReloadHooks{})
	fsm := newTestMetaFSMWithConfigStore(t, store)

	payload := buildConfigPutPayload(t, "audit.deny-only", "true")
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeConfigPut, payload)
	if err := fsm.applyCmd(cmd); err != nil {
		t.Fatalf("applyCmd: %v", err)
	}

	got, ok := store.GetBool("audit.deny-only")
	if !ok {
		t.Fatal("key not registered")
	}
	if !got {
		t.Fatalf("post-apply value = %v, want true", got)
	}
}

func TestApply_ConfigDelete_RemovesValue(t *testing.T) {
	store := config.NewStore()
	config.RegisterClusterKeys(store, config.ReloadHooks{})

	// Pre-set the value.
	if err := store.Set(context.Background(), "audit.deny-only", "true"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	fsm := newTestMetaFSMWithConfigStore(t, store)

	payload := buildConfigDeletePayload(t, "audit.deny-only")
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeConfigDelete, payload)
	if err := fsm.applyCmd(cmd); err != nil {
		t.Fatalf("applyCmd: %v", err)
	}

	got, ok := store.GetBool("audit.deny-only")
	if !ok {
		t.Fatal("key not registered")
	}
	if got {
		t.Fatalf("post-delete value = %v, want false (default)", got)
	}
}

func TestApply_ConfigPut_NilStore_IsNoOp(t *testing.T) {
	// MetaFSM with no config store wired: ConfigPut must be a safe no-op.
	fsm := NewMetaFSM() // cfg is nil

	payload := buildConfigPutPayload(t, "audit.deny-only", "true")
	cmd := buildMetaCmd(t, clusterpb.MetaCmdTypeConfigPut, payload)
	if err := fsm.applyCmd(cmd); err != nil {
		t.Fatalf("expected no error with nil config store, got: %v", err)
	}
}
