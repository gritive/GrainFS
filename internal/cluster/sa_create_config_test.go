package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/iam"
)

// newFSMWithIAMAndConfig builds a MetaFSM with IAM + a fully-registered config
// store wired, ready for SA/config interaction tests.
func newFSMWithIAMAndConfig(t *testing.T) (*MetaFSM, *iam.Store, *config.Store) {
	t.Helper()
	enc := newIAMTestEncryptor(t)
	iamStore := iam.NewStore()
	iamApplier := iam.NewApplier(iamStore, enc)

	cfgStore := config.NewStore()
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{})

	f := NewMetaFSM()
	f.SetIAM(iamStore, iamApplier)
	f.SetConfigStore(cfgStore)
	return f, iamStore, cfgStore
}

func TestSACreateDoesNotMutateAnonymousConfig(t *testing.T) {
	f, _, cfgStore := newFSMWithIAMAndConfig(t)

	before := cfgStore.Snapshot()
	if _, ok := cfgStore.GetBool("iam.anon-enabled"); ok {
		t.Fatal("iam.anon-enabled should not be registered")
	}

	if err := f.applyCmd(buildSACreateCmdForAtomic(t, "sa-first")); err != nil {
		t.Fatalf("applyCmd IAMSACreate: %v", err)
	}

	after := cfgStore.Snapshot()
	if len(after) != len(before) {
		t.Fatalf("SA create should not mutate config snapshot: before=%v after=%v", before, after)
	}
	if _, ok := cfgStore.GetBool("iam.anon-enabled"); ok {
		t.Fatal("iam.anon-enabled should not be recreated after SA create")
	}
}

func TestSACreateErrorDoesNotMutateConfig(t *testing.T) {
	f, _, cfgStore := newFSMWithIAMAndConfig(t)

	before := cfgStore.Snapshot()
	err := f.applyCmd(buildSACreateCmdForAtomic(t, ""))
	if err == nil {
		t.Fatal("expected error from IAMSACreate with empty sa_id")
	}

	after := cfgStore.Snapshot()
	if len(after) != len(before) {
		t.Fatalf("failed SA create should not mutate config snapshot: before=%v after=%v", before, after)
	}
}
