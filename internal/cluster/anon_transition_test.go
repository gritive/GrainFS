package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/iam"
)

// newFSMWithIAMAndConfig builds a MetaFSM with IAM + a fully-registered config
// store wired, ready for anon-transition tests.
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

// TestSACreate_FlipsAnonEnabledAtomically verifies that the first SA create
// sets iam.anon-enabled = false in the same apply.
func TestSACreate_FlipsAnonEnabledAtomically(t *testing.T) {
	f, _, cfgStore := newFSMWithIAMAndConfig(t)

	// Pre-state: key not explicitly set → defaults to true.
	if v, ok := cfgStore.GetBool("iam.anon-enabled"); !ok {
		t.Fatal("iam.anon-enabled not registered")
	} else if !v {
		t.Fatal("pre-condition: expected iam.anon-enabled default = true")
	}

	if err := f.applyCmd(buildSACreateCmdForAtomic(t, "sa-first")); err != nil {
		t.Fatalf("applyCmd IAMSACreate: %v", err)
	}

	got, ok := cfgStore.GetBool("iam.anon-enabled")
	if !ok {
		t.Fatal("iam.anon-enabled not registered after apply")
	}
	if got {
		t.Fatal("iam.anon-enabled must be false after first SA create, got true")
	}
}

// TestSACreate_SecondSADoesNotFlip verifies that after the first SA flips the
// flag to false, an operator who explicitly re-enables anon is not disturbed by
// a second SA create.
func TestSACreate_SecondSADoesNotFlip(t *testing.T) {
	f, _, cfgStore := newFSMWithIAMAndConfig(t)

	// First SA create → flips to false.
	if err := f.applyCmd(buildSACreateCmdForAtomic(t, "sa-first")); err != nil {
		t.Fatalf("first SA create: %v", err)
	}

	// Operator explicitly re-enables anonymous access.
	if err := cfgStore.Set(context.Background(), "iam.anon-enabled", "true"); err != nil {
		t.Fatalf("operator re-enable: %v", err)
	}

	// Second SA create must NOT flip the flag.
	if err := f.applyCmd(buildSACreateCmdForAtomic(t, "sa-second")); err != nil {
		t.Fatalf("second SA create: %v", err)
	}

	got, _ := cfgStore.GetBool("iam.anon-enabled")
	if !got {
		t.Fatal("iam.anon-enabled must stay true after operator re-enable; second SA create must not flip it")
	}
}

// TestSACreate_ErrorDoesNotFlip verifies that a failed ApplySACreate
// (empty sa_id) does not flip iam.anon-enabled.
func TestSACreate_ErrorDoesNotFlip(t *testing.T) {
	f, _, cfgStore := newFSMWithIAMAndConfig(t)

	// Build an IAMSACreate cmd with an empty sa_id (triggers ApplySACreate error).
	badCmd := buildSACreateCmdForAtomic(t, "") // empty ID → ApplySACreate returns error

	err := f.applyCmd(badCmd)
	if err == nil {
		t.Fatal("expected error from IAMSACreate with empty sa_id")
	}

	// Flag must remain at its default (true).
	got, ok := cfgStore.GetBool("iam.anon-enabled")
	if !ok {
		t.Fatal("iam.anon-enabled not registered")
	}
	if !got {
		t.Fatal("iam.anon-enabled must stay true when SA create fails")
	}
}
