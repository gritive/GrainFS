package cluster

import (
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/raft"
)

// TestMetaFSMRestore_DecodesIAMUnderDEKBeforeKeeperRebuild — R2 codex P0
// regression. A snapshot whose IAM credentials are sealed under the DEK
// (active gen 0, mirroring genesis-bootstrap) MUST decrypt during Restore
// even though the destination FSM's IAM applier has NO encryptor wired
// (mimicking the boot order where the live DEKKeeper is only installed
// AFTER Restore returns).
//
// The two-pass decode in MetaFSM.Restore constructs a transient
// TransientDataEncryptor from the snapshot's DEK trailer and uses it to
// decrypt the IAM trailer — no dependency on the destination's live keeper.
func TestMetaFSMRestore_DecodesIAMUnderDEKBeforeKeeperRebuild(t *testing.T) {
	store := iam.NewStore()
	applier := iam.NewApplier(store, nil)

	src := NewMetaFSM()
	enc := wireTestKEKAndDEK(t, src) // gen 0 active, KEK 0 active
	applier.SetEncryptor(enc)
	src.SetIAM(store, applier)

	now := time.Unix(1700000000, 0).UTC()
	if err := applier.ApplySACreate(buildSACreatePayloadForTest("sa-r2", "r2-test", now)); err != nil {
		t.Fatalf("ApplySACreate: %v", err)
	}
	wrapped, gen, err := iam.WrapSecret(enc, "sa-r2", "AKR2TEST", "r2-plaintext")
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	if gen != 0 {
		t.Fatalf("genesis-bootstrap should seal under gen 0, got %d", gen)
	}
	if err := applier.ApplyKeyCreate(buildKeyCreatePayloadForTestWithGen("AKR2TEST", "sa-r2", wrapped, gen, now)); err != nil {
		t.Fatalf("ApplyKeyCreate: %v", err)
	}

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Destination FSM has NO DEK keeper and NO applier encryptor. This mirrors
	// the boot order: meta-raft Restore runs BEFORE wireDEKKeeper +
	// wireIAMEncryptor land in T8. The two-pass decode must build a transient
	// adapter from the DEK trailer and decrypt IAM with it.
	store2 := iam.NewStore()
	applier2 := iam.NewApplier(store2, nil)
	dst := NewMetaFSM()
	wireTestKEK(t, dst) // KEK store only — NO DEK keeper
	dst.SetIAM(store2, applier2)

	if err := dst.Restore(raft.SnapshotMeta{}, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	k, ok := store2.LookupKey("AKR2TEST")
	if !ok {
		t.Fatal("AccessKey missing after two-pass decode Restore")
	}
	if k.SecretKey != "r2-plaintext" {
		t.Fatalf("decrypted secret = %q, want r2-plaintext", k.SecretKey)
	}
	if k.SecretKeyDEKGen != 0 {
		t.Fatalf("gen lost across snapshot: got %d, want 0", k.SecretKeyDEKGen)
	}
}
