package serveruntime

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func TestPhaseA_FreshBoot_GeneratesV0(t *testing.T) {
	dir := t.TempDir()
	store, err := encrypt.LoadOrInitKEKStoreDir(filepath.Join(dir, "keys"))
	if err != nil {
		t.Fatalf("LoadOrInitKEKStoreDir: %v", err)
	}
	if got := store.ActiveVersion(); got != 0 {
		t.Errorf("ActiveVersion = %d, want 0", got)
	}
	versions := store.Versions()
	if len(versions) != 1 || versions[0] != 0 {
		t.Errorf("Versions = %v, want [0]", versions)
	}
}

func TestPhaseA_ReloadAfterRestart_PreservesKey(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	s1, err := encrypt.LoadOrInitKEKStoreDir(keysDir)
	if err != nil {
		t.Fatalf("first boot: %v", err)
	}
	k0a, err := s1.Get(0)
	if err != nil {
		t.Fatalf("Get(0) on first boot: %v", err)
	}

	// Simulate restart: load a fresh KEKStore from the same disk.
	s2, err := encrypt.LoadOrInitKEKStoreDir(keysDir)
	if err != nil {
		t.Fatalf("second boot: %v", err)
	}
	k0b, _ := s2.Get(0)
	if !bytes.Equal(k0a, k0b) {
		t.Errorf("KEK changed across restart")
	}
}

func TestPhaseA_LegacyKEKRefusesBoot(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, "kek.key")
	if err := os.WriteFile(legacy, make([]byte, encrypt.KEKSize), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	_, err := encrypt.LoadOrInitKEKStoreDir(filepath.Join(dir, "keys"))
	if err == nil {
		t.Fatal("expected boot refusal on legacy kek.key, got nil")
	}
	if !errors.Is(err, encrypt.ErrLegacyKEKDetected) {
		t.Errorf("err = %v, want ErrLegacyKEKDetected", err)
	}
}

func TestPhaseA_DiskMonitor_GatesBeforeKeystoreLoad(t *testing.T) {
	// CheckKeystoreDiskSpace probes statfs of the given dir. Provide a
	// path that exists (the parent of the keystore subdir).
	dir := t.TempDir()
	if err := CheckKeystoreDiskSpace(dir, MinKeystoreFreeBytes); err != nil {
		t.Errorf("disk check on tmpdir refused: %v", err)
	}
}

func TestPhaseA_KeyAcrossKEKStore_DEKKeeper_AAD(t *testing.T) {
	// End-to-end inside Phase A: provision a KEKStore from disk, get
	// the active KEK, build a DEKKeeper, exercise SealWithAAD/OpenWithAAD.
	// This proves the assembled boot-time stack actually works for the
	// data-path API.
	dir := t.TempDir()
	store, err := encrypt.LoadOrInitKEKStoreDir(filepath.Join(dir, "keys"))
	if err != nil {
		t.Fatalf("LoadOrInitKEKStoreDir: %v", err)
	}
	activeKEK, err := store.ActiveKEK()
	if err != nil {
		t.Fatalf("ActiveKEK: %v", err)
	}
	keeper, err := encrypt.NewDEKKeeper(activeKEK)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	clusterID := bytes.Repeat([]byte{0xAB}, 16)
	aad := encrypt.BuildAAD(encrypt.DomainShard, clusterID,
		encrypt.FieldBytes([]byte("bucket/object")),
		encrypt.FieldUint32(0),
	)
	plain := []byte("phase-a smoke payload")
	ct, gen, err := keeper.SealWithAAD(plain, aad)
	if err != nil {
		t.Fatalf("SealWithAAD: %v", err)
	}
	got, err := keeper.OpenWithAAD(ct, gen, aad)
	if err != nil {
		t.Fatalf("OpenWithAAD: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Errorf("AAD-bound round-trip mismatch")
	}
}

func TestPhaseA_HandshakeRoundTrip_ViaKEKStore(t *testing.T) {
	// Two parties (joiner, verifier) sharing the same KEK + cluster_id
	// complete a handshake via the new transcript-bound API.
	dir := t.TempDir()
	store, err := encrypt.LoadOrInitKEKStoreDir(filepath.Join(dir, "keys"))
	if err != nil {
		t.Fatalf("LoadOrInitKEKStoreDir: %v", err)
	}
	clusterID := bytes.Repeat([]byte{0xCD}, 16)
	verifier := encrypt.NewHandshakeVerifier(store, clusterID)

	nonce, err := verifier.IssueChallenge(verifier.Store().ActiveVersion())
	if err != nil {
		t.Fatalf("IssueChallenge: %v", err)
	}
	transcript := encrypt.JoinTranscript{
		ClusterID: clusterID,
		Nonce:     nonce,
		NodeID:    "smoke-node",
		Address:   "127.0.0.1:7000",
	}
	mac, err := encrypt.ComputeHandshakeResponse(store, verifier.Store().ActiveVersion(), transcript)
	if err != nil {
		t.Fatalf("ComputeHandshakeResponse: %v", err)
	}
	if err := verifier.VerifyResponse(verifier.Store().ActiveVersion(), transcript, mac); err != nil {
		t.Errorf("VerifyResponse: %v", err)
	}
}
