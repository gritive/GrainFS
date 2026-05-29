package serveruntime

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	iampdp "github.com/gritive/GrainFS/internal/iam/pdp"
	"github.com/gritive/GrainFS/internal/storage"
)

// staticTestEncryptor mirrors internal/server/admin/handlers_pdp_test.go: a
// deterministic DataEncryptor over a fixed KEK + clusterID (DEK keeper, active
// gen 0). Replaces the retired static encryptor seam; both seal at gen 0.
func staticTestEncryptor(t testing.TB) storage.DataEncryptor {
	t.Helper()
	clusterID := []byte("0123456789abcdef")
	kek := make([]byte, encrypt.KEKSize)
	for i := range kek {
		kek[i] = byte(i + 1)
	}
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	if err != nil {
		t.Fatalf("new dek keeper: %v", err)
	}
	return storage.NewDEKKeeperAdapter(keeper, clusterID)
}

func TestPDPTokenSource_GenChangesOnReseal(t *testing.T) {
	cfg := config.NewStore()
	config.RegisterClusterKeys(cfg, config.ReloadHooks{})
	enc := staticTestEncryptor(t)

	ts := newPDPTokenSource(cfg)
	ts.setEncryptor(enc)

	if _, _, st := ts.CurrentToken(); st != iampdp.TokenAbsent {
		t.Fatalf("expected TokenAbsent, got %v", st)
	}

	envJSON, err := iampdp.SealToken(func(s, k, p string) ([]byte, uint32, error) {
		return iam.WrapSecret(enc, s, k, p)
	}, "tok-1")
	if err != nil {
		t.Fatalf("seal tok-1: %v", err)
	}
	if err := cfg.Set(context.Background(), "iam.pdp.token", string(envJSON)); err != nil {
		t.Fatal(err)
	}

	tok, gen1, st := ts.CurrentToken()
	if st != iampdp.TokenReady || tok != "tok-1" {
		t.Fatalf("tok=%q status=%v", tok, st)
	}
	_, gen1b, _ := ts.CurrentToken()
	if gen1 != gen1b {
		t.Fatal("gen must be stable when unchanged")
	}

	envJSON2, err := iampdp.SealToken(func(s, k, p string) ([]byte, uint32, error) {
		return iam.WrapSecret(enc, s, k, p)
	}, "tok-2")
	if err != nil {
		t.Fatalf("seal tok-2: %v", err)
	}
	if err := cfg.Set(context.Background(), "iam.pdp.token", string(envJSON2)); err != nil {
		t.Fatal(err)
	}
	tok2, gen2, _ := ts.CurrentToken()
	if tok2 != "tok-2" || gen2 == gen1 {
		t.Fatalf("rotation must change token+gen: tok=%q gen=%q (was %q)", tok2, gen2, gen1)
	}
}

// TestPDPTokenSource_ConfiguredButNoEncryptorIsError proves that a configured
// token with the encryptor NOT yet wired reports TokenError (not TokenAbsent) —
// the decorator hard-denies rather than silently calling the PDP token-less.
func TestPDPTokenSource_ConfiguredButNoEncryptorIsError(t *testing.T) {
	cfg := config.NewStore()
	config.RegisterClusterKeys(cfg, config.ReloadHooks{})
	enc := staticTestEncryptor(t)
	envJSON, err := iampdp.SealToken(func(s, k, p string) ([]byte, uint32, error) {
		return iam.WrapSecret(enc, s, k, p)
	}, "tok-x")
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if err := cfg.Set(context.Background(), "iam.pdp.token", string(envJSON)); err != nil {
		t.Fatal(err)
	}
	ts := newPDPTokenSource(cfg) // NOTE: setEncryptor intentionally NOT called
	if _, _, st := ts.CurrentToken(); st != iampdp.TokenError {
		t.Fatalf("configured token with no encryptor must be TokenError, got %v", st)
	}
}
