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
// deterministic EncryptorAdapter over a fixed 32-byte key (gen always 0).
func staticTestEncryptor(t testing.TB) storage.DataEncryptor {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	enc, err := encrypt.NewEncryptor(key)
	if err != nil {
		t.Fatalf("new encryptor: %v", err)
	}
	return storage.NewEncryptorAdapter(enc, []byte("0123456789abcdef"))
}

func TestPDPTokenSource_GenChangesOnReseal(t *testing.T) {
	cfg := config.NewStore()
	config.RegisterClusterKeys(cfg, config.ReloadHooks{})
	enc := staticTestEncryptor(t)

	ts := newPDPTokenSource(cfg)
	ts.setEncryptor(enc)

	if _, _, ok := ts.CurrentToken(); ok {
		t.Fatal("expected no token")
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

	tok, gen1, ok := ts.CurrentToken()
	if !ok || tok != "tok-1" {
		t.Fatalf("tok=%q ok=%v", tok, ok)
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
