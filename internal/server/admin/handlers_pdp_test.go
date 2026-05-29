package admin_test

import (
	"context"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	iampdp "github.com/gritive/GrainFS/internal/iam/pdp"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/storage"
)

// staticTestEncryptor mirrors internal/iam/encrypt_test.go: a deterministic
// EncryptorAdapter over a fixed 32-byte key (gen always 0).
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

// fakePDPTokens implements admin.PDPTokenManager.
type fakePDPTokens struct {
	enc    storage.DataEncryptor
	token  string
	gen    string
	status iampdp.TokenStatus
}

func (f *fakePDPTokens) CurrentToken() (string, string, iampdp.TokenStatus) {
	return f.token, f.gen, f.status
}
func (f *fakePDPTokens) CurrentEncryptor() storage.DataEncryptor { return f.enc }

func TestPDPSetToken_SealsBeforePropose(t *testing.T) {
	ctx := context.Background()
	cfg := &routeConfigService{}
	d := &admin.Deps{
		ConfigProposer: cfg,
		PDPTokens:      &fakePDPTokens{enc: staticTestEncryptor(t), status: iampdp.TokenAbsent},
	}

	const plaintext = "super-secret-bearer-token"
	if err := admin.PDPSetToken(ctx, d, plaintext); err != nil {
		t.Fatalf("PDPSetToken: %v", err)
	}

	got, ok := cfg.puts[iampdp.TokenConfigKey]
	if !ok {
		t.Fatalf("expected ProposeConfigPut for %q, puts=%v", iampdp.TokenConfigKey, cfg.puts)
	}
	if strings.Contains(got, plaintext) {
		t.Fatalf("proposed value must NOT contain the plaintext token, got %q", got)
	}
	if _, err := iampdp.ParseTokenEnvelope([]byte(got)); err != nil {
		t.Fatalf("proposed value must parse as a token envelope: %v", err)
	}
}

func TestPDPSetToken_NoOpWhenUnchanged(t *testing.T) {
	ctx := context.Background()
	cfg := &routeConfigService{}
	const plaintext = "unchanged-token"
	d := &admin.Deps{
		ConfigProposer: cfg,
		PDPTokens:      &fakePDPTokens{enc: staticTestEncryptor(t), token: plaintext, gen: "1", status: iampdp.TokenReady},
	}

	if err := admin.PDPSetToken(ctx, d, plaintext); err != nil {
		t.Fatalf("PDPSetToken: %v", err)
	}
	if len(cfg.puts) != 0 {
		t.Fatalf("idempotent re-set must NOT call ProposeConfigPut, puts=%v", cfg.puts)
	}
}

func TestBuildPDPStatus_NeverPrintsToken(t *testing.T) {
	const token = "a-very-secret-token"
	pdpRaw := `{"enabled":true,"endpoint":"https://pdp.example.com:8443","tls":{"min_version":"1.3","ca_pem":"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----"}}`

	out := admin.BuildPDPStatus(pdpRaw, token, iampdp.TokenReady)

	if strings.Contains(out, token) {
		t.Fatalf("status output must NEVER contain the token, got:\n%s", out)
	}
	if !strings.Contains(out, iampdp.Fingerprint(token)) {
		t.Fatalf("status output must contain the token fingerprint %q, got:\n%s", iampdp.Fingerprint(token), out)
	}
	if !strings.Contains(out, "token_configured") {
		t.Fatalf("status output must contain token_configured, got:\n%s", out)
	}
}
