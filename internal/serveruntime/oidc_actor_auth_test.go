package serveruntime

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/iam/oidc"
	"github.com/gritive/GrainFS/internal/iam/principal"
)

func TestOIDCActorAuthenticatorFirstMatchingIssuer(t *testing.T) {
	key := mustOIDCTestRSAKey(t)
	cfgStore := newOIDCTestConfigStore(t)
	raw := `[` +
		`{"name":"other","issuer_url":"https://other.example.com/","audience":"grainfs","jwks_url":"https://other.example.com/jwks.json","groups_claim":"groups","group_prefix":"oidc:other:"},` +
		`{"name":"example","issuer_url":"https://idp.example.com/","audience":"grainfs","jwks_url":"https://idp.example.com/jwks.json","groups_claim":"groups","group_prefix":"oidc:example:"}` +
		`]`
	require.NoError(t, cfgStore.Set(context.Background(), "iam.oidc.issuers", raw))
	auth := newOIDCActorAuthenticatorWithKeys(cfgStore, staticOIDCKeySource{"kid-1": &key.PublicKey}, func() time.Time {
		return time.Unix(100, 0)
	})
	token := signOIDCTestJWT(t, "kid-1", key, map[string]any{
		"iss": "https://idp.example.com/", "aud": "grainfs", "sub": "alice", "exp": int64(200), "groups": []string{"storage-admins"},
	})

	p, err := auth.AuthenticateActor(context.Background(), token)

	require.NoError(t, err)
	require.Equal(t, principal.KindOIDC, p.Kind)
	require.Equal(t, "https://idp.example.com/", p.Issuer)
	require.Equal(t, "alice", p.Subject)
	require.Equal(t, []string{"oidc:example:storage-admins"}, p.Groups)
}

func TestOIDCActorAuthenticatorFailsWhenAllIssuersReject(t *testing.T) {
	key := mustOIDCTestRSAKey(t)
	cfgStore := newOIDCTestConfigStore(t)
	require.NoError(t, cfgStore.Set(context.Background(), "iam.oidc.issuers",
		`[{"name":"example","issuer_url":"https://idp.example.com/","audience":"grainfs","jwks_url":"https://idp.example.com/jwks.json","groups_claim":"groups","group_prefix":"oidc:example:"}]`))
	auth := newOIDCActorAuthenticatorWithKeys(cfgStore, staticOIDCKeySource{"kid-1": &key.PublicKey}, func() time.Time {
		return time.Unix(300, 0)
	})
	token := signOIDCTestJWT(t, "kid-1", key, map[string]any{
		"iss": "https://idp.example.com/", "aud": "grainfs", "sub": "alice", "exp": int64(200), "groups": []string{"storage-admins"},
	})

	_, err := auth.AuthenticateActor(context.Background(), token)

	require.ErrorContains(t, err, "oidc authentication failed")
	require.NotContains(t, err.Error(), token)
}

func TestOIDCActorAuthenticatorEmptyConfigFailsClosed(t *testing.T) {
	cfgStore := newOIDCTestConfigStore(t)
	auth := newOIDCActorAuthenticatorWithKeys(cfgStore, staticOIDCKeySource{}, func() time.Time {
		return time.Unix(100, 0)
	})

	_, err := auth.AuthenticateActor(context.Background(), "token")

	require.ErrorContains(t, err, "oidc issuers not configured")
	require.Nil(t, newOIDCActorAuthenticator(nil))
}

type staticOIDCKeySource map[string]*rsa.PublicKey

func (s staticOIDCKeySource) Key(_ context.Context, _ oidc.IssuerConfig, kid string, _ time.Time) (*rsa.PublicKey, error) {
	key, ok := s[kid]
	if !ok {
		return nil, oidc.ErrUnknownKID
	}
	return key, nil
}

func newOIDCTestConfigStore(t *testing.T) *config.Store {
	t.Helper()
	cfgStore := config.NewStore()
	config.RegisterClusterKeys(cfgStore, config.ReloadHooks{})
	return cfgStore
}

func mustOIDCTestRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return key
}

func signOIDCTestJWT(t *testing.T, kid string, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	header := map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid}
	hb, err := json.Marshal(header)
	require.NoError(t, err)
	cb, err := json.Marshal(claims)
	require.NoError(t, err)
	payload := base64.RawURLEncoding.EncodeToString(hb) + "." + base64.RawURLEncoding.EncodeToString(cb)
	sum := sha256.Sum256([]byte(payload))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	require.NoError(t, err)
	return payload + "." + base64.RawURLEncoding.EncodeToString(sig)
}
