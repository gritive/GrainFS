package oidc

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAuthenticatorVerifiesTokenAndNormalizesPrincipal(t *testing.T) {
	key := mustRSAKey(t)
	cfg := IssuerConfig{
		Name:              "example",
		IssuerURL:         "https://idp.example.com/",
		Audience:          "grainfs",
		JWKSURL:           "https://idp.example.com/jwks.json",
		GroupsClaim:       "groups",
		GroupPrefix:       "oidc:example:",
		JWKSFailurePolicy: FailurePolicyStrict,
	}
	auth := NewAuthenticator(cfg, staticKeySource{"kid-1": &key.PublicKey})

	token := signRS256(t, "kid-1", key, map[string]any{
		"iss":    "https://idp.example.com/",
		"aud":    "grainfs",
		"sub":    "user-1",
		"iat":    int64(90),
		"nbf":    int64(90),
		"exp":    int64(200),
		"groups": []string{"data-eng", "ops"},
	})

	p, err := auth.Authenticate(context.Background(), token, time.Unix(100, 0))
	require.NoError(t, err)
	require.Equal(t, "oidc", string(p.Kind))
	require.Equal(t, "https://idp.example.com/", p.Issuer)
	require.Equal(t, "user-1", p.Subject)
	require.Equal(t, []string{"oidc:example:data-eng", "oidc:example:ops"}, p.Groups)
	require.True(t, strings.HasPrefix(p.ID, "oidc:"))
	require.NotContains(t, p.ID, "user-1")
}

func TestAuthenticatorRejectsWrongAudience(t *testing.T) {
	key := mustRSAKey(t)
	cfg := IssuerConfig{Name: "example", IssuerURL: "https://idp.example.com/", Audience: "grainfs", GroupsClaim: "groups", GroupPrefix: "oidc:example:"}
	auth := NewAuthenticator(cfg, staticKeySource{"kid-1": &key.PublicKey})
	token := signRS256(t, "kid-1", key, map[string]any{
		"iss": "https://idp.example.com/", "aud": "other", "sub": "user-1", "iat": int64(90), "exp": int64(200), "groups": []string{"data-eng"},
	})

	_, err := auth.Authenticate(context.Background(), token, time.Unix(100, 0))
	require.ErrorContains(t, err, "audience")
}

func TestAuthenticatorRejectsExpiredTokenEvenWithKeyAvailable(t *testing.T) {
	key := mustRSAKey(t)
	cfg := IssuerConfig{Name: "example", IssuerURL: "https://idp.example.com/", Audience: "grainfs", GroupsClaim: "groups", GroupPrefix: "oidc:example:"}
	auth := NewAuthenticator(cfg, staticKeySource{"kid-1": &key.PublicKey})
	token := signRS256(t, "kid-1", key, map[string]any{
		"iss": "https://idp.example.com/", "aud": "grainfs", "sub": "user-1", "iat": int64(1), "exp": int64(50), "groups": []string{"data-eng"},
	})

	_, err := auth.Authenticate(context.Background(), token, time.Unix(100, 0))
	require.ErrorContains(t, err, "expired")
}

func TestAuthenticatorRejectsRecentlyExpiredTokenWithoutClockSkew(t *testing.T) {
	key := mustRSAKey(t)
	cfg := IssuerConfig{Name: "example", IssuerURL: "https://idp.example.com/", Audience: "grainfs", GroupsClaim: "groups", GroupPrefix: "oidc:example:"}
	auth := NewAuthenticator(cfg, staticKeySource{"kid-1": &key.PublicKey})
	token := signRS256(t, "kid-1", key, map[string]any{
		"iss": "https://idp.example.com/", "aud": "grainfs", "sub": "user-1", "iat": int64(90), "exp": int64(99), "groups": []string{"data-eng"},
	})

	_, err := auth.Authenticate(context.Background(), token, time.Unix(100, 0))
	require.ErrorContains(t, err, "expired")
}

func TestAuthenticatorRejectsInvalidGroupClaim(t *testing.T) {
	key := mustRSAKey(t)
	cfg := IssuerConfig{Name: "example", IssuerURL: "https://idp.example.com/", Audience: "grainfs", GroupsClaim: "groups", GroupPrefix: "oidc:example:"}
	auth := NewAuthenticator(cfg, staticKeySource{"kid-1": &key.PublicKey})
	token := signRS256(t, "kid-1", key, map[string]any{
		"iss": "https://idp.example.com/", "aud": "grainfs", "sub": "user-1", "iat": int64(90), "exp": int64(200), "groups": []string{"../admin"},
	})

	_, err := auth.Authenticate(context.Background(), token, time.Unix(100, 0))
	require.ErrorContains(t, err, "group")
}

func TestAuthenticatorRejectsUnknownKID(t *testing.T) {
	key := mustRSAKey(t)
	cfg := IssuerConfig{Name: "example", IssuerURL: "https://idp.example.com/", Audience: "grainfs", GroupsClaim: "groups", GroupPrefix: "oidc:example:"}
	auth := NewAuthenticator(cfg, staticKeySource{})
	token := signRS256(t, "kid-1", key, map[string]any{
		"iss": "https://idp.example.com/", "aud": "grainfs", "sub": "user-1", "iat": int64(90), "exp": int64(200), "groups": []string{"data-eng"},
	})

	_, err := auth.Authenticate(context.Background(), token, time.Unix(100, 0))
	require.ErrorContains(t, err, "kid")
}

func TestAuthenticatorRejectsAlgNone(t *testing.T) {
	cfg := IssuerConfig{Name: "example", IssuerURL: "https://idp.example.com/", Audience: "grainfs", GroupsClaim: "groups", GroupPrefix: "oidc:example:"}
	auth := NewAuthenticator(cfg, staticKeySource{})
	token := unsignedJWT(t, map[string]any{"alg": "none", "kid": "kid-1"}, map[string]any{
		"iss": "https://idp.example.com/", "aud": "grainfs", "sub": "user-1", "exp": int64(200), "groups": []string{"data-eng"},
	})

	_, err := auth.Authenticate(context.Background(), token, time.Unix(100, 0))
	require.ErrorContains(t, err, "RS256")
}

func TestAuthenticatorRejectsMissingKIDMissingSubjectMalformedAudienceAndFutureNBF(t *testing.T) {
	key := mustRSAKey(t)
	cfg := IssuerConfig{Name: "example", IssuerURL: "https://idp.example.com/", Audience: "grainfs", GroupsClaim: "groups", GroupPrefix: "oidc:example:"}
	auth := NewAuthenticator(cfg, staticKeySource{"kid-1": &key.PublicKey})

	for name, token := range map[string]string{
		"missing kid": signRS256WithHeader(t, map[string]any{"alg": "RS256"}, key, map[string]any{
			"iss": "https://idp.example.com/", "aud": "grainfs", "sub": "user-1", "exp": int64(200), "groups": []string{"data-eng"},
		}),
		"missing sub": signRS256(t, "kid-1", key, map[string]any{
			"iss": "https://idp.example.com/", "aud": "grainfs", "exp": int64(200), "groups": []string{"data-eng"},
		}),
		"malformed aud": signRS256(t, "kid-1", key, map[string]any{
			"iss": "https://idp.example.com/", "aud": 7, "sub": "user-1", "exp": int64(200), "groups": []string{"data-eng"},
		}),
		"future nbf": signRS256(t, "kid-1", key, map[string]any{
			"iss": "https://idp.example.com/", "aud": "grainfs", "sub": "user-1", "nbf": int64(200), "exp": int64(300), "groups": []string{"data-eng"},
		}),
	} {
		_, err := auth.Authenticate(context.Background(), token, time.Unix(100, 0))
		require.Error(t, err, name)
	}
}

type staticKeySource map[string]*rsa.PublicKey

func (s staticKeySource) Key(_ context.Context, _ IssuerConfig, kid string, _ time.Time) (*rsa.PublicKey, error) {
	key, ok := s[kid]
	if !ok {
		return nil, ErrUnknownKID
	}
	return key, nil
}

func signRS256(t *testing.T, kid string, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	header := map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid}
	return signRS256WithHeader(t, header, key, claims)
}

func signRS256WithHeader(t *testing.T, header map[string]any, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
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

func unsignedJWT(t *testing.T, header map[string]any, claims map[string]any) string {
	t.Helper()
	hb, err := json.Marshal(header)
	require.NoError(t, err)
	cb, err := json.Marshal(claims)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(hb) + "." + base64.RawURLEncoding.EncodeToString(cb) + "."
}
