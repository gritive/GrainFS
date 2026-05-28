package oidc

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/iam/principal"
)

const oidcClockSkew = 30 * time.Second

var ErrUnknownKID = errors.New("kid unknown")

type KeySource interface {
	Key(ctx context.Context, cfg IssuerConfig, kid string, now time.Time) (*rsa.PublicKey, error)
}

type Authenticator struct {
	cfg  IssuerConfig
	keys KeySource
}

func NewAuthenticator(cfg IssuerConfig, keys KeySource) *Authenticator {
	return &Authenticator{cfg: cfg, keys: keys}
}

func (a *Authenticator) Authenticate(ctx context.Context, token string, now time.Time) (principal.Principal, error) {
	header, claims, signingInput, sig, err := parseCompactJWT(token)
	if err != nil {
		return principal.Principal{}, err
	}
	if header.Alg != "RS256" {
		return principal.Principal{}, fmt.Errorf("oidc token alg must be RS256, got %q", header.Alg)
	}
	if header.Kid == "" {
		return principal.Principal{}, errors.New("oidc token kid required")
	}
	key, err := a.keys.Key(ctx, a.cfg, header.Kid, now)
	if err != nil {
		return principal.Principal{}, err
	}
	sum := sha256.Sum256([]byte(signingInput))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
		return principal.Principal{}, fmt.Errorf("oidc token signature: %w", err)
	}
	if err := validateClaims(a.cfg, claims, now); err != nil {
		return principal.Principal{}, err
	}
	rawGroups, err := claimStrings(claims, a.cfg.GroupsClaim)
	if err != nil {
		return principal.Principal{}, fmt.Errorf("group claim %q: %w", a.cfg.GroupsClaim, err)
	}
	groups, err := MapGroups(a.cfg, rawGroups)
	if err != nil {
		return principal.Principal{}, fmt.Errorf("group mapping: %w", err)
	}
	id := principalID(a.cfg.IssuerURL, claims.Subject)
	return principal.OIDC(a.cfg.IssuerURL, claims.Subject, id, groups), nil
}

type jwtHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	Typ string `json:"typ"`
}

type jwtClaims struct {
	Issuer    string          `json:"iss"`
	Subject   string          `json:"sub"`
	Audience  json.RawMessage `json:"aud"`
	IssuedAt  int64           `json:"iat"`
	NotBefore int64           `json:"nbf"`
	Expires   int64           `json:"exp"`
	Raw       map[string]any  `json:"-"`
}

func parseCompactJWT(token string) (jwtHeader, jwtClaims, string, []byte, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return jwtHeader{}, jwtClaims{}, "", nil, errors.New("malformed oidc token")
	}
	hb, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return jwtHeader{}, jwtClaims{}, "", nil, errors.New("malformed oidc token header")
	}
	cb, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return jwtHeader{}, jwtClaims{}, "", nil, errors.New("malformed oidc token claims")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return jwtHeader{}, jwtClaims{}, "", nil, errors.New("malformed oidc token signature")
	}
	var header jwtHeader
	if err := json.Unmarshal(hb, &header); err != nil {
		return jwtHeader{}, jwtClaims{}, "", nil, errors.New("malformed oidc token header")
	}
	var claims jwtClaims
	if err := json.Unmarshal(cb, &claims); err != nil {
		return jwtHeader{}, jwtClaims{}, "", nil, errors.New("malformed oidc token claims")
	}
	if err := json.Unmarshal(cb, &claims.Raw); err != nil {
		return jwtHeader{}, jwtClaims{}, "", nil, errors.New("malformed oidc token claims")
	}
	return header, claims, parts[0] + "." + parts[1], sig, nil
}

func validateClaims(cfg IssuerConfig, claims jwtClaims, now time.Time) error {
	if claims.Issuer != cfg.IssuerURL {
		return errors.New("issuer mismatch")
	}
	if claims.Subject == "" {
		return errors.New("subject required")
	}
	if !audienceContains(claims.Audience, cfg.Audience) {
		return errors.New("audience mismatch")
	}
	if claims.Expires == 0 || !time.Unix(claims.Expires, 0).After(now) {
		return errors.New("token expired")
	}
	if claims.NotBefore != 0 && time.Unix(claims.NotBefore, 0).After(now.Add(oidcClockSkew)) {
		return errors.New("token not yet valid")
	}
	if claims.IssuedAt != 0 && time.Unix(claims.IssuedAt, 0).After(now.Add(oidcClockSkew)) {
		return errors.New("token issued in the future")
	}
	return nil
}

func audienceContains(raw json.RawMessage, want string) bool {
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return single == want
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err == nil {
		for _, aud := range many {
			if aud == want {
				return true
			}
		}
	}
	return false
}

func claimStrings(claims jwtClaims, name string) ([]string, error) {
	value, ok := claims.Raw[name]
	if !ok {
		return nil, errors.New("missing")
	}
	switch v := value.(type) {
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, errors.New("expected string array")
			}
			out = append(out, s)
		}
		return out, nil
	case string:
		return []string{v}, nil
	default:
		return nil, errors.New("expected string or string array")
	}
}

func principalID(issuer, subject string) string {
	issuerSum := sha256.Sum256([]byte(issuer))
	subjectSum := sha256.Sum256([]byte(subject))
	return fmt.Sprintf("oidc:%x:%x", issuerSum[:8], subjectSum[:16])
}
