package pdp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// TokenConfigKey is the config-store key holding the sealed bearer token envelope.
// RESERVED: only the dedicated pdp admin handler writes it (Task 5/8 guard).
const TokenConfigKey = "iam.pdp.token"

// Sentinel AAD identity so the PDP token rides the SAME cluster-consistent DEK
// path AccessKey credentials use (iam.WrapSecret(enc, saID, accessKey, pt)).
const (
	pdpTokenSealID  = "iam.pdp"
	pdpTokenSealKey = "bearer-token"
)

// TokenEnvelope is the JSON stored under iam.pdp.token: base64 ciphertext + DEK gen.
type TokenEnvelope struct {
	CTB64  string `json:"ct_b64"`
	DEKGen uint32 `json:"dek_gen"`
}

// ParseTokenEnvelope validates the envelope shape (the load-bearing validator that
// rejects a plaintext or malformed token on every config write path). Does NOT
// verify the ciphertext decrypts (needs the live encryptor).
func ParseTokenEnvelope(raw []byte) (TokenEnvelope, error) {
	var e TokenEnvelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return TokenEnvelope{}, fmt.Errorf("iam.pdp.token: invalid envelope JSON: %w", err)
	}
	if e.CTB64 == "" {
		return TokenEnvelope{}, fmt.Errorf("iam.pdp.token: empty ciphertext")
	}
	if _, err := base64.StdEncoding.DecodeString(e.CTB64); err != nil {
		return TokenEnvelope{}, fmt.Errorf("iam.pdp.token: ct_b64 not base64: %w", err)
	}
	return e, nil
}

// SealToken seals plaintext via the cluster-consistent IAM DEK path and returns the
// envelope JSON. wrap is iam.WrapSecret bound (injected to avoid an import cycle):
//
//	func(saID, accessKey, pt string) ([]byte, uint32, error)
func SealToken(wrap func(saID, accessKey, pt string) ([]byte, uint32, error), plaintext string) ([]byte, error) {
	ct, gen, err := wrap(pdpTokenSealID, pdpTokenSealKey, plaintext)
	if err != nil {
		return nil, fmt.Errorf("iam.pdp.token: seal: %w", err)
	}
	return json.Marshal(TokenEnvelope{CTB64: base64.StdEncoding.EncodeToString(ct), DEKGen: gen})
}

// OpenToken reverses SealToken. unwrap is iam.UnwrapSecret bound:
//
//	func(saID, accessKey string, gen uint32, ct []byte) (string, error)
func OpenToken(unwrap func(saID, accessKey string, gen uint32, ct []byte) (string, error), env TokenEnvelope) (string, error) {
	ct, err := base64.StdEncoding.DecodeString(env.CTB64)
	if err != nil {
		return "", fmt.Errorf("iam.pdp.token: ct_b64 decode: %w", err)
	}
	return unwrap(pdpTokenSealID, pdpTokenSealKey, env.DEKGen, ct)
}

// Fingerprint is the first 8 hex chars of SHA-256(token) — safe to show; never the token.
func Fingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])[:8]
}

// TokenStatus tri-states the configured bearer token so the decorator can tell
// "no token configured" (proceed without Authorization — valid for an http
// loopback sidecar or an unauthenticated PDP) apart from "a token IS configured
// but cannot be made usable" (parse/unseal failure, or the encryptor is not
// ready). The latter must NOT silently degrade to a token-less call — a
// configured-but-broken secret hard-denies (see Decorator.chain), so a
// fail-open policy can never turn a corrupt/misconfigured token into an allow.
type TokenStatus int

const (
	// TokenAbsent: no iam.pdp.token configured. Proceed without a bearer.
	TokenAbsent TokenStatus = iota
	// TokenReady: a usable token (token+gen populated).
	TokenReady
	// TokenError: a token is configured but unusable (bad envelope, unseal
	// failure, or encryptor not ready). The decorator hard-denies.
	TokenError
)

// TokenSource gives the decorator the current bearer token + an opaque generation
// that changes on rotation (feeds clientID + configGen). Implemented in
// serveruntime (reads the live encryptor + the iam.pdp.token config value).
type TokenSource interface {
	CurrentToken() (token string, gen string, status TokenStatus)
}
