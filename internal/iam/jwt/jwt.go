// internal/iam/jwt/jwt.go
package jwt

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	algHS256       = "HS256"
	clockSkewLimit = 30 * time.Second

	// MaxJWTTokenTTL is the conservative upper bound on any minted token's TTL.
	// Used by JWTKeyStore.PrunePrevSafe to ensure no live tokens reference a
	// demoted previous key. Matches the default TTL in MintAt and the OAuth
	// handler at 3600s.
	MaxJWTTokenTTL = time.Hour
)

var (
	ErrNoCurrentKey   = errors.New("no current signing key")
	ErrTokenMalformed = errors.New("malformed token")
	ErrAlgNotHS256    = errors.New("alg must be HS256")
	ErrKidUnknown     = errors.New("kid unknown (not current and not previous)")
	ErrSignature      = errors.New("invalid signature")
	ErrExpired        = errors.New("token expired")
	ErrClockSkew      = errors.New("token iat outside clock-skew window")
	ErrPrunePrev      = errors.New("previous key still has live tokens; prune refused (F#1)")
)

type Claims struct {
	Sub       string        `json:"sub"`
	Warehouse string        `json:"warehouse"`
	Iss       string        `json:"iss"`
	Iat       int64         `json:"iat"`
	Exp       int64         `json:"exp"`
	Kid       string        `json:"-"`
	TTL       time.Duration `json:"-"`
}

type keyEntry struct {
	kid     string
	secret  []byte
	expires time.Time // for previous-key tokens; set when key is demoted
}

type KeySet struct {
	mu       sync.RWMutex
	current  *keyEntry
	previous *keyEntry
}

func NewKeySet() *KeySet { return &KeySet{} }

func (ks *KeySet) GenerateCurrent() (string, error) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	if ks.current != nil {
		return ks.current.kid, nil
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", fmt.Errorf("generate signing secret: %w", err)
	}
	kid, err := newKid()
	if err != nil {
		return "", err
	}
	ks.current = &keyEntry{kid: kid, secret: secret}
	return kid, nil
}

func (ks *KeySet) Rotate() (string, error) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	if ks.current == nil {
		return "", ErrNoCurrentKey
	}
	demoted := ks.current
	demoted.expires = time.Now().Add(2 * time.Hour) // safety: tokens live ≤ TTL; we keep prev around at least their max TTL
	ks.previous = demoted
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", fmt.Errorf("generate signing secret: %w", err)
	}
	kid, err := newKid()
	if err != nil {
		return "", err
	}
	ks.current = &keyEntry{kid: kid, secret: secret}
	return kid, nil
}

func (ks *KeySet) Prune(safe bool) error {
	if !safe {
		return ErrPrunePrev
	}
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.previous = nil
	return nil
}

func (ks *KeySet) Mint(c Claims) (string, error) {
	return ks.MintAt(c, time.Now())
}

func (ks *KeySet) MintAt(c Claims, iat time.Time) (string, error) {
	ks.mu.RLock()
	cur := ks.current
	ks.mu.RUnlock()
	if cur == nil {
		return "", ErrNoCurrentKey
	}
	c.Iat = iat.Unix()
	if c.TTL == 0 {
		c.TTL = time.Hour
	}
	c.Exp = iat.Add(c.TTL).Unix()
	if c.Iss == "" {
		c.Iss = "grainfs"
	}
	hdr := map[string]string{"alg": algHS256, "typ": "JWT", "kid": cur.kid}
	hdrB, err := json.Marshal(hdr)
	if err != nil {
		return "", fmt.Errorf("marshal jwt header: %w", err)
	}
	bodyB, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("marshal jwt claims: %w", err)
	}
	hdr64 := base64URLEncode(hdrB)
	body64 := base64URLEncode(bodyB)
	sig := hmacSign(cur.secret, hdr64+"."+body64)
	return hdr64 + "." + body64 + "." + sig, nil
}

func (ks *KeySet) Verify(tok string) (*Claims, error) {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return nil, ErrTokenMalformed
	}
	hdrB, err := base64URLDecode(parts[0])
	if err != nil {
		return nil, ErrTokenMalformed
	}
	var hdr struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(hdrB, &hdr); err != nil {
		return nil, ErrTokenMalformed
	}
	if hdr.Alg != algHS256 {
		return nil, ErrAlgNotHS256
	}
	ks.mu.RLock()
	var key *keyEntry
	switch {
	case ks.current != nil && ks.current.kid == hdr.Kid:
		key = ks.current
	case ks.previous != nil && ks.previous.kid == hdr.Kid:
		key = ks.previous
	}
	ks.mu.RUnlock()
	if key == nil {
		return nil, ErrKidUnknown
	}
	expectSig := hmacSign(key.secret, parts[0]+"."+parts[1])
	if !hmac.Equal([]byte(expectSig), []byte(parts[2])) {
		return nil, ErrSignature
	}
	bodyB, err := base64URLDecode(parts[1])
	if err != nil {
		return nil, ErrTokenMalformed
	}
	var c Claims
	if err := json.Unmarshal(bodyB, &c); err != nil {
		return nil, ErrTokenMalformed
	}
	c.Kid = hdr.Kid
	now := time.Now()
	if iat := time.Unix(c.Iat, 0); iat.After(now.Add(clockSkewLimit)) {
		return nil, ErrClockSkew
	}
	if exp := time.Unix(c.Exp, 0); exp.Before(now.Add(-clockSkewLimit)) {
		return nil, ErrExpired
	}
	return &c, nil
}

func base64URLEncode(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
func base64URLDecode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}

func hmacSign(secret []byte, payload string) string {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(payload))
	return base64URLEncode(h.Sum(nil))
}

func newKid() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate kid: %w", err)
	}
	return fmt.Sprintf("k_%x", b), nil
}

// NewKid generates a new random key ID. Exported for use by the meta-FSM apply path.
func NewKid() (string, error) { return newKid() }

// CurrentKID returns the KID of the current signing key, or empty string if
// no current key is loaded. Safe for concurrent use.
func (ks *KeySet) CurrentKID() string {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	if ks.current == nil {
		return ""
	}
	return ks.current.kid
}

// PreviousKID returns the KID of the previous (demoted) signing key, or empty
// string if no previous key exists. Safe for concurrent use.
func (ks *KeySet) PreviousKID() string {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	if ks.previous == nil {
		return ""
	}
	return ks.previous.kid
}

// KeySeed is the persisted, wrapped representation of a JWT signing key stored
// in the meta-FSM snapshot. WrappedSecret is sealed with the DEK identified by DekGen.
type KeySeed struct {
	Kid           string
	WrappedSecret []byte
	DekGen        uint32
	Role          string // "current" or "previous"
	DemotedAt     int64  // unix seconds; 0 for "current"; set when demoted to "previous"
}

// Unwrapper is the minimal interface needed to unseal a wrapped JWT signing secret.
// *encrypt.DEKKeeper satisfies this interface.
type Unwrapper interface {
	Open(ct []byte, gen uint32) ([]byte, error)
}

// LoadFromSeeds replaces the in-memory keyset with seeds unwrapped via u.
// Roles other than "current"/"previous" are ignored. Multiple "current" seeds:
// the last one in iteration order wins (FSM emits at most one).
func (ks *KeySet) LoadFromSeeds(seeds []KeySeed, u Unwrapper) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.current = nil
	ks.previous = nil
	for _, s := range seeds {
		plain, err := u.Open(s.WrappedSecret, s.DekGen)
		if err != nil {
			return fmt.Errorf("jwt: LoadFromSeeds: unwrap kid %s: %w", s.Kid, err)
		}
		entry := &keyEntry{kid: s.Kid, secret: plain}
		switch s.Role {
		case "current":
			ks.current = entry
		case "previous":
			if s.DemotedAt != 0 {
				entry.expires = time.Unix(s.DemotedAt, 0).Add(MaxJWTTokenTTL)
			}
			ks.previous = entry
		}
	}
	return nil
}

// InstallCurrent installs s as the new current key without touching previous.
// Used by the FSM apply path. Grabs ks.mu.Lock() internally.
func (ks *KeySet) InstallCurrent(s KeySeed, u Unwrapper) error {
	plain, err := u.Open(s.WrappedSecret, s.DekGen)
	if err != nil {
		return fmt.Errorf("jwt: InstallCurrent: unwrap kid %s: %w", s.Kid, err)
	}
	ks.mu.Lock()
	ks.current = &keyEntry{kid: s.Kid, secret: plain}
	ks.mu.Unlock()
	return nil
}

// DemoteCurrentToPrevious moves current → previous with the demotion timestamp.
// No-op if current is nil. Used by the FSM apply path.
func (ks *KeySet) DemoteCurrentToPrevious(now time.Time) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	if ks.current == nil {
		return
	}
	demoted := ks.current
	demoted.expires = now.Add(MaxJWTTokenTTL)
	ks.previous = demoted
	ks.current = nil
}
