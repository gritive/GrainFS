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
		return "", err
	}
	kid := newKid()
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
		return "", err
	}
	kid := newKid()
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
	hdrB, _ := json.Marshal(hdr)
	bodyB, _ := json.Marshal(c)
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

func newKid() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("k_%x", b)
}
