// Package encrypt: KEK challenge-response handshake.
//
// Spec: D#15 + F#23 + F#27 of the auth-redesign plan (§7).
//
// Two nodes prove they share the same KEK without transmitting it. The peer
// issues a 32-byte random nonce ("challenge"); the joiner returns
// HMAC-SHA256(KEK, nonce) ("response"); the peer verifies the HMAC under its
// own KEK. A match means both sides hold the same KEK.
//
// Protections:
//   - Single-use nonces (F#27: replay rejected)
//   - 60s default TTL (expired nonces rejected)
//   - Wrong-KEK rejected (F#23)
//   - Mismatched response burns the nonce too, blocking guess-and-retry
package encrypt

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Sentinel errors for the KEK handshake.
var (
	ErrHandshakeMismatch = errors.New("KEK handshake HMAC mismatch")
	ErrHandshakeReplay   = errors.New("KEK handshake nonce replay rejected")
	ErrHandshakeExpired  = errors.New("KEK handshake nonce expired")
)

// defaultNonceTTL bounds how long an issued challenge remains acceptable.
// Joiners that take longer than this to respond must request a fresh nonce.
const defaultNonceTTL = 60 * time.Second

// handshakeNonceSize is the byte length of an issued challenge nonce.
const handshakeNonceSize = 32

// HandshakeVerifier issues challenges and verifies responses against a held
// KEK.
//
// Concurrency: a sync.Mutex guards the issued/used maps. The verify path is a
// composite check-then-delete-and-mark across two maps; atomic/channel
// alternatives would not be simpler or faster here, so a plain Mutex is the
// right primitive (see ~/.claude memory feedback_lockfree_over_mutex).
type HandshakeVerifier struct {
	kek    []byte
	ttl    time.Duration
	mu     sync.Mutex
	issued map[string]time.Time
	used   map[string]struct{}
}

// NewHandshakeVerifier returns a verifier holding the given KEK and using
// defaultNonceTTL.
func NewHandshakeVerifier(kek []byte) *HandshakeVerifier {
	return NewHandshakeVerifierWithTTL(kek, defaultNonceTTL)
}

// NewHandshakeVerifierWithTTL returns a verifier with a caller-specified
// nonce TTL. Useful for tests; production should use defaultNonceTTL.
func NewHandshakeVerifierWithTTL(kek []byte, ttl time.Duration) *HandshakeVerifier {
	return &HandshakeVerifier{
		kek:    kek,
		ttl:    ttl,
		issued: make(map[string]time.Time),
		used:   make(map[string]struct{}),
	}
}

// IssueChallenge generates a fresh 32-byte random nonce, records it as
// issued, and returns it. Expired entries are garbage-collected on each call
// so the maps cannot grow unboundedly under sustained join traffic.
func (v *HandshakeVerifier) IssueChallenge() ([]byte, error) {
	nonce := make([]byte, handshakeNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("KEK handshake: read random nonce: %w", err)
	}

	now := time.Now()
	key := hex.EncodeToString(nonce)

	v.mu.Lock()
	v.gcExpiredLocked(now)
	v.issued[key] = now
	v.mu.Unlock()

	return nonce, nil
}

// VerifyResponse checks that resp == HMAC-SHA256(v.kek, nonce) for a
// previously-issued, unexpired, unused nonce. Side effects:
//
//   - Match: nonce moved from issued to used (single-use).
//   - Mismatch: nonce burned (added to used, removed from issued) so an
//     attacker cannot guess-and-retry.
//   - Expired: nonce removed from issued.
//   - Replay (already used / never issued): no state change.
func (v *HandshakeVerifier) VerifyResponse(nonce, resp []byte) error {
	key := hex.EncodeToString(nonce)

	v.mu.Lock()
	defer v.mu.Unlock()

	if _, already := v.used[key]; already {
		return ErrHandshakeReplay
	}
	issuedAt, ok := v.issued[key]
	if !ok {
		return ErrHandshakeReplay
	}
	if time.Since(issuedAt) > v.ttl {
		delete(v.issued, key)
		return ErrHandshakeExpired
	}

	expected := computeHandshakeResponse(v.kek, nonce)
	if !hmac.Equal(resp, expected) {
		// Burn the nonce: mismatch prevents the joiner from retrying with
		// another guess against the same challenge (F#23 hardening).
		delete(v.issued, key)
		v.used[key] = struct{}{}
		return ErrHandshakeMismatch
	}

	delete(v.issued, key)
	v.used[key] = struct{}{}
	return nil
}

// gcExpiredLocked removes issued entries older than v.ttl. Caller must hold
// v.mu. Bounded by len(issued); the cost amortizes against IssueChallenge.
func (v *HandshakeVerifier) gcExpiredLocked(now time.Time) {
	for k, t := range v.issued {
		if now.Sub(t) > v.ttl {
			delete(v.issued, k)
		}
	}
}

// ComputeHandshakeResponse returns HMAC-SHA256(kek, nonce). Joiners call this
// to answer a challenge from the peer.
func ComputeHandshakeResponse(kek, nonce []byte) []byte {
	return computeHandshakeResponse(kek, nonce)
}

func computeHandshakeResponse(kek, nonce []byte) []byte {
	mac := hmac.New(sha256.New, kek)
	mac.Write(nonce)
	return mac.Sum(nil)
}
