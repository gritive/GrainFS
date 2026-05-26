// Package encrypt: KEK challenge-response handshake (version-aware).
//
// Two nodes prove they share the same KEK at a specific version without
// transmitting it. The peer issues a 32-byte random nonce ("challenge")
// bound to a specific version; the joiner returns
// HMAC-SHA256(K_version, transcript) where transcript includes cluster_id,
// nonce, node_id, address, joiner_version, leader_active_version. The peer
// verifies the HMAC under K_version from its keystore.
//
// Protections:
//   - Single-use nonces (replay rejected)
//   - 60s default TTL (expired nonces rejected)
//   - Cluster-id binding (cross-cluster replay rejected)
//   - Full transcript binding (relay/MITM cannot rebind proof to different
//     join params)
//   - Mismatched response burns the nonce too, blocking guess-and-retry
package encrypt

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrHandshakeMismatch   = errors.New("KEK handshake HMAC mismatch")
	ErrHandshakeReplay     = errors.New("KEK handshake nonce replay rejected")
	ErrHandshakeExpired    = errors.New("KEK handshake nonce expired")
	ErrHandshakeUnknownVer = errors.New("KEK handshake unknown KEK version")
	ErrHandshakeBadCluster = errors.New("KEK handshake cluster_id mismatch")
)

const defaultNonceTTL = 60 * time.Second
const handshakeNonceSize = 32

// JoinTranscript captures every field that contributes to the handshake
// MAC. All callers MUST populate every field — empty bytes for absent
// values is not safe (an attacker could exploit ambiguity between absent
// and zero).
type JoinTranscript struct {
	ClusterID           []byte // 16 bytes
	Nonce               []byte // 32 bytes
	NodeID              string
	Address             string
	JoinerVersion       uint32
	LeaderActiveVersion uint32
}

// HandshakeVerifier issues version-specific challenges and verifies
// responses against the matching KEK in the KEKStore. The cluster_id is
// bound into the MAC so a captured proof from one cluster cannot be
// replayed against another cluster sharing the same KEK material
// (operator-mistake case).
type HandshakeVerifier struct {
	store     *KEKStore
	clusterID []byte
	ttl       time.Duration

	mu     sync.Mutex
	issued map[string]issuedNonce
	used   map[string]struct{}
}

type issuedNonce struct {
	version uint32
	at      time.Time
}

// NewHandshakeVerifier returns a verifier holding the given store and
// cluster_id, using the default nonce TTL (60s).
func NewHandshakeVerifier(store *KEKStore, clusterID []byte) *HandshakeVerifier {
	return NewHandshakeVerifierWithTTL(store, clusterID, defaultNonceTTL)
}

// NewHandshakeVerifierWithTTL returns a verifier with a caller-specified
// nonce TTL. Useful for tests; production should use defaultNonceTTL.
func NewHandshakeVerifierWithTTL(store *KEKStore, clusterID []byte, ttl time.Duration) *HandshakeVerifier {
	if len(clusterID) != 16 {
		panic(fmt.Sprintf("NewHandshakeVerifier: cluster_id must be 16 bytes, got %d", len(clusterID)))
	}
	return &HandshakeVerifier{
		store:     store,
		clusterID: append([]byte(nil), clusterID...),
		ttl:       ttl,
		issued:    make(map[string]issuedNonce),
		used:      make(map[string]struct{}),
	}
}

// IssueChallenge generates a fresh 32-byte nonce bound to the given KEK
// version. The version MUST exist in the store; absent versions are
// rejected — joiners that hold an unknown version cannot make the peer
// waste nonces.
func (v *HandshakeVerifier) IssueChallenge(version uint32) ([]byte, error) {
	if _, err := v.store.Get(version); err != nil {
		return nil, fmt.Errorf("%w: %d", ErrHandshakeUnknownVer, version)
	}
	nonce := make([]byte, handshakeNonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("KEK handshake: read random nonce: %w", err)
	}
	now := time.Now()
	key := hex.EncodeToString(nonce)
	v.mu.Lock()
	v.gcExpiredLocked(now)
	v.issued[key] = issuedNonce{version: version, at: now}
	v.mu.Unlock()
	return nonce, nil
}

// VerifyResponse checks the MAC against the transcript using K_{version}
// and burns the nonce on success or mismatch. Replay/expired/unknown-version
// failures do not burn the nonce (no extra info to attacker).
func (v *HandshakeVerifier) VerifyResponse(version uint32, transcript JoinTranscript, mac []byte) error {
	if !hmac.Equal(transcript.ClusterID, v.clusterID) {
		return ErrHandshakeBadCluster
	}
	key := hex.EncodeToString(transcript.Nonce)

	v.mu.Lock()
	defer v.mu.Unlock()

	if _, already := v.used[key]; already {
		return ErrHandshakeReplay
	}
	got, ok := v.issued[key]
	if !ok {
		return ErrHandshakeReplay
	}
	if got.version != version {
		return ErrHandshakeMismatch
	}
	if time.Since(got.at) > v.ttl {
		delete(v.issued, key)
		return ErrHandshakeExpired
	}

	kek, err := v.store.Get(version)
	if err != nil {
		return fmt.Errorf("%w: %d", ErrHandshakeUnknownVer, version)
	}
	expected, err := computeMACFromKEK(kek, transcript)
	if err != nil {
		return err
	}
	if !hmac.Equal(mac, expected) {
		delete(v.issued, key)
		v.used[key] = struct{}{}
		return ErrHandshakeMismatch
	}
	delete(v.issued, key)
	v.used[key] = struct{}{}
	return nil
}

func (v *HandshakeVerifier) gcExpiredLocked(now time.Time) {
	for k, e := range v.issued {
		if now.Sub(e.at) > v.ttl {
			delete(v.issued, k)
		}
	}
}

// ComputeHandshakeResponse is called by the joiner. It looks up its own
// KEK at version from store and produces the MAC over transcript.
func ComputeHandshakeResponse(store *KEKStore, version uint32, transcript JoinTranscript) ([]byte, error) {
	kek, err := store.Get(version)
	if err != nil {
		return nil, err
	}
	return computeMACFromKEK(kek, transcript)
}

func computeMACFromKEK(kek []byte, t JoinTranscript) ([]byte, error) {
	if len(t.ClusterID) != 16 {
		return nil, fmt.Errorf("transcript cluster_id must be 16 bytes, got %d", len(t.ClusterID))
	}
	if len(t.Nonce) != handshakeNonceSize {
		return nil, fmt.Errorf("transcript nonce must be %d bytes, got %d", handshakeNonceSize, len(t.Nonce))
	}
	m := hmac.New(sha256.New, kek)
	m.Write([]byte("join-v1"))
	m.Write(t.ClusterID)
	m.Write(t.Nonce)
	writeLenPrefixed(m, []byte(t.NodeID))
	writeLenPrefixed(m, []byte(t.Address))
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], t.JoinerVersion)
	m.Write(buf[:])
	binary.BigEndian.PutUint32(buf[:], t.LeaderActiveVersion)
	m.Write(buf[:])
	return m.Sum(nil), nil
}

func writeLenPrefixed(m interface{ Write([]byte) (int, error) }, b []byte) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(len(b)))
	m.Write(buf[:])
	m.Write(b)
}
