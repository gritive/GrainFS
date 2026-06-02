package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"golang.org/x/crypto/hkdf"
)

// recycleJitter returns 0..250ms to de-synchronize cluster-wide reconnects.
func recycleJitter() time.Duration {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}
	return time.Duration(binary.BigEndian.Uint16(b[:])%251) * time.Millisecond
}

// pinAcceptedSPKI returns a VerifyPeerCertificate callback that accepts any
// peer cert whose SPKI hash is in snap's accept set (O(1) map lookup).
func pinAcceptedSPKI(snap *IdentitySnapshot) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("no peer cert presented")
		}
		cert, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse peer cert: %w", err)
		}
		spki := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
		if !snap.Accepts(spki) {
			return errors.New("peer cert SPKI does not match any accepted cluster identity")
		}
		return nil
	}
}

// checkResponseStatus maps a transport response's status to an error (nil on OK).
func checkResponseStatus(addr string, resp *Message) (*Message, error) {
	if resp == nil {
		return nil, errors.New("transport: nil response")
	}
	if resp.Status == StatusOK {
		return resp, nil
	}
	return nil, fmt.Errorf("transport response from %s status %d: %s", addr, resp.Status, string(resp.Payload))
}

// StreamHandler processes an incoming request message and returns a response.
type StreamHandler func(req *Message) *Message

// StreamBodyHandler processes a framed request followed by raw body bytes on
// the same stream. The handler must read body before returning a response.
type StreamBodyHandler func(req *Message, body io.Reader) *Message

// StreamReadHandler processes a framed request and returns a framed metadata
// response followed by a raw response body on the same stream.
type StreamReadHandler func(req *Message) (*Message, io.ReadCloser)

type TrafficLimits struct {
	Control int
	Meta    int
	Data    int
	Bulk    int
}

type TrafficLimiter struct {
	control chan struct{}
	meta    chan struct{}
	data    chan struct{}
	bulk    chan struct{}
}

func NewTrafficLimiter(l TrafficLimits) *TrafficLimiter {
	return &TrafficLimiter{
		control: makeLimit(l.Control),
		meta:    makeLimit(l.Meta),
		data:    makeLimit(l.Data),
		bulk:    makeLimit(l.Bulk),
	}
}

func makeLimit(n int) chan struct{} {
	if n <= 0 {
		return nil
	}
	return make(chan struct{}, n)
}

func (l *TrafficLimiter) Acquire(ctx context.Context, st StreamType) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	var ch chan struct{}
	switch ClassOf(st) {
	case StreamClassControl:
		ch = l.control
	case StreamClassMeta:
		ch = l.meta
	case StreamClassData:
		ch = l.data
	case StreamClassBulk:
		ch = l.bulk
	}
	if ch == nil {
		return func() {}, nil
	}
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// StreamRouter routes incoming messages to different handlers based on StreamType.
type StreamRouter struct {
	mu           sync.RWMutex
	handlers     map[StreamType]StreamHandler
	bodyHandlers map[StreamType]StreamBodyHandler
	readHandlers map[StreamType]StreamReadHandler
}

// NewStreamRouter creates a router that dispatches by StreamType.
func NewStreamRouter() *StreamRouter {
	return &StreamRouter{
		handlers:     make(map[StreamType]StreamHandler),
		bodyHandlers: make(map[StreamType]StreamBodyHandler),
		readHandlers: make(map[StreamType]StreamReadHandler),
	}
}

// Handle registers a handler for a specific stream type.
func (r *StreamRouter) Handle(st StreamType, h StreamHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[st] = h
}

// HandleBody registers a handler that receives the framed request payload plus
// any remaining bytes on the same stream as a streaming body.
func (r *StreamRouter) HandleBody(st StreamType, h StreamBodyHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bodyHandlers[st] = h
}

// HandleRead registers a handler that writes a framed metadata response and
// then streams any returned body bytes on the same stream.
func (r *StreamRouter) HandleRead(st StreamType, h StreamReadHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readHandlers[st] = h
}

// Dispatch finds the handler for the message's stream type and calls it.
func (r *StreamRouter) Dispatch(req *Message) *Message {
	r.mu.RLock()
	h, ok := r.handlers[req.Type]
	r.mu.RUnlock()
	if !ok {
		return nil
	}
	return h(req)
}

// Lookup returns the handler for the given stream type, if registered.
func (r *StreamRouter) Lookup(st StreamType) (StreamHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[st]
	return h, ok
}

// LookupBody returns the streaming body handler for the stream type, if any.
func (r *StreamRouter) LookupBody(st StreamType) (StreamBodyHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.bodyHandlers[st]
	return h, ok
}

// LookupRead returns the streaming read handler for the stream type, if any.
func (r *StreamRouter) LookupRead(st StreamType) (StreamReadHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.readHandlers[st]
	return h, ok
}

// IdentitySnapshot is the active TLS identity state of a cluster transport.
// During steady-state, AcceptSPKIs has one entry. During phases 2/3 of a
// rolling cluster-key rotation (rotation-spec §5.3), the rotation worker
// installs a snapshot with two entries (OLD + NEW) so peers presenting
// either identity are accepted.
//
// Snapshots are immutable after Store; concurrent readers may hold the
// pointer indefinitely. The transport reads the active snapshot per
// handshake via its identity.Load().
type IdentitySnapshot struct {
	AcceptSPKIs [][32]byte      // 1 entry steady, 2 during rotation phase 2/3 (kept for back-compat/debug)
	PresentCert tls.Certificate // active cert this node presents
	PresentSPKI [32]byte        // SHA256 of PresentCert.RawSubjectPublicKeyInfo

	acceptSet map[[32]byte]struct{} // O(1) lookup derived from AcceptSPKIs
}

// NewIdentitySnapshot builds a snapshot with the O(1) accept set precomputed.
// Callers MUST construct via this so acceptSet is populated; a literal with a
// nil acceptSet accepts nothing.
func NewIdentitySnapshot(accept [][32]byte, present tls.Certificate, presentSPKI [32]byte) *IdentitySnapshot {
	set := make(map[[32]byte]struct{}, len(accept))
	for _, s := range accept {
		set[s] = struct{}{}
	}
	return &IdentitySnapshot{
		AcceptSPKIs: accept,
		PresentCert: present,
		PresentSPKI: presentSPKI,
		acceptSet:   set,
	}
}

// Accepts reports whether spki is in the accept set (O(1) map lookup).
// SPKI is sha256 of the peer's RawSubjectPublicKeyInfo — a public value sent
// in cleartext during the TLS handshake — so map-lookup timing reveals nothing
// not already visible on the wire; constant-time comparison is unnecessary here.
func (s *IdentitySnapshot) Accepts(spki [32]byte) bool {
	if s.acceptSet != nil {
		_, ok := s.acceptSet[spki]
		return ok
	}
	// Defensive fallback: a snapshot built via a raw struct literal (not
	// NewIdentitySnapshot) has a nil acceptSet. Scan AcceptSPKIs directly so it
	// still verifies correctly (O(N), misuse path only) rather than silently
	// accepting nothing.
	for _, a := range s.AcceptSPKIs {
		if a == spki {
			return true
		}
	}
	return false
}

// DeriveClusterIdentity is exported so internal/cluster/rotation_worker.go
// can recompute SPKIs for FSM verification of operator-distributed key files.
func DeriveClusterIdentity(psk string) (tls.Certificate, [32]byte, error) {
	if psk == "" {
		return tls.Certificate{}, [32]byte{}, ErrEmptyClusterKey
	}

	// 1) Derive keypair from psk via HKDF.
	// We CANNOT use ecdsa.GenerateKey here: since Go 1.21 it mixes in
	// fresh crypto/rand for side-channel masking, which breaks the
	// determinism we need (same psk → same SPKI on every node).
	// The modular-reduction bias for P-256 is at most 2^-128 (curve order
	// is within 2^-128 of 2^256), well below any cryptographic threshold.
	reader := hkdf.New(sha256.New, []byte(psk), nil, []byte("grainfs-cluster-identity-v1"))
	priv, err := derivePrivKeyFromHKDF(reader, elliptic.P256())
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("derive cluster keypair: %w", err)
	}

	// 2) Self-signed cert with crypto/rand for signature.
	// NotBefore predates likely deployment by an hour to tolerate clock
	// skew; NotAfter is far enough out that operators rotate via PSK
	// change rather than cert renewal.
	now := time.Now().UTC()
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "grainfs-cluster"},
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(100 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("create cluster cert: %w", err)
	}

	// 3) SPKI hash for pinning.
	parsed, err := x509.ParseCertificate(certDER)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("parse cluster cert: %w", err)
	}
	spki := sha256.Sum256(parsed.RawSubjectPublicKeyInfo)

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  priv,
		Leaf:        parsed,
	}, spki, nil
}

// derivePrivKeyFromHKDF reads scalar bytes from r, reduces modulo curve order N,
// rejects zero. Re-reads on rejection (HKDF Expand can produce up to
// 255*HashLen bytes for SHA-256, far more than enough for a few retries).
//
// Note: stdlib's ecdsa.GenerateKey is NOT used because since Go 1.21 it
// blends crypto/rand into the keypair derivation for side-channel masking,
// breaking determinism. The modular-reduction bias for P-256 is ≤ 2^-128.
func derivePrivKeyFromHKDF(r io.Reader, curve elliptic.Curve) (*ecdsa.PrivateKey, error) {
	params := curve.Params()
	N := params.N
	byteLen := (N.BitLen() + 7) / 8
	buf := make([]byte, byteLen)

	for attempt := 0; attempt < 8; attempt++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("hkdf expand: %w", err)
		}
		k := new(big.Int).SetBytes(buf)
		k.Mod(k, N)
		if k.Sign() == 0 {
			continue
		}
		priv := new(ecdsa.PrivateKey)
		priv.PublicKey.Curve = curve
		priv.D = k
		priv.PublicKey.X, priv.PublicKey.Y = curve.ScalarBaseMult(k.Bytes())
		return priv, nil
	}
	return nil, errors.New("hkdf produced 8 consecutive zero scalars (impossible without a broken PSK)")
}
