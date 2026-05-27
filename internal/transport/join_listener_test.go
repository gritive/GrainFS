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
	"math/big"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

// newTestJoinCert generates a fresh self-signed P-256 cert whose SPKI is in NO
// accept-set, returns the tls.Certificate and its SPKI hash.
func newTestJoinCert(t *testing.T, cn string) (tls.Certificate, [32]byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv, Leaf: leaf}
	return cert, sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
}

// TestJoinListenerCapturesUnknownPeerSPKI proves the bypass: a joiner whose SPKI
// is in NO accept-set still reaches the handler, and the captured SPKI equals
// sha256(presented cert RawSubjectPublicKeyInfo).
func TestJoinListenerCapturesUnknownPeerSPKI(t *testing.T) {
	serverCert, _ := newTestJoinCert(t, "join-leader")
	clientCert, wantSPKI := newTestJoinCert(t, "join-joiner")

	type captured struct {
		spki [32]byte
	}
	got := make(chan captured, 1)
	handler := func(_ context.Context, peerSPKI [32]byte, stream *quic.Stream) {
		got <- captured{spki: peerSPKI}
		_ = stream.Close()
	}

	ln, err := NewJoinListener("127.0.0.1:0", serverCert, handler)
	if err != nil {
		t.Fatalf("NewJoinListener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, closeConn, err := DialJoin(ctx, ln.Addr(), ln.SPKI(), clientCert)
	if err != nil {
		t.Fatalf("DialJoin: %v", err)
	}
	defer func() { _ = closeConn() }()
	// Open the stream by writing one byte so the server's AcceptStream returns.
	if _, err := stream.Write([]byte{0}); err != nil {
		t.Fatalf("stream write: %v", err)
	}
	_ = stream.Close()

	select {
	case c := <-got:
		if c.spki != wantSPKI {
			t.Fatalf("captured SPKI mismatch: got %x want %x", c.spki, wantSPKI)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler not invoked within timeout (bypass failed)")
	}
}

// TestDialJoinRejectsWrongServerSPKI proves the joiner pins the leader: dialing
// with a wrong expected server SPKI fails the handshake (MITM/relay defense).
func TestDialJoinRejectsWrongServerSPKI(t *testing.T) {
	serverCert, _ := newTestJoinCert(t, "join-leader")
	clientCert, _ := newTestJoinCert(t, "join-joiner")
	_, wrongSPKI := newTestJoinCert(t, "not-the-leader")

	handler := func(_ context.Context, _ [32]byte, stream *quic.Stream) {
		_ = stream.Close()
	}
	ln, err := NewJoinListener("127.0.0.1:0", serverCert, handler)
	if err != nil {
		t.Fatalf("NewJoinListener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, closeConn, err := DialJoin(ctx, ln.Addr(), wrongSPKI, clientCert)
	if err == nil {
		if closeConn != nil {
			_ = closeConn()
		}
		t.Fatal("expected handshake failure with wrong server SPKI, got nil error")
	}
}
