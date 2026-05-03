package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func longPSK(seed string) string {
	if len(seed) >= 64 {
		return seed[:64]
	}
	return strings.Repeat(seed, 64/len(seed)+1)[:64]
}

// generateRandomTLSCert produces a self-signed cert with a fresh keypair —
// NOT derived from any cluster PSK. Used to simulate an attacker that
// doesn't know the cluster identity.
func generateRandomTLSCert(t *testing.T) tls.Certificate {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "attacker"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{certDER}, PrivateKey: priv}
}

func TestQUIC_Handshake_SamePSK_Succeeds(t *testing.T) {
	psk := longPSK("a")
	server, err := NewQUICTransport(psk)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Listen(ctx, "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}

	dialer, err := NewQUICTransport(psk)
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()

	if err := dialer.Connect(ctx, server.LocalAddr()); err != nil {
		t.Fatalf("dial with matching PSK should succeed: %v", err)
	}
}

func TestQUIC_Handshake_DifferentPSK_FailsWithSPKI(t *testing.T) {
	server, err := NewQUICTransport(longPSK("a"))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Listen(ctx, "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}

	dialer, err := NewQUICTransport(longPSK("b"))
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()

	err = dialer.Connect(ctx, server.LocalAddr())
	if err == nil {
		t.Fatal("dial with different PSK should fail")
	}
	// Match substring — exact wording varies by Go/quic-go version.
	if !strings.Contains(err.Error(), "SPKI") &&
		!strings.Contains(err.Error(), "cert") &&
		!strings.Contains(err.Error(), "bad certificate") {
		t.Fatalf("expected SPKI/cert error, got: %v", err)
	}
}

// REGRESSION (D5=A): server must reject a client that presents no certificate.
// Without ClientAuth: RequireAnyClientCert, this test passes trivially because
// the server never asks for a cert. With it, the handshake fails.
func TestQUIC_ServerRejectsClientWithNoCert(t *testing.T) {
	server, err := NewQUICTransport(longPSK("a"))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Listen(ctx, "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"grainfs-mux-v1", "grainfs"},
		// Certificates: nil — attacker presents no cert
	}
	_, err = quic.DialAddr(ctx, server.LocalAddr(), tlsConf, defaultQUICConfig())
	if err == nil {
		t.Fatal("server must reject dialer with no client cert")
	}
}

// REGRESSION (D5=A): server must reject a client that presents a random cert
// not derived from the cluster PSK. Proves the server-side SPKI pin fires.
func TestQUIC_ServerRejectsClientWithWrongCert(t *testing.T) {
	server, err := NewQUICTransport(longPSK("a"))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Listen(ctx, "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"grainfs-mux-v1", "grainfs"},
		Certificates:       []tls.Certificate{generateRandomTLSCert(t)},
	}
	_, err = quic.DialAddr(ctx, server.LocalAddr(), tlsConf, defaultQUICConfig())
	if err == nil {
		t.Fatal("server must reject dialer with non-cluster cert")
	}
}
