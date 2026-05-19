package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// writeSelfSignedCert writes a freshly-generated ECDSA self-signed cert +
// key into dir/cert.pem and dir/key.pem. Returns the two paths.
func writeSelfSignedCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "grainfs-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

// newTestNodeConfig returns an NC whose TLSCertPath/TLSKeyPath point at
// the given dir, by setting the env overrides for the duration of the
// test. We use env overrides so the NC's resolution path matches prod.
func newTestNodeConfig(t *testing.T) (*nodeconfig.NodeConfig, string) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("GRAINFS_TLS_CERT", filepath.Join(dir, "cert.pem"))
	t.Setenv("GRAINFS_TLS_KEY", filepath.Join(dir, "key.pem"))
	return nodeconfig.New(dir), dir
}

func TestTLSListener_BootNoCert_Plaintext(t *testing.T) {
	nc, _ := newTestNodeConfig(t)
	l := NewHotTLSListener(nc, "127.0.0.1:0")
	if err := l.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	if l.IsTLS() {
		t.Fatalf("IsTLS() = true; want false (no cert files)")
	}

	// Connect plaintext, write a byte, expect echo via a tiny accept loop.
	done := make(chan error, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			done <- err
			return
		}
		defer c.Close()
		buf := make([]byte, 1)
		if _, err := io.ReadFull(c, buf); err != nil {
			done <- err
			return
		}
		_, err = c.Write(buf)
		done <- err
	}()

	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte{0x42}); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if buf[0] != 0x42 {
		t.Fatalf("got %x, want 0x42", buf[0])
	}
	if err := <-done; err != nil {
		t.Fatalf("accept loop: %v", err)
	}
}

func TestTLSListener_BootWithCert_TLSActive(t *testing.T) {
	nc, dir := newTestNodeConfig(t)
	writeSelfSignedCert(t, dir)

	l := NewHotTLSListener(nc, "127.0.0.1:0")
	if err := l.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	if !l.IsTLS() {
		t.Fatalf("IsTLS() = false; want true (cert files present)")
	}

	// Drive a real TLS handshake to verify Accept wraps the conn.
	handshakeOK := make(chan error, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			handshakeOK <- err
			return
		}
		defer c.Close()
		tc, ok := c.(*tls.Conn)
		if !ok {
			handshakeOK <- net.UnknownNetworkError("not a *tls.Conn")
			return
		}
		handshakeOK <- tc.Handshake()
	}()

	dialer := &tls.Dialer{Config: &tls.Config{InsecureSkipVerify: true}}
	c, err := dialer.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	defer c.Close()
	if err := <-handshakeOK; err != nil {
		t.Fatalf("server handshake: %v", err)
	}
}

func TestTLSListener_SIGHUP_HotSwap(t *testing.T) {
	nc, dir := newTestNodeConfig(t)
	l := NewHotTLSListener(nc, "127.0.0.1:0")
	if err := l.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if l.IsTLS() {
		t.Fatalf("initial IsTLS() = true; want false")
	}

	// Write cert + key into the watched paths, then Reload.
	writeSelfSignedCert(t, dir)
	if err := l.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if !l.IsTLS() {
		t.Fatalf("post-reload IsTLS() = false; want true")
	}

	// And verify a new accept now produces a *tls.Conn that completes a
	// TLS handshake against a tls.Dialer.
	type acceptResult struct {
		isTLS    bool
		hsErr    error
		closeErr error
	}
	got := make(chan acceptResult, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			got <- acceptResult{}
			return
		}
		tc, ok := c.(*tls.Conn)
		if !ok {
			_ = c.Close()
			got <- acceptResult{isTLS: false}
			return
		}
		hsErr := tc.Handshake()
		// Keep conn alive until dialer completes; closer races with dialer's
		// own close, so swallow whichever wins.
		got <- acceptResult{isTLS: true, hsErr: hsErr, closeErr: tc.Close()}
	}()
	dialer := &tls.Dialer{Config: &tls.Config{InsecureSkipVerify: true}}
	c, err := dialer.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	_ = c.Close()
	r := <-got
	if !r.isTLS {
		t.Fatalf("accept did not yield *tls.Conn after Reload")
	}
	if r.hsErr != nil {
		t.Fatalf("server handshake after Reload: %v", r.hsErr)
	}
}

func TestTLSListener_PartialCert_StartupRefuses(t *testing.T) {
	nc, dir := newTestNodeConfig(t)
	// Write ONLY the cert file; leave the key absent.
	certPath, _ := writeSelfSignedCert(t, dir)
	if err := os.Remove(filepath.Join(dir, "key.pem")); err != nil {
		t.Fatalf("remove key: %v", err)
	}
	_ = certPath

	l := NewHotTLSListener(nc, "127.0.0.1:0")
	err := l.Start()
	if err == nil {
		_ = l.Close()
		t.Fatalf("Start succeeded; want error for partial cert (cert without key)")
	}

	// Now flip it: only key, no cert.
	nc2, dir2 := newTestNodeConfig(t)
	_, keyPath := writeSelfSignedCert(t, dir2)
	if err := os.Remove(filepath.Join(dir2, "cert.pem")); err != nil {
		t.Fatalf("remove cert: %v", err)
	}
	_ = keyPath

	l2 := NewHotTLSListener(nc2, "127.0.0.1:0")
	if err := l2.Start(); err == nil {
		_ = l2.Close()
		t.Fatalf("Start succeeded; want error for partial cert (key without cert)")
	}
}
