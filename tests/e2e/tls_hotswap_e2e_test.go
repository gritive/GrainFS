// TLS hot-swap e2e (auth-redesign §5 T43).
//
// Spawns an isolated grainfs server with the GRAINFS_TLS_CERT / GRAINFS_TLS_KEY
// env vars pointing at a TempDir. Boots with neither file present, verifies
// /metrics serves over plaintext HTTP, then writes a self-signed cert+key into
// those paths and sends SIGHUP. Verifies subsequent connections require a TLS
// handshake (plaintext probe is rejected as a malformed HTTP response,
// HTTPS probe with InsecureSkipVerify succeeds).
//
// SingleNode-only — cluster TLS posture is a separate concern (every node
// would need its own cert reload coordinated) and is out of scope for T43.
package e2e

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTLSHotSwapE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runTLSHotSwapCase(t)
	})
	// Cluster4Node intentionally omitted — see file header.
}

func runTLSHotSwapCase(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "grainfs-tls-hotswap-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dataDir) })

	certDir := t.TempDir()
	certPath := filepath.Join(certDir, "cert.pem")
	keyPath := filepath.Join(certDir, "key.pem")

	port := freePort()
	cmd := exec.Command(getBinary(), "serve",
		"--data", dataDir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	)
	cmd.Env = append(os.Environ(),
		"GRAINFS_TLS_CERT="+certPath,
		"GRAINFS_TLS_KEY="+keyPath,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { terminateProcess(cmd) })

	waitForPort(t, port, 30*time.Second)

	// Phase 1: plaintext works. Disable keepalives so the pooled conn from
	// this probe does NOT leak into Phase 3's plaintext-must-fail check —
	// HotTLSListener documents that already-accepted conns retain their
	// pre-reload wrapping, so a reused keepalive conn would still serve
	// plaintext and confuse the post-reload assertion.
	plaintextURL := fmt.Sprintf("http://127.0.0.1:%d/metrics", port)
	plainClient := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	resp, err := plainClient.Get(plaintextURL)
	require.NoError(t, err, "plaintext GET /metrics must succeed before TLS swap")
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Phase 2: write self-signed cert + key, SIGHUP.
	writeSelfSignedCertE2E(t, certPath, keyPath)
	require.NoError(t, cmd.Process.Signal(syscall.SIGHUP))

	// Phase 3: after a short settle, plaintext must fail and TLS must succeed.
	// Reload is synchronous on the signal handler goroutine, but we still
	// allow a brief window for the goroutine to be scheduled.
	require.Eventually(t, func() bool {
		httpsURL := fmt.Sprintf("https://127.0.0.1:%d/metrics", port)
		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: 3 * time.Second,
		}
		r, err := client.Get(httpsURL)
		if err != nil {
			return false
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		return r.StatusCode == http.StatusOK
	}, 10*time.Second, 100*time.Millisecond, "TLS probe must succeed after SIGHUP")

	// And a plaintext probe must now fail at the protocol layer: the server
	// expects a ClientHello, so the HTTP request line confuses it. We accept
	// either an explicit error or a non-OK status. Reuse plainClient — its
	// transport has DisableKeepAlives, so this Get opens a fresh TCP conn.
	r2, err := plainClient.Get(plaintextURL)
	if err == nil {
		_, _ = io.Copy(io.Discard, r2.Body)
		_ = r2.Body.Close()
		require.NotEqual(t, http.StatusOK, r2.StatusCode,
			"plaintext GET on TLS-active port must not succeed")
	}
}

// writeSelfSignedCertE2E mirrors writeSelfSignedCert from the unit test but
// is duplicated here because the e2e package can't import internal/server.
// Both files are short and the duplication keeps the e2e fixture self-
// contained.
func writeSelfSignedCertE2E(t *testing.T, certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "grainfs-e2e"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &priv.PublicKey, priv)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
}
