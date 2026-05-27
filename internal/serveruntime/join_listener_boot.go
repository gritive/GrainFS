package serveruntime

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/transport"
)

// joinListenerHandlerTimeout bounds a single invite-handler round (Raft commit
// budget mirrors cluster.metaJoinTimeout, which is unexported; 60s matches it).
const joinListenerHandlerTimeout = 60 * time.Second

// startJoinListener loads-or-creates the persisted stable join-listener cert,
// starts a transport.JoinListener on the resolved address, and stores it on
// state (closed on shutdown via AddCleanup). The handler reads the framed
// JoinRequest off the stream, runs the two-phase invite handler against the
// TLS-captured peer SPKI, and writes the framed JoinReply back — all binary
// (no JSON), delegated to MetaJoinReceiver.HandleJoinStream.
func startJoinListener(state *bootState, receiver *cluster.MetaJoinReceiver) error {
	cert, spki, err := LoadOrCreateJoinListenerCert(state.cfg.DataDir)
	if err != nil {
		return err
	}
	addr := resolveJoinListenAddr(state.cfg.JoinListenAddr, state.raftAddr)
	handler := func(handlerCtx context.Context, peerSPKI [32]byte, stream *quic.Stream) {
		ctx, cancel := context.WithTimeout(handlerCtx, joinListenerHandlerTimeout)
		defer cancel()
		receiver.HandleJoinStream(ctx, peerSPKI, stream)
	}
	ln, err := transport.NewJoinListener(addr, cert, handler)
	if err != nil {
		return err
	}
	state.joinListener = ln
	state.AddCleanup(func() { _ = ln.Close() })
	log.Info().
		Str("addr", ln.Addr()).
		Hex("spki", spki[:]).
		Msg("zero-CA join listener started (leader-side invite handler)")
	return nil
}

// JoinListenerAddr returns the resolved bind address of the Zero-CA join
// listener, or "" when not started (single-node). Exposed for W10 invite-bundle
// advertisement.
func (s *bootState) JoinListenerAddr() string {
	if s.joinListener == nil {
		return ""
	}
	return s.joinListener.Addr()
}

// JoinListenerSPKI returns the persisted-stable SPKI of the Zero-CA join
// listener cert, or the zero value when not started. Exposed for W10 (the
// joiner pins this SPKI from the invite bundle).
func (s *bootState) JoinListenerSPKI() [32]byte {
	if s.joinListener == nil {
		return [32]byte{}
	}
	return s.joinListener.SPKI()
}

// joinListenerKeyFile is the persisted, stable join-listener identity slot under
// keys.d/. UNLIKE node.key.enc it is NOT KEK-sealed: the join listener's SPKI
// must be derivable before any cluster KEK is available (a joiner pins this SPKI
// from the invite bundle, and outstanding bundles pin it across leader
// restarts), so the key is stored as plain PKCS#8 PEM at mode 0600.
const joinListenerKeyFile = "join_listener.key"

// LoadOrCreateJoinListenerCert loads the persisted join-listener ECDSA P-256
// identity from <dataDir>/keys.d/join_listener.key, creating it on first call.
// The returned cert + SPKI are STABLE across restarts (same on-disk key →
// same SPKI), which outstanding invite bundles rely on. The on-disk key is
// plain PKCS#8 PEM (mode 0600); see joinListenerKeyFile for why it is not
// KEK-sealed.
func LoadOrCreateJoinListenerCert(dataDir string) (tls.Certificate, [32]byte, error) {
	keysDir := filepath.Join(dataDir, "keys.d")
	path := filepath.Join(keysDir, joinListenerKeyFile)

	pemBytes, err := os.ReadFile(path)
	if err == nil {
		return joinListenerCertFromPEM(pemBytes)
	}
	if !os.IsNotExist(err) {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadOrCreateJoinListenerCert: read: %w", err)
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadOrCreateJoinListenerCert: generate key: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadOrCreateJoinListenerCert: marshal key: %w", err)
	}
	pemBytes = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := writeJoinListenerKey(keysDir, path, pemBytes); err != nil {
		return tls.Certificate{}, [32]byte{}, err
	}
	return joinListenerCertFromKey(priv)
}

// writeJoinListenerKey writes pemBytes to path atomically (temp+rename),
// mode 0600, O_NOFOLLOW, with a directory fsync — mirrors node_keystore.go.
func writeJoinListenerKey(keysDir, path string, pemBytes []byte) error {
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		return fmt.Errorf("LoadOrCreateJoinListenerCert: mkdir: %w", err)
	}
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("LoadOrCreateJoinListenerCert: create tmp: %w", err)
	}
	if _, err := f.Write(pemBytes); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("LoadOrCreateJoinListenerCert: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("LoadOrCreateJoinListenerCert: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("LoadOrCreateJoinListenerCert: close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("LoadOrCreateJoinListenerCert: rename: %w", err)
	}
	d, err := os.Open(keysDir)
	if err != nil {
		return fmt.Errorf("LoadOrCreateJoinListenerCert: open dir: %w", err)
	}
	defer d.Close()
	return d.Sync()
}

func joinListenerCertFromPEM(pemBytes []byte) (tls.Certificate, [32]byte, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadOrCreateJoinListenerCert: no PEM block")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadOrCreateJoinListenerCert: parse key: %w", err)
	}
	priv, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadOrCreateJoinListenerCert: key is %T, want *ecdsa.PrivateKey", key)
	}
	return joinListenerCertFromKey(priv)
}

// joinListenerCertFromKey rebuilds a deterministic self-signed cert for priv.
// The SPKI = sha256(RawSubjectPublicKeyInfo) depends only on the public key, so
// it is stable across restarts regardless of the (regenerated) cert envelope.
func joinListenerCertFromKey(priv *ecdsa.PrivateKey) (tls.Certificate, [32]byte, error) {
	now := time.Now().UTC()
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(100 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadOrCreateJoinListenerCert: create cert: %w", err)
	}
	parsed, err := x509.ParseCertificate(certDER)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadOrCreateJoinListenerCert: parse cert: %w", err)
	}
	spki := sha256.Sum256(parsed.RawSubjectPublicKeyInfo)
	return tls.Certificate{Certificate: [][]byte{certDER}, PrivateKey: priv, Leaf: parsed}, spki, nil
}

// resolveJoinListenAddr returns the address the join listener should bind to.
// When cfg.JoinListenAddr is set it is used verbatim; otherwise it derives a
// kernel-picked port on the raftAddr host (host:0). SPKI pinning protects the
// joiner regardless of the chosen port, so a derived ephemeral port is safe;
// the resolved listener.Addr() is what W10 advertises in the invite bundle.
func resolveJoinListenAddr(joinListenAddr, raftAddr string) string {
	if joinListenAddr != "" {
		return joinListenAddr
	}
	host := "127.0.0.1"
	if raftAddr != "" {
		if h, _, err := net.SplitHostPort(raftAddr); err == nil && h != "" {
			host = h
		}
	}
	return net.JoinHostPort(host, "0")
}
