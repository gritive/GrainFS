package transport

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// nodeKeyFile is the encrypted per-node identity slot inside keys.d/.
const nodeKeyFile = "node.key.enc"

// fsyncDir fsyncs a directory so a preceding rename is durable across crashes.
// (Sibling keystore.go has its own copy; minor duplication is an accepted
// follow-up rather than a shared helper.)
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// SealNodeKey persists cert's ECDSA private key to {dataDir}/keys.d/node.key.enc,
// encrypted under the 32-byte node-local KEK via AES-256-GCM (spec D7). Plaintext
// key is PKCS#8 PEM before sealing; on-disk file is ciphertext only. Atomic
// temp+rename, mode 0600, O_NOFOLLOW — mirrors keystore.go.
func SealNodeKey(dataDir string, kek []byte, cert tls.Certificate) error {
	priv, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		return fmt.Errorf("SealNodeKey: cert.PrivateKey is %T, want *ecdsa.PrivateKey", cert.PrivateKey)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return fmt.Errorf("SealNodeKey: marshal key: %w", err)
	}
	// Best-effort zeroize of the serialized private-key buffers. Note:
	// *ecdsa.PrivateKey.D itself cannot be cleared in pure Go.
	defer func() {
		for i := range der {
			der[i] = 0
		}
	}()
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	defer func() {
		for i := range pemBytes {
			pemBytes[i] = 0
		}
	}()
	sealed, err := encrypt.AESGCMSeal(kek, pemBytes)
	if err != nil {
		return fmt.Errorf("SealNodeKey: seal: %w", err)
	}
	keysDir := filepath.Join(dataDir, "keys.d")
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		return fmt.Errorf("SealNodeKey: mkdir: %w", err)
	}
	if err := os.Chmod(keysDir, 0o700); err != nil {
		return fmt.Errorf("SealNodeKey: chmod dir: %w", err)
	}
	path := filepath.Join(keysDir, nodeKeyFile)
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("SealNodeKey: create tmp: %w", err)
	}
	if _, err := f.Write(sealed); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("SealNodeKey: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("SealNodeKey: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("SealNodeKey: close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("SealNodeKey: rename: %w", err)
	}
	return fsyncDir(keysDir)
}

// LoadNodeKey reads and decrypts the per-node identity, rebuilding a
// tls.Certificate (with Leaf). Returns os.ErrNotExist when the slot is absent
// (so callers can keygen-on-first-boot). Wrong KEK surfaces as a GCM auth error.
func LoadNodeKey(dataDir string, kek []byte) (tls.Certificate, [32]byte, error) {
	path := filepath.Join(dataDir, "keys.d", nodeKeyFile)
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, err // includes os.ErrNotExist
	}
	defer f.Close()
	// Bound the read: a sealed P-256 PKCS#8 key is a few hundred bytes. Cap
	// generously so a corrupt/oversized/tampered file can't blow up memory
	// before GCM rejects it.
	const maxSealedNodeKey = 64 << 10
	sealed, err := io.ReadAll(io.LimitReader(f, maxSealedNodeKey+1))
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadNodeKey: read: %w", err)
	}
	if len(sealed) > maxSealedNodeKey {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadNodeKey: sealed key exceeds %d bytes (corrupt or tampered)", maxSealedNodeKey)
	}
	pemBytes, err := encrypt.AESGCMOpen(kek, sealed)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadNodeKey: open (wrong KEK or corrupt): %w", err)
	}
	// Best-effort zeroize of the decrypted private-key buffer. Note:
	// *ecdsa.PrivateKey.D itself cannot be cleared in pure Go.
	defer func() {
		for i := range pemBytes {
			pemBytes[i] = 0
		}
	}()
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadNodeKey: no PEM block")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadNodeKey: parse key: %w", err)
	}
	priv, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("LoadNodeKey: key is %T, want *ecdsa.PrivateKey", key)
	}
	cert, spki, err := certFromKey(priv)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, err
	}
	return cert, spki, nil
}

// certFromKey rebuilds a minimal self-signed cert (no SAN) for a loaded key.
// What must survive restart is the KEY (and thus the SPKI); the reloaded cert
// is SAN-less today.
// TODO(phase-2): re-apply the grainfs:// SAN from node config when wiring
// LoadNodeKey into startup — no caller re-applies it yet, so a reloaded cert
// currently carries no node identity for logs/audit (D4). Likely changes this
// signature to take (clusterID, nodeID) or seals a SAN sidecar as AEAD AAD.
func certFromKey(priv *ecdsa.PrivateKey) (tls.Certificate, [32]byte, error) {
	now := time.Now().UTC()
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(100 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("certFromKey: create: %w", err)
	}
	parsed, err := x509.ParseCertificate(certDER)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("certFromKey: parse: %w", err)
	}
	spki := sha256.Sum256(parsed.RawSubjectPublicKeyInfo)
	return tls.Certificate{Certificate: [][]byte{certDER}, PrivateKey: priv, Leaf: parsed}, spki, nil
}
