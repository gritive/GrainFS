package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"strings"
	"time"
)

// sanScheme is the URI scheme used to carry node identity in the cert SAN:
//
//	grainfs://<cluster-id>/<node-id>
//
// This makes TLS connections attributable to a specific node in logs/audits
// (spec D4) instead of every node presenting CommonName "grainfs-cluster".
const sanScheme = "grainfs"

// GenerateNodeIdentity creates a UNIQUE, RANDOM per-node ECDSA P-256 identity
// (spec D1) and a self-signed cert whose SAN carries the node's identity URI
// (spec D4). Unlike DeriveClusterIdentity, the key is from crypto/rand, so two
// calls — even for the same node-id — produce different keys. Persist the
// result with SealNodeKey (later task) so it is stable across restarts.
//
// Returns (cert, spki, err) where spki = sha256(RawSubjectPublicKeyInfo).
func GenerateNodeIdentity(clusterID, nodeID string) (tls.Certificate, [32]byte, error) {
	if clusterID == "" || nodeID == "" {
		return tls.Certificate{}, [32]byte{}, errors.New("clusterID and nodeID are required")
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("generate node key: %w", err)
	}

	sanURI := &url.URL{Scheme: sanScheme, Host: clusterID, Path: "/" + nodeID}
	now := time.Now().UTC()
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: clusterID},
		URIs:         []*url.URL{sanURI},
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(100 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("create node cert: %w", err)
	}
	parsed, err := x509.ParseCertificate(certDER)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("parse node cert: %w", err)
	}
	spki := sha256.Sum256(parsed.RawSubjectPublicKeyInfo)

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  priv,
		Leaf:        parsed,
	}, spki, nil
}

// NodeIDFromCert extracts (clusterID, nodeID) from a cert's grainfs SAN URI.
// Returns an error if no grainfs:// SAN is present (e.g. a legacy
// DeriveClusterIdentity cert).
func NodeIDFromCert(leaf *x509.Certificate) (clusterID, nodeID string, err error) {
	if leaf == nil {
		return "", "", errors.New("nil cert")
	}
	for _, u := range leaf.URIs {
		if u.Scheme != sanScheme {
			continue
		}
		node := strings.TrimPrefix(u.Path, "/")
		if u.Host == "" || node == "" {
			return "", "", fmt.Errorf("malformed grainfs SAN %q", u.String())
		}
		return u.Host, node, nil
	}
	return "", "", errors.New("cert has no grainfs:// SAN")
}
