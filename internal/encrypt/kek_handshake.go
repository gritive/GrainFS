// Package encrypt: cluster-identity + KEK-store holder for the Zero-CA
// invite-join path.
//
// This file formerly hosted the KEK challenge-response handshake (a joiner
// proved it held the cluster KEK by returning HMAC-SHA256(K_version, transcript)
// over an issued nonce). That mechanism was retired together with the legacy
// cluster-join path (offline `cluster join`, runtime `grainfs join`, and the
// `.join-pending` boot mode). Invite-join proves KEK possession differently: the
// leader seals the bootstrap secrets (KEK generations + transport PSK) to the
// joiner's per-node identity key, so only the intended joiner can open them.
//
// What remains is a lightweight holder of the KEKStore and the 16-byte
// cluster_id, read by the invite path to bind the invite transcript
// (ClusterID()) and to look up the active KEK version (Store()). The type name
// is retained for call-site stability.
package encrypt

import "fmt"

// HandshakeVerifier holds the node's KEKStore and the 16-byte cluster identity.
type HandshakeVerifier struct {
	store     *KEKStore
	clusterID []byte
}

// NewHandshakeVerifier returns a holder bound to the given store and cluster_id.
func NewHandshakeVerifier(store *KEKStore, clusterID []byte) *HandshakeVerifier {
	if len(clusterID) != 16 {
		panic(fmt.Sprintf("NewHandshakeVerifier: cluster_id must be 16 bytes, got %d", len(clusterID)))
	}
	return &HandshakeVerifier{
		store:     store,
		clusterID: append([]byte(nil), clusterID...),
	}
}

// Store returns the holder's KEKStore. The invite path uses it to look up the
// active KEK version.
func (v *HandshakeVerifier) Store() *KEKStore { return v.store }

// ClusterID returns a copy of the 16-byte cluster_id this holder is bound to.
// The invite path binds it into the invite transcript.
func (v *HandshakeVerifier) ClusterID() []byte {
	out := make([]byte, len(v.clusterID))
	copy(out, v.clusterID)
	return out
}
