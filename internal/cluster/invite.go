package cluster

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// MintInviteKeypair generates a one-time Ed25519 invite keypair and a random
// invite-id (uuid v7, time-ordered per project preference). The PUBLIC key + id
// + TTL are committed to Raft; the PRIVATE key is the token, shipped to the
// joiner in an InviteBundle. (spec D2/§4.2A)
func MintInviteKeypair() (priv ed25519.PrivateKey, pub ed25519.PublicKey, id string, err error) {
	pub, priv, err = ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, "", fmt.Errorf("mint invite keypair: %w", err)
	}
	u, err := uuid.NewV7()
	if err != nil {
		return nil, nil, "", fmt.Errorf("mint invite id: %w", err)
	}
	return priv, pub, u.String(), nil
}

// InviteBundle is what the operator hands to a joining node out-of-band. It is
// NOT stored server-side; only the public key + id + TTL live in Raft.
type InviteBundle struct {
	InvitePriv ed25519.PrivateKey `json:"invite_priv"`
	InviteID   string             `json:"invite_id"`
	ClusterID  string             `json:"cluster_id"`
	SeedSPKI   [32]byte           `json:"seed_spki"`
}

// EncodeInviteBundle serializes a bundle to a single base64 token string for
// GRAINFS_INVITE_BUNDLE. (JSON inner form is fine — this is operator-facing,
// not the node-to-node wire; FlatBuffers governs the wire only.)
func EncodeInviteBundle(b InviteBundle) string {
	// Marshal cannot fail for this fixed struct (only []byte/string/[32]byte fields).
	raw, _ := json.Marshal(b)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// DecodeInviteBundle parses a GRAINFS_INVITE_BUNDLE token.
func DecodeInviteBundle(token string) (InviteBundle, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return InviteBundle{}, fmt.Errorf("decode invite bundle: %w", err)
	}
	var b InviteBundle
	if err := json.Unmarshal(raw, &b); err != nil {
		return InviteBundle{}, fmt.Errorf("parse invite bundle: %w", err)
	}
	if len(b.InvitePriv) != ed25519.PrivateKeySize || b.InviteID == "" || b.ClusterID == "" {
		return InviteBundle{}, fmt.Errorf("invite bundle missing required fields")
	}
	return b, nil
}
