package serveruntime

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// nodeKeyGenFile is the legacy sidecar that recorded the KEK generation
// node.key.enc was sealed under (Phase 2). It is no longer written: node.key.enc
// is now sealed under the STATIC encryption.key (which never rotates or prunes),
// so there is no generation to pin. The back-compat migration deletes any
// leftover sidecar after re-sealing an old KEK-gen-sealed key.
const nodeKeyGenFile = "node.key.gen"

// nodeKeyEncFile mirrors transport.nodeKeyFile (unexported there) — the sealed
// per-node identity slot. Used only to probe presence; LoadNodeKey/SealNodeKey
// own the canonical path.
const nodeKeyEncFile = "node.key.enc"

// ensureNodeIdentity guarantees this node has a persisted, reloadable per-node
// ECDSA transport identity (spec §6 D-rev3 step 1). Genesis/normal-boot nodes
// otherwise have no steady per-node identity; this seals one once and reloads it
// thereafter. It NEVER changes what the node presents on the wire — it only
// persists the identity so Task 6 (self-register) can bind the SPKI.
//
// node.key.enc is sealed under the STATIC encryption.key (encKey, the 32-byte
// bulk-data key). That key never rotates or prunes and is loaded at boot, so the
// identity is decryptable on every restart — unlike a pinned KEK generation,
// which a rotate+prune could delete and brick the node (code-gate P1).
//
// kekStore is retained ONLY for the back-compat path: keys that an invite-join
// sealed under a KEK generation (Phase 2) before this change. It may be nil for
// test/dev configs without a KEK store; back-compat is then skipped.
//
// Behavior:
//   - node.key.enc ABSENT: generate a fresh identity, seal under encKey, return
//     SPKI. No sidecar is written.
//   - node.key.enc PRESENT, decrypts under encKey: reload, return its SPKI.
//   - node.key.enc PRESENT, encKey fails, a retained KEK gen decrypts (old
//     invite-join key): RE-SEAL under encKey (migrate), delete any stale
//     node.key.gen, return SPKI.
//   - node.key.enc PRESENT, nothing decrypts: return an error.
//
// It NEVER regenerates when node.key.enc exists: a fresh key changes the SPKI
// and the registry node-id-rebind guard would reject the later re-registration
// (partition).
func ensureNodeIdentity(dataDir, clusterID, nodeID string, encKey []byte, kekStore *encrypt.KEKStore) (cert tls.Certificate, spki [32]byte, err error) {
	if len(encKey) != 32 {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("ensureNodeIdentity: encryption key must be 32 bytes, got %d", len(encKey))
	}

	encPath := filepath.Join(dataDir, "keys.d", nodeKeyEncFile)
	if _, statErr := os.Stat(encPath); statErr == nil {
		return reloadNodeIdentity(dataDir, encKey, kekStore)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("ensureNodeIdentity: stat node key: %w", statErr)
	}

	// Absent: generate + seal under the static encryption.key.
	cert, spki, err = transport.GenerateNodeIdentity(clusterID, nodeID)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("ensureNodeIdentity: generate identity: %w", err)
	}
	if err := transport.SealNodeKey(dataDir, encKey, cert); err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("ensureNodeIdentity: seal node key: %w", err)
	}
	return cert, spki, nil
}

// reloadNodeIdentity loads an existing node.key.enc. It first tries the static
// encryption.key (the steady-state seal). On failure it falls back to retained
// KEK gens (back-compat for keys an invite-join sealed under a gen before the
// static-key seal existed); on a gen hit it re-seals under encKey, deletes any
// stale node.key.gen, and returns the SPKI.
func reloadNodeIdentity(dataDir string, encKey []byte, kekStore *encrypt.KEKStore) (tls.Certificate, [32]byte, error) {
	if cert, spki, err := transport.LoadNodeKey(dataDir, encKey); err == nil {
		return cert, spki, nil
	}

	// Back-compat: the key may have been sealed under a KEK gen (Phase 2). Try
	// retained gens newest-first, then migrate the survivor to encKey.
	if kekStore != nil {
		versions := kekStore.Versions() // ascending
		for i := len(versions) - 1; i >= 0; i-- {
			gen := versions[i]
			kek, err := kekStore.Get(gen)
			if err != nil {
				continue
			}
			cert, spki, err := transport.LoadNodeKey(dataDir, kek)
			if err != nil {
				continue // wrong KEK (GCM auth failure) — try the next gen.
			}
			// Migrate: re-seal under the static encryption.key so future boots
			// load it directly and never depend on a prunable KEK gen.
			if err := transport.SealNodeKey(dataDir, encKey, cert); err != nil {
				return tls.Certificate{}, [32]byte{}, fmt.Errorf("ensureNodeIdentity: re-seal node key under encryption key: %w", err)
			}
			if err := deleteNodeKeyGen(dataDir); err != nil {
				return tls.Certificate{}, [32]byte{}, err
			}
			return cert, spki, nil
		}
	}
	// NEVER regenerate: a fresh key changes the SPKI and partitions the node.
	return tls.Certificate{}, [32]byte{}, errors.New("ensureNodeIdentity: node.key.enc present but neither the encryption key nor any retained KEK gen could decrypt it")
}

// deleteNodeKeyGen removes a leftover keys.d/node.key.gen sidecar from the
// Phase-2 KEK-gen seal. Absent is a no-op; the sidecar is obsolete now that
// node.key.enc is sealed under the static encryption.key.
func deleteNodeKeyGen(dataDir string) error {
	path := filepath.Join(dataDir, "keys.d", nodeKeyGenFile)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("ensureNodeIdentity: remove stale node.key.gen: %w", err)
	}
	return nil
}
