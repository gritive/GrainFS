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

// nodeKeyGenFile records the KEK generation node.key.enc was sealed under.
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
// node.key.enc is sealed under the active KEK generation, with node.key.gen as
// the durable generation pointer. encKey is optional and retained only for
// one-way migration from directories that were sealed under the old static
// encryption.key path.
//
// Behavior:
//   - node.key.enc ABSENT: generate a fresh identity, seal under active KEK,
//     persist node.key.gen, return SPKI.
//   - node.key.enc PRESENT with node.key.gen: reload under that generation and
//     re-seal under active KEK when stale.
//   - node.key.enc PRESENT without node.key.gen but decrypts under encKey:
//     migrate old static-key state to active KEK and persist node.key.gen.
//   - node.key.enc PRESENT, nothing decrypts: return an error.
//
// It NEVER regenerates when node.key.enc exists: a fresh key changes the SPKI
// and the registry node-id-rebind guard would reject the later re-registration
// (partition).
func ensureNodeIdentity(dataDir, clusterID, nodeID string, encKey []byte, kekStore *encrypt.KEKStore) (cert tls.Certificate, spki [32]byte, nodeKeyKEKGen uint32, err error) {
	activeGen, activeKEK, err := activeNodeKeyKEK(kekStore)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, 0, fmt.Errorf("ensureNodeIdentity: %w", err)
	}

	encPath := filepath.Join(dataDir, "keys.d", nodeKeyEncFile)
	if _, statErr := os.Stat(encPath); statErr == nil {
		return reloadNodeIdentity(dataDir, encKey, kekStore, activeGen, activeKEK)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return tls.Certificate{}, [32]byte{}, 0, fmt.Errorf("ensureNodeIdentity: stat node key: %w", statErr)
	}

	// Absent: generate + seal under the active KEK generation.
	cert, spki, err = transport.GenerateNodeIdentity(clusterID, nodeID)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, 0, fmt.Errorf("ensureNodeIdentity: generate identity: %w", err)
	}
	if err := sealNodeKeyAtGen(dataDir, activeGen, activeKEK, cert); err != nil {
		return tls.Certificate{}, [32]byte{}, 0, fmt.Errorf("ensureNodeIdentity: seal node key: %w", err)
	}
	return cert, spki, activeGen, nil
}

// reloadNodeIdentity loads an existing node.key.enc. node.key.gen is the
// canonical KEK-generation pointer; the static encKey path exists only to
// migrate directories written by the previous static-key slice.
func reloadNodeIdentity(dataDir string, encKey []byte, kekStore *encrypt.KEKStore, activeGen uint32, activeKEK []byte) (tls.Certificate, [32]byte, uint32, error) {
	if sealedGen, ok := readNodeKeyGen(dataDir); ok {
		kek, err := kekStore.Get(sealedGen)
		if err != nil {
			if cert, spki, migrated, migrateErr := tryMigrateStaticNodeKey(dataDir, encKey, activeGen, activeKEK); migrateErr != nil {
				return tls.Certificate{}, [32]byte{}, 0, migrateErr
			} else if migrated {
				return cert, spki, activeGen, nil
			}
			return tls.Certificate{}, [32]byte{}, 0, fmt.Errorf("load node key KEK gen %d: %w", sealedGen, err)
		}
		cert, spki, err := transport.LoadNodeKey(dataDir, kek)
		if err != nil {
			if cert, spki, migrated, migrateErr := tryMigrateStaticNodeKey(dataDir, encKey, activeGen, activeKEK); migrateErr != nil {
				return tls.Certificate{}, [32]byte{}, 0, migrateErr
			} else if migrated {
				return cert, spki, activeGen, nil
			}
			return tls.Certificate{}, [32]byte{}, 0, fmt.Errorf("load node key sealed under KEK gen %d: %w", sealedGen, err)
		}
		if sealedGen != activeGen {
			if err := sealNodeKeyAtGen(dataDir, activeGen, activeKEK, cert); err != nil {
				return tls.Certificate{}, [32]byte{}, 0, fmt.Errorf("re-seal node key under active KEK gen %d: %w", activeGen, err)
			}
			return cert, spki, activeGen, nil
		}
		return cert, spki, sealedGen, nil
	}

	if cert, spki, migrated, err := tryMigrateStaticNodeKey(dataDir, encKey, activeGen, activeKEK); err != nil {
		return tls.Certificate{}, [32]byte{}, 0, err
	} else if migrated {
		return cert, spki, activeGen, nil
	}
	// NEVER regenerate: a fresh key changes the SPKI and partitions the node.
	return tls.Certificate{}, [32]byte{}, 0, errors.New("ensureNodeIdentity: node.key.enc present but no usable node.key.gen or static-key migration path could decrypt it")
}

func activeNodeKeyKEK(kekStore *encrypt.KEKStore) (uint32, []byte, error) {
	if kekStore == nil {
		return 0, nil, errors.New("KEK store not wired")
	}
	activeGen := kekStore.ActiveVersion()
	kek, err := kekStore.Get(activeGen)
	if err != nil {
		return 0, nil, fmt.Errorf("load active KEK gen %d: %w", activeGen, err)
	}
	return activeGen, kek, nil
}

func sealNodeKeyAtGen(dataDir string, gen uint32, kek []byte, cert tls.Certificate) error {
	if err := transport.SealNodeKey(dataDir, kek, cert); err != nil {
		return err
	}
	if err := writeNodeKeyGen(dataDir, gen); err != nil {
		return err
	}
	return nil
}

func tryMigrateStaticNodeKey(dataDir string, encKey []byte, activeGen uint32, activeKEK []byte) (tls.Certificate, [32]byte, bool, error) {
	if len(encKey) != 32 {
		return tls.Certificate{}, [32]byte{}, false, nil
	}
	cert, spki, err := transport.LoadNodeKey(dataDir, encKey)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, false, nil
	}
	if err := sealNodeKeyAtGen(dataDir, activeGen, activeKEK, cert); err != nil {
		return tls.Certificate{}, [32]byte{}, false, fmt.Errorf("migrate static-sealed node key to active KEK gen %d: %w", activeGen, err)
	}
	return cert, spki, true, nil
}
