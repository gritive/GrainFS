package serveruntime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// nodeKeyGenFile records, as decimal text, the KEK generation node.key.enc was
// sealed under. It lives alongside node.key.enc in keys.d/ so a later boot can
// reload the per-node identity deterministically under the correct KEK without
// guessing (spec §6 D-rev3 step 1).
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
// Behavior:
//   - node.key.enc present + node.key.gen present: reload under that gen's KEK,
//     return its SPKI.
//   - node.key.enc present + node.key.gen MISSING (invite-joined before this
//     task): try retained KEK gens newest-first until decrypt succeeds, then
//     write node.key.gen with the gen that worked.
//   - node.key.enc ABSENT: generate a fresh identity, seal under the ACTIVE gen,
//     write node.key.gen, return SPKI.
//
// It NEVER regenerates when node.key.enc exists: a fresh key changes the SPKI
// and the registry node-id-rebind guard would reject the later re-registration
// (partition). If no retained gen decrypts an existing key, it returns an error.
func ensureNodeIdentity(dataDir, clusterID, nodeID string, kekStore *encrypt.KEKStore) (spki [32]byte, err error) {
	if kekStore == nil {
		return [32]byte{}, errors.New("ensureNodeIdentity: nil KEK store")
	}

	encPath := filepath.Join(dataDir, "keys.d", nodeKeyEncFile)
	if _, statErr := os.Stat(encPath); statErr == nil {
		return reloadNodeIdentity(dataDir, kekStore)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return [32]byte{}, fmt.Errorf("ensureNodeIdentity: stat node key: %w", statErr)
	}

	// Absent: generate + seal under the active gen.
	activeGen := kekStore.ActiveVersion()
	kek, err := kekStore.Get(activeGen)
	if err != nil {
		return [32]byte{}, fmt.Errorf("ensureNodeIdentity: get active KEK gen %d: %w", activeGen, err)
	}
	cert, spki, err := transport.GenerateNodeIdentity(clusterID, nodeID)
	if err != nil {
		return [32]byte{}, fmt.Errorf("ensureNodeIdentity: generate identity: %w", err)
	}
	if err := transport.SealNodeKey(dataDir, kek, cert); err != nil {
		return [32]byte{}, fmt.Errorf("ensureNodeIdentity: seal node key: %w", err)
	}
	if err := writeNodeKeyGen(dataDir, activeGen); err != nil {
		return [32]byte{}, err
	}
	return spki, nil
}

// reloadNodeIdentity loads an existing node.key.enc. When node.key.gen is
// present it loads under that exact gen; otherwise it tries retained gens
// newest-first (back-compat for keys invite-joined before the sidecar existed)
// and writes node.key.gen with the gen that worked.
func reloadNodeIdentity(dataDir string, kekStore *encrypt.KEKStore) ([32]byte, error) {
	if gen, ok, err := readNodeKeyGen(dataDir); err != nil {
		return [32]byte{}, err
	} else if ok {
		kek, err := kekStore.Get(gen)
		if err != nil {
			return [32]byte{}, fmt.Errorf("ensureNodeIdentity: get KEK gen %d: %w", gen, err)
		}
		_, spki, err := transport.LoadNodeKey(dataDir, kek)
		if err != nil {
			return [32]byte{}, fmt.Errorf("ensureNodeIdentity: load node key under gen %d: %w", gen, err)
		}
		return spki, nil
	}

	// Back-compat: no sidecar. Try retained gens newest-first.
	versions := kekStore.Versions() // ascending
	for i := len(versions) - 1; i >= 0; i-- {
		gen := versions[i]
		kek, err := kekStore.Get(gen)
		if err != nil {
			continue
		}
		_, spki, err := transport.LoadNodeKey(dataDir, kek)
		if err != nil {
			continue // wrong KEK (GCM auth failure) — try the next gen.
		}
		if err := writeNodeKeyGen(dataDir, gen); err != nil {
			return [32]byte{}, err
		}
		return spki, nil
	}
	// NEVER regenerate: a fresh key changes the SPKI and partitions the node.
	return [32]byte{}, errors.New("ensureNodeIdentity: node.key.enc present but no retained KEK gen could decrypt it")
}

// readNodeKeyGen reads keys.d/node.key.gen. Returns (gen, true, nil) when
// present, (0, false, nil) when absent, or an error on a malformed/unreadable
// file.
func readNodeKeyGen(dataDir string) (uint32, bool, error) {
	path := filepath.Join(dataDir, "keys.d", nodeKeyGenFile)
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("ensureNodeIdentity: read node.key.gen: %w", err)
	}
	v, err := strconv.ParseUint(string(trimSpaceASCII(b)), 10, 32)
	if err != nil {
		return 0, false, fmt.Errorf("ensureNodeIdentity: parse node.key.gen: %w", err)
	}
	return uint32(v), true, nil
}

// writeNodeKeyGen atomically persists the KEK gen as decimal text to
// keys.d/node.key.gen (temp+rename, mode 0600, O_NOFOLLOW, dir fsync) mirroring
// SealNodeKey's durability.
func writeNodeKeyGen(dataDir string, gen uint32) error {
	keysDir := filepath.Join(dataDir, "keys.d")
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		return fmt.Errorf("ensureNodeIdentity: mkdir keys.d: %w", err)
	}
	path := filepath.Join(keysDir, nodeKeyGenFile)
	tmp := path + ".tmp"
	_ = os.Remove(tmp)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("ensureNodeIdentity: create node.key.gen tmp: %w", err)
	}
	if _, err := f.WriteString(strconv.FormatUint(uint64(gen), 10) + "\n"); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("ensureNodeIdentity: write node.key.gen: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("ensureNodeIdentity: fsync node.key.gen: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("ensureNodeIdentity: close node.key.gen: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("ensureNodeIdentity: rename node.key.gen: %w", err)
	}
	d, err := os.Open(keysDir)
	if err != nil {
		return fmt.Errorf("ensureNodeIdentity: open keys.d for fsync: %w", err)
	}
	defer d.Close()
	return d.Sync()
}

// trimSpaceASCII trims surrounding ASCII whitespace without pulling in strings.
func trimSpaceASCII(b []byte) []byte {
	start := 0
	for start < len(b) && isASCIISpace(b[start]) {
		start++
	}
	end := len(b)
	for end > start && isASCIISpace(b[end-1]) {
		end--
	}
	return b[start:end]
}

func isASCIISpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
