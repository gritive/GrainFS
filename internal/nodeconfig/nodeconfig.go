package nodeconfig

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/google/uuid"
)

// ClusterIDFile is the filename within dataDir that stores the 16-byte
// cluster identity (UUID v7 raw bytes).
const ClusterIDFile = "cluster.id"

// ErrClusterIDMissing indicates the cluster.id file is absent on a node
// that is joining an existing cluster (where it MUST be copied from a
// healthy peer, not auto-generated). Distinct from a first-boot init
// path which uses LoadOrInitClusterID.
var ErrClusterIDMissing = errors.New("cluster.id not found")

type NodeConfig struct {
	dataDir string
}

func New(dataDir string) *NodeConfig { return &NodeConfig{dataDir: dataDir} }

func (n *NodeConfig) TLSCertPath() string {
	if v := os.Getenv("GRAINFS_TLS_CERT"); v != "" {
		return v
	}
	return filepath.Join(n.dataDir, "tls", "cert.pem")
}

func (n *NodeConfig) TLSKeyPath() string {
	if v := os.Getenv("GRAINFS_TLS_KEY"); v != "" {
		return v
	}
	return filepath.Join(n.dataDir, "tls", "key.pem")
}

// KEKDir returns the directory that holds keys/<V>.key files. Honors the
// GRAINFS_KEK_DIR env var override (tests, custom layouts).
func (n *NodeConfig) KEKDir() string {
	if v := os.Getenv("GRAINFS_KEK_DIR"); v != "" {
		return v
	}
	return filepath.Join(n.dataDir, "keys")
}

func (n *NodeConfig) LogLevel() string {
	if v := os.Getenv("GRAINFS_LOG_LEVEL"); v != "" {
		return v
	}
	return "info"
}

// LoadOrInitClusterID loads the 16-byte cluster identity from
// <dataDir>/cluster.id, or generates a fresh UUID v7 and persists it on
// first boot. Returns exactly 16 raw bytes.
//
// First-boot semantics: if the file is absent, a UUID v7 is generated and
// written with mode 0o600 via atomic rename. Joiners are expected to
// receive this file out-of-band (operator scp's it alongside the active
// KEK before `cluster join` runs). If the file is present but does not
// contain exactly 16 bytes, returns an error rather than silently
// overwriting — operator must intervene.
func (n *NodeConfig) LoadOrInitClusterID() ([]byte, error) {
	path := filepath.Join(n.dataDir, ClusterIDFile)
	data, err := os.ReadFile(path)
	if err == nil {
		if len(data) != 16 {
			return nil, fmt.Errorf("nodeconfig: %s has %d bytes, want 16", path, len(data))
		}
		return data, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("nodeconfig: read %s: %w", path, err)
	}
	// First boot: generate UUID v7 and persist atomically with the same
	// durability guarantees as KEKStore writeKEKFileAtomic — O_EXCL + fsync
	// + atomic rename so a crash mid-write cannot leave a partial file.
	// Durably create dataDir if absent — POSIX requires fsync of the parent
	// to make a freshly-created directory entry survive a crash. Skip the
	// fsync when the dir already existed (no new entry to durably commit).
	if _, err := os.Stat(n.dataDir); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("nodeconfig: stat %s: %w", n.dataDir, err)
		}
		if err := os.MkdirAll(n.dataDir, 0o700); err != nil {
			return nil, fmt.Errorf("nodeconfig: mkdir %s: %w", n.dataDir, err)
		}
		if err := fsyncDir(filepath.Dir(n.dataDir)); err != nil {
			return nil, fmt.Errorf("nodeconfig: durability fsync after mkdir %s: %w", n.dataDir, err)
		}
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("nodeconfig: generate cluster.id: %w", err)
	}
	raw := id[:]
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("nodeconfig: open tmp %s: %w", tmp, err)
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		os.Remove(tmp)
		return nil, fmt.Errorf("nodeconfig: write tmp %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return nil, fmt.Errorf("nodeconfig: fsync tmp %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return nil, fmt.Errorf("nodeconfig: close tmp %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return nil, fmt.Errorf("nodeconfig: rename %s: %w", path, err)
	}
	if err := fsyncDir(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("nodeconfig: durability fsync after rename %s: %w", path, err)
	}
	// Defensive copy so caller cannot mutate the underlying uuid bytes.
	out := make([]byte, 16)
	copy(out, raw)
	return out, nil
}

// LoadClusterID loads cluster.id from disk WITHOUT auto-generating.
// Returns ErrClusterIDMissing if the file is absent. Used by join paths
// where a missing cluster.id should be a clear operator error ("scp
// cluster.id from a healthy peer") rather than silently fabricating a
// fresh identity that fails the handshake later.
func (n *NodeConfig) LoadClusterID() ([]byte, error) {
	path := filepath.Join(n.dataDir, ClusterIDFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s — copy from a healthy peer alongside the active KEK", ErrClusterIDMissing, path)
		}
		return nil, fmt.Errorf("nodeconfig: read %s: %w", path, err)
	}
	if len(data) != 16 {
		return nil, fmt.Errorf("nodeconfig: %s has %d bytes, want 16", path, len(data))
	}
	return data, nil
}

// fsyncDir opens the directory at path and fsyncs it, ensuring that any
// preceding rename into that directory is durable across crashes. POSIX
// requires this for the rename to survive a power loss.
func fsyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open dir %q for fsync: %w", path, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir %q: %w", path, err)
	}
	return nil
}
