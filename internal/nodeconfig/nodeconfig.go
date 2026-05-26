package nodeconfig

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

// ClusterIDFile is the filename within dataDir that stores the 16-byte
// cluster identity (UUID v7 raw bytes).
const ClusterIDFile = "cluster.id"

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

func (n *NodeConfig) KEKSource() string {
	if v := os.Getenv("GRAINFS_KEK_SOURCE"); v != "" {
		return v
	}
	return "file://" + filepath.Join(n.dataDir, "kek.key")
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
	if err := os.MkdirAll(n.dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("nodeconfig: mkdir %s: %w", n.dataDir, err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("nodeconfig: generate cluster.id: %w", err)
	}
	raw := id[:]
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return nil, fmt.Errorf("nodeconfig: write tmp %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return nil, fmt.Errorf("nodeconfig: rename %s: %w", path, err)
	}
	// Defensive copy so caller cannot mutate the underlying uuid bytes.
	out := make([]byte, 16)
	copy(out, raw)
	return out, nil
}
