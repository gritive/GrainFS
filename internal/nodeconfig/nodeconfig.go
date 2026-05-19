package nodeconfig

import (
	"os"
	"path/filepath"
)

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

func (n *NodeConfig) LogLevel() string {
	if v := os.Getenv("GRAINFS_LOG_LEVEL"); v != "" {
		return v
	}
	return "info"
}
