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
