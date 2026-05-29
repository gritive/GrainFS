package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestClusterKey_Empty_ReturnsError verifies the cluster-key guard still fires
// when genesis self-seed does NOT apply. On a FRESH data dir an empty key now
// self-seeds (see genesis-cluster-key-self-seed); the guard is reserved for the
// non-genesis cases — here a node with existing raft state (priorState) and no
// key, which must still demand --cluster-key rather than self-seed over identity.
func TestClusterKey_Empty_ReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := t.TempDir()
	// priorState: existing raft state blocks self-seed (condition 5).
	if err := os.MkdirAll(filepath.Join(dataDir, "raft"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "raft", "x"), []byte("y"), 0o600); err != nil {
		t.Fatal(err)
	}
	opts := ServeOptions{
		NodeID:     "node1",
		RaftAddr:   "127.0.0.1:0",
		DataDir:    dataDir,
		ClusterKey: "", // empty cluster-key + priorState → guard fires (no self-seed)
	}
	cfg := optionsToConfig(opts, ":9000", nil, nil, nil)
	err := Run(ctx, cfg)

	if err == nil {
		t.Fatal("expected error for empty clusterKey on a priorState dir")
	}
	if !strings.Contains(err.Error(), "--cluster-key is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
