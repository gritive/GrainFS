package serveruntime

import (
	"context"
	"strings"
	"testing"
)

// TestClusterKey_Empty_ReturnsError verifies the cluster-key guard.
// Since clusterMode is always true, ClusterKey is required in all modes.
//
// Relocated from cmd/grainfs/serve_cluster_key_test.go as part of Task 5b
// (pre-cutover): the original test exercised buildClusterConfig, but after
// the cmd thin-runner refactor (Task 6) the cobra-free entry point is
// optionsToConfig in this package.
func TestClusterKey_Empty_ReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := t.TempDir()
	opts := ServeOptions{
		NodeID:     "node1",
		RaftAddr:   "127.0.0.1:0",
		DataDir:    dataDir,
		ClusterKey: "", // empty cluster-key triggers the guard
	}
	cfg := optionsToConfig(opts, ":9000", nil, nil, nil, nil)
	err := Run(ctx, cfg)

	if err == nil {
		t.Fatal("expected error for empty clusterKey")
	}
	if !strings.Contains(err.Error(), "--cluster-key is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
