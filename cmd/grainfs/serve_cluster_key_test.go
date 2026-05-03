package main

import (
	"context"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func newClusterTestCmd(clusterKey string) *cobra.Command {
	cmd := &cobra.Command{Use: "serve"}
	cmd.Flags().String("cluster-key", clusterKey, "")
	cmd.Flags().String("peers", "127.0.0.1:7001,127.0.0.1:7002", "")
	cmd.Flags().String("node-id", "test-node", "")
	cmd.Flags().String("raft-addr", "127.0.0.1:0", "")
	cmd.Flags().String("join", "", "")
	cmd.Flags().Int("nfs-port", 0, "")
	cmd.Flags().Int("nfs4-port", 0, "")
	cmd.Flags().Int("nbd-port", 0, "")
	cmd.Flags().Bool("no-encryption", true, "")
	cmd.Flags().String("encryption-key-file", "", "")
	cmd.Flags().Int("ec-data", 4, "")
	cmd.Flags().Int("ec-parity", 2, "")
	cmd.Flags().String("upstream", "", "")
	cmd.Flags().Bool("heal-receipt-enabled", false, "")
	cmd.Flags().String("heal-receipt-psk", "", "")
	return cmd
}

// TestRunCluster_EmptyClusterKey_ReturnsError verifies the runCluster guard
// added in A7. The function must refuse to start when --cluster-key is empty.
func TestRunCluster_EmptyClusterKey_ReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := newClusterTestCmd("")
	err := runCluster(
		ctx, cmd,
		":9000", t.TempDir(), "node1", "127.0.0.1:0",
		"127.0.0.1:7001,127.0.0.1:7002",
		"", // clusterKey
		nil, nil,
	)

	if err == nil {
		t.Fatal("expected error for empty clusterKey")
	}
	if !strings.Contains(err.Error(), "--cluster-key is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
