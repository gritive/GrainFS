package main

import (
	"context"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/serveruntime"
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
	cmd.Flags().String("upstream", "", "")
	cmd.Flags().Bool("heal-receipt-enabled", false, "")
	cmd.Flags().String("heal-receipt-psk", "", "")
	return cmd
}

func TestServeCmd_RemovesManualECFlags(t *testing.T) {
	require.Nil(t, serveCmd.Flags().Lookup("ec-data"))
	require.Nil(t, serveCmd.Flags().Lookup("ec-parity"))
}

// TestRunCluster_EmptyClusterKey_ReturnsError verifies the cluster-key guard
// added in A7. serveruntime.Run must refuse to start when --cluster-key is empty.
func TestRunCluster_EmptyClusterKey_ReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := newClusterTestCmd("")
	cfg := buildClusterConfig(cmd, ":9000", t.TempDir(), "node1", "127.0.0.1:0", "", nil, nil)
	err := serveruntime.Run(ctx, cfg)

	if err == nil {
		t.Fatal("expected error for empty clusterKey")
	}
	if !strings.Contains(err.Error(), "--cluster-key is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
