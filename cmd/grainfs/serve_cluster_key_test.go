package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/serveruntime"
)

func newServeTestCmd(clusterKey string) *cobra.Command {
	cmd := &cobra.Command{Use: "serve"}
	cmd.Flags().String("cluster-key", clusterKey, "")
	cmd.Flags().String("node-id", "test-node", "")
	cmd.Flags().String("raft-addr", "127.0.0.1:0", "")
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
	require.Nil(t, serveCmd.Flags().Lookup("seed-groups"))
}

// TestRunCluster_EmptyClusterKey_ReturnsError verifies the cluster-key guard.
// serveruntime.Run must refuse to start in join mode when --cluster-key is empty.
// Join mode is triggered by a .join-pending file in the data dir.
func TestRunCluster_EmptyClusterKey_ReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := t.TempDir()
	// Write join-pending sentinel to trigger join mode (which requires cluster-key).
	require.NoError(t, os.WriteFile(
		filepath.Join(dataDir, serveruntime.JoinPendingFile),
		[]byte("127.0.0.1:7001"), 0o600))

	cmd := newServeTestCmd("" /* empty cluster-key */)
	cfg := buildClusterConfig(cmd, ":9000", dataDir, "node1", "127.0.0.1:0", "", nil, nil, nil, nil)
	err := serveruntime.Run(ctx, cfg)

	if err == nil {
		t.Fatal("expected error for empty clusterKey")
	}
	if !strings.Contains(err.Error(), "--cluster-key is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
