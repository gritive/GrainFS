// §7 T56 — `grainfs cluster join <peer>` CLI.
//
// This is the offline-bootstrap join path: a NOT-YET-RUNNING node uses this
// command to handshake directly with an existing cluster peer over QUIC.
// The handshake proves KEK possession (HMAC-SHA256 challenge-response) before
// the leader admits the joiner via AddVoter.
//
// Compare with `grainfs join` (cmd/grainfs/join.go) which goes through the
// admin UDS of an already-running node (runtime restart-into-join). Both
// exist intentionally — different operational scenarios.
//
// Thin-runner shape: KEK load + QUIC transport + handshake state machine
// live in internal/cluster.PerformOfflineJoin. This file only owns Cobra
// wiring + flag validation.
package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/cluster"
)

func clusterJoinCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "join <peer-addr>",
		Short: "Join an existing cluster via KEK challenge-response handshake (offline bootstrap)",
		Long: `Performs the §7 cluster-join handshake against <peer-addr> directly over
QUIC. Intended for first-time bootstrap of a not-yet-running node: loads the
local KEK from <data>/kek.key (or $GRAINFS_KEK_SOURCE), runs Challenge to
get a fresh nonce from the peer, computes HMAC-SHA256(KEK, nonce), and sends
Join with the response. The peer's MetaJoinReceiver verifies the response
under its own KEK and admits the node via AddVoter only on match.

For runtime restart-into-join on an already-running node, use ` + "`grainfs join`" + ` (which
talks to that node's admin UDS).

Example:
  grainfs cluster join 192.168.1.10:7001 \
    --data /var/grainfs/data \
    --node-id node-2 \
    --bind-addr 192.168.1.11:7001 \
    --cluster-key $GRAINFS_CLUSTER_KEY`,
		Args: cobra.ExactArgs(1),
		RunE: runClusterJoin,
	}
	cmd.Flags().StringP("data", "d", "./data", "data directory (used to locate kek.key)")
	cmd.Flags().String("node-id", "", "unique node ID for the joining node (required)")
	cmd.Flags().String("bind-addr", "", "raft listen address the joining node will advertise (required)")
	cmd.Flags().String("cluster-key", "", "pre-shared cluster key (required; same as serve --cluster-key)")
	cmd.Flags().Duration("timeout", 30*time.Second, "join request timeout")
	return cmd
}

func runClusterJoin(cmd *cobra.Command, args []string) error {
	timeout, _ := cmd.Flags().GetDuration("timeout")
	nodeID, _ := cmd.Flags().GetString("node-id")
	bindAddr, _ := cmd.Flags().GetString("bind-addr")
	clusterKey, _ := cmd.Flags().GetString("cluster-key")
	dataDir, _ := cmd.Flags().GetString("data")
	if nodeID == "" {
		return fmt.Errorf("--node-id is required")
	}
	if bindAddr == "" {
		return fmt.Errorf("--bind-addr is required")
	}
	if clusterKey == "" {
		return fmt.Errorf("--cluster-key is required (same value as serve --cluster-key on peer)")
	}
	return cluster.PerformOfflineJoin(cmd.Context(), cluster.OfflineJoinOptions{
		Peer:       args[0],
		NodeID:     nodeID,
		BindAddr:   bindAddr,
		ClusterKey: clusterKey,
		DataDir:    dataDir,
		Timeout:    timeout,
		Stdout:     cmd.OutOrStdout(),
	})
}
