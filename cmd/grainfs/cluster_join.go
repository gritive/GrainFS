package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/transport"
)

var clusterJoinCmd = &cobra.Command{
	Use:   "join <peer-address>",
	Short: "Join this node to an existing meta-Raft cluster",
	Long: `Sends a join request to the given peer address.
If the peer is not the current leader, the command follows the leader hint when
the peer can provide one. The command starts the local MetaRaft node before
sending the join request.`,
	Args: cobra.ExactArgs(1),
	RunE: runClusterJoin,
}

var runClusterJoinNode = runClusterJoinNodeReal

func runClusterJoin(cmd *cobra.Command, args []string) error {
	peerAddr := args[0]
	dataDir, _ := cmd.Flags().GetString("data-dir")
	nodeID, _ := cmd.Flags().GetString("node-id")
	raftAddr, _ := cmd.Flags().GetString("raft-addr")
	clusterKey, _ := cmd.Flags().GetString("cluster-key")

	if dataDir == "" {
		return fmt.Errorf("--data-dir is required")
	}
	if nodeID == "" {
		nodeID = generateNodeID(dataDir)
	}
	if raftAddr == "" {
		return fmt.Errorf("--raft-addr is required to join a cluster (must be reachable by peers)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := runClusterJoinNode(ctx, peerAddr, dataDir, nodeID, raftAddr, clusterKey); err != nil {
		return err
	}
	fmt.Printf("meta-raft join accepted (node-id: %s)\n", nodeID)
	return nil
}

func runClusterJoinNodeReal(ctx context.Context, peerAddr, dataDir, nodeID, raftAddr, clusterKey string) error {
	if err := transport.ValidateClusterKey(clusterKey); err != nil {
		if errors.Is(err, transport.ErrEmptyClusterKey) {
			return fmt.Errorf("--cluster-key is required in cluster mode (generate with: openssl rand -hex 32)")
		}
		log.Warn().Err(err).Msg("--cluster-key is below recommended length")
	}
	quicTransport, err := transport.NewQUICTransport(clusterKey)
	if err != nil {
		return fmt.Errorf("init QUIC transport: %w", err)
	}
	if err := quicTransport.Listen(ctx, raftAddr); err != nil {
		return fmt.Errorf("start QUIC transport: %w", err)
	}
	defer quicTransport.Close()

	if err := quicTransport.Connect(ctx, peerAddr); err != nil {
		return fmt.Errorf("connect to peer %s: %w", peerAddr, err)
	}

	metaRaft, err := cluster.NewMetaRaft(cluster.MetaRaftConfig{
		NodeID:  nodeID,
		Peers:   []string{peerAddr},
		DataDir: dataDir,
	})
	if err != nil {
		return fmt.Errorf("init meta-raft: %w", err)
	}

	metaTransport := cluster.NewMetaTransportQUIC(quicTransport, metaRaft.Node())
	metaRaft.SetTransport(metaTransport)

	if err := metaRaft.Bootstrap(); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	if err := metaRaft.Start(ctx); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	defer metaRaft.Close()

	return performMetaJoin(ctx, quicTransport, []string{peerAddr}, nodeID, raftAddr)
}

func init() {
	clusterJoinCmd.Flags().String("data-dir", "", "data directory (required)")
	clusterJoinCmd.Flags().String("node-id", "", "node ID (auto-generated from data-dir if empty)")
	clusterJoinCmd.Flags().String("raft-addr", "", "local Raft address reachable by peers (required)")
	clusterJoinCmd.Flags().String("cluster-key", "", "shared cluster encryption key")
	clusterCmd.AddCommand(clusterJoinCmd)
}
