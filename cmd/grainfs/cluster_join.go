package main

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/transport"
)

var clusterJoinCmd = &cobra.Command{
	Use:   "join <peer-address>",
	Short: "Join this node to an existing meta-Raft cluster",
	Long: `Sends a join request to the given peer address.
The peer must be the current leader; non-leader peers return ErrNotLeader.
The local MetaRaft node must have been bootstrapped first (grainfs serve starts it automatically).`,
	Args: cobra.ExactArgs(1),
	RunE: runClusterJoin,
}

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

	quicTransport := transport.NewQUICTransport(clusterKey)
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

	// TODO(PR-E): 현재 로컬 MetaRaft 준비만 완료. 실제 참가는 리더에게
	// "AddLearner + PromoteToVoter + ProposeAddNode"를 수행하도록 admin RPC
	// (SendJoin 스트림 타입)를 추가해야 한다. MetaTransport에 JoinRequest
	// 엔드포인트 추가 및 MetaTransportQUIC 구현은 PR-E scope.
	fmt.Printf("local meta-raft ready (node-id: %s); leader join via PR-E admin RPC pending\n", nodeID)
	return nil
}

func init() {
	clusterJoinCmd.Flags().String("data-dir", "", "data directory (required)")
	clusterJoinCmd.Flags().String("node-id", "", "node ID (auto-generated from data-dir if empty)")
	clusterJoinCmd.Flags().String("raft-addr", "", "local Raft address reachable by peers (required)")
	clusterJoinCmd.Flags().String("cluster-key", "", "shared cluster encryption key")
	clusterCmd.AddCommand(clusterJoinCmd)
}
