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
// References:
//   - T54 (encrypt.ComputeHandshakeResponse / HandshakeVerifier)
//   - T55 (Challenge/Admit RPC plumbing in MetaChallengeSender + JoinRequest fields)
package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/transport"
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
	peer := args[0]
	dataDir, _ := cmd.Flags().GetString("data")
	nodeID, _ := cmd.Flags().GetString("node-id")
	bindAddr, _ := cmd.Flags().GetString("bind-addr")
	clusterKey, _ := cmd.Flags().GetString("cluster-key")
	timeout, _ := cmd.Flags().GetDuration("timeout")

	if nodeID == "" {
		return fmt.Errorf("--node-id is required")
	}
	if bindAddr == "" {
		return fmt.Errorf("--bind-addr is required")
	}
	if clusterKey == "" {
		return fmt.Errorf("--cluster-key is required (same value as serve --cluster-key on peer)")
	}

	nc := nodeconfig.New(dataDir)
	kek, err := encrypt.LoadOrGenerateKEK(nc.KEKSource())
	if err != nil {
		return fmt.Errorf("load KEK: %w", err)
	}

	qt, err := transport.NewQUICTransport(clusterKey)
	if err != nil {
		return fmt.Errorf("init transport: %w", err)
	}
	defer qt.Close()

	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()

	chalSender := cluster.NewMetaChallengeSender(func(p string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaJoinChallenge, Payload: payload}
		reply, err := qt.Call(ctx, p, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	})
	joinSender := cluster.NewMetaJoinSender(func(p string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaJoin, Payload: payload}
		reply, err := qt.Call(ctx, p, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	})

	return performClusterJoin(ctx, chalSender, joinSender, peer, nodeID, bindAddr, kek, cmd.OutOrStdout())
}

// performClusterJoin runs the Challenge → Join handshake. Factored out for
// test injection: tests pass senders whose dialers route bytes directly into
// in-process MetaChallengeReceiver / MetaJoinReceiver handlers (no QUIC).
func performClusterJoin(
	ctx context.Context,
	chalSender *cluster.MetaChallengeSender,
	joinSender *cluster.MetaJoinSender,
	peer, nodeID, bindAddr string,
	kek []byte,
	out io.Writer,
) error {
	chalReply, err := chalSender.SendChallenge(ctx, peer, cluster.ChallengeRequest{NodeID: nodeID})
	if err != nil {
		return fmt.Errorf("challenge: %w", err)
	}
	if chalReply.Status != cluster.JoinStatusOK {
		return fmt.Errorf("challenge refused: %s: %s", chalReply.Status, chalReply.Message)
	}

	resp := encrypt.ComputeHandshakeResponse(kek, chalReply.Nonce)

	joinReply, err := joinSender.SendJoin(ctx, []string{peer}, cluster.JoinRequest{
		NodeID:            nodeID,
		Address:           bindAddr,
		HandshakeNonce:    chalReply.Nonce,
		HandshakeResponse: resp,
	})
	if err != nil {
		return fmt.Errorf("join: %w", err)
	}

	switch joinReply.Status {
	case cluster.JoinStatusOK:
		fmt.Fprintln(out, "joined cluster successfully")
		return nil
	case cluster.JoinStatusAlreadyMember:
		fmt.Fprintln(out, "already a cluster member (no-op)")
		return nil
	case cluster.JoinStatusKEKMismatch:
		return fmt.Errorf("join refused: KEK mismatch with peer — restore kek.key by `scp <data>/kek.key` from any healthy node, then retry (%s)", joinReply.Message)
	default:
		if joinReply.Message != "" {
			return fmt.Errorf("join refused: %s: %s", joinReply.Status, joinReply.Message)
		}
		return fmt.Errorf("join refused: %s", joinReply.Status)
	}
}
