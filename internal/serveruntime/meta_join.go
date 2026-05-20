package serveruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// PerformMetaJoin issues the §7 cluster-join handshake — Challenge → compute
// HMAC-SHA256(KEK, nonce) → Join — against each peer in turn until one
// accepts the join. Used by --join-mode bootstrap so a fresh node can
// register itself with the meta-raft leader without operator intervention.
//
// kek is the local 32-byte KEK loaded by wireDEKKeeper. It is required: a
// joiner with no KEK cannot answer the Challenge under the cluster's KEK and
// would be refused with JoinStatusKEKMismatch anyway. B2 (§7).
func PerformMetaJoin(ctx context.Context, quicTransport *transport.QUICTransport, peers []string, nodeID, raftAddr string, kek []byte) error {
	if len(kek) == 0 {
		return fmt.Errorf("meta join: KEK is required for the challenge-response handshake")
	}
	joinCtx, cancel := context.WithTimeout(ctx, 75*time.Second)
	defer cancel()
	chalSender := cluster.NewMetaChallengeSender(func(peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaJoinChallenge, Payload: payload}
		reply, err := quicTransport.Call(joinCtx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	})
	joinSender := cluster.NewMetaJoinSender(func(peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaJoin, Payload: payload}
		reply, err := quicTransport.Call(joinCtx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	})

	var lastErr error
	for _, peer := range peers {
		if err := joinCtx.Err(); err != nil {
			return err
		}
		chalReply, err := chalSender.SendChallenge(joinCtx, peer, cluster.ChallengeRequest{NodeID: nodeID})
		if err != nil {
			lastErr = fmt.Errorf("meta join challenge to %s: %w", peer, err)
			continue
		}
		if chalReply.Status != cluster.JoinStatusOK {
			lastErr = fmt.Errorf("meta join challenge refused by %s: %s: %s", peer, chalReply.Status, chalReply.Message)
			continue
		}
		response := encrypt.ComputeHandshakeResponse(kek, chalReply.Nonce)
		reply, err := joinSender.SendJoin(joinCtx, []string{peer}, cluster.JoinRequest{
			NodeID:            nodeID,
			Address:           raftAddr,
			HandshakeNonce:    chalReply.Nonce,
			HandshakeResponse: response,
		})
		if err != nil {
			lastErr = fmt.Errorf("meta join: %w", err)
			continue
		}
		if !reply.Accepted {
			if reply.Status == cluster.JoinStatusKEKMismatch {
				return fmt.Errorf("meta join refused: KEK mismatch with peer %s — restore kek.key by `scp <peer>:<data>/kek.key` from any healthy node, then retry (%s)", peer, reply.Message)
			}
			if reply.Message != "" {
				return fmt.Errorf("meta join rejected: %s: %s", reply.Status, reply.Message)
			}
			return fmt.Errorf("meta join rejected: %s", reply.Status)
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("meta join: no peers")
}
