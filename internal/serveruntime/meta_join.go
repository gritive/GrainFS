package serveruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

// PerformMetaJoin issues the §7 cluster-join handshake — Challenge →
// compute HMAC-SHA256(K_active, transcript) → Join — against each peer
// in turn until one accepts the join. Used by --join-mode bootstrap so
// a fresh node can register itself with the meta-raft leader without
// operator intervention.
//
// store is the local KEKStore loaded by wireDEKKeeper. clusterID is the
// 16-byte cluster identity persisted at <dataDir>/cluster.id. Both are
// required: a joiner with no KEK cannot answer the Challenge under the
// cluster's KEK and would be refused with JoinStatusKEKMismatch anyway.
//
// Phase A pins joiner_version and leader_active_version to 0 on both
// sides; the wire-level transcript exchange is Phase C scope.
func PerformMetaJoin(ctx context.Context, quicTransport *transport.QUICTransport, peers []string, nodeID, raftAddr string, store *encrypt.KEKStore, clusterID []byte) error {
	if store == nil {
		return fmt.Errorf("meta join: KEK store is required for the challenge-response handshake")
	}
	if len(clusterID) != 16 {
		return fmt.Errorf("meta join: cluster_id must be 16 bytes, got %d", len(clusterID))
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
		transcript := encrypt.JoinTranscript{
			ClusterID:           clusterID,
			Nonce:               chalReply.Nonce,
			NodeID:              nodeID,
			Address:             raftAddr,
			JoinerVersion:       0,
			LeaderActiveVersion: 0,
		}
		response, err := encrypt.ComputeHandshakeResponse(store, store.ActiveVersion(), transcript)
		if err != nil {
			lastErr = fmt.Errorf("meta join: compute handshake response: %w", err)
			continue
		}
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
				return fmt.Errorf("meta join refused: KEK mismatch with peer %s — restore the cluster KEK by `scp <peer>:<data>/keys/0.key <data>/keys/` from any healthy node, then retry (%s)", peer, reply.Message)
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
