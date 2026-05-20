// §7 — Offline cluster-join helper (pushed down from cmd/grainfs).
//
// PerformOfflineJoin runs the not-yet-running-node bootstrap path:
//  1. Load the local KEK (strict — no auto-generate; a joining node MUST
//     already hold the cluster's KEK or its Challenge response can never
//     verify).
//  2. Stand up a QUIC transport against the peer.
//  3. Run the Challenge → Join handshake state machine.
//
// Compare with the runtime restart-into-join path (admin UDS), which is
// owned elsewhere. Both exist intentionally — different operational
// scenarios.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/transport"
)

// OfflineJoinOptions configures PerformOfflineJoin.
type OfflineJoinOptions struct {
	Peer       string
	NodeID     string
	BindAddr   string
	ClusterKey string
	DataDir    string
	Timeout    time.Duration
	Stdout     io.Writer
}

// PerformOfflineJoin executes the offline cluster-join handshake against
// opts.Peer. The caller's ctx is wrapped with opts.Timeout.
func PerformOfflineJoin(ctx context.Context, opts OfflineJoinOptions) error {
	nc := nodeconfig.New(opts.DataDir)
	// §7 T57 (F#21): a joining node MUST already have the cluster's KEK
	// in place — auto-generating a fresh random KEK here would produce a
	// node whose KEK can never decrypt the cluster's FSM-wrapped DEKs and
	// whose Challenge handshake would fail anyway. LoadKEK (strict) makes
	// that failure mode loud and explicit.
	kek, err := encrypt.LoadKEK(nc.KEKSource())
	if err != nil {
		if errors.Is(err, encrypt.ErrKEKNotFound) {
			return fmt.Errorf("KEK not found at %s. "+
				"A joining node must already hold the cluster's KEK before "+
				"`cluster join` can complete the challenge-response handshake. "+
				"Copy kek.key from any healthy peer (e.g. `scp peer:/var/lib/grainfs/kek.key %s`) "+
				"and re-run cluster join. Underlying error: %w",
				nc.KEKSource(), nc.KEKSource()[len("file://"):], err)
		}
		return fmt.Errorf("load KEK: %w", err)
	}

	qt, err := transport.NewQUICTransport(opts.ClusterKey)
	if err != nil {
		return fmt.Errorf("init transport: %w", err)
	}
	defer qt.Close()

	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	chalSender := NewMetaChallengeSender(func(p string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaJoinChallenge, Payload: payload}
		reply, err := qt.Call(ctx, p, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	})
	joinSender := NewMetaJoinSender(func(p string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaJoin, Payload: payload}
		reply, err := qt.Call(ctx, p, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	})

	return runOfflineJoinHandshake(ctx, chalSender, joinSender, opts.Peer, opts.NodeID, opts.BindAddr, kek, opts.Stdout)
}

// runOfflineJoinHandshake runs the Challenge → Join state machine. Factored
// out for test injection: tests pass senders whose dialers route bytes
// directly into in-process MetaChallengeReceiver / MetaJoinReceiver handlers
// (no QUIC).
func runOfflineJoinHandshake(
	ctx context.Context,
	chalSender *MetaChallengeSender,
	joinSender *MetaJoinSender,
	peer, nodeID, bindAddr string,
	kek []byte,
	out io.Writer,
) error {
	chalReply, err := chalSender.SendChallenge(ctx, peer, ChallengeRequest{NodeID: nodeID})
	if err != nil {
		return fmt.Errorf("challenge: %w", err)
	}
	if chalReply.Status != JoinStatusOK {
		return fmt.Errorf("challenge refused: %s: %s", chalReply.Status, chalReply.Message)
	}

	resp := encrypt.ComputeHandshakeResponse(kek, chalReply.Nonce)

	joinReply, err := joinSender.SendJoin(ctx, []string{peer}, JoinRequest{
		NodeID:            nodeID,
		Address:           bindAddr,
		HandshakeNonce:    chalReply.Nonce,
		HandshakeResponse: resp,
	})
	if err != nil {
		return fmt.Errorf("join: %w", err)
	}

	switch joinReply.Status {
	case JoinStatusOK:
		fmt.Fprintln(out, "joined cluster successfully")
		return nil
	case JoinStatusAlreadyMember:
		fmt.Fprintln(out, "already a cluster member (no-op)")
		return nil
	case JoinStatusKEKMismatch:
		return fmt.Errorf("join refused: KEK mismatch with peer — restore kek.key by `scp <data>/kek.key` from any healthy node, then retry (%s)", joinReply.Message)
	default:
		if joinReply.Message != "" {
			return fmt.Errorf("join refused: %s: %s", joinReply.Status, joinReply.Message)
		}
		return fmt.Errorf("join refused: %s", joinReply.Status)
	}
}
