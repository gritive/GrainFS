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
	"fmt"
	"io"
	"os"
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
	// Phase A no longer honors GRAINFS_KEK_SOURCE — the keystore is always
	// at <dataDir>/keys/<V>.key (configurable via GRAINFS_KEK_DIR for tests).
	// Refuse the env var consistently across boot paths so the operator gets
	// the same error regardless of mode.
	if v := os.Getenv("GRAINFS_KEK_SOURCE"); v != "" {
		return fmt.Errorf("offline-join: GRAINFS_KEK_SOURCE is no longer supported (was: %q). Phase A uses <dataDir>/keys/<V>.key. Unset GRAINFS_KEK_SOURCE and stage your KEK at <dataDir>/keys/0.key (override the directory with GRAINFS_KEK_DIR if needed).", v)
	}

	nc := nodeconfig.New(opts.DataDir)
	keysDir := nc.KEKDir()
	// §7 T57 (F#21): a joining node MUST already hold the cluster's KEK
	// in place — auto-generating a fresh random KEK here would produce a
	// node whose KEK can never decrypt the cluster's FSM-wrapped DEKs and
	// whose Challenge handshake would fail anyway. Pre-flight checks the
	// keys/ directory is non-empty before LoadOrInitKEKStoreDir, which
	// otherwise auto-generates v0 on an empty directory.
	if empty, err := encrypt.KeysDirIsEmpty(keysDir); err != nil {
		return fmt.Errorf("offline-join: stat %s: %w", keysDir, err)
	} else if empty {
		return fmt.Errorf("KEK not found at %s. "+
			"A joining node must already hold the cluster's KEK before "+
			"`cluster join` can complete the challenge-response handshake. "+
			"Copy the cluster KEK from any healthy peer (e.g. `scp peer:%s/0.key %s/`) "+
			"and re-run cluster join.",
			keysDir, keysDir, keysDir)
	}
	store, err := encrypt.LoadOrInitKEKStoreDir(keysDir)
	if err != nil {
		return fmt.Errorf("offline-join: load keystore %s: %w", keysDir, err)
	}
	clusterID, err := nc.LoadClusterID()
	if err != nil {
		return fmt.Errorf("offline-join: cluster.id: %w", err)
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

	return runOfflineJoinHandshakeV2(ctx, chalSender, joinSender, opts.Peer, opts.NodeID, opts.BindAddr, store, clusterID, opts.Stdout)
}

// runOfflineJoinHandshakeV2 runs the Challenge → Join state machine using
// a local KEKStore + cluster_id for transcript construction. Factored out
// for test injection: tests pass senders whose dialers route bytes
// directly into in-process MetaChallengeReceiver / MetaJoinReceiver
// handlers (no QUIC).
//
// Phase A pins joiner_version and leader_active_version to 0 on both
// sides; the full wire-level transcript exchange is Phase C scope.
func runOfflineJoinHandshakeV2(
	ctx context.Context,
	chalSender *MetaChallengeSender,
	joinSender *MetaJoinSender,
	peer, nodeID, bindAddr string,
	store *encrypt.KEKStore,
	clusterID []byte,
	out io.Writer,
) error {
	chalReply, err := chalSender.SendChallenge(ctx, peer, ChallengeRequest{NodeID: nodeID})
	if err != nil {
		return fmt.Errorf("challenge: %w", err)
	}
	if chalReply.Status != JoinStatusOK {
		return fmt.Errorf("challenge refused: %s: %s", chalReply.Status, chalReply.Message)
	}

	transcript := encrypt.JoinTranscript{
		ClusterID:           clusterID,
		Nonce:               chalReply.Nonce,
		NodeID:              nodeID,
		Address:             bindAddr,
		JoinerVersion:       0,
		LeaderActiveVersion: 0,
	}
	resp, err := encrypt.ComputeHandshakeResponse(store, store.ActiveVersion(), transcript)
	if err != nil {
		return fmt.Errorf("compute handshake response: %w", err)
	}

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
		return fmt.Errorf("join refused: KEK mismatch with peer — ensure <data>/keys/0.key matches the active KEK on the cluster and <data>/cluster.id matches the cluster's identity file (scp both from any healthy peer), then retry (%s)", joinReply.Message)
	default:
		if joinReply.Message != "" {
			return fmt.Errorf("join refused: %s: %s", joinReply.Status, joinReply.Message)
		}
		return fmt.Errorf("join refused: %s", joinReply.Status)
	}
}
