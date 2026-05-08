package serveruntime

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// bootQUICTransport resolves the cluster key (disk wins over flag, ephemeral
// when both empty in solo mode), constructs the QUIC transport, applies bulk
// traffic limits for forwarded S3 PUT fan-outs, and Listens on raftAddr. On
// success state.quicTransport is populated, state.raftAddr is updated to the
// kernel-picked port (when operator passed 127.0.0.1:0), and Close is queued
// on the cleanup stack.
//
// Cluster key resolution order (rotation-spec D10):
//  1. keys.d/current.key wins over --cluster-key flag if both differ (warn).
//  2. Disk only: use disk silently.
//  3. Flag only: use flag, mirror to keys.d/current.key on first boot.
//  4. Both empty + cluster mode: rejected upstream by ValidateClusterKey.
//     Both empty + solo mode: generate ephemeral so zero-config holds.
func bootQUICTransport(ctx context.Context, state *bootState) error {
	resolvedKey, warn, err := ResolveClusterKey(state.cfg.DataDir, state.cfg.ClusterKey)
	if err != nil {
		return fmt.Errorf("resolve cluster key: %w", err)
	}
	if warn != "" {
		log.Warn().Msg(warn)
	}
	state.transportPSK = resolvedKey
	if state.transportPSK == "" {
		ephemeral, err := GenerateEphemeralClusterKey()
		if err != nil {
			return fmt.Errorf("init QUIC transport: %w", err)
		}
		state.transportPSK = ephemeral
	}

	quicTransport, err := transport.NewQUICTransport(state.transportPSK)
	if err != nil {
		return fmt.Errorf("init QUIC transport: %w", err)
	}
	// Forwarded S3 PUTs can fan out into EC shard body streams on the bucket
	// owner. Keep enough bulk capacity for that nested data path while meta
	// and raft traffic remain independently classed.
	quicTransport.SetTrafficLimits(transport.TrafficLimits{Bulk: 64})
	if err := quicTransport.Listen(ctx, state.raftAddr); err != nil {
		return fmt.Errorf("start QUIC transport on %s: %w\n  recovery: confirm UDP port is free (lsof -i UDP:%s), check firewall, or pass --raft-addr=127.0.0.1:0 to pick any free port", state.raftAddr, err, state.raftAddr)
	}
	state.quicTransport = quicTransport
	state.AddCleanup(func() { quicTransport.Close() })

	// Resolve raftAddr to the actual bound port. When the operator asked for
	// 127.0.0.1:0 (singleton default) QUIC picks a free UDP port; we need
	// that concrete address for shard placement so peers see a dialable self.
	if local := quicTransport.LocalAddr(); local != "" {
		state.raftAddr = local
	}
	return nil
}

// bootPeerConnections opens a QUIC connection to each peer. Connection
// failures are logged but non-fatal — the transport retries lazily on the
// first send. Empty peer list (solo mode) is a clean no-op.
func bootPeerConnections(ctx context.Context, state *bootState) error {
	for _, peer := range state.peers {
		if err := state.quicTransport.Connect(ctx, peer); err != nil {
			log.Warn().Str("peer", peer).Err(err).Msg("failed to connect to peer (will retry lazily)")
		}
	}
	return nil
}

// bootGroupRaftMux constructs the GroupRaftQUICMux that multiplexes per-group
// raft RPCs over StreamGroupRaft. Must run BEFORE NewMetaTransportQUICMux so
// the meta-raft transport can auto-register its node on the mux at
// construction time. If the mux were created later, a startup race would let
// inbound meta calls hit "mux: unknown group __meta__" and stall meta
// election (codex P1 #3).
func bootGroupRaftMux(state *bootState) error {
	state.groupRaftMux = raft.NewGroupRaftQUICMux(state.quicTransport)
	if state.cfg.QUICMuxEnabled {
		state.groupRaftMux.EnableMux(state.cfg.QUICMuxPoolSize, state.cfg.QUICMuxFlushWindow)
		log.Info().
			Int("pool", state.cfg.QUICMuxPoolSize).
			Dur("flush", state.cfg.QUICMuxFlushWindow).
			Msg("group raft mux mode enabled (R+H Phase 2 prototype)")
	}
	return nil
}
