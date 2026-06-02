package serveruntime

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// bootClusterTransport resolves the cluster key (disk wins over flag, ephemeral
// when both empty in solo mode), constructs the cluster transport (TCP by default
// after the S5c-3 flip; QUIC under the `--transport quic` opt-out), applies bulk
// traffic limits for forwarded S3 PUT fan-outs, and Listens on raftAddr. On
// success state.quicTransport is populated, state.raftAddr is updated to the
// kernel-picked port (when operator passed 127.0.0.1:0), and Close is queued
// on the cleanup stack.
//
// Cluster key resolution order (rotation-spec D10):
//  1. keys.d/current.key wins over cluster-key flag if both differ (warn).
//  2. Disk only: use disk silently.
//  3. Flag only: use flag, mirror to keys.d/current.key on first boot.
//  4. Both empty + cluster mode: rejected upstream by ValidateClusterKey.
//     Both empty + solo mode: generate ephemeral so zero-config holds.
func bootClusterTransport(ctx context.Context, state *bootState) error {
	resolvedKey, warn, err := ResolveClusterKey(state.cfg.DataDir, state.cfg.ClusterKey, state.cfg)
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
			return fmt.Errorf("init cluster transport: %w", err)
		}
		state.transportPSK = ephemeral
	}

	// TCP is the sole cluster transport (S6 removed the legacy QUIC stack). All
	// post-construction setup below is transport-agnostic (ClusterTransport interface
	// methods); state.quicTransport is the interface-typed field (legacy name).
	var clusterTransport transport.ClusterTransport
	tcpTransport, terr := transport.NewTCPTransport(state.transportPSK)
	if terr != nil {
		return fmt.Errorf("init cluster transport: %w", terr)
	}
	clusterTransport = tcpTransport
	// Forwarded S3 PUTs can fan out into EC shard body streams on the bucket
	// owner. Keep enough bulk capacity for that nested data path while meta
	// and raft traffic remain independently classed.
	clusterTransport.SetTrafficLimits(transport.TrafficLimits{Bulk: 64})
	if err := applyPostDropInviteJoinIdentity(state, clusterTransport); err != nil {
		return err
	}
	// M3: pre-seed peer SPKIs from the invite-join bootstrap so the joiner
	// accepts incumbents' per-node certs from the first inbound handshake
	// (PR-2a §8f). No-op on normal (non-invite) boot and when peer_spkis is
	// empty (rolling-upgrade compat — old leader omitted the field, M5).
	if state.inviteJoin != nil && len(state.inviteJoin.peerSPKIs) > 0 {
		clusterTransport.SeedInitialPeerSPKIs(state.inviteJoin.peerSPKIs)
	}
	if err := clusterTransport.Listen(ctx, state.raftAddr); err != nil {
		return fmt.Errorf("start cluster transport on %s: %w\n  recovery: confirm the port is free (lsof -i :%s), check firewall, or pass --raft-addr=127.0.0.1:0 to pick any free port", state.raftAddr, err, state.raftAddr)
	}
	state.quicTransport = clusterTransport
	state.AddCleanup(func() { clusterTransport.Close() })

	// Resolve raftAddr to the actual bound port. When the operator asked for
	// 127.0.0.1:0 (singleton default) the transport picks a free port; we need
	// that concrete address for shard placement so peers see a dialable self.
	if local := clusterTransport.LocalAddr(); local != "" {
		state.raftAddr = local
	}
	return nil
}

type postDropInviteJoinTransport interface {
	FlipPresent(cert tls.Certificate, spki [32]byte)
	SetDropped()
}

func applyPostDropInviteJoinIdentity(state *bootState, quicTransport postDropInviteJoinTransport) error {
	if state == nil || quicTransport == nil || state.inviteJoin == nil || !state.inviteJoin.clusterKeyDropped {
		return nil
	}
	cert, spki, err := loadPostDropInviteNodeKey(state)
	if err != nil {
		return fmt.Errorf("post-drop invite-join: load per-node cert: %w", err)
	}
	quicTransport.FlipPresent(cert, spki)
	quicTransport.SetDropped()
	state.perNodeCert = cert
	state.perNodeSPKI = spki
	state.perNodeKeyKEKGen = state.inviteJoin.nodeKeyKEKGen
	log.Info().Msg("post-drop invite-join: per-node cert pinned before Listen")
	return nil
}

func loadPostDropInviteNodeKey(state *bootState) (tls.Certificate, [32]byte, error) {
	kek, err := loadInviteNodeKeyKEKFromDisk(state.cfg.DataDir, state.inviteJoin.nodeKeyKEKGen)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, err
	}
	return transport.LoadNodeKey(state.cfg.DataDir, kek)
}

func loadInviteNodeKeyKEKFromDisk(dataDir string, gen uint32) ([]byte, error) {
	path := filepath.Join(nodeconfig.New(dataDir).KEKDir(), fmt.Sprintf("%d.key", gen))
	kek, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read KEK gen %d: %w", gen, err)
	}
	if len(kek) != encrypt.KEKSize {
		return nil, fmt.Errorf("KEK gen %d len = %d, want %d", gen, len(kek), encrypt.KEKSize)
	}
	return kek, nil
}

// bootPeerConnections opens a cluster-transport connection to each peer.
// Connection failures are logged but non-fatal — the transport retries lazily on
// the first send. Empty peer list (solo mode) is a clean no-op.
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
