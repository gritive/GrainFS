package serveruntime

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage/wal"
	"github.com/gritive/GrainFS/internal/transport"
)

// bootWALAndForwarders opens the WAL, builds the v0.0.7.1 PR-D ForwardSender +
// ForwardReceiver, the meta-propose forward sender/receiver pair, the
// meta-catalog read sender, the meta-join receiver, and the
// ClusterCoordinator. Also performs the join-mode meta-join + initial Router
// sync.
//
// Inputs:  state.cfg.DataDir, state.quicTransport, state.metaRaft,
//
//	state.streamRouter, state.dgMgr, state.clusterRouter, state.shardSvc,
//	state.distBackend, state.nodeID, state.raftAddr, state.peers,
//	state.effectiveEC, state.cfg.JoinMode.
//
// Outputs: state.wal, state.walDir, state.forwardSender, state.forwardReceiver,
//
//	state.metaForwardSender, state.metaReadSender, state.clusterCoord,
//	state.seedGroups.
//
// Cleanup: state.AddCleanup closes the WAL.
//
// Ordering: must run AFTER bootBalancerAndGossip (no direct dep, but matches
// run.go); MUST run BEFORE bootBackendWrap (which wraps state.distBackend
// through the wal.Backend).
func bootWALAndForwarders(ctx context.Context, state *bootState) error {
	state.walDir = filepath.Join(state.cfg.DataDir, "wal")
	w, err := wal.Open(state.walDir)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	state.wal = w
	state.AddCleanup(func() { w.Close() })

	// Seed data groups from cluster size only. Operators no longer choose this:
	// group count is placement headroom, not a durability policy.
	clusterSize := 1 + len(state.peers)
	seedGroups := clusterSize * 4
	if seedGroups < 8 {
		seedGroups = 8
	}
	state.seedGroups = seedGroups

	// v0.0.7.1 PR-D: Live multi-raft routing — ClusterCoordinator + ForwardSender/Receiver.
	// ClusterCoordinator implements storage.Backend and routes bucket-scoped ops to the
	// correct group leader via ForwardSender. 0x08 handler (ForwardReceiver) receives
	// forwarded calls on voter nodes and dispatches to local GroupBackend.
	quicTransport := state.quicTransport
	metaRaft := state.metaRaft
	peers := state.peers

	forwardDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}
		reply, err := quicTransport.Call(callCtx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	forwardStreamDialer := func(callCtx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamGroupForwardBody, Payload: payload}
		reply, err := quicTransport.CallWithBody(callCtx, peer, msg, body)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	forwardReadStreamDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, io.ReadCloser, error) {
		msg := &transport.Message{Type: transport.StreamGroupForwardRead, Payload: payload}
		reply, body, err := quicTransport.CallRead(callCtx, peer, msg)
		if err != nil {
			return nil, nil, err
		}
		return reply.Payload, body, nil
	}

	state.forwardSender = cluster.NewForwardSender(forwardDialer).
		WithStreamDialer(forwardStreamDialer).
		WithReadStreamDialer(forwardReadStreamDialer).
		WithLeaderHintResolver(func(hint string) string {
			if addr, ok := cluster.ResolveNodeAddress(metaRaft.FSM(), hint); ok {
				return addr
			}
			return hint
		})
	state.forwardReceiver = cluster.NewForwardReceiver(state.dgMgr)
	state.forwardReceiver.Register(state.shardSvc)

	metaForwardDialer := func(peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaProposeForward, Payload: payload}
		reply, err := quicTransport.Call(ctx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	state.metaForwardSender = cluster.NewMetaProposeForwardSender(metaForwardDialer)

	// MetaRaft.Propose follower→leader forwarding: when the local node is not
	// the meta-Raft leader, forward encoded MetaCmd bytes to the current leader
	// via the existing QUIC forwardPropose RPC. Address resolution uses the
	// meta-FSM node map (same pattern as ForwardSender.WithLeaderHintResolver).
	state.metaRaft.SetForwarder(func(ctx context.Context, data []byte) (uint64, error) {
		leaderID := state.metaRaft.LeaderID()
		if leaderID == "" {
			return 0, fmt.Errorf("meta_raft forward: no leader known")
		}
		addr, ok := cluster.ResolveNodeAddress(state.metaRaft.FSM(), leaderID)
		if !ok {
			return 0, fmt.Errorf("meta_raft forward: cannot resolve leader %q address", leaderID)
		}
		return state.distBackend.ForwardPropose(ctx, addr, data)
	})

	state.distBackend.SetBucketAssigner(cluster.NewForwardingBucketAssigner(metaRaft, func(ctx context.Context, command []byte) error {
		return state.metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
	}))
	metaForwardReceiver := cluster.NewMetaProposeForwardReceiver(metaRaft)
	state.streamRouter.Handle(transport.StreamMetaProposeForward, metaForwardReceiver.Handle)
	metaJoinReceiver := cluster.NewMetaJoinReceiver(metaRaft)
	state.streamRouter.Handle(transport.StreamMetaJoin, metaJoinReceiver.Handle)
	metaReadDialer := func(peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaCatalogRead, Payload: payload}
		reply, err := quicTransport.Call(ctx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	state.metaReadSender = cluster.NewMetaCatalogReadSender(metaReadDialer)

	state.clusterCoord = cluster.NewClusterCoordinator(
		state.distBackend, // base for cluster-wide ops (CreateBucket, etc.)
		state.dgMgr,       // local owned groups (self-leader shortcut)
		state.clusterRouter,
		metaRaft.FSM(), // ShardGroupSource (PeerIDs, leader hints)
		state.nodeID,   // selfID for leader check
	).WithForwardSender(state.forwardSender).
		WithNodeAddressResolver(metaRaft.FSM()).
		WithSelfPeerAlias(state.raftAddr).
		WithECConfig(state.effectiveEC).
		WithObjectIndexProposer(cluster.NewForwardingObjectIndexProposer(metaRaft, func(ctx context.Context, command []byte) error {
			return state.metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
		}))
	metaReadReceiver := cluster.NewMetaCatalogReadReceiver(cluster.NewMetaCatalog(metaRaft, state.clusterCoord, "s3://grainfs-tables/warehouse"))
	state.streamRouter.Handle(transport.StreamMetaCatalogRead, metaReadReceiver.Handle)

	if state.cfg.JoinMode {
		if err := PerformMetaJoin(ctx, quicTransport, peers, state.nodeID, state.raftAddr); err != nil {
			return err
		}
		if err := WaitForShardGroupCount(ctx, metaRaft.FSM(), seedGroups, 30*time.Second); err != nil {
			return err
		}
		state.clusterRouter.Sync(metaRaft.FSM().BucketAssignments())
		state.clusterRouter.SetRequireExplicitAssignments(true)
	}

	log.Info().Msg("v0.0.7.1 PR-D: ClusterCoordinator wired — live multi-raft routing enabled")
	return nil
}
