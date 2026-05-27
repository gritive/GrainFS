package serveruntime

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/compat"
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
//	state.effectiveEC, state.joinMode.
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
	if state.dataWAL == nil {
		return fmt.Errorf("data WAL must be opened before logical WAL/forwarders")
	}
	state.walDir = filepath.Join(state.cfg.DataDir, "wal")
	w, err := wal.OpenEncrypted(state.walDir, state.cfg.Encryptor)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	state.wal = w
	state.AddCleanup(func() { w.Close() })

	// Seed data groups from cluster size only. Operators no longer choose this:
	// group count is placement headroom, not a durability policy.
	clusterSize := 1 + len(state.peers)
	seedGroups := seedGroupCountForClusterSize(clusterSize)
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
		WithReadinessRetry(5 * time.Second).
		WithLeaderHintResolver(func(hint string) string {
			if addr, ok := cluster.ResolveNodeAddress(metaRaft.FSM(), hint); ok {
				return addr
			}
			return hint
		})
	indexProposer := cluster.NewForwardingObjectIndexProposer(metaRaft, func(ctx context.Context, command []byte) error {
		return state.metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
	}).WithIndexForwarder(func(ctx context.Context, command []byte) (uint64, error) {
		return state.metaForwardSender.SendWithIndex(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
	})
	state.forwardReceiver = cluster.NewForwardReceiver(state.dgMgr).
		WithObjectIndexProposer(indexProposer)
	state.forwardReceiver.Register(state.shardSvc)

	metaForwardDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaProposeForward, Payload: payload}
		reply, err := quicTransport.Call(callCtx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	state.metaForwardSender = cluster.NewMetaProposeForwardSender(metaForwardDialer)

	// MetaRaft.Propose follower→leader forwarding: when the local node is not
	// the meta-Raft leader, forward encoded MetaCmd bytes to the current leader
	// via StreamMetaProposeForward (the same path iceberg commits use).
	metaRaft.SetForwarder(func(ctx context.Context, data []byte) error {
		return state.metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), data)
	})
	metaRaft.SetForwarderWithIndex(func(ctx context.Context, data []byte) (uint64, error) {
		return state.metaForwardSender.SendWithIndex(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), data)
	})
	metaRaft.SetForwarderWithGate(func(ctx context.Context, data []byte, plan compat.GatePlan) (uint64, error) {
		return state.metaForwardSender.SendWithGate(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), data, plan)
	})

	state.distBackend.SetBucketAssigner(cluster.NewForwardingBucketAssigner(metaRaft, func(ctx context.Context, command []byte) error {
		return state.metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
	}))
	metaForwardReceiver := cluster.NewMetaProposeForwardReceiver(metaRaft).
		WithGateRefresh(func() { refreshCapabilityGate(state) })
	state.streamRouter.Handle(transport.StreamMetaProposeForward, metaForwardReceiver.Handle)
	// §7 B1 — share the same *encrypt.HandshakeVerifier instance between the
	// Join receiver (reads issued-nonce map) and the Challenge receiver
	// (writes it). state.handshakeVerifier is set by wireDEKKeeper bound to
	// state.kekStore; the verifier is keyed by the local KEK so VerifyResponse
	// passes iff the joiner holds the same KEK.
	metaJoinReceiver := cluster.NewMetaJoinReceiver(metaRaft).
		WithHandshakeVerifier(state.handshakeVerifier).
		WithBootstrapSecretProvider(newBootstrapSecretProvider(state))
	if state.handshakeVerifier != nil {
		metaJoinReceiver = metaJoinReceiver.WithClusterID(state.handshakeVerifier.ClusterID())
	}
	metaJoinReceiver = metaJoinReceiver.WithPostJoinHook(func(joinCtx context.Context, req cluster.JoinRequest) error {
		if err := addJoinedNodeToLegacyDataRaft(joinCtx, state.node, state.metaRaft.FSM().Nodes(), req.NodeID); err != nil {
			return err
		}
		return expandShardGroupsForJoinedNode(joinCtx, state, req.NodeID)
	})
	state.streamRouter.Handle(transport.StreamMetaJoin, metaJoinReceiver.Handle)
	// Zero-CA QUIC join listener (W9, leader side): a dedicated QUIC listener on
	// its own ALPN serving the two-phase invite handler. A brand-new joiner whose
	// self-signed SPKI is in nobody's accept-set cannot reach the production
	// cluster listener, so the invite flow rides this isolated transport. The
	// listener cert is persisted+stable (outstanding invite bundles pin its SPKI
	// across leader restarts). Only clustered nodes start it; single-node skips.
	if state.clusterMode {
		if err := startJoinListener(state, metaJoinReceiver); err != nil {
			return fmt.Errorf("start join listener: %w", err)
		}
	}
	metaChallengeReceiver := cluster.NewMetaChallengeReceiver(state.handshakeVerifier)
	state.streamRouter.Handle(transport.StreamMetaJoinChallenge, metaChallengeReceiver.Handle)
	metaReadDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaCatalogRead, Payload: payload}
		reply, err := quicTransport.Call(callCtx, peer, msg)
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
		WithObjectIndexProposer(indexProposer).
		WithCapabilityGate(state.capabilityGate)
	state.clusterCoord.SetAppendForwardBufferConfig(cluster.AppendForwardBufferConfig{
		TotalBytes:    state.cfg.AppendForwardBufferTotalBytes,
		MaxPerRequest: state.cfg.AppendForwardBufferMaxPerRequest,
	})

	coalesceCfg := cluster.DefaultCoalesceConfig()
	coalesceCfg.SizeCapBytes = state.cfg.AppendSizeCapBytes
	state.distBackend.SetCoalesceConfig(coalesceCfg)
	state.distBackend.SetScrubOrphanAge(state.cfg.ScrubOrphanAge)
	// Propagate the cap to any GroupBackend instances already registered in
	// dgMgr (groups 1-N created by bootOwnedGroupsAndEC before this phase
	// ran). Without this they would keep the default 5 TiB cap.
	for _, dg := range state.dgMgr.All() {
		if gb := dg.Backend(); gb != nil {
			gb.SetCoalesceConfig(coalesceCfg)
		}
	}
	state.coalesceCfg = coalesceCfg

	metaReadReceiver := cluster.NewMetaCatalogReadReceiver(cluster.NewMetaCatalog(metaRaft, state.clusterCoord, "s3://grainfs-tables/warehouse"))
	state.streamRouter.Handle(transport.StreamMetaCatalogRead, metaReadReceiver.Handle)

	if state.joinMode {
		if err := PerformMetaJoin(ctx, quicTransport, []string{state.joinAddr}, state.nodeID, state.raftAddr, state.kekStore, state.handshakeVerifier.ClusterID()); err != nil {
			return err
		}
		if err := WaitForShardGroupCount(ctx, metaRaft.FSM(), seedGroups, 30*time.Second); err != nil {
			return err
		}
		state.clusterRouter.Sync(metaRaft.FSM().BucketAssignments())
		state.clusterRouter.SetRequireExplicitAssignments(true)
	}

	// Zero-CA invite-join (W9b): post-boot Phase-2 ACK. Placed in the SAME window
	// as the legacy join (after bootMetaRaftStart, before run.go's WaitDEKReady
	// gate) so the joiner finalizes raft membership and starts catching up the
	// log — including the gen-0 DEK — before the DEK-readiness gate runs. The
	// node key was sealed under the cluster KEK gen-0 (now staged + loaded by
	// wireDEKKeeper), so LoadNodeKey here succeeds.
	if state.inviteJoinMode {
		if err := bootInviteJoinPhase2(ctx, state); err != nil {
			return err
		}
		// Parity with the legacy joinMode branch above: after membership lands,
		// wait for the shard-group assignments to replay into this node's FSM,
		// then sync the cluster router so reads routed THROUGH this joiner resolve
		// to the right group leader. Without this the joiner is a voter but its
		// router has no bucket assignments, so GETs return "not the leader".
		if err := WaitForShardGroupCount(ctx, metaRaft.FSM(), seedGroups, 30*time.Second); err != nil {
			return err
		}
		state.clusterRouter.Sync(metaRaft.FSM().BucketAssignments())
		state.clusterRouter.SetRequireExplicitAssignments(true)
	}

	log.Info().Msg("v0.0.7.1 PR-D: ClusterCoordinator wired — live multi-raft routing enabled")
	return nil
}

type legacyDataRaftMembership interface {
	ID() string
	Peers() []string
	AddVoterCtx(ctx context.Context, id, addr string) error
}

func addJoinedNodeToLegacyDataRaft(ctx context.Context, node legacyDataRaftMembership, nodes []cluster.MetaNodeEntry, nodeID string) error {
	if node == nil || nodeID == "" || nodeID == node.ID() {
		return nil
	}
	addr := ""
	for _, n := range nodes {
		if n.ID == nodeID {
			addr = n.Address
			break
		}
	}
	if addr == "" {
		return fmt.Errorf("add joined node to legacy data raft: node %q not found in meta membership", nodeID)
	}
	// Legacy group-0 data raft uses voter IDs directly as QUIC dial targets;
	// v2 AddVoterCtx currently ignores its addr parameter. Only auto-extend
	// this raft when the node ID is already the dialable raft address. Stable
	// node IDs continue through the per-group/meta paths until this legacy
	// transport can resolve node IDs via the address book.
	if nodeID != addr {
		return nil
	}
	for _, peer := range node.Peers() {
		if peer == nodeID || peer == addr {
			return nil
		}
	}
	if err := node.AddVoterCtx(ctx, nodeID, addr); err != nil {
		return fmt.Errorf("add joined node %q to legacy data raft: %w", nodeID, err)
	}
	return nil
}

func expandShardGroupsForJoinedNode(ctx context.Context, state *bootState, nodeID string) error {
	nodes := state.metaRaft.FSM().Nodes()
	refreshRuntimeTopologyFromMetaNodes(state, nodes)
	missingGroups := MissingSeedShardGroups(
		state.nodeID,
		state.raftAddr,
		nodes,
		state.metaRaft.FSM().ShardGroups(),
		cluster.AutoECConfigForClusterSize(len(nodes)).NumShards(),
	)
	for _, group := range missingGroups {
		if err := state.metaRaft.ProposeShardGroup(ctx, group); err != nil {
			return fmt.Errorf("expand shard groups for joined node %q: propose seed group %s: %w", nodeID, group.ID, err)
		}
	}
	if len(missingGroups) > 0 {
		state.seedGroups = seedGroupCountForClusterSize(len(nodes))
		log.Info().Str("node_id", nodeID).Int("groups", len(missingGroups)).Int("seed_groups", state.seedGroups).Msg("seeded shard groups for joined node count")
	}

	return nil
}
