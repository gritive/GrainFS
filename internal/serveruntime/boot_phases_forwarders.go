package serveruntime

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/transport"
)

// bootWALAndForwardersPart1 builds the v0.0.7.1 PR-D ForwardSender +
// ForwardReceiver, the meta-propose forward sender/receiver pair, the
// meta-catalog read sender, and the meta-join receiver. The ClusterCoordinator
// is wired later by bootClusterCoordinatorRouting after the distributed backend
// exists. Also performs the join-mode meta-join + initial Router sync.
//
// R1 (narrow): the logical/PITR WAL (wal.OpenEncrypted) is DEK-sealed and
// decrypts existing records at open, so it is opened in bootLogicalWALOpen
// (post-gate, after WaitDEKReady). This phase keeps forwarder + coordinator
// CONSTRUCTION and the keeper-population catch-up (legacy PerformMetaJoin /
// invite-Phase-2 bootInviteJoinPhase2 below), which MUST run BEFORE
// WaitDEKReady so a joiner's empty keeper fills via the meta-raft apply loop
// before the gate. forwardReceiver.Register is now in bootRegisterForwardHandlers
// (R-FSM-α) so bootShardService can run past WaitDEKReady.
//
// Inputs:  state.cfg.DataDir, state.clusterTransport, state.metaRaft,
//
//	state.streamRouter, state.dgMgr, state.clusterRouter,
//	state.distBackend, state.nodeID, state.raftAddr, state.peers,
//	state.effectiveEC, state.joinMode.
//
// Outputs: state.forwardSender, state.forwardReceiver,
//
//	state.metaForwardSender, state.metaReadSender, state.seedGroups.
//
// Ordering: R-FSM-α moves this BEFORE WaitDEKReady (so the keeper-population
// catch-up runs pre-gate) and BEFORE bootShardService (handler registration
// happens later in bootRegisterForwardHandlers). MUST run BEFORE
// bootLogicalWALOpen + bootClusterCoordinatorRouting + bootBackendWrap.
func bootWALAndForwardersPart1(ctx context.Context, state *bootState) error {
	// Seed data groups from cluster size only. Operators no longer choose this:
	// group count is placement headroom, not a durability policy.
	clusterSize := 1 + len(state.peers)
	seedGroups := seedGroupCountForClusterSize(clusterSize)
	state.seedGroups = seedGroups

	// v0.0.7.1 PR-D: Live multi-raft routing — ClusterCoordinator + ForwardSender/Receiver.
	// ClusterCoordinator implements storage.Backend and routes bucket-scoped ops to the
	// correct group leader via ForwardSender. 0x08 handler (ForwardReceiver) receives
	// forwarded calls on voter nodes and dispatches to local GroupBackend.
	clusterTransport := state.clusterTransport
	metaRaft := state.metaRaft
	peers := state.peers

	forwardDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}
		reply, err := clusterTransport.Call(callCtx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	forwardStreamDialer := func(callCtx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamGroupForwardBody, Payload: payload}
		reply, err := clusterTransport.CallWithBody(callCtx, peer, msg, body)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	forwardReadStreamDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, io.ReadCloser, error) {
		msg := &transport.Message{Type: transport.StreamGroupForwardRead, Payload: payload}
		reply, body, err := clusterTransport.CallRead(callCtx, peer, msg)
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

	metaForwardDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaProposeForward, Payload: payload}
		reply, err := clusterTransport.Call(callCtx, peer, msg)
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

	metaForwardReceiver := cluster.NewMetaProposeForwardReceiver(metaRaft).
		WithGateRefresh(func() { refreshCapabilityGate(state) })
	state.streamRouter.Handle(transport.StreamMetaProposeForward, metaForwardReceiver.Handle)
	// Zero-CA invite-join receiver. It serves the two-phase invite flow over the
	// dedicated join listener (HandleJoinStream); state.handshakeVerifier (set by
	// wireDEKKeeper) supplies the 16-byte cluster id bound into the invite
	// transcript via WithClusterID.
	metaJoinReceiver := cluster.NewMetaJoinReceiver(metaRaft).
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
	// Zero-CA join listener (W9, leader side): a dedicated listener on
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
	metaReadDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaCatalogRead, Payload: payload}
		reply, err := clusterTransport.Call(callCtx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	state.metaReadSender = cluster.NewMetaCatalogReadSender(metaReadDialer)

	coalesceCfg := cluster.DefaultCoalesceConfig()
	coalesceCfg.SizeCapBytes = state.cfg.AppendSizeCapBytes
	state.coalesceCfg = coalesceCfg

	// Zero-CA invite-join (W9b): post-boot Phase-2 ACK. Placed after
	// bootMetaRaftStart and before run.go's WaitDEKReady
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

	log.Info().Msg("v0.0.7.1 PR-D: forwarders wired — live multi-raft routing pending backend construction")
	return nil
}

func bootClusterCoordinatorRouting(state *bootState) error {
	if state.distBackend == nil {
		return fmt.Errorf("bootClusterCoordinatorRouting: distBackend is nil — bootOwnedGroupsAndEC must run first")
	}
	if state.forwardSender == nil {
		return fmt.Errorf("bootClusterCoordinatorRouting: forwardSender is nil — bootWALAndForwardersPart1 must run first")
	}
	if state.metaForwardSender == nil {
		return fmt.Errorf("bootClusterCoordinatorRouting: metaForwardSender is nil — bootWALAndForwardersPart1 must run first")
	}
	metaRaft := state.metaRaft
	peers := state.peers

	state.distBackend.SetBucketAssigner(cluster.NewForwardingBucketAssigner(metaRaft, func(ctx context.Context, command []byte) error {
		return state.metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
	}))

	indexProposer := cluster.NewForwardingObjectIndexProposer(metaRaft, func(ctx context.Context, command []byte) error {
		return state.metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
	}).WithIndexForwarder(func(ctx context.Context, command []byte) (uint64, error) {
		return state.metaForwardSender.SendWithIndex(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
	})

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

	state.distBackend.SetCoalesceConfig(state.coalesceCfg)
	state.distBackend.SetScrubOrphanAge(state.cfg.ScrubOrphanAge)
	for _, dg := range state.dgMgr.All() {
		if gb := dg.Backend(); gb != nil {
			gb.SetCoalesceConfig(state.coalesceCfg)
		}
	}

	metaReadReceiver := cluster.NewMetaCatalogReadReceiver(cluster.NewMetaCatalog(metaRaft, state.clusterCoord, "s3://grainfs-tables/warehouse"))
	state.streamRouter.Handle(transport.StreamMetaCatalogRead, metaReadReceiver.Handle)
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
	// Legacy group-0 data raft uses voter IDs directly as transport dial targets;
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
	refreshRuntimeTopologyFromMetaNodes(state, nodes) // topology/address-book: all members, incl. revoked
	// Voter-candidate pool and cluster-size count must exclude revoked nodes so a
	// revoked node is never (re-)seeded as a data-group voter (pairs with the evacuator).
	liveNodes := liveNonRevokedNodes(state.metaRaft.FSM())
	missingGroups := MissingSeedShardGroups(
		state.nodeID,
		state.raftAddr,
		liveNodes,
		state.metaRaft.FSM().ShardGroups(),
		cluster.AutoECConfigForClusterSize(len(liveNodes)).NumShards(),
	)
	for _, group := range missingGroups {
		if err := state.metaRaft.ProposeShardGroup(ctx, group); err != nil {
			return fmt.Errorf("expand shard groups for joined node %q: propose seed group %s: %w", nodeID, group.ID, err)
		}
	}
	if len(missingGroups) > 0 {
		state.seedGroups = seedGroupCountForClusterSize(len(liveNodes))
		log.Info().Str("node_id", nodeID).Int("groups", len(missingGroups)).Int("seed_groups", state.seedGroups).Msg("seeded shard groups for joined node count")
	}

	return nil
}

// bootRegisterForwardHandlers registers the data-shard RPC handlers into the
// previously-constructed forwardReceiver. Extracted from
// bootWALAndForwardersPart1 in R-FSM-α so that handler registration can happen
// AFTER bootShardService (which constructs state.shardSvc), allowing
// bootShardService to run past WaitDEKReady.
//
// Prerequisites:
//   - state.forwardReceiver must be set by bootWALAndForwardersPart1.
//   - state.shardSvc must be set by bootShardService.
func bootRegisterForwardHandlers(state *bootState) error {
	if state.forwardReceiver == nil {
		return fmt.Errorf("bootRegisterForwardHandlers: forwardReceiver is nil — bootWALAndForwardersPart1 must run first")
	}
	if state.shardSvc == nil {
		return fmt.Errorf("bootRegisterForwardHandlers: shardSvc is nil — bootShardService must run first")
	}
	state.forwardReceiver.Register(state.shardSvc)
	return nil
}
