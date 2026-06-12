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

	// All follower→leader forward dialers (group propose, meta propose, meta read)
	// use CallPooled, NOT Call: connection-per-RPC Call opens a fresh TLS handshake
	// per forward, so a conc≥16 multipart-under-load burst becomes a handshake storm
	// against :7000 and forwards dial-time-out ("no reachable peer") or blow the
	// readiness deadline. CallPooled reuses the bounded control pool
	// (MaxControlConnsPerPeer) — same pattern shardSvc.SendRequest already uses —
	// turning the storm into bounded backpressure. (This mirrors 730222ee, which
	// only covered shardSvc; these boot dialers were the remaining conn-per-RPC path.)
	forwardDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, error) {
		// Native /forward/propose/group buffered route (Phase 8 N7-3); pooled
		// HTTP conns give the same bounded-backpressure property.
		return clusterTransport.CallBuffered(callCtx, peer, transport.RouteForwardProposeGroup, payload)
	}
	forwardStreamDialer := func(callCtx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		// Native /forward/write route (Phase 8 N7-2): frame in the family header,
		// raw body streamed, FB ForwardReply as the response body.
		return clusterTransport.ForwardWrite(callCtx, peer, payload, body)
	}
	forwardReadStreamDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, io.ReadCloser, error) {
		// Native /forward/read route: reply metadata in the family response
		// header, object bytes streamed.
		return clusterTransport.ForwardRead(callCtx, peer, payload)
	}

	state.forwardSender = cluster.NewForwardSender(forwardDialer).
		WithStreamDialer(forwardStreamDialer).
		WithReadStreamDialer(forwardReadStreamDialer).
		// A deadline-less forward (e.g. CompleteMultipartUpload, which carries no
		// caller deadline) is bounded by this readiness retry. A hardcoded 5s
		// guillotined forwards whose leader-side commit legitimately takes ~5.5s
		// under conc≥16 load — the local-leader path is uncapped and finishes at
		// the same latency, proving 5s sat BELOW the operation's normal under-load
		// time. That premature cut also caused the NoSuchUpload retry-tail
		// (phantom commit: sender gave up at 5s, commit landed at ~5.5s, uploadId
		// consumed, warp retried → 404). Align with the receiver's commit bound.
		WithReadinessRetry(cluster.ProposeForwardTimeout()).
		WithLeaderHintResolver(func(hint string) string {
			if addr, ok := cluster.ResolveNodeAddress(metaRaft.FSM(), hint); ok {
				return addr
			}
			return hint
		})
	state.forwardReceiver = cluster.NewForwardReceiver(state.dgMgr)

	metaForwardDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaProposeForward, Payload: payload}
		reply, err := clusterTransport.CallPooled(callCtx, peer, msg)
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
		reply, err := clusterTransport.CallPooled(callCtx, peer, msg)
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
		//
		// Option B: in a deferred-seed cluster (--bootstrap-expect-nodes>1) genesis
		// has not seeded groups yet — it waits for the target node count. With
		// sequential joins, hard-blocking here deadlocks: this joiner would wait
		// for groups that only appear once LATER joiners arrive. Skip the wait;
		// groups + bucket assignments propagate live via OnShardGroupAdded /
		// OnBucketAssigned (router.AssignBucket) once seed-on-quorum fires. The DEK
		// gate (WaitDEKReady) is meta-raft based and independent of shard groups.
		if state.cfg.BootstrapExpectNodes <= 1 {
			if err := WaitForShardGroupCount(ctx, metaRaft.FSM(), seedGroups, 30*time.Second); err != nil {
				return err
			}
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

	// Option B deferred seed: while a deferred genesis seed is pending, the
	// deferred-seed lifecycle OWNS shard-group creation — it seeds all groups
	// once at the uniform EC width when the target count joins, and suppresses
	// the per-join expand below until then. Running the normal expand while
	// pending would create partial-RF groups at the current (sub-target) size
	// and defeat uniform seeding. handled=true means this hook is done.
	if handled, err := handleDeferredSeed(ctx, state, liveNodes); err != nil {
		return err
	} else if handled {
		return nil
	}

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

// handleDeferredSeed drives the Option B deferred-seed lifecycle for one
// post-join hook invocation. The "pending" condition is DERIVED, not stored:
// any leader (original or one elected mid-bootstrap) computes it from its own
// --bootstrap-expect-nodes flag + the replicated shard-group count + live node
// count, so the decision survives a leader change with no persistent marker.
//
// Contract (the early-return guard the caller relies on):
//   - not a deferred-seed cluster (expect<=1), OR the deferred batch is already
//     fully seeded → (false, nil): caller runs the normal per-join expand.
//   - deferred batch incomplete, quorum NOT yet reached → (true, nil): caller
//     returns; the per-join expand is SUPPRESSED so no partial-RF groups are
//     created before the target size is reached.
//   - deferred batch incomplete, quorum reached → seed the MISSING initial groups
//     at the uniform EC width (propose-only-absent = idempotent + convergent under
//     leader-change re-entry), open the router, return (true, nil).
//
// seedMu serializes overlapping post-join hooks within one process; the
// recheck-under-lock + MissingSeedShardGroups idempotency together prevent a
// double-seed even across a leader move.
// deferredSeedDecision is the pure post-join verdict for Option B. It is a
// function ONLY of replicated/declared state (the --bootstrap-expect-nodes flag,
// the replicated shard-group count, and the live node count), so every leader —
// original or one elected mid-bootstrap — computes the same verdict. That is
// what makes deferred seeding leader-change-safe without a persistent marker.
type deferredSeedDecision int

const (
	seedPassthrough deferredSeedDecision = iota // not deferred, or batch already seeded → run normal expand
	seedSuppress                                // deferred, quorum not yet reached → skip expand (no partial RF)
	seedNow                                     // deferred, quorum reached, batch incomplete → seed missing groups
)

func decideDeferredSeed(expectNodes, existingGroups, liveNodes int) deferredSeedDecision {
	if expectNodes <= 1 {
		return seedPassthrough // not a deferred-seed cluster (default behavior)
	}
	if existingGroups >= seedGroupCountForClusterSize(expectNodes) {
		return seedPassthrough // deferred batch complete; normal expand handles growth beyond N
	}
	if liveNodes < expectNodes {
		return seedSuppress // below target: suppress per-join expand until quorum (no partial-RF groups)
	}
	return seedNow // quorum reached, batch incomplete (incl. leader-change re-entry mid-seed)
}

func handleDeferredSeed(ctx context.Context, state *bootState, liveNodes []cluster.MetaNodeEntry) (bool, error) {
	expectN := state.cfg.BootstrapExpectNodes
	switch decideDeferredSeed(expectN, len(state.metaRaft.FSM().ShardGroups()), len(liveNodes)) {
	case seedPassthrough:
		return false, nil
	case seedSuppress:
		return true, nil
	}
	// seedNow: take the lock and re-decide under it (a concurrent hook may have
	// completed the seed while we waited), then seed the MISSING groups only.
	state.seedMu.Lock()
	defer state.seedMu.Unlock()
	if decideDeferredSeed(expectN, len(state.metaRaft.FSM().ShardGroups()), len(liveNodes)) != seedNow {
		return true, nil // another hook seeded it while we waited on the lock
	}
	targetGroups := seedGroupCountForClusterSize(expectN)
	voters := cluster.AutoECConfigForClusterSize(expectN).NumShards()

	missing := MissingSeedShardGroups(state.nodeID, state.raftAddr, liveNodes, state.metaRaft.FSM().ShardGroups(), voters)
	for _, group := range missing {
		if err := state.metaRaft.ProposeShardGroup(ctx, group); err != nil {
			return false, fmt.Errorf("option-B seed-on-quorum: propose %s: %w", group.ID, err)
		}
	}
	if err := WaitForShardGroupCount(ctx, state.metaRaft.FSM(), targetGroups, 30*time.Second); err != nil {
		return false, fmt.Errorf("option-B seed-on-quorum wait: %w", err)
	}
	state.clusterRouter.Sync(state.metaRaft.FSM().BucketAssignments())
	state.clusterRouter.SetRequireExplicitAssignments(true)
	log.Info().
		Int("nodes", len(liveNodes)).
		Int("groups", targetGroups).
		Int("voters", voters).
		Msg("Option B: seed-on-quorum complete — uniform EC seeded across target node set")
	return true, nil
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
	// Phase 8 N7-2: native forward routes. The tunnel registrations above stay
	// until N8; all in-tree streaming-forward clients now dial the native routes.
	state.clusterTransport.RegisterForwardWriteHandler(state.forwardReceiver.NativeWriteHandler())
	state.clusterTransport.RegisterForwardReadHandler(state.forwardReceiver.NativeReadHandler())
	return nil
}
