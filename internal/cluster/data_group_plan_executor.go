package cluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
)

// ErrLeadershipTransferred is returned by MoveReplica when the node must remove
// itself: it transfers leadership first, then the caller retries on the new leader.
var ErrLeadershipTransferred = errors.New("leadership transferred to another voter")

// ErrRefuseLastVoter is returned by EvacuateVoter when removing the revoked node
// would empty the voter set; the group would lose quorum permanently.
var ErrRefuseLastVoter = errors.New("refusing to evict last voter")

// ErrDataGroupNotLocalLeader is returned when an operation requires the local
// process to lead a data group but leadership is elsewhere or not established.
var ErrDataGroupNotLocalLeader = errors.New("data group not led locally")

// NodeAddressBook resolves cluster nodeIDs to their QUIC addresses.
// *MetaFSM implements this interface.
type NodeAddressBook interface {
	Nodes() []MetaNodeEntry
}

// ShardGroupUpdater persists shard-group voter membership to the cluster metadata.
// *MetaRaft implements this interface.
type ShardGroupUpdater interface {
	ProposeShardGroup(ctx context.Context, sg ShardGroupEntry) error
	ProposeShardGroupForwarding(ctx context.Context, sg ShardGroupEntry) error
}

// dataRaftNode is the subset of RaftNode used by DataGroupPlanExecutor.
// It is a strict subset of the RaftNode interface; any RaftNode satisfies it.
// Defined separately to allow minimal fake injection in unit tests.
type dataRaftNode interface {
	IsLeader() bool
	CommittedIndex() uint64
	AddLearner(id, addr string) error
	PromoteToVoter(id string) error
	RemoveVoter(id string) error
	PeerMatchIndex(peerKey string) (uint64, bool)
	TransferLeadership() error
	AddVoterCtx(ctx context.Context, id, addr string) error
	ChangeMembership(ctx context.Context, adds []raft.ServerEntry, removes []string) error
	Configuration() raft.Configuration
}

// DataGroupPlanExecutor implements GroupRebalancer via real Raft voter migration.
//
// MoveReplica algorithm (PR-K2):
//
//	ChangeMembership(adds=[toNode], removes=[fromNode]) → ProposeShardGroup (MetaFSM) →
//	DataGroupManager.Add
//
// Self-removal is handled by the joint commit-time step-down hook in
// internal/raft/raft.go: jointPromoteCh closes BEFORE state=Follower, so
// the caller wakes up with nil even when removing self.
//
// Must be called from the data-group Raft leader only.
type DataGroupPlanExecutor struct {
	localNodeID    string
	dgMgr          *DataGroupManager
	addrBook       NodeAddressBook
	sgUpdater      ShardGroupUpdater
	catchUpTimeout time.Duration // max wait for learner catch-up; default 30s
	// nodeFor is the default impl; override in tests via newDataGroupPlanExecutorForTest.
	nodeFor func(*DataGroup) dataRaftNode
}

// NewDataGroupPlanExecutor creates a DataGroupPlanExecutor.
// localNodeID identifies this node and enables the self-removal guard in MoveReplica.
// addrBook is typically metaRaft.FSM(); sgUpdater is typically metaRaft.
func NewDataGroupPlanExecutor(
	localNodeID string,
	dgMgr *DataGroupManager,
	addrBook NodeAddressBook,
	sgUpdater ShardGroupUpdater,
) *DataGroupPlanExecutor {
	e := &DataGroupPlanExecutor{
		localNodeID:    localNodeID,
		dgMgr:          dgMgr,
		addrBook:       addrBook,
		sgUpdater:      sgUpdater,
		catchUpTimeout: 30 * time.Second,
	}
	e.nodeFor = func(dg *DataGroup) dataRaftNode {
		// Node() returns the RaftNode interface, which satisfies dataRaftNode
		// for both v1 (*raft.Node) and v2 (*raftV2Node). Under v2, membership
		// methods that are not yet implemented surface ErrNotImplemented so
		// operators see a clear error rather than a silent skip.
		return dg.Backend().Node()
	}
	return e
}

// resolveAddr returns the QUIC address for nodeID from the address book.
func (e *DataGroupPlanExecutor) resolveAddr(nodeID string) (string, error) {
	for _, n := range e.addrBook.Nodes() {
		if n.ID == nodeID {
			return n.Address, nil
		}
	}
	return "", fmt.Errorf("data_group_executor: node %q not found in address book", nodeID)
}

// AddReplica adds toNode as a voter in groupID without removing existing voters.
//
// This is used when a new cluster node joins after shard groups were initially
// seeded from a single bootstrap voter. Must be called from the data-group Raft
// leader only.
func (e *DataGroupPlanExecutor) AddReplica(ctx context.Context, groupID, toNode string) error {
	dg := e.dgMgr.Get(groupID)
	if dg == nil {
		return fmt.Errorf("data_group_executor: group %q not found", groupID)
	}
	if slices.Contains(dg.PeerIDs(), toNode) {
		return nil
	}

	node := e.nodeFor(dg)
	if err := e.waitForLocalLeader(ctx, groupID, node); err != nil {
		return err
	}

	for _, peer := range ResolveShardGroupPeers(e.addrBook, ShardGroupEntry{ID: groupID, PeerIDs: dg.PeerIDs()}) {
		if peer.Unresolved {
			return fmt.Errorf("data_group_executor: group %q has unresolved legacy peer %q", groupID, peer.Input)
		}
	}

	toAddr, err := e.resolveAddr(toNode)
	if err != nil {
		return fmt.Errorf("data_group_executor: resolve toNode: %w", err)
	}

	originalPeers := slices.Clone(dg.PeerIDs())
	newPeers := append(slices.Clone(originalPeers), toNode)
	if err := e.sgUpdater.ProposeShardGroup(ctx, ShardGroupEntry{ID: groupID, PeerIDs: newPeers}); err != nil {
		return fmt.Errorf("data_group_executor: ProposeShardGroup desired peers: %w", err)
	}

	addCtx, cancel := context.WithTimeout(ctx, e.catchUpTimeout)
	defer cancel()
	if err := node.ChangeMembership(addCtx,
		[]raft.ServerEntry{{ID: toNode, Address: toAddr, Suffrage: raft.Voter}},
		nil,
	); err != nil {
		if rollbackErr := e.sgUpdater.ProposeShardGroup(ctx, ShardGroupEntry{ID: groupID, PeerIDs: originalPeers}); rollbackErr != nil {
			return fmt.Errorf("data_group_executor: ChangeMembership: %w; rollback desired peers: %w", err, rollbackErr)
		}
		return fmt.Errorf("data_group_executor: ChangeMembership: %w", err)
	}

	if err := e.sgUpdater.ProposeShardGroup(ctx, ShardGroupEntry{ID: groupID, PeerIDs: newPeers}); err != nil {
		return fmt.Errorf("data_group_executor: ProposeShardGroup: %w", err)
	}

	e.dgMgr.Add(NewDataGroupWithBackend(groupID, newPeers, dg.Backend()))
	return nil
}

// MoveReplica migrates a Raft voter in groupID from fromNode to toNode. It issues
// one ChangeMembership(adds, removes) which the adapter SEQUENCES as add-then-remove
// (not atomic; see internal/cluster/raftnode_adapter.go). A partial failure can
// leave an added-but-not-removed voter, which EvacuateVoter recovers from
// idempotently.
//
// Self-removal (fromNode == localNodeID): transfers leadership to another voter
// and returns ErrLeadershipTransferred so the caller retries on the new leader.
func (e *DataGroupPlanExecutor) MoveReplica(ctx context.Context, groupID, fromNode, toNode string) error {
	dg := e.dgMgr.Get(groupID)
	if dg == nil {
		return fmt.Errorf("data_group_executor: group %q not found", groupID)
	}

	node := e.nodeFor(dg)
	if !node.IsLeader() {
		return fmt.Errorf("data_group_executor: not leader of group %q", groupID)
	}

	for _, peer := range ResolveShardGroupPeers(e.addrBook, ShardGroupEntry{ID: groupID, PeerIDs: dg.PeerIDs()}) {
		if peer.Unresolved {
			return fmt.Errorf("data_group_executor: group %q has unresolved legacy peer %q", groupID, peer.Input)
		}
	}

	// Pre-flight: fromNode must be a current voter so we don't accidentally
	// evict an unrelated node if the rebalancer has a stale plan.
	if !slices.Contains(dg.PeerIDs(), fromNode) {
		return fmt.Errorf("data_group_executor: fromNode %q is not a voter in group %q", fromNode, groupID)
	}

	// Self-removal guard: if this node is being removed, transfer leadership first
	// so the new leader can retry the migration.
	if fromNode == e.localNodeID {
		if err := e.waitForLeadershipTransferTarget(ctx, dg, node, fromNode); err != nil {
			return err
		}
		if err := node.TransferLeadership(); err != nil {
			return fmt.Errorf("data_group_executor: TransferLeadership: %w", err)
		}
		return ErrLeadershipTransferred
	}

	toAddr, err := e.resolveAddr(toNode)
	if err != nil {
		return fmt.Errorf("data_group_executor: resolve toNode: %w", err)
	}

	// ChangeMembership is sequenced add-then-remove by the adapter (not atomic).
	// The data-group raft config keys voters by NODE ID (GroupLifecycleConfig.NodeID
	// = the node id from the seeded peer set), so the add carries the resolved
	// address but the remove MUST be the node id (fromNode), symmetric with the add.
	addCtx, cancel := context.WithTimeout(ctx, e.catchUpTimeout)
	defer cancel()
	if err := node.ChangeMembership(addCtx,
		[]raft.ServerEntry{{ID: toNode, Address: toAddr, Suffrage: raft.Voter}},
		[]string{fromNode}, // data-group raft keys voters by node id
	); err != nil {
		return fmt.Errorf("data_group_executor: ChangeMembership: %w", err)
	}

	// Build new peerIDs (replace fromNode with toNode).
	oldPeers := dg.PeerIDs()
	newPeers := make([]string, 0, len(oldPeers))
	for _, p := range oldPeers {
		if p != fromNode {
			newPeers = append(newPeers, p)
		}
	}
	newPeers = append(newPeers, toNode)

	// Persist new membership to MetaFSM. A non-meta-leader group leader still
	// converges by forwarding to the meta leader (P1-1).
	if err := e.sgUpdater.ProposeShardGroupForwarding(ctx, ShardGroupEntry{ID: groupID, PeerIDs: newPeers}); err != nil {
		return fmt.Errorf("data_group_executor: ProposeShardGroupForwarding: %w", err)
	}

	e.dgMgr.Add(NewDataGroupWithBackend(groupID, newPeers, dg.Backend()))
	return nil
}

// EvacuateVoter idempotently removes revokedID from groupID's voter set, reading
// the group's REAL raft config (node.Configuration()) to decide the minimal
// membership change. Safe to re-run after a partial/crashed prior attempt (P1-2).
//
// COUNT-BASED, PICK-STABLE decision: target = the group's intended replication
// factor = len(dg.PeerIDs()) (the mirror, which MoveReplica updates ONLY after a
// ChangeMembership fully succeeds, so it is stable across a crashed move).
// survivors = REAL voters minus the revoked node being evicted. If removing the
// revoked voter still leaves >= target voters (survivors >= target), a prior
// (possibly crashed) move already supplied a replacement -> issue REMOVE-ONLY and
// never add a second replacement, regardless of which node a prior attempt picked.
// (Keying the decision on whether the SPECIFIC replacementID is already a voter —
// "pick-based" — is the bug this replaces: a retry that picks a different node
// would over-replicate.) replacementID == "" always requests a shrink.
//
// Must run on the data-group raft leader. Refuses to drop the last voter.
// A revoked node id that is not among the current voters is an idempotent no-op.
// revoked == local leader -> transfer leadership and let the new leader retry.
func (e *DataGroupPlanExecutor) EvacuateVoter(ctx context.Context, groupID, revokedID, replacementID string) error {
	dg := e.dgMgr.Get(groupID)
	if dg == nil {
		return fmt.Errorf("data_group_executor: group %q not found", groupID)
	}
	node := e.nodeFor(dg)
	if !node.IsLeader() {
		return fmt.Errorf("data_group_executor: not leader of group %q", groupID)
	}

	// REAL voter set, normalized to NODE IDs. data-group raft Server.ID IS the
	// node ID (seed/MatchLocal/GroupLifecycleConfig.NodeID); normalize anyway so
	// any address-form id also resolves. Keep the raw Server.ID as the removal key
	// (ChangeMembership/RemoveVoter operate in the config's own id space).
	voterByNodeID := make(map[string]string) // nodeID -> raw Server.ID (removal key)
	for _, s := range node.Configuration().Servers {
		if s.Suffrage != raft.Voter {
			continue
		}
		nid := ResolveShardGroupPeer(e.addrBook, s.ID).NodeID
		if nid == "" {
			nid = s.ID
		}
		voterByNodeID[nid] = s.ID
	}

	rawRevoked, present := voterByNodeID[revokedID]
	if !present {
		return nil // idempotent no-op: already absent
	}
	if len(voterByNodeID)-1 == 0 {
		return fmt.Errorf("data_group_executor: refusing to evict last voter of group %q: %w", groupID, ErrRefuseLastVoter)
	}

	// Self-removal: the revoked node leads this group. Step down so a surviving
	// voter takes over and evicts us on its next tick. v2 TransferLeadership picks
	// the most-caught-up peer from the leader's REAL matchIndex and steps down
	// regardless (raft §3.10). We must NOT pre-wait via waitForLeadershipTransferTarget
	// here: it reads the adapter's PeerMatchIndex, which is always (0,false) under
	// v2 (raftnode_adapter.go), so the pre-wait can never observe catch-up and would
	// time out — leaving a revoked leader permanently un-evictable.
	if revokedID == e.localNodeID {
		if err := node.TransferLeadership(); err != nil {
			return fmt.Errorf("data_group_executor: TransferLeadership: %w", err)
		}
		return ErrLeadershipTransferred
	}

	// COUNT-BASED, PICK-STABLE (P1-2): target = intended RF = mirror size (stable
	// across a crashed move, which updates the mirror only after success).
	// survivors = real voters minus the revoked one. survivors >= target (or no
	// replacement) => remove-only; never adds a 2nd replacement on a crashed-move
	// retry regardless of which node a prior attempt picked.
	target := len(dg.PeerIDs())
	survivors := len(voterByNodeID) - 1

	if replacementID == "" || survivors >= target {
		rmCtx, cancel := context.WithTimeout(ctx, e.catchUpTimeout)
		defer cancel()
		if err := node.ChangeMembership(rmCtx, nil, []string{rawRevoked}); err != nil {
			return fmt.Errorf("data_group_executor: ChangeMembership remove: %w", err)
		}
		return e.convergePeerIDsFromRealConfig(ctx, groupID, dg, node, revokedID)
	}

	// Fresh move: survivors < target and a replacement was provided. Reuse
	// MoveReplica (v2 catch-up is unobservable — reuse, do not reimplement).
	if err := e.MoveReplica(ctx, groupID, revokedID, replacementID); err != nil {
		return err
	}
	return e.convergePeerIDsFromRealConfig(ctx, groupID, dg, node, revokedID)
}

// convergePeerIDsFromRealConfig builds PeerIDs from the REAL voter set (not the
// stale mirror), minus removeID, then forwards to the meta-leader so a non-meta-
// leader group leader still converges (P1-1). Server.ID is an address; resolve
// each to a node ID.
func (e *DataGroupPlanExecutor) convergePeerIDsFromRealConfig(ctx context.Context, groupID string, dg *DataGroup, node dataRaftNode, removeID string) error {
	var newPeers []string
	for _, s := range node.Configuration().Servers {
		if s.Suffrage != raft.Voter {
			continue
		}
		id := ResolveShardGroupPeer(e.addrBook, s.ID).NodeID
		if id == "" {
			id = s.ID
		}
		if id == removeID {
			continue // immune to RemoveVoter commit-timing on the immediate re-read
		}
		newPeers = append(newPeers, id)
	}
	if err := e.sgUpdater.ProposeShardGroupForwarding(ctx, ShardGroupEntry{ID: groupID, PeerIDs: newPeers}); err != nil {
		return fmt.Errorf("data_group_executor: ProposeShardGroupForwarding: %w", err)
	}
	e.dgMgr.Add(NewDataGroupWithBackend(groupID, newPeers, dg.Backend()))
	return nil
}

func (e *DataGroupPlanExecutor) waitForLeadershipTransferTarget(
	ctx context.Context,
	dg *DataGroup,
	node dataRaftNode,
	fromNode string,
) error {
	peerIDs := dg.PeerIDs()
	hasTarget := false
	for _, peerID := range peerIDs {
		if peerID != fromNode {
			hasTarget = true
			break
		}
	}
	if !hasTarget {
		return nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, e.catchUpTimeout)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		committed := node.CommittedIndex()
		for _, peerID := range peerIDs {
			if peerID == fromNode {
				continue
			}
			if e.peerCaughtUp(node, peerID, committed) {
				return nil
			}
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf("data_group_executor: no caught-up voter available for leadership transfer from %q: %w",
				fromNode, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (e *DataGroupPlanExecutor) waitForLocalLeader(ctx context.Context, groupID string, node dataRaftNode) error {
	waitCtx, cancel := context.WithTimeout(ctx, e.catchUpTimeout)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if node.IsLeader() {
			return nil
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf("data_group_executor: not leader of group %q: %w: %w", groupID, ErrDataGroupNotLocalLeader, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (e *DataGroupPlanExecutor) peerCaughtUp(node dataRaftNode, peerID string, committed uint64) bool {
	peerKey := peerID
	if addr, err := e.resolveAddr(peerID); err == nil && addr != "" {
		peerKey = addr
	}
	if match, ok := node.PeerMatchIndex(peerKey); ok && match >= committed {
		return true
	}
	if peerKey != peerID {
		if match, ok := node.PeerMatchIndex(peerID); ok && match >= committed {
			return true
		}
	}
	return false
}

// compile-time check: the production adapter must satisfy dataRaftNode.
var _ dataRaftNode = (*raftNodeAdapter)(nil)

// DataRaftNode is the exported alias of dataRaftNode for test injection.
type DataRaftNode = dataRaftNode

// NewDataGroupPlanExecutorForTest creates a DataGroupPlanExecutor with custom nodeFor.
// Only for use in tests.
func NewDataGroupPlanExecutorForTest(
	localNodeID string,
	dgMgr *DataGroupManager,
	addrBook NodeAddressBook,
	sgUpdater ShardGroupUpdater,
	nodeFor func(*DataGroup) DataRaftNode,
) *DataGroupPlanExecutor {
	return &DataGroupPlanExecutor{
		localNodeID:    localNodeID,
		dgMgr:          dgMgr,
		addrBook:       addrBook,
		sgUpdater:      sgUpdater,
		catchUpTimeout: 30 * time.Second,
		nodeFor:        nodeFor,
	}
}

// DGMgr exposes the DataGroupManager for test setup.
func (e *DataGroupPlanExecutor) DGMgr() *DataGroupManager { return e.dgMgr }
