package cluster

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
)

// NodeAddressBook resolves cluster nodeIDs to their QUIC addresses.
// *MetaFSM implements this interface.
type NodeAddressBook interface {
	Nodes() []MetaNodeEntry
}

// ShardGroupUpdater persists shard-group voter membership to the cluster metadata.
// *MetaRaft implements this interface.
type ShardGroupUpdater interface {
	ProposeShardGroup(ctx context.Context, sg ShardGroupEntry) error
}

// dataRaftNode is the subset of *raft.Node used by DataGroupPlanExecutor.
// Defined as an interface to allow fake injection in unit tests.
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
}

// DataGroupPlanExecutor implements GroupRebalancer via real Raft voter migration.
//
// MoveReplica algorithm (PR-K2 — single §4.3 atomic call):
//
//	ChangeMembership(adds=[toNode], removes=[fromAddr]) → ProposeShardGroup (MetaFSM) →
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
	e.nodeFor = func(dg *DataGroup) dataRaftNode { return dg.Backend().RaftNode() }
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

// MoveReplica migrates a Raft voter in groupID from fromNode to toNode using a
// single §4.3 atomic ChangeMembership call. Self-removal is handled by the joint
// commit-time step-down hook in raft.Node (jointPromoteCh closes BEFORE
// state=Follower), so the caller wakes up with nil even when removing self.
func (e *DataGroupPlanExecutor) MoveReplica(ctx context.Context, groupID, fromNode, toNode string) error {
	dg := e.dgMgr.Get(groupID)
	if dg == nil {
		return fmt.Errorf("data_group_executor: group %q not found", groupID)
	}

	node := e.nodeFor(dg)
	if !node.IsLeader() {
		return fmt.Errorf("data_group_executor: not leader of group %q", groupID)
	}

	// Pre-flight: fromNode must be a current voter so we don't accidentally
	// evict an unrelated node if the rebalancer has a stale plan.
	if !slices.Contains(dg.PeerIDs(), fromNode) {
		return fmt.Errorf("data_group_executor: fromNode %q is not a voter in group %q", fromNode, groupID)
	}

	toAddr, err := e.resolveAddr(toNode)
	if err != nil {
		return fmt.Errorf("data_group_executor: resolve toNode: %w", err)
	}
	fromAddr, err := e.resolveAddr(fromNode)
	if err != nil {
		return fmt.Errorf("data_group_executor: resolve fromNode: %w", err)
	}

	// Single atomic §4.3 call: learner-first add + joint propose + commit + leave.
	// Self-removal: joint commit-time hook closes jointPromoteCh BEFORE state=Follower
	// (verified ordering invariant in internal/raft/raft.go), so caller wakes up nil.
	addCtx, cancel := context.WithTimeout(ctx, e.catchUpTimeout)
	defer cancel()
	if err := node.ChangeMembership(addCtx,
		[]raft.ServerEntry{{ID: toNode, Address: toAddr, Suffrage: raft.Voter}},
		[]string{fromAddr}, // data-Raft uses address as peerKey
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

	// Persist new membership to MetaFSM. Even after self-removal of the data-Raft
	// leader, this node may still be meta-Raft leader; if not, ProposeShardGroup
	// forwards or fails — caller can reconcile later.
	if err := e.sgUpdater.ProposeShardGroup(ctx, ShardGroupEntry{ID: groupID, PeerIDs: newPeers}); err != nil {
		return fmt.Errorf("data_group_executor: ProposeShardGroup: %w", err)
	}

	e.dgMgr.Add(NewDataGroupWithBackend(groupID, newPeers, dg.Backend()))
	return nil
}

// compile-time check: *raft.Node must satisfy dataRaftNode.
var _ dataRaftNode = (*raft.Node)(nil)

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
