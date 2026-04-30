package cluster

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
)

// ErrLeadershipTransferred is returned by MoveReplica when fromNode is the local
// node. Leadership has been handed off; the caller should abort the plan and let
// the new leader re-propose.
var ErrLeadershipTransferred = errors.New("data_group_executor: leadership transferred to peer — retry on new leader")

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
}

// DataGroupPlanExecutor implements GroupRebalancer via real Raft voter migration.
//
// MoveReplica algorithm:
//
//	AddLearner(toNode) → wait learner tracked → wait catch-up → PromoteToVoter →
//	RemoveVoter(fromAddr) → ProposeShardGroup (MetaFSM) → DataGroupManager.Add
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

// MoveReplica migrates a Raft voter in groupID from fromNode to toNode.
// fromNode must not be the local leader; leadership transfer is caller's responsibility.
func (e *DataGroupPlanExecutor) MoveReplica(ctx context.Context, groupID, fromNode, toNode string) error {
	dg := e.dgMgr.Get(groupID)
	if dg == nil {
		return fmt.Errorf("data_group_executor: group %q not found", groupID)
	}

	node := e.nodeFor(dg)
	if !node.IsLeader() {
		return fmt.Errorf("data_group_executor: not leader of group %q", groupID)
	}

	// Self-removal guard: if we are the data-Raft leader and fromNode is this
	// node, transferring ourselves out is unsafe mid-migration. Hand off
	// leadership first; the new leader will pick up the plan on the next cycle.
	if e.localNodeID != "" && fromNode == e.localNodeID {
		if err := node.TransferLeadership(); err != nil {
			return fmt.Errorf("data_group_executor: TransferLeadership before self-removal: %w", err)
		}
		return ErrLeadershipTransferred
	}

	// Pre-flight: fromNode must be a current voter so we don't accidentally
	// evict an unrelated node if the rebalancer has a stale plan.
	if !slices.Contains(dg.PeerIDs(), fromNode) {
		return fmt.Errorf("data_group_executor: fromNode %q is not a voter in group %q", fromNode, groupID)
	}

	// Resolve toNode's QUIC address
	toAddr, err := e.resolveAddr(toNode)
	if err != nil {
		return fmt.Errorf("data_group_executor: resolve toNode: %w", err)
	}

	// Steps 1-4 unified: AddVoterCtx performs learner-first internally
	// (AddLearner → catch-up watcher → PromoteToVoter), bounded by catchUpTimeout.
	addCtx, cancel := context.WithTimeout(ctx, e.catchUpTimeout)
	defer cancel()
	if err := node.AddVoterCtx(addCtx, toNode, toAddr); err != nil {
		return fmt.Errorf("data_group_executor: AddVoter %s: %w", toNode, err)
	}

	// Step 5: Remove old voter (data-Raft uses QUIC address as voter key).
	// NOTE: after this point the Raft membership has changed. If any subsequent
	// step fails, the Raft log reflects the new membership but MetaFSM and the
	// local DataGroupManager remain stale. Callers should retry MoveReplica or
	// reconcile via a full rebalance pass; the operation is idempotent when
	// toNode is already a voter and fromNode is already absent.
	fromAddr, err := e.resolveAddr(fromNode)
	if err != nil {
		return fmt.Errorf("data_group_executor: resolve fromNode: %w", err)
	}
	if err := node.RemoveVoter(fromAddr); err != nil {
		return fmt.Errorf("data_group_executor: RemoveVoter %s: %w", fromNode, err)
	}

	// Step 6: Build new peerIDs (replace fromNode with toNode)
	oldPeers := dg.PeerIDs()
	newPeers := make([]string, 0, len(oldPeers))
	for _, p := range oldPeers {
		if p != fromNode {
			newPeers = append(newPeers, p)
		}
	}
	newPeers = append(newPeers, toNode)

	// Step 7: Persist new membership to MetaFSM
	if err := e.sgUpdater.ProposeShardGroup(ctx, ShardGroupEntry{ID: groupID, PeerIDs: newPeers}); err != nil {
		return fmt.Errorf("data_group_executor: ProposeShardGroup: %w", err)
	}

	// Step 8: Update local DataGroupManager (COW; Add overwrites by ID)
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
