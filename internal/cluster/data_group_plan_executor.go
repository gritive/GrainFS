package cluster

import (
	"context"
	"fmt"
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
	dgMgr          *DataGroupManager
	addrBook       NodeAddressBook
	sgUpdater      ShardGroupUpdater
	catchUpTimeout time.Duration // max wait for learner catch-up; default 30s
	// nodeFor is the default impl; override in tests via newDataGroupPlanExecutorForTest.
	nodeFor func(*DataGroup) dataRaftNode
}

// NewDataGroupPlanExecutor creates a DataGroupPlanExecutor.
// addrBook is typically metaRaft.FSM(); sgUpdater is typically metaRaft.
func NewDataGroupPlanExecutor(
	dgMgr *DataGroupManager,
	addrBook NodeAddressBook,
	sgUpdater ShardGroupUpdater,
) *DataGroupPlanExecutor {
	e := &DataGroupPlanExecutor{
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

	// Resolve toNode's QUIC address
	toAddr, err := e.resolveAddr(toNode)
	if err != nil {
		return fmt.Errorf("data_group_executor: resolve toNode: %w", err)
	}

	// Step 1: Add toNode as learner
	if err := node.AddLearner(toNode, toAddr); err != nil {
		return fmt.Errorf("data_group_executor: AddLearner %s: %w", toNode, err)
	}

	// Step 2: Wait for learner ConfChange to be applied (matchIndex entry appears)
	const trackTimeout = 500 * time.Millisecond
	deadline := time.Now().Add(trackTimeout)
	for {
		if _, ok := node.PeerMatchIndex(toAddr); ok {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("data_group_executor: learner %s not tracked after %v", toNode, trackTimeout)
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Step 3: Wait for catch-up — learner's matchIndex >= committed at the time of AddLearner.
	// Guarded by catchUpTimeout so a stalled learner never blocks the caller forever.
	committed := node.CommittedIndex()
	catchCtx, cancel := context.WithTimeout(ctx, e.catchUpTimeout)
	defer cancel()
	for {
		if match, ok := node.PeerMatchIndex(toAddr); ok && match >= committed {
			break
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-catchCtx.Done():
			return fmt.Errorf("data_group_executor: catch-up timed out waiting for %s: %w", toNode, catchCtx.Err())
		}
	}

	// Step 4: Promote learner to full voter
	if err := node.PromoteToVoter(toNode); err != nil {
		return fmt.Errorf("data_group_executor: PromoteToVoter %s: %w", toNode, err)
	}

	// Step 5: Remove old voter (data-Raft uses QUIC address as voter key)
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
