package cluster

import (
	"fmt"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// MetaCmdType aliases the FlatBuffers-generated type for use within this package.
type MetaCmdType = clusterpb.MetaCmdType

const (
	MetaCmdTypeNoOp                 = clusterpb.MetaCmdTypeNoOp
	MetaCmdTypeAddNode              = clusterpb.MetaCmdTypeAddNode
	MetaCmdTypeRemoveNode           = clusterpb.MetaCmdTypeRemoveNode
	MetaCmdTypePutShardGroup        = clusterpb.MetaCmdTypePutShardGroup        // PR-C
	MetaCmdTypePutBucketAssignment  = clusterpb.MetaCmdTypePutBucketAssignment  // PR-D
	MetaCmdTypeSetLoadSnapshot      = clusterpb.MetaCmdTypeSetLoadSnapshot      // PR-D
	MetaCmdTypeProposeRebalancePlan = clusterpb.MetaCmdTypeProposeRebalancePlan // PR-D
	MetaCmdTypeAbortPlan            = clusterpb.MetaCmdTypeAbortPlan            // PR-D
)

// MetaNodeEntry is the plain-Go representation of a cluster member.
type MetaNodeEntry struct {
	ID      string
	Address string
	Role    uint8 // 0=Voter 1=Learner
}

// ShardGroupEntry describes data Raft group membership.
// bucket→group mapping is managed separately by Router. key-range sharding excluded.
type ShardGroupEntry struct {
	ID      string
	PeerIDs []string
}

// LoadStatEntry is the plain-Go representation of per-node load statistics.
type LoadStatEntry struct {
	NodeID         string
	DiskUsedPct    float64
	DiskAvailBytes uint64
	RequestsPerSec float64
	UpdatedAt      time.Time
}

// RebalancePlan describes a single voter migration between data Raft group nodes.
type RebalancePlan struct {
	PlanID    string
	GroupID   string
	FromNode  string
	ToNode    string
	CreatedAt time.Time
}

// MetaFSM implements raft.Snapshotter for the meta-Raft group.
// It holds cluster membership state.
//
// Lock discipline: mu is a RWMutex shared by all three state maps (nodes,
// shardGroups, bucketAssignments) and the callback field.
// RWMutex is justified here because:
//   - There is exactly ONE writer goroutine (runApplyLoop), so write contention
//     is zero and write locks are never contended.
//   - Multiple reader goroutines (HTTP handlers, routing) hold RLock concurrently
//     while the writer is idle — RWMutex allows this, plain Mutex would not.
//   - Snapshot() and Restore() must read/write all three maps atomically; a
//     single lock (rather than per-map atomics) is the simplest consistency guarantee.
//   - onBucketAssigned is stored in the same lock to ensure the callback always
//     sees the freshly updated map state without a separate atomic.
type MetaFSM struct {
	mu                sync.RWMutex
	nodes             map[string]MetaNodeEntry
	shardGroups       map[string]ShardGroupEntry // key = group ID
	bucketAssignments map[string]string          // bucket → group_id (PR-D)
	loadSnapshot      map[string]LoadStatEntry   // node_id → stats (PR-D)
	activePlan        *RebalancePlan             // nil = no active plan (PR-D)
	onBucketAssigned  func(string, string)       // protected by mu; set before Start() (PR-D)
	onRebalancePlan   func(*RebalancePlan)       // must not block; set before Start() (PR-D)
	onShardGroupAdded func(ShardGroupEntry)      // fired after PutShardGroup applies; protected by mu (v0.0.7.0)
}

func NewMetaFSM() *MetaFSM {
	return &MetaFSM{
		nodes:             make(map[string]MetaNodeEntry),
		shardGroups:       make(map[string]ShardGroupEntry),
		bucketAssignments: make(map[string]string),
		loadSnapshot:      make(map[string]LoadStatEntry),
	}
}

// applyCmd decodes a MetaCmd FlatBuffers envelope and mutates state.
// Called by MetaRaft.runApplyLoop on each committed log entry.
func (f *MetaFSM) applyCmd(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: empty command")
	}
	var (
		cmd    *clusterpb.MetaCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaCmd flatbuffer: %v", r)
			}
		}()
		cmd = clusterpb.GetRootAsMetaCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	switch cmd.Type() {
	case clusterpb.MetaCmdTypeNoOp:
		return nil
	case clusterpb.MetaCmdTypeAddNode:
		return f.applyAddNode(cmd.DataBytes())
	case clusterpb.MetaCmdTypeRemoveNode:
		return f.applyRemoveNode(cmd.DataBytes())
	case clusterpb.MetaCmdTypePutShardGroup:
		return f.applyPutShardGroup(cmd.DataBytes())
	case clusterpb.MetaCmdTypePutBucketAssignment:
		return f.applyPutBucketAssignment(cmd.DataBytes())
	case clusterpb.MetaCmdTypeSetLoadSnapshot:
		return f.applySetLoadSnapshot(cmd.DataBytes())
	case clusterpb.MetaCmdTypeProposeRebalancePlan:
		return f.applyProposeRebalancePlan(cmd.DataBytes())
	case clusterpb.MetaCmdTypeAbortPlan:
		return f.applyAbortPlan(cmd.DataBytes())
	default:
		log.Warn().Stringer("type", cmd.Type()).Msg("meta_fsm: unknown command type, ignoring")
		return nil
	}
}

func (f *MetaFSM) applyAddNode(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: AddNode: empty payload")
	}
	var (
		c      *clusterpb.MetaAddNodeCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaAddNodeCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaAddNodeCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	node := c.Node(nil)
	if node == nil {
		return fmt.Errorf("meta_fsm: AddNode: nil node")
	}
	entry := MetaNodeEntry{
		ID:      string(node.Id()),
		Address: string(node.Address()),
		Role:    node.Role(),
	}
	if entry.ID == "" {
		return fmt.Errorf("meta_fsm: AddNode: empty node ID")
	}
	f.mu.Lock()
	f.nodes[entry.ID] = entry
	f.mu.Unlock()
	return nil
}

func (f *MetaFSM) applyRemoveNode(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: RemoveNode: empty payload")
	}
	var (
		c      *clusterpb.MetaRemoveNodeCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaRemoveNodeCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaRemoveNodeCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	nodeID := string(c.NodeId())
	f.mu.Lock()
	delete(f.nodes, nodeID)
	f.mu.Unlock()
	return nil
}

func (f *MetaFSM) applyPutShardGroup(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: PutShardGroup: empty payload")
	}
	var (
		c      *clusterpb.MetaPutShardGroupCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaPutShardGroupCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaPutShardGroupCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	sg := c.Group(nil)
	if sg == nil {
		return fmt.Errorf("meta_fsm: PutShardGroup: nil group")
	}
	peers := make([]string, sg.PeerIdsLength())
	for i := 0; i < sg.PeerIdsLength(); i++ {
		peers[i] = string(sg.PeerIds(i))
	}
	entry := ShardGroupEntry{
		ID:      string(sg.Id()),
		PeerIDs: peers,
	}
	if entry.ID == "" {
		return fmt.Errorf("meta_fsm: PutShardGroup: empty group ID")
	}
	if len(peers) == 0 {
		return fmt.Errorf("meta_fsm: PutShardGroup: group %q has no peers", entry.ID)
	}
	f.mu.Lock()
	f.shardGroups[entry.ID] = entry
	cb := f.onShardGroupAdded
	f.mu.Unlock()
	if cb != nil {
		// Defensive copy of peers — callback may keep references.
		cbPeers := make([]string, len(entry.PeerIDs))
		copy(cbPeers, entry.PeerIDs)
		cb(ShardGroupEntry{ID: entry.ID, PeerIDs: cbPeers})
	}
	return nil
}

func (f *MetaFSM) applyPutBucketAssignment(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: PutBucketAssignment: empty payload")
	}
	var (
		c      *clusterpb.MetaPutBucketAssignmentCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaPutBucketAssignmentCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaPutBucketAssignmentCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	entry := c.Entry(nil)
	if entry == nil {
		return fmt.Errorf("meta_fsm: PutBucketAssignment: nil entry")
	}
	bucket := string(entry.Bucket())
	groupID := string(entry.GroupId())
	if bucket == "" {
		return fmt.Errorf("meta_fsm: PutBucketAssignment: empty bucket")
	}
	if groupID == "" {
		return fmt.Errorf("meta_fsm: PutBucketAssignment: empty groupID")
	}

	f.mu.Lock()
	f.bucketAssignments[bucket] = groupID
	cb := f.onBucketAssigned
	f.mu.Unlock()

	if cb != nil {
		cb(bucket, groupID)
	}
	return nil
}

func (f *MetaFSM) applySetLoadSnapshot(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: SetLoadSnapshot: empty payload")
	}
	var (
		c      *clusterpb.MetaSetLoadSnapshotCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaSetLoadSnapshotCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaSetLoadSnapshotCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	newSnap := make(map[string]LoadStatEntry, c.EntriesLength())
	var e clusterpb.LoadStatEntry
	for i := 0; i < c.EntriesLength(); i++ {
		if !c.Entries(&e, i) {
			continue
		}
		entry := LoadStatEntry{
			NodeID:         string(e.NodeId()),
			DiskUsedPct:    e.DiskUsedPct(),
			DiskAvailBytes: e.DiskAvailBytes(),
			RequestsPerSec: e.RequestsPerSec(),
			UpdatedAt:      time.Unix(e.UpdatedAtUnix(), 0),
		}
		newSnap[entry.NodeID] = entry
	}
	f.mu.Lock()
	f.loadSnapshot = newSnap
	f.mu.Unlock()
	return nil
}

func (f *MetaFSM) applyProposeRebalancePlan(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: ProposeRebalancePlan: empty payload")
	}
	var (
		c      *clusterpb.MetaProposeRebalancePlanCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaProposeRebalancePlanCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaProposeRebalancePlanCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	p := c.Plan(nil)
	if p == nil {
		return fmt.Errorf("meta_fsm: ProposeRebalancePlan: nil plan")
	}
	plan := &RebalancePlan{
		PlanID:    string(p.PlanId()),
		GroupID:   string(p.GroupId()),
		FromNode:  string(p.FromNode()),
		ToNode:    string(p.ToNode()),
		CreatedAt: time.Unix(p.CreatedAtUnix(), 0),
	}
	if plan.PlanID == "" {
		return fmt.Errorf("meta_fsm: ProposeRebalancePlan: empty plan ID")
	}

	f.mu.Lock()
	if f.activePlan != nil {
		f.mu.Unlock()
		return fmt.Errorf("meta_fsm: ProposeRebalancePlan: active plan %q already exists", f.activePlan.PlanID)
	}
	f.activePlan = plan
	cb := f.onRebalancePlan
	f.mu.Unlock()

	if cb != nil {
		cb(plan)
	}
	return nil
}

func (f *MetaFSM) applyAbortPlan(data []byte) error {
	if len(data) == 0 {
		return nil // idempotent: empty payload treated as no-op
	}
	var (
		c      *clusterpb.MetaAbortPlanCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaAbortPlanCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaAbortPlanCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	planID := string(c.PlanId())
	reason := c.Reason()
	f.mu.Lock()
	if f.activePlan == nil || f.activePlan.PlanID != planID {
		f.mu.Unlock()
		return nil // idempotent: no-op if plan absent or ID mismatch (M5)
	}
	log.Info().Str("plan_id", planID).Str("reason", reason.String()).Msg("meta_fsm: aborting active plan")
	f.activePlan = nil
	f.mu.Unlock()
	return nil
}

// LoadSnapshot returns a copy of the current per-node load statistics.
func (f *MetaFSM) LoadSnapshot() map[string]LoadStatEntry {
	f.mu.RLock()
	out := make(map[string]LoadStatEntry, len(f.loadSnapshot))
	for k, v := range f.loadSnapshot {
		out[k] = v
	}
	f.mu.RUnlock()
	return out
}

// ActivePlanID returns the plan ID of the currently active rebalance plan, or "".
func (f *MetaFSM) ActivePlanID() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.activePlan == nil {
		return ""
	}
	return f.activePlan.PlanID
}

// ActivePlan returns a copy of the currently active rebalance plan, or nil.
func (f *MetaFSM) ActivePlan() *RebalancePlan {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.activePlan == nil {
		return nil
	}
	cp := *f.activePlan
	return &cp
}

// SetOnRebalancePlan registers a callback fired after each ProposeRebalancePlan is applied.
// The callback must not block; it is called with f.mu released.
// Must be called before MetaRaft.Start() to avoid a data race with the apply loop.
func (f *MetaFSM) SetOnRebalancePlan(fn func(*RebalancePlan)) {
	f.mu.Lock()
	f.onRebalancePlan = fn
	f.mu.Unlock()
}

// SetOnShardGroupAdded registers a callback fired after each PutShardGroup is applied.
// The callback fires on every applied entry (including idempotent overwrites with
// identical PeerIDs) — caller must dedupe if needed.
// Must not block. Set before Start() to avoid races with the apply loop.
func (f *MetaFSM) SetOnShardGroupAdded(fn func(ShardGroupEntry)) {
	f.mu.Lock()
	f.onShardGroupAdded = fn
	f.mu.Unlock()
}

// SetOnBucketAssigned registers a callback fired after each PutBucketAssignment is applied.
// Must be called before MetaRaft.Start() to avoid a data race with the apply loop.
func (f *MetaFSM) SetOnBucketAssigned(fn func(bucket, groupID string)) {
	f.mu.Lock()
	f.onBucketAssigned = fn
	f.mu.Unlock()
}

// BucketAssignments returns a copy of the current bucket→group_id map.
func (f *MetaFSM) BucketAssignments() map[string]string {
	f.mu.RLock()
	out := make(map[string]string, len(f.bucketAssignments))
	for k, v := range f.bucketAssignments {
		out[k] = v
	}
	f.mu.RUnlock()
	return out
}

// ShardGroups returns a deep copy of current shard groups.
// PeerIDs slices are copied so callers cannot mutate FSM state.
func (f *MetaFSM) ShardGroups() []ShardGroupEntry {
	f.mu.RLock()
	out := make([]ShardGroupEntry, 0, len(f.shardGroups))
	for _, sg := range f.shardGroups {
		peers := make([]string, len(sg.PeerIDs))
		copy(peers, sg.PeerIDs)
		out = append(out, ShardGroupEntry{ID: sg.ID, PeerIDs: peers})
	}
	f.mu.RUnlock()
	return out
}

// ShardGroup returns the entry for id and true, or zero-value and false if not found.
// Returned PeerIDs is a defensive copy.
func (f *MetaFSM) ShardGroup(id string) (ShardGroupEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	g, ok := f.shardGroups[id]
	if !ok {
		return ShardGroupEntry{}, false
	}
	peers := make([]string, len(g.PeerIDs))
	copy(peers, g.PeerIDs)
	return ShardGroupEntry{ID: g.ID, PeerIDs: peers}, true
}

// Snapshot serializes current state as FlatBuffers MetaStateSnapshot.
func (f *MetaFSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	nodes := make([]MetaNodeEntry, 0, len(f.nodes))
	for _, n := range f.nodes {
		nodes = append(nodes, n)
	}
	shardGroups := make([]ShardGroupEntry, 0, len(f.shardGroups))
	for _, sg := range f.shardGroups {
		shardGroups = append(shardGroups, sg)
	}
	type bucketKV struct{ bucket, groupID string }
	buckets := make([]bucketKV, 0, len(f.bucketAssignments))
	for bucket, groupID := range f.bucketAssignments {
		buckets = append(buckets, bucketKV{bucket, groupID})
	}
	lsEntries := make([]LoadStatEntry, 0, len(f.loadSnapshot))
	for _, v := range f.loadSnapshot {
		lsEntries = append(lsEntries, v)
	}
	var activePlanCopy *RebalancePlan
	if f.activePlan != nil {
		cp := *f.activePlan
		activePlanCopy = &cp
	}
	f.mu.RUnlock()

	b := clusterBuilderPool.Get()

	// All nested objects must be built BEFORE MetaStateSnapshotStart (A1).

	// Build ShardGroupEntry offsets
	sgOffs := make([]flatbuffers.UOffsetT, len(shardGroups))
	for i := len(shardGroups) - 1; i >= 0; i-- {
		sg := shardGroups[i]
		idOff := b.CreateString(sg.ID)
		peerOffs := make([]flatbuffers.UOffsetT, len(sg.PeerIDs))
		for j := len(sg.PeerIDs) - 1; j >= 0; j-- {
			peerOffs[j] = b.CreateString(sg.PeerIDs[j])
		}
		clusterpb.ShardGroupEntryStartPeerIdsVector(b, len(peerOffs))
		for j := len(peerOffs) - 1; j >= 0; j-- {
			b.PrependUOffsetT(peerOffs[j])
		}
		peerVec := b.EndVector(len(peerOffs))
		clusterpb.ShardGroupEntryStart(b)
		clusterpb.ShardGroupEntryAddId(b, idOff)
		clusterpb.ShardGroupEntryAddPeerIds(b, peerVec)
		sgOffs[i] = clusterpb.ShardGroupEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartShardGroupsVector(b, len(sgOffs))
	for i := len(sgOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(sgOffs[i])
	}
	sgVec := b.EndVector(len(sgOffs))

	// Build MetaNodeEntry offsets
	nodeOffs := make([]flatbuffers.UOffsetT, len(nodes))
	for i := len(nodes) - 1; i >= 0; i-- {
		n := nodes[i]
		idOff := b.CreateString(n.ID)
		addrOff := b.CreateString(n.Address)
		clusterpb.MetaNodeEntryStart(b)
		clusterpb.MetaNodeEntryAddId(b, idOff)
		clusterpb.MetaNodeEntryAddAddress(b, addrOff)
		clusterpb.MetaNodeEntryAddRole(b, n.Role)
		nodeOffs[i] = clusterpb.MetaNodeEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartNodesVector(b, len(nodeOffs))
	for i := len(nodeOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(nodeOffs[i])
	}
	nodesVec := b.EndVector(len(nodeOffs))

	// Build BucketAssignmentEntry offsets
	baOffs := make([]flatbuffers.UOffsetT, len(buckets))
	for i := len(buckets) - 1; i >= 0; i-- {
		bkt := buckets[i]
		bucketOff := b.CreateString(bkt.bucket)
		groupIDOff := b.CreateString(bkt.groupID)
		clusterpb.BucketAssignmentEntryStart(b)
		clusterpb.BucketAssignmentEntryAddBucket(b, bucketOff)
		clusterpb.BucketAssignmentEntryAddGroupId(b, groupIDOff)
		baOffs[i] = clusterpb.BucketAssignmentEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartBucketAssignmentsVector(b, len(baOffs))
	for i := len(baOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(baOffs[i])
	}
	baVec := b.EndVector(len(baOffs))

	// Build LoadStatEntry offsets (lsVec must be complete before MetaStateSnapshotStart)
	lsOffs := make([]flatbuffers.UOffsetT, len(lsEntries))
	for i := len(lsEntries) - 1; i >= 0; i-- {
		e := lsEntries[i]
		nodeIDOff := b.CreateString(e.NodeID)
		clusterpb.LoadStatEntryStart(b)
		clusterpb.LoadStatEntryAddNodeId(b, nodeIDOff)
		clusterpb.LoadStatEntryAddDiskUsedPct(b, e.DiskUsedPct)
		clusterpb.LoadStatEntryAddDiskAvailBytes(b, e.DiskAvailBytes)
		clusterpb.LoadStatEntryAddRequestsPerSec(b, e.RequestsPerSec)
		clusterpb.LoadStatEntryAddUpdatedAtUnix(b, e.UpdatedAt.Unix())
		lsOffs[i] = clusterpb.LoadStatEntryEnd(b)
	}
	clusterpb.MetaStateSnapshotStartLoadSnapshotVector(b, len(lsOffs))
	for i := len(lsOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(lsOffs[i])
	}
	lsVec := b.EndVector(len(lsOffs))

	// Build active plan offset (activePlanOff must be complete before MetaStateSnapshotStart)
	var activePlanOff flatbuffers.UOffsetT
	if activePlanCopy != nil {
		planIDOff := b.CreateString(activePlanCopy.PlanID)
		groupIDOff := b.CreateString(activePlanCopy.GroupID)
		fromOff := b.CreateString(activePlanCopy.FromNode)
		toOff := b.CreateString(activePlanCopy.ToNode)
		clusterpb.RebalancePlanStart(b)
		clusterpb.RebalancePlanAddPlanId(b, planIDOff)
		clusterpb.RebalancePlanAddGroupId(b, groupIDOff)
		clusterpb.RebalancePlanAddFromNode(b, fromOff)
		clusterpb.RebalancePlanAddToNode(b, toOff)
		clusterpb.RebalancePlanAddCreatedAtUnix(b, activePlanCopy.CreatedAt.Unix())
		activePlanOff = clusterpb.RebalancePlanEnd(b)
	}

	clusterpb.MetaStateSnapshotStart(b)
	clusterpb.MetaStateSnapshotAddNodes(b, nodesVec)
	clusterpb.MetaStateSnapshotAddShardGroups(b, sgVec)
	clusterpb.MetaStateSnapshotAddBucketAssignments(b, baVec)
	clusterpb.MetaStateSnapshotAddLoadSnapshot(b, lsVec)
	if activePlanCopy != nil {
		clusterpb.MetaStateSnapshotAddActivePlan(b, activePlanOff)
	}
	root := clusterpb.MetaStateSnapshotEnd(b)
	return fbFinish(b, root), nil
}

// Restore deserializes a MetaStateSnapshot and replaces current state.
func (f *MetaFSM) Restore(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: Restore: empty snapshot")
	}
	var (
		snap   *clusterpb.MetaStateSnapshot
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaStateSnapshot flatbuffer: %v", r)
			}
		}()
		snap = clusterpb.GetRootAsMetaStateSnapshot(data, 0)
	}()
	if decErr != nil {
		return decErr
	}

	newNodes := make(map[string]MetaNodeEntry, snap.NodesLength())
	var nodeEntry clusterpb.MetaNodeEntry
	for i := 0; i < snap.NodesLength(); i++ {
		if snap.Nodes(&nodeEntry, i) {
			e := MetaNodeEntry{
				ID:      string(nodeEntry.Id()),
				Address: string(nodeEntry.Address()),
				Role:    nodeEntry.Role(),
			}
			newNodes[e.ID] = e
		}
	}

	newShardGroups := make(map[string]ShardGroupEntry, snap.ShardGroupsLength())
	var sgEntry clusterpb.ShardGroupEntry
	for i := 0; i < snap.ShardGroupsLength(); i++ {
		if !snap.ShardGroups(&sgEntry, i) {
			return fmt.Errorf("meta_fsm: Restore: shard group %d decode failed", i)
		}
		peers := make([]string, sgEntry.PeerIdsLength())
		for j := 0; j < sgEntry.PeerIdsLength(); j++ {
			peers[j] = string(sgEntry.PeerIds(j))
		}
		e := ShardGroupEntry{
			ID:      string(sgEntry.Id()),
			PeerIDs: peers,
		}
		newShardGroups[e.ID] = e
	}

	newBucketAssignments := make(map[string]string, snap.BucketAssignmentsLength())
	var baEntry clusterpb.BucketAssignmentEntry
	for i := 0; i < snap.BucketAssignmentsLength(); i++ {
		if !snap.BucketAssignments(&baEntry, i) {
			return fmt.Errorf("meta_fsm: Restore: bucket assignment %d decode failed", i)
		}
		bucket := string(baEntry.Bucket())
		newBucketAssignments[bucket] = string(baEntry.GroupId())
	}

	newLoadSnapshot := make(map[string]LoadStatEntry, snap.LoadSnapshotLength())
	var lsEntry clusterpb.LoadStatEntry
	for i := 0; i < snap.LoadSnapshotLength(); i++ {
		if !snap.LoadSnapshot(&lsEntry, i) {
			continue
		}
		e := LoadStatEntry{
			NodeID:         string(lsEntry.NodeId()),
			DiskUsedPct:    lsEntry.DiskUsedPct(),
			DiskAvailBytes: lsEntry.DiskAvailBytes(),
			RequestsPerSec: lsEntry.RequestsPerSec(),
			UpdatedAt:      time.Unix(lsEntry.UpdatedAtUnix(), 0),
		}
		newLoadSnapshot[e.NodeID] = e
	}

	var newActivePlan *RebalancePlan
	var planFB clusterpb.RebalancePlan
	if p := snap.ActivePlan(&planFB); p != nil && len(p.PlanId()) > 0 {
		newActivePlan = &RebalancePlan{
			PlanID:    string(p.PlanId()),
			GroupID:   string(p.GroupId()),
			FromNode:  string(p.FromNode()),
			ToNode:    string(p.ToNode()),
			CreatedAt: time.Unix(p.CreatedAtUnix(), 0),
		}
	}

	f.mu.Lock()
	f.nodes = newNodes
	f.shardGroups = newShardGroups
	f.bucketAssignments = newBucketAssignments
	f.loadSnapshot = newLoadSnapshot
	f.activePlan = newActivePlan
	cb := f.onBucketAssigned
	f.mu.Unlock()
	if cb != nil {
		for bucket, groupID := range newBucketAssignments {
			cb(bucket, groupID)
		}
	}
	// onRebalancePlan is intentionally NOT called here.
	// Rebalancer handles resume on next tick by checking ActivePlan().
	return nil
}

// Nodes returns a copy of current cluster members.
func (f *MetaFSM) Nodes() []MetaNodeEntry {
	f.mu.RLock()
	out := make([]MetaNodeEntry, 0, len(f.nodes))
	for _, n := range f.nodes {
		out = append(out, n)
	}
	f.mu.RUnlock()
	return out
}

// --- encoding helpers ---

// encodeMetaCmd wraps a typed payload in a MetaCmd FlatBuffers envelope.
func encodeMetaCmd(cmdType MetaCmdType, payload []byte) ([]byte, error) {
	b := clusterBuilderPool.Get()
	var dataOff flatbuffers.UOffsetT
	if len(payload) > 0 {
		dataOff = b.CreateByteVector(payload)
	}
	clusterpb.MetaCmdStart(b)
	clusterpb.MetaCmdAddType(b, cmdType)
	if len(payload) > 0 {
		clusterpb.MetaCmdAddData(b, dataOff)
	}
	return fbFinish(b, clusterpb.MetaCmdEnd(b)), nil
}

func encodeMetaAddNodeCmd(node MetaNodeEntry) ([]byte, error) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString(node.ID)
	addrOff := b.CreateString(node.Address)
	clusterpb.MetaNodeEntryStart(b)
	clusterpb.MetaNodeEntryAddId(b, idOff)
	clusterpb.MetaNodeEntryAddAddress(b, addrOff)
	clusterpb.MetaNodeEntryAddRole(b, node.Role)
	nodeOff := clusterpb.MetaNodeEntryEnd(b)

	clusterpb.MetaAddNodeCmdStart(b)
	clusterpb.MetaAddNodeCmdAddNode(b, nodeOff)
	return fbFinish(b, clusterpb.MetaAddNodeCmdEnd(b)), nil
}

func encodeMetaRemoveNodeCmd(nodeID string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString(nodeID)
	clusterpb.MetaRemoveNodeCmdStart(b)
	clusterpb.MetaRemoveNodeCmdAddNodeId(b, idOff)
	return fbFinish(b, clusterpb.MetaRemoveNodeCmdEnd(b)), nil
}

func encodeMetaPutBucketAssignmentCmd(bucket, groupID string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	bucketOff := b.CreateString(bucket)
	groupIDOff := b.CreateString(groupID)
	clusterpb.BucketAssignmentEntryStart(b)
	clusterpb.BucketAssignmentEntryAddBucket(b, bucketOff)
	clusterpb.BucketAssignmentEntryAddGroupId(b, groupIDOff)
	entryOff := clusterpb.BucketAssignmentEntryEnd(b)
	clusterpb.MetaPutBucketAssignmentCmdStart(b)
	clusterpb.MetaPutBucketAssignmentCmdAddEntry(b, entryOff)
	return fbFinish(b, clusterpb.MetaPutBucketAssignmentCmdEnd(b)), nil
}

func encodeMetaSetLoadSnapshotCmd(entries []LoadStatEntry) ([]byte, error) {
	b := clusterBuilderPool.Get()
	entryOffs := make([]flatbuffers.UOffsetT, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		nodeIDOff := b.CreateString(e.NodeID)
		clusterpb.LoadStatEntryStart(b)
		clusterpb.LoadStatEntryAddNodeId(b, nodeIDOff)
		clusterpb.LoadStatEntryAddDiskUsedPct(b, e.DiskUsedPct)
		clusterpb.LoadStatEntryAddDiskAvailBytes(b, e.DiskAvailBytes)
		clusterpb.LoadStatEntryAddRequestsPerSec(b, e.RequestsPerSec)
		clusterpb.LoadStatEntryAddUpdatedAtUnix(b, e.UpdatedAt.Unix())
		entryOffs[i] = clusterpb.LoadStatEntryEnd(b)
	}
	clusterpb.MetaSetLoadSnapshotCmdStartEntriesVector(b, len(entryOffs))
	for i := len(entryOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(entryOffs[i])
	}
	entriesVec := b.EndVector(len(entryOffs))
	clusterpb.MetaSetLoadSnapshotCmdStart(b)
	clusterpb.MetaSetLoadSnapshotCmdAddEntries(b, entriesVec)
	return fbFinish(b, clusterpb.MetaSetLoadSnapshotCmdEnd(b)), nil
}

func encodeMetaProposeRebalancePlanCmd(plan RebalancePlan) ([]byte, error) {
	b := clusterBuilderPool.Get()
	planIDOff := b.CreateString(plan.PlanID)
	groupIDOff := b.CreateString(plan.GroupID)
	fromOff := b.CreateString(plan.FromNode)
	toOff := b.CreateString(plan.ToNode)
	clusterpb.RebalancePlanStart(b)
	clusterpb.RebalancePlanAddPlanId(b, planIDOff)
	clusterpb.RebalancePlanAddGroupId(b, groupIDOff)
	clusterpb.RebalancePlanAddFromNode(b, fromOff)
	clusterpb.RebalancePlanAddToNode(b, toOff)
	clusterpb.RebalancePlanAddCreatedAtUnix(b, plan.CreatedAt.Unix())
	planOff := clusterpb.RebalancePlanEnd(b)
	clusterpb.MetaProposeRebalancePlanCmdStart(b)
	clusterpb.MetaProposeRebalancePlanCmdAddPlan(b, planOff)
	return fbFinish(b, clusterpb.MetaProposeRebalancePlanCmdEnd(b)), nil
}

func encodeMetaAbortPlanCmd(planID string, reason clusterpb.AbortPlanReason) ([]byte, error) {
	b := clusterBuilderPool.Get()
	planIDOff := b.CreateString(planID)
	clusterpb.MetaAbortPlanCmdStart(b)
	clusterpb.MetaAbortPlanCmdAddPlanId(b, planIDOff)
	clusterpb.MetaAbortPlanCmdAddReason(b, reason)
	return fbFinish(b, clusterpb.MetaAbortPlanCmdEnd(b)), nil
}

func encodeMetaPutShardGroupCmd(sg ShardGroupEntry) ([]byte, error) {
	b := clusterBuilderPool.Get()

	idOff := b.CreateString(sg.ID)
	peerOffs := make([]flatbuffers.UOffsetT, len(sg.PeerIDs))
	for i := len(sg.PeerIDs) - 1; i >= 0; i-- {
		peerOffs[i] = b.CreateString(sg.PeerIDs[i])
	}
	clusterpb.ShardGroupEntryStartPeerIdsVector(b, len(peerOffs))
	for i := len(peerOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(peerOffs[i])
	}
	peerVec := b.EndVector(len(peerOffs))

	clusterpb.ShardGroupEntryStart(b)
	clusterpb.ShardGroupEntryAddId(b, idOff)
	clusterpb.ShardGroupEntryAddPeerIds(b, peerVec)
	sgOff := clusterpb.ShardGroupEntryEnd(b)

	clusterpb.MetaPutShardGroupCmdStart(b)
	clusterpb.MetaPutShardGroupCmdAddGroup(b, sgOff)
	return fbFinish(b, clusterpb.MetaPutShardGroupCmdEnd(b)), nil
}
