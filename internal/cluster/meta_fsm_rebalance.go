package cluster

import (
	"fmt"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

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
