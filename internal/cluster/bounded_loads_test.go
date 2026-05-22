package cluster

import (
	"testing"
	"time"
)

func TestBoundedLoads_EmptySnapshot(t *testing.T) {
	store := NewNodeStatsStore(2 * time.Minute)
	bl := NewBoundedLoads(store, BoundedLoadsParams{C: 1.25, CLow: 1.0})

	snap := bl.Snapshot()
	if snap.AvgRPS != 0 {
		t.Fatalf("empty store: AvgRPS=%v want 0", snap.AvgRPS)
	}
	if len(snap.HotSet) != 0 {
		t.Fatalf("empty store: HotSet=%v want empty", snap.HotSet)
	}
}

func TestBoundedLoads_IsHotMissingNode(t *testing.T) {
	store := NewNodeStatsStore(2 * time.Minute)
	bl := NewBoundedLoads(store, BoundedLoadsParams{C: 1.25, CLow: 1.0})
	if bl.IsHot("missing-node") {
		t.Fatalf("missing node should not be hot")
	}
}
