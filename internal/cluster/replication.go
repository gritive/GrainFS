package cluster

import (
	"sync"
)

// ReplicaInfo describes the replication deficit for a key.
type ReplicaInfo struct {
	Key     string
	Current int
	Desired int
}

// RepairPlan describes a repair action: copy data from source to target.
type RepairPlan struct {
	Key        string
	SourceNode string
	TargetNode string
}

// ReplicationMonitor tracks which nodes hold replicas of each object
// and identifies under-replicated keys.
//
// TODO(phase18): currently 0 production callers; will be renamed to
// ShardPlacementMonitor and wired to Raft FSM ownership map in Cluster EC work.
// See ~/.gstack/projects/gritive-grains/whitekid-master-design-20260421-024627.md
type ReplicationMonitor struct {
	mu       sync.RWMutex
	desired  int
	replicas map[string]map[string]struct{} // key -> set of node IDs
}

// NewReplicationMonitor creates a monitor that expects `desired` replicas per key.
func NewReplicationMonitor(desired int) *ReplicationMonitor {
	return &ReplicationMonitor{
		desired:  desired,
		replicas: make(map[string]map[string]struct{}),
	}
}

// RecordReplica records that a node holds a replica of the given key.
func (rm *ReplicationMonitor) RecordReplica(key, nodeID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.replicas[key] == nil {
		rm.replicas[key] = make(map[string]struct{})
	}
	rm.replicas[key][nodeID] = struct{}{}
}

// RemoveNode removes a node from all replica sets (simulating node failure).
func (rm *ReplicationMonitor) RemoveNode(nodeID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for key, nodes := range rm.replicas {
		delete(nodes, nodeID)
		if len(nodes) == 0 {
			delete(rm.replicas, key)
		}
	}
}

// ClearKey removes tracking for a key (e.g., after deletion).
func (rm *ReplicationMonitor) ClearKey(key string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.replicas, key)
}

// UnderReplicated returns keys that have fewer replicas than desired.
func (rm *ReplicationMonitor) UnderReplicated() []ReplicaInfo {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	var result []ReplicaInfo
	for key, nodes := range rm.replicas {
		if len(nodes) < rm.desired {
			result = append(result, ReplicaInfo{
				Key:     key,
				Current: len(nodes),
				Desired: rm.desired,
			})
		}
	}
	return result
}

// PlanRepairs generates repair plans for under-replicated keys.
// It picks a source node (one that has the data) and a target node
// (one that doesn't) for each deficient key.
func (rm *ReplicationMonitor) PlanRepairs(availableNodes []string) []RepairPlan {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	var plans []RepairPlan
	for key, holders := range rm.replicas {
		deficit := rm.desired - len(holders)
		if deficit <= 0 {
			continue
		}

		// Pick a source (any node that has it)
		var source string
		for n := range holders {
			source = n
			break
		}

		// Find targets (nodes that don't have it)
		repaired := 0
		for _, candidate := range availableNodes {
			if repaired >= deficit {
				break
			}
			if _, has := holders[candidate]; has {
				continue
			}
			plans = append(plans, RepairPlan{
				Key:        key,
				SourceNode: source,
				TargetNode: candidate,
			})
			repaired++
		}
	}
	return plans
}
