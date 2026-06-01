package cluster

import "fmt"

// applyDEKRewrapProgress records that nodeID finished rewrapping gen. The
// done-set is replicated FSM state; the leader derives prune-safety from it via
// IsGenFullyRewrapped. (S6a produces no such commands — this path is exercised
// by unit tests until a later slice wires a producer.)
func (f *MetaFSM) applyDEKRewrapProgress(data []byte) error {
	nodeID, gen, err := decodeMetaDEKRewrapProgressCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: DEKRewrapProgress: %w", err)
	}
	if nodeID == "" {
		return fmt.Errorf("meta_fsm: DEKRewrapProgress: empty node_id")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dekRewrapDone == nil {
		f.dekRewrapDone = make(map[uint32]map[string]struct{})
	}
	set := f.dekRewrapDone[gen]
	if set == nil {
		set = make(map[string]struct{})
		f.dekRewrapDone[gen] = set
	}
	set[nodeID] = struct{}{}
	return nil
}

// IsGenFullyRewrapped reports whether every node in nodes has reported
// completion for gen. The caller supplies the node set: prune-safety needs the
// set of nodes that can HOLD gen data (e.g. EC-shard owners), which is not
// necessarily the meta-raft voter set — resolving that set is the later prune
// consumer's responsibility. An empty node set is never "fully rewrapped".
//
// Semantics of a single completion record
//
// A record means "at the time this node reported, it held no at-rest data below
// gen within the ENUMERATED LANE CATEGORIES": EC shards of all object versions
// (S6c-allversions, #692) and packed-blob entries (S6b, #687). It does NOT
// cover:
//   - In-flight S4 seal-at-pinned-gen encodes: a shard currently being written
//     by an ongoing PUT may still be pinned to an older gen even after this node
//     reports done. The pin is released when the write completes.
//   - At-rest categories not yet wired into the sweep (datawal, logical WAL,
//     FSM values, IAM data, snapshots — all remain as future S6 sub-slices).
//
// Consequence for S7 prune
//
// The ledger is a necessary condition for pruning a retired generation, not a
// sufficient one. A safe prune MUST additionally confirm:
//   - dekRefCounts[gen] == 0 (no active reader holds a reference), AND
//   - no in-flight S4-pinned encode is still writing under gen.
// Treat IsGenFullyRewrapped as one gate in an AND-conjunction, not a standalone
// prune authorization.
func (f *MetaFSM) IsGenFullyRewrapped(gen uint32, nodes []string) bool {
	if len(nodes) == 0 {
		return false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	set := f.dekRewrapDone[gen]
	if set == nil {
		return false
	}
	for _, n := range nodes {
		if _, ok := set[n]; !ok {
			return false
		}
	}
	return true
}
