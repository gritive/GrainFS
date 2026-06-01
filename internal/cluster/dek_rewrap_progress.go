package cluster

import "fmt"

// applyDEKRewrapProgress records that nodeID finished rewrapping gen. The
// done-set is replicated FSM state; the leader derives prune-safety from it via
// IsGenFullyRewrapped. (S6a produces no such commands — this path is exercised
// by unit tests until a later slice wires a producer.)
func (f *MetaFSM) applyDEKRewrapProgress(data []byte) error {
	nodeID, gen, epoch, err := decodeMetaDEKRewrapProgressCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: DEKRewrapProgress: %w", err)
	}
	if nodeID == "" {
		return fmt.Errorf("meta_fsm: DEKRewrapProgress: empty node_id")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dekRewrapDone == nil {
		f.dekRewrapDone = make(map[uint32]map[string]uint32)
	}
	set := f.dekRewrapDone[gen]
	if set == nil {
		set = make(map[string]uint32)
		f.dekRewrapDone[gen] = set
	}
	// max-monotonic: record the epoch only when it advances. A stale re-report
	// (epoch lower than what is already stored) is silently ignored — it
	// cannot lower the recorded epoch because that would loosen the prune gate
	// for consumers that require a minimum epoch.
	if cur, ok := set[nodeID]; !ok || epoch > cur {
		set[nodeID] = epoch
	}
	return nil
}

// IsGenFullyRewrapped reports whether every node in nodes has reported
// completion for gen at or above requiredEpoch. The caller supplies the node
// set: prune-safety needs the set of nodes that can HOLD gen data (e.g.
// EC-shard owners), which is not necessarily the meta-raft voter set —
// resolving that set is the later prune consumer's responsibility. An empty
// node set is never "fully rewrapped".
//
// Semantics of a single completion record
//
// A record means "at the time this node reported, it held no at-rest data below
// gen within the ENUMERATED LANE CATEGORIES covered by the reported epoch":
//   - epoch 0: EC shards of all object versions (S6c-allversions, #692) and
//     packed-blob entries (S6b, #687).
//   - epoch 1 (future, S7-1a): + FSM-value lane.
//   - Each new lane bumps CurrentRewrapLaneSetEpoch when it is registered.
//
// A record does NOT cover:
//   - In-flight S4 seal-at-pinned-gen encodes: a shard currently being written
//     by an ongoing PUT may still be pinned to an older gen even after this node
//     reports done. The pin is released when the write completes.
//   - Lanes whose epoch has not yet been included in the node's reported epoch.
//
// Consequence for S7 prune
//
// The ledger is a necessary condition for pruning a retired generation, not a
// sufficient one. A safe prune MUST additionally confirm:
//   - dekRefCounts[gen] == 0 (no active reader holds a reference), AND
//   - no in-flight S4-pinned encode is still writing under gen, AND
//   - requiredEpoch covers all lanes that hold data for gen.
// Treat IsGenFullyRewrapped as one gate in an AND-conjunction, not a standalone
// prune authorization.
func (f *MetaFSM) IsGenFullyRewrapped(gen uint32, nodes []string, requiredEpoch uint32) bool {
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
		e, ok := set[n]
		if !ok || e < requiredEpoch {
			return false
		}
	}
	return true
}
