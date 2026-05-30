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
func (f *MetaFSM) IsGenFullyRewrapped(gen uint32, nodes []string) bool {
	if len(nodes) == 0 {
		return false
	}
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
