package raft

import "sort"

// membershipView is the immutable publication boundary for quorum readers.
//
// Canonical mutable state under n.mu:
//
//	config.Peers, learnerIDs, jointPhase, jointOldVoters, jointNewVoters
//	                      |
//	                      v
//	    publishMembershipViewLocked copies a complete snapshot
//	                      |
//	                      v
//	      lock-free readers observe one coherent membershipView
type membershipView struct {
	phase             jointPhase
	currentVoters     []string
	oldVoters         []string
	peerVoters        []string
	learnersByID      map[string]string
	managedLearnerIDs []string
	removedSelf       bool
	enterIndex        uint64
}

func (n *Node) publishMembershipViewLocked() {
	old := n.membership.Load()
	view := &membershipView{
		phase:             n.jointPhase,
		peerVoters:        append([]string(nil), n.config.Peers...),
		learnersByID:      copyStringMap(n.learnerIDs),
		managedLearnerIDs: sortedManagedLearners(n.jointManagedLearners),
		removedSelf:       n.removedFromCluster,
		enterIndex:        n.jointEnterIndex,
	}
	if n.jointPhase == JointEntering {
		view.currentVoters = append([]string(nil), n.jointNewVoters...)
		view.oldVoters = append([]string(nil), n.jointOldVoters...)
	} else {
		if n.removedFromCluster {
			view.currentVoters = append([]string(nil), n.config.Peers...)
		} else {
			view.currentVoters = make([]string, 0, len(n.config.Peers)+1)
			view.currentVoters = append(view.currentVoters, n.id)
			view.currentVoters = append(view.currentVoters, n.config.Peers...)
		}
	}
	n.membership.Store(view)
	if old != nil {
		n.drainPendingReadIndex(ErrMembershipChanged)
	}
	if n.state == Leader && n.peerReplicatorsActive {
		n.reconcilePeerReplicatorsLocked(view)
	}
}

func (n *Node) membershipViewSnapshot() *membershipView {
	if view := n.membership.Load(); view != nil {
		return view
	}
	// Fallback for tests that construct Node literals instead of NewNode.
	// Production nodes publish during NewNode before goroutines start.
	n.publishMembershipViewLocked()
	return n.membership.Load()
}

func (view *membershipView) hasMajority(self string, matched map[string]bool, set []string) bool {
	if len(set) == 0 {
		return false
	}
	count := 0
	for _, id := range set {
		if id == self || matched[id] {
			count++
		}
	}
	return count > len(set)/2
}

func (view *membershipView) dualMajority(self string, matched map[string]bool) bool {
	if !view.hasMajority(self, matched, view.currentVoters) {
		return false
	}
	if view.oldVoters != nil && !view.hasMajority(self, matched, view.oldVoters) {
		return false
	}
	return true
}

func (view *membershipView) allVotersExcept(self string) []string {
	seen := make(map[string]struct{}, len(view.currentVoters)+len(view.oldVoters))
	for _, id := range view.currentVoters {
		if id != self {
			seen[id] = struct{}{}
		}
	}
	for _, id := range view.oldVoters {
		if id != self {
			seen[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	return out
}

func (view *membershipView) configuration(self string) Configuration {
	if view.phase == JointEntering {
		seen := make(map[string]struct{}, len(view.currentVoters)+len(view.oldVoters)+len(view.learnersByID))
		servers := make([]Server, 0, len(view.currentVoters)+len(view.oldVoters)+len(view.learnersByID))
		for _, id := range view.oldVoters {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			servers = append(servers, Server{ID: id, Suffrage: Voter})
		}
		for _, id := range view.currentVoters {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			servers = append(servers, Server{ID: id, Suffrage: Voter})
		}
		for _, peerKey := range view.learnersByID {
			if _, ok := seen[peerKey]; ok {
				continue
			}
			seen[peerKey] = struct{}{}
			servers = append(servers, Server{ID: peerKey, Suffrage: NonVoter})
		}
		return Configuration{Servers: servers}
	}
	return Configuration{Servers: view.snapshotServers(self)}
}

func (view *membershipView) snapshotServers(self string) []Server {
	servers := make([]Server, 0, len(view.peerVoters)+1+len(view.learnersByID))
	if !view.removedSelf {
		servers = append(servers, Server{ID: self, Suffrage: Voter})
	}
	for _, peer := range view.peerVoters {
		servers = append(servers, Server{ID: peer, Suffrage: Voter})
	}
	for _, peerKey := range view.learnersByID {
		servers = append(servers, Server{ID: peerKey, Suffrage: NonVoter})
	}
	return servers
}

func (view *membershipView) jointSnapshotState() (phase int8, oldVoters, newVoters []string, enterIndex uint64, managedLearners []string) {
	return int8(view.phase),
		append([]string(nil), view.oldVoters...),
		append([]string(nil), view.currentVoters...),
		view.enterIndex,
		append([]string(nil), view.managedLearnerIDs...)
}

func copyStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func sortedManagedLearners(in map[string]struct{}) []string {
	out := make([]string, 0, len(in))
	for id := range in {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func stringSetEqualSlice(set map[string]struct{}, values []string) bool {
	if len(set) != len(values) {
		return false
	}
	for _, value := range values {
		if _, ok := set[value]; !ok {
			return false
		}
	}
	return true
}

func stringMapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}
