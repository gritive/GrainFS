package raft

import "sort"

type membershipView struct {
	phase           jointPhase
	current         []string
	old             []string
	peers           []string
	learners        map[string]string
	managedLearners []string
	removedSelf     bool
	enterIndex      uint64
}

func (n *Node) publishMembershipViewLocked() {
	view := &membershipView{
		phase:           n.jointPhase,
		peers:           append([]string(nil), n.config.Peers...),
		learners:        copyStringMap(n.learnerIDs),
		managedLearners: sortedManagedLearners(n.jointManagedLearners),
		removedSelf:     n.removedFromCluster,
		enterIndex:      n.jointEnterIndex,
	}
	if n.jointPhase == JointEntering {
		view.current = append([]string(nil), n.jointNewVoters...)
		view.old = append([]string(nil), n.jointOldVoters...)
	} else {
		if n.removedFromCluster {
			view.current = append([]string(nil), n.config.Peers...)
		} else {
			view.current = make([]string, 0, len(n.config.Peers)+1)
			view.current = append(view.current, n.id)
			view.current = append(view.current, n.config.Peers...)
		}
	}
	n.membership.Store(view)
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
	if !view.hasMajority(self, matched, view.current) {
		return false
	}
	if view.old != nil && !view.hasMajority(self, matched, view.old) {
		return false
	}
	return true
}

func (view *membershipView) allVotersExcept(self string) []string {
	seen := make(map[string]struct{}, len(view.current)+len(view.old))
	for _, id := range view.current {
		if id != self {
			seen[id] = struct{}{}
		}
	}
	for _, id := range view.old {
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
		seen := make(map[string]struct{}, len(view.current)+len(view.old)+len(view.learners))
		servers := make([]Server, 0, len(view.current)+len(view.old)+len(view.learners))
		for _, id := range view.old {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			servers = append(servers, Server{ID: id, Suffrage: Voter})
		}
		for _, id := range view.current {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			servers = append(servers, Server{ID: id, Suffrage: Voter})
		}
		for _, peerKey := range view.learners {
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
	servers := make([]Server, 0, len(view.peers)+1+len(view.learners))
	if !view.removedSelf {
		servers = append(servers, Server{ID: self, Suffrage: Voter})
	}
	for _, peer := range view.peers {
		servers = append(servers, Server{ID: peer, Suffrage: Voter})
	}
	for _, peerKey := range view.learners {
		servers = append(servers, Server{ID: peerKey, Suffrage: NonVoter})
	}
	return servers
}

func (view *membershipView) jointSnapshotState() (phase int8, oldVoters, newVoters []string, enterIndex uint64, managedLearners []string) {
	return int8(view.phase),
		append([]string(nil), view.old...),
		append([]string(nil), view.current...),
		view.enterIndex,
		append([]string(nil), view.managedLearners...)
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
