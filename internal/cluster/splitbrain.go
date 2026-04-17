package cluster

// PeerStatus represents the observed state of a cluster peer.
type PeerStatus struct {
	NodeID string
	State  string // "Leader", "Follower", "Candidate"
	Term   uint64
}

// SplitBrainDetector checks a snapshot of peer states for split brain indicators.
type SplitBrainDetector struct{}

// NewSplitBrainDetector creates a new detector.
func NewSplitBrainDetector() *SplitBrainDetector {
	return &SplitBrainDetector{}
}

// Detect returns true if the given peer states suggest a split brain condition:
//   - More than one node claims to be Leader, OR
//   - Term values diverge by more than 1 (indicates isolated partitions)
func (d *SplitBrainDetector) Detect(peers []PeerStatus) bool {
	if len(peers) < 2 {
		return false
	}

	leaders := 0
	minTerm, maxTerm := peers[0].Term, peers[0].Term
	for _, p := range peers {
		if p.State == "Leader" {
			leaders++
		}
		if p.Term < minTerm {
			minTerm = p.Term
		}
		if p.Term > maxTerm {
			maxTerm = p.Term
		}
	}

	if leaders > 1 {
		return true
	}
	if maxTerm-minTerm > 1 {
		return true
	}
	return false
}
