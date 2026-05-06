package cluster

const (
	RemovePeerPreflightAllowed            = "allowed"
	RemovePeerPreflightNotInCluster       = "not_in_cluster"
	RemovePeerPreflightIdentityUnresolved = "identity_unresolved"
	RemovePeerPreflightQuorumWouldBreak   = "quorum_would_break"
)

type RemovePeerPreflightInput struct {
	TargetID string
	Voters   []string
	Snapshot []PeerLivenessRow
}

type RemovePeerPreflightResult struct {
	Allowed       bool
	Reason        string
	VotersAfter   int
	AliveAfter    int
	NewQuorum     int
	BlockingPeers []string
}

func EvaluateRemovePeerPreflight(input RemovePeerPreflightInput) RemovePeerPreflightResult {
	votersAfter := len(input.Voters)
	newQuorum := votersAfter/2 + 1
	if newQuorum < 1 {
		newQuorum = 1
	}
	result := RemovePeerPreflightResult{
		Reason:      RemovePeerPreflightAllowed,
		VotersAfter: votersAfter,
		NewQuorum:   newQuorum,
	}

	if !removePeerTargetInCluster(input.TargetID, input.Voters, input.Snapshot) {
		result.Reason = RemovePeerPreflightNotInCluster
		return result
	}

	for _, row := range input.Snapshot {
		if BlocksMembershipMutation(row) && row.PeerID != input.TargetID {
			result.BlockingPeers = append(result.BlockingPeers, row.PeerID)
		}
	}
	if len(result.BlockingPeers) > 0 {
		result.Reason = RemovePeerPreflightIdentityUnresolved
		return result
	}

	for _, row := range input.Snapshot {
		if row.PeerID == input.TargetID {
			continue
		}
		if IsAliveForMembershipMutation(row) {
			result.AliveAfter++
		}
	}
	if result.AliveAfter < result.NewQuorum {
		result.Reason = RemovePeerPreflightQuorumWouldBreak
		return result
	}

	result.Allowed = true
	return result
}

func removePeerTargetInCluster(target string, voters []string, snapshot []PeerLivenessRow) bool {
	for _, voter := range voters {
		if voter == target {
			return true
		}
	}
	for _, row := range snapshot {
		if row.PeerID == target && row.IdentityState != PeerIdentitySelf {
			return true
		}
	}
	return false
}
