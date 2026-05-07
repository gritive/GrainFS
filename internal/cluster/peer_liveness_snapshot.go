package cluster

import "time"

type PeerIdentityState string

const (
	PeerIdentitySelf             PeerIdentityState = "self"
	PeerIdentityResolved         PeerIdentityState = "resolved"
	PeerIdentityUnresolvedLegacy PeerIdentityState = "unresolved_legacy"
)

type PeerLivenessState string

const (
	PeerLivenessConfigured     PeerLivenessState = "configured"
	PeerLivenessLive           PeerLivenessState = "live"
	PeerLivenessHealthCooldown PeerLivenessState = "health_cooldown"
	PeerLivenessProbeFailed    PeerLivenessState = "probe_failed"
)

type PeerLivenessInput struct {
	SelfID       string
	Voters       []string
	AddressBook  NodeAddressBook
	PeerHealth   []PeerHealthEntry
	ProbeResults []PeerProbeResult
}

type PeerProbeResult struct {
	PeerID     string
	Live       bool
	ObservedAt time.Time
	Reason     string
}

type PeerLivenessRow struct {
	PeerID        string            `json:"peer_id"`
	RaftAddr      string            `json:"raft_addr,omitempty"`
	IdentityState PeerIdentityState `json:"identity_state"`
	LivenessState PeerLivenessState `json:"liveness_state"`
	Reason        string            `json:"reason,omitempty"`
}

func BuildPeerLivenessSnapshot(input PeerLivenessInput) []PeerLivenessRow {
	if input.SelfID == "" {
		return nil
	}
	rows := make([]PeerLivenessRow, 0, len(input.Voters)+1)
	rows = append(rows, PeerLivenessRow{
		PeerID:        input.SelfID,
		IdentityState: PeerIdentitySelf,
		LivenessState: PeerLivenessLive,
		Reason:        "self",
	})

	health := peerHealthByID(input.PeerHealth)
	probes := peerProbeByID(input.ProbeResults)
	for _, voter := range input.Voters {
		resolved := ResolveShardGroupPeer(input.AddressBook, voter)
		row := PeerLivenessRow{
			PeerID:        resolved.NodeID,
			RaftAddr:      resolved.RaftAddr,
			IdentityState: PeerIdentityResolved,
			LivenessState: PeerLivenessConfigured,
			Reason:        "configured",
		}
		if row.PeerID == "" {
			row.PeerID = voter
		}
		if resolved.Unresolved {
			row.PeerID = voter
			row.RaftAddr = resolved.RaftAddr
			row.IdentityState = PeerIdentityUnresolvedLegacy
			row.LivenessState = PeerLivenessConfigured
			row.Reason = "identity_unresolved"
			rows = append(rows, row)
			continue
		}

		if probe, ok := probes[row.PeerID]; ok {
			if probe.Live {
				row.LivenessState = PeerLivenessLive
				row.Reason = "probe_live"
			} else {
				row.LivenessState = PeerLivenessProbeFailed
				row.Reason = probe.Reason
				if row.Reason == "" {
					row.Reason = "probe_failed"
				}
			}
		} else if entry, ok := health[row.PeerID]; ok && !entry.Healthy {
			row.LivenessState = PeerLivenessHealthCooldown
			row.Reason = "peer_health_cooldown"
		}
		rows = append(rows, row)
	}
	return rows
}

func IsExplicitlyDown(row PeerLivenessRow) bool {
	return row.LivenessState == PeerLivenessHealthCooldown || row.LivenessState == PeerLivenessProbeFailed
}

func IsAliveForMembershipMutation(row PeerLivenessRow) bool {
	if row.IdentityState == PeerIdentitySelf {
		return true
	}
	return row.IdentityState == PeerIdentityResolved && row.LivenessState == PeerLivenessLive
}

func BlocksMembershipMutation(row PeerLivenessRow) bool {
	return row.IdentityState == PeerIdentityUnresolvedLegacy
}

func peerHealthByID(entries []PeerHealthEntry) map[string]PeerHealthEntry {
	out := make(map[string]PeerHealthEntry, len(entries))
	for _, entry := range entries {
		out[entry.ID] = entry
	}
	return out
}

func peerProbeByID(entries []PeerProbeResult) map[string]PeerProbeResult {
	out := make(map[string]PeerProbeResult, len(entries))
	for _, entry := range entries {
		if current, ok := out[entry.PeerID]; ok && peerProbePrecedes(current, entry) {
			continue
		}
		out[entry.PeerID] = entry
	}
	return out
}

func peerProbePrecedes(current, candidate PeerProbeResult) bool {
	if current.Live != candidate.Live {
		return current.Live
	}
	return !candidate.ObservedAt.After(current.ObservedAt)
}
