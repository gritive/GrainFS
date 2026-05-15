package server

import "github.com/gritive/GrainFS/internal/cluster"

// mapPeerHealthRows translates internal PeerLivenessRow into the wire
// PeerHealthRow shape. State mapping:
//
//	PeerIdentitySelf                  -> "self"
//	PeerLivenessLive                  -> "live"
//	PeerLivenessHealthCooldown        -> "cooldown"
//	PeerLivenessProbeFailed           -> "down"
//	PeerLivenessConfigured            -> "configured"
//	(any future state added upstream) -> string(r.LivenessState)
//
// The fall-through preserves the raw internal name so a new state isn't
// silently rendered as "configured" - operators see something unfamiliar
// and can investigate, and deriveIssues can match on it later if needed.
func mapPeerHealthRows(rows []cluster.PeerLivenessRow) []PeerHealthRow {
	out := make([]PeerHealthRow, 0, len(rows))
	for _, r := range rows {
		var state string
		switch {
		case r.IdentityState == cluster.PeerIdentitySelf:
			state = "self"
		case r.LivenessState == cluster.PeerLivenessLive:
			state = "live"
		case r.LivenessState == cluster.PeerLivenessHealthCooldown:
			state = "cooldown"
		case r.LivenessState == cluster.PeerLivenessProbeFailed:
			state = "down"
		case r.LivenessState == cluster.PeerLivenessConfigured:
			state = "configured"
		default:
			state = string(r.LivenessState)
		}
		out = append(out, PeerHealthRow{
			PeerID:   r.PeerID,
			State:    state,
			RaftAddr: r.RaftAddr,
		})
	}
	return out
}
