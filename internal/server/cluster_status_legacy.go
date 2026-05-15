package server

import "github.com/gritive/GrainFS/internal/cluster"

func legacyPeersFromSnapshot(rows []cluster.PeerLivenessRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.IdentityState == cluster.PeerIdentitySelf {
			continue
		}
		out = append(out, row.PeerID)
	}
	return out
}

func legacyPeerAddrsFromSnapshot(rows []cluster.PeerLivenessRow) map[string]string {
	out := make(map[string]string)
	for _, row := range rows {
		if row.IdentityState == cluster.PeerIdentitySelf || row.RaftAddr == "" {
			continue
		}
		out[row.PeerID] = row.RaftAddr
	}
	return out
}

func legacyPeerStatesFromSnapshot(rows []cluster.PeerLivenessRow) map[string]string {
	out := make(map[string]string)
	for _, row := range rows {
		if row.IdentityState == cluster.PeerIdentitySelf {
			continue
		}
		if row.IdentityState == cluster.PeerIdentityUnresolvedLegacy {
			out[row.PeerID] = string(cluster.PeerIdentityUnresolvedLegacy)
			continue
		}
		out[row.PeerID] = string(row.LivenessState)
	}
	return out
}

func legacyDownNodesFromSnapshot(rows []cluster.PeerLivenessRow) []string {
	out := make([]string, 0)
	for _, row := range rows {
		if row.IdentityState == cluster.PeerIdentitySelf || row.IdentityState == cluster.PeerIdentityUnresolvedLegacy {
			continue
		}
		if cluster.IsExplicitlyDown(row) {
			out = append(out, row.PeerID)
		}
	}
	return out
}
