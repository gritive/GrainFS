package server

import "fmt"

// deriveIssues applies Phase-1 rules to produce human-readable issues.
// Pure function for table-driven testing.
//
// Rules:
//  1. Mode=local with configured peers -> "single-node mode"
//  2. Cluster mode + quorum lost -> "QUORUM LOST: A/V alive, need R"
//  3. Cluster mode + voters down/cooldown (quorum still healthy) -> per-voter notice
//  4. Degraded EC flag -> "EC degraded mode"
//  5. (extension point)
func deriveIssues(h Health, hasConfiguredPeers bool) []string {
	var issues []string
	if h.Mode == "local" && hasConfiguredPeers {
		issues = append(issues, "single-node mode")
	}
	if h.Mode == "cluster" {
		if !h.Quorum.Healthy {
			issues = append(issues, fmt.Sprintf(
				"QUORUM LOST: only %d/%d voters alive, need %d",
				h.Quorum.AliveCount, h.Quorum.VotersTotal, h.Quorum.Required))
		} else if h.Quorum.AliveCount < h.Quorum.VotersTotal {
			for _, p := range h.Peers {
				if p.State == "down" || p.State == "cooldown" {
					issues = append(issues, fmt.Sprintf("voter %s %s — investigate", p.PeerID, p.State))
				}
			}
		}
	}
	if h.Degraded {
		issues = append(issues, "EC degraded mode")
	}
	return issues
}
