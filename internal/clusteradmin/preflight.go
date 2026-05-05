package clusteradmin

import "fmt"

// Preflight summarises the projected post-removal voter state so callers can
// surface the math to the operator before the network round-trip and so the
// rule itself stays unit-tested in isolation.
type Preflight struct {
	VotersBefore int
	VotersAfter  int
	AliveBefore  int
	AliveAfter   int
	NewQuorum    int
	WouldBlock   bool
}

// Check produces a Preflight for removing target. peers is the **remote**
// voter set (excludes the running node); the running node is implicitly
// counted as a voter because it must reach itself to commit anything. Live
// is the set of reachable voters and is expected to include the running
// node — that mirrors backend.LiveNodes() / cluster.LivePeers().
//
// "remove self" is rejected upstream this round (Q2 design — no cluster
// leave); the math here treats target as a remote.
func Check(peers, live []string, target string) Preflight {
	totalVoters := len(peers) + 1 // peers excludes self; +1 for the running node
	votersAfter := totalVoters - 1
	aliveAfter := 0
	for _, p := range live {
		if p == target {
			continue
		}
		aliveAfter++
	}
	q := votersAfter/2 + 1
	if q < 1 {
		q = 1
	}
	return Preflight{
		VotersBefore: totalVoters,
		VotersAfter:  votersAfter,
		AliveBefore:  len(live),
		AliveAfter:   aliveAfter,
		NewQuorum:    q,
		WouldBlock:   aliveAfter < q,
	}
}

// Summary returns a one-line render suitable for stderr.
func (p Preflight) Summary() string {
	return fmt.Sprintf("voters: %d -> %d   alive_after: %d   new_quorum: %d",
		p.VotersBefore, p.VotersAfter, p.AliveAfter, p.NewQuorum)
}

// Contains reports whether s is in xs. Exposed because both Status checks
// and rendering share the same membership question.
func Contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// LivePeersFromStatus derives live = (peers ∪ {selfID}) \ down_nodes. Mirrors
// what server-side cluster.LivePeers() returns: self is always live, peers
// listed in down_nodes are filtered out.
func LivePeersFromStatus(peers, downNodes []string, selfID string) []string {
	down := make(map[string]struct{}, len(downNodes))
	for _, d := range downNodes {
		down[d] = struct{}{}
	}
	out := make([]string, 0, len(peers)+1)
	if selfID != "" {
		out = append(out, selfID)
	}
	for _, p := range peers {
		if _, isDown := down[p]; isDown {
			continue
		}
		out = append(out, p)
	}
	return out
}
