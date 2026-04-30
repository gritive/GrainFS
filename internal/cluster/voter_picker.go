package cluster

import (
	"bytes"
	"crypto/sha256"
	"sort"
)

// pickVoters returns RF voters for groupID, deterministically chosen from allNodes
// using rendezvous hashing (HRW). Stable across processes/restarts.
//
// Result is sorted alphabetically — peer lists must be deterministic for raft
// membership comparison.
func pickVoters(groupID string, allNodes []string, rf int) []string {
	if len(allNodes) == 0 {
		return nil
	}
	if rf > len(allNodes) {
		rf = len(allNodes)
	}
	type ranked struct {
		id    string
		score [32]byte
	}
	items := make([]ranked, len(allNodes))
	for i, n := range allNodes {
		items[i] = ranked{id: n, score: sha256.Sum256([]byte(groupID + "/" + n))}
	}
	sort.Slice(items, func(i, j int) bool {
		return bytes.Compare(items[i].score[:], items[j].score[:]) < 0
	})
	out := make([]string, rf)
	for i := 0; i < rf; i++ {
		out[i] = items[i].id
	}
	sort.Strings(out)
	return out
}

// PickVoters is the exported wrapper for pickVoters. Used by cmd/grainfs/serve.go.
func PickVoters(groupID string, allNodes []string, rf int) []string {
	return pickVoters(groupID, allNodes, rf)
}
