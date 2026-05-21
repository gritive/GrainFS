package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"slices"
)

// PlaceShards selects `count` nodes from `nodes` to host the EC shards of `key`,
// using Weighted Rendezvous Hashing (Schindelhauer / Wang-Ravishankar).
//
// weights == nil  : 모든 노드를 weight 1.0으로 간주 (plain HRW와 동치).
// weights != nil  : len(weights) == len(nodes), 모든 값 >= 0.
//
//	weight=0 노드는 placement 후보에서 제외 (drain 의미).
//
// Deterministic: same (key, nodes, weights) → same ordered NodeIDs.
// 노드 한 개 추가/제거 시 placement 변동 비율 ≈ (해당 노드 weight) / Σweights.
func PlaceShards(key string, nodes []string, weights []float64, count int) []string {
	if len(nodes) == 0 || count <= 0 {
		return nil
	}
	if weights != nil && len(weights) != len(nodes) {
		panic("PlaceShards: len(weights) must equal len(nodes)")
	}

	type ranked struct {
		node  string
		score float64
	}
	var arr [64]ranked
	var heap []ranked
	used := arr[:0]
	if len(nodes) > cap(arr) {
		heap = make([]ranked, 0, len(nodes))
		used = heap
	}

	for i, n := range nodes {
		var w float64 = 1
		if weights != nil {
			w = weights[i]
			if w <= 0 {
				continue
			}
		}
		u := hrwUniform(key, n)
		var s float64
		if weights == nil {
			// Fast path: 모든 weight 동일 → ranking ∝ u 단조. math.Log 스킵.
			// u → 1일수록 -1/ln(u) → +∞ 으로 단조 증가하므로, u 내림차순 = score 내림차순.
			s = u
		} else {
			s = -w / math.Log(u)
		}
		used = append(used, ranked{node: n, score: s})
	}

	slices.SortFunc(used, func(a, b ranked) int {
		switch {
		case a.score > b.score:
			return -1
		case a.score < b.score:
			return 1
		default:
			// tiebreak: lexicographic NodeID for total order
			if a.node < b.node {
				return -1
			} else if a.node > b.node {
				return 1
			}
			return 0
		}
	})

	if count > len(used) {
		count = len(used)
	}
	out := make([]string, count)
	for i := 0; i < count; i++ {
		out[i] = used[i].node
	}
	return out
}

// hrwUniform returns u ∈ (0, 1] derived from sha256(key + "/" + nodeID).
func hrwUniform(key, nodeID string) float64 {
	h := sha256.New()
	h.Write([]byte(key))
	h.Write([]byte{'/'})
	h.Write([]byte(nodeID))
	sum := h.Sum(nil)
	v := binary.BigEndian.Uint64(sum[:8])
	// 53-bit precision (float64 mantissa), shift to (0, 1].
	return (float64(v>>11) + 1) / (1 << 53)
}
