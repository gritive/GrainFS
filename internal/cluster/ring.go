package cluster

import (
	"hash/fnv"
	"sort"
	"strconv"
)

// RingVersion은 링 스냅샷의 단조 증가 버전 번호다.
type RingVersion uint64

// VirtualNode는 링 위 가상 노드다.
type VirtualNode struct {
	Token  uint32 // 링 위치 (0 ~ 2^32-1)
	NodeID string
}

// Ring은 컨시스턴트 해시 링이다.
type Ring struct {
	Version  RingVersion
	VNodes   []VirtualNode // Token 기준 정렬
	VPerNode int           // 물리 노드당 가상 노드 수
}

// NewRing은 nodes 목록으로 Ring을 생성한다. vPerNode <= 0이면 150 사용.
func NewRing(version RingVersion, nodes []string, vPerNode int) *Ring {
	if vPerNode <= 0 {
		vPerNode = 150
	}
	vnodes := make([]VirtualNode, 0, len(nodes)*vPerNode)
	for _, nodeID := range nodes {
		for i := 0; i < vPerNode; i++ {
			token := ringFNV32(nodeID + "/" + strconv.Itoa(i))
			vnodes = append(vnodes, VirtualNode{Token: token, NodeID: nodeID})
		}
	}
	sort.Slice(vnodes, func(i, j int) bool {
		return vnodes[i].Token < vnodes[j].Token
	})
	return &Ring{Version: version, VNodes: vnodes, VPerNode: vPerNode}
}

// PlacementForKey는 key의 EC 샤드 k+m개를 담당할 물리 노드 목록을 반환한다.
// 샤드 i마다 hash(key+"/"+i)에서 시계방향으로 걸어가며 아직 선택되지 않은 노드를 선택한다.
// 결과는 결정론적: 동일한 링 + 동일한 key → 항상 동일한 노드 목록.
func (r *Ring) PlacementForKey(cfg ECConfig, key string) []string {
	n := cfg.NumShards()
	chosen := make([]string, 0, n)
	seen := make(map[string]bool, n)

	for i := 0; i < n; i++ {
		token := ringFNV32(key + "/" + strconv.Itoa(i))
		nodeID := r.walkCW(token, seen)
		if nodeID == "" {
			break // 노드 부족 — 호출자가 처리
		}
		chosen = append(chosen, nodeID)
		seen[nodeID] = true
	}
	return chosen
}

// walkCW는 token에서 시계방향으로 걸어가며 seen에 없는 첫 번째 물리 노드 ID를 반환한다.
// 모든 노드가 seen에 있으면 "" 반환.
func (r *Ring) walkCW(token uint32, seen map[string]bool) string {
	if len(r.VNodes) == 0 {
		return ""
	}
	// 시작 인덱스: token 이상인 첫 번째 vnode
	start := sort.Search(len(r.VNodes), func(i int) bool {
		return r.VNodes[i].Token >= token
	})
	// 링을 최대 한 바퀴 순회
	for i := 0; i < len(r.VNodes); i++ {
		idx := (start + i) % len(r.VNodes)
		nodeID := r.VNodes[idx].NodeID
		if !seen[nodeID] {
			return nodeID
		}
	}
	return ""
}

// ringFNV32는 FNV-1a 32비트 해시를 반환한다.
func ringFNV32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
