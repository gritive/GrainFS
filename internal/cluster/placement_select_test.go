package cluster

// placement_select_test.go: selectECPlacement 단위 테스트.
//
// putObjectEC가 ring 경로 vs PlacementForNodes 폴백을 선택할 때의 모든 분기를
// 검증한다. 폴백은 ring 후보에 unhealthy/제거된 노드가 포함될 때 트리거되며
// (write-all 정합성 위반 방지), ringVer=0이 되어 read 시 metaNodeIDs 경로를
//타도록 한다.

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectECPlacement_NoRing(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	liveNodes := []string{"a", "b", "c"}

	placement, ringVer := selectECPlacement(nil, errors.New("ring not found"), cfg, liveNodes, "obj/v1")

	assert.Equal(t, RingVersion(0), ringVer, "missing ring → ringVer=0")
	assert.Len(t, placement, cfg.NumShards(), "fallback must produce k+m placement")
	expected := PlacementForNodes(cfg, liveNodes, "obj/v1")
	assert.Equal(t, expected, placement, "fallback must match PlacementForNodes(liveNodes)")
}

func TestSelectECPlacement_RingAllLive(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	liveNodes := []string{"a", "b", "c"}
	ring := NewRing(7, liveNodes, 10)

	placement, ringVer := selectECPlacement(ring, nil, cfg, liveNodes, "obj/v1")

	assert.Equal(t, RingVersion(7), ringVer, "all-live ring path → ringVer=ring.Version")
	expected := ring.PlacementForKey(cfg, "obj/v1")
	assert.Equal(t, expected, placement, "all-live ring path → ring.PlacementForKey")
}

func TestSelectECPlacement_RingHasDeadNode(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	liveNodes := []string{"a", "b", "c"}
	// ring에는 dead-node가 포함된 4 노드 토폴로지. 노드별 vnode 수가 충분히 많아
	// 적어도 한 샤드는 dead-node로 매핑될 가능성이 높지만, 결정론을 위해 dead-node
	// 만 포함한 ring을 사용해 모든 후보가 죽은 케이스로 강제한다.
	ring := NewRing(11, []string{"dead-node"}, 4)

	placement, ringVer := selectECPlacement(ring, nil, cfg, liveNodes, "obj/v1")

	assert.Equal(t, RingVersion(0), ringVer, "dead-node가 후보에 있으면 ringVer=0으로 폴백")
	expected := PlacementForNodes(cfg, liveNodes, "obj/v1")
	assert.Equal(t, expected, placement, "폴백은 PlacementForNodes(liveNodes)와 동일해야 함")
}

func TestSelectECPlacement_RingPartialDead(t *testing.T) {
	// ring은 6노드로 만들어졌으나 노드 둘이 죽었다고 가정.
	// liveNodes에 없는 노드가 ring 후보 중 하나라도 있으면 폴백해야 한다.
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	allNodes := []string{"n0", "n1", "n2", "n3", "n4", "n5"}
	liveNodes := []string{"n0", "n1", "n2", "n3"} // n4, n5 dead
	ring := NewRing(13, allNodes, 50)

	// 결정론적 비교: ring이 n4/n5를 후보에 포함시키는 키를 선정.
	// 다양한 키로 시도하여 dead-node를 포함한 placement를 사용한다.
	var triggerKey string
	for _, k := range []string{"obj/v1", "alpha/v1", "beta/v1", "gamma/v1", "delta/v1", "echo/v1"} {
		cand := ring.PlacementForKey(cfg, k)
		hasDead := false
		for _, n := range cand {
			if n == "n4" || n == "n5" {
				hasDead = true
				break
			}
		}
		if hasDead {
			triggerKey = k
			break
		}
	}
	if triggerKey == "" {
		t.Skip("랜덤 키들이 모두 live 노드만 포함 — 환경/해시 의존")
	}

	placement, ringVer := selectECPlacement(ring, nil, cfg, liveNodes, triggerKey)
	assert.Equal(t, RingVersion(0), ringVer, "후보에 dead-node 하나라도 있으면 ringVer=0")
	expected := PlacementForNodes(cfg, liveNodes, triggerKey)
	assert.Equal(t, expected, placement)
}

func TestSelectECPlacement_RingErrPropagates(t *testing.T) {
	// ring != nil + ringErr != nil인 비현실적 케이스 — 안전 측면에서 폴백.
	cfg := ECConfig{DataShards: 1, ParityShards: 1}
	liveNodes := []string{"a", "b"}
	ring := NewRing(3, liveNodes, 4)

	placement, ringVer := selectECPlacement(ring, errors.New("transient"), cfg, liveNodes, "k/v")
	assert.Equal(t, RingVersion(0), ringVer, "ringErr이면 무조건 폴백")
	expected := PlacementForNodes(cfg, liveNodes, "k/v")
	assert.Equal(t, expected, placement)
}
