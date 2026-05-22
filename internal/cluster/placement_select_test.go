package cluster

// placement_select_test.go: selectECPlacementWeighted 단위 테스트.
//
// PlaceShards rendezvous hashing 위임을 검증한다. weightedEnabled=false로
// 레거시 비가중 경로를 테스트하며, 기존 selectECPlacement와 동치임을 보장한다.

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectECPlacement_AllLive(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	liveNodes := []string{"a", "b", "c"}

	placement := selectECPlacementWeighted(cfg, liveNodes, "obj/v1", nil, nil, false, false)

	assert.Len(t, placement, cfg.NumShards())
	expected := PlaceShards("obj/v1", liveNodes, nil, cfg.NumShards())
	assert.Equal(t, expected, placement)
}

func TestSelectECPlacement_DeadNode(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	liveNodes := []string{"a", "b", "c"}

	placement := selectECPlacementWeighted(cfg, liveNodes, "obj/v1", nil, nil, false, false)

	assert.Len(t, placement, cfg.NumShards())
	expected := PlaceShards("obj/v1", liveNodes, nil, cfg.NumShards())
	assert.Equal(t, expected, placement)
}

func TestSelectECPlacement_PartialDead(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	allNodes := []string{"n0", "n1", "n2", "n3", "n4", "n5"}
	liveNodes := []string{"n0", "n1", "n2", "n3"} // n4, n5 dead

	// 결정론적 비교: HRW가 n4/n5를 후보에 포함시키는 키를 선정한다.
	var triggerKey string
	for i := 0; i < 10_000; i++ {
		k := "obj/" + strconv.Itoa(i)
		cand := PlaceShards(k, allNodes, nil, cfg.NumShards())
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
	require.NotEmpty(t, triggerKey, "HRW never selected a dead node candidate")

	placement := selectECPlacementWeighted(cfg, liveNodes, triggerKey, nil, nil, false, false)
	expected := PlaceShards(triggerKey, liveNodes, nil, cfg.NumShards())
	assert.Equal(t, expected, placement)
}
