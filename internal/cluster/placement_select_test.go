package cluster

// placement_select_test.go: selectECPlacementFromNodeStates 단위 테스트.
//
// PlaceShards rendezvous hashing 위임을 검증한다. weightedEnabled=false로
// 레거시 비가중 경로를 테스트하며, 기존 selectECPlacement와 동치임을 보장한다.

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/hrw"
)

func TestSelectECPlacement_AllLive(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	liveNodes := []string{"a", "b", "c"}

	placement := selectECPlacementFromNodeStates(cfg, liveNodes, "obj/v1", nil, false)

	assert.Len(t, placement, cfg.NumShards())
	expected := hrw.PlaceShards("obj/v1", liveNodes, nil, cfg.NumShards())
	assert.Equal(t, expected, placement)
}

func TestSelectECPlacement_DeadNode(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	liveNodes := []string{"a", "b", "c"}

	placement := selectECPlacementFromNodeStates(cfg, liveNodes, "obj/v1", nil, false)

	assert.Len(t, placement, cfg.NumShards())
	expected := hrw.PlaceShards("obj/v1", liveNodes, nil, cfg.NumShards())
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
		cand := hrw.PlaceShards(k, allNodes, nil, cfg.NumShards())
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

	placement := selectECPlacementFromNodeStates(cfg, liveNodes, triggerKey, nil, false)
	expected := hrw.PlaceShards(triggerKey, liveNodes, nil, cfg.NumShards())
	assert.Equal(t, expected, placement)
}

// TestSelectShardPlacement_Weighted exercises the shared helper's weighted path
// directly: a non-degenerate weight slice must produce the weighted HRW order.
func TestSelectShardPlacement_Weighted(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	peers := []string{"a", "b", "c", "d"}
	weights := []float64{10, 20, 30, 40}

	got := selectShardPlacement(cfg, peers, "obj/seg", weights, true)
	want := hrw.PlaceShards("obj/seg", peers, weights, cfg.NumShards())
	assert.Equal(t, want, got)
	assert.Len(t, got, cfg.NumShards())
}

// TestSelectShardPlacement_DisabledIgnoresWeights: weightedEnabled=false must
// ignore the supplied weights and use unweighted HRW.
func TestSelectShardPlacement_DisabledIgnoresWeights(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	peers := []string{"a", "b", "c", "d"}
	weights := []float64{10, 20, 30, 40}

	got := selectShardPlacement(cfg, peers, "obj/seg", weights, false)
	want := hrw.PlaceShards("obj/seg", peers, nil, cfg.NumShards())
	assert.Equal(t, want, got)
}

// TestSelectShardPlacement_AllStaleFallback: when every supplied weight is 0
// (no current capacity stats), the helper falls back to unweighted HRW over all
// peers so the write still places a full-width stripe.
func TestSelectShardPlacement_AllStaleFallback(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	peers := []string{"a", "b", "c", "d"}
	weights := []float64{0, 0, 0, 0}

	got := selectShardPlacement(cfg, peers, "obj/seg", weights, true)
	want := hrw.PlaceShards("obj/seg", peers, nil, cfg.NumShards())
	assert.Equal(t, want, got)
	assert.Len(t, got, cfg.NumShards())
}

// TestSelectShardPlacement_WeightShortfallFallback: an exactly-k+m group where
// some peers have weight 0 (stale stats) would shrink weighted HRW below
// NumShards; the helper must fall back to unweighted HRW over all peers
// (including the stale ones) so the full-width stripe still places.
func TestSelectShardPlacement_WeightShortfallFallback(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	peers := []string{"a", "b", "c", "d"} // NumShards == 4 == len(peers)
	weights := []float64{100, 0, 0, 0}    // only 1 peer has positive weight

	got := selectShardPlacement(cfg, peers, "obj/seg", weights, true)
	require.Len(t, got, cfg.NumShards(), "must still place a full-width stripe")
	want := hrw.PlaceShards("obj/seg", peers, nil, cfg.NumShards())
	assert.Equal(t, want, got, "shortfall falls back to unweighted HRW over all peers")
	// All shards distinct (group N == k+m).
	seen := make(map[string]bool, len(got))
	for _, n := range got {
		assert.Falsef(t, seen[n], "dup node %q", n)
		seen[n] = true
	}
}
