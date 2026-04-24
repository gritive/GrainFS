package cluster

import (
	"testing"
)

func TestECSplit_AllocsBounded(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 4*1024)
	// warmup to populate encoder cache
	_, _ = ECSplit(cfg, data)
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = ECSplit(cfg, data)
	})
	t.Logf("ECSplit allocs after pool: %.0f", allocs)
	if allocs > 15 {
		t.Errorf("ECSplit allocates %.0f times (want ≤15, baseline was 59)", allocs)
	}
}

func TestECReconstruct_AllocsBounded(t *testing.T) {
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	data := make([]byte, 4*1024)
	shards, _ := ECSplit(cfg, data)
	// warmup to populate encoder cache
	_, _ = ECReconstruct(cfg, shards)
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = ECReconstruct(cfg, shards)
	})
	t.Logf("ECReconstruct allocs after pool: %.0f", allocs)
	if allocs > 12 {
		t.Errorf("ECReconstruct allocates %.0f times (want ≤12, baseline was 48)", allocs)
	}
}
