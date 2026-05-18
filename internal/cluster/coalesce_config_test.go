package cluster

import "testing"

func TestCoalesceConfigDefaults(t *testing.T) {
	cfg := DefaultCoalesceConfig()
	if cfg.SegmentCount != 16 {
		t.Fatalf("SegmentCount default = %d, want 16", cfg.SegmentCount)
	}
	if cfg.SizeBytes != 64*1024*1024 {
		t.Fatalf("SizeBytes default = %d, want %d", cfg.SizeBytes, 64*1024*1024)
	}
	if cfg.IdleTimeout.Seconds() != 30 {
		t.Fatalf("IdleTimeout default = %v, want 30s", cfg.IdleTimeout)
	}
	if cfg.CleanupInterval.Seconds() != 60 {
		t.Fatalf("CleanupInterval default = %v, want 60s", cfg.CleanupInterval)
	}
}

func TestCoalesceConfigSizeCapDefault(t *testing.T) {
	cfg := DefaultCoalesceConfig()
	const fiveTiB = int64(5) * 1024 * 1024 * 1024 * 1024
	if cfg.SizeCapBytes != fiveTiB {
		t.Errorf("SizeCapBytes default = %d; want 5 TiB (%d)", cfg.SizeCapBytes, fiveTiB)
	}
}
