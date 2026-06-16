package serveruntime

import "testing"

// TestRedundancyUpgradeMax covers the boot-wiring decision helper that maps
// Config into the scrubber.EnableRedundancyUpgrade(max) call.
func TestRedundancyUpgradeMax(t *testing.T) {
	tests := []struct {
		name        string
		cfg         Config
		wantEnabled bool
		wantMax     int
	}{
		{
			name:        "enabled with explicit max",
			cfg:         Config{ECRedundancyUpgrade: true, ECRedundancyUpgradeMax: 8},
			wantEnabled: true,
			wantMax:     8,
		},
		{
			name:        "disabled is off regardless of max",
			cfg:         Config{ECRedundancyUpgrade: false, ECRedundancyUpgradeMax: 8},
			wantEnabled: false,
		},
		{
			name:        "enabled with zero max falls back to default 8",
			cfg:         Config{ECRedundancyUpgrade: true, ECRedundancyUpgradeMax: 0},
			wantEnabled: true,
			wantMax:     8,
		},
		{
			name:        "enabled with negative max falls back to default 8",
			cfg:         Config{ECRedundancyUpgrade: true, ECRedundancyUpgradeMax: -3},
			wantEnabled: true,
			wantMax:     8,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enabled, max := redundancyUpgradeMax(tt.cfg)
			if enabled != tt.wantEnabled {
				t.Fatalf("enabled = %v, want %v", enabled, tt.wantEnabled)
			}
			if enabled && max != tt.wantMax {
				t.Fatalf("max = %d, want %d", max, tt.wantMax)
			}
		})
	}
}
