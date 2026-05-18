package cluster

import "testing"

func TestDefaultAppendForwardBufferConfig(t *testing.T) {
	cfg := DefaultAppendForwardBufferConfig()
	if cfg.TotalBytes != 512*1024*1024 {
		t.Errorf("TotalBytes default = %d; want 512 MiB", cfg.TotalBytes)
	}
	if cfg.MaxPerRequest != 64*1024*1024 {
		t.Errorf("MaxPerRequest default = %d; want 64 MiB", cfg.MaxPerRequest)
	}
}
