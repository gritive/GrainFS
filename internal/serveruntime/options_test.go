package serveruntime

import (
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/resourceguard"
)

func TestServeOptionsZeroValueIsUsable(t *testing.T) {
	opts := ServeOptions{}
	opts.DataDir = "/tmp/x"
	opts.Port = 9000
	opts.OTelSampleRate = 0.01
	opts.FDOpts = resourceguard.FDOptions{PollInterval: time.Second}
	if opts.DataDir != "/tmp/x" || opts.Port != 9000 {
		t.Fatal("field round-trip failed")
	}
}
