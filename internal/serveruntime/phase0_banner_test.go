package serveruntime

import (
	"bytes"
	"strings"
	"testing"
)

func TestBootPhase0BannerEmitsDefaultBucketWarning(t *testing.T) {
	var buf bytes.Buffer
	state := &bootState{bannerWriter: &buf}
	if err := bootPhase0Banner(state); err != nil {
		t.Fatalf("bootPhase0Banner: %v", err)
	}
	if !strings.Contains(buf.String(), "default bucket anonymous access") {
		t.Fatalf("expected default bucket warning, got: %q", buf.String())
	}
}

func TestBootPhase0BannerNilWriter(t *testing.T) {
	if err := bootPhase0Banner(&bootState{}); err != nil {
		t.Fatalf("nil writer should be a no-op, got: %v", err)
	}
}
