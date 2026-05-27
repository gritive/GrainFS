package server

import (
	"bytes"
	"strings"
	"testing"
)

func TestDefaultBucketBannerEmits(t *testing.T) {
	var buf bytes.Buffer
	EmitBanner(&buf, false)
	if !strings.Contains(buf.String(), "default bucket anonymous access") {
		t.Fatalf("expected default bucket anonymous warning, got: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "s3://default") {
		t.Fatalf("expected s3://default in warning, got: %q", buf.String())
	}
}

func TestDefaultBucketBannerAlreadyEmittedSuppresses(t *testing.T) {
	var buf bytes.Buffer
	EmitBanner(&buf, true)
	if buf.Len() != 0 {
		t.Fatalf("expected no output when alreadyEmitted=true, got: %q", buf.String())
	}
}
